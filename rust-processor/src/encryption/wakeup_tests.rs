//! Actual TLS/PG/Redis lifecycle controlled by Go; no synthetic transport errors.
use super::{tests::local_store, *};
use crate::{
    encryption::{
        delivery_runner::{DeliveryRunner, RunnerConfig},
        discovery::TrustedServer,
        provisioning::{DeviceProvisioning, Reconciliation},
        signal_state::CryptoCoordinator,
    },
    proto::e2ee::{EncryptedPayload, TextMessage, encrypted_payload},
};
use std::{
    io::{BufRead, BufReader, Write},
    sync::{Arc, Mutex},
};
use tokio::sync::watch;

struct Control(Arc<Mutex<BufReader<std::io::Stdin>>>);
impl Control {
    async fn phase(&self, phase: &'static str) {
        println!("\nzchat-outage-control:{phase}");
        std::io::stdout().flush().unwrap();
        let reader = self.0.clone();
        let reply = tokio::task::spawn_blocking(move || {
            let mut line = String::new();
            assert!(reader.lock().unwrap().read_line(&mut line).unwrap() > 0);
            assert!(line.len() < 1024);
            line
        })
        .await
        .unwrap();
        assert_eq!(reply.trim(), format!("ready:{phase}"));
    }
}

#[test]
#[ignore = "requires two Go TLS endpoints, PostgreSQL and disposable Redis control"]
fn go_wakeup_worker() {
    let mut reader = BufReader::new(std::io::stdin());
    let mut fields = Vec::new();
    for _ in 0..6 {
        let mut line = Zeroizing::new(String::new());
        assert!(reader.read_line(&mut line).unwrap() > 0);
        assert!(line.len() < 16384);
        while line.ends_with(['\r', '\n']) {
            line.pop();
        }
        fields.push(line);
    }
    let control = Control(Arc::new(Mutex::new(reader)));
    let ca = std::fs::read(fields[2].as_str()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let root = tempfile::tempdir().unwrap();
            let origin = "https://wake-interop.example.test";
            let mut accounts = Vec::new();
            let mut stores = Vec::new();
            let mut devices = Vec::new();
            for (endpoint, token) in [(&fields[0], &fields[3]), (&fields[1], &fields[4])] {
                let mut account = TrustedServer::new(origin, endpoint, Some(&ca))
                    .unwrap()
                    .discover(origin, Zeroizing::new(token.to_string()))
                    .await
                    .unwrap();
                let mut local =
                    DeviceProvisioning::new(local_store(root.path(), account.scope(), true));
                assert_eq!(
                    account.resume_initial(&mut local).await.unwrap(),
                    Reconciliation::Ready
                );
                devices.push(local.ready_device().unwrap());
                stores.push(local.into_store());
                accounts.push(account);
            }
            let mut alice_account = accounts.remove(0);
            let mut bob_account = accounts.remove(0);
            let alice_api = alice_account.device_client().unwrap();
            let bob_api = bob_account.device_client().unwrap();
            let alice = CryptoCoordinator::start(stores.remove(0), devices[0].clone())
                .await
                .unwrap();
            let bob = CryptoCoordinator::start(stores.remove(0), devices[1].clone())
                .await
                .unwrap();
            let chat = alice_api
                .create_conversation(&alice, fields[5].parse().unwrap())
                .await
                .unwrap();
            bob_api
                .get_conversation(&bob, &chat.conversation_id)
                .await
                .unwrap();
            let scope = alice
                .conversation_scope(chat.conversation_id.clone())
                .await
                .unwrap();
            let acquisition = alice.prepare_acquisition(scope.clone()).await.unwrap();
            let bundle = alice_api.acquire(&acquisition.request).await.unwrap();
            alice.bootstrap(scope.clone(), bundle).await.unwrap();

            let wakeups = bob_api.inbox_wakeups().unwrap();
            let config = RunnerConfig {
                poll_interval: Duration::from_secs(2),
                round_timeout: Duration::from_secs(5),
                ..Default::default()
            };
            let mut runner = DeliveryRunner::new(&mut *bob_api, config).unwrap();
            let (stop, receiver) = watch::channel(false);
            let exercise = async {
                control.phase("wake-ready").await;
                for (index, value) in [
                    "redis hint",
                    "poll during redis outage",
                    "redis reconnect and duplicates",
                ]
                .into_iter()
                .enumerate()
                {
                    if index == 1 {
                        control.phase("redis-stop").await;
                    }
                    if index == 2 {
                        control.phase("redis-start").await;
                    }
                    let payload = EncryptedPayload {
                        format_version: 1,
                        message_id: Uuid::new_v4().to_string(),
                        conversation_id: chat.conversation_id.clone(),
                        sender: Some(devices[0].clone()),
                        recipient: Some(devices[1].clone()),
                        created_at_unix_ms: 1_700_000_000_123,
                        content: Some(encrypted_payload::Content::Text(TextMessage {
                            text: value.into(),
                        })),
                    };
                    let envelope = alice.encrypt(scope.clone(), payload.clone()).await.unwrap();
                    let plan = alice
                        .outgoing_plan(payload.message_id.clone())
                        .await
                        .unwrap();
                    let receipt = alice_api.send_outbox(&plan).await.unwrap();
                    alice.accept_send_receipt(receipt).await.unwrap();
                    if index == 2 {
                        for _ in 0..8 {
                            alice_api.send_outbox(&plan).await.unwrap();
                        }
                    }
                    tokio::time::timeout(Duration::from_secs(12), async {
                        loop {
                            let progress = alice
                                .outgoing_plan(payload.message_id.clone())
                                .await
                                .unwrap();
                            let statuses = alice_api.refresh_outgoing(&[progress]).await.unwrap();
                            let updated = alice.accept_message_statuses(statuses).await.unwrap();
                            if updated[0].status().is_some_and(|s| {
                                s.state == crate::proto::e2ee::MessageState::RecipientStored as i32
                            }) {
                                break;
                            }
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    })
                    .await
                    .unwrap();
                    let received_scope = bob
                        .conversation_scope(chat.conversation_id.clone())
                        .await
                        .unwrap();
                    assert_eq!(
                        bob.decrypt(received_scope, envelope).await.unwrap(),
                        payload
                    );
                    // Consume the automatic cryptographic confirmation before the next text.
                    if index == 0 {
                        let mut reverse =
                            DeliveryRunner::new(&mut *alice_api, RunnerConfig::default()).unwrap();
                        tokio::time::timeout(Duration::from_secs(8), async {
                            loop {
                                let report = reverse.poll_once(&alice).await.unwrap();
                                if report.received > 0 {
                                    break;
                                }
                                tokio::time::sleep(Duration::from_millis(50)).await;
                            }
                        })
                        .await
                        .unwrap();
                    }
                    println!("zchat-wake-phase-complete:{index}");
                }
                control.phase("watch-reconnected").await;
                // A sender seeing Stored can race the recipient's local ACK marker.
                // Let the active runner finish that journal update before stopping it.
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !bob
                        .pending_acknowledgements(None)
                        .await
                        .unwrap()
                        .acknowledgements
                        .is_empty()
                    {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                })
                .await
                .unwrap();
                stop.send(true).unwrap();
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(65), async {
                tokio::join!(runner.run_with_wakeups(&bob, receiver, wakeups), exercise)
            })
            .await
            .unwrap();
            result.unwrap();
            assert!(
                bob.pending_acknowledgements(None)
                    .await
                    .unwrap()
                    .acknowledgements
                    .is_empty()
            );
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
            println!("zchat-native-wakeup-complete:v1");
        });
}
