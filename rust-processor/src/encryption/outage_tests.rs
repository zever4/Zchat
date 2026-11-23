//! Real socket faults are controlled by Go's dedicated TLS/PostgreSQL fixture.
//! No mocked transport or production test-only endpoint is introduced.
use super::{tests::local_store, *};
use crate::{
    encryption::{
        delivery_runner::{DeliveryIssue, DeliveryRunner, RunnerConfig},
        discovery::TrustedServer,
        provisioning::{DeviceProvisioning, Reconciliation},
        signal_state::{CryptoCoordinator, CryptoError, DurableAck},
    },
    proto::e2ee::{CiphertextType, EncryptedPayload, TextMessage, encrypted_payload},
};
use std::{
    io::{BufRead, BufReader, Write},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::watch;

struct Control(Arc<Mutex<BufReader<std::io::Stdin>>>);
impl Control {
    async fn phase(&self, phase: &'static str) {
        // libtest may have printed its test-name prefix without a newline.
        println!("\nzchat-outage-control:{phase}");
        std::io::stdout().flush().unwrap();
        let reader = self.0.clone();
        let response = tokio::task::spawn_blocking(move || {
            let mut line = String::new();
            assert!(reader.lock().unwrap().read_line(&mut line).unwrap() > 0);
            assert!(line.len() < 1024);
            line
        })
        .await
        .unwrap();
        assert_eq!(response.trim(), format!("ready:{phase}"));
    }
}
fn retryable(error: &RpcError) -> bool {
    matches!(
        error,
        RpcError::Transport
            | RpcError::LocalDeadline
            | RpcError::Status(
                Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::Unknown
            )
    )
}
fn fast_config() -> RunnerConfig {
    RunnerConfig {
        poll_interval: Duration::from_millis(50),
        retry_initial: Duration::from_millis(100),
        retry_max: Duration::from_millis(500),
        round_timeout: Duration::from_secs(5),
    }
}
enum Target<'a> {
    Receipt(&'a str),
    AckEmpty,
}
async fn drive_until(api: &mut NativeV1Client, worker: &CryptoCoordinator, target: Target<'_>) {
    let mut runner = DeliveryRunner::new(api, fast_config()).unwrap();
    let (stop, receiver) = watch::channel(false);
    let observed = async {
        loop {
            let ready = match &target {
                Target::Receipt(id) => worker
                    .outgoing_plan((*id).into())
                    .await
                    .map(|p| p.progress().receipt().is_some()),
                Target::AckEmpty => worker
                    .pending_acknowledgements(None)
                    .await
                    .map(|p| p.acknowledgements.is_empty()),
            };
            match ready {
                Ok(true) => {
                    stop.send(true).unwrap();
                    return;
                }
                Ok(false) | Err(CryptoError::Busy) => (),
                Err(error) => panic!("durable recovery observation failed: {error}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(30), async {
        tokio::join!(runner.run(worker, receiver), observed)
    })
    .await
    .expect("native runner failed to recover through its ORIGINAL tonic channel");
    result.unwrap();
}
async fn poll(api: &mut NativeV1Client, worker: &CryptoCoordinator) -> usize {
    let report = DeliveryRunner::new(api, fast_config())
        .unwrap()
        .poll_once(worker)
        .await
        .unwrap();
    assert!(
        report.issues.is_empty(),
        "unexpected real transport issue: {:?}",
        report.issues
    );
    report.received
}
async fn cancel_pending(
    control: &Control,
    api: &mut NativeV1Client,
    worker: &CryptoCoordinator,
    round_timeout: bool,
) {
    control.phase("arm-pending-stall").await;
    let config = if round_timeout {
        RunnerConfig {
            round_timeout: Duration::from_secs(2),
            retry_initial: Duration::from_secs(30),
            retry_max: Duration::from_secs(30),
            ..fast_config()
        }
    } else {
        RunnerConfig {
            round_timeout: Duration::from_secs(20),
            ..fast_config()
        }
    };
    let mut runner = DeliveryRunner::new(api, config).unwrap();
    let (stop, receiver) = watch::channel(false);
    let cancel = async {
        control.phase("wait-started").await;
        if round_timeout {
            control.phase("wait-cancelled").await;
        }
        stop.send(true).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(12), async {
        tokio::join!(runner.run(worker, receiver), cancel)
    })
    .await
    .unwrap();
    result.unwrap();
    if round_timeout {
        assert!(
            runner
                .last_report()
                .unwrap()
                .issues
                .iter()
                .any(|i| matches!(i.error, DeliveryIssue::Timeout))
        );
    } else {
        control.phase("wait-cancelled").await;
    }
}
fn text(chat: &str, own: &DeviceRef, peer: &DeviceRef, value: &str) -> EncryptedPayload {
    EncryptedPayload {
        format_version: 1,
        message_id: Uuid::new_v4().to_string(),
        conversation_id: chat.into(),
        sender: Some(own.clone()),
        recipient: Some(peer.clone()),
        created_at_unix_ms: 1_700_000_000_123,
        content: Some(encrypted_payload::Content::Text(TextMessage {
            text: value.into(),
        })),
    }
}

#[test]
#[ignore = "requires Go-controlled actual TLS/server outage and PostgreSQL fixture"]
fn go_outage_worker() {
    let mut reader = BufReader::new(std::io::stdin());
    let mut fields = Vec::new();
    for _ in 0..5 {
        let mut line = Zeroizing::new(String::new());
        assert!(reader.read_line(&mut line).unwrap() > 0);
        assert!(line.len() < 16384);
        while line.ends_with(['\r', '\n']) {
            line.pop();
        }
        fields.push(line);
    }
    let control = Control(Arc::new(Mutex::new(reader)));
    let ca = std::fs::read(fields[1].as_str()).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let root = tempfile::tempdir().unwrap();
            let origin = "https://outage-interop.example.test";
            let mut accounts = Vec::new();
            let mut stores = Vec::new();
            let mut devices = Vec::new();
            let mut scopes = Vec::new();
            for token in [&fields[2], &fields[3]] {
                let mut account = TrustedServer::new(origin, &fields[0], Some(&ca))
                    .unwrap()
                    .discover(origin, Zeroizing::new(token.to_string()))
                    .await
                    .unwrap();
                assert!(account.state().active_device.is_none());
                let scope = account.scope();
                let mut local =
                    DeviceProvisioning::new(local_store(root.path(), scope.clone(), true));
                assert_eq!(
                    account.resume_initial(&mut local).await.unwrap(),
                    Reconciliation::Ready
                );
                devices.push(local.ready_device().unwrap());
                scopes.push(scope);
                stores.push(local.into_store());
                accounts.push(account);
            }
            let mut alice_account = accounts.remove(0);
            let mut bob_account = accounts.remove(0);
            let alice_api = alice_account.device_client().unwrap();
            let bob_api = bob_account.device_client().unwrap();
            // These clients/channels/credentials survive every server restart below.
            let alice = CryptoCoordinator::start(stores.remove(0), devices[0].clone())
                .await
                .unwrap();
            let bob = CryptoCoordinator::start(stores.remove(0), devices[1].clone())
                .await
                .unwrap();
            let chat = alice_api
                .create_conversation(&alice, fields[4].parse().unwrap())
                .await
                .unwrap();
            bob_api
                .get_conversation(&bob, &chat.conversation_id)
                .await
                .unwrap();
            let a = alice
                .conversation_scope(chat.conversation_id.clone())
                .await
                .unwrap();
            let b = bob
                .conversation_scope(chat.conversation_id.clone())
                .await
                .unwrap();
            let acquisition = alice.prepare_acquisition(a.clone()).await.unwrap();
            let bundle = alice_api.acquire(&acquisition.request).await.unwrap();
            alice.bootstrap(a.clone(), bundle).await.unwrap();

            let first = text(
                &chat.conversation_id,
                &devices[0],
                &devices[1],
                "persisted before forced TLS close",
            );
            let first_envelope = alice.encrypt(a, first.clone()).await.unwrap();
            let first_plan = alice.outgoing_plan(first.message_id.clone()).await.unwrap();
            control.phase("arm-send-drop").await;
            match alice_api.send_outbox(&first_plan).await {
                Err(error) => assert!(retryable(&error), "unexpected drop result: {error}"),
                Ok(_) => panic!("fixture must close transport after COMMIT but before receipt"),
            }
            control.phase("wait-cancelled").await;
            // Stay genuinely offline for one request; no artificial RPC error response.
            match tokio::time::timeout(Duration::from_secs(12), alice_api.send_outbox(&first_plan))
                .await
                .unwrap()
            {
                Err(error) => assert!(retryable(&error)),
                Ok(_) => panic!("stopped TLS endpoint must not reply"),
            }
            assert!(
                alice
                    .outgoing_plan(first.message_id.clone())
                    .await
                    .unwrap()
                    .progress()
                    .receipt()
                    .is_none()
            );
            alice.shutdown().await.unwrap();
            let alice = CryptoCoordinator::start(
                local_store(root.path(), scopes[0].clone(), false),
                devices[0].clone(),
            )
            .await
            .unwrap();
            assert_eq!(
                alice
                    .outgoing_plan(first.message_id.clone())
                    .await
                    .unwrap()
                    .request(),
                first_plan.request()
            );
            let a = alice
                .conversation_scope(chat.conversation_id.clone())
                .await
                .unwrap();
            control.phase("restart").await;
            drive_until(alice_api, &alice, Target::Receipt(&first.message_id)).await;
            assert_eq!(
                alice
                    .outgoing_plan(first.message_id.clone())
                    .await
                    .unwrap()
                    .request()
                    .envelope
                    .as_ref(),
                Some(&first_envelope)
            );

            let pending = bob_api.pending().await.unwrap();
            assert_eq!(pending.messages.len(), 1);
            let (received, proof) = bob
                .receive_for_delivery(b.clone(), pending.messages[0].clone())
                .await
                .unwrap();
            assert_eq!(received, first);
            control.phase("arm-ack-drop").await;
            match bob_api.acknowledge(&proof).await {
                Err(error) => assert!(retryable(&error)),
                Ok(_) => panic!("fixture must drop ACK reply after server COMMIT"),
            }
            control.phase("wait-cancelled").await;
            bob.shutdown().await.unwrap();
            let bob = CryptoCoordinator::start(
                local_store(root.path(), scopes[1].clone(), false),
                devices[1].clone(),
            )
            .await
            .unwrap();
            let recovered: Vec<DurableAck> = bob
                .pending_acknowledgements(None)
                .await
                .unwrap()
                .acknowledgements;
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].request(), proof.request());
            let original_reply = bob
                .outbox_page(None)
                .await
                .unwrap()
                .messages
                .remove(0)
                .request()
                .clone();
            let reply_id = original_reply.envelope.as_ref().unwrap().message_id.clone();
            control.phase("restart").await;
            drive_until(bob_api, &bob, Target::AckEmpty).await;
            drive_until(bob_api, &bob, Target::Receipt(&reply_id)).await;
            assert_eq!(
                bob.outgoing_plan(reply_id).await.unwrap().request(),
                &original_reply
            );
            assert_eq!(poll(alice_api, &alice).await, 1); // confirmation received/ACKed, no loop
            assert_eq!(poll(bob_api, &bob).await, 0); // confirmation sender status is terminal

            // Real cancellation must emit RST_STREAM and cancel the Go handler context.
            cancel_pending(&control, alice_api, &alice, false).await;
            control.phase("arm-pending-stall").await;
            let started = Instant::now();
            let error = alice_api
                .pending()
                .await
                .expect_err("native10s RPC deadline must expire");
            assert_eq!(error, RpcError::LocalDeadline);
            assert!(
                started.elapsed() >= Duration::from_secs(9)
                    && started.elapsed() < Duration::from_secs(15)
            );
            println!("zchat-outage-native-deadline:{error:?}");
            control.phase("wait-cancelled").await;
            cancel_pending(&control, alice_api, &alice, true).await;

            let second = text(
                &chat.conversation_id,
                &devices[0],
                &devices[1],
                "same channel still usable after cancellation and deadlines",
            );
            let second_envelope = alice.encrypt(a.clone(), second.clone()).await.unwrap();
            assert_eq!(second_envelope.message_type, CiphertextType::Signal as i32);
            drive_until(alice_api, &alice, Target::Receipt(&second.message_id)).await;
            assert_eq!(poll(bob_api, &bob).await, 1);
            poll(alice_api, &alice).await;

            // Cancel a real Send after its COMMIT without dropping the whole channel.
            let third = text(
                &chat.conversation_id,
                &devices[0],
                &devices[1],
                "cancelled after remote commit; retry exact outbox",
            );
            let third_envelope = alice.encrypt(a, third.clone()).await.unwrap();
            control.phase("arm-send-stall").await;
            let mut runner = DeliveryRunner::new(
                &mut *alice_api,
                RunnerConfig {
                    round_timeout: Duration::from_secs(20),
                    ..fast_config()
                },
            )
            .unwrap();
            let (stop, receiver) = watch::channel(false);
            let cancel = async {
                control.phase("wait-started").await;
                stop.send(true).unwrap();
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(12), async {
                tokio::join!(runner.run(&alice, receiver), cancel)
            })
            .await
            .unwrap();
            result.unwrap();
            drop(runner);
            control.phase("wait-cancelled").await;
            let third_plan = alice.outgoing_plan(third.message_id.clone()).await.unwrap();
            assert!(third_plan.progress().receipt().is_none());
            assert_eq!(
                third_plan.request().envelope.as_ref(),
                Some(&third_envelope)
            );
            drive_until(alice_api, &alice, Target::Receipt(&third.message_id)).await;
            assert_eq!(poll(bob_api, &bob).await, 1);
            poll(alice_api, &alice).await;
            let b = bob.conversation_scope(chat.conversation_id).await.unwrap();
            assert_eq!(bob.decrypt(b, third_envelope).await.unwrap(), third);
            assert!(alice_api.pending().await.unwrap().messages.is_empty());
            assert!(bob_api.pending().await.unwrap().messages.is_empty());
            assert!(
                alice
                    .pending_acknowledgements(None)
                    .await
                    .unwrap()
                    .acknowledgements
                    .is_empty()
            );
            assert!(
                bob.pending_acknowledgements(None)
                    .await
                    .unwrap()
                    .acknowledgements
                    .is_empty()
            );
            assert!(alice.outbox_page(None).await.unwrap().messages.is_empty());
            assert!(bob.outbox_page(None).await.unwrap().messages.is_empty());
            for id in [&first.message_id, &second.message_id, &third.message_id] {
                let saved = alice.outgoing_plan(id.clone()).await.unwrap();
                assert!(saved.progress().receipt().is_some());
                assert_eq!(
                    saved.progress().status().unwrap().state,
                    crate::proto::e2ee::MessageState::RecipientStored as i32
                );
            }
            assert_eq!(
                bob.outgoing_plan(original_reply.envelope.as_ref().unwrap().message_id.clone())
                    .await
                    .unwrap()
                    .request(),
                &original_reply
            );
            alice.shutdown().await.unwrap();
            bob.shutdown().await.unwrap();
            control.phase("done").await;
            println!("zchat-native-outage-complete:v1");
        });
}
