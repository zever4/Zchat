//! Actual owned runtime against the production Go registry/v2/TLS/PG graph.
use super::*;
use crate::{
    encryption::{
        account_owner::{AccountClient, AccountConfig, AccountOwner, AccountPhase, ReceivedText},
        delivery_runner::RunnerConfig,
        discovery::TrustedServer,
        prekey_maintenance::MaintenanceConfig,
        provisioning::{DeviceProvisioning, Reconciliation},
        signal_state::{
            CryptoCoordinator, EpochRecoveryDecision, EpochScope, ViewPage, ViewRequest,
        },
    },
    proto::{e2ee as common, e2ee_v2 as v2},
};
use std::io::Read;

pub(super) fn config() -> AccountConfig {
    AccountConfig {
        delivery: RunnerConfig {
            poll_interval: Duration::from_millis(30),
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_millis(500),
            round_timeout: Duration::from_secs(5),
        },
        maintenance: MaintenanceConfig {
            step_timeout: Duration::from_secs(5),
            ..Default::default()
        },
    }
}
pub(super) async fn ready(c: &AccountClient) {
    let mut changes = c.subscribe();
    tokio::time::timeout(Duration::from_secs(15), async {
        while c.status().phase != AccountPhase::Running {
            changes.changed().await.expect("owner failed to initialize");
        }
    })
    .await
    .unwrap();
}
async fn received(c: &AccountClient, id: &str) -> ReceivedText {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert_eq!(c.status().phase, AccountPhase::Running);
            if let Ok(Some(text)) = c.received_text(id).await {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap()
}
pub(super) async fn stored(c: &AccountClient, id: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let p = c.outgoing(id).await.unwrap();
            if p.stored_on_server && p.status == Some(common::MessageState::RecipientStored) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}
pub(super) async fn epoch(c: &AccountClient, chat: &str, expected: u64) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if c.conversation(chat).await.unwrap().unwrap().crypto_epoch == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}
pub(super) async fn settled(c: &AccountClient, require_bootstrap: bool) {
    let first = c.status().delivery_rounds;
    let mut changes = c.subscribe();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let s = c.status();
            if s.delivery_rounds > first
                && (!require_bootstrap || s.bootstrap_completions > 0)
                && s.delivery_health
                    == Some(crate::encryption::account_owner::DeliveryHealth::Healthy)
            {
                return;
            }
            changes
                .changed()
                .await
                .expect("owner stopped before completing recovery");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("owner did not settle: {:?}", c.status()));
}
fn payload(s: &EpochScope, text: &str) -> v2::EncryptedPayload {
    v2::EncryptedPayload {
        format_version: 2,
        message_id: Uuid::new_v4().to_string(),
        conversation_id: s.conversation_id().into(),
        sender: Some(s.own_device().clone()),
        recipient: Some(s.peer_device().clone()),
        created_at_unix_ms: 1_700_000_000_002,
        content: Some(v2::encrypted_payload::Content::Text(common::TextMessage {
            text: text.into(),
        })),
        crypto_epoch: Some(s.crypto_epoch()),
    }
}

#[test]
#[ignore = "requires the Go owned-account TLS/PostgreSQL fixture"]
fn go_account_owner_worker() {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(24576)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input.lines().collect();
    assert_eq!(fields.len(), 5);
    let ca = std::fs::read(fields[1]).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(Box::pin(run(&fields, &ca)));
    println!("zchat-native-account-owner-complete:v2");
}
async fn run(fields: &[&str], ca: &[u8]) {
    let root = tempfile::tempdir().unwrap();
    let origin = "https://account-owner.test";
    let mut accounts = Vec::new();
    let mut locals = Vec::new();
    let mut scopes = Vec::new();
    let mut devices = Vec::new();
    for token in [fields[2], fields[3]] {
        let mut a = TrustedServer::new(origin, fields[0], Some(ca))
            .unwrap()
            .discover(origin, Zeroizing::new(token.into()))
            .await
            .unwrap();
        let scope = a.scope();
        let mut local =
            DeviceProvisioning::new(tests::local_store(root.path(), scope.clone(), true));
        assert_eq!(
            a.resume_initial(&mut local).await.unwrap(),
            Reconciliation::Ready
        );
        devices.push(local.ready_device().unwrap());
        scopes.push(scope);
        locals.push(local);
        accounts.push(a);
    }
    assert_ne!(
        scopes[0].database_path(root.path()),
        scopes[1].database_path(root.path())
    );
    let wa = CryptoCoordinator::start(locals.remove(0).into_store(), devices[0].clone())
        .await
        .unwrap();
    let wb = CryptoCoordinator::start(locals.remove(0).into_store(), devices[1].clone())
        .await
        .unwrap();
    let mut aa = accounts.remove(0);
    let mut bb = accounts.remove(0);
    let chat = aa
        .device_client()
        .unwrap()
        .create_conversation(&wa, fields[4].parse().unwrap())
        .await
        .unwrap()
        .conversation_id;
    bb.device_client()
        .unwrap()
        .get_conversation(&wb, &chat)
        .await
        .unwrap();
    let mut av2 = aa.device_client().unwrap().epoch_transport().unwrap();
    let mut bv2 = bb.device_client().unwrap().epoch_transport().unwrap();
    let q = wa.prepare_initial_epoch(chat.clone()).await.unwrap();
    let a = wa
        .accept_initial_epoch(av2.initial_epoch_catalog(&q).await.unwrap())
        .await
        .unwrap();
    let q = wb.prepare_initial_epoch(chat.clone()).await.unwrap();
    wb.accept_initial_epoch(bv2.initial_epoch_catalog(&q).await.unwrap())
        .await
        .unwrap();
    let p = wa.prepare_epoch_acquisition(a.clone()).await.unwrap();
    let EpochAcquisitionOutcome::Bundle(b) = av2.acquire_prepared(&p).await.unwrap() else {
        panic!("initial acquisition expired")
    };
    wa.bootstrap_epoch(*b).await.unwrap();
    let text = payload(&a, "durable text delivered by owned runtime");
    let id = text.message_id.clone();
    wa.encrypt_epoch(a, text).await.unwrap();
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
    drop(av2);
    drop(bv2);
    let oa = AccountOwner::start(
        aa,
        DeviceProvisioning::new(tests::local_store(root.path(), scopes[0].clone(), false)),
        config(),
    )
    .await
    .unwrap();
    let ob = AccountOwner::start(
        bb,
        DeviceProvisioning::new(tests::local_store(root.path(), scopes[1].clone(), false)),
        config(),
    )
    .await
    .unwrap();
    let ac = oa.client();
    let bc = ob.client();
    ready(&ac).await;
    ready(&bc).await;
    assert_eq!(
        received(&bc, &id).await.text,
        "durable text delivered by owned runtime"
    );
    stored(&ac, &id).await;
    let proposal = ac.propose_recovery(&chat).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let r = bc.recoveries(&chat).await.unwrap();
            if let Some(r) = r.iter().find(|r| r.proposal_id == proposal) {
                assert!(!r.own_proposal && r.decision.is_none());
                if r.state == Some(common::RecoveryState::Pending) {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        bc.conversation(&chat).await.unwrap().unwrap().crypto_epoch,
        0
    );
    bc.decide_recovery(&chat, &proposal, EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    epoch(&ac, &chat, 1).await;
    epoch(&bc, &chat, 1).await;
    settled(&ac, true).await;
    oa.shutdown().await.unwrap();
    assert!(matches!(
        ac.conversation(&chat).await,
        Err(crate::encryption::account_owner::AccountError::Stopped)
    ));
    assert_eq!(bc.status().phase, AccountPhase::Running);
    assert_eq!(received(&bc, &id).await.crypto_epoch, 0);

    // The future text IPC will prepare this durable user outbox. Here it is
    // deliberately prepared while the owner is stopped, never a second worker.
    let w = CryptoCoordinator::start(
        tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let a = w.active_epoch_scope(chat.clone()).await.unwrap().unwrap();
    // Bootstrap may have committed after activation, or be resumed by the next
    // owner. Wait for applied before making a new user message in this fixture.
    let p = w.prepare_epoch_acquisition(a.clone()).await.unwrap();
    assert!(
        p.applied(),
        "approved proposer bootstrap did not commit before shutdown"
    );
    let text = payload(&a, "epoch1 after account logout/reopen");
    let next = text.message_id.clone();
    w.encrypt_epoch(a, text).await.unwrap();
    w.shutdown().await.unwrap();
    let mut aa = TrustedServer::new(origin, fields[0], Some(ca))
        .unwrap()
        .discover(origin, Zeroizing::new(fields[2].into()))
        .await
        .unwrap();
    let mut local =
        DeviceProvisioning::new(tests::local_store(root.path(), scopes[0].clone(), false));
    assert_eq!(
        aa.resume_initial(&mut local).await.unwrap(),
        Reconciliation::Ready
    );
    assert_eq!(local.ready_device().unwrap(), devices[0]);
    let oa = AccountOwner::start(aa, local, config()).await.unwrap();
    let fresh = oa.client();
    ready(&fresh).await;
    assert_eq!(
        received(&bc, &next).await.text,
        "epoch1 after account logout/reopen"
    );
    stored(&fresh, &next).await;
    // Local presentation reads retain both epochs and hide automatic replies.
    for client in [&fresh, &bc] {
        let ViewPage::History(rows, cursor) = client
            .read_views(ViewRequest::History(chat.clone(), None))
            .await
            .unwrap()
        else {
            panic!("wrong page")
        };
        assert!(cursor.is_none());
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter().map(|r| r.crypto_epoch).collect::<Vec<_>>(),
            vec![1, 0]
        );
        assert_eq!(rows[0].message_id, next);
        assert_eq!(rows[1].message_id, id);
    }
    // Recipient storage of the user text precedes the separately queued Signal
    // confirmation. Complete Bob's send round, then Alice's receive/ACK round;
    // shutdown itself deliberately never waits for peers or drains the server.
    settled(&bc, false).await;
    settled(&fresh, false).await;
    assert!(fresh.status().maintenance_steps > 0 && bc.status().maintenance_steps > 0);
    oa.shutdown().await.unwrap();
    ob.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(
        tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let a = w.active_epoch_scope(chat.clone()).await.unwrap().unwrap();
    let signal = w
        .encrypt_epoch(
            a.clone(),
            payload(&a, "Signal state retained after owner shutdown"),
        )
        .await
        .unwrap();
    assert_eq!(
        signal.request().envelope.as_ref().unwrap().message_type,
        common::CiphertextType::Signal as i32
    );
    w.shutdown().await.unwrap();
    // Both account store locks have been released, and old/new history coexist.
    let w = CryptoCoordinator::start(
        tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    assert!(w.epoch_incoming_message(id).await.is_ok());
    assert!(w.epoch_incoming_message(next).await.is_ok());
    w.shutdown().await.unwrap();
}
