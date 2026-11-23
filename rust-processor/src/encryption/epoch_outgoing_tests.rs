use super::super::super::tests::store;
use super::super::tests::{Fixture, fixture, index, initialize, records, rewrite_frozen, wrapped};
use super::*;
use crate::proto::e2ee::{ConversationCloseReason, TextMessage};
use rusqlite::Connection;
use uuid::Uuid;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
pub(in crate::encryption::signal_state::epoch_state) async fn prepared() -> (Fixture, EpochScope) {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let old = &f.bundle;
    let b = v2::AcquiredBundle {
        protocol_version: 2,
        conversation_id: p.request().conversation_id.clone(),
        acquisition_id: p.request().acquisition_id.clone(),
        device: old.device.clone(),
        signed_pre_key: old.signed_pre_key.clone(),
        one_time_pre_key: old.one_time_pre_key.clone(),
        kyber_pre_key: old.kyber_pre_key.clone(),
        not_after_unix_ms: old.not_after_unix_ms,
        crypto_epoch: p.request().crypto_epoch,
    };
    f.alice
        .bootstrap_epoch(EpochObservation::test_observation(s.scope.clone(), p, b))
        .await
        .unwrap();
    (f, s)
}
pub(super) fn payload(s: &EpochScope, text: &str) -> v2::EncryptedPayload {
    v2::EncryptedPayload {
        format_version: 2,
        message_id: Uuid::new_v4().to_string(),
        conversation_id: s.conversation.clone(),
        sender: Some(s.own.clone()),
        recipient: Some(s.peer.clone()),
        created_at_unix_ms: 1_700_000_000_000,
        content: Some(v2::encrypted_payload::Content::Text(TextMessage {
            text: text.into(),
        })),
        crypto_epoch: Some(s.epoch),
    }
}
pub(super) fn receipt(p: &EpochOutgoingPlan) -> v2::SendReceipt {
    let e = p.request.envelope.as_ref().unwrap();
    v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    }
}
fn observed(
    p: &EpochOutgoingPlan,
    r: v2::SendReceipt,
) -> EpochObservation<EpochOutgoingPlan, v2::SendReceipt> {
    EpochObservation::test_observation(p.scope.scope.clone(), p.clone(), r)
}
pub(in crate::encryption::signal_state::epoch_state) fn db(root: &std::path::Path) -> Connection {
    Connection::open(
        StoreScope::new("https://chat.example.test", A)
            .unwrap()
            .database_path(root),
    )
    .unwrap()
}

#[tokio::test]
async fn outgoing_commit_exact_retry_and_reopen_keep_ciphertext_and_private_keys() {
    let (f, s) = prepared().await;
    let before = records(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    let text = payload(&s, "outbox-v2-sensitive-plaintext-marker");
    let p = f
        .alice
        .encrypt_epoch(s.clone(), text.clone())
        .await
        .unwrap();
    assert!(p.receipt().is_none());
    assert_eq!(p.request.context.as_ref().unwrap().protocol_version, 2);
    assert_eq!(p.request.envelope.as_ref().unwrap().crypto_epoch, Some(0));
    let after = records(f.root.path(), A);
    for r in before.into_iter().filter(|r| r.0 <= 5) {
        assert!(after.contains(&r));
    }
    assert_eq!(wrapped(f.root.path(), A), master);
    assert!(
        !std::fs::read(
            StoreScope::new("https://chat.example.test", A)
                .unwrap()
                .database_path(f.root.path())
        )
        .unwrap()
        .windows(b"outbox-v2-sensitive-plaintext-marker".len())
        .any(|b| b == b"outbox-v2-sensitive-plaintext-marker")
    );
    assert_eq!(
        f.alice
            .encrypt_epoch(s.clone(), text.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    let page = f.alice.epoch_outbox_page(None).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert!(page.next.is_none());
    assert!(f.alice.outbox_page(None).await.unwrap().messages.is_empty());
    assert_eq!(records(f.root.path(), A), after);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_outgoing_plan(text.message_id.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    assert_eq!(
        w.encrypt_epoch(s, text).await.unwrap().request(),
        p.request()
    );
    assert_eq!(records(f.root.path(), A), after);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn outgoing_failed_commit_exposes_no_plan_and_rolls_back_ratchet_history_guard() {
    let (f, s) = prepared().await;
    let p = payload(&s, "rollback all message state");
    let before = records(f.root.path(), A);
    let raw = db(f.root.path());
    raw.execute_batch("CREATE TABLE out_fault_parent(id INTEGER PRIMARY KEY);CREATE TABLE out_fault_child(id INTEGER REFERENCES out_fault_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER out_fault AFTER INSERT ON protected_records WHEN NEW.kind=11 BEGIN INSERT INTO out_fault_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.alice.encrypt_epoch(s.clone(), p.clone()).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert!(matches!(
        f.alice.epoch_outgoing_plan(p.message_id.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    raw.execute_batch("DROP TRIGGER out_fault").unwrap();
    drop(raw);
    f.alice.encrypt_epoch(s, p).await.unwrap();
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_is_bound_exact_id_epoch_digest_ttl_account_and_stays_pending() {
    let (f, s) = prepared().await;
    let p = f
        .alice
        .encrypt_epoch(s.clone(), payload(&s, "server ACK is not peer ACK"))
        .await
        .unwrap();
    let good = receipt(&p);
    let before = records(f.root.path(), A);
    let mut cases = vec![];
    let mut r = good.clone();
    r.message_id = Uuid::new_v4().to_string();
    cases.push(r);
    let mut r = good.clone();
    r.crypto_epoch = None;
    cases.push(r);
    let mut r = good.clone();
    r.crypto_epoch = Some(1);
    cases.push(r);
    let mut r = good.clone();
    r.envelope_digest[0] ^= 1;
    cases.push(r);
    let mut r = good.clone();
    r.expires_at_unix_ms += 1;
    cases.push(r);
    for r in cases {
        assert!(
            f.alice
                .accept_epoch_send_receipt(observed(&p, r))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
    }
    assert!(
        f.bob
            .accept_epoch_send_receipt(observed(&p, good.clone()))
            .await
            .is_err()
    );
    let foreign = StoreScope::new("https://other.example.test", A).unwrap();
    assert!(
        f.alice
            .accept_epoch_send_receipt(EpochObservation::test_observation(
                foreign,
                p.clone(),
                good.clone()
            ))
            .await
            .is_err()
    );
    f.alice
        .accept_epoch_send_receipt(observed(&p, good.clone()))
        .await
        .unwrap();
    let after = records(f.root.path(), A);
    f.alice
        .accept_epoch_send_receipt(observed(&p, good.clone()))
        .await
        .unwrap();
    assert_eq!(records(f.root.path(), A), after);
    assert_eq!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own)
        .await
        .unwrap();
    let again = w
        .epoch_outgoing_plan(good.message_id.clone())
        .await
        .unwrap();
    assert_eq!(again.request(), p.request());
    assert_eq!(again.receipt(), Some(&good));
    let mut changed = good.clone();
    changed.stored_at_unix_ms += 1;
    changed.expires_at_unix_ms += 1;
    assert!(matches!(
        w.accept_epoch_send_receipt(observed(&p, changed)).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), after);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_commit_failure_preserves_unknown_outcome_for_exact_retry() {
    let (f, s) = prepared().await;
    let p = f
        .alice
        .encrypt_epoch(s.clone(), payload(&s, "lost local receipt"))
        .await
        .unwrap();
    let r = receipt(&p);
    let before = records(f.root.path(), A);
    let raw = db(f.root.path());
    let id = record_id(&s, &r.message_id).unwrap();
    raw.execute_batch(&format!("CREATE TABLE receipt_parent(id INTEGER PRIMARY KEY);CREATE TABLE receipt_child(id INTEGER REFERENCES receipt_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER receipt_fault AFTER UPDATE ON protected_records WHEN NEW.record_id='delivery-out:{id}' BEGIN INSERT INTO receipt_child VALUES(1);END;")).unwrap();
    assert!(matches!(
        f.alice
            .accept_epoch_send_receipt(observed(&p, r.clone()))
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert!(
        f.alice
            .epoch_outgoing_plan(r.message_id.clone())
            .await
            .unwrap()
            .receipt()
            .is_none()
    );
    raw.execute_batch("DROP TRIGGER receipt_fault").unwrap();
    drop(raw);
    f.alice
        .accept_epoch_send_receipt(observed(&p, r))
        .await
        .unwrap();
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn payload_binding_uuid_conflicts_and_other_account_do_not_advance_ratchet() {
    let (f, s) = prepared().await;
    let p = payload(&s, "binding");
    let before = records(f.root.path(), A);
    let mut cases = vec![];
    let mut bad = p.clone();
    bad.crypto_epoch = None;
    cases.push(bad);
    let mut bad = p.clone();
    bad.crypto_epoch = Some(1);
    cases.push(bad);
    let mut bad = p.clone();
    bad.format_version = 1;
    cases.push(bad);
    let mut bad = p.clone();
    bad.sender = p.recipient.clone();
    cases.push(bad);
    for bad in cases {
        assert!(f.alice.encrypt_epoch(s.clone(), bad).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
    }
    assert!(f.bob.encrypt_epoch(s.clone(), p.clone()).await.is_err());
    f.alice.encrypt_epoch(s.clone(), p.clone()).await.unwrap();
    let after = records(f.root.path(), A);
    let mut bad = p;
    bad.created_at_unix_ms += 1;
    assert!(matches!(
        f.alice.encrypt_epoch(s, bad).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), after);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_same_uuid_requests_return_one_committed_ciphertext() {
    let (f, s) = prepared().await;
    let p = payload(&s, "one transaction wins");
    let (a, b) = tokio::join!(
        f.alice.encrypt_epoch(s.clone(), p.clone()),
        f.alice.encrypt_epoch(s, p)
    );
    assert_eq!(a.unwrap().request(), b.unwrap().request());
    assert_eq!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn saved_message_replays_after_freeze_and_peer_closure_but_new_encryption_is_blocked() {
    for frozen in [false, true] {
        let (f, s) = prepared().await;
        let text = payload(&s, "keep exact unknown Send");
        let p = f
            .alice
            .encrypt_epoch(s.clone(), text.clone())
            .await
            .unwrap();
        f.alice.shutdown().await.unwrap();
        if frozen {
            rewrite_frozen(f.root.path(), &f.a, true);
        }
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        if !frozen {
            let mut closed = f.a.clone();
            closed.state = ConversationState::Closed as i32;
            closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            closed.closed_generation_id = s.peer.generation_id.clone();
            closed.closed_at_unix_ms = 1_700_000_000_200;
            index(&w, &closed).await;
        }
        let before = records(f.root.path(), A);
        assert_eq!(
            w.encrypt_epoch(s.clone(), text).await.unwrap().request(),
            p.request()
        );
        assert!(
            w.encrypt_epoch(s.clone(), payload(&s, "must be blocked"))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
        w.accept_epoch_send_receipt(observed(&p, receipt(&p)))
            .await
            .unwrap();
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn missing_link_history_outbox_journal_or_guard_is_not_permission_to_reencrypt() {
    for case in 0..5 {
        let (f, s) = prepared().await;
        let text = payload(&s, "no repair");
        let p = f
            .alice
            .encrypt_epoch(s.clone(), text.clone())
            .await
            .unwrap();
        f.alice.shutdown().await.unwrap();
        let raw = db(f.root.path());
        raw.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
        let id = record_id(&s, &text.message_id).unwrap();
        let (kind, key) = match case {
            0 => (10, id.clone()),
            1 => (11, id.clone()),
            2 => (13, format!("message-v2:{}", text.message_id)),
            3 => (13, format!("delivery-out:{id}")),
            _ => (
                13,
                format!("epoch-v2:{}:traffic:0000000000000000", s.conversation),
            ),
        };
        raw.execute(
            "DELETE FROM protected_records WHERE kind=?1 AND record_id=?2",
            rusqlite::params![kind, key],
        )
        .unwrap();
        drop(raw);
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        assert!(
            w.epoch_outgoing_plan(text.message_id.clone())
                .await
                .is_err()
        );
        assert!(w.encrypt_epoch(s, text).await.is_err());
        assert!(
            w.accept_epoch_send_receipt(observed(&p, receipt(&p)))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn bounded_v2_pages_and_explicit_index_seed_never_feed_prototype_decoder() {
    let (f, s) = prepared().await;
    let mut expected = std::collections::BTreeMap::new();
    for i in 0..65 {
        let p = f
            .alice
            .encrypt_epoch(s.clone(), payload(&s, &format!("bounded {i}")))
            .await
            .unwrap();
        expected.insert(
            p.request.envelope.as_ref().unwrap().message_id.clone(),
            p.request.clone(),
        );
    }
    let first = f.alice.epoch_outbox_page(None).await.unwrap();
    assert_eq!(first.messages.len(), 32);
    assert!(f.bob.epoch_outbox_page(first.next.clone()).await.is_err());
    f.alice.shutdown().await.unwrap();
    let raw = db(f.root.path());
    raw.execute_batch("PRAGMA foreign_keys=OFF;UPDATE protected_records SET recovery_ref=NULL;DELETE FROM protected_recovery;DELETE FROM protected_records WHERE record_id IN ('recovery-index:state:v1','recovery-index:anchor:v1');").unwrap();
    drop(raw);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own)
        .await
        .unwrap();
    assert!(matches!(
        w.epoch_outbox_page(None).await,
        Err(CryptoError::RecoveryIndexesNotReady)
    ));
    let mut rounds = 0;
    loop {
        let p = w.seed_recovery_indexes().await.unwrap();
        assert!(p.scanned <= 32);
        rounds += 1;
        if p.ready {
            break;
        }
        assert!(rounds < 20);
    }
    assert!(rounds > 5);
    let mut cursor = None;
    let mut found = std::collections::BTreeMap::new();
    loop {
        let page = w.epoch_outbox_page(cursor).await.unwrap();
        assert!(page.messages.len() <= 32);
        for p in page.messages {
            assert!(
                found
                    .insert(
                        p.request.envelope.as_ref().unwrap().message_id.clone(),
                        p.request
                    )
                    .is_none()
            );
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(found, expected);
    assert!(w.outbox_page(None).await.unwrap().messages.is_empty());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn committed_outbox_is_real_signal_ciphertext_decryptable_with_peer_keys() {
    let (f, s) = prepared().await;
    let peer = initialize(&f.bob).await;
    let p = payload(&s, "real v2 authenticated plaintext");
    let sent = f.alice.encrypt_epoch(s, p.clone()).await.unwrap();
    let e = sent.request.envelope.unwrap();
    f.bob.shutdown().await.unwrap();
    let root = f.root.path().to_owned();
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let mut db = store(&root, B, false);
        db.transaction_with::<_, CryptoError>(|tx| {
            let view = View::epoch(tx, &peer)?;
            let encrypted = CiphertextMessage::PreKeySignalMessage(PreKeySignalMessage::try_from(
                e.ciphertext.as_slice(),
            )?);
            let plain = Zeroizing::new(rt.block_on(message_decrypt(
                &encrypted,
                &address(&peer.peer),
                &address(&peer.own),
                &mut view.clone(),
                &mut view.clone(),
                &mut view.clone(),
                &view.clone(),
                &mut view.clone(),
                &mut rand::rng(),
            ))?);
            let actual = v2::EncryptedPayload::decode(plain.as_slice()).unwrap();
            data::validate_payload_binding(&actual, &e).unwrap();
            assert_eq!(actual, p);
            Ok(())
        })
        .unwrap();
    })
    .await
    .unwrap();
    // Crypto-only peer callback: no v2 inbox/history/ACK is written here.
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn uuid_cannot_be_reused_in_another_chat_before_consuming_its_ratchet() {
    let (f, s) = prepared().await;
    let text = payload(&s, "same UUID, different chat");
    f.alice.encrypt_epoch(s, text.clone()).await.unwrap();
    let mut c = f.a.clone();
    c.conversation_id = Uuid::new_v4().to_string();
    index(&f.alice, &c).await;
    let q = f
        .alice
        .prepare_initial_epoch(c.conversation_id.clone())
        .await
        .unwrap();
    let response = v2::ConversationEpochs {
        protocol_version: 2,
        conversation: Some(c),
        active_epoch: Some(0),
        epoch_revision: Some(0),
        epochs: vec![v2::EpochRecord {
            epoch: Some(0),
            revision: Some(0),
            created_at_unix_ms: 1_700_000_000_000,
            ..Default::default()
        }],
        next_after_epoch: None,
    };
    let s = f
        .alice
        .accept_initial_epoch(EpochObservation::test_observation(
            q.scope.clone(),
            q,
            response,
        ))
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    let mut collision = payload(&s, "would consume a second ratchet");
    collision.message_id = text.message_id;
    assert!(matches!(
        f.alice.encrypt_epoch(s, collision).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn outgoing_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_OUTGOING_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_OUTGOING_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let p = v2::EncryptedPayload::decode(
                std::fs::read(root.join("synthetic-text.pb"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let own = p.sender.clone().unwrap();
            let mut db = store(root, A, false);
            db.test_exit_on_record(
                RecordKey::new(
                    &own.generation_id,
                    RecordKind::Outbox,
                    &format!(
                        "v2:{}:{:016x}:{}",
                        p.conversation_id,
                        p.crypto_epoch.unwrap(),
                        p.message_id
                    ),
                )
                .unwrap(),
                after,
            );
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let s = w.epoch_scope(p.conversation_id.clone(), 0).await.unwrap();
            let _ = w.encrypt_epoch(s, p).await;
            panic!("outgoing crash hook did not fire");
        });
}
#[tokio::test]
async fn real_exit_before_and_after_outbox_commit_recovers_only_atomic_message() {
    for after in [false, true] {
        let (f, s) = prepared().await;
        let p = payload(&s, "synthetic crash fixture");
        std::fs::write(f.root.path().join("synthetic-text.pb"), p.encode_to_vec()).unwrap();
        f.alice.shutdown().await.unwrap();
        let before = records(f.root.path(), A);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::epoch_state::outgoing::tests::outgoing_crash_child",
                "--nocapture",
            ])
            .env("ZCHAT_OUTGOING_CRASH_PATH", f.root.path())
            .env("ZCHAT_OUTGOING_CRASH_AFTER", if after { "1" } else { "0" })
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        if after {
            let saved = w.epoch_outgoing_plan(p.message_id.clone()).await.unwrap();
            assert_eq!(
                w.encrypt_epoch(s, p).await.unwrap().request(),
                saved.request()
            );
        } else {
            assert_eq!(records(f.root.path(), A), before);
            assert!(matches!(
                w.epoch_outgoing_plan(p.message_id.clone()).await,
                Err(CryptoError::MessageUnavailable)
            ));
            assert_eq!(records(f.root.path(), A), before);
            w.encrypt_epoch(s, p).await.unwrap();
        }
        assert_eq!(w.epoch_outbox_page(None).await.unwrap().messages.len(), 1);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
