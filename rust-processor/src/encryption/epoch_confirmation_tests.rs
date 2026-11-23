use super::super::super::tests::store;
use super::super::{
    incoming::tests::{accepted, b_conversation, delivery, payload, prepared, raw, send},
    tests::{index, records, rewrite_frozen, wrapped},
};
use super::*;
use crate::proto::e2ee::{self as common, ConversationCloseReason};

const B: &str = "22222222-2222-4222-8222-222222222222";

fn wire(p: &EpochOutgoingPlan, sequence: i64) -> v2::Delivery {
    let e = p.request().envelope.clone().unwrap();
    v2::Delivery {
        sequence,
        envelope_digest: data::envelope_digest(&e).unwrap().to_vec(),
        envelope: Some(e),
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
    }
}
async fn one(w: &CryptoCoordinator) -> EpochOutgoingPlan {
    let mut p = w.epoch_outbox_page(None).await.unwrap();
    assert_eq!(p.messages.len(), 1);
    p.messages.pop().unwrap()
}

#[tokio::test]
async fn atomic_confirmation_confirms_signal_without_reply_loop_and_replays_after_reopen() {
    let (f, a, b) = prepared().await;
    let p = payload(&a, "v2 automatic confirmation");
    let d = send(&f.alice, &a, p.clone(), 1).await;
    assert_eq!(
        d.envelope.as_ref().unwrap().message_type,
        CiphertextType::PrekeySignal as i32
    );
    let master = wrapped(f.root.path(), B);
    let (_, ack) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let reply = one(&f.bob).await;
    let exact = reply.request().clone();
    assert_eq!(
        exact.envelope.as_ref().unwrap().message_type,
        CiphertextType::Signal as i32
    );
    let committed = records(f.root.path(), B);
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert_eq!(records(f.root.path(), B), committed);
    f.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert_eq!(one(&bob).await.request(), &exact);
    let reply_wire = wire(&one(&bob).await, 2);
    let (control, control_ack) = f
        .alice
        .receive_epoch(a.clone(), delivery(&a, &reply_wire))
        .await
        .unwrap();
    assert!(
        matches!(control.content, Some(v2::encrypted_payload::Content::SessionConfirmation(ref c))
        if c.in_reply_to_message_id == p.message_id && c.crypto_epoch == Some(0))
    );
    assert_eq!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    f.alice
        .accept_epoch_ack(accepted(&control_ack))
        .await
        .unwrap();
    let next = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "confirmed without Bob typing"))
        .await
        .unwrap();
    assert_eq!(
        next.request().envelope.as_ref().unwrap().message_type,
        CiphertextType::Signal as i32
    );
    bob.receive_epoch(b.clone(), delivery(&b, &wire(&next, 3)))
        .await
        .unwrap();
    assert_eq!(one(&bob).await.request(), &exact); // Ordinary Signal text adds no reply.
    bob.accept_epoch_ack(accepted(&ack)).await.unwrap();
    assert_eq!(wrapped(f.root.path(), B), master);
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn several_prekeys_each_have_one_exact_epoch_bound_reply_in_bounded_outbox() {
    let (f, a, b) = prepared().await;
    let mut incoming = Vec::new();
    for n in 0..3 {
        let d = send(&f.alice, &a, payload(&a, &format!("prekey {n}")), n + 1).await;
        assert_eq!(
            d.envelope.as_ref().unwrap().message_type,
            CiphertextType::PrekeySignal as i32
        );
        incoming.push(d);
    }
    for d in &incoming {
        f.bob
            .receive_epoch(b.clone(), delivery(&b, d))
            .await
            .unwrap();
    }
    let before = records(f.root.path(), B);
    let replies = f.bob.epoch_outbox_page(None).await.unwrap().messages;
    assert_eq!(replies.len(), 3);
    let mut targets = std::collections::BTreeSet::new();
    for (n, p) in replies.iter().enumerate() {
        let (plain, _) = f
            .alice
            .receive_epoch(a.clone(), delivery(&a, &wire(p, n as i64 + 4)))
            .await
            .unwrap();
        let Some(v2::encrypted_payload::Content::SessionConfirmation(c)) = plain.content else {
            panic!("not confirmation")
        };
        assert_eq!(c.crypto_epoch, Some(0));
        assert!(targets.insert(c.in_reply_to_message_id));
    }
    assert_eq!(
        targets,
        incoming
            .iter()
            .map(|d| d.envelope.as_ref().unwrap().message_id.clone())
            .collect()
    );
    for d in incoming.into_iter().rev() {
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
    }
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .len(),
        3
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn even_prekey_confirmation_never_generates_confirmation() {
    let (f, a, b) = prepared().await;
    let mut p = payload(&a, "unused");
    p.content = Some(v2::encrypted_payload::Content::SessionConfirmation(
        v2::SessionConfirmation {
            in_reply_to_message_id: uuid::Uuid::new_v4().to_string(),
            crypto_epoch: Some(0),
        },
    ));
    let d = send(&f.alice, &a, p.clone(), 1).await;
    assert_eq!(
        d.envelope.as_ref().unwrap().message_type,
        CiphertextType::PrekeySignal as i32
    );
    let (actual, _) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert_eq!(actual, p);
    assert!(
        f.bob
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn frozen_or_closed_receive_creates_no_reply_and_replay_never_appends_one() {
    for frozen in [true, false] {
        let (f, a, b) = prepared().await;
        let d = send(&f.alice, &a, payload(&a, "accepted old PreKey"), 1).await;
        if frozen {
            f.bob.shutdown().await.unwrap();
            let mut c = f.a.clone();
            c.own_device = Some(b.own.clone());
            rewrite_frozen(f.root.path(), &c, true);
        } else {
            let mut c = b_conversation(&f.a, &b);
            c.state = ConversationState::Closed as i32;
            c.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            c.closed_generation_id = b.peer.generation_id.clone();
            c.closed_at_unix_ms = 1_700_000_000_000;
            index(&f.bob, &c).await;
            f.bob.shutdown().await.unwrap();
        }
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        let (_, ack) = bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        assert!(
            bob.epoch_outbox_page(None)
                .await
                .unwrap()
                .messages
                .is_empty()
        );
        bob.shutdown().await.unwrap();
        if frozen {
            let mut c = f.a.clone();
            c.own_device = Some(b.own.clone());
            rewrite_frozen(f.root.path(), &c, false);
        }
        let before = records(f.root.path(), B);
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        bob.receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        assert_eq!(records(f.root.path(), B), before);
        assert!(
            bob.epoch_outbox_page(None)
                .await
                .unwrap()
                .messages
                .is_empty()
        );
        bob.accept_epoch_ack(accepted(&ack)).await.unwrap();
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn confirmation_commit_failure_rolls_back_receive_keys_ratchet_link_and_both_queues() {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "atomic receive plus reply"), 1).await;
    let before = records(f.root.path(), B);
    let db = raw(f.root.path());
    db.execute_batch("CREATE TABLE conf_parent(id INTEGER PRIMARY KEY);CREATE TABLE conf_child(id INTEGER REFERENCES conf_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER conf_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE 'session-confirmation:v2:%' BEGIN INSERT INTO conf_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.bob.receive_epoch(b.clone(), delivery(&b, &d)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    assert!(
        f.bob
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert!(
        f.bob
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    db.execute_batch("DROP TRIGGER conf_fault").unwrap();
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    one(&f.bob).await;
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn incomplete_or_mismatched_reply_cohort_blocks_history_replay_and_ack_without_repair() {
    for fault in 0..8 {
        let (f, a, b) = prepared().await;
        let d = send(&f.alice, &a, payload(&a, "required reply cohort"), 1).await;
        let (_, ack) = f
            .bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        let reply = one(&f.bob).await;
        let id = outgoing::record_id(&b, &reply.request().envelope.as_ref().unwrap().message_id)
            .unwrap();
        f.bob.shutdown().await.unwrap();
        let mut db = store(f.root.path(), B, false);
        db.transaction(|tx| {
            let link = key(&b, ack.message_id()).unwrap();
            match fault {
                0 => (),
                1 | 2 => {
                    let bytes = tx.get(&link)?.unwrap();
                    let mut l = Link::decode(bytes.as_slice()).unwrap();
                    if fault == 1 {
                        l.incoming_digest[0] ^= 1;
                    } else {
                        l.reply_digest[0] ^= 1;
                    }
                    tx.put(&link, &l.encode_to_vec())?;
                }
                3..=5 => (),
                6 => {
                    let key = RecordKey::new(
                        &b.own.generation_id,
                        RecordKind::Control,
                        &format!(
                            "delivery-in:{}",
                            outgoing::record_id(&b, ack.message_id()).unwrap()
                        ),
                    )
                    .unwrap();
                    let mut bytes = tx.get(&key)?.unwrap();
                    // Required optional bool tag4=true ends this fresh receive journal.
                    assert!(bytes.ends_with(&[0x20, 1, 0x28, 0]));
                    let size = bytes.len();
                    bytes.drain(size - 4..size - 2);
                    tx.put(&key, &bytes)?;
                }
                _ => (),
            }
            Ok(())
        })
        .unwrap();
        drop(db);
        if matches!(fault, 0 | 3..=5) {
            let (kind, deleted) = match fault {
                0 => (
                    RecordKind::Control,
                    format!(
                        "session-confirmation:{}",
                        outgoing::record_id(&b, ack.message_id()).unwrap()
                    ),
                ),
                3 => (RecordKind::History, id.clone()),
                4 => (RecordKind::Outbox, id.clone()),
                _ => (RecordKind::Control, format!("delivery-out:{id}")),
            };
            let corrupt = raw(f.root.path());
            // Intentionally break a required cohort, independent of platform
            // SQLite FK defaults. This is confined to this temporary fixture.
            corrupt.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            corrupt
                .execute(
                    "DELETE FROM protected_records WHERE kind=?1 AND record_id=?2",
                    rusqlite::params![kind as i64, deleted],
                )
                .unwrap();
        }
        if fault == 7 {
            raw(f.root.path())
                .execute(
                    "UPDATE protected_recovery SET pending=0 WHERE record_id=?1 AND kind=?2",
                    rusqlite::params![id, RecordKind::Outbox as i64],
                )
                .unwrap();
        }
        let before = records(f.root.path(), B);
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        assert!(
            bob.epoch_ack(ack.message_id().into()).await.is_err(),
            "fault{fault}"
        );
        assert!(
            bob.epoch_incoming_message(ack.message_id().into())
                .await
                .is_err(),
            "fault{fault}"
        );
        assert!(bob.epoch_ack_page(None).await.is_err(), "fault{fault}");
        assert!(
            bob.receive_epoch(b.clone(), delivery(&b, &d))
                .await
                .is_err(),
            "fault{fault}"
        );
        assert!(
            bob.accept_epoch_ack(accepted(&ack)).await.is_err(),
            "fault{fault}"
        );
        assert_eq!(records(f.root.path(), B), before);
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn terminal_reply_remains_required_for_ack_and_bounded_reseed_after_freeze() {
    let (f, a, b) = prepared().await;
    let d = send(
        &f.alice,
        &a,
        payload(&a, "retained terminal confirmation"),
        1,
    )
    .await;
    let (_, ack) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let reply = one(&f.bob).await;
    let wire = wire(&reply, 2);
    let e = wire.envelope.as_ref().unwrap();
    let receipt = v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: wire.envelope_digest.clone(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: wire.expires_at_unix_ms,
        crypto_epoch: Some(0),
    };
    let statuses = crate::encryption::native::EpochStatuses::test_observation(
        b.scope.clone(),
        vec![reply.clone()],
        v2::MessageStatuses {
            messages: vec![v2::MessageStatus {
                receipt: Some(receipt),
                state: common::MessageState::RecipientStored as i32,
                observed_at_unix_ms: 1_700_000_000_001,
            }],
        },
    );
    f.bob.accept_epoch_statuses(statuses).await.unwrap();
    assert!(
        f.bob
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    f.bob.shutdown().await.unwrap();
    let mut c = f.a.clone();
    c.own_device = Some(b.own.clone());
    rewrite_frozen(f.root.path(), &c, true);
    raw(f.root.path()).execute_batch("PRAGMA foreign_keys=OFF;UPDATE protected_records SET recovery_ref=NULL;DELETE FROM protected_recovery;DELETE FROM protected_records WHERE record_id IN ('recovery-index:state:v1','recovery-index:anchor:v1');").unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        bob.epoch_ack(ack.message_id().into()).await,
        Err(CryptoError::RecoveryIndexesNotReady)
    ));
    for _ in 0..12 {
        let page = bob.seed_recovery_indexes().await.unwrap();
        assert!(page.scanned <= 32);
        if page.ready {
            break;
        }
    }
    assert_eq!(
        bob.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    assert!(
        bob.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert_eq!(
        bob.epoch_outgoing_plan(e.message_id.clone())
            .await
            .unwrap()
            .request(),
        reply.request()
    );
    let before = records(f.root.path(), B);
    bob.receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert_eq!(records(f.root.path(), B), before);
    bob.accept_epoch_ack(accepted(&ack)).await.unwrap();
    assert!(
        bob.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[test]
fn confirmation_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_EPOCH_CONFIRM_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_EPOCH_CONFIRM_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let d = v2::Delivery::decode(
                std::fs::read(root.join("public-confirmation-delivery.pb"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let e = d.envelope.as_ref().unwrap();
            let own = e.recipient.clone().unwrap();
            let mut db = store(root, B, false);
            let id = format!(
                "session-confirmation:v2:{}:{:016x}:{}",
                e.conversation_id,
                e.crypto_epoch.unwrap(),
                e.message_id
            );
            db.test_exit_on_record(
                RecordKey::new(&own.generation_id, RecordKind::Control, &id).unwrap(),
                after,
            );
            let bob = CryptoCoordinator::start(db, own).await.unwrap();
            let s = bob.epoch_scope(e.conversation_id.clone(), 0).await.unwrap();
            let _ = bob.receive_epoch(s.clone(), delivery(&s, &d)).await;
            panic!("confirmation crash hook did not fire");
        });
}

#[tokio::test]
async fn real_process_exit_before_after_confirmation_commit_recovers_both_or_neither() {
    for after in [false, true] {
        let (f, a, b) = prepared().await;
        let d = send(
            &f.alice,
            &a,
            payload(&a, "atomic crash receive and confirmation"),
            1,
        )
        .await;
        std::fs::write(
            f.root.path().join("public-confirmation-delivery.pb"),
            d.encode_to_vec(),
        )
        .unwrap();
        f.bob.shutdown().await.unwrap();
        let before = records(f.root.path(), B);
        let result=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","encryption::signal_state::epoch_state::confirmation::tests::confirmation_crash_child","--nocapture"])
            .env("ZCHAT_EPOCH_CONFIRM_CRASH_PATH",f.root.path()).env("ZCHAT_EPOCH_CONFIRM_CRASH_AFTER",if after{"1"}else{"0"}).status().unwrap();
        assert_eq!(result.code(), Some(if after { 74 } else { 73 }));
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        if !after {
            assert_eq!(records(f.root.path(), B), before);
            assert!(
                bob.epoch_ack(d.envelope.as_ref().unwrap().message_id.clone())
                    .await
                    .is_err()
            );
            assert!(
                bob.epoch_outbox_page(None)
                    .await
                    .unwrap()
                    .messages
                    .is_empty()
            );
        }
        bob.receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        let exact = one(&bob).await.request().clone();
        let committed = records(f.root.path(), B);
        bob.receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        assert_eq!(records(f.root.path(), B), committed);
        assert_eq!(one(&bob).await.request(), &exact);
        assert_eq!(
            bob.epoch_ack_page(None)
                .await
                .unwrap()
                .acknowledgements
                .len(),
            1
        );
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}
