use super::super::super::super::tests::store;
use super::super::super::tests::{index, records, rewrite_frozen};
use super::super::tests::{db, payload, prepared, receipt};
use super::*;
use crate::proto::e2ee::ConversationCloseReason;
const A: &str = "11111111-1111-4111-8111-111111111111";

fn fact(p: &EpochOutgoingPlan, state: MessageState) -> v2::MessageStatus {
    let r = receipt(p);
    let observed = match state {
        MessageState::Pending => r.stored_at_unix_ms,
        MessageState::Expired => r.expires_at_unix_ms + 1,
        _ => r.expires_at_unix_ms + 2,
    };
    v2::MessageStatus {
        receipt: Some(r),
        state: state as i32,
        observed_at_unix_ms: observed,
    }
}
fn observed(plans: Vec<EpochOutgoingPlan>, messages: Vec<v2::MessageStatus>) -> EpochStatuses {
    EpochObservation::test_observation(
        plans[0].scope.scope.clone(),
        plans,
        v2::MessageStatuses { messages },
    )
}
async fn apply(
    w: &CryptoCoordinator,
    p: &EpochOutgoingPlan,
    s: v2::MessageStatus,
) -> EpochOutgoingPlan {
    w.accept_epoch_statuses(observed(vec![p.clone()], vec![s]))
        .await
        .unwrap()
        .remove(0)
}
#[tokio::test]
async fn pending_expired_late_stored_keep_removal_and_exact_ciphertext_after_reopen() {
    let (f, s) = prepared().await;
    let text = payload(&s, "status facts");
    let p = f
        .alice
        .encrypt_epoch(s.clone(), text.clone())
        .await
        .unwrap();
    let original = records(f.root.path(), A);
    assert!(p.needs_send() && p.needs_status_poll());
    let pending = fact(&p, MessageState::Pending);
    let q = apply(&f.alice, &p, pending.clone()).await;
    assert!(!q.needs_send() && q.needs_status_poll());
    let expired = fact(&p, MessageState::Expired);
    let q = apply(&f.alice, &p, expired.clone()).await;
    assert_eq!(q.status(), Some(&expired));
    assert_eq!(q.removal(), Some(&expired));
    assert!(q.needs_status_poll());
    assert!(!q.needs_send());
    let before = records(f.root.path(), A);
    apply(&f.alice, &p, pending.clone()).await;
    assert_eq!(records(f.root.path(), A), before);
    let ack = fact(&p, MessageState::RecipientStored);
    let q = apply(&f.alice, &p, ack.clone()).await;
    assert_eq!(q.removal(), Some(&expired));
    assert_eq!(q.status(), Some(&ack));
    assert!(!q.needs_status_poll());
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let after = records(f.root.path(), A);
    for old in original.into_iter().filter(|r| r.0 < 13) {
        assert!(after.contains(&old));
    }
    apply(&f.alice, &p, pending).await;
    apply(&f.alice, &p, expired.clone()).await;
    apply(&f.alice, &p, ack.clone()).await;
    assert_eq!(records(f.root.path(), A), after);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    let q = w
        .epoch_outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    assert_eq!(q.request(), p.request());
    assert_eq!(q.status(), Some(&ack));
    assert_eq!(q.removal(), Some(&expired));
    assert_eq!(
        w.encrypt_epoch(s, text).await.unwrap().request(),
        p.request()
    );
    assert_eq!(records(f.root.path(), A), after);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn out_of_order_ack_then_removal_preserves_both_facts_and_ack_precedence() {
    for removed in [MessageState::Expired, MessageState::RecipientReplaced] {
        let (f, s) = prepared().await;
        let p = f
            .alice
            .encrypt_epoch(s.clone(), payload(&s, "out of order"))
            .await
            .unwrap();
        let ack = fact(&p, MessageState::RecipientStored);
        apply(&f.alice, &p, ack.clone()).await;
        let removal = fact(&p, removed);
        let q = apply(&f.alice, &p, removal.clone()).await;
        assert_eq!(q.status(), Some(&ack));
        assert_eq!(q.removal(), Some(&removal));
        assert!(!q.needs_status_poll());
        let before = records(f.root.path(), A);
        let mut shifted = ack.clone();
        let receipt = shifted.receipt.as_mut().unwrap();
        receipt.stored_at_unix_ms += 1;
        receipt.expires_at_unix_ms += 1;
        assert!(matches!(
            f.alice
                .accept_epoch_statuses(observed(vec![p.clone()], vec![shifted]))
                .await,
            Err(CryptoError::Conflict)
        ));
        apply(&f.alice, &p, fact(&p, MessageState::Pending)).await;
        assert_eq!(records(f.root.path(), A), before);
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn rejected_and_replaced_stop_pending_but_expired_can_observe_late_ack() {
    let (f, s) = prepared().await;
    for state in [
        MessageState::RecipientRejected,
        MessageState::RecipientReplaced,
        MessageState::Expired,
    ] {
        let p = f
            .alice
            .encrypt_epoch(s.clone(), payload(&s, "classification"))
            .await
            .unwrap();
        let q = apply(&f.alice, &p, fact(&p, state)).await;
        assert_eq!(q.needs_status_poll(), state == MessageState::Expired);
        assert!(!q.needs_send());
        if state == MessageState::RecipientReplaced {
            let q = apply(&f.alice, &p, fact(&p, MessageState::RecipientStored)).await;
            assert_eq!(q.removal().unwrap().state, state as i32);
        }
    }
    let page = f.alice.epoch_outbox_page(None).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(
        page.messages[0].status().unwrap().state,
        MessageState::Expired as i32
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn contradictory_ack_removal_or_fact_timestamp_never_overwrite_committed_state() {
    for (first, next) in [
        (
            MessageState::RecipientStored,
            MessageState::RecipientRejected,
        ),
        (MessageState::Expired, MessageState::RecipientReplaced),
        (MessageState::RecipientReplaced, MessageState::Expired),
    ] {
        let (f, s) = prepared().await;
        let p = f
            .alice
            .encrypt_epoch(s.clone(), payload(&s, "conflicting fact"))
            .await
            .unwrap();
        let first = fact(&p, first);
        apply(&f.alice, &p, first.clone()).await;
        let before = records(f.root.path(), A);
        assert!(matches!(
            f.alice
                .accept_epoch_statuses(observed(vec![p.clone()], vec![fact(&p, next)]))
                .await,
            Err(CryptoError::Conflict)
        ));
        let mut changed = first.clone();
        changed.observed_at_unix_ms += 1;
        assert!(
            f.alice
                .accept_epoch_statuses(observed(vec![p], vec![changed]))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn status_binding_batch_order_origin_and_account_are_checked_before_updates() {
    let (f, s) = prepared().await;
    let p = f
        .alice
        .encrypt_epoch(s.clone(), payload(&s, "binding"))
        .await
        .unwrap();
    let q = f
        .alice
        .encrypt_epoch(s.clone(), payload(&s, "second"))
        .await
        .unwrap();
    let valid = fact(&p, MessageState::Pending);
    let before = records(f.root.path(), A);
    let mut bads = vec![];
    let mut bad = valid.clone();
    bad.receipt.as_mut().unwrap().crypto_epoch = None;
    bads.push(bad);
    let mut bad = valid.clone();
    bad.receipt.as_mut().unwrap().crypto_epoch = Some(1);
    bads.push(bad);
    let mut bad = valid.clone();
    bad.receipt.as_mut().unwrap().envelope_digest[0] ^= 1;
    bads.push(bad);
    let mut bad = valid.clone();
    bad.receipt.as_mut().unwrap().expires_at_unix_ms += 1;
    bads.push(bad);
    let mut bad = valid.clone();
    bad.state = MessageState::Unspecified as i32;
    bads.push(bad);
    for bad in bads {
        assert!(
            f.alice
                .accept_epoch_statuses(observed(vec![p.clone()], vec![bad]))
                .await
                .is_err()
        );
    }
    assert!(
        f.alice
            .accept_epoch_statuses(observed(
                vec![p.clone(), q.clone()],
                vec![fact(&q, MessageState::Pending), valid.clone()]
            ))
            .await
            .is_err()
    );
    assert!(
        f.bob
            .accept_epoch_statuses(observed(vec![p.clone()], vec![valid.clone()]))
            .await
            .is_err()
    );
    let foreign = StoreScope::new("https://other.example.test", A).unwrap();
    assert!(
        f.alice
            .accept_epoch_statuses(EpochObservation::test_observation(
                foreign,
                vec![p.clone()],
                v2::MessageStatuses {
                    messages: vec![valid.clone()]
                }
            ))
            .await
            .is_err()
    );
    assert!(
        f.alice
            .accept_epoch_statuses(observed(vec![p.clone(); 33], vec![valid.clone(); 33]))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn batch_conflict_or_deferred_commit_failure_rolls_back_all_statuses_and_projection() {
    let (f, s) = prepared().await;
    let p = f
        .alice
        .encrypt_epoch(s.clone(), payload(&s, "batch first"))
        .await
        .unwrap();
    let q = f
        .alice
        .encrypt_epoch(s.clone(), payload(&s, "batch second"))
        .await
        .unwrap();
    apply(&f.alice, &q, fact(&q, MessageState::RecipientStored)).await;
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice
            .accept_epoch_statuses(observed(
                vec![p.clone(), q.clone()],
                vec![
                    fact(&p, MessageState::RecipientStored),
                    fact(&q, MessageState::RecipientRejected)
                ]
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), before);
    let raw = db(f.root.path());
    raw.execute_batch("CREATE TABLE status_parent(id INTEGER PRIMARY KEY);CREATE TABLE status_child(id INTEGER REFERENCES status_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER status_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE 'delivery-progress:%' BEGIN INSERT INTO status_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.alice
            .accept_epoch_statuses(observed(
                vec![p.clone()],
                vec![fact(&p, MessageState::RecipientStored)]
            ))
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert_eq!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    raw.execute_batch("DROP TRIGGER status_fault").unwrap();
    drop(raw);
    apply(&f.alice, &p, fact(&p, MessageState::RecipientStored)).await;
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_anchor_or_old_authenticated_journal_replay_cannot_requeue_terminal_message() {
    for old_journal in [false, true] {
        let (f, s) = prepared().await;
        let p = f
            .alice
            .encrypt_epoch(s.clone(), payload(&s, "anchor"))
            .await
            .unwrap();
        let id = record_id(&s, &p.request.envelope.as_ref().unwrap().message_id).unwrap();
        let raw = db(f.root.path());
        let old: Vec<u8> = raw
            .query_row(
                "SELECT payload FROM protected_records WHERE record_id=?1",
                [format!("delivery-out:{id}")],
                |r| r.get(0),
            )
            .unwrap();
        apply(&f.alice, &p, fact(&p, MessageState::RecipientStored)).await;
        f.alice.shutdown().await.unwrap();
        if old_journal {
            raw.execute(
                "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
                rusqlite::params![old, format!("delivery-out:{id}")],
            )
            .unwrap();
        } else {
            raw.execute(
                "DELETE FROM protected_records WHERE record_id=?1",
                [format!("delivery-progress:{id}")],
            )
            .unwrap();
        }
        drop(raw);
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        assert!(
            w.epoch_outgoing_plan(p.request.envelope.as_ref().unwrap().message_id.clone())
                .await
                .is_err()
        );
        assert!(
            w.accept_epoch_statuses(observed(
                vec![p.clone()],
                vec![fact(&p, MessageState::RecipientStored)]
            ))
            .await
            .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn status_refresh_remains_available_after_send_freeze_or_peer_closure() {
    for frozen in [true, false] {
        let (f, s) = prepared().await;
        let p = f
            .alice
            .encrypt_epoch(s.clone(), payload(&s, "status for closed chat"))
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
            let mut c = f.a.clone();
            c.state = ConversationState::Closed as i32;
            c.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            c.closed_generation_id = s.peer.generation_id.clone();
            c.closed_at_unix_ms = 1_700_000_000_200;
            index(&w, &c).await;
        }
        apply(&w, &p, fact(&p, MessageState::RecipientStored)).await;
        assert!(w.epoch_outbox_page(None).await.unwrap().messages.is_empty());
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn bounded_recovery_reseed_preserves_terminal_expired_and_unknown_classification() {
    let (f, s) = prepared().await;
    let mut plans = Vec::new();
    for i in 0..65 {
        plans.push(
            f.alice
                .encrypt_epoch(s.clone(), payload(&s, &format!("status page {i}")))
                .await
                .unwrap(),
        );
    }
    plans.sort_by(|a, b| {
        a.request
            .envelope
            .as_ref()
            .unwrap()
            .message_id
            .cmp(&b.request.envelope.as_ref().unwrap().message_id)
    });
    let cursor = f.alice.epoch_outbox_page(None).await.unwrap().next.unwrap();
    let facts = plans[..32]
        .iter()
        .map(|p| fact(p, MessageState::RecipientStored))
        .collect();
    f.alice
        .accept_epoch_statuses(observed(plans[..32].to_vec(), facts))
        .await
        .unwrap();
    let facts = plans[32..50]
        .iter()
        .enumerate()
        .map(|(i, p)| {
            fact(
                p,
                if i < 8 {
                    MessageState::RecipientReplaced
                } else {
                    MessageState::Expired
                },
            )
        })
        .collect();
    f.alice
        .accept_epoch_statuses(observed(plans[32..50].to_vec(), facts))
        .await
        .unwrap();
    let page = f.alice.epoch_outbox_page(Some(cursor)).await.unwrap();
    assert_eq!(page.messages.len(), 25);
    assert_eq!(page.messages.iter().filter(|p| p.needs_send()).count(), 15);
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
    let page = w.epoch_outbox_page(None).await.unwrap();
    assert_eq!(page.messages.len(), 25);
    assert_eq!(page.messages.iter().filter(|p| p.needs_send()).count(), 15);
    for p in &plans[..40] {
        assert!(
            !w.epoch_outgoing_plan(p.request.envelope.as_ref().unwrap().message_id.clone())
                .await
                .unwrap()
                .needs_status_poll()
        );
    }
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn status_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_STATUS_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_STATUS_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let s = v2::MessageStatus::decode(
                std::fs::read(root.join("public-status.pb"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let uuid = s.receipt.as_ref().unwrap().message_id.clone();
            let own = DeviceRef {
                user_id: A.into(),
                device_id: 1,
                generation_id: "33333333-3333-4333-8333-333333333333".into(),
            };
            let mut db = store(root, A, false);
            let id = format!("v2:66666666-6666-4666-8666-666666666666:0000000000000000:{uuid}");
            db.test_exit_on_record(anchor_key(&own, &id).unwrap(), after);
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let p = w.epoch_outgoing_plan(uuid).await.unwrap();
            let _ = w.accept_epoch_statuses(observed(vec![p], vec![s])).await;
            panic!("status crash hook did not fire");
        });
}
#[tokio::test]
async fn real_process_exit_before_after_status_commit_recovers_pending_or_terminal() {
    for after in [false, true] {
        let (f, s) = prepared().await;
        let p = f
            .alice
            .encrypt_epoch(s.clone(), payload(&s, "synthetic status crash"))
            .await
            .unwrap();
        let status = fact(&p, MessageState::RecipientStored);
        std::fs::write(
            f.root.path().join("public-status.pb"),
            status.encode_to_vec(),
        )
        .unwrap();
        f.alice.shutdown().await.unwrap();
        let before = records(f.root.path(), A);
        let result=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","encryption::signal_state::epoch_state::outgoing::status::tests::status_crash_child","--nocapture"]).env("ZCHAT_STATUS_CRASH_PATH",f.root.path()).env("ZCHAT_STATUS_CRASH_AFTER",if after{"1"}else{"0"}).status().unwrap();
        assert_eq!(result.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own)
            .await
            .unwrap();
        let q = w
            .epoch_outgoing_plan(p.request.envelope.as_ref().unwrap().message_id.clone())
            .await
            .unwrap();
        assert_eq!(q.needs_send(), !after);
        assert_eq!(q.needs_status_poll(), !after);
        if !after {
            assert_eq!(records(f.root.path(), A), before);
        }
        apply(&w, &p, status.clone()).await;
        let saved = records(f.root.path(), A);
        apply(&w, &p, status).await;
        assert_eq!(records(f.root.path(), A), saved);
        assert!(w.epoch_outbox_page(None).await.unwrap().messages.is_empty());
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
