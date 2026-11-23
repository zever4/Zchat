use super::*;
use crate::encryption::native::NativeEpochClient;
use crate::encryption::signal_state::{
    epoch_state::{
        attachment_tests::message,
        incoming::tests::prepared,
        outgoing::tests::{db, payload, receipt},
        tests::{index, records, rewrite_frozen, wrapped},
    },
    tests::store,
};
use crate::proto::e2ee::{ConversationCloseReason, MessageState};
const A: &str = "11111111-1111-4111-8111-111111111111";

async fn seed(w: &CryptoCoordinator) {
    loop {
        let p = w.seed_epoch_file_status_index().await.unwrap();
        assert!(p.scanned <= 32);
        if p.ready {
            break;
        }
    }
}
async fn accepted(w: &CryptoCoordinator, s: &EpochScope) -> EpochOutgoingPlan {
    let (p, _) = message(s);
    let p = w.encrypt_epoch(s.clone(), p).await.unwrap();
    w.accept_epoch_send_receipt(EpochObservation::test_observation(
        s.scope.clone(),
        p.clone(),
        receipt(&p),
    ))
    .await
    .unwrap();
    w.epoch_outgoing_plan(p.request.envelope.as_ref().unwrap().message_id.clone())
        .await
        .unwrap()
}
fn fact(
    p: &EpochOutgoingPlan,
    observed: i64,
    ack: Option<i64>,
    replaced: Option<i64>,
) -> v2::FileStatus {
    let r = p.receipt().unwrap();
    v2::FileStatus {
        format_version: 1,
        reference: Some(reference(p.request()).unwrap()),
        receipt: Some(r.clone()),
        observed_at_unix_ms: observed,
        expires_at_unix_ms: r.stored_at_unix_ms + contract::FILE_RETENTION_MS,
        acknowledged_at_unix_ms: ack,
        recipient_replaced_at_unix_ms: replaced,
    }
}
fn observed(plans: Vec<EpochOutgoingPlan>, files: Vec<v2::FileStatus>) -> EpochFileStatuses {
    EpochObservation::test_observation(
        plans[0].scope.scope.clone(),
        plans,
        v2::FileStatuses { files },
    )
}
async fn apply(
    w: &CryptoCoordinator,
    p: &EpochOutgoingPlan,
    f: v2::FileStatus,
) -> EpochFileProgress {
    // Exercise the real opaque native observation validator with genuine plans.
    let client =
        NativeEpochClient::test_metadata_transport(p.scope.scope.clone(), p.scope.own.clone());
    let b = client
        .test_file_status_observation(std::slice::from_ref(p), v2::FileStatuses { files: vec![f] })
        .unwrap();
    w.accept_epoch_file_statuses(b).await.unwrap().remove(0)
}
fn uuid(p: &EpochOutgoingPlan) -> String {
    p.request.envelope.as_ref().unwrap().message_id.clone()
}
fn immutable(rows: Vec<(i64, String, Vec<u8>)>) -> Vec<(i64, String, Vec<u8>)> {
    rows.into_iter()
        .filter(|r| !r.1.starts_with("file-progress"))
        .collect()
}
async fn descriptor_ack(w: &CryptoCoordinator, p: &EpochOutgoingPlan) {
    w.accept_epoch_statuses(EpochObservation::test_observation(
        p.scope.scope.clone(),
        vec![p.clone()],
        v2::MessageStatuses {
            messages: vec![v2::MessageStatus {
                receipt: Some(receipt(p)),
                state: MessageState::RecipientStored as i32,
                observed_at_unix_ms: receipt(p).stored_at_unix_ms + 1,
            }],
        },
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn file_status_expiry_late_ack_reopen_and_message_ack_have_independent_progress() {
    let (f, s, _) = prepared().await;
    let p = accepted(&f.alice, &s).await;
    assert!(matches!(
        f.alice.pending_epoch_file_statuses(None).await,
        Err(CryptoError::FileStatusIndexNotReady)
    ));
    seed(&f.alice).await;
    let q = f.alice.epoch_file_progress(uuid(&p)).await.unwrap();
    assert_eq!(q.observed_at_unix_ms(), None);
    assert!(!q.deadline_observed()); // Receipt from 2023 is NOT a fresh DB time.
    descriptor_ack(&f.alice, &p).await;
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert_eq!(
        f.alice
            .pending_epoch_file_statuses(None)
            .await
            .unwrap()
            .files
            .len(),
        1
    );
    let before = immutable(records(f.root.path(), A));
    let master = wrapped(f.root.path(), A);
    let stored = p.receipt().unwrap().stored_at_unix_ms;
    let deadline = stored + contract::FILE_RETENTION_MS;
    let expired = fact(&p, deadline + 10, None, None);
    let q = apply(&f.alice, &p, expired.clone()).await;
    assert!(q.deadline_observed() && q.needs_poll());
    let ack = fact(&p, deadline + 30, Some(deadline + 20), None);
    let q = apply(&f.alice, &p, ack.clone()).await;
    assert!(q.deadline_observed() && !q.needs_poll());
    assert_eq!(q.acknowledged_at_unix_ms(), Some(deadline + 20));
    let after = records(f.root.path(), A);
    apply(&f.alice, &p, expired).await;
    apply(&f.alice, &p, fact(&p, stored, None, None)).await;
    apply(&f.alice, &p, ack).await;
    assert_eq!(records(f.root.path(), A), after); // No revision/write on replay.
    assert!(
        f.alice
            .pending_epoch_file_statuses(None)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    let q = w.epoch_file_progress(uuid(&p)).await.unwrap();
    assert_eq!(q.receipt(), p.receipt().unwrap());
    assert_eq!(q.observed_at_unix_ms(), Some(deadline + 30));
    assert!(q.deadline_observed() && !q.needs_poll());
    assert_eq!(immutable(records(f.root.path(), A)), before);
    assert_eq!(wrapped(f.root.path(), A), master);
    assert_eq!(
        w.epoch_outgoing_plan(uuid(&p)).await.unwrap().request(),
        p.request()
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_out_of_order_positive_ack_and_retirement_facts_are_retained() {
    let (f, s, _) = prepared().await;
    let p = accepted(&f.alice, &s).await;
    seed(&f.alice).await;
    let at = p.receipt().unwrap().stored_at_unix_ms;
    let q = apply(&f.alice, &p, fact(&p, at + 100, None, Some(at + 80))).await;
    assert!(!q.needs_poll());
    // Earlier observation can add a positive fact missing from a later reply.
    let q = apply(&f.alice, &p, fact(&p, at + 70, Some(at + 60), None)).await;
    assert_eq!(q.observed_at_unix_ms(), Some(at + 100));
    assert_eq!(q.acknowledged_at_unix_ms(), Some(at + 60));
    assert_eq!(q.recipient_replaced_at_unix_ms(), Some(at + 80));
    assert!(!q.deadline_observed() && !q.needs_poll());
    let before = records(f.root.path(), A);
    for bad in [
        fact(&p, at + 110, Some(at + 61), None),
        fact(&p, at + 110, None, Some(at + 81)),
    ] {
        assert!(matches!(
            f.alice
                .accept_epoch_file_statuses(observed(vec![p.clone()], vec![bad]))
                .await,
            Err(CryptoError::Conflict)
        ));
        assert_eq!(records(f.root.path(), A), before);
    }
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_bad_bindings_scopes_and_changed_receipts_never_update_progress() {
    let (f, s, _) = prepared().await;
    let p = accepted(&f.alice, &s).await;
    seed(&f.alice).await;
    let good = fact(&p, p.receipt().unwrap().stored_at_unix_ms + 10, None, None);
    let before = records(f.root.path(), A);
    for case in 0..8 {
        let mut bad = good.clone();
        match case {
            0 => bad.format_version = 2,
            1 => {
                bad.reference
                    .as_mut()
                    .unwrap()
                    .blob
                    .as_mut()
                    .unwrap()
                    .sha256[0] ^= 1
            }
            2 => {
                bad.reference
                    .as_mut()
                    .unwrap()
                    .binding
                    .as_mut()
                    .unwrap()
                    .epoch += 1
            }
            3 => bad.receipt.as_mut().unwrap().envelope_digest[0] ^= 1,
            4 => bad.acknowledged_at_unix_ms = Some(0),
            5 => bad.observed_at_unix_ms = 0,
            6 => {
                bad.reference
                    .as_mut()
                    .unwrap()
                    .binding
                    .as_mut()
                    .unwrap()
                    .sender
                    .as_mut()
                    .unwrap()
                    .generation_id = uuid::Uuid::new_v4().to_string()
            }
            _ => {
                bad.receipt.as_mut().unwrap().stored_at_unix_ms += 1;
                bad.receipt.as_mut().unwrap().expires_at_unix_ms += 1;
                bad.expires_at_unix_ms += 1;
            }
        }
        assert!(
            f.alice
                .accept_epoch_file_statuses(observed(vec![p.clone()], vec![bad]))
                .await
                .is_err()
        );
    }
    assert!(
        f.bob
            .accept_epoch_file_statuses(observed(vec![p.clone()], vec![good.clone()]))
            .await
            .is_err()
    );
    assert!(
        f.alice
            .accept_epoch_file_statuses(EpochObservation::test_observation(
                StoreScope::new("https://other.example.test", A).unwrap(),
                vec![p.clone()],
                v2::FileStatuses {
                    files: vec![good.clone()]
                }
            ))
            .await
            .is_err()
    );
    assert!(
        f.alice
            .accept_epoch_file_statuses(observed(vec![p.clone(); 2], vec![good.clone(); 2]))
            .await
            .is_err()
    );
    assert!(
        f.alice
            .accept_epoch_file_statuses(observed(vec![p; 33], vec![good; 33]))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_batch_conflict_or_commit_failure_rolls_back_facts_anchor_and_pending() {
    let (f, s, _) = prepared().await;
    let p = accepted(&f.alice, &s).await;
    let q = accepted(&f.alice, &s).await;
    seed(&f.alice).await;
    let at = p.receipt().unwrap().stored_at_unix_ms + 10;
    apply(&f.alice, &q, fact(&q, at, Some(at), None)).await;
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice
            .accept_epoch_file_statuses(observed(
                vec![p.clone(), q.clone()],
                vec![
                    fact(&p, at + 1, Some(at), None),
                    fact(&q, at + 1, Some(at + 1), None)
                ]
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), before);
    let raw = db(f.root.path());
    raw.execute_batch("CREATE TABLE fp_parent(id INTEGER PRIMARY KEY); CREATE TABLE fp_child(id INTEGER REFERENCES fp_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fp_fault AFTER UPDATE ON protected_records WHEN NEW.record_id LIKE 'file-progress-anchor:%' BEGIN INSERT INTO fp_child VALUES(1); END;").unwrap();
    assert!(matches!(
        f.alice
            .accept_epoch_file_statuses(observed(
                vec![p.clone()],
                vec![fact(&p, at, Some(at), None)]
            ))
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert_eq!(
        f.alice
            .pending_epoch_file_statuses(None)
            .await
            .unwrap()
            .files
            .len(),
        1
    );
    raw.execute_batch("DROP TRIGGER fp_fault").unwrap();
    drop(raw);
    apply(&f.alice, &p, fact(&p, at, Some(at), None)).await;
    assert!(
        f.alice
            .pending_epoch_file_statuses(None)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_receipt_paths_commit_tracking_atomically_including_terminal_descriptor_status()
{
    for via_status in [false, true] {
        let (f, s, _) = prepared().await;
        seed(&f.alice).await;
        let (p, _) = message(&s);
        let p = f.alice.encrypt_epoch(s.clone(), p).await.unwrap();
        let before = records(f.root.path(), A);
        let raw = db(f.root.path());
        raw.execute_batch("CREATE TABLE fp_parent(id INTEGER PRIMARY KEY); CREATE TABLE fp_child(id INTEGER REFERENCES fp_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fp_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE 'file-progress:%' BEGIN INSERT INTO fp_child VALUES(1); END;").unwrap();
        let result = if via_status {
            f.alice
                .accept_epoch_statuses(EpochObservation::test_observation(
                    s.scope.clone(),
                    vec![p.clone()],
                    v2::MessageStatuses {
                        messages: vec![v2::MessageStatus {
                            receipt: Some(receipt(&p)),
                            state: MessageState::RecipientStored as i32,
                            observed_at_unix_ms: receipt(&p).stored_at_unix_ms + 1,
                        }],
                    },
                ))
                .await
                .map(|_| ())
        } else {
            f.alice
                .accept_epoch_send_receipt(EpochObservation::test_observation(
                    s.scope.clone(),
                    p.clone(),
                    receipt(&p),
                ))
                .await
        };
        assert!(matches!(result, Err(CryptoError::Storage(_))));
        assert_eq!(records(f.root.path(), A), before);
        assert!(
            f.alice
                .epoch_outgoing_plan(uuid(&p))
                .await
                .unwrap()
                .needs_send()
        );
        raw.execute_batch("DROP TRIGGER fp_fault").unwrap();
        drop(raw);
        if via_status {
            descriptor_ack(&f.alice, &p).await;
        } else {
            f.alice
                .accept_epoch_send_receipt(EpochObservation::test_observation(
                    s.scope.clone(),
                    p.clone(),
                    receipt(&p),
                ))
                .await
                .unwrap();
        }
        assert_eq!(
            f.alice
                .pending_epoch_file_statuses(None)
                .await
                .unwrap()
                .files
                .len(),
            1
        );
        let before = records(f.root.path(), A);
        f.alice
            .accept_epoch_send_receipt(EpochObservation::test_observation(
                s.scope.clone(),
                p.clone(),
                receipt(&p),
            ))
            .await
            .unwrap();
        assert_eq!(records(f.root.path(), A), before);
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn file_status_bounded_backfill_uses_all_outboxes_and_new_writes_behind_cursor_are_tracked() {
    let (f, s, _) = prepared().await;
    let mut plans = Vec::new();
    for _ in 0..65 {
        let p = accepted(&f.alice, &s).await;
        descriptor_ack(&f.alice, &p).await;
        plans.push(p);
    }
    let text = f
        .alice
        .encrypt_epoch(s.clone(), payload(&s, "not a file"))
        .await
        .unwrap();
    descriptor_ack(&f.alice, &text).await;
    let mut sorted: Vec<_> = plans.iter().map(uuid).collect();
    sorted.sort();
    let unknown = loop {
        let (p, _) = message(&s);
        if p.message_id < sorted[16] {
            break p;
        }
    };
    let unknown = f.alice.encrypt_epoch(s.clone(), unknown).await.unwrap();
    f.alice.shutdown().await.unwrap();
    let raw = db(f.root.path());
    // Reproduce pre-feature data. No production reset/backfill shortcut.
    raw.execute(
        "DELETE FROM protected_records WHERE record_id LIKE 'file-progress%'",
        [],
    )
    .unwrap();
    drop(raw);
    let before = records(f.root.path(), A);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert_eq!(records(f.root.path(), A), before); // DB open did not scan/seed.
    assert!(matches!(
        w.pending_epoch_file_statuses(None).await,
        Err(CryptoError::FileStatusIndexNotReady)
    ));
    let first = w.seed_epoch_file_status_index().await.unwrap();
    assert_eq!(first.scanned, 32);
    assert!(!first.ready);
    // This unknown outbox was among the first 32 and is now behind the boundary.
    // Receipt acceptance must track it without waiting for/restarting the scan.
    w.accept_epoch_send_receipt(EpochObservation::test_observation(
        s.scope.clone(),
        unknown.clone(),
        receipt(&unknown),
    ))
    .await
    .unwrap();
    plans.push(w.epoch_outgoing_plan(uuid(&unknown)).await.unwrap());
    w.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    let raw = db(f.root.path());
    raw.execute_batch("CREATE TABLE seed_parent(id INTEGER PRIMARY KEY); CREATE TABLE seed_child(id INTEGER REFERENCES seed_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER seed_fault AFTER UPDATE ON protected_records WHEN NEW.record_id='file-progress-index-anchor:v1' BEGIN INSERT INTO seed_child VALUES(1); END;").unwrap();
    assert!(matches!(
        w.seed_epoch_file_status_index().await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    raw.execute_batch("DROP TRIGGER seed_fault").unwrap();
    drop(raw);
    let second = w.seed_epoch_file_status_index().await.unwrap();
    assert_eq!(second.scanned, 32);
    assert!(!second.ready);
    seed(&w).await;
    let mut all = Vec::new();
    let mut cursor = None;
    loop {
        let page = w.pending_epoch_file_statuses(cursor).await.unwrap();
        assert!(page.files.len() <= 32);
        all.extend(page.files.iter().map(uuid));
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    all.sort();
    let mut expected: Vec<_> = plans.iter().map(uuid).collect();
    expected.sort();
    assert_eq!(all, expected);
    let cursor = w
        .pending_epoch_file_statuses(None)
        .await
        .unwrap()
        .next
        .unwrap();
    assert!(
        f.bob
            .pending_epoch_file_statuses(Some(cursor.clone()))
            .await
            .is_err()
    );
    for p in &plans {
        apply(
            &w,
            p,
            fact(
                p,
                p.receipt().unwrap().stored_at_unix_ms + 1,
                Some(p.receipt().unwrap().stored_at_unix_ms + 1),
                None,
            ),
        )
        .await;
    }
    assert!(
        w.pending_epoch_file_statuses(Some(cursor))
            .await
            .unwrap()
            .files
            .is_empty()
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_partial_journal_anchor_or_pending_replay_is_rejected_without_repair() {
    for target in [JOURNALS, ANCHORS, PENDING, INDEX_ANCHOR] {
        let (f, s, _) = prepared().await;
        let p = accepted(&f.alice, &s).await;
        seed(&f.alice).await;
        let id = record_id(&s, &uuid(&p)).unwrap();
        let raw = db(f.root.path());
        let name = if target == INDEX_ANCHOR {
            INDEX_ANCHOR.into()
        } else {
            format!("{target}{id}")
        };
        let old: Vec<u8> = raw
            .query_row(
                "SELECT payload FROM protected_records WHERE record_id=?1",
                [&name],
                |r| r.get(0),
            )
            .unwrap();
        let at = p.receipt().unwrap().stored_at_unix_ms + 1;
        apply(&f.alice, &p, fact(&p, at, None, None)).await;
        f.alice.shutdown().await.unwrap();
        if target == INDEX_ANCHOR {
            raw.execute("DELETE FROM protected_records WHERE record_id=?1", [&name])
                .unwrap();
        } else {
            raw.execute(
                "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
                rusqlite::params![old, name],
            )
            .unwrap();
        }
        drop(raw);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        let before = records(f.root.path(), A);
        assert!(w.epoch_file_progress(uuid(&p)).await.is_err());
        assert!(w.pending_epoch_file_statuses(None).await.is_err());
        assert!(
            w.accept_epoch_file_statuses(observed(vec![p.clone()], vec![fact(&p, at, None, None)]))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn file_status_freeze_peer_closure_allowed_but_own_closure_rejects_stale_reply() {
    for own_closure in [false, true] {
        let (f, s, _) = prepared().await;
        let p = accepted(&f.alice, &s).await;
        seed(&f.alice).await;
        f.alice.shutdown().await.unwrap();
        rewrite_frozen(f.root.path(), &f.a, true);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        let mut c = f.a.clone();
        c.state = ConversationState::Closed as i32;
        c.close_reason = ConversationCloseReason::DeviceReplaced as i32;
        c.closed_generation_id = if own_closure {
            s.own.generation_id.clone()
        } else {
            s.peer.generation_id.clone()
        };
        c.closed_at_unix_ms = 1_700_000_000_200;
        index(&w, &c).await;
        let before = records(f.root.path(), A);
        let at = p.receipt().unwrap().stored_at_unix_ms + 1;
        let response = fact(&p, at, Some(at), None);
        let result = w
            .accept_epoch_file_statuses(observed(vec![p], vec![response]))
            .await;
        if own_closure {
            assert!(matches!(result, Err(CryptoError::ArchivedGeneration)));
            assert_eq!(records(f.root.path(), A), before);
        } else {
            assert!(result.unwrap()[0].acknowledged_at_unix_ms().is_some());
        }
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[test]
fn file_status_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_FILE_PROGRESS_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_FILE_PROGRESS_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let fact = v2::FileStatus::decode(
                std::fs::read(root.join("public-file-progress.pb"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let uuid = fact.receipt.as_ref().unwrap().message_id.clone();
            let binding = fact.reference.as_ref().unwrap().binding.as_ref().unwrap();
            let own = binding.sender.clone().unwrap();
            let id = format!(
                "v2:{}:{:016x}:{uuid}",
                binding.conversation_id, binding.epoch
            );
            let mut protected = store(root, A, false);
            protected
                .test_exit_on_record_change(row_key(&own, ANCHORS, &id).unwrap(), after)
                .unwrap();
            let w = CryptoCoordinator::start(protected, own).await.unwrap();
            let plan = w.epoch_outgoing_plan(uuid).await.unwrap();
            let _ = w
                .accept_epoch_file_statuses(observed(vec![plan], vec![fact]))
                .await;
            panic!("file progress crash hook did not fire");
        });
}

#[tokio::test]
async fn file_status_process_exit_before_after_commit_keeps_pending_or_ack_atomically() {
    for after in [false, true] {
        let (f, s, _) = prepared().await;
        let p = accepted(&f.alice, &s).await;
        seed(&f.alice).await;
        let at = p.receipt().unwrap().stored_at_unix_ms + 1;
        std::fs::write(
            f.root.path().join("public-file-progress.pb"),
            fact(&p, at, Some(at), None).encode_to_vec(),
        )
        .unwrap();
        f.alice.shutdown().await.unwrap();
        let before = records(f.root.path(), A);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "encryption::signal_state::epoch_state::outgoing::file_progress::tests::file_status_crash_child", "--nocapture"])
            .env("ZCHAT_FILE_PROGRESS_CRASH_PATH", f.root.path())
            .env("ZCHAT_FILE_PROGRESS_CRASH_AFTER", if after { "1" } else { "0" })
            .status().unwrap();
        assert_eq!(result.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        let q = w.epoch_file_progress(uuid(&p)).await.unwrap();
        assert_eq!(q.acknowledged_at_unix_ms(), after.then_some(at));
        assert_eq!(q.needs_poll(), !after);
        assert_eq!(
            w.pending_epoch_file_statuses(None)
                .await
                .unwrap()
                .files
                .len(),
            usize::from(!after)
        );
        assert_eq!(
            w.epoch_outgoing_plan(uuid(&p)).await.unwrap().request(),
            p.request()
        );
        if !after {
            assert_eq!(records(f.root.path(), A), before);
        }
        assert_eq!(immutable(records(f.root.path(), A)), immutable(before));
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
