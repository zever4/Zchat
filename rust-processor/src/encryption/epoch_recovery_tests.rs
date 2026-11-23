use super::super::super::tests::store;
use super::super::tests::{fixture, initialize, records};
use super::*;
use rusqlite::Connection;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
pub(super) fn response(
    p: &EpochRecoveryIntent,
    d: Option<&EpochRecoveryIntent>,
    state: common::RecoveryState,
) -> v2::RecoveryRecord {
    let c = p.proposal.control.as_ref().unwrap();
    v2::RecoveryRecord {
        outcome: Some(common::RecoveryOutcome {
            protocol_version: 2,
            proposal: Some(p.proposal.clone()),
            decision: d.and_then(|d| d.request.command.clone()),
            state: state as i32,
            new_epoch: if state == common::RecoveryState::Accepted {
                c.expected_epoch + 1
            } else {
                0
            },
            epoch_revision: if state == common::RecoveryState::Accepted {
                c.expected_revision + 1
            } else {
                0
            },
        }),
        created_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_086_400_000,
        resolved_at_unix_ms: if state == common::RecoveryState::Pending {
            None
        } else if state == common::RecoveryState::Expired {
            Some(1_700_086_400_000)
        } else {
            Some(1_700_000_000_001)
        },
    }
}
pub(super) fn observed(
    p: &EpochRecoveryIntent,
    r: &v2::RecoveryRecord,
) -> EpochObservation<EpochRecoveryIntent, v2::RecoveryRecord> {
    EpochObservation::test_observation(p.scope.scope.clone(), p.clone(), r.clone())
}
pub(super) async fn query(
    w: &CryptoCoordinator,
    s: &EpochScope,
    r: &v2::RecoveryRecord,
) -> CryptoResult<EpochRecoverySnapshot> {
    let id = r
        .outcome
        .as_ref()
        .unwrap()
        .proposal
        .as_ref()
        .unwrap()
        .control
        .as_ref()
        .unwrap()
        .proposal_id
        .clone();
    let q = w.prepare_epoch_recovery_query(s.clone(), id).await?;
    w.accept_epoch_recovery_query(EpochObservation::test_observation(
        s.scope.clone(),
        q,
        r.clone(),
    ))
    .await
}
fn raw(root: &std::path::Path, user: &str) -> Connection {
    Connection::open(
        StoreScope::new("https://chat.example.test", user)
            .unwrap()
            .database_path(root),
    )
    .unwrap()
}

#[tokio::test]
async fn proposal_is_signed_frozen_and_exact_before_any_rpc() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_recovery(s.clone()).await.unwrap();
    p.pins
        .verify_command(p.proposal(), p.request().command.as_ref().unwrap(), true)
        .unwrap();
    assert!(
        f.alice
            .epoch_scope(s.conversation.clone(), 0)
            .await
            .unwrap()
            .sending_frozen()
    );
    let before = records(f.root.path(), &s.own.user_id);
    let same = f.alice.prepare_epoch_recovery(s.clone()).await.unwrap();
    assert_eq!(p.request(), same.request());
    assert_eq!(records(f.root.path(), &s.own.user_id), before);
    assert!(matches!(
        f.alice.prepare_epoch_acquisition(s).await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_acceptance_is_required_and_exact_decision_survives_reopen() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let pending = response(&p, None, common::RecoveryState::Pending);
    f.alice
        .accept_epoch_recovery_result(observed(&p, &pending))
        .await
        .unwrap();
    let peer = query(&f.bob, &b, &pending).await.unwrap();
    assert_eq!(peer.state(), Some(common::RecoveryState::Pending));
    assert!(!peer.own_proposal() && !peer.sending_frozen());
    let private: Vec<_> = records(f.root.path(), B)
        .into_iter()
        .filter(|r| r.0 != RecordKind::Control as i64)
        .collect();
    let d = f
        .bob
        .prepare_epoch_recovery_decision(
            b.clone(),
            peer.proposal_id().into(),
            EpochRecoveryDecision::Accept,
        )
        .await
        .unwrap();
    assert!(
        f.bob
            .epoch_scope(CHAT.into(), 0)
            .await
            .unwrap()
            .sending_frozen()
    );
    assert_eq!(
        records(f.root.path(), B)
            .into_iter()
            .filter(|r| r.0 != RecordKind::Control as i64)
            .collect::<Vec<_>>(),
        private
    );
    assert!(matches!(
        f.bob
            .prepare_epoch_recovery_decision(
                b.clone(),
                peer.proposal_id().into(),
                EpochRecoveryDecision::Decline
            )
            .await,
        Err(CryptoError::Conflict)
    ));
    f.bob.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert_eq!(
        w.prepare_epoch_recovery_decision(
            b.clone(),
            peer.proposal_id().into(),
            EpochRecoveryDecision::Accept
        )
        .await
        .unwrap()
        .request(),
        d.request()
    );
    let accepted = response(&p, Some(&d), common::RecoveryState::Accepted);
    let next = w
        .accept_epoch_recovery_result(observed(&d, &accepted))
        .await
        .unwrap();
    assert_eq!(next.state(), Some(common::RecoveryState::Accepted));
    assert!(next.sending_frozen()); // Activation/unknown old outbox is a separate gate.
    assert!(w.epoch_scope(CHAT.into(), 1).await.is_err());
    assert_eq!(
        query(&f.alice, &a, &accepted).await.unwrap().state(),
        Some(common::RecoveryState::Accepted)
    );
    let before = records(f.root.path(), B);
    query(&w, &b, &pending).await.unwrap(); // Stale pending cannot undo acceptance.
    assert_eq!(records(f.root.path(), B), before);
    w.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn signatures_without_matching_local_intent_cannot_authorize_acceptance() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let pending = response(&p, None, common::RecoveryState::Pending);
    query(&f.bob, &b, &pending).await.unwrap();
    // A valid signature generated outside the lane is deliberately NOT local consent.
    f.bob.shutdown().await.unwrap();
    let mut db = store(f.root.path(), B, false);
    let p2 = p.clone();
    let b2 = b.clone();
    let signed = db
        .transaction_with::<_, CryptoError>(|tx| {
            let mut c = p2.proposal.control.clone().unwrap();
            c.action = common::RecoveryAction::Accept as i32;
            c.request_id = uuid::Uuid::new_v4().to_string();
            c.proposal_digest = control::control_digest(p2.proposal.control.as_ref().unwrap())
                .unwrap()
                .to_vec();
            sign(tx, &b2, c)
        })
        .unwrap();
    drop(db);
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    let mut accepted = response(&p, None, common::RecoveryState::Accepted);
    accepted.outcome.as_mut().unwrap().decision = Some(signed);
    let before = records(f.root.path(), B);
    assert!(query(&w, &b, &accepted).await.is_err());
    assert_eq!(records(f.root.path(), B), before);
    // A server cannot reconstruct a missing own proposal journal from its signature.
    let mut unknown = p.clone();
    let mut c = unknown.proposal.control.clone().unwrap();
    c.request_id = uuid::Uuid::new_v4().to_string();
    c.proposal_id = c.request_id.clone();
    f.alice.shutdown().await.unwrap();
    let mut db = store(f.root.path(), A, false);
    unknown.proposal = db
        .transaction_with::<_, CryptoError>(|tx| sign(tx, &a, c))
        .unwrap();
    drop(db);
    let wa = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    assert!(
        query(
            &wa,
            &a,
            &response(&unknown, None, common::RecoveryState::Pending)
        )
        .await
        .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    wa.shutdown().await.unwrap();
    w.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_proposal_creation_and_mutual_proposals_preserve_exact_intents() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let (p, p2) = tokio::join!(
        f.alice.prepare_epoch_recovery(a.clone()),
        f.alice.prepare_epoch_recovery(a.clone())
    );
    let p = p.unwrap();
    assert_eq!(p.request(), p2.unwrap().request());
    let q = f.bob.prepare_epoch_recovery(b.clone()).await.unwrap();
    assert_ne!(
        p.proposal.control.as_ref().unwrap().proposal_id,
        q.proposal.control.as_ref().unwrap().proposal_id
    );
    query(
        &f.alice,
        &a,
        &response(&q, None, common::RecoveryState::Pending),
    )
    .await
    .unwrap();
    let current = f.alice.current_epoch_recoveries(a.clone()).await.unwrap();
    assert_eq!(current.len(), 2);
    assert!(current[0].own_proposal());
    assert!(!current[1].own_proposal());
    assert_eq!(
        current[0].proposal_id(),
        p.proposal.control.as_ref().unwrap().proposal_id
    );
    assert_eq!(
        current[1].proposal_id(),
        q.proposal.control.as_ref().unwrap().proposal_id
    );
    query(
        &f.bob,
        &b,
        &response(&p, None, common::RecoveryState::Pending),
    )
    .await
    .unwrap();
    let decision = f
        .alice
        .prepare_epoch_recovery_decision(
            a.clone(),
            q.proposal.control.as_ref().unwrap().proposal_id.clone(),
            EpochRecoveryDecision::Accept,
        )
        .await
        .unwrap();
    let accepted = response(&q, Some(&decision), common::RecoveryState::Accepted);
    assert!(
        f.alice
            .accept_epoch_recovery_result(observed(&decision, &accepted))
            .await
            .unwrap()
            .sending_frozen()
    );
    assert_eq!(
        f.alice.prepare_epoch_recovery(a).await.unwrap().request(),
        p.request()
    );
    assert!(
        f.bob
            .accept_epoch_recovery_result(observed(&q, &accepted))
            .await
            .unwrap()
            .sending_frozen()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn acceptance_and_observation_commit_failures_preserve_exact_resume_state() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    assert!(
        f.bob
            .current_epoch_recoveries(b.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let p = f.alice.prepare_epoch_recovery(a).await.unwrap();
    let id = p.proposal.control.as_ref().unwrap().proposal_id.clone();
    query(
        &f.bob,
        &b,
        &response(&p, None, common::RecoveryState::Pending),
    )
    .await
    .unwrap();
    let db = raw(f.root.path(), B);
    // UPSERT of an existing journal is UPDATE. A deferred constraint fails at
    // COMMIT after journal/head/root writes, not before the transaction starts.
    db.execute_batch("CREATE TABLE recovery_parent(id INTEGER PRIMARY KEY);CREATE TABLE recovery_child(id INTEGER REFERENCES recovery_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER recovery_commit_fault AFTER UPDATE ON protected_records WHEN NEW.record_id LIKE '%:recovery:%' BEGIN INSERT INTO recovery_child VALUES(1);END;").unwrap();
    let before = records(f.root.path(), B);
    assert!(matches!(
        f.bob
            .prepare_epoch_recovery_decision(b.clone(), id.clone(), EpochRecoveryDecision::Accept)
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    let current = f.bob.current_epoch_recoveries(b.clone()).await.unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].decision(), None);
    assert!(!current[0].sending_frozen());
    db.execute_batch("DROP TRIGGER recovery_commit_fault")
        .unwrap();
    let d = f
        .bob
        .prepare_epoch_recovery_decision(b.clone(), id.clone(), EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    let accepted = response(&p, Some(&d), common::RecoveryState::Accepted);
    db.execute_batch("CREATE TRIGGER recovery_commit_fault AFTER UPDATE ON protected_records WHEN NEW.record_id LIKE '%:recovery:%' BEGIN INSERT INTO recovery_child VALUES(1);END;").unwrap();
    let before = records(f.root.path(), B);
    assert!(matches!(
        f.bob
            .accept_epoch_recovery_result(observed(&d, &accepted))
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    db.execute_batch("DROP TRIGGER recovery_commit_fault")
        .unwrap();
    drop(db);
    f.bob.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    let current = w.current_epoch_recoveries(b.clone()).await.unwrap();
    assert_eq!(current.len(), 1);
    assert_eq!(current[0].proposal_id(), id);
    assert_eq!(current[0].state(), Some(common::RecoveryState::Pending));
    assert_eq!(current[0].decision(), Some(EpochRecoveryDecision::Accept));
    assert!(current[0].sending_frozen());
    assert_eq!(
        w.prepare_epoch_recovery_decision(b, id, EpochRecoveryDecision::Accept)
            .await
            .unwrap()
            .request(),
        d.request()
    );
    assert_eq!(
        w.accept_epoch_recovery_result(observed(&d, &accepted))
            .await
            .unwrap()
            .state(),
        Some(common::RecoveryState::Accepted)
    );
    w.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn verified_terminal_releases_only_its_freeze_and_stale_reply_cannot_replace_new_intent() {
    for terminal in [
        common::RecoveryState::Declined,
        common::RecoveryState::Cancelled,
        common::RecoveryState::Expired,
    ] {
        let f = fixture().await;
        let a = initialize(&f.alice).await;
        let b = initialize(&f.bob).await;
        let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
        let id = p.proposal.control.as_ref().unwrap().proposal_id.clone();
        let pending = response(&p, None, common::RecoveryState::Pending);
        f.alice
            .accept_epoch_recovery_result(observed(&p, &pending))
            .await
            .unwrap();
        query(&f.bob, &b, &pending).await.unwrap();
        let d = match terminal {
            common::RecoveryState::Declined => Some(
                f.bob
                    .prepare_epoch_recovery_decision(
                        b.clone(),
                        id.clone(),
                        EpochRecoveryDecision::Decline,
                    )
                    .await
                    .unwrap(),
            ),
            common::RecoveryState::Cancelled => Some(
                f.alice
                    .prepare_epoch_recovery_decision(
                        a.clone(),
                        id.clone(),
                        EpochRecoveryDecision::Cancel,
                    )
                    .await
                    .unwrap(),
            ),
            _ => None,
        };
        let r = response(&p, d.as_ref(), terminal);
        assert!(!query(&f.alice, &a, &r).await.unwrap().sending_frozen());
        query(&f.bob, &b, &r).await.unwrap();
        let next = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
        assert_ne!(next.request(), p.request());
        let before = records(f.root.path(), A);
        assert!(
            query(&f.alice, &a, &pending)
                .await
                .unwrap()
                .sending_frozen()
        );
        assert_eq!(records(f.root.path(), A), before);
        assert_eq!(
            f.alice.prepare_epoch_recovery(a).await.unwrap().request(),
            next.request()
        );
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn late_acceptance_wins_cancel_but_superseded_never_unfreezes_on_its_own() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let id = p.proposal.control.as_ref().unwrap().proposal_id.clone();
    query(
        &f.bob,
        &b,
        &response(&p, None, common::RecoveryState::Pending),
    )
    .await
    .unwrap();
    let cancel = f
        .alice
        .prepare_epoch_recovery_decision(a.clone(), id.clone(), EpochRecoveryDecision::Cancel)
        .await
        .unwrap();
    let accept = f
        .bob
        .prepare_epoch_recovery_decision(b.clone(), id, EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    let r = response(&p, Some(&accept), common::RecoveryState::Accepted);
    assert_eq!(
        f.alice
            .accept_epoch_recovery_result(observed(&cancel, &r))
            .await
            .unwrap()
            .state(),
        Some(common::RecoveryState::Accepted)
    );
    let before = records(f.root.path(), A);
    assert!(
        query(
            &f.alice,
            &a,
            &response(&p, Some(&cancel), common::RecoveryState::Cancelled)
        )
        .await
        .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    assert!(
        query(
            &f.alice,
            &a,
            &response(&p, None, common::RecoveryState::Superseded)
        )
        .await
        .unwrap()
        .sending_frozen()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_observation_bindings_signatures_and_terminal_changes_are_atomic() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let pending = response(&p, None, common::RecoveryState::Pending);
    f.alice
        .accept_epoch_recovery_result(observed(&p, &pending))
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    for fault in 0..7 {
        let proof = match fault {
            0 => EpochObservation::test_observation(
                StoreScope::new("https://other.example.test", A).unwrap(),
                p.clone(),
                pending.clone(),
            ),
            1 => {
                let mut r = pending.clone();
                r.outcome
                    .as_mut()
                    .unwrap()
                    .proposal
                    .as_mut()
                    .unwrap()
                    .signature[0] ^= 1;
                observed(&p, &r)
            }
            2 => {
                let mut r = pending.clone();
                r.created_at_unix_ms += 1;
                observed(&p, &r)
            }
            3 => {
                let mut r = pending.clone();
                r.expires_at_unix_ms += 1;
                observed(&p, &r)
            }
            4 => {
                let mut wrong = p.clone();
                wrong.request.command.as_mut().unwrap().signature[0] ^= 1;
                observed(&wrong, &pending)
            }
            5 => {
                let mut wrong = p.clone();
                wrong.request.context.as_mut().unwrap().protocol_version = 1;
                observed(&wrong, &pending)
            }
            _ => {
                let mut wrong = p.clone();
                wrong.scope.own.user_id = B.into();
                observed(&wrong, &pending)
            }
        };
        assert!(f.alice.accept_epoch_recovery_result(proof).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
    }
    assert!(
        f.bob
            .accept_epoch_recovery_result(observed(&p, &pending))
            .await
            .is_err()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn proposal_commit_failure_releases_no_intent_or_freeze_and_keeps_private_state() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let before = records(f.root.path(), A);
    let db = raw(f.root.path(), A);
    db.execute_batch("CREATE TABLE recovery_parent(id INTEGER PRIMARY KEY);CREATE TABLE recovery_child(id INTEGER REFERENCES recovery_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER recovery_commit_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE '%:recovery:%' BEGIN INSERT INTO recovery_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.alice.prepare_epoch_recovery(a.clone()).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert!(
        !f.alice
            .epoch_scope(CHAT.into(), 0)
            .await
            .unwrap()
            .sending_frozen()
    );
    db.execute_batch("DROP TRIGGER recovery_commit_fault")
        .unwrap();
    f.alice.prepare_epoch_recovery(a).await.unwrap();
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn peer_closure_blocks_fresh_consent_but_preserves_exact_retries_and_results() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let p = f.bob.prepare_epoch_recovery(b.clone()).await.unwrap();
    let id = p.proposal.control.as_ref().unwrap().proposal_id.clone();
    query(
        &f.alice,
        &a,
        &response(&p, None, common::RecoveryState::Pending),
    )
    .await
    .unwrap();
    let mut closed = f.a.clone();
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = common::ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_generation_id = a.peer.generation_id.clone();
    closed.closed_at_unix_ms = 1_700_000_000_200;
    super::super::tests::index(&f.alice, &closed).await;
    let before = records(f.root.path(), A);
    assert!(f.alice.prepare_epoch_recovery(a.clone()).await.is_err());
    assert!(
        f.alice
            .prepare_epoch_recovery_decision(a.clone(), id, EpochRecoveryDecision::Accept)
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    assert_eq!(
        query(
            &f.alice,
            &a,
            &response(&p, None, common::RecoveryState::Expired)
        )
        .await
        .unwrap()
        .state(),
        Some(common::RecoveryState::Expired)
    );
    assert!(
        f.alice
            .epoch_scope(CHAT.into(), 0)
            .await
            .unwrap()
            .receive_only()
    );
    // Freeze/closure snapshots cannot mint a new request; existing exact intent
    // remains recoverable after the peer installation is replaced.
    let mut peer_closed = f
        .bob
        .prepare_initial_epoch(CHAT.into())
        .await
        .unwrap()
        .pins
        .conversation()
        .clone();
    peer_closed.state = ConversationState::Closed as i32;
    peer_closed.close_reason = common::ConversationCloseReason::DeviceReplaced as i32;
    peer_closed.closed_generation_id = b.peer.generation_id.clone();
    peer_closed.closed_at_unix_ms = 1_700_000_000_200;
    super::super::tests::index(&f.bob, &peer_closed).await;
    assert_eq!(
        f.bob.prepare_epoch_recovery(b).await.unwrap().request(),
        p.request()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_or_replayed_recovery_cohort_blocks_scope_and_never_recreates_intent() {
    for fault in 0..5 {
        let f = fixture().await;
        let a = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
        let id = p.proposal.control.as_ref().unwrap().proposal_id.clone();
        let before = records(f.root.path(), A);
        f.alice
            .prepare_epoch_recovery_decision(a.clone(), id.clone(), EpochRecoveryDecision::Cancel)
            .await
            .unwrap();
        f.alice.shutdown().await.unwrap();
        let db = raw(f.root.path(), A);
        let target = match fault {
            0 => format!("epoch-v2:{CHAT}:recovery-head:0000000000000000"),
            1 => format!("epoch-v2:{CHAT}:recovery:{id}"),
            2 => format!("epoch-v2:{CHAT}:recovery-anchor:{id}"),
            3 => format!("epoch-v2:{CHAT}:recovery:{id}"),
            _ => format!("epoch-v2:{CHAT}:recovery-head:0000000000000000"),
        };
        if fault < 3 {
            db.execute(
                "DELETE FROM protected_records WHERE record_id=?1",
                [&target],
            )
            .unwrap();
        } else {
            let old = &before.iter().find(|r| r.1 == target).unwrap().2;
            db.execute(
                "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
                rusqlite::params![old, target],
            )
            .unwrap();
        }
        drop(db);
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
            .await
            .unwrap();
        assert!(w.epoch_scope(CHAT.into(), 0).await.is_err());
        assert!(w.prepare_epoch_recovery(a).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[test]
fn recovery_intent_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_RECOVERY_INTENT_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_RECOVERY_INTENT_CRASH_AFTER").unwrap() == "1";
    let accept = std::env::var("ZCHAT_RECOVERY_INTENT_CRASH_ACCEPT").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let user = if accept { B } else { A };
            let own = DeviceRef {
                user_id: user.into(),
                device_id: 1,
                generation_id: if accept {
                    "44444444-4444-4444-8444-444444444444"
                } else {
                    "33333333-3333-4333-8333-333333333333"
                }
                .into(),
            };
            let mut db = store(root, user, false);
            db.test_exit_on_record_change(head_key(&own, CHAT, 0).unwrap(), after)
                .unwrap();
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let s = w.epoch_scope(CHAT.into(), 0).await.unwrap();
            if accept {
                let id = std::fs::read_to_string(root.join("public-proposal-id.txt")).unwrap();
                let _ = w
                    .prepare_epoch_recovery_decision(s, id, EpochRecoveryDecision::Accept)
                    .await;
            } else {
                let _ = w.prepare_epoch_recovery(s).await;
            }
            panic!("recovery crash hook did not fire");
        });
}

#[tokio::test]
async fn process_exit_before_after_proposal_and_acceptance_commits_preserves_exact_freeze() {
    for accept in [false, true] {
        for after in [false, true] {
            let f = fixture().await;
            let a = initialize(&f.alice).await;
            let b = initialize(&f.bob).await;
            let id = if accept {
                let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
                query(
                    &f.bob,
                    &b,
                    &response(&p, None, common::RecoveryState::Pending),
                )
                .await
                .unwrap();
                p.proposal.control.as_ref().unwrap().proposal_id.clone()
            } else {
                String::new()
            };
            std::fs::write(f.root.path().join("public-proposal-id.txt"), &id).unwrap();
            f.alice.shutdown().await.unwrap();
            f.bob.shutdown().await.unwrap();
            let s = if accept { b } else { a };
            let user = s.own.user_id.clone();
            let before = records(f.root.path(), &user);
            let status=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","encryption::signal_state::epoch_state::recovery::tests::recovery_intent_crash_child","--nocapture"]).env("ZCHAT_RECOVERY_INTENT_CRASH_PATH",f.root.path()).env("ZCHAT_RECOVERY_INTENT_CRASH_AFTER",if after {"1"} else {"0"}).env("ZCHAT_RECOVERY_INTENT_CRASH_ACCEPT",if accept {"1"} else {"0"}).status().unwrap();
            assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
            let w = CryptoCoordinator::start(store(f.root.path(), &user, false), s.own.clone())
                .await
                .unwrap();
            assert_eq!(
                w.epoch_scope(CHAT.into(), 0)
                    .await
                    .unwrap()
                    .sending_frozen(),
                after
            );
            if !after {
                assert_eq!(records(f.root.path(), &user), before);
            }
            let p = if accept {
                w.prepare_epoch_recovery_decision(
                    s.clone(),
                    id.clone(),
                    EpochRecoveryDecision::Accept,
                )
                .await
                .unwrap()
            } else {
                w.prepare_epoch_recovery(s.clone()).await.unwrap()
            };
            let committed = records(f.root.path(), &user);
            let same = if accept {
                w.prepare_epoch_recovery_decision(s, id, EpochRecoveryDecision::Accept)
                    .await
                    .unwrap()
            } else {
                w.prepare_epoch_recovery(s).await.unwrap()
            };
            assert_eq!(same.request(), p.request());
            assert_eq!(records(f.root.path(), &user), committed);
            w.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn overview_is_one_readonly_snapshot_and_never_initializes_an_empty_dialog() {
    let f = fixture().await;
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice.epoch_recovery_overview(CHAT.into()).await,
        Err(CryptoError::RecoveryIndexesNotReady)
    ));
    assert!(records(f.root.path(), A) == before);
    while !f.alice.seed_recovery_indexes().await.unwrap().ready {}
    let before = records(f.root.path(), A);
    let empty = f.alice.epoch_recovery_overview(CHAT.into()).await.unwrap();
    assert_eq!(empty.crypto_epoch, None);
    assert!(!empty.sending_frozen && !empty.receive_only && empty.proposals.is_empty());
    assert!(records(f.root.path(), A) == before);
    let a = initialize(&f.alice).await;
    let before = records(f.root.path(), A);
    let initial = f.alice.epoch_recovery_overview(CHAT.into()).await.unwrap();
    assert_eq!(initial.crypto_epoch, Some(0));
    assert!(!initial.sending_frozen && initial.proposals.is_empty());
    assert!(records(f.root.path(), A) == before);
    f.alice.prepare_epoch_recovery_if(a, None).await.unwrap();
    let before = records(f.root.path(), A);
    let pending = f.alice.epoch_recovery_overview(CHAT.into()).await.unwrap();
    assert_eq!(pending.crypto_epoch, Some(0));
    assert!(
        pending.sending_frozen
            && pending.proposals.len() == 1
            && pending.proposals[0].own_proposal()
    );
    assert!(pending.proposals[0].state().is_none());
    assert!(records(f.root.path(), A) == before);
    assert!(f.bob.epoch_recovery_overview(A.into()).await.is_err());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn conditional_proposal_checks_own_reference_inside_signing_tx_and_cannot_advance_on_old_retry()
 {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let p = f
        .alice
        .prepare_epoch_recovery_if(a.clone(), None)
        .await
        .unwrap();
    let id = p.proposal().control.as_ref().unwrap().proposal_id.clone();
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice.prepare_epoch_recovery_if(a.clone(), None).await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.alice
            .prepare_epoch_recovery_if(a.clone(), Some(B.into()))
            .await,
        Err(CryptoError::Conflict)
    ));
    let exact = f
        .alice
        .prepare_epoch_recovery_if(a.clone(), Some(id.clone()))
        .await
        .unwrap();
    assert_eq!(p.request(), exact.request());
    assert!(records(f.root.path(), A) == before);
    let expired = response(&p, None, common::RecoveryState::Expired);
    query(&f.alice, &a, &expired).await.unwrap();
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice.prepare_epoch_recovery_if(a.clone(), None).await,
        Err(CryptoError::Conflict)
    ));
    assert!(records(f.root.path(), A) == before);
    let next = f
        .alice
        .prepare_epoch_recovery_if(a, Some(id.clone()))
        .await
        .unwrap();
    assert_ne!(next.proposal().control.as_ref().unwrap().proposal_id, id);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn overview_of_peer_proposal_does_not_freeze_or_implicitly_accept() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let p = f.alice.prepare_epoch_recovery_if(a, None).await.unwrap();
    query(
        &f.bob,
        &b,
        &response(&p, None, common::RecoveryState::Pending),
    )
    .await
    .unwrap();
    let before = records(f.root.path(), B);
    let view = f.bob.epoch_recovery_overview(CHAT.into()).await.unwrap();
    assert!(!view.sending_frozen && view.proposals.len() == 1);
    assert!(!view.proposals[0].own_proposal() && view.proposals[0].decision().is_none());
    assert_eq!(
        view.proposals[0].state(),
        Some(common::RecoveryState::Pending)
    );
    assert!(records(f.root.path(), B) == before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn offline_cancel_replays_original_proposal_before_exact_saved_decision_without_resigning() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let proposal = f
        .alice
        .prepare_epoch_recovery_if(a.clone(), None)
        .await
        .unwrap();
    let id = proposal
        .proposal()
        .control
        .as_ref()
        .unwrap()
        .proposal_id
        .clone();
    let cancel = f
        .alice
        .prepare_epoch_recovery_decision(a.clone(), id.clone(), EpochRecoveryDecision::Cancel)
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    let first = f
        .alice
        .resume_epoch_recovery(a.clone(), id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.request(), proposal.request());
    assert!(records(f.root.path(), A) == before);
    let pending = response(&proposal, None, common::RecoveryState::Pending);
    f.alice
        .accept_epoch_recovery_result(observed(&first, &pending))
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    let second = f
        .alice
        .resume_epoch_recovery(a.clone(), id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.request(), cancel.request());
    assert!(records(f.root.path(), A) == before);
    assert!(
        f.alice
            .epoch_recovery_overview(CHAT.into())
            .await
            .unwrap()
            .sending_frozen
    );
    let cancelled = response(&proposal, Some(&cancel), common::RecoveryState::Cancelled);
    f.alice
        .accept_epoch_recovery_result(observed(&second, &cancelled))
        .await
        .unwrap();
    assert!(
        !f.alice
            .epoch_recovery_overview(CHAT.into())
            .await
            .unwrap()
            .sending_frozen
    );
    assert!(
        f.alice
            .resume_epoch_recovery(a, id)
            .await
            .unwrap()
            .is_none()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
