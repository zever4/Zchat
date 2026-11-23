//! Worker invariants with real Signal state. RPC proofs are constructed only in
//! these trusted unit tests; native TLS/PG interop tests mint actual RPC proofs.
use super::*;
use crate::encryption::contract::{MESSAGE_RETENTION_MS, envelope_digest};
use crate::proto::e2ee::{Delivery, MessageState, MessageStatus, MessageStatuses, SendReceipt};

fn delivery(envelope: Envelope) -> Delivery {
    Delivery {
        sequence: 1,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        envelope: Some(envelope),
    }
}
fn receipt(plan: &OutgoingPlan) -> SendReceipt {
    let e = plan.request().envelope.as_ref().unwrap();
    SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
    }
}
fn observation(plan: &OutgoingPlan, state: MessageState) -> MessageStatus {
    let r = receipt(plan);
    let at = if state == MessageState::Pending {
        r.stored_at_unix_ms
    } else {
        r.expires_at_unix_ms + 1000
    };
    MessageStatus {
        receipt: Some(r),
        state: state as i32,
        observed_at_unix_ms: at,
    }
}
async fn apply(
    worker: &CryptoCoordinator,
    plan: &OutgoingPlan,
    state: MessageState,
) -> CryptoResult<Vec<OutgoingProgress>> {
    worker
        .accept_message_statuses(VerifiedMessageStatuses::from_rpc(
            vec![plan.clone()],
            MessageStatuses {
                messages: vec![observation(plan, state)],
            },
        ))
        .await
}
fn control_key(scope: &SessionScope, prefix: &str, id: &str) -> RecordKey {
    scope
        .key(RecordKind::Control, &format!("{prefix}{id}"))
        .unwrap()
}
fn fault(root: &std::path::Path, scope: &SessionScope, prefix: &str, id: &str, install: bool) {
    let path = StoreScope::new(SERVER, &scope.own.user_id)
        .unwrap()
        .database_path(root);
    let db = rusqlite::Connection::open(path).unwrap();
    if install {
        let record = format!("{prefix}{id}");
        assert!(
            record
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b':' || b == b'-')
        );
        db.execute_batch(&format!("CREATE TABLE delivery_parent(id INTEGER PRIMARY KEY);CREATE TABLE delivery_child(id INTEGER REFERENCES delivery_parent(id) DEFERRABLE INITIALLY DEFERRED);
            CREATE TRIGGER delivery_insert AFTER INSERT ON protected_records WHEN NEW.kind=13 AND NEW.record_id='{record}' BEGIN INSERT INTO delivery_child VALUES(1); END;
            CREATE TRIGGER delivery_update AFTER UPDATE ON protected_records WHEN NEW.kind=13 AND NEW.record_id='{record}' BEGIN INSERT INTO delivery_child VALUES(1); END;")).unwrap();
    } else {
        db.execute_batch("DROP TRIGGER delivery_insert;DROP TRIGGER delivery_update;DROP TABLE delivery_child;DROP TABLE delivery_parent;").unwrap()
    }
}

#[tokio::test]
async fn outgoing_receipt_reopens_without_reencrypt_and_conflicting_receipt_is_rejected() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "exact outbox survives lost server receipt");
    let envelope = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    let plan = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    let r = receipt(&plan);
    assert!(plan.progress().receipt().is_none());
    let progress = p
        .alice
        .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan.clone(), r.clone()))
        .await
        .unwrap();
    assert_eq!(progress.receipt(), Some(&r));
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a).unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), p.a.own.clone())
        .await
        .unwrap();
    let reopened = alice.outgoing_plan(text.message_id.clone()).await.unwrap();
    assert_eq!(reopened.request().envelope.as_ref(), Some(&envelope));
    assert_eq!(reopened.progress().receipt(), Some(&r));
    alice
        .accept_send_receipt(VerifiedSendReceipt::from_rpc(reopened.clone(), r.clone()))
        .await
        .unwrap();
    let mut conflict = r.clone();
    conflict.stored_at_unix_ms += 1;
    conflict.expires_at_unix_ms += 1;
    assert!(matches!(
        alice
            .accept_send_receipt(VerifiedSendReceipt::from_rpc(reopened, conflict))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        alice.outgoing_plan(Uuid::new_v4().to_string()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    alice.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.a).unwrap(), before);
    assert_eq!(
        saved(p.root.path(), &p.a, RecordKind::Outbox, &text.message_id).unwrap(),
        envelope.encode_to_vec()
    );
}

#[tokio::test]
async fn receipt_commit_failure_preserves_unknown_delivery_and_exact_outbox() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "receipt failure");
    let encrypted = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    let plan = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    let r = receipt(&plan);
    fault(p.root.path(), &p.a, "delivery-out:", &text.message_id, true);
    assert!(
        p.alice
            .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan.clone(), r.clone()))
            .await
            .is_err()
    );
    assert!(
        p.alice
            .outgoing_plan(text.message_id.clone())
            .await
            .unwrap()
            .progress()
            .receipt()
            .is_none()
    );
    fault(
        p.root.path(),
        &p.a,
        "delivery-out:",
        &text.message_id,
        false,
    );
    p.alice
        .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan, r))
        .await
        .unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert_eq!(
        saved(p.root.path(), &p.a, RecordKind::Outbox, &text.message_id).unwrap(),
        encrypted.encode_to_vec()
    );
}

#[tokio::test]
async fn status_merge_keeps_ack_and_receipt_under_late_responses() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "monotonic statuses");
    p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    let plan = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    for state in [
        MessageState::Pending,
        MessageState::Expired,
        MessageState::RecipientStored,
        MessageState::Expired,
        MessageState::Pending,
        MessageState::RecipientReplaced,
    ] {
        apply(&p.alice, &plan, state).await.unwrap();
    }
    let current = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    assert_eq!(
        current.progress().status().unwrap().state,
        MessageState::RecipientStored as i32
    );
    assert_eq!(current.progress().receipt(), Some(&receipt(&plan)));
    assert!(matches!(
        apply(&p.alice, &plan, MessageState::RecipientRejected).await,
        Err(CryptoError::Conflict)
    ));
    let mut wrong = observation(&plan, MessageState::RecipientStored);
    wrong.observed_at_unix_ms += 1;
    assert!(matches!(
        p.alice
            .accept_message_statuses(VerifiedMessageStatuses::from_rpc(
                vec![plan],
                MessageStatuses {
                    messages: vec![wrong]
                }
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), p.a.own)
        .await
        .unwrap();
    assert_eq!(
        alice
            .outgoing_plan(text.message_id)
            .await
            .unwrap()
            .progress()
            .status()
            .unwrap()
            .state,
        MessageState::RecipientStored as i32
    );
    alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn status_batch_rolls_back_on_conflicting_last_receipt() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let mut plans = Vec::new();
    for _ in 0..2 {
        let text = payload(&p.a, "batch");
        p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
        plans.push(p.alice.outgoing_plan(text.message_id).await.unwrap());
    }
    let r = receipt(&plans[1]);
    p.alice
        .accept_send_receipt(VerifiedSendReceipt::from_rpc(plans[1].clone(), r))
        .await
        .unwrap();
    let first = observation(&plans[0], MessageState::RecipientStored);
    let mut second = observation(&plans[1], MessageState::RecipientStored);
    let receipt = second.receipt.as_mut().unwrap();
    receipt.stored_at_unix_ms += 1;
    receipt.expires_at_unix_ms += 1;
    assert!(matches!(
        p.alice
            .accept_message_statuses(VerifiedMessageStatuses::from_rpc(
                plans.clone(),
                MessageStatuses {
                    messages: vec![first, second]
                }
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(
        p.alice
            .outgoing_plan(
                plans[0]
                    .request()
                    .envelope
                    .as_ref()
                    .unwrap()
                    .message_id
                    .clone()
            )
            .await
            .unwrap()
            .progress()
            .receipt()
            .is_none()
    );
    assert!(
        p.alice
            .outgoing_plan(
                plans[1]
                    .request()
                    .envelope
                    .as_ref()
                    .unwrap()
                    .message_id
                    .clone()
            )
            .await
            .unwrap()
            .progress()
            .status()
            .is_none()
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn receive_commits_signal_history_inbox_and_proof_then_recovers_after_restart() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "durable ACK");
    let wire = delivery(p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap());
    let (received, proof) = p
        .bob
        .receive_delivery(p.b.clone(), wire.clone())
        .await
        .unwrap();
    assert_eq!(received, text);
    assert_eq!(proof.request().messages[0].disposition, 1);
    let (replayed, again) = p.bob.receive_delivery(p.b.clone(), wire).await.unwrap();
    assert_eq!(received, replayed);
    assert_eq!(proof.request(), again.request());
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.b).unwrap();
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own.clone())
        .await
        .unwrap();
    let page = bob.pending_acknowledgements(None).await.unwrap();
    assert_eq!(page.acknowledgements.len(), 1);
    assert_eq!(page.acknowledgements[0].request(), proof.request());
    bob.mark_acknowledged(VerifiedAck::from_rpc(proof.clone()))
        .await
        .unwrap();
    bob.mark_acknowledged(VerifiedAck::from_rpc(proof))
        .await
        .unwrap();
    assert!(
        bob.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    bob.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.b).unwrap(), before);
    assert!(saved(p.root.path(), &p.b, RecordKind::Inbox, &text.message_id).is_some());
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::History, &text.message_id).unwrap(),
        text.encode_to_vec()
    );
}

#[tokio::test]
async fn failed_receive_commit_returns_neither_plaintext_nor_ack_and_rolls_back_keys() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "atomic receive");
    let wire = delivery(p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap());
    fault(p.root.path(), &p.b, "delivery-in:", &text.message_id, true);
    assert!(
        p.bob
            .receive_delivery(p.b.clone(), wire.clone())
            .await
            .is_err()
    );
    assert!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert!(matches!(
        p.bob.prepare_ack(p.b.clone(), wire.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    fault(p.root.path(), &p.b, "delivery-in:", &text.message_id, false);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.b).is_none());
    for kind in [RecordKind::Inbox, RecordKind::History] {
        assert!(saved(p.root.path(), &p.b, kind, &text.message_id).is_none());
    }
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1],
        0
    );
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::PqPreKey, &PQ.to_string()).unwrap()[1],
        0
    );
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own.clone())
        .await
        .unwrap();
    assert_eq!(bob.receive_delivery(p.b, wire).await.unwrap().0, text);
    bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn quarantine_retains_ciphertext_without_signal_mutation_and_disposition_is_fixed() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "quarantine");
    let mut envelope = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    envelope.ciphertext = vec![1];
    let wire = delivery(envelope.clone());
    assert!(
        p.bob
            .receive_delivery(p.b.clone(), wire.clone())
            .await
            .is_err()
    );
    assert!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    let proof = p
        .bob
        .quarantine_delivery(
            p.b.clone(),
            wire.clone(),
            QuarantineReason::InvalidCiphertext,
        )
        .await
        .unwrap();
    assert_eq!(proof.request().messages[0].disposition, 2);
    assert_eq!(
        p.bob
            .quarantine_delivery(
                p.b.clone(),
                wire.clone(),
                QuarantineReason::InvalidCiphertext
            )
            .await
            .unwrap()
            .request(),
        proof.request()
    );
    assert!(matches!(
        p.bob.decrypt(p.b.clone(), envelope.clone()).await,
        Err(CryptoError::Quarantined)
    ));
    assert!(p.bob.prepare_ack(p.b.clone(), wire.clone()).await.is_err());
    let mut changed = wire;
    changed.sequence += 1;
    assert!(matches!(
        p.bob
            .quarantine_delivery(p.b.clone(), changed, QuarantineReason::InvalidCiphertext)
            .await,
        Err(CryptoError::Conflict)
    ));
    let mut collision = payload(&p.b, "cannot reuse quarantined UUID for outgoing");
    collision.message_id = text.message_id.clone();
    assert!(matches!(
        p.bob.encrypt(p.b.clone(), collision).await,
        Err(CryptoError::Conflict)
    ));
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.b).is_none());
    assert!(saved(p.root.path(), &p.b, RecordKind::History, &text.message_id).is_none());
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::Inbox, &text.message_id).unwrap(),
        envelope.encode_to_vec()
    );
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1],
        0
    );
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own)
        .await
        .unwrap();
    assert_eq!(
        bob.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements[0]
            .request(),
        proof.request()
    );
    bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn quarantine_and_ack_marker_commit_failures_remain_retryable() {
    let p = pair(true, false).await;
    let envelope =
        p.b.envelope(&Uuid::new_v4().to_string(), vec![1], CiphertextType::Signal);
    // Orient a well-formed incoming envelope without requiring a valid Signal body.
    let incoming = Envelope {
        sender: Some(p.b.peer.clone()),
        recipient: Some(p.b.own.clone()),
        ..envelope
    };
    let wire = delivery(incoming);
    let id = wire.envelope.as_ref().unwrap().message_id.clone();
    fault(p.root.path(), &p.b, "delivery-in:", &id, true);
    assert!(
        p.bob
            .quarantine_delivery(
                p.b.clone(),
                wire.clone(),
                QuarantineReason::InvalidCiphertext
            )
            .await
            .is_err()
    );
    fault(p.root.path(), &p.b, "delivery-in:", &id, false);
    let proof = p
        .bob
        .quarantine_delivery(p.b.clone(), wire, QuarantineReason::InvalidCiphertext)
        .await
        .unwrap();
    fault(p.root.path(), &p.b, "delivery-in:", &id, true);
    assert!(
        p.bob
            .mark_acknowledged(VerifiedAck::from_rpc(proof.clone()))
            .await
            .is_err()
    );
    assert_eq!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    fault(p.root.path(), &p.b, "delivery-in:", &id, false);
    p.bob
        .mark_acknowledged(VerifiedAck::from_rpc(proof))
        .await
        .unwrap();
    assert!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn ack_proof_rejects_unsaved_altered_or_already_stored_quarantine() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "proof bindings");
    let wire = delivery(p.alice.encrypt(p.a.clone(), text).await.unwrap());
    assert!(matches!(
        p.bob.prepare_ack(p.b.clone(), wire.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    p.bob
        .receive_delivery(p.b.clone(), wire.clone())
        .await
        .unwrap();
    assert!(matches!(
        p.bob
            .quarantine_delivery(
                p.b.clone(),
                wire.clone(),
                QuarantineReason::InvalidCiphertext
            )
            .await,
        Err(CryptoError::Conflict)
    ));
    let mut bad = wire.clone();
    bad.expires_at_unix_ms += 1;
    assert!(matches!(
        p.bob.prepare_ack(p.b.clone(), bad).await,
        Err(CryptoError::Conflict)
    ));
    let mut bad = wire;
    bad.envelope_digest[0] ^= 1;
    assert!(p.bob.prepare_ack(p.b.clone(), bad).await.is_err());
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_pages_are_bounded_and_skip_completed_ack_history() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    for n in 0..35 {
        let text = payload(&p.a, "paged recovery");
        let env = p.alice.encrypt(p.a.clone(), text).await.unwrap();
        let mut wire = delivery(env);
        wire.sequence = n + 1;
        p.bob
            .quarantine_delivery(p.b.clone(), wire, QuarantineReason::InvalidCiphertext)
            .await
            .unwrap();
    }
    let first = p.alice.outbox_page(None).await.unwrap();
    assert_eq!(first.messages.len(), 32);
    let rest = p.alice.outbox_page(first.next).await.unwrap();
    assert_eq!(rest.messages.len(), 3);
    assert!(rest.next.is_none());
    let first = p.bob.pending_acknowledgements(None).await.unwrap();
    assert_eq!(first.acknowledgements.len(), 32);
    for proof in first.acknowledgements {
        p.bob
            .mark_acknowledged(VerifiedAck::from_rpc(proof))
            .await
            .unwrap();
    }
    let rest = p.bob.pending_acknowledgements(None).await.unwrap();
    assert_eq!(rest.acknowledgements.len(), 3);
    assert!(rest.next.is_none());
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn proofs_reject_other_account_generation_and_server_scope() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "scope proof");
    let env = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    let plan = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    assert!(
        p.bob
            .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan.clone(), receipt(&plan)))
            .await
            .is_err()
    );
    let root = tempfile::tempdir().unwrap();
    let other_scope = StoreScope::new("https://other.example.test", A).unwrap();
    let other = CryptoCoordinator::start(test_open(root.path(), other_scope, true), device(A, AG))
        .await
        .unwrap();
    assert!(matches!(
        other
            .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan.clone(), receipt(&plan)))
            .await,
        Err(CryptoError::Conflict)
    ));
    let (_, proof) = p
        .bob
        .receive_delivery(p.b.clone(), delivery(env))
        .await
        .unwrap();
    assert!(
        p.alice
            .mark_acknowledged(VerifiedAck::from_rpc(proof))
            .await
            .is_err()
    );
    other.shutdown().await.unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let other_generation = CryptoCoordinator::start(
        store(p.root.path(), A, false),
        device(A, &Uuid::new_v4().to_string()),
    )
    .await
    .unwrap();
    assert!(
        other_generation
            .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan.clone(), receipt(&plan)))
            .await
            .is_err()
    );
    other_generation.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupted_receipt_or_inbox_prevents_recovery_proof_without_repair() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "corruption");
    let env = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    let plan = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    p.alice
        .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan.clone(), receipt(&plan)))
        .await
        .unwrap();
    p.bob
        .receive_delivery(p.b.clone(), delivery(env))
        .await
        .unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let mut alice = store(p.root.path(), A, false);
    alice
        .transaction(|tx| tx.put(&control_key(&p.a, "delivery-out:", &text.message_id), &[1]))
        .unwrap();
    drop(alice);
    let mut bob = store(p.root.path(), B, false);
    bob.transaction(|tx| tx.put(&p.b.key(RecordKind::Inbox, &text.message_id)?, &[1]))
        .unwrap();
    drop(bob);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), p.a.own)
        .await
        .unwrap();
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own)
        .await
        .unwrap();
    assert!(alice.outbox_page(None).await.is_err());
    assert!(bob.pending_acknowledgements(None).await.is_err());
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

fn conversation_for(scope: &SessionScope) -> Conversation {
    Conversation {
        conversation_id: scope.conversation.clone(),
        mode: ConversationMode::SignalPq1 as i32,
        own_device: Some(scope.own.clone()),
        peer_device: Some(Device {
            reference: Some(scope.peer.clone()),
            registration_id: scope.peer_registration,
            identity_key: scope.peer_identity.serialize().to_vec(),
            public_material_ready: true,
            public_material_revision: 1,
        }),
        state: ConversationState::Active as i32,
        ..Default::default()
    }
}
fn crash(
    root: &std::path::Path,
    scope: &SessionScope,
    wire: &Delivery,
    mode: &str,
    after: bool,
) -> std::process::Output {
    std::fs::write(
        root.join("delivery-conversation.bin"),
        conversation_for(scope).encode_to_vec(),
    )
    .unwrap();
    std::fs::write(root.join("delivery-message.bin"), wire.encode_to_vec()).unwrap();
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "encryption::signal_state::tests::delivery::delivery_crash_worker",
            "--nocapture",
        ])
        .env("ZCHAT_DELIVERY_CHILD_ROOT", root)
        .env("ZCHAT_DELIVERY_CHILD_MODE", mode)
        .env(
            "ZCHAT_DELIVERY_CHILD_AFTER",
            if after { "yes" } else { "no" },
        )
        .output()
        .unwrap()
}

#[tokio::test]
async fn process_exit_recovers_receipt_receive_and_quarantine_atomic_commits() {
    for mode in ["receipt", "receive", "quarantine"] {
        let p = pair(true, false).await;
        p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
        let text = payload(&p.a, "crash delivery");
        let mut env = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
        if mode == "quarantine" {
            env.ciphertext = vec![1]
        }
        let wire = delivery(env);
        p.alice.shutdown().await.unwrap();
        p.bob.shutdown().await.unwrap();
        let own = if mode == "receipt" { &p.a } else { &p.b };
        assert_eq!(
            crash(p.root.path(), own, &wire, mode, false).status.code(),
            Some(73)
        );
        if mode == "receipt" {
            assert!(
                saved(
                    p.root.path(),
                    own,
                    RecordKind::Control,
                    &format!("delivery-out:{}", text.message_id)
                )
                .is_none()
            );
        } else {
            assert!(saved(p.root.path(), own, RecordKind::Inbox, &text.message_id).is_none());
            assert!(session(p.root.path(), own).is_none());
        }
        assert_eq!(
            crash(p.root.path(), own, &wire, mode, true).status.code(),
            Some(74)
        );
        let worker = CryptoCoordinator::start(
            store(p.root.path(), &own.own.user_id, false),
            own.own.clone(),
        )
        .await
        .unwrap();
        if mode == "receipt" {
            let plan = worker.outgoing_plan(text.message_id.clone()).await.unwrap();
            assert_eq!(plan.progress().receipt(), Some(&receipt(&plan)));
        } else {
            let page = worker.pending_acknowledgements(None).await.unwrap();
            assert_eq!(page.acknowledgements.len(), 1);
            assert_eq!(
                page.acknowledgements[0].request().messages[0].disposition,
                if mode == "receive" { 1 } else { 2 }
            );
        }
        worker.shutdown().await.unwrap();
        if mode == "receive" {
            assert!(session(p.root.path(), own).is_some());
            assert_eq!(
                saved(p.root.path(), own, RecordKind::History, &text.message_id).unwrap(),
                text.encode_to_vec()
            );
        }
        if mode == "quarantine" {
            assert!(session(p.root.path(), own).is_none());
            assert!(saved(p.root.path(), own, RecordKind::History, &text.message_id).is_none());
        }
    }
}

#[test]
fn delivery_crash_worker() {
    let Ok(root) = std::env::var("ZCHAT_DELIVERY_CHILD_ROOT") else {
        return;
    };
    let root = std::path::Path::new(&root);
    let mode = std::env::var("ZCHAT_DELIVERY_CHILD_MODE").unwrap();
    let after = std::env::var("ZCHAT_DELIVERY_CHILD_AFTER").unwrap() == "yes";
    let conversation = Conversation::decode(
        std::fs::read(root.join("delivery-conversation.bin"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let scope = SessionScope::from_conversation(&conversation).unwrap();
    let wire = Delivery::decode(
        std::fs::read(root.join("delivery-message.bin"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let id = wire.envelope.as_ref().unwrap().message_id.clone();
    let mut store = store(root, &scope.own.user_id, false);
    store.test_exit_on_record(
        control_key(
            &scope,
            if mode == "receipt" {
                "delivery-out:"
            } else {
                "delivery-in:"
            },
            &id,
        ),
        after,
    );
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let worker = CryptoCoordinator::start(store, scope.own.clone())
                .await
                .unwrap();
            match mode.as_str() {
                "receipt" => {
                    let plan = worker.outgoing_plan(id).await.unwrap();
                    let r = receipt(&plan);
                    worker
                        .accept_send_receipt(VerifiedSendReceipt::from_rpc(plan, r))
                        .await
                        .unwrap();
                }
                "receive" => {
                    worker.receive_delivery(scope, wire).await.unwrap();
                }
                "quarantine" => {
                    worker
                        .quarantine_delivery(scope, wire, QuarantineReason::InvalidCiphertext)
                        .await
                        .unwrap();
                }
                _ => panic!("invalid fixture mode"),
            };
        });
    panic!("test worker should have exited at the transaction boundary")
}
