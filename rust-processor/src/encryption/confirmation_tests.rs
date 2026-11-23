//! Real libsignal, protected SQLite, transaction failure and process-exit tests.
use super::*;
use crate::encryption::contract::{MESSAGE_RETENTION_MS, envelope_digest};
use crate::proto::e2ee::{ConversationCloseReason, Delivery, SessionConfirmation};

fn conversation(scope: &SessionScope) -> Conversation {
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
async fn apply(worker: &CryptoCoordinator, value: Conversation) -> SessionScope {
    let id = value.conversation_id.clone();
    let plan = worker
        .prepare_conversation_lookup(id.clone())
        .await
        .unwrap();
    worker
        .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(plan, value))
        .await
        .unwrap();
    worker.conversation_scope(id).await.unwrap()
}
fn wire(envelope: Envelope) -> Delivery {
    Delivery {
        sequence: 1,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        envelope: Some(envelope),
    }
}
fn link(scope: &SessionScope, id: &str) -> RecordKey {
    scope
        .key(RecordKind::Control, &format!("session-confirmation:{id}"))
        .unwrap()
}
async fn one(worker: &CryptoCoordinator) -> OutgoingPlan {
    let mut page = worker.outbox_page(None).await.unwrap();
    assert_eq!(page.messages.len(), 1);
    page.messages.pop().unwrap()
}
async fn first(p: &Pair) -> (SessionScope, SessionScope, EncryptedPayload, Delivery) {
    let a = apply(&p.alice, conversation(&p.a)).await;
    let b = apply(&p.bob, conversation(&p.b)).await;
    p.alice
        .bootstrap(a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let text = payload(&a, "automatic session confirmation");
    let wire = wire(p.alice.encrypt(a.clone(), text.clone()).await.unwrap());
    assert_eq!(
        wire.envelope.as_ref().unwrap().message_type,
        CiphertextType::PrekeySignal as i32
    );
    (a, b, text, wire)
}

#[tokio::test]
async fn confirmation_changes_next_sender_message_to_signal_and_never_loops() {
    let p = pair(true, false).await;
    let (a, b, text, initial) = first(&p).await;
    let (received, proof) = p
        .bob
        .receive_for_delivery(b.clone(), initial.clone())
        .await
        .unwrap();
    assert_eq!(received, text);
    assert_eq!(proof.request().messages[0].disposition, 1);
    let reply = one(&p.bob).await;
    let response = reply.request().envelope.clone().unwrap();
    assert_ne!(response.message_id, text.message_id);
    assert_eq!(response.message_type, CiphertextType::Signal as i32);
    let (reply_payload, _) = p
        .alice
        .receive_for_delivery(a.clone(), wire(response.clone()))
        .await
        .unwrap();
    assert!(
        matches!(reply_payload.content, Some(encrypted_payload::Content::SessionConfirmation(ref value)) if value.in_reply_to_message_id == text.message_id)
    );
    // No confirmation of a confirmation; Alice still has only the original text.
    assert_eq!(p.alice.outbox_page(None).await.unwrap().messages.len(), 1);
    let next = payload(&a, "already confirmed before Bob typed anything");
    let next_envelope = p.alice.encrypt(a.clone(), next).await.unwrap();
    assert_eq!(next_envelope.message_type, CiphertextType::Signal as i32);
    let before = one(&p.bob).await.request().clone();
    p.bob
        .receive_for_delivery(b.clone(), wire(next_envelope))
        .await
        .unwrap();
    assert_eq!(one(&p.bob).await.request(), &before);
    // Replaying the first delivery must not advance the ratchet or re-encrypt.
    p.bob
        .receive_for_delivery(b.clone(), initial.clone())
        .await
        .unwrap();
    assert_eq!(one(&p.bob).await.request(), &before);
    p.bob.shutdown().await.unwrap();
    let before_session = session(p.root.path(), &b);
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    bob.receive_for_delivery(b.clone(), initial).await.unwrap();
    assert_eq!(one(&bob).await.request(), &before);
    bob.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &b), before_session);
    assert!(
        saved(
            p.root.path(),
            &b,
            RecordKind::Control,
            &format!("session-confirmation:{}", text.message_id)
        )
        .is_some()
    );
    p.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn several_prekeys_each_have_one_stable_reply_even_when_received_again() {
    let p = pair(true, false).await;
    let (_, b, _, first) = first(&p).await;
    let mut messages = vec![first];
    for i in 0..2 {
        let text = payload(&p.a, &format!("pending prekey {i}"));
        let envelope = p.alice.encrypt(p.a.clone(), text).await.unwrap();
        assert_eq!(envelope.message_type, CiphertextType::PrekeySignal as i32);
        messages.push(wire(envelope));
    }
    for delivery in &messages {
        p.bob
            .receive_for_delivery(b.clone(), delivery.clone())
            .await
            .unwrap();
    }
    let original = p
        .bob
        .outbox_page(None)
        .await
        .unwrap()
        .messages
        .into_iter()
        .map(|p| p.request().clone())
        .collect::<Vec<_>>();
    assert_eq!(original.len(), 3);
    for delivery in messages.into_iter().rev() {
        p.bob
            .receive_for_delivery(b.clone(), delivery)
            .await
            .unwrap();
    }
    let replay = p
        .bob
        .outbox_page(None)
        .await
        .unwrap()
        .messages
        .into_iter()
        .map(|p| p.request().clone())
        .collect::<Vec<_>>();
    assert_eq!(original, replay);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn even_prekey_confirmation_does_not_trigger_a_reply() {
    let p = pair(true, false).await;
    let a = apply(&p.alice, conversation(&p.a)).await;
    let b = apply(&p.bob, conversation(&p.b)).await;
    p.alice
        .bootstrap(a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let mut payload = payload(&a, "unused");
    payload.content = Some(encrypted_payload::Content::SessionConfirmation(
        SessionConfirmation {
            in_reply_to_message_id: Uuid::new_v4().to_string(),
        },
    ));
    let envelope = p.alice.encrypt(a, payload).await.unwrap();
    assert_eq!(envelope.message_type, CiphertextType::PrekeySignal as i32);
    p.bob.receive_for_delivery(b, wire(envelope)).await.unwrap();
    assert!(p.bob.outbox_page(None).await.unwrap().messages.is_empty());
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn cached_active_scope_observes_closure_and_only_receives_without_confirmation() {
    let p = pair(true, false).await;
    let (_, b, _, delivery) = first(&p).await;
    let mut value = conversation(&b);
    value.state = ConversationState::Closed as i32;
    value.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    value.closed_generation_id = AG.into();
    value.closed_at_unix_ms = 1_700_000_000_000;
    value.peer_device.as_mut().unwrap().public_material_ready = false;
    apply(&p.bob, value).await;
    let (payload, proof) = p
        .bob
        .receive_for_delivery(b.clone(), delivery)
        .await
        .unwrap();
    assert_eq!(proof.request().messages[0].disposition, 1);
    assert!(p.bob.outbox_page(None).await.unwrap().messages.is_empty());
    p.bob.shutdown().await.unwrap();
    assert!(
        saved(
            p.root.path(),
            &b,
            RecordKind::Control,
            &format!("session-confirmation:{}", payload.message_id)
        )
        .is_none()
    );
    assert!(session(p.root.path(), &b).is_some());
    p.alice.shutdown().await.unwrap();
}

fn commit_fault(root: &std::path::Path, scope: &SessionScope, id: &str, install: bool) {
    let path = StoreScope::new(SERVER, &scope.own.user_id)
        .unwrap()
        .database_path(root);
    let db = rusqlite::Connection::open(path).unwrap();
    if install {
        let id = format!("session-confirmation:{id}");
        db.execute_batch(&format!("CREATE TABLE confirmation_parent(id INTEGER PRIMARY KEY);CREATE TABLE confirmation_child(id INTEGER REFERENCES confirmation_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER confirmation_insert AFTER INSERT ON protected_records WHEN NEW.kind=13 AND NEW.record_id='{id}' BEGIN INSERT INTO confirmation_child VALUES(1); END;")).unwrap();
    } else {
        db.execute_batch("DROP TRIGGER confirmation_insert;DROP TABLE confirmation_child;DROP TABLE confirmation_parent;").unwrap();
    }
}
#[tokio::test]
async fn failed_confirmation_commit_rolls_back_incoming_ratchet_prekeys_ack_and_reply() {
    let p = pair(true, false).await;
    let (_, b, text, delivery) = first(&p).await;
    commit_fault(p.root.path(), &b, &text.message_id, true);
    assert!(matches!(
        p.bob
            .receive_for_delivery(b.clone(), delivery.clone())
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert!(p.bob.outbox_page(None).await.unwrap().messages.is_empty());
    p.bob.shutdown().await.unwrap();
    for kind in [RecordKind::Inbox, RecordKind::History] {
        assert!(saved(p.root.path(), &b, kind, &text.message_id).is_none());
    }
    assert!(session(p.root.path(), &b).is_none());
    assert!(
        saved(
            p.root.path(),
            &b,
            RecordKind::Control,
            &format!("session-confirmation:{}", text.message_id)
        )
        .is_none()
    );
    commit_fault(p.root.path(), &b, &text.message_id, false);
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert_eq!(bob.receive_for_delivery(b, delivery).await.unwrap().0, text);
    one(&bob).await;
    bob.shutdown().await.unwrap();
    p.alice.shutdown().await.unwrap();
}

#[derive(Message)]
struct LinkFixture {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    message_id: String,
}
#[tokio::test]
async fn invalid_or_missing_link_target_fails_closed_without_regenerating_reply() {
    for invalid in [
        vec![255],
        LinkFixture {
            format: 2,
            message_id: Uuid::new_v4().to_string(),
        }
        .encode_to_vec(),
        LinkFixture {
            format: 1,
            message_id: Uuid::new_v4().to_string(),
        }
        .encode_to_vec(),
    ] {
        let p = pair(true, false).await;
        let (_, b, text, delivery) = first(&p).await;
        p.bob
            .receive_for_delivery(b.clone(), delivery.clone())
            .await
            .unwrap();
        let original = one(&p.bob).await.request().clone();
        p.bob.shutdown().await.unwrap();
        let before = session(p.root.path(), &b);
        store(p.root.path(), B, false)
            .transaction(|tx| tx.put(&link(&b, &text.message_id), &invalid))
            .unwrap();
        let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
            .await
            .unwrap();
        assert!(bob.receive_for_delivery(b.clone(), delivery).await.is_err());
        assert_eq!(one(&bob).await.request(), &original);
        bob.shutdown().await.unwrap();
        assert_eq!(session(p.root.path(), &b), before);
        p.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn deleted_required_link_blocks_receive_and_ack_recovery_without_new_uuid() {
    let p = pair(true, false).await;
    let (_, b, text, delivery) = first(&p).await;
    p.bob
        .receive_for_delivery(b.clone(), delivery.clone())
        .await
        .unwrap();
    let original = one(&p.bob).await.request().clone();
    p.bob.shutdown().await.unwrap();
    let before = session(p.root.path(), &b);
    let path = StoreScope::new(SERVER, B)
        .unwrap()
        .database_path(p.root.path());
    let db = rusqlite::Connection::open(path).unwrap();
    assert_eq!(
        db.execute(
            "DELETE FROM protected_records WHERE kind=13 AND record_id=?1",
            [format!("session-confirmation:{}", text.message_id)]
        )
        .unwrap(),
        1
    );
    drop(db);
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert!(matches!(
        bob.receive_for_delivery(b.clone(), delivery).await,
        Err(CryptoError::Invalid)
    ));
    assert!(matches!(
        bob.pending_acknowledgements(None).await,
        Err(CryptoError::Invalid)
    ));
    assert_eq!(one(&bob).await.request(), &original);
    bob.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &b), before);
    p.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn existing_link_cannot_point_to_the_reply_for_another_incoming_message() {
    let p = pair(true, false).await;
    let (_, b, text, initial) = first(&p).await;
    p.bob
        .receive_for_delivery(b.clone(), initial.clone())
        .await
        .unwrap();
    let original = one(&p.bob)
        .await
        .request()
        .envelope
        .clone()
        .unwrap()
        .message_id;
    let next = p
        .alice
        .encrypt(p.a.clone(), payload(&p.a, "another pending prekey"))
        .await
        .unwrap();
    p.bob
        .receive_for_delivery(b.clone(), wire(next))
        .await
        .unwrap();
    let other = p
        .bob
        .outbox_page(None)
        .await
        .unwrap()
        .messages
        .into_iter()
        .map(|p| p.request().envelope.as_ref().unwrap().message_id.clone())
        .find(|id| *id != original)
        .unwrap();
    p.bob.shutdown().await.unwrap();
    store(p.root.path(), B, false)
        .transaction(|tx| {
            tx.put(
                &link(&b, &text.message_id),
                &LinkFixture {
                    format: 1,
                    message_id: other,
                }
                .encode_to_vec(),
            )
        })
        .unwrap();
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert!(matches!(
        bob.receive_for_delivery(b, initial).await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        bob.pending_acknowledgements(None).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(bob.outbox_page(None).await.unwrap().messages.len(), 2);
    bob.shutdown().await.unwrap();
    p.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn prior_plain_receive_journal_is_compatible_and_confirmation_is_added_once() {
    let p = pair(true, false).await;
    let (_, b, _, initial) = first(&p).await;
    p.bob
        .receive_delivery(b.clone(), initial.clone())
        .await
        .unwrap();
    assert!(p.bob.outbox_page(None).await.unwrap().messages.is_empty());
    p.bob
        .receive_for_delivery(b.clone(), initial.clone())
        .await
        .unwrap();
    let original = one(&p.bob).await.request().clone();
    p.bob.receive_for_delivery(b, initial).await.unwrap();
    assert_eq!(one(&p.bob).await.request(), &original);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

fn crash(root: &std::path::Path, after: bool) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "encryption::signal_state::tests::confirmation::confirmation_crash_worker",
            "--nocapture",
        ])
        .env("ZCHAT_CONFIRMATION_CHILD_ROOT", root)
        .env(
            "ZCHAT_CONFIRMATION_CHILD_AFTER",
            if after { "yes" } else { "no" },
        )
        .output()
        .unwrap()
}
#[tokio::test]
async fn process_exit_before_and_after_commit_retains_none_or_complete_confirmation() {
    let p = pair(true, false).await;
    let (_, b, text, delivery) = first(&p).await;
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    std::fs::write(
        p.root.path().join("confirmation-conversation.bin"),
        conversation(&b).encode_to_vec(),
    )
    .unwrap();
    std::fs::write(
        p.root.path().join("confirmation-delivery.bin"),
        delivery.encode_to_vec(),
    )
    .unwrap();
    assert_eq!(crash(p.root.path(), false).status.code(), Some(73));
    assert!(session(p.root.path(), &b).is_none());
    for kind in [RecordKind::Inbox, RecordKind::History] {
        assert!(saved(p.root.path(), &b, kind, &text.message_id).is_none());
    }
    assert!(
        saved(
            p.root.path(),
            &b,
            RecordKind::Control,
            &format!("session-confirmation:{}", text.message_id)
        )
        .is_none()
    );
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert!(bob.outbox_page(None).await.unwrap().messages.is_empty());
    assert!(
        bob.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    bob.shutdown().await.unwrap();
    assert_eq!(crash(p.root.path(), true).status.code(), Some(74));
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    let before = one(&bob).await.request().clone();
    assert_eq!(
        bob.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    let (payload, _) = bob.receive_for_delivery(b.clone(), delivery).await.unwrap();
    assert_eq!(payload, text);
    assert_eq!(one(&bob).await.request(), &before);
    bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &b).is_some());
    assert_eq!(
        saved(p.root.path(), &b, RecordKind::History, &text.message_id).unwrap(),
        text.encode_to_vec()
    );
}
#[test]
fn confirmation_crash_worker() {
    let Ok(root) = std::env::var("ZCHAT_CONFIRMATION_CHILD_ROOT") else {
        return;
    };
    let root = std::path::Path::new(&root);
    let after = std::env::var("ZCHAT_CONFIRMATION_CHILD_AFTER").unwrap() == "yes";
    let value = Conversation::decode(
        std::fs::read(root.join("confirmation-conversation.bin"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let delivery = Delivery::decode(
        std::fs::read(root.join("confirmation-delivery.bin"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let own = value.own_device.unwrap();
    let mut store = store(root, &own.user_id, false);
    let id = &delivery.envelope.as_ref().unwrap().message_id;
    store.test_exit_on_record(
        RecordKey::new(
            &own.generation_id,
            RecordKind::Control,
            &format!("session-confirmation:{id}"),
        )
        .unwrap(),
        after,
    );
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let worker = CryptoCoordinator::start(store, own).await.unwrap();
            let scope = worker.conversation_scope(CHAT.into()).await.unwrap();
            worker.receive_for_delivery(scope, delivery).await.unwrap();
        });
    panic!("child must exit at the confirmation transaction boundary")
}
