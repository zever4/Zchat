//! Authenticated Get/Create convergence and managed scopes, with real Signal.
use super::*;
use crate::encryption::contract::envelope_digest;
use crate::proto::e2ee::{ConversationCloseReason, Delivery};

fn value(scope: &SessionScope) -> Conversation {
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
fn closed(scope: &SessionScope) -> Conversation {
    let mut result = value(scope);
    result.state = ConversationState::Closed as i32;
    result.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    result.closed_generation_id = scope.peer.generation_id.clone();
    result.closed_at_unix_ms = 1_700_000_000_000;
    result.peer_device.as_mut().unwrap().public_material_ready = false;
    result
}
async fn apply(
    worker: &CryptoCoordinator,
    scope: &SessionScope,
    response: Conversation,
) -> CryptoResult<Conversation> {
    let attempt = worker
        .prepare_conversation_lookup(scope.conversation.clone())
        .await?;
    worker
        .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(attempt, response))
        .await
}
fn wire(envelope: Envelope) -> Delivery {
    Delivery {
        sequence: 1,
        expires_at_unix_ms: 1_700_000_000_000,
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        envelope: Some(envelope),
    }
}
fn index_key(scope: &SessionScope) -> RecordKey {
    scope
        .key(
            RecordKind::Control,
            &format!("conversation:{}", scope.conversation),
        )
        .unwrap()
}
fn fault(root: &std::path::Path, scope: &SessionScope, install: bool) {
    let path = StoreScope::new(SERVER, &scope.own.user_id)
        .unwrap()
        .database_path(root);
    let db = rusqlite::Connection::open(path).unwrap();
    if install {
        let id = format!("conversation:{}", scope.conversation);
        db.execute_batch(&format!("CREATE TABLE lifecycle_parent(id INTEGER PRIMARY KEY);CREATE TABLE lifecycle_child(id INTEGER REFERENCES lifecycle_parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER lifecycle_insert AFTER INSERT ON protected_records WHEN NEW.kind=13 AND NEW.record_id='{id}' BEGIN INSERT INTO lifecycle_child VALUES(1); END;
        CREATE TRIGGER lifecycle_update AFTER UPDATE ON protected_records WHEN NEW.kind=13 AND NEW.record_id='{id}' BEGIN INSERT INTO lifecycle_child VALUES(1); END;")).unwrap();
    } else {
        db.execute_batch("DROP TRIGGER lifecycle_insert;DROP TRIGGER lifecycle_update;DROP TABLE lifecycle_child;DROP TABLE lifecycle_parent;").unwrap();
    }
}

#[tokio::test]
async fn get_only_closure_blocks_cached_scope_send_bootstrap_acquisition_and_renewal() {
    let p = pair(true, false).await;
    let active = value(&p.a);
    apply(&p.alice, &p.a, active.clone()).await.unwrap();
    let scope = p.alice.conversation_scope(CHAT.into()).await.unwrap();
    assert!(scope.store_scope.is_some());
    p.alice
        .bootstrap(scope.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let text = payload(&p.a, "preserve ratchet and exact outbox after closure");
    let env = p.alice.encrypt(scope.clone(), text.clone()).await.unwrap();
    let pending = p.alice.prepare_acquisition(scope.clone()).await.unwrap();
    let expired = ExpiredAcquisition::test_proof(pending.request);
    let attempt = p
        .alice
        .prepare_conversation_lookup(CHAT.into())
        .await
        .unwrap();
    let delayed = VerifiedConversationLookup::from_rpc(attempt, active);
    let expected = closed(&p.a);
    apply(&p.alice, &p.a, expected.clone()).await.unwrap();
    assert_eq!(
        p.alice.accept_conversation_lookup(delayed).await.unwrap(),
        expected
    );
    assert!(matches!(
        p.alice
            .encrypt(scope.clone(), payload(&p.a, "blocked"))
            .await,
        Err(CryptoError::ConversationClosed)
    ));
    assert!(matches!(
        p.alice.prepare_acquisition(scope.clone()).await,
        Err(CryptoError::ConversationClosed)
    ));
    assert!(matches!(
        p.alice.bootstrap(scope.clone(), p.acquisition).await,
        Err(CryptoError::ConversationClosed)
    ));
    assert!(matches!(
        p.alice.renew_acquisition(scope, expired).await,
        Err(CryptoError::ConversationClosed)
    ));
    let receive_only = p.alice.conversation_scope(CHAT.into()).await.unwrap();
    assert!(receive_only.receive_only);
    assert!(matches!(
        p.alice
            .encrypt(receive_only, payload(&p.a, "also blocked"))
            .await,
        Err(CryptoError::ConversationClosed)
    ));
    assert_eq!(
        p.alice
            .outgoing_plan(text.message_id)
            .await
            .unwrap()
            .request()
            .envelope
            .as_ref(),
        Some(&env)
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), p.a.own)
        .await
        .unwrap();
    assert!(
        alice
            .conversation_scope(CHAT.into())
            .await
            .unwrap()
            .receive_only
    );
    alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn closed_peer_first_prekey_message_commits_history_and_ack_then_replays_after_restart() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(
        &p.a,
        "first prekey message delivered after sender retirement",
    );
    let envelope = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    assert_eq!(envelope.message_type, CiphertextType::PrekeySignal as i32);
    apply(&p.bob, &p.b, closed(&p.b)).await.unwrap();
    let receive_only = p.bob.conversation_scope(CHAT.into()).await.unwrap();
    assert!(receive_only.receive_only);
    let delivery = wire(envelope.clone());
    let (received, proof) = p
        .bob
        .receive_delivery(receive_only.clone(), delivery.clone())
        .await
        .unwrap();
    assert_eq!(received, text);
    assert!(matches!(
        p.bob
            .encrypt(receive_only, payload(&p.b, "no reply to retired peer"))
            .await,
        Err(CryptoError::ConversationClosed)
    ));
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.b).is_some());
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1],
        1
    );
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::PqPreKey, &PQ.to_string()).unwrap()[1],
        1
    );
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own)
        .await
        .unwrap();
    let scope = bob.conversation_scope(CHAT.into()).await.unwrap();
    assert!(scope.receive_only);
    let (again, recovered) = bob.receive_delivery(scope, delivery).await.unwrap();
    assert_eq!(again, text);
    assert_eq!(recovered.request(), proof.request());
    bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn create_and_get_share_closure_and_explicit_new_dialogue_cas() {
    for create_first in [false, true] {
        let p = pair(true, false).await;
        let attempt = p.alice.prepare_conversation_creation(42).await.unwrap();
        let original = value(&p.a);
        if create_first {
            p.alice
                .accept_conversation_creation(VerifiedConversationCreation::from_rpc(
                    attempt.clone(),
                    original.clone(),
                ))
                .await
                .unwrap();
        }
        let expected = closed(&p.a);
        apply(&p.alice, &p.a, expected.clone()).await.unwrap();
        assert_eq!(
            p.alice
                .accept_conversation_creation(VerifiedConversationCreation::from_rpc(
                    attempt.clone(),
                    original.clone()
                ))
                .await
                .unwrap(),
            expected
        );
        let next = p
            .alice
            .prepare_new_conversation_creation(42, attempt.request().creation_request_id.clone())
            .await
            .unwrap();
        assert_ne!(
            next.request().creation_request_id,
            attempt.request().creation_request_id
        );
        assert!(matches!(
            p.alice
                .accept_conversation_creation(VerifiedConversationCreation::from_rpc(
                    attempt, original
                ))
                .await,
            Err(CryptoError::Conflict)
        ));
        assert!(
            p.alice
                .conversation_scope(CHAT.into())
                .await
                .unwrap()
                .receive_only
        );
        p.alice.shutdown().await.unwrap();
        p.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn lookup_detects_binding_identity_registration_and_closure_event_changes() {
    let p = pair(true, false).await;
    let original = value(&p.b);
    apply(&p.bob, &p.b, original.clone()).await.unwrap();
    for change in 0..6 {
        let mut bad = closed(&p.b);
        let peer = bad.peer_device.as_mut().unwrap();
        match change {
            0 => {
                peer.identity_key = IdentityKeyPair::generate(&mut rand::rng())
                    .public_key()
                    .serialize()
                    .to_vec()
            }
            1 => peer.registration_id += 1,
            2 => peer.reference.as_mut().unwrap().generation_id = Uuid::new_v4().to_string(),
            3 => peer.reference.as_mut().unwrap().user_id = Uuid::new_v4().to_string(),
            4 => bad.own_device.as_mut().unwrap().generation_id = Uuid::new_v4().to_string(),
            _ => bad.conversation_id = Uuid::new_v4().to_string(),
        };
        assert!(apply(&p.bob, &p.b, bad).await.is_err());
    }
    let expected = closed(&p.b);
    apply(&p.bob, &p.b, expected.clone()).await.unwrap();
    let mut conflict = expected;
    conflict.closed_at_unix_ms += 1;
    assert!(matches!(
        apply(&p.bob, &p.b, conflict).await,
        Err(CryptoError::Conflict)
    ));
    assert!(
        p.bob
            .conversation_scope(CHAT.into())
            .await
            .unwrap()
            .receive_only
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn first_get_index_respects_a_signal_identity_pinned_before_the_index_existed() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "establish pin without a lookup journal");
    let env = p.alice.encrypt(p.a.clone(), text).await.unwrap();
    p.bob.decrypt(p.b.clone(), env).await.unwrap();
    let mut wrong = value(&p.b);
    wrong.peer_device.as_mut().unwrap().identity_key = IdentityKeyPair::generate(&mut rand::rng())
        .public_key()
        .serialize()
        .to_vec();
    assert!(matches!(
        apply(&p.bob, &p.b, wrong).await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        p.bob.conversation_scope(CHAT.into()).await,
        Err(CryptoError::ConversationUnavailable)
    ));
    apply(&p.bob, &p.b, closed(&p.b)).await.unwrap();
    assert!(
        p.bob
            .conversation_scope(CHAT.into())
            .await
            .unwrap()
            .receive_only
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn own_generation_closure_never_yields_a_receiving_scope_or_new_crypto_state() {
    let p = pair(true, false).await;
    apply(&p.bob, &p.b, value(&p.b)).await.unwrap();
    let cached = p.bob.conversation_scope(CHAT.into()).await.unwrap();
    let mut retired = closed(&p.b);
    retired.closed_generation_id = p.b.own.generation_id.clone();
    apply(&p.bob, &p.b, retired).await.unwrap();
    assert!(matches!(
        p.bob.conversation_scope(CHAT.into()).await,
        Err(CryptoError::ArchivedGeneration)
    ));
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let env = p
        .alice
        .encrypt(
            p.a.clone(),
            payload(&p.a, "retired recipient must not receive"),
        )
        .await
        .unwrap();
    assert!(matches!(
        p.bob
            .receive_delivery(cached.clone(), wire(env.clone()))
            .await,
        Err(CryptoError::ArchivedGeneration)
    ));
    assert!(matches!(
        p.bob
            .quarantine_delivery(
                cached.clone(),
                wire(env),
                QuarantineReason::InvalidCiphertext
            )
            .await,
        Err(CryptoError::ArchivedGeneration)
    ));
    assert!(matches!(
        p.bob
            .encrypt(cached, payload(&p.b, "no retired sender"))
            .await,
        Err(CryptoError::ArchivedGeneration)
    ));
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.b).is_none());
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1],
        0
    );
}

#[tokio::test]
async fn missing_or_corrupt_index_blocks_cached_managed_scope_without_repair() {
    for corrupt in [false, true] {
        let p = pair(true, false).await;
        apply(&p.bob, &p.b, value(&p.b)).await.unwrap();
        let cached = p.bob.conversation_scope(CHAT.into()).await.unwrap();
        p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
        let text = payload(&p.a, "no index repair on receive");
        let env = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
        p.bob.shutdown().await.unwrap();
        if corrupt {
            let mut db = store(p.root.path(), B, false);
            db.transaction(|tx| tx.put(&index_key(&p.b), &[1])).unwrap();
        } else {
            let db = rusqlite::Connection::open(
                StoreScope::new(SERVER, B)
                    .unwrap()
                    .database_path(p.root.path()),
            )
            .unwrap();
            db.execute(
                "DELETE FROM protected_records WHERE kind=13 AND record_id=?1",
                [format!("conversation:{CHAT}")],
            )
            .unwrap();
        }
        let bob = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own.clone())
            .await
            .unwrap();
        assert!(bob.receive_delivery(cached, wire(env)).await.is_err());
        assert!(bob.conversation_scope(CHAT.into()).await.is_err());
        bob.shutdown().await.unwrap();
        p.alice.shutdown().await.unwrap();
        assert!(saved(p.root.path(), &p.b, RecordKind::Inbox, &text.message_id).is_none());
        assert!(session(p.root.path(), &p.b).is_none());
    }
}

#[tokio::test]
async fn lookup_proof_and_managed_scope_cannot_cross_server_account_or_generation() {
    let p = pair(true, false).await;
    let attempt = p
        .alice
        .prepare_conversation_lookup(CHAT.into())
        .await
        .unwrap();
    assert!(
        p.bob
            .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(
                attempt.clone(),
                value(&p.a)
            ))
            .await
            .is_err()
    );
    let root = tempfile::tempdir().unwrap();
    let foreign = CryptoCoordinator::start(
        test_open(
            root.path(),
            StoreScope::new("https://other.example.test", A).unwrap(),
            true,
        ),
        p.a.own.clone(),
    )
    .await
    .unwrap();
    assert!(
        foreign
            .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(
                attempt.clone(),
                value(&p.a)
            ))
            .await
            .is_err()
    );
    apply(&p.alice, &p.a, value(&p.a)).await.unwrap();
    let managed = p.alice.conversation_scope(CHAT.into()).await.unwrap();
    assert!(matches!(
        foreign.prepare_acquisition(managed).await,
        Err(CryptoError::Invalid)
    ));
    foreign.shutdown().await.unwrap();
    p.alice.shutdown().await.unwrap();
    let replacement = CryptoCoordinator::start(
        store(p.root.path(), A, false),
        device(A, &Uuid::new_v4().to_string()),
    )
    .await
    .unwrap();
    assert!(
        replacement
            .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(attempt, value(&p.a)))
            .await
            .is_err()
    );
    replacement.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn lookup_and_closure_commit_failures_leave_no_false_index_and_preserve_keys() {
    let p = pair(true, false).await;
    let attempt = p
        .bob
        .prepare_conversation_lookup(CHAT.into())
        .await
        .unwrap();
    fault(p.root.path(), &p.b, true);
    assert!(
        p.bob
            .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(
                attempt.clone(),
                value(&p.b)
            ))
            .await
            .is_err()
    );
    assert!(matches!(
        p.bob.conversation_scope(CHAT.into()).await,
        Err(CryptoError::ConversationUnavailable)
    ));
    fault(p.root.path(), &p.b, false);
    p.bob
        .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(attempt, value(&p.b)))
        .await
        .unwrap();
    fault(p.root.path(), &p.b, true);
    assert!(apply(&p.bob, &p.b, closed(&p.b)).await.is_err());
    assert!(
        !p.bob
            .conversation_scope(CHAT.into())
            .await
            .unwrap()
            .receive_only
    );
    fault(p.root.path(), &p.b, false);
    apply(&p.bob, &p.b, closed(&p.b)).await.unwrap();
    assert!(
        p.bob
            .conversation_scope(CHAT.into())
            .await
            .unwrap()
            .receive_only
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.b).is_none());
}

#[tokio::test]
async fn lookup_process_exit_before_and_after_commit_recovers_the_same_index_and_keys() {
    let p = pair(true, false).await;
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let before = saved(p.root.path(), &p.b, RecordKind::Identity, "local").unwrap();
    std::fs::write(
        p.root.path().join("lookup-conversation.bin"),
        closed(&p.b).encode_to_vec(),
    )
    .unwrap();
    for after in [false, true] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::tests::lifecycle::lookup_crash_worker",
                "--nocapture",
            ])
            .env("ZCHAT_LOOKUP_CHILD_ROOT", p.root.path())
            .env("ZCHAT_LOOKUP_CHILD_AFTER", if after { "yes" } else { "no" })
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(if after { 74 } else { 73 }));
        let worker = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own.clone())
            .await
            .unwrap();
        let scope = worker.conversation_scope(CHAT.into()).await;
        if after {
            assert!(scope.unwrap().receive_only)
        } else {
            assert!(matches!(scope, Err(CryptoError::ConversationUnavailable)))
        }
        worker.shutdown().await.unwrap();
        assert_eq!(
            saved(p.root.path(), &p.b, RecordKind::Identity, "local").unwrap(),
            before
        );
    }
}

#[test]
fn lookup_crash_worker() {
    let Ok(root) = std::env::var("ZCHAT_LOOKUP_CHILD_ROOT") else {
        return;
    };
    let root = std::path::Path::new(&root);
    let after = std::env::var("ZCHAT_LOOKUP_CHILD_AFTER").unwrap() == "yes";
    let value = Conversation::decode(
        std::fs::read(root.join("lookup-conversation.bin"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let own = value.own_device.clone().unwrap();
    let mut store = store(root, &own.user_id, false);
    store.test_exit_on_record(
        RecordKey::new(
            &own.generation_id,
            RecordKind::Control,
            &format!("conversation:{}", value.conversation_id),
        )
        .unwrap(),
        after,
    );
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let worker = CryptoCoordinator::start(store, own).await.unwrap();
            let attempt = worker
                .prepare_conversation_lookup(value.conversation_id.clone())
                .await
                .unwrap();
            worker
                .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(attempt, value))
                .await
                .unwrap();
        });
    panic!("fixture did not exit at COMMIT");
}
