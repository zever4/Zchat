use super::*;
use crate::encryption::native::ExpiredAcquisition;

#[tokio::test]
async fn renewal_cas_is_idempotent_after_concurrent_retry_and_reopen() {
    let p = pair(true, false).await;
    let old = p
        .alice
        .prepare_acquisition(p.a.clone())
        .await
        .unwrap()
        .request;
    let proof = ExpiredAcquisition::test_proof(old.clone());
    let (first, duplicate) = tokio::join!(
        p.alice.renew_acquisition(p.a.clone(), proof.clone()),
        p.alice.renew_acquisition(p.a.clone(), proof.clone()),
    );
    let fresh = first.unwrap();
    assert_eq!(fresh.request, duplicate.unwrap().request);
    assert_ne!(fresh.request.acquisition_id, old.acquisition_id);
    assert!(!fresh.applied);
    p.alice.shutdown().await.unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert_eq!(
        alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .request,
        fresh.request
    );
    assert_eq!(
        alice
            .renew_acquisition(p.a.clone(), proof)
            .await
            .unwrap()
            .request,
        fresh.request
    );
    // A delayed response to the old RPC cannot bootstrap the replacement plan.
    assert!(matches!(
        alice.bootstrap(p.a.clone(), p.acquisition.clone()).await,
        Err(CryptoError::Conflict)
    ));
    let mut bundle = p.acquisition;
    bundle.acquisition_id = fresh.request.acquisition_id;
    alice.bootstrap(p.a.clone(), bundle).await.unwrap();
    let message = payload(&p.a, "fresh plan boots once");
    let encrypted = alice.encrypt(p.a, message.clone()).await.unwrap();
    assert_eq!(p.bob.decrypt(p.b, encrypted).await.unwrap(), message);
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn renewal_rejects_stale_and_foreign_expiry_without_changing_current_plan() {
    let p = pair(true, false).await;
    let old = p
        .alice
        .prepare_acquisition(p.a.clone())
        .await
        .unwrap()
        .request;
    let first = p
        .alice
        .renew_acquisition(p.a.clone(), ExpiredAcquisition::test_proof(old.clone()))
        .await
        .unwrap();
    let second = p
        .alice
        .renew_acquisition(p.a.clone(), ExpiredAcquisition::test_proof(first.request))
        .await
        .unwrap();
    assert!(matches!(
        p.alice
            .renew_acquisition(p.a.clone(), ExpiredAcquisition::test_proof(old))
            .await,
        Err(CryptoError::Conflict)
    ));
    for mutation in 0..4 {
        let mut bad = second.request.clone();
        match mutation {
            0 => bad.conversation_id = Uuid::new_v4().to_string(),
            1 => bad.expected_peer.as_mut().unwrap().generation_id = Uuid::new_v4().to_string(),
            2 => bad.context.as_mut().unwrap().device_generation_id = Uuid::new_v4().to_string(),
            _ => bad.acquisition_id = Uuid::new_v4().to_string(),
        }
        assert!(
            p.alice
                .renew_acquisition(p.a.clone(), ExpiredAcquisition::test_proof(bad))
                .await
                .is_err()
        );
    }
    assert_eq!(
        p.alice.prepare_acquisition(p.a).await.unwrap().request,
        second.request
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn renewal_cannot_rewind_applied_session_or_uncertain_outbox() {
    let p = pair(true, false).await;
    let old = p
        .alice
        .prepare_acquisition(p.a.clone())
        .await
        .unwrap()
        .request;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let message = payload(&p.a, "ciphertext may already be in flight");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    let path = StoreScope::new(SERVER, A)
        .unwrap()
        .database_path(p.root.path());
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        p.alice
            .renew_acquisition(p.a.clone(), ExpiredAcquisition::test_proof(old))
            .await,
        Err(CryptoError::SessionExists)
    ));
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(
        p.alice.encrypt(p.a, message.clone()).await.unwrap(),
        envelope
    );
    assert_eq!(p.bob.decrypt(p.b, envelope).await.unwrap(), message);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_session_wins_over_an_expiry_response_in_flight() {
    let p = pair(true, false).await;
    let pending = p.bob.prepare_acquisition(p.b.clone()).await.unwrap();
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let message = payload(&p.a, "inbound prekey creates a session before renewal");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    p.bob.decrypt(p.b.clone(), envelope.clone()).await.unwrap();
    let path = StoreScope::new(SERVER, B)
        .unwrap()
        .database_path(p.root.path());
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        p.bob
            .renew_acquisition(p.b.clone(), ExpiredAcquisition::test_proof(pending.request))
            .await,
        Err(CryptoError::SessionExists)
    ));
    assert_eq!(std::fs::read(path).unwrap(), before);
    assert_eq!(p.bob.decrypt(p.b.clone(), envelope).await.unwrap(), message);
    let reply = payload(&p.b, "the inbound ratchet survives");
    let envelope = p.bob.encrypt(p.b, reply.clone()).await.unwrap();
    assert_eq!(p.alice.decrypt(p.a, envelope).await.unwrap(), reply);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_session_with_saved_messages_blocks_renewal_even_without_a_receipt() {
    for kind in [RecordKind::Outbox, RecordKind::Inbox, RecordKind::History] {
        let p = pair(true, false).await;
        let old = p
            .alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .request;
        p.alice.shutdown().await.unwrap();
        let message = payload(&p.a, "partial/restored state must not be overwritten");
        let data = if kind == RecordKind::History {
            message.encode_to_vec()
        } else {
            p.a.envelope(&message.message_id, vec![1], CiphertextType::Signal)
                .encode_to_vec()
        };
        store(p.root.path(), A, false)
            .transaction(|tx| tx.put(&p.a.key(kind, &message.message_id)?, &data))
            .unwrap();
        let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
            .await
            .unwrap();
        assert!(matches!(
            alice
                .renew_acquisition(p.a.clone(), ExpiredAcquisition::test_proof(old.clone()))
                .await,
            Err(CryptoError::Conflict | CryptoError::Invalid)
        ));
        assert!(alice.prepare_acquisition(p.a).await.is_err());
        alice.shutdown().await.unwrap();
        p.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn unrelated_conversation_messages_do_not_block_renewal() {
    let p = pair(true, false).await;
    let old = p
        .alice
        .prepare_acquisition(p.a.clone())
        .await
        .unwrap()
        .request;
    let mut other = p.a.clone();
    other.conversation = Uuid::new_v4().to_string();
    let other_request = p.alice.prepare_acquisition(other.clone()).await.unwrap();
    let mut other_bundle = p.acquisition.clone();
    other_bundle.conversation_id = other.conversation.clone();
    other_bundle.acquisition_id = other_request.request.acquisition_id;
    p.alice
        .bootstrap(other.clone(), other_bundle)
        .await
        .unwrap();
    let message = payload(&other, "unrelated conversation");
    p.alice.encrypt(other, message).await.unwrap();
    let alice = p.alice;
    assert!(
        alice
            .renew_acquisition(p.a, ExpiredAcquisition::test_proof(old))
            .await
            .is_ok()
    );
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn renewal_commit_failure_keeps_the_old_request_for_retry() {
    let p = pair(true, false).await;
    let old = p
        .alice
        .prepare_acquisition(p.a.clone())
        .await
        .unwrap()
        .request;
    let path = StoreScope::new(SERVER, A)
        .unwrap()
        .database_path(p.root.path());
    rusqlite::Connection::open(path).unwrap().execute_batch("CREATE TABLE fault_parent(id INTEGER PRIMARY KEY);
        CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER fault_after_crypto AFTER UPDATE ON protected_records WHEN NEW.kind=13
        BEGIN INSERT INTO fault_child VALUES(1); END;").unwrap();
    let proof = ExpiredAcquisition::test_proof(old.clone());
    assert!(matches!(
        p.alice.renew_acquisition(p.a.clone(), proof.clone()).await,
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    assert_eq!(
        p.alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .request,
        old
    );
    commit_fault(p.root.path(), A, RecordKind::Control as u8, false);
    assert_ne!(
        p.alice
            .renew_acquisition(p.a, proof)
            .await
            .unwrap()
            .request
            .acquisition_id,
        old.acquisition_id
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn process_exit_before_and_after_renewal_commit_preserves_exact_request() {
    for after in [false, true] {
        let p = pair(true, false).await;
        let old = p
            .alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .request;
        p.alice.shutdown().await.unwrap();
        p.bob.shutdown().await.unwrap();
        std::fs::write(
            p.root.path().join("renewal-conversation.bin"),
            child_scope(&p.a).encode_to_vec(),
        )
        .unwrap();
        std::fs::write(
            p.root.path().join("renewal-request.bin"),
            old.encode_to_vec(),
        )
        .unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::tests::renewal::renewal_crash_worker",
                "--nocapture",
            ])
            .env("ZCHAT_RENEWAL_TEST_ROOT", p.root.path())
            .env("ZCHAT_RENEWAL_TEST_AFTER", if after { "yes" } else { "no" })
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(if after { 74 } else { 73 }),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let saved = store(p.root.path(), A, false)
            .transaction_with(|tx| load_acquisition(tx, &p.a))
            .unwrap()
            .unwrap();
        assert_eq!(saved.acquisition_id != old.acquisition_id, after);
        let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
            .await
            .unwrap();
        let replay = alice
            .renew_acquisition(p.a.clone(), ExpiredAcquisition::test_proof(old))
            .await
            .unwrap();
        if after {
            assert_eq!(replay.request, saved);
        }
        assert_eq!(
            alice.prepare_acquisition(p.a).await.unwrap().request,
            replay.request
        );
        alice.shutdown().await.unwrap();
    }
}

#[test]
fn renewal_crash_worker() {
    let Some(root) = std::env::var_os("ZCHAT_RENEWAL_TEST_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let scope = SessionScope::from_conversation(
        &Conversation::decode(
            std::fs::read(root.join("renewal-conversation.bin"))
                .unwrap()
                .as_slice(),
        )
        .unwrap(),
    )
    .unwrap();
    let request = AcquireBundleRequest::decode(
        std::fs::read(root.join("renewal-request.bin"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let mut state = store(&root, A, false);
    state.test_exit_on_record(
        scope.acquisition_key().unwrap(),
        std::env::var("ZCHAT_RENEWAL_TEST_AFTER").unwrap() == "yes",
    );
    let mut engine = Engine {
        store: state,
        own: scope.own.clone(),
        runtime: tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    };
    engine
        .renew_acquisition(&scope, &ExpiredAcquisition::test_proof(request))
        .unwrap();
    panic!("renewal fault hook did not terminate child");
}
