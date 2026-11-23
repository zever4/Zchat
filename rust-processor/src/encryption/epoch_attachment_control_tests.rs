use super::*;
use crate::encryption::{
    blob_control as boundary,
    native::{EpochBlobLease, EpochObservation},
};

#[path = "epoch_blob_upload_tests.rs"]
mod upload;

#[tokio::test]
async fn candidate_selection_omits_outgoing_markers_without_granting_upload_authority() {
    let (f, own, id, _) = setup().await;
    f.alice.shutdown().await.unwrap();
    let mut db = store(f.root.path(), A, false);
    // Synthetic local marker only, NOT a real Signal/Send3 acceptance proof.
    db.transaction_with::<_, CryptoError>(|tx| {
        tx.put(&outgoing::link_key(&own, &id)?, &[1])?;
        Ok(())
    })
    .unwrap();
    drop(db);
    let worker = CryptoCoordinator::start(store(f.root.path(), A, false), own)
        .await
        .unwrap();
    assert!(
        worker
            .epoch_attachment_candidates()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        worker.epoch_attachment_manifest().await.unwrap(),
        vec![id.clone()]
    );
    assert!(matches!(
        worker.epoch_attachment_control(id).await,
        Err(CryptoError::Conflict)
    ));
    worker.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn candidate_manifest_filters_generation_and_rejects_global_overflow_before_selection() {
    use crate::encryption::protected::StoreError;
    let (f, _, id, p) = setup().await;
    let before = wrapped(f.root.path(), A);
    let db = raw(f.root.path(), A);
    // Corrupt projection fixture: foreign manifests have no protected journals.
    // They are hints only and must never become authority, or evade global bounds.
    db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
    let generation = Uuid::parse_str("44444444-4444-4444-8444-444444444444").unwrap();
    for n in 0..64 {
        let message = Uuid::new_v4().to_string();
        let record = format!("attachment-prepare:{message}");
        let reference = format!("{}:13:{record}", generation.simple());
        db.execute(
            "INSERT INTO protected_attachment_blobs VALUES(?1,?2,13,?3,?4,?5,37,1)",
            rusqlite::params![
                reference,
                generation.as_bytes().as_slice(),
                record,
                message,
                Uuid::new_v4().to_string()
            ],
        )
        .unwrap();
        if n == 0 {
            assert_eq!(
                f.alice.epoch_attachment_candidates().await.unwrap(),
                vec![id.clone()]
            );
            assert_eq!(
                f.alice.epoch_attachment_manifest().await.unwrap(),
                vec![id.clone()]
            );
        }
    }
    assert!(matches!(
        f.alice.epoch_attachment_candidates().await,
        Err(CryptoError::Storage(StoreError::Format))
    ));
    assert!(matches!(
        f.alice.epoch_attachment_manifest().await,
        Err(CryptoError::Storage(StoreError::Format))
    ));
    assert_eq!(
        f.alice
            .epoch_attachment_control(id)
            .await
            .unwrap()
            .request(),
        p.request()
    );
    assert_eq!(wrapped(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

pub(super) fn grant(p: &EpochBlobControlPlan, state: v2::BlobLeaseState) -> v2::BlobLease {
    v2::BlobLease {
        message_id: p.request().message_id.clone(),
        operation_id: p.request().operation_id.clone(),
        attempt_id: Uuid::new_v4().to_string(),
        revision: p.request().expected_revision + 1,
        reserved_at_unix_ms: 1000,
        expires_at_unix_ms: 1000 + boundary::STAGING_LEASE_MS,
        observed_at_unix_ms: 1000,
        state: state as i32,
        expired: false,
    }
}
pub(super) fn observed(p: &EpochBlobControlPlan, g: v2::BlobLease) -> EpochBlobLease {
    EpochObservation::test_observation(StoreScope::new(SERVER, A).unwrap(), p.clone(), g)
}
fn expire(g: &mut v2::BlobLease) {
    g.observed_at_unix_ms = g.expires_at_unix_ms;
    g.expired = true;
}

#[tokio::test]
async fn upload_snapshot_requires_current_durable_reserved_lease_and_retains_file_budget() {
    let (f, _, id, p) = setup().await;
    assert!(matches!(
        f.alice.epoch_blob_upload(id.clone()).await,
        Err(CryptoError::Conflict)
    ));
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    f.alice
        .accept_epoch_blob_lease(observed(&p, g.clone()))
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    let upload = f.alice.epoch_blob_upload(id.clone()).await.unwrap();
    assert_eq!(
        f.alice.epoch_attachment_candidates().await.unwrap(),
        vec![id.clone()]
    );
    assert_eq!(upload.plan().request(), p.request());
    assert_eq!(upload.grant(), &g);
    assert_eq!(
        upload.bytes().len() as u64,
        p.request().blob.as_ref().unwrap().ciphertext_bytes
    );
    assert_eq!(
        <[u8; 32]>::from(Sha256::digest(upload.bytes())).as_slice(),
        p.request().blob.as_ref().unwrap().sha256
    );
    assert!(matches!(
        f.alice.epoch_attachment_control(id.clone()).await,
        Err(CryptoError::Busy)
    ));
    // Dropping/cancelling transport must not remove or mutate the durable cohort.
    drop(upload);
    assert_eq!(snapshot(f.root.path(), A), saved);
    assert_eq!(
        f.alice
            .epoch_attachment_control(id.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    let mut ready = g;
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    f.alice
        .accept_epoch_blob_lease(observed(&p, ready))
        .await
        .unwrap();
    assert!(matches!(
        f.alice.epoch_blob_upload(id).await,
        Err(CryptoError::Conflict)
    ));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
async fn setup() -> (
    super::super::super::tests::Fixture,
    DeviceRef,
    String,
    EpochBlobControlPlan,
) {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    let p = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
    (f, a.own, id, p)
}

#[tokio::test]
async fn upload_operation_commits_with_blob_and_reopens_exactly_without_local_reseed() {
    let (f, own, id, p) = setup().await;
    let old = f
        .alice
        .prepared_epoch_attachment(id.clone())
        .await
        .unwrap()
        .unwrap();
    let bytes = f
        .alice
        .prepared_epoch_attachment_blob(old)
        .await
        .unwrap()
        .as_bytes()
        .to_vec();
    let baseline = snapshot(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    assert_eq!(p.request().expected_revision, 0);
    assert!(p.request().expected_attempt_id.is_empty());
    boundary::validate_request(p.request()).unwrap();
    assert!(matches!(
        f.alice.renew_epoch_blob_control(p.clone()).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), baseline);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own.clone())
        .await
        .unwrap();
    let restored = w.epoch_attachment_control(id.clone()).await.unwrap();
    assert_eq!(restored.request(), p.request());
    assert_eq!(snapshot(f.root.path(), A), baseline);
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    w.accept_epoch_blob_lease(observed(&p, g.clone()))
        .await
        .unwrap();
    let after = snapshot(f.root.path(), A);
    w.accept_epoch_blob_lease(observed(&p, g)).await.unwrap();
    assert_eq!(snapshot(f.root.path(), A), after);
    let fresh = w.prepared_epoch_attachment(id).await.unwrap().unwrap();
    assert_eq!(
        w.prepared_epoch_attachment_blob(fresh)
            .await
            .unwrap()
            .as_bytes(),
        bytes
    );
    assert_eq!(wrapped(f.root.path(), A), master);
    assert!(w.epoch_outbox_page(None).await.unwrap().messages.is_empty());
    w.shutdown().await.unwrap();
    verify_secret(f.root.path(), &own, p.request().message_id.as_str(), &bytes);
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn eligible_remote_observation_renews_once_and_rejects_old_results_after_reopen() {
    let (f, own, id, p) = setup().await;
    let mut g = grant(&p, v2::BlobLeaseState::Ready);
    f.alice
        .accept_epoch_blob_lease(observed(&p, g.clone()))
        .await
        .unwrap();
    assert!(matches!(
        f.alice.renew_epoch_blob_control(p.clone()).await,
        Err(CryptoError::Conflict)
    ));
    expire(&mut g);
    f.alice
        .accept_epoch_blob_lease(observed(&p, g.clone()))
        .await
        .unwrap();
    let next = f.alice.renew_epoch_blob_control(p.clone()).await.unwrap();
    assert_ne!(p.request().operation_id, next.request().operation_id);
    assert_eq!(next.request().expected_revision, g.revision);
    assert_eq!(next.request().expected_attempt_id, g.attempt_id);
    let mut stable = next.request().clone();
    stable.operation_id = p.request().operation_id.clone();
    stable.expected_revision = 0;
    stable.expected_attempt_id.clear();
    assert_eq!(&stable, p.request());
    let saved = snapshot(f.root.path(), A);
    let retry = f.alice.renew_epoch_blob_control(p.clone()).await.unwrap();
    assert_eq!(retry.request(), next.request());
    assert_eq!(snapshot(f.root.path(), A), saved);
    assert!(matches!(
        f.alice.accept_epoch_blob_lease(observed(&p, g)).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own)
        .await
        .unwrap();
    assert_eq!(
        w.epoch_attachment_control(id).await.unwrap().request(),
        next.request()
    );
    assert_eq!(
        w.renew_epoch_blob_control(p.clone())
            .await
            .unwrap()
            .request(),
        next.request()
    );
    let mut second = grant(&next, v2::BlobLeaseState::Reclaimed);
    second.reserved_at_unix_ms = 10_000;
    second.expires_at_unix_ms = second.reserved_at_unix_ms + boundary::STAGING_LEASE_MS;
    second.observed_at_unix_ms = second.reserved_at_unix_ms;
    w.accept_epoch_blob_lease(observed(&next, second))
        .await
        .unwrap();
    let third = w.renew_epoch_blob_control(next).await.unwrap();
    assert_eq!(third.request().expected_revision, 2);
    let saved = snapshot(f.root.path(), A);
    assert!(matches!(
        w.renew_epoch_blob_control(p).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn lease_facts_never_regress_and_linked_remains_sticky_after_reclaim() {
    let (f, _, _, p) = setup().await;
    let first = grant(&p, v2::BlobLeaseState::Reserved);
    let mut ready = first.clone();
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    f.alice
        .accept_epoch_blob_lease(observed(&p, ready.clone()))
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    f.alice
        .accept_epoch_blob_lease(observed(&p, first.clone()))
        .await
        .unwrap();
    assert_eq!(snapshot(f.root.path(), A), saved);
    let mut late = first.clone();
    late.observed_at_unix_ms += 2;
    assert!(matches!(
        f.alice.accept_epoch_blob_lease(observed(&p, late)).await,
        Err(CryptoError::Conflict)
    ));
    let mut changed = ready.clone();
    changed.attempt_id = Uuid::new_v4().to_string();
    assert!(matches!(
        f.alice.accept_epoch_blob_lease(observed(&p, changed)).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    let mut linked = ready;
    linked.state = v2::BlobLeaseState::Linked as i32;
    linked.observed_at_unix_ms += 1;
    f.alice
        .accept_epoch_blob_lease(observed(&p, linked.clone()))
        .await
        .unwrap();
    linked.state = v2::BlobLeaseState::Reclaimed as i32;
    expire(&mut linked);
    f.alice
        .accept_epoch_blob_lease(observed(&p, linked))
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice.renew_epoch_blob_control(p).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn foreign_scope_frozen_sending_and_corrupt_intent_cannot_mint_network_plan() {
    let (f, own, id, p) = setup().await;
    initialize(&f.bob).await;
    prepare(&f.bob, &id).await.unwrap();
    let bob = f.bob.epoch_attachment_control(id.clone()).await.unwrap();
    assert_ne!(bob.request().operation_id, p.request().operation_id);
    assert_ne!(bob.request().blob, p.request().blob);
    let bob_before = snapshot(f.root.path(), B);
    let foreign = EpochObservation::test_observation(
        StoreScope::new("https://other.example.test", A).unwrap(),
        p.clone(),
        grant(&p, v2::BlobLeaseState::Ready),
    );
    assert!(matches!(
        f.alice.accept_epoch_blob_lease(foreign).await,
        Err(CryptoError::Conflict)
    ));
    let foreign = EpochObservation::test_observation(
        StoreScope::new(SERVER, A).unwrap(),
        p.clone(),
        grant(&p, v2::BlobLeaseState::Ready),
    );
    assert!(f.bob.accept_epoch_blob_lease(foreign).await.is_err());
    assert_eq!(snapshot(f.root.path(), B), bob_before);
    f.alice.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own.clone())
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    assert!(matches!(
        w.epoch_attachment_control(id.clone()).await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    assert!(matches!(
        w.renew_epoch_blob_control(p.clone()).await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    assert!(matches!(
        w.accept_epoch_blob_lease(observed(&p, grant(&p, v2::BlobLeaseState::Ready)))
            .await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    w.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, false);
    let mut db = store(f.root.path(), A, false);
    db.transaction_with::<_, CryptoError>(|tx| {
        let mut j = load_journal(tx, &own, &id)?.unwrap();
        j.operation = None;
        tx.put(&key(&own, &id)?, &Zeroizing::new(j.encode_to_vec()))?;
        Ok(())
    })
    .unwrap();
    drop(db);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own)
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    assert!(w.epoch_attachment_control(id.clone()).await.is_err());
    assert!(w.prepared_epoch_attachment(id.clone()).await.is_err());
    assert!(prepare(&w, &id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), saved);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_commit_failure_and_abandoned_commands_cannot_publish_successor() {
    let (f, _, id, p) = setup().await;
    let mut g = grant(&p, v2::BlobLeaseState::Reserved);
    expire(&mut g);
    f.alice
        .accept_epoch_blob_lease(observed(&p, g.clone()))
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    let db = raw(f.root.path(), A);
    db.execute_batch("CREATE TABLE controls_parent(id INTEGER PRIMARY KEY);CREATE TABLE controls_child(id INTEGER REFERENCES controls_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER controls_fault AFTER UPDATE OF payload ON protected_records BEGIN INSERT INTO controls_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.alice.renew_epoch_blob_control(p.clone()).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    assert_eq!(
        f.alice
            .epoch_attachment_control(id.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    db.execute_batch("DROP TRIGGER controls_fault;").unwrap();
    drop(db);
    for accept in [false, true] {
        let (reply, abandoned) = tokio::sync::oneshot::channel();
        drop(abandoned);
        let command = if accept {
            control::ControlCommand::Accept(Box::new(observed(&p, g.clone())), reply)
        } else {
            control::ControlCommand::Renew(Box::new(p.clone()), reply)
        };
        let permit = f.alice.attachment_permit().unwrap();
        f.alice
            .sender
            .try_send(Command::Epoch(EpochCommand::Attachment(
                AttachmentCommand::Control(Box::new(command), permit),
            )))
            .unwrap_or_else(|_| panic!("fixture queue"));
        f.alice.epoch_scope(CHAT.into(), 0).await.unwrap();
        assert_eq!(snapshot(f.root.path(), A), saved);
    }
    let next = f.alice.renew_epoch_blob_control(p).await.unwrap();
    assert_eq!(
        f.alice
            .epoch_attachment_control(id)
            .await
            .unwrap()
            .request(),
        next.request()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn attachment_control_crash_child() {
    let Ok(path) = std::env::var("ZCHAT_BLOB_CONTROL_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_BLOB_CONTROL_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let own = DeviceRef {
                user_id: A.into(),
                generation_id: AG.into(),
                device_id: 1,
            };
            let mut db = store(Path::new(&path), A, false);
            db.test_exit_on_record_change(key(&own, CRASH_ID).unwrap(), after)
                .unwrap();
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let p = w.epoch_attachment_control(CRASH_ID.into()).await.unwrap();
            let _ = w.renew_epoch_blob_control(p).await;
            panic!("control crash hook did not fire");
        });
}
#[tokio::test]
async fn renewal_process_exit_before_or_after_commit_recovers_exact_intent() {
    for after in [false, true] {
        let f = fixture().await;
        let own = initialize(&f.alice).await.own;
        prepare(&f.alice, CRASH_ID).await.unwrap();
        let old = f
            .alice
            .epoch_attachment_control(CRASH_ID.into())
            .await
            .unwrap();
        let mut g = grant(&old, v2::BlobLeaseState::Ready);
        expire(&mut g);
        f.alice
            .accept_epoch_blob_lease(observed(&old, g))
            .await
            .unwrap();
        f.alice.shutdown().await.unwrap();
        let before = snapshot(f.root.path(), A);
        let child=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","encryption::signal_state::epoch_state::attachment_prepare::tests::controls::attachment_control_crash_child","--nocapture"]).env("ZCHAT_BLOB_CONTROL_CRASH_PATH",f.root.path()).env("ZCHAT_BLOB_CONTROL_CRASH_AFTER",if after{"1"}else{"0"}).status().unwrap();
        assert_eq!(child.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), own)
            .await
            .unwrap();
        let restored = w.epoch_attachment_control(CRASH_ID.into()).await.unwrap();
        if after {
            assert_ne!(restored.request().operation_id, old.request().operation_id);
        } else {
            assert_eq!(snapshot(f.root.path(), A), before);
            assert_eq!(restored.request(), old.request());
        }
        let saved = snapshot(f.root.path(), A);
        let next = w.renew_epoch_blob_control(old).await.unwrap();
        if after {
            assert_eq!(next.request(), restored.request());
            assert_eq!(snapshot(f.root.path(), A), saved);
        }
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
