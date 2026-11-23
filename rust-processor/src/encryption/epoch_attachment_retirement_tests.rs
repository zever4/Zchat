//! Real Signal promotion/cache and validated server-receipt observations.
use super::archive_tests::promote;
use super::*;
use crate::encryption::native::{EpochBlobSendFailure, EpochObservation};

async fn remove(w: &CryptoCoordinator, s: &EpochScope, id: &str) -> CryptoResult<()> {
    w.remove_local_epoch_attachment(s.own.generation_id.clone(), CHAT.into(), id.into())
        .await
}
async fn receipt(w: &CryptoCoordinator, p: &EpochOutgoingPlan) {
    let e = p.request().envelope.as_ref().unwrap();
    w.accept_epoch_send_receipt(EpochObservation::test_observation(
        p.store_scope().clone(),
        p.clone(),
        v2::SendReceipt {
            message_id: e.message_id.clone(),
            envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
            stored_at_unix_ms: 1_700_000_000_000,
            expires_at_unix_ms: 1_700_000_000_000
                + crate::encryption::contract::MESSAGE_RETENTION_MS,
            crypto_epoch: e.crypto_epoch,
        },
    ))
    .await
    .unwrap();
}
fn failure(p: &EpochOutgoingPlan) -> EpochBlobSendFailure {
    EpochObservation::test_observation(
        p.store_scope().clone(),
        p.clone(),
        v2::BlobSendFailure {
            reason: 1,
            binding: Some(data::send_binding(p.request().envelope.as_ref().unwrap()).unwrap()),
        },
    )
}
fn stable(root: &Path) -> Vec<(i64, String, Vec<u8>)> {
    records(root, A)
        .into_iter()
        .filter(|(_, id, _)| !id.starts_with("attachment-spool-removed:"))
        .collect()
}
fn marker_key(own: &DeviceRef, id: &str) -> RecordKey {
    RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("attachment-spool-removed:{id}"),
    )
    .unwrap()
}

#[tokio::test]
async fn removed_spool_rejects_stray_chunks_instead_of_hiding_or_cleaning_them() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let p = promote(&f.alice, &id).await;
    receipt(&f.alice, &p).await;
    remove(&f.alice, &a, &id).await.unwrap();
    let db = raw(f.root.path(), A);
    db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
    let reference = format!(
        "{}:13:attachment-prepare:{id}",
        a.own.generation_id.replace('-', "")
    );
    db.execute(
        "INSERT INTO protected_attachment_chunks VALUES(?1,0,?2)",
        rusqlite::params![reference, vec![1u8]],
    )
    .unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(remove(&f.alice, &a, &id).await.is_err());
    assert!(f.alice.epoch_attachment_outgoing(id.clone()).await.is_err());
    assert!(
        f.alice
            .open_archived_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id)
            .await
            .is_err()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_queued_outgoing_removal_retains_admission_then_leaves_copy_intact() {
    use super::super::super::attachment_download::DownloadCommand;
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let p = promote(&f.alice, &id).await;
    receipt(&f.alice, &p).await;
    let before = snapshot(f.root.path(), A);
    let (release, wait) = std::sync::mpsc::channel();
    let (entered, ready) = tokio::sync::oneshot::channel();
    assert!(
        f.alice
            .sender
            .try_send(Command::Epoch(EpochCommand::AttachmentDownload(
                DownloadCommand::Barrier(wait, entered)
            )))
            .is_ok()
    );
    ready.await.unwrap();
    let mut call = Box::pin(remove(&f.alice, &a, &id));
    {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        assert!(matches!(
            call.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
    }
    drop(call);
    assert!(matches!(
        f.alice.attachment_permit(),
        Err(CryptoError::Busy)
    ));
    release.send(()).unwrap();
    f.alice.join_attachment_work().await.unwrap();
    assert_eq!(snapshot(f.root.path(), A), before);
    remove(&f.alice, &a, &id).await.unwrap();
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn accepted_spool_removal_preserves_all_private_metadata_and_rejects_stale_file_plans() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let old = prepare(&f.alice, &id).await.unwrap();
    let control = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
    let p = promote(&f.alice, &id).await;
    let retry = f.alice.epoch_attachment_retry(failure(&p)).await.unwrap();
    receipt(&f.alice, &p).await;
    let accepted = f
        .alice
        .epoch_attachment_outgoing(id.clone())
        .await
        .unwrap()
        .unwrap();
    let before = stable(f.root.path());
    let master = wrapped(f.root.path(), A);
    remove(&f.alice, &a, &id).await.unwrap();
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    assert_eq!(
        f.alice
            .epoch_attachment_input_state(CHAT.into(), id.clone())
            .await
            .unwrap(),
        EpochAttachmentInputState::Outgoing {
            crypto_epoch: 0,
            stored_on_server: true,
            sealed_unsent: false
        }
    );
    assert_eq!(stable(f.root.path()), before);
    assert!(matches!(
        f.alice
            .open_archived_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id.clone())
            .await,
        Err(CryptoError::AttachmentRemoved)
    ));
    assert!(f.alice.prepared_epoch_attachment_blob(old).await.is_err());
    assert!(f.alice.prepared_epoch_attachment(id.clone()).await.is_err());
    assert!(f.alice.epoch_blob_upload(id.clone()).await.is_err());
    assert!(f.alice.epoch_attachment_retry(failure(&p)).await.is_err());
    assert!(
        f.alice
            .refresh_epoch_attachment_retry(retry.clone())
            .await
            .is_err()
    );
    assert!(
        f.alice
            .renew_epoch_attachment_retry(retry.clone())
            .await
            .is_err()
    );
    assert!(
        f.alice
            .epoch_attachment_retry_upload(retry.clone())
            .await
            .is_err()
    );
    assert!(
        f.alice
            .accept_epoch_attachment_retry_lease(
                retry,
                controls::observed(
                    &control,
                    controls::grant(&control, v2::BlobLeaseState::Ready)
                )
            )
            .await
            .is_err()
    );
    let EpochAttachmentPromotion::Queued(replay) =
        f.alice.promote_epoch_attachment_if(control).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(replay.request(), accepted.request());
    assert_eq!(replay.receipt(), accepted.receipt());
    receipt(&f.alice, &p).await; // Late exact reply after deletion does not recreate files.
    let saved = records(f.root.path(), A);
    remove(&f.alice, &a, &id).await.unwrap();
    assert_eq!(records(f.root.path(), A), saved);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    remove(&w, &a, &id).await.unwrap();
    assert_eq!(
        w.epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap()
            .request(),
        p.request()
    );
    assert!(matches!(
        w.open_archived_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id.clone())
            .await,
        Err(CryptoError::AttachmentRemoved)
    ));
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    assert_eq!(stable(f.root.path()), before);
    assert_eq!(wrapped(f.root.path(), A), master);
    // Message status may advance after local deletion, without invalidating
    // the immutable retirement evidence or recreating ciphertext.
    let marker_id = format!("attachment-spool-removed:{id}");
    let marker = || {
        raw(f.root.path(), A)
            .query_row::<Vec<u8>, _, _>(
                "SELECT payload FROM protected_records WHERE record_id=?1",
                [&marker_id],
                |r| r.get(0),
            )
            .unwrap()
    };
    let original_marker = marker();
    let saved = w
        .epoch_attachment_outgoing(id.clone())
        .await
        .unwrap()
        .unwrap();
    let receipt = saved.receipt().unwrap().clone();
    for (state, delta) in [
        (crate::proto::e2ee::MessageState::Expired, 1),
        (crate::proto::e2ee::MessageState::RecipientStored, 2),
    ] {
        w.accept_epoch_statuses(EpochObservation::test_observation(
            saved.store_scope().clone(),
            vec![saved.clone()],
            v2::MessageStatuses {
                messages: vec![v2::MessageStatus {
                    receipt: Some(receipt.clone()),
                    state: state as i32,
                    observed_at_unix_ms: receipt.expires_at_unix_ms + delta,
                }],
            },
        ))
        .await
        .unwrap();
        remove(&w, &a, &id).await.unwrap();
    }
    let final_plan = w
        .epoch_attachment_outgoing(id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        final_plan.status().unwrap().state,
        crate::proto::e2ee::MessageState::RecipientStored as i32
    );
    assert_eq!(
        final_plan.removal().unwrap().state,
        crate::proto::e2ee::MessageState::Expired as i32
    );
    assert_eq!(marker(), original_marker);
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_send_unknown_reply_expired_lease_and_held_file_never_remove_bytes() {
    let (f, a, b) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(remove(&f.alice, &a, &id).await.is_err()); // Not promoted, no outbox authority.
    assert_eq!(snapshot(f.root.path(), A), before);
    let p = promote(&f.alice, &id).await;
    let retry = f.alice.epoch_attachment_retry(failure(&p)).await.unwrap();
    let control = retry.control().clone();
    let mut lease = retry.lease().unwrap().clone();
    lease.expired = true;
    lease.observed_at_unix_ms = lease.expires_at_unix_ms;
    f.alice
        .accept_epoch_attachment_retry_lease(retry, controls::observed(&control, lease))
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    for _ in 0..2 {
        assert!(matches!(
            remove(&f.alice, &a, &id).await,
            Err(CryptoError::AttachmentSendPending)
        ));
        assert_eq!(snapshot(f.root.path(), A), before);
    }
    assert!(remove(&f.bob, &b, &id).await.is_err());
    assert!(
        f.alice
            .remove_local_epoch_attachment(
                a.own.generation_id.clone(),
                Uuid::new_v4().to_string(),
                id.clone()
            )
            .await
            .is_err()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    receipt(&f.alice, &p).await;
    let held = f
        .alice
        .open_archived_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id.clone())
        .await
        .unwrap();
    assert!(matches!(
        remove(&f.alice, &a, &id).await,
        Err(CryptoError::Busy)
    ));
    assert_eq!(held.as_bytes(), source().as_slice());
    drop(held);
    remove(&f.alice, &a, &id).await.unwrap();
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn outgoing_removal_commit_failure_restores_ref_marker_chunks_and_all_metadata() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let p = promote(&f.alice, &id).await;
    receipt(&f.alice, &p).await;
    let db = raw(f.root.path(), A);
    let refs = || {
        db.query_row::<String, _, _>(
            "SELECT attachment_ref FROM protected_records WHERE record_id=?1",
            [format!("attachment-prepare:{id}")],
            |r| r.get(0),
        )
        .unwrap()
    };
    let original_ref = refs();
    let before = snapshot(f.root.path(), A);
    db.execute_batch("CREATE TABLE retired_fault_parent(id INTEGER PRIMARY KEY); CREATE TABLE retired_fault_child(id INTEGER REFERENCES retired_fault_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER retired_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE 'attachment-spool-removed:%' BEGIN INSERT INTO retired_fault_child VALUES(1); END;").unwrap();
    assert!(remove(&f.alice, &a, &id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(refs(), original_ref);
    assert_eq!(
        f.alice
            .open_archived_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id.clone())
            .await
            .unwrap()
            .as_bytes(),
        source().as_slice()
    );
    db.execute_batch("DROP TRIGGER retired_fault; DROP TABLE retired_fault_child; DROP TABLE retired_fault_parent").unwrap();
    remove(&f.alice, &a, &id).await.unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT count(*) FROM protected_records WHERE record_id=?1 AND attachment_ref IS NULL",
            [format!("attachment-prepare:{id}")],
            |r| r.get(0)
        )
        .unwrap(),
        1
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_outgoing_bytes_or_retirement_marker_fail_without_cleanup_or_resurrection() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let p = promote(&f.alice, &id).await;
    receipt(&f.alice, &p).await;
    raw(f.root.path(), A)
        .execute(
            "UPDATE protected_attachment_chunks SET payload=zeroblob(length(payload))",
            [],
        )
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(remove(&f.alice, &a, &id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let p = promote(&f.alice, &id).await;
    receipt(&f.alice, &p).await;
    remove(&f.alice, &a, &id).await.unwrap();
    f.alice.shutdown().await.unwrap();
    let mut s = store(f.root.path(), A, false);
    // Valid AEAD at the correct marker key, but not a valid retirement proof.
    s.transaction(|tx| tx.put(&marker_key(&a.own, &id), &[8, 99]))
        .unwrap();
    drop(s);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(remove(&w, &a, &id).await.is_err());
    assert!(w.epoch_attachment_outgoing(id.clone()).await.is_err());
    assert!(
        w.open_archived_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id)
            .await
            .is_err()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
