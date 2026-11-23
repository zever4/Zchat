//! Real incoming Signal/history/cache, synthetic validated server ACK replies.
//! Deletion never treats a missing reply as evidence of remote failure.
use super::*;
use crate::encryption::native::EpochObservation;

fn acknowledged(
    proof: &EpochDurableFileAck,
    at: i64,
) -> EpochObservation<EpochDurableFileAck, v2::FileAcknowledgements> {
    EpochObservation::test_observation(
        proof.store_scope().clone(),
        proof.clone(),
        v2::FileAcknowledgements {
            files: vec![v2::FileAcknowledgementReceipt {
                acknowledgement: Some(proof.request().files[0].clone()),
                acknowledged_at_unix_ms: at,
            }],
        },
    )
}
async fn save(
    f: &super::super::super::tests::Fixture,
    a: &EpochScope,
    b: &EpochScope,
) -> (String, EpochAttachmentDownloadPlan, EpochAttachmentCache) {
    let (p, file) = message(a, b"known local incoming bytes");
    let id = p.message_id.clone();
    receive(&f.alice, &f.bob, a, b, p).await;
    let plan = f
        .bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan.clone()).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    let cache = finish(&f.bob, run).await.unwrap();
    (id, plan, cache)
}
async fn remove(w: &CryptoCoordinator, b: &EpochScope, id: &str) -> CryptoResult<()> {
    w.remove_local_epoch_attachment(
        b.own.generation_id.clone(),
        b.conversation.clone(),
        id.into(),
    )
    .await
}
fn artifacts(root: &std::path::Path) -> (i64, i64, i64) {
    db(root,B).query_row("SELECT (SELECT count(*) FROM protected_attachment_downloads),(SELECT count(*) FROM protected_attachment_download_chunks),(SELECT coalesce(sum(ciphertext_size),0) FROM protected_attachment_downloads)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
}
fn non_cache_records(root: &std::path::Path) -> Vec<(i64, String, Vec<u8>)> {
    records(root, B)
        .into_iter()
        .filter(|(_, id, _)| {
            !id.starts_with("attachment-download:") && !id.starts_with("attachment-removed:")
        })
        .collect()
}
async fn begin_result(
    w: &CryptoCoordinator,
    plan: EpochAttachmentDownloadPlan,
) -> CryptoResult<Start> {
    let permit = Arc::new(w.attachment_permit()?);
    w.request(|r| {
        Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Begin(
            Box::new(plan),
            permit,
            r,
        )))
    })
    .await
}

#[tokio::test]
async fn confirmed_removal_preserves_history_ack_and_replay_but_blocks_all_stale_open_paths() {
    let (f, a, b) = prepared().await;
    let (id, plan, cache) = save(&f, &a, &b).await;
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    let at = 1_700_000_000_010;
    f.bob
        .accept_epoch_file_ack(acknowledged(&proof, at))
        .await
        .unwrap();
    let before = non_cache_records(f.root.path());
    assert_eq!(artifacts(f.root.path()).0, 1);
    remove(&f.bob, &b, &id).await.unwrap();
    assert_eq!(artifacts(f.root.path()), (0, 0, 0));
    assert_eq!(non_cache_records(f.root.path()), before);
    assert_eq!(
        f.bob.epoch_file_ack_status(id.clone()).await.unwrap(),
        Some(at)
    );
    assert!(
        f.bob
            .pending_epoch_file_acks(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert!(matches!(
        f.bob.open_cached_epoch_attachment(cache).await,
        Err(CryptoError::AttachmentRemoved)
    ));
    assert!(matches!(
        f.bob.cached_epoch_attachment(id.clone()).await,
        Err(CryptoError::AttachmentRemoved)
    ));
    assert!(matches!(
        f.bob
            .open_archived_epoch_attachment(
                b.own.generation_id.clone(),
                b.conversation.clone(),
                id.clone()
            )
            .await,
        Err(CryptoError::AttachmentRemoved)
    ));
    assert!(matches!(
        begin_result(&f.bob, plan).await,
        Err(CryptoError::AttachmentRemoved)
    ));
    assert!(matches!(
        f.bob.epoch_file_ack(id.clone()).await,
        Err(CryptoError::AttachmentRemoved)
    ));
    let saved = records(f.root.path(), B);
    // A proof minted before ACK completion/removal can have a delayed duplicate
    // reply. Only the exact first ACK time replays; no cache row is recreated.
    f.bob
        .accept_epoch_file_ack(acknowledged(&proof, at))
        .await
        .unwrap();
    assert!(
        f.bob
            .accept_epoch_file_ack(acknowledged(&proof, at + 1))
            .await
            .is_err()
    );
    remove(&f.bob, &b, &id).await.unwrap();
    assert_eq!(records(f.root.path(), B), saved);
    f.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    remove(&bob, &b, &id).await.unwrap();
    let fresh = bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    assert!(matches!(
        begin_result(&bob, fresh).await,
        Err(CryptoError::AttachmentRemoved)
    ));
    assert_eq!(bob.epoch_file_ack_status(id).await.unwrap(), Some(at));
    assert_eq!(records(f.root.path(), B), saved);
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn pending_or_lost_ack_result_and_held_buffer_do_not_delete_bytes() {
    let (f, a, b) = prepared().await;
    let (id, _, cache) = save(&f, &a, &b).await;
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    let before = records(f.root.path(), B);
    let charged = artifacts(f.root.path());
    for _ in 0..2 {
        assert!(matches!(
            remove(&f.bob, &b, &id).await,
            Err(CryptoError::AttachmentAckPending)
        ));
        assert_eq!(records(f.root.path(), B), before);
        assert_eq!(artifacts(f.root.path()), charged);
    }
    f.bob
        .accept_epoch_file_ack(acknowledged(&proof, 1_700_000_000_010))
        .await
        .unwrap();
    let held = f.bob.open_cached_epoch_attachment(cache).await.unwrap();
    assert!(matches!(
        remove(&f.bob, &b, &id).await,
        Err(CryptoError::Busy)
    ));
    assert_eq!(held.as_bytes(), b"known local incoming bytes");
    drop(held);
    remove(&f.bob, &b, &id).await.unwrap();
    assert_eq!(artifacts(f.root.path()), (0, 0, 0));
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn removal_commit_failure_rolls_back_marker_chunks_manifest_and_quota() {
    let (f, a, b) = prepared().await;
    let (id, _, cache) = save(&f, &a, &b).await;
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    f.bob
        .accept_epoch_file_ack(acknowledged(&proof, 1_700_000_000_010))
        .await
        .unwrap();
    let before = records(f.root.path(), B);
    let charged = artifacts(f.root.path());
    let db = db(f.root.path(), B);
    db.execute_batch("CREATE TABLE removal_fault_parent(id INTEGER PRIMARY KEY); CREATE TABLE removal_fault_child(id INTEGER REFERENCES removal_fault_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER removal_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE 'attachment-removed:%' BEGIN INSERT INTO removal_fault_child VALUES(1); END;").unwrap();
    assert!(remove(&f.bob, &b, &id).await.is_err());
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(artifacts(f.root.path()), charged);
    assert_eq!(
        f.bob
            .open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        b"known local incoming bytes"
    );
    db.execute_batch("DROP TRIGGER removal_fault; DROP TABLE removal_fault_child; DROP TABLE removal_fault_parent").unwrap();
    remove(&f.bob, &b, &id).await.unwrap();
    assert_eq!(artifacts(f.root.path()), (0, 0, 0));
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn removal_rejects_partial_foreign_and_corrupt_cohorts_without_repair() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a, b"not complete");
    let id = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f
        .bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, &file.blob()[..5]).await.unwrap();
    assert!(matches!(
        remove(&f.bob, &b, &id).await,
        Err(CryptoError::Busy)
    ));
    drop(run);
    // A completed append can still hold its copy of the file permit until
    // the worker returns. Join it before checking the incomplete cache.
    f.bob.join_attachment_work().await.unwrap();
    let before = records(f.root.path(), B);
    let charged = artifacts(f.root.path());
    let result = remove(&f.bob, &b, &id).await;
    assert!(
        matches!(result, Err(CryptoError::MessageUnavailable)),
        "{result:?}"
    );
    assert!(
        f.alice
            .remove_local_epoch_attachment(
                a.own.generation_id.clone(),
                a.conversation.clone(),
                id.clone()
            )
            .await
            .is_err()
    );
    assert!(
        f.bob
            .remove_local_epoch_attachment(
                b.own.generation_id.clone(),
                uuid::Uuid::new_v4().to_string(),
                id.clone()
            )
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(artifacts(f.root.path()), charged);
    let plan = f
        .bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    finish(&f.bob, run).await.unwrap();
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    f.bob
        .accept_epoch_file_ack(acknowledged(&proof, 1_700_000_000_010))
        .await
        .unwrap();
    db(f.root.path(), B)
        .execute(
            "UPDATE protected_attachment_download_chunks SET payload=zeroblob(length(payload))",
            [],
        )
        .unwrap();
    let corrupt = records(f.root.path(), B);
    let charged = artifacts(f.root.path());
    assert!(remove(&f.bob, &b, &id).await.is_err());
    assert_eq!(records(f.root.path(), B), corrupt);
    assert_eq!(artifacts(f.root.path()), charged);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn queued_cancel_retains_file_admission_and_unknown_completed_reply_replays_removal() {
    let (f, a, b) = prepared().await;
    let (id, _, _) = save(&f, &a, &b).await;
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    f.bob
        .accept_epoch_file_ack(acknowledged(&proof, 1_700_000_000_010))
        .await
        .unwrap();
    let before = records(f.root.path(), B);
    let charged = artifacts(f.root.path());
    let (release, wait) = std::sync::mpsc::channel();
    let (entered, ready) = tokio::sync::oneshot::channel();
    assert!(
        f.bob
            .sender
            .try_send(Command::Epoch(EpochCommand::AttachmentDownload(
                DownloadCommand::Barrier(wait, entered)
            )))
            .is_ok()
    );
    ready.await.unwrap();
    let mut call = Box::pin(remove(&f.bob, &b, &id));
    {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(call.as_mut().poll(&mut context), Poll::Pending));
    }
    drop(call);
    assert!(matches!(f.bob.attachment_permit(), Err(CryptoError::Busy)));
    release.send(()).unwrap();
    f.bob.join_attachment_work().await.unwrap();
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(artifacts(f.root.path()), charged);
    let (reply, unobserved) = tokio::sync::oneshot::channel();
    let permit = Arc::new(f.bob.attachment_permit().unwrap());
    assert!(
        f.bob
            .sender
            .try_send(Command::Epoch(EpochCommand::AttachmentDownload(
                DownloadCommand::Remove(
                    b.own.generation_id.clone(),
                    b.conversation.clone(),
                    id.clone(),
                    permit,
                    reply
                )
            )))
            .is_ok()
    );
    // The FIFO join proves COMMIT/reply publication before the caller loses it.
    f.bob.join_attachment_work().await.unwrap();
    drop(unobserved);
    assert_eq!(artifacts(f.root.path()), (0, 0, 0));
    let removed = records(f.root.path(), B);
    remove(&f.bob, &b, &id).await.unwrap();
    assert_eq!(records(f.root.path(), B), removed);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_removal_marker_and_dual_live_marker_are_not_cleaned_or_resurrected() {
    let (f, a, b) = prepared().await;
    let (id, _, _) = save(&f, &a, &b).await;
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    f.bob
        .accept_epoch_file_ack(acknowledged(&proof, 1_700_000_000_010))
        .await
        .unwrap();
    remove(&f.bob, &b, &id).await.unwrap();
    f.bob.shutdown().await.unwrap();
    let mut local = store(f.root.path(), B, false);
    local
        .transaction_with(|tx| {
            let marker = tx.get(&removal::key(&b.own, &id)?)?.unwrap();
            let mut journal = Journal::decode(marker.as_slice()).unwrap();
            journal.format = 2;
            tx.put(&key(&b.own, &id)?, &journal.encode_to_vec())?;
            Ok::<_, CryptoError>(())
        })
        .unwrap();
    drop(local);
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    let broken = records(f.root.path(), B);
    assert!(remove(&bob, &b, &id).await.is_err());
    assert!(bob.cached_epoch_attachment(id).await.is_err());
    assert_eq!(records(f.root.path(), B), broken);
    assert_eq!(artifacts(f.root.path()), (0, 0, 0));
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}
