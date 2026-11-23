use super::controls::{grant, observed};
use super::*;
use crate::encryption::native::{EpochBlobSendFailure, EpochObservation};

async fn promote(w: &CryptoCoordinator, id: &str) -> EpochOutgoingPlan {
    prepare(w, id).await.unwrap();
    let c = w.epoch_attachment_control(id.into()).await.unwrap();
    w.accept_epoch_blob_lease(observed(&c, grant(&c, v2::BlobLeaseState::Ready)))
        .await
        .unwrap();
    let EpochAttachmentPromotion::Queued(outgoing) =
        w.promote_epoch_attachment(id.into()).await.unwrap()
    else {
        panic!("outbox")
    };
    *outgoing
}
fn failed(p: &EpochOutgoingPlan, reason: i32) -> EpochBlobSendFailure {
    EpochObservation::test_observation(
        p.store_scope().clone(),
        p.clone(),
        v2::BlobSendFailure {
            reason,
            binding: Some(data::send_binding(p.request().envelope.as_ref().unwrap()).unwrap()),
        },
    )
}
fn stable_records(root: &Path) -> Vec<(i64, String, Vec<u8>)> {
    records(root, A)
        .into_iter()
        .filter(|r| !r.1.starts_with("attachment-prepare:"))
        .collect()
}
async fn expire(w: &CryptoCoordinator, p: EpochAttachmentRetryPlan) -> EpochAttachmentRetryPlan {
    let c = p.control().clone();
    let mut g = p.lease().unwrap().clone();
    g.observed_at_unix_ms = g.expires_at_unix_ms;
    g.expired = true;
    w.accept_epoch_attachment_retry_lease(p, observed(&c, g))
        .await
        .unwrap()
}
fn receipt(p: &EpochOutgoingPlan) -> v2::SendReceipt {
    let e = p.request().envelope.as_ref().unwrap();
    v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    }
}

#[tokio::test]
async fn retained_retry_renews_only_after_saved_db_proof_and_reuses_all_ciphertext_after_reopen() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let send = promote(&f.alice, &id).await;
    let original = f
        .alice
        .epoch_attachment_retry(failed(&send, 2))
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    let stable = stable_records(f.root.path());
    let chunks = cohort(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    // Even LEASE_EXPIRED Send says nothing about which local operation may renew.
    assert!(
        f.alice
            .renew_epoch_attachment_retry(original.clone())
            .await
            .is_err()
    );
    assert!(
        f.alice
            .epoch_attachment_retry_upload(original.clone())
            .await
            .is_err()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    let expired = expire(&f.alice, original).await;
    let next = f
        .alice
        .renew_epoch_attachment_retry(expired.clone())
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    assert!(
        f.alice
            .refresh_epoch_attachment_retry(expired.clone())
            .await
            .is_err()
    );
    let checked = f
        .alice
        .refresh_epoch_attachment_retry(next.clone())
        .await
        .unwrap();
    assert_eq!(checked.control().request(), next.control().request());
    let replay = f
        .alice
        .renew_epoch_attachment_retry(expired.clone())
        .await
        .unwrap();
    assert_eq!(next.control().request(), replay.control().request());
    assert_eq!(snapshot(f.root.path(), A), saved);
    assert_ne!(
        next.control().request().operation_id,
        expired.control().request().operation_id
    );
    assert_eq!(
        next.control().request().expected_attempt_id,
        expired.lease().unwrap().attempt_id
    );
    assert_eq!(
        next.control().request().blob,
        expired.control().request().blob
    );
    assert!(next.lease().is_none());
    // The old preparation/control/upload APIs remain unavailable after promotion.
    assert!(f.alice.epoch_attachment_control(id.clone()).await.is_err());
    assert!(f.alice.epoch_blob_upload(id.clone()).await.is_err());
    let c = next.control().clone();
    let mut g = grant(&c, v2::BlobLeaseState::Reserved);
    g.reserved_at_unix_ms = expired.lease().unwrap().observed_at_unix_ms + 1;
    g.expires_at_unix_ms =
        g.reserved_at_unix_ms + crate::encryption::blob_control::STAGING_LEASE_MS;
    g.observed_at_unix_ms = g.reserved_at_unix_ms;
    let reserved = f
        .alice
        .accept_epoch_attachment_retry_lease(next, observed(&c, g.clone()))
        .await
        .unwrap();
    let upload = f
        .alice
        .epoch_attachment_retry_upload(reserved.clone())
        .await
        .unwrap();
    let exact = upload.bytes().to_vec();
    assert_eq!(
        Sha256::digest(&exact).as_slice(),
        send.request()
            .envelope
            .as_ref()
            .unwrap()
            .blob
            .as_ref()
            .unwrap()
            .sha256
    );
    assert_eq!(upload.plan().request(), reserved.control().request());
    drop(upload);
    // Stale operation responses/read authority cannot overwrite the successor.
    let stale = observed(expired.control(), expired.lease().unwrap().clone());
    assert!(
        f.alice
            .accept_epoch_attachment_retry_lease(expired.clone(), stale)
            .await
            .is_err()
    );
    assert!(
        f.alice
            .epoch_attachment_retry_upload(expired)
            .await
            .is_err()
    );
    g.state = v2::BlobLeaseState::Ready as i32;
    g.observed_at_unix_ms += 1;
    let ready = f
        .alice
        .accept_epoch_attachment_retry_lease(reserved, observed(&c, g))
        .await
        .unwrap();
    assert_eq!(ready.outgoing().request(), send.request());
    assert_eq!(stable_records(f.root.path()), stable);
    assert_eq!(cohort(f.root.path(), A), chunks);
    assert_eq!(wrapped(f.root.path(), A), master);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    let restored = w
        .epoch_attachment_outgoing(id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.request(), send.request());
    let retry = w
        .epoch_attachment_retry(failed(&restored, 1))
        .await
        .unwrap();
    assert_eq!(retry.control().request(), c.request());
    assert_eq!(retry.lease(), ready.lease());
    assert_eq!(stable_records(f.root.path()), stable);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn receipt_blocks_every_retained_retry_operation_and_stale_failure_without_mutation() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let send = promote(&f.alice, &id).await;
    let p = expire(
        &f.alice,
        f.alice
            .epoch_attachment_retry(failed(&send, 1))
            .await
            .unwrap(),
    )
    .await;
    f.alice
        .accept_epoch_send_receipt(EpochObservation::test_observation(
            send.store_scope().clone(),
            send.clone(),
            receipt(&send),
        ))
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(
        f.alice
            .epoch_attachment_retry(failed(&send, 1))
            .await
            .is_err()
    );
    assert!(
        f.alice
            .refresh_epoch_attachment_retry(p.clone())
            .await
            .is_err()
    );
    assert!(
        f.alice
            .renew_epoch_attachment_retry(p.clone())
            .await
            .is_err()
    );
    assert!(
        f.alice
            .epoch_attachment_retry_upload(p.clone())
            .await
            .is_err()
    );
    let response = observed(p.control(), p.lease().unwrap().clone());
    assert!(
        f.alice
            .accept_epoch_attachment_retry_lease(p, response)
            .await
            .is_err()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    let exact = f
        .alice
        .epoch_attachment_outgoing(id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(exact.request(), send.request());
    assert!(!exact.needs_send());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_retry_never_adopts_foreign_or_malformed_failure_or_lease_cohort() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let send = promote(&f.alice, &id).await;
    prepare(&f.bob, &id).await.unwrap();
    let a = snapshot(f.root.path(), A);
    let b = snapshot(f.root.path(), B);
    assert!(
        f.bob
            .epoch_attachment_retry(failed(&send, 1))
            .await
            .is_err()
    );
    for fault in 0..4 {
        let mut obs = failed(&send, 1); // Build altered TEST observations, never renderer inputs.
        let mut bad = obs.response().clone();
        match fault {
            0 => bad.reason = 0,
            1 => bad.binding.as_mut().unwrap().envelope_digest[0] ^= 1,
            2 => bad.binding.as_mut().unwrap().epoch += 1,
            _ => bad.binding = None,
        }
        obs = EpochObservation::test_observation(send.store_scope().clone(), send.clone(), bad);
        assert!(f.alice.epoch_attachment_retry(obs).await.is_err());
    }
    let foreign = EpochObservation::test_observation(
        StoreScope::new("https://other.example.test", A).unwrap(),
        send.clone(),
        failed(&send, 1).response().clone(),
    );
    assert!(f.alice.epoch_attachment_retry(foreign).await.is_err());
    let p = f
        .alice
        .epoch_attachment_retry(failed(&send, 1))
        .await
        .unwrap();
    let wrong = EpochBlobControlPlan::test_plan(
        p.control().store_scope().clone(),
        p.control().request().clone(),
    );
    assert!(
        f.alice
            .accept_epoch_attachment_retry_lease(
                p,
                observed(&wrong, grant(&wrong, v2::BlobLeaseState::Ready))
            )
            .await
            .is_err()
    );
    assert_eq!(snapshot(f.root.path(), A), a);
    assert_eq!(snapshot(f.root.path(), B), b);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_retry_checks_fresh_freeze_closure_and_chunks_while_outbox_stays_readable() {
    for kind in ["frozen", "closed", "missing chunks"] {
        let (f, a, _) = prepared().await;
        let id = Uuid::new_v4().to_string();
        let send = promote(&f.alice, &id).await;
        let p = f
            .alice
            .epoch_attachment_retry(failed(&send, 1))
            .await
            .unwrap();
        if kind == "closed" {
            let mut closed = f.a.clone();
            closed.state = ConversationState::Closed as i32;
            closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            closed.closed_at_unix_ms = 1_700_000_000_000;
            closed.closed_generation_id = closed
                .peer_device
                .as_ref()
                .unwrap()
                .reference
                .as_ref()
                .unwrap()
                .generation_id
                .clone();
            index(&f.alice, &closed).await;
        }
        f.alice.shutdown().await.unwrap();
        if kind == "frozen" {
            rewrite_frozen(f.root.path(), &f.a, true);
        }
        if kind == "missing chunks" {
            raw(f.root.path(), A)
                .execute("DELETE FROM protected_attachment_chunks", [])
                .unwrap();
        }
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
            .await
            .unwrap();
        let before = snapshot(f.root.path(), A);
        assert!(
            w.refresh_epoch_attachment_retry(p.clone()).await.is_err(),
            "{kind}"
        );
        assert!(
            w.epoch_attachment_retry(failed(&send, 1)).await.is_err(),
            "{kind}"
        );
        assert!(
            w.epoch_attachment_retry_upload(p.clone()).await.is_err(),
            "{kind}"
        );
        assert!(
            w.renew_epoch_attachment_retry(p.clone()).await.is_err(),
            "{kind}"
        );
        let g = observed(p.control(), p.lease().unwrap().clone());
        assert!(
            w.accept_epoch_attachment_retry_lease(p, g).await.is_err(),
            "{kind}"
        );
        assert_eq!(snapshot(f.root.path(), A), before);
        assert_eq!(
            w.epoch_attachment_outgoing(id)
                .await
                .unwrap()
                .unwrap()
                .request(),
            send.request()
        );
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn retained_retry_keeps_file_permit_until_last_source_drop() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let send = promote(&f.alice, &id).await;
    let p = expire(
        &f.alice,
        f.alice
            .epoch_attachment_retry(failed(&send, 1))
            .await
            .unwrap(),
    )
    .await;
    let p = f.alice.renew_epoch_attachment_retry(p).await.unwrap();
    let c = p.control().clone();
    let p = f
        .alice
        .accept_epoch_attachment_retry_lease(
            p,
            observed(&c, grant(&c, v2::BlobLeaseState::Reserved)),
        )
        .await
        .unwrap();
    let body = f
        .alice
        .epoch_attachment_retry_upload(p.clone())
        .await
        .unwrap();
    assert!(matches!(
        f.alice.epoch_attachment_retry_upload(p.clone()).await,
        Err(CryptoError::Busy)
    ));
    assert_eq!(
        f.alice
            .epoch_attachment_outgoing(id)
            .await
            .unwrap()
            .unwrap()
            .request(),
        send.request()
    );
    drop(body);
    assert!(f.alice.epoch_attachment_retry_upload(p).await.is_ok());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn retained_retry_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_RETAINED_RETRY_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_RETAINED_RETRY_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let own = DeviceRef {
                user_id: A.into(),
                generation_id: AG.into(),
                device_id: 1,
            };
            let mut db = store(Path::new(&root), A, false);
            db.test_exit_on_record_change(key(&own, CRASH_ID).unwrap(), after)
                .unwrap();
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let send = w
                .epoch_attachment_outgoing(CRASH_ID.into())
                .await
                .unwrap()
                .unwrap();
            let p = w.epoch_attachment_retry(failed(&send, 1)).await.unwrap();
            let _ = w.renew_epoch_attachment_retry(p).await;
            panic!("retained retry crash hook did not fire");
        });
}

#[tokio::test]
async fn retained_retry_process_exit_recovers_one_successor_without_changing_signal() {
    for after in [false, true] {
        let (f, a, _) = prepared().await;
        let send = promote(&f.alice, CRASH_ID).await;
        let p = expire(
            &f.alice,
            f.alice
                .epoch_attachment_retry(failed(&send, 1))
                .await
                .unwrap(),
        )
        .await;
        f.alice.shutdown().await.unwrap();
        let before = snapshot(f.root.path(), A);
        let stable = stable_records(f.root.path());
        let chunks = cohort(f.root.path(), A);
        let status=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","encryption::signal_state::epoch_state::attachment_prepare::tests::retry_tests::retained_retry_crash_child","--nocapture"]).env("ZCHAT_RETAINED_RETRY_CRASH_PATH",f.root.path()).env("ZCHAT_RETAINED_RETRY_CRASH_AFTER",if after {"1"}else{"0"}).status().unwrap();
        assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
            .await
            .unwrap();
        let restored = w.epoch_attachment_retry(failed(&send, 1)).await.unwrap();
        if after {
            assert_ne!(
                restored.control().request().operation_id,
                p.control().request().operation_id
            );
        } else {
            assert_eq!(snapshot(f.root.path(), A), before);
        }
        let saved = snapshot(f.root.path(), A);
        let next = w.renew_epoch_attachment_retry(p).await.unwrap();
        if after {
            assert_eq!(next.control().request(), restored.control().request());
            assert_eq!(snapshot(f.root.path(), A), saved);
        }
        assert_eq!(stable_records(f.root.path()), stable);
        assert_eq!(cohort(f.root.path(), A), chunks);
        assert_eq!(
            w.epoch_attachment_outgoing(CRASH_ID.into())
                .await
                .unwrap()
                .unwrap()
                .request(),
            send.request()
        );
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
