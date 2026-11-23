//! A real signed two-peer accepted recovery is required, not a boolean "unsent".
use super::*;

#[tokio::test]
async fn signed_sealed_unsent_allows_spool_retirement_and_preserves_epoch_activation() {
    let (f, a, b) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    f.alice
        .prepare_epoch_attachment(
            CHAT.into(),
            id.clone(),
            crate::encryption::attachment_payload::AttachmentDraft {
                kind: v2::AttachmentKind::File,
                filename: Zeroizing::new("sealed-local.bin".into()),
                mime_type: Zeroizing::new("application/octet-stream".into()),
                caption: Zeroizing::new(String::new()),
                photo_metadata_stripped: None,
            },
            Zeroizing::new(b"file retained until signed terminal evidence".to_vec()),
        )
        .await
        .unwrap();
    let lease = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
    f.alice
        .accept_epoch_blob_lease(EpochObservation::test_observation(
            a.scope.clone(),
            lease.clone(),
            v2::BlobLease {
                message_id: id.clone(),
                operation_id: lease.request().operation_id.clone(),
                attempt_id: uuid::Uuid::new_v4().to_string(),
                revision: 1,
                reserved_at_unix_ms: 1000,
                expires_at_unix_ms: 1000 + crate::encryption::blob_control::STAGING_LEASE_MS,
                observed_at_unix_ms: 1000,
                state: v2::BlobLeaseState::Ready as i32,
                expired: false,
            },
        ))
        .await
        .unwrap();
    let EpochAttachmentPromotion::Queued(p) =
        f.alice.promote_epoch_attachment(id.clone()).await.unwrap()
    else {
        panic!()
    };
    let r = accepted(&f, &a, &b).await;
    let proposal = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    let proof = sealed(&p, &r);
    let mut bad = proof.clone();
    bad.fence
        .as_mut()
        .unwrap()
        .binding
        .as_mut()
        .unwrap()
        .message_id = uuid::Uuid::new_v4().to_string();
    assert!(
        f.alice
            .accept_epoch_send_fence(fence(&a, &p, &bad))
            .await
            .is_err()
    );
    assert!(matches!(
        f.alice
            .remove_local_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id.clone())
            .await,
        Err(CryptoError::AttachmentSendPending)
    ));
    f.alice
        .accept_epoch_send_fence(fence(&a, &p, &proof))
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    f.alice
        .remove_local_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id.clone())
        .await
        .unwrap();
    assert_eq!(
        records(f.root.path(), A)
            .into_iter()
            .filter(|(_, k, _)| !k.starts_with("attachment-spool-removed:"))
            .collect::<Vec<_>>(),
        before
    );
    assert_eq!(
        db(f.root.path(), A)
            .query_row::<i64, _, _>("SELECT count(*) FROM protected_attachment_blobs", [], |r| r
                .get(0))
            .unwrap(),
        0
    );
    let retained = f
        .alice
        .epoch_attachment_outgoing(id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained.request(), p.request());
    assert_eq!(retained.sealed_failure(), Some(&proof));
    assert!(retained.receipt().is_none());
    assert!(
        f.alice
            .advance_epoch_transition(a.clone(), proposal)
            .await
            .unwrap()
            .activated()
    );
    f.alice
        .remove_local_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id.clone())
        .await
        .unwrap();
    assert!(matches!(
        f.alice
            .open_archived_epoch_attachment(a.own.generation_id.clone(), CHAT.into(), id)
            .await,
        Err(CryptoError::AttachmentRemoved)
    ));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
