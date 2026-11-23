//! Real Signal promotion and protected files; no transport needed for reading.
use super::*;
use crate::encryption::native::EpochObservation;

pub(super) async fn promote(w: &CryptoCoordinator, id: &str) -> EpochOutgoingPlan {
    prepare(w, id).await.unwrap();
    let c = w.epoch_attachment_control(id.into()).await.unwrap();
    w.accept_epoch_blob_lease(controls::observed(
        &c,
        controls::grant(&c, v2::BlobLeaseState::Ready),
    ))
    .await
    .unwrap();
    let EpochAttachmentPromotion::Queued(p) = w.promote_epoch_attachment(id.into()).await.unwrap()
    else {
        panic!("expected Signal outbox");
    };
    *p
}
async fn open(
    w: &CryptoCoordinator,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<EpochAttachmentFile> {
    w.open_archived_epoch_attachment(own.generation_id.clone(), CHAT.into(), id.into())
        .await
}

#[tokio::test]
async fn outgoing_archive_pending_and_receipted_reads_preserve_exact_outbox_and_reopen() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let p = promote(&f.alice, &id).await;
    assert!(p.needs_send());
    let before = snapshot(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    let file = open(&f.alice, &a.own, &id).await.unwrap();
    assert_eq!(file.as_bytes(), source().as_slice());
    assert!(matches!(
        open(&f.alice, &a.own, &id).await,
        Err(CryptoError::Busy)
    ));
    assert!(matches!(
        f.alice.epoch_attachment_control(id.clone()).await,
        Err(CryptoError::Busy)
    ));
    drop(file);
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(
        f.alice
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap()
            .request(),
        p.request()
    );
    // The archive did not turn promoted bytes back into preparation authority.
    assert!(f.alice.prepared_epoch_attachment(id.clone()).await.is_err());
    assert!(f.alice.epoch_blob_upload(id.clone()).await.is_err());
    assert!(f.alice.epoch_file_ack(id.clone()).await.is_err());
    let e = p.request().envelope.as_ref().unwrap();
    f.alice
        .accept_epoch_send_receipt(EpochObservation::test_observation(
            p.store_scope().clone(),
            p.clone(),
            v2::SendReceipt {
                message_id: id.clone(),
                envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
                stored_at_unix_ms: 1_700_000_000_000,
                expires_at_unix_ms: 1_700_000_000_000
                    + crate::encryption::contract::MESSAGE_RETENTION_MS,
                crypto_epoch: e.crypto_epoch,
            },
        ))
        .await
        .unwrap();
    let after = snapshot(f.root.path(), A);
    assert_eq!(
        open(&f.alice, &a.own, &id).await.unwrap().as_bytes(),
        source().as_slice()
    );
    assert_eq!(snapshot(f.root.path(), A), after);
    f.alice.shutdown().await.unwrap();
    let worker = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    assert_eq!(
        open(&worker, &a.own, &id).await.unwrap().as_bytes(),
        source().as_slice()
    );
    assert_eq!(snapshot(f.root.path(), A), after);
    assert_eq!(wrapped(f.root.path(), A), master);
    worker.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn outgoing_archive_rejects_unpromoted_foreign_scopes_and_nonattachment_history() {
    let (f, a, b) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(open(&f.alice, &a.own, &id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), before);
    let c = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
    f.alice
        .accept_epoch_blob_lease(controls::observed(
            &c,
            controls::grant(&c, v2::BlobLeaseState::Ready),
        ))
        .await
        .unwrap();
    f.alice.promote_epoch_attachment(id.clone()).await.unwrap();
    let before = snapshot(f.root.path(), A);
    for (generation, chat, msg) in [
        (b.own.generation_id.clone(), CHAT.into(), id.clone()),
        (
            a.own.generation_id.clone(),
            Uuid::new_v4().to_string(),
            id.clone(),
        ),
        (
            a.own.generation_id.clone(),
            CHAT.into(),
            Uuid::new_v4().to_string(),
        ),
    ] {
        assert!(
            f.alice
                .open_archived_epoch_attachment(generation, chat, msg)
                .await
                .is_err()
        );
    }
    assert!(open(&f.bob, &b.own, &id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), before);
    let other = CryptoCoordinator::start(
        crate::encryption::protected::test_open(
            f.root.path(),
            StoreScope::new("https://other.example.test", A).unwrap(),
            true,
        ),
        a.own.clone(),
    )
    .await
    .unwrap();
    assert!(open(&other, &a.own, &id).await.is_err());
    other.shutdown().await.unwrap();
    let p = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "ordinary text"))
        .await
        .unwrap();
    let text = p.request().envelope.as_ref().unwrap().message_id.clone();
    let before = snapshot(f.root.path(), A);
    assert!(open(&f.alice, &a.own, &text).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn outgoing_archive_reads_frozen_and_own_closed_history_without_upload_or_send_resume() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let p = promote(&f.alice, &id).await;
    f.alice.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert_eq!(
        open(&w, &a.own, &id).await.unwrap().as_bytes(),
        source().as_slice()
    );
    assert_eq!(
        w.epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap()
            .request(),
        p.request()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    let mut closed = f.a.clone();
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_at_unix_ms = 1_700_000_000_000;
    closed.closed_generation_id = a.own.generation_id.clone();
    index(&w, &closed).await;
    let before = snapshot(f.root.path(), A);
    assert_eq!(
        open(&w, &a.own, &id).await.unwrap().as_bytes(),
        source().as_slice()
    );
    assert!(matches!(
        w.epoch_attachment_outgoing(id.clone()).await,
        Err(CryptoError::ArchivedGeneration)
    ));
    assert!(w.epoch_attachment_control(id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn outgoing_archive_corrupt_spool_and_inconsistent_promotion_fail_without_repair() {
    for mode in [
        "missing chunks",
        "ciphertext",
        "manifest",
        "private journal",
        "history",
        "promotion pin",
    ] {
        let (f, a, _) = prepared().await;
        let id = Uuid::new_v4().to_string();
        promote(&f.alice, &id).await;
        f.alice.shutdown().await.unwrap();
        let mut s = store(f.root.path(), A, false);
        if mode == "history" || mode == "promotion pin" {
            s.transaction_with::<_, CryptoError>(|tx| {
                let mut l = outgoing::load_link(tx, &a.own, &id)?;
                if mode == "history" {
                    let key = outgoing::record_key(&a.own, RecordKind::History, &l.id)?;
                    let bytes = tx.get(&key)?.unwrap();
                    let mut p = v2::EncryptedPayload::decode(bytes.as_slice()).unwrap();
                    let Some(v2::encrypted_payload::Content::Attachment(a)) = p.content.as_mut()
                    else {
                        panic!()
                    };
                    a.caption.push_str(" changed");
                    tx.put(&key, &p.encode_to_vec())?;
                } else {
                    l.attachment_preparation = None;
                    tx.put(&outgoing::link_key(&a.own, &id)?, &l.encode_to_vec())?;
                }
                Ok(())
            })
            .unwrap();
        } else {
            let db = raw(f.root.path(), A);
            match mode {
                "missing chunks" => {
                    db.execute("DELETE FROM protected_attachment_chunks", [])
                        .unwrap();
                }
                "ciphertext" => {
                    db.execute(
                        "UPDATE protected_attachment_chunks SET payload=zeroblob(length(payload))",
                        [],
                    )
                    .unwrap();
                }
                "manifest" => {
                    db.execute(
                        "UPDATE protected_attachment_blobs SET blob_id=?1",
                        [Uuid::new_v4().to_string()],
                    )
                    .unwrap();
                }
                "private journal" => {
                    db.execute("UPDATE protected_records SET payload=zeroblob(length(payload)) WHERE record_id=?1",[format!("attachment-prepare:{id}")]).unwrap();
                }
                _ => unreachable!(),
            }
        }
        drop(s);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
            .await
            .unwrap();
        let before = snapshot(f.root.path(), A);
        assert!(open(&w, &a.own, &id).await.is_err(), "{mode}");
        assert_eq!(snapshot(f.root.path(), A), before, "{mode} repaired state");
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
