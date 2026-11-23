use super::controls::{grant, observed};
use super::*;
use crate::encryption::native::EpochObservation;

fn queued(v: EpochAttachmentPromotion) -> EpochOutgoingPlan {
    match v {
        EpochAttachmentPromotion::Queued(p) => *p,
        _ => panic!("expected committed attachment outbox"),
    }
}
async fn ready(w: &CryptoCoordinator, id: &str) {
    let p = w.epoch_attachment_control(id.into()).await.unwrap();
    w.accept_epoch_blob_lease(EpochObservation::test_observation(
        p.store_scope().clone(),
        p.clone(),
        grant(&p, v2::BlobLeaseState::Ready),
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn expected_handoff_binds_current_operation_but_preserves_an_existing_commit() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    let old = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
    let mut expired = grant(&old, v2::BlobLeaseState::Ready);
    expired.observed_at_unix_ms = expired.expires_at_unix_ms;
    expired.expired = true;
    f.alice
        .accept_epoch_blob_lease(observed(&old, expired))
        .await
        .unwrap();
    let current = f.alice.renew_epoch_blob_control(old.clone()).await.unwrap();
    f.alice
        .accept_epoch_blob_lease(observed(
            &current,
            grant(&current, v2::BlobLeaseState::Ready),
        ))
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice.promote_epoch_attachment_if(old.clone()).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    let committed = queued(f.alice.promote_epoch_attachment_if(current).await.unwrap());
    let after = snapshot(f.root.path(), A);
    assert_eq!(
        queued(f.alice.promote_epoch_attachment_if(old).await.unwrap()).request(),
        committed.request()
    );
    assert_eq!(snapshot(f.root.path(), A), after);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn expected_handoff_cannot_cross_accounts_even_with_the_same_message_uuid() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    prepare(&f.bob, &id).await.unwrap();
    ready(&f.bob, &id).await;
    let foreign = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
    let before = snapshot(f.root.path(), B);
    assert!(matches!(
        f.bob.promote_epoch_attachment_if(foreign).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), B), before);
    assert!(f.bob.epoch_attachment_outgoing(id).await.unwrap().is_none());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
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
async fn promotion_commits_signal_file_cohort_and_reopens_exactly() {
    let (f, a, b) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let plan = prepare(&f.alice, &id).await.unwrap();
    let blob = f.alice.prepared_epoch_attachment_blob(plan).await.unwrap();
    let exact = blob.as_bytes().to_vec();
    drop(blob);
    ready(&f.alice, &id).await;
    let chunks = cohort(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    assert!(
        f.alice
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    let p = queued(f.alice.promote_epoch_attachment(id.clone()).await.unwrap());
    let envelope = p.request().envelope.as_ref().unwrap();
    assert_eq!(envelope.protocol_version, 3);
    assert_eq!(
        envelope.blob.as_ref().unwrap().sha256,
        Sha256::digest(&exact).as_slice()
    );
    assert!(p.needs_send());
    let committed = snapshot(f.root.path(), A);
    for _ in 0..2 {
        assert_eq!(
            queued(f.alice.promote_epoch_attachment(id.clone()).await.unwrap()).request(),
            p.request()
        );
        assert_eq!(
            f.alice
                .epoch_attachment_outgoing(id.clone())
                .await
                .unwrap()
                .unwrap()
                .request(),
            p.request()
        );
        assert_eq!(
            f.alice
                .epoch_outgoing_plan(id.clone())
                .await
                .unwrap()
                .request(),
            p.request()
        );
        assert_eq!(snapshot(f.root.path(), A), committed);
    }
    assert_eq!(cohort(f.root.path(), A), chunks);
    assert_eq!(wrapped(f.root.path(), A), master);
    // The old preparation/upload route must not become a bypass after promotion.
    assert!(matches!(
        f.alice.prepared_epoch_attachment(id.clone()).await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.alice.epoch_attachment_control(id.clone()).await,
        Err(CryptoError::Conflict)
    ));
    assert!(
        f.alice
            .epoch_attachment_candidates()
            .await
            .unwrap()
            .is_empty()
    );
    let d = v2::Delivery {
        sequence: 1,
        envelope: Some(envelope.clone()),
        envelope_digest: data::envelope_digest(envelope).unwrap().to_vec(),
        expires_at_unix_ms: receipt(&p).expires_at_unix_ms,
    };
    let (payload, ack) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let part = part(&payload).unwrap();
    let binding =
        attachment::AttachmentBinding::new(CHAT, 0, &id, &part.blob_id, &a.own, &b.own).unwrap();
    let descriptor = AttachmentDescriptor::decode_from_signal(&part.private_descriptor).unwrap();
    assert_eq!(
        attachment::open(&binding, &descriptor, &exact)
            .unwrap()
            .as_slice(),
        source().as_slice()
    );
    assert_eq!(ack.request().messages[0].envelope_digest, d.envelope_digest);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    assert_eq!(
        w.epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap()
            .request(),
        p.request()
    );
    assert_eq!(
        queued(w.promote_epoch_attachment(id).await.unwrap()).request(),
        p.request()
    );
    assert_eq!(snapshot(f.root.path(), A), committed);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn promotion_requires_durable_ready_and_never_adopts_unexplained_linked() {
    for kind in [
        "missing",
        "reserved",
        "writing",
        "expired",
        "linked",
        "deleting",
        "reclaimed",
    ] {
        let (f, _, _) = prepared().await;
        let id = Uuid::new_v4().to_string();
        prepare(&f.alice, &id).await.unwrap();
        if kind != "missing" {
            let p = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
            let state = match kind {
                "reserved" => v2::BlobLeaseState::Reserved,
                "writing" => v2::BlobLeaseState::Writing,
                "expired" => v2::BlobLeaseState::Ready,
                "linked" => v2::BlobLeaseState::Linked,
                "deleting" => v2::BlobLeaseState::Deleting,
                _ => v2::BlobLeaseState::Reclaimed,
            };
            let mut g = grant(&p, state);
            if kind == "expired" {
                g.observed_at_unix_ms = g.expires_at_unix_ms;
                g.expired = true;
            }
            f.alice
                .accept_epoch_blob_lease(observed(&p, g))
                .await
                .unwrap();
        }
        let before = snapshot(f.root.path(), A);
        assert!(
            matches!(
                f.alice.promote_epoch_attachment(id).await,
                Err(CryptoError::Conflict)
            ),
            "{kind}"
        );
        assert_eq!(snapshot(f.root.path(), A), before, "{kind}");
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn ready_without_signal_returns_bootstrap_required_without_acceptance_or_changes() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    ready(&f.alice, &id).await;
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice.promote_epoch_attachment(id.clone()).await.unwrap(),
        EpochAttachmentPromotion::BootstrapRequired
    ));
    assert!(
        f.alice
            .epoch_attachment_outgoing(id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_commit_failure_rolls_back_signal_history_outbox_and_promotion_marker() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    ready(&f.alice, &id).await;
    let before = snapshot(f.root.path(), A);
    let chunks = cohort(f.root.path(), A);
    let db = raw(f.root.path(), A);
    db.execute_batch("CREATE TABLE promo_parent(id INTEGER PRIMARY KEY);CREATE TABLE promo_child(id INTEGER REFERENCES promo_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER promo_fault AFTER UPDATE ON protected_records WHEN NEW.kind=13 AND NEW.record_id LIKE 'attachment-prepare:%' BEGIN INSERT INTO promo_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.alice.promote_epoch_attachment(id.clone()).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(cohort(f.root.path(), A), chunks);
    assert!(
        f.alice
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    db.execute_batch("DROP TRIGGER promo_fault").unwrap();
    drop(db);
    let p = queued(f.alice.promote_epoch_attachment(id).await.unwrap());
    assert_eq!(
        p.request().envelope.as_ref().unwrap().message_type,
        CiphertextType::PrekeySignal as i32
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_outbox_survives_frozen_scope_missing_chunks_and_descriptor_receipt() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    ready(&f.alice, &id).await;
    let p = queued(f.alice.promote_epoch_attachment(id.clone()).await.unwrap());
    f.alice.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    raw(f.root.path(), A)
        .execute("DELETE FROM protected_attachment_chunks", [])
        .unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert_eq!(
        w.epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap()
            .request(),
        p.request()
    );
    assert_eq!(
        queued(w.promote_epoch_attachment(id.clone()).await.unwrap()).request(),
        p.request()
    );
    assert_eq!(
        w.epoch_outbox_page(None).await.unwrap().messages[0].request(),
        p.request()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    w.accept_epoch_send_receipt(EpochObservation::test_observation(
        a.scope,
        p.clone(),
        receipt(&p),
    ))
    .await
    .unwrap();
    let saved = w.epoch_attachment_outgoing(id).await.unwrap().unwrap();
    assert!(!saved.needs_send());
    assert_eq!(saved.request(), p.request());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepromotion_freeze_and_corrupt_chunks_cannot_consume_signal() {
    for frozen in [false, true] {
        let (f, a, _) = prepared().await;
        let id = Uuid::new_v4().to_string();
        prepare(&f.alice, &id).await.unwrap();
        ready(&f.alice, &id).await;
        f.alice.shutdown().await.unwrap();
        if frozen {
            rewrite_frozen(f.root.path(), &f.a, true);
        } else {
            raw(f.root.path(),A).execute("UPDATE protected_attachment_chunks SET payload=zeroblob(length(payload)) WHERE chunk_index=0",[]).unwrap();
        }
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
            .await
            .unwrap();
        let before = snapshot(f.root.path(), A);
        assert!(w.promote_epoch_attachment(id.clone()).await.is_err());
        assert!(w.epoch_attachment_outgoing(id).await.unwrap().is_none());
        assert_eq!(snapshot(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn retained_read_is_account_scoped_and_does_not_need_the_file_buffer_permit() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    ready(&f.alice, &id).await;
    let p = queued(f.alice.promote_epoch_attachment(id.clone()).await.unwrap());
    let permit = f
        .alice
        .attachment_budget
        .clone()
        .try_acquire_owned()
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert_eq!(
        f.alice
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap()
            .request(),
        p.request()
    );
    assert!(
        f.bob
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        f.bob.promote_epoch_attachment(id).await,
        Err(CryptoError::MessageUnavailable)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    drop(permit);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_and_shared_readers_reject_partial_or_mixed_promotion_cohorts_without_repair() {
    for field in [
        "source", "payload", "digest", "marker", "pin", "journal", "history",
    ] {
        let (f, a, _) = prepared().await;
        let id = Uuid::new_v4().to_string();
        prepare(&f.alice, &id).await.unwrap();
        ready(&f.alice, &id).await;
        queued(f.alice.promote_epoch_attachment(id.clone()).await.unwrap());
        f.alice.shutdown().await.unwrap();
        if field == "journal" {
            let db = raw(f.root.path(), A);
            // Deliberately corrupt the disposable SQLite from outside the
            // store API. Normal FK enforcement correctly refuses this delete.
            db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            db.execute(
                "DELETE FROM protected_records WHERE kind=13 AND record_id=?1",
                [format!("attachment-prepare:{id}")],
            )
            .unwrap();
        } else {
            let mut db = store(f.root.path(), A, false);
            db.transaction_with::<_, CryptoError>(|tx| {
                let mut j = load_retained_journal(tx, &a.own, &id)?.unwrap();
                let mut link = outgoing::load_link(tx, &a.own, &id)?;
                match field {
                    "source" => j.source_digest[0] ^= 1,
                    "payload" => {
                        if let Some(v2::encrypted_payload::Content::Attachment(p)) =
                            j.payload.as_mut().unwrap().content.as_mut()
                        {
                            p.caption.push('x')
                        }
                    }
                    "digest" => j.promoted_digest.as_mut().unwrap()[0] ^= 1,
                    "marker" => j.promoted_digest = None,
                    "pin" => link.attachment_preparation = None,
                    "history" => {
                        let k = outgoing::record_key(&a.own, RecordKind::History, &link.id)?;
                        let mut p =
                            v2::EncryptedPayload::decode(tx.get(&k)?.unwrap().as_slice()).unwrap();
                        if let Some(v2::encrypted_payload::Content::Attachment(p)) =
                            p.content.as_mut()
                        {
                            p.caption.push('x')
                        }
                        tx.put(&k, &Zeroizing::new(p.encode_to_vec()))?;
                        tx.project_recovery(&k, false)?;
                    }
                    _ => unreachable!(),
                }
                tx.put(&key(&a.own, &id)?, &Zeroizing::new(j.encode_to_vec()))?;
                tx.put(&outgoing::link_key(&a.own, &id)?, &link.encode_to_vec())?;
                Ok(())
            })
            .unwrap();
        }
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
            .await
            .unwrap();
        let before = snapshot(f.root.path(), A);
        assert!(
            w.epoch_attachment_outgoing(id.clone()).await.is_err(),
            "{field}"
        );
        assert!(w.epoch_outgoing_plan(id.clone()).await.is_err(), "{field}");
        assert!(w.promote_epoch_attachment(id).await.is_err(), "{field}");
        assert_eq!(snapshot(f.root.path(), A), before, "{field}");
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[test]
fn promotion_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_PROMOTION_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_PROMOTION_CRASH_AFTER").unwrap() == "1";
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
            let id = format!("v2:{CHAT}:0000000000000000:{CRASH_ID}");
            db.test_exit_on_record(
                outgoing::record_key(&own, RecordKind::Outbox, &id).unwrap(),
                after,
            );
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let _ = w.promote_epoch_attachment(CRASH_ID.into()).await;
            panic!("promotion crash hook did not fire");
        });
}

#[tokio::test]
async fn process_exit_around_promotion_commit_recovers_file_and_only_one_signal_outbox() {
    for after in [false, true] {
        let (f, a, _) = prepared().await;
        prepare(&f.alice, CRASH_ID).await.unwrap();
        ready(&f.alice, CRASH_ID).await;
        f.alice.shutdown().await.unwrap();
        let before = snapshot(f.root.path(), A);
        let blobs = cohort(f.root.path(), A);
        let child=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","encryption::signal_state::epoch_state::attachment_prepare::tests::promotion::promotion_crash_child","--nocapture"])
            .env("ZCHAT_PROMOTION_CRASH_PATH",f.root.path()).env("ZCHAT_PROMOTION_CRASH_AFTER",if after {"1"} else {"0"}).status().unwrap();
        assert_eq!(child.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
            .await
            .unwrap();
        let committed = snapshot(f.root.path(), A);
        let retained = w.epoch_attachment_outgoing(CRASH_ID.into()).await.unwrap();
        if after {
            assert!(retained.is_some());
        } else {
            assert!(retained.is_none());
            assert_eq!(committed, before);
        }
        let p = queued(w.promote_epoch_attachment(CRASH_ID.into()).await.unwrap());
        if let Some(old) = retained {
            assert_eq!(old.request(), p.request());
            assert_eq!(snapshot(f.root.path(), A), committed);
        }
        assert_eq!(w.epoch_outbox_page(None).await.unwrap().messages.len(), 1);
        assert_eq!(cohort(f.root.path(), A), blobs);
        assert_eq!(
            queued(w.promote_epoch_attachment(CRASH_ID.into()).await.unwrap()).request(),
            p.request()
        );
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
