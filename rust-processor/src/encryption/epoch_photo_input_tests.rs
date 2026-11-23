//! Real Signal/private descriptor/cache path, with preprocessing invoked once.
use super::*;
use crate::{compression::PhotoChoice, encryption::photo_preparation::prepare_photo_attachment};

#[tokio::test]
async fn prepared_photos_bind_metadata_before_encryption_and_retained_retry_never_needs_codec() {
    use super::super::super::attachment_download::tests::{append, begin, finish};
    for original in [false, true] {
        let (f, a, b) = prepared().await;
        let mut source = Vec::new();
        {
            let mut png = png::Encoder::new(&mut source, 2, 3);
            png.set_color(png::ColorType::Rgb);
            png.add_text_chunk("Comment".into(), "PRIVATE-CAMERA-GPS-LOCATION".into())
                .unwrap();
            let mut writer = png.write_header().unwrap();
            writer.write_image_data(&[20, 40, 60].repeat(6)).unwrap();
        }
        let choice = if original {
            PhotoChoice::Original {
                metadata_warning_acknowledged: true,
            }
        } else {
            PhotoChoice::default()
        };
        let id = Uuid::new_v4().to_string();
        assert_eq!(
            f.alice
                .epoch_attachment_input_state(CHAT.into(), id.clone())
                .await
                .unwrap(),
            EpochAttachmentInputState::Vacant
        );
        let ticket = f
            .alice
            .new_epoch_photo_input(CHAT.into(), id.clone())
            .await
            .unwrap()
            .unwrap();
        let prepared = prepare_photo_attachment(
            &source,
            Zeroizing::new("snapshot.wrong-extension".into()),
            Zeroizing::new("private caption".into()),
            choice,
        )
        .unwrap();
        // Assert expected clear bytes independently, keeping the native opaque
        // handoff intact until the bound account consumes it.
        let expected = crate::compression::prepare_photo(&source, choice)
            .unwrap()
            .into_bytes();
        if original {
            assert_eq!(expected, source);
        } else {
            assert!(!expected.windows(7).any(|b| b == b"PRIVATE"));
        }
        let p = plan(
            f.alice
                .finish_epoch_photo_input(ticket, prepared)
                .await
                .unwrap(),
        );
        let blob = f.alice.prepared_epoch_attachment_blob(p).await.unwrap();
        assert_eq!(
            f.alice
                .epoch_attachment_input_state(CHAT.into(), id.clone())
                .await
                .unwrap(),
            EpochAttachmentInputState::Prepared { crypto_epoch: 0 }
        );
        let ciphertext = blob.as_bytes().to_vec();
        drop(blob);
        let c = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
        f.alice
            .accept_epoch_blob_lease(controls::observed(
                &c,
                controls::grant(&c, v2::BlobLeaseState::Ready),
            ))
            .await
            .unwrap();
        let EpochAttachmentPromotion::Queued(sent) =
            f.alice.promote_epoch_attachment(id.clone()).await.unwrap()
        else {
            panic!()
        };
        let envelope = sent.request().envelope.as_ref().unwrap();
        assert_eq!(
            f.alice
                .epoch_attachment_input_state(CHAT.into(), id.clone())
                .await
                .unwrap(),
            EpochAttachmentInputState::Outgoing {
                crypto_epoch: 0,
                stored_on_server: false,
                sealed_unsent: false
            }
        );
        let d = v2::Delivery {
            sequence: 1,
            envelope: Some(envelope.clone()),
            envelope_digest: data::envelope_digest(envelope).unwrap().to_vec(),
            expires_at_unix_ms: 1_700_000_000_000
                + crate::encryption::contract::MESSAGE_RETENTION_MS,
        };
        let (received, _ack) = f
            .bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        let a_part = part(&received).unwrap();
        assert_eq!(a_part.kind, AttachmentKind::Photo as i32);
        assert_eq!(a_part.photo_metadata_stripped, Some(!original));
        assert_eq!(a_part.mime_type, "image/png");
        assert_eq!(a_part.filename, "snapshot.png");
        let incoming = f
            .bob
            .epoch_attachment_download_plan(id.clone())
            .await
            .unwrap();
        let mut run = begin(&f.bob, incoming).await;
        append(&f.bob, &mut run, &ciphertext).await.unwrap();
        let cache = finish(&f.bob, run).await.unwrap();
        assert_eq!(
            f.bob
                .open_cached_epoch_attachment(cache)
                .await
                .unwrap()
                .as_bytes(),
            expected
        );
        let before = snapshot(f.root.path(), A);
        f.alice.shutdown().await.unwrap();
        let restored = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
            .await
            .unwrap();
        let retry = restored
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retry.request(), sent.request());
        assert!(retry.needs_send());
        assert!(matches!(
            restored.new_epoch_photo_input(CHAT.into(), id).await,
            Err(CryptoError::Conflict)
        ));
        assert_eq!(snapshot(f.root.path(), A), before);
        restored.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

fn private_photo() -> crate::encryption::photo_preparation::PreparedPhotoAttachment {
    let mut out = std::io::Cursor::new(Vec::new());
    image::DynamicImage::new_rgb8(2, 3)
        .write_to(&mut out, image::ImageFormat::Png)
        .unwrap();
    prepare_photo_attachment(
        &out.into_inner(),
        Zeroizing::new("photo.png".into()),
        Zeroizing::new(String::new()),
        PhotoChoice::default(),
    )
    .unwrap()
}

#[tokio::test]
async fn photo_ticket_rejects_other_account_origin_and_reopened_instance_without_holding_db_lease()
{
    let (f, a, _) = prepared().await;
    seed(&f.bob).await;
    let next = || Uuid::new_v4().to_string();
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), next())
        .await
        .unwrap()
        .unwrap();
    let before = snapshot(f.root.path(), B);
    assert!(matches!(
        f.bob
            .finish_epoch_photo_input(ticket, private_photo())
            .await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), B), before);
    let foreign = crate::encryption::protected::test_open(
        f.root.path(),
        StoreScope::new("https://other.example.test", A).unwrap(),
        true,
    );
    let foreign = CryptoCoordinator::start(foreign, a.own.clone())
        .await
        .unwrap();
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), next())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        foreign
            .finish_epoch_photo_input(ticket, private_photo())
            .await,
        Err(CryptoError::Conflict)
    ));
    foreign.shutdown().await.unwrap();
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), next())
        .await
        .unwrap()
        .unwrap();
    let before = snapshot(f.root.path(), A);
    let wrapped_before = wrapped(f.root.path(), A);
    // A held codec ticket owns only memory admission/public binding, not a
    // worker sender or DB/key lease. Reopening while it is alive must succeed.
    tokio::time::timeout(std::time::Duration::from_secs(5), f.alice.shutdown())
        .await
        .unwrap()
        .unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    assert!(matches!(
        w.finish_epoch_photo_input(ticket, private_photo()).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(wrapped(f.root.path(), A), wrapped_before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn photo_ticket_rechecks_signed_freeze_before_any_durable_preparation() {
    let (f, a, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), id.clone())
        .await
        .unwrap()
        .unwrap();
    f.alice.prepare_epoch_recovery(a).await.unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice
            .finish_epoch_photo_input(ticket, private_photo())
            .await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    assert!(matches!(
        f.alice.new_epoch_photo_input(CHAT.into(), id).await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn photo_ticket_rechecks_device_closure_before_any_durable_preparation() {
    let (f, _, _) = prepared().await;
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), Uuid::new_v4().to_string())
        .await
        .unwrap()
        .unwrap();
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
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice
            .finish_epoch_photo_input(ticket, private_photo())
            .await,
        Err(CryptoError::ConversationClosed)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn photo_ticket_no_catalog_busy_and_dropped_input_do_not_allocate_or_retain_file_memory() {
    let f = fixture().await;
    seed(&f.alice).await;
    let before = snapshot(f.root.path(), A);
    assert!(
        f.alice
            .new_epoch_photo_input(CHAT.into(), Uuid::new_v4().to_string())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    initialize(&f.alice).await;
    let before = snapshot(f.root.path(), A);
    let id = Uuid::new_v4().to_string();
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), id.clone())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        f.alice.new_epoch_photo_input(CHAT.into(), id.clone()).await,
        Err(CryptoError::Busy)
    ));
    assert!(matches!(
        f.alice.prepared_epoch_attachment(id.clone()).await,
        Err(CryptoError::Busy)
    ));
    // Crypto/control work is not serialized behind the held whole-file slot.
    assert!(
        f.alice
            .active_epoch_scope(CHAT.into())
            .await
            .unwrap()
            .is_some()
    );
    drop(ticket);
    assert!(
        f.alice
            .prepared_epoch_attachment(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    drop(
        f.alice
            .new_epoch_photo_input(CHAT.into(), id)
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn photo_ticket_checks_late_uuid_collision_in_the_preparation_transaction() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), id.clone())
        .await
        .unwrap()
        .unwrap();
    f.alice
        .prepare_epoch_text(
            CHAT.into(),
            id.clone(),
            Zeroizing::new("racing text".into()),
        )
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice
            .epoch_attachment_input_state(CHAT.into(), id.clone())
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.alice
            .finish_epoch_photo_input(ticket, private_photo())
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.alice.new_epoch_photo_input(CHAT.into(), id).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
