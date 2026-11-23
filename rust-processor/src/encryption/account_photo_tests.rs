//! Public native client -> account input lane -> existing HTTP/Signal pipeline.
use super::*;
use crate::{compression::PhotoChoice, encryption::photo_preparation::PhotoAttachmentError};

// Production intentionally has one process-wide E2EE codec slot. Parallel test
// cases must not accidentally claim it from each other.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn initial_catalog_failure_is_retryable_and_shared_text_bootstrap_keeps_exact_uuid() {
    let _serial = SERIAL.lock().await;
    let f = fixture().await;
    let own = f.a.own_device.clone().unwrap();
    let rpc = Arc::new(Probe::default());
    *rpc.lookup_reply.lock().unwrap() = Some(f.a.clone());
    *rpc.acquire_bundle.lock().unwrap() = Some(f.bundle.clone());
    let owner = launch(f.alice, &rpc);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    assert!(matches!(
        c.ensure_attachment_catalog(CHAT).await,
        Err(AccountError::Rpc(RpcError::Transport))
    ));
    assert!(c.conversation(CHAT).await.unwrap().is_none());
    assert!(rpc.acquire_requests.lock().unwrap().is_empty());
    rpc.initial_accept.store(true, Ordering::SeqCst);
    c.ensure_attachment_catalog(CHAT).await.unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    let queued = c
        .send_text(CHAT, &id, "first text after public catalogue")
        .await
        .unwrap();
    assert_eq!(queued.message_id, id);
    assert_eq!(queued.crypto_epoch, 0);
    assert_eq!(
        c.send_text(CHAT, &id, "first text after public catalogue")
            .await
            .unwrap(),
        queued
    );
    assert_eq!(
        rpc.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|&&v| v == "initial")
            .count(),
        2
    );
    assert_eq!(rpc.acquire_requests.lock().unwrap().len(), 1);
    owner.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own)
        .await
        .unwrap();
    assert!(w.epoch_outgoing_plan(id).await.unwrap().needs_send());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

fn photo() -> Zeroizing<Vec<u8>> {
    let mut out = Vec::new();
    {
        let mut png = png::Encoder::new(&mut out, 2, 3);
        png.set_color(png::ColorType::Rgb);
        png.add_text_chunk("Comment".into(), "PRIVATE-CAMERA-GPS-LOCATION".into())
            .unwrap();
        let mut writer = png.write_header().unwrap();
        writer.write_image_data(&[20, 40, 60].repeat(6)).unwrap();
    }
    Zeroizing::new(out)
}
async fn prepare_photo(
    c: &AccountClient,
    id: &str,
    bytes: Zeroizing<Vec<u8>>,
    choice: PhotoChoice,
) -> Result<crate::encryption::signal_state::EpochAttachmentPreparation, AccountError> {
    c.prepare_photo(
        CHAT,
        id,
        PhotoSource {
            bytes,
            filename: Zeroizing::new("camera.untrusted-extension".into()),
            caption: Zeroizing::new("private caption".into()),
            choice,
        },
    )
    .await
}

#[tokio::test]
async fn native_photo_input_errors_conflict_retry_reopen_and_stale_client_keep_the_same_preparation()
 {
    let _serial = SERIAL.lock().await;
    let (f, a, _) = prepared().await;
    let rpc = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &rpc);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    entered(&rpc.pending_started).await;
    for bytes in [
        Zeroizing::new(Vec::new()),
        Zeroizing::new(vec![0; crate::compression::MAX_INPUT_BYTES + 1]),
    ] {
        assert!(matches!(
            prepare_photo(
                &c,
                &uuid::Uuid::new_v4().to_string(),
                bytes,
                PhotoChoice::default()
            )
            .await,
            Err(AccountError::Invalid)
        ));
    }
    assert!(matches!(
        prepare_photo(&c, "not-a-uuid", photo(), PhotoChoice::default()).await,
        Err(AccountError::Invalid)
    ));
    assert!(matches!(
        prepare_photo(
            &c,
            &uuid::Uuid::new_v4().to_string(),
            Zeroizing::new(vec![1; 32]),
            PhotoChoice::default()
        )
        .await,
        Err(AccountError::Photo(_))
    ));
    assert!(matches!(
        prepare_photo(
            &c,
            &uuid::Uuid::new_v4().to_string(),
            photo(),
            PhotoChoice::Original {
                metadata_warning_acknowledged: false
            }
        )
        .await,
        Err(AccountError::Photo(PhotoAttachmentError::Preparation(
            crate::compression::PhotoPreparationError::OriginalNeedsConfirmation
        )))
    ));
    assert!(matches!(
        c.prepare_photo(
            CHAT,
            &uuid::Uuid::new_v4().to_string(),
            PhotoSource {
                bytes: photo(),
                filename: Zeroizing::new("../private.png".into()),
                caption: Zeroizing::new(String::new()),
                choice: PhotoChoice::default(),
            }
        )
        .await,
        Err(AccountError::Photo(PhotoAttachmentError::Metadata))
    ));
    let id = uuid::Uuid::new_v4().to_string();
    let EpochAttachmentPreparation::Prepared(plan) =
        prepare_photo(&c, &id, photo(), PhotoChoice::default())
            .await
            .unwrap()
    else {
        panic!("missing native preparation");
    };
    let identity = (
        plan.blob_id().to_owned(),
        *plan.blob_digest(),
        plan.blob_len(),
    );
    let retained = c.prepared_attachment(&id).await.unwrap().unwrap();
    assert_eq!(
        (
            retained.blob_id(),
            retained.blob_digest(),
            retained.blob_len()
        ),
        (identity.0.as_str(), &identity.1, identity.2)
    );
    // Both a bad source and an unacknowledged different choice would fail in a
    // codec. Occupied UUID must instead be rejected BEFORE opening that codec.
    for (bytes, choice) in [
        (Zeroizing::new(vec![1; 32]), PhotoChoice::default()),
        (
            photo(),
            PhotoChoice::Original {
                metadata_warning_acknowledged: false,
            },
        ),
    ] {
        assert!(matches!(
            prepare_photo(&c, &id, bytes, choice).await,
            Err(AccountError::Crypto(CryptoError::Conflict))
        ));
    }
    c.send_text(CHAT, &uuid::Uuid::new_v4().to_string(), "still responsive")
        .await
        .unwrap();
    assert_eq!(rpc.active.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    assert!(matches!(
        prepare_photo(
            &c,
            &uuid::Uuid::new_v4().to_string(),
            photo(),
            PhotoChoice::default()
        )
        .await,
        Err(AccountError::Stopped)
    ));
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    let retained = w.prepared_epoch_attachment(id).await.unwrap().unwrap();
    assert_eq!(
        (
            retained.blob_id(),
            retained.blob_digest(),
            retained.blob_len()
        ),
        (identity.0.as_str(), &identity.1, identity.2)
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn account_prepared_attachments_automatically_upload_and_decrypt_with_expected_bytes() {
    let _serial = SERIAL.lock().await;
    use crate::encryption::signal_state::EpochAttachmentPromotion;
    for (choice, voice) in [
        (Some(PhotoChoice::default()), false),
        (
            Some(PhotoChoice::Clean {
                quality: Some(75),
                max_dimension: 0,
            }),
            false,
        ),
        (
            Some(PhotoChoice::Original {
                metadata_warning_acknowledged: true,
            }),
            false,
        ),
        (None, false),
        (None, true),
    ] {
        let f = fixture().await;
        let own = f.a.own_device.clone().unwrap();
        let b = initialize(&f.bob).await;
        let rpc = Arc::new(Probe::default());
        *rpc.lookup_reply.lock().unwrap() = Some(f.a.clone());
        *rpc.acquire_bundle.lock().unwrap() = Some(f.bundle.clone());
        rpc.initial_accept.store(true, Ordering::SeqCst);
        rpc.send_accept.store(true, Ordering::SeqCst);
        let body = Arc::new(BodyProbe::default());
        let owner = launch_upload(f.alice, &rpc, &body);
        let c = owner.client();
        phase(&c, AccountPhase::Running).await;
        let source = if voice {
            Zeroizing::new(crate::encryption::voice_format::WEBVIEW_SILENCE.to_vec())
        } else {
            photo()
        };
        let clean = matches!(choice, Some(PhotoChoice::Clean { .. }));
        let expected = if let Some(choice) = choice {
            crate::compression::prepare_photo(&source, choice)
                .unwrap()
                .into_bytes()
        } else {
            source.to_vec()
        };
        let id = uuid::Uuid::new_v4().to_string();
        assert_eq!(
            c.attachment_input_state(CHAT, &id).await.unwrap(),
            crate::encryption::signal_state::EpochAttachmentInputState::Vacant
        );
        assert!(c.conversation(CHAT).await.unwrap().is_none());
        c.ensure_attachment_catalog(CHAT).await.unwrap();
        c.ensure_attachment_catalog(CHAT).await.unwrap();
        assert_eq!(
            rpc.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|&&v| v == "initial")
                .count(),
            1
        );
        assert!(rpc.acquire_requests.lock().unwrap().is_empty());
        let result = if let Some(choice) = choice {
            let admission = c.admit_photo(CHAT, &id, source.len()).unwrap();
            c.prepare_admitted_photo(
                admission,
                PhotoSource {
                    bytes: source,
                    filename: Zeroizing::new("camera.untrusted-extension".into()),
                    caption: Zeroizing::new("private caption".into()),
                    choice,
                },
            )
            .await
        } else if voice {
            let invalid = Zeroizing::new(b"not an audio recording".to_vec());
            let admission = c.admit_voice(CHAT, &id, invalid.len(), 0).unwrap();
            assert!(matches!(
                c.prepare_admitted_voice(admission, invalid, Zeroizing::new(String::new()))
                    .await,
                Err(AccountError::VoiceFormat)
            ));
            assert!(matches!(
                c.admit_voice(
                    CHAT,
                    &id,
                    crate::encryption::attachment_payload::MAX_MEDIA_BYTES + 1,
                    0
                ),
                Err(AccountError::Invalid)
            ));
            let admission = c.admit_voice(CHAT, &id, source.len(), 0).unwrap();
            c.prepare_admitted_voice(admission, source, Zeroizing::new("private caption".into()))
                .await
        } else {
            let file = || FileSource {
                bytes: source.clone(),
                filename: Zeroizing::new("camera.untrusted-extension".into()),
                caption: Zeroizing::new("private caption".into()),
            };
            let len = source.len();
            assert!(matches!(
                c.admit_file(
                    CHAT,
                    &id,
                    crate::encryption::attachment::MAX_PLAINTEXT_BYTES + 1,
                    0
                ),
                Err(AccountError::Invalid)
            ));
            let wrong = c.admit_file(CHAT, &id, len, 1).unwrap();
            assert!(matches!(
                c.admit_photo(CHAT, &id, len),
                Err(AccountError::Busy)
            ));
            assert!(matches!(
                c.prepare_admitted_file(wrong, file()).await,
                Err(AccountError::Crypto(CryptoError::Conflict))
            ));
            assert_eq!(
                c.attachment_input_state(CHAT, &id).await.unwrap(),
                crate::encryption::signal_state::EpochAttachmentInputState::Vacant
            );
            let admission = c.admit_file(CHAT, &id, len, 0).unwrap();
            c.prepare_admitted_file(admission, file()).await
        };
        assert!(matches!(
            result.unwrap(),
            EpochAttachmentPreparation::Prepared(_)
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if c.outgoing(&id).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        owner.shutdown().await.unwrap();
        assert_eq!(
            body.ids.lock().unwrap().as_slice(),
            std::slice::from_ref(&id)
        );
        let ciphertext = body.ciphertexts.lock().unwrap()[0].clone();
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), own)
            .await
            .unwrap();
        let outgoing = w
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap();
        let env = outgoing.request().envelope.as_ref().unwrap();
        let delivery = v2::Delivery {
            sequence: 1,
            envelope: Some(env.clone()),
            envelope_digest: crate::encryption::epoch_contract::envelope_digest(env)
                .unwrap()
                .to_vec(),
            expires_at_unix_ms: 1_700_000_000_000
                + crate::encryption::contract::MESSAGE_RETENTION_MS,
        };
        let (received, ack) = f
            .bob
            .receive_epoch(
                b.clone(),
                super::super::super::incoming::tests::delivery(&b, &delivery),
            )
            .await
            .unwrap();
        assert_eq!(ack.message_id(), id);
        let Some(v2::encrypted_payload::Content::Attachment(part)) = &received.content else {
            panic!("not an attachment");
        };
        assert_eq!(part.caption, "private caption");
        if choice.is_some() {
            assert_eq!(part.kind, v2::AttachmentKind::Photo as i32);
            assert_eq!(part.photo_metadata_stripped, Some(clean));
            let compressed = matches!(
                choice,
                Some(PhotoChoice::Clean {
                    quality: Some(_),
                    ..
                })
            );
            assert_eq!(
                (part.filename.as_str(), part.mime_type.as_str()),
                if compressed {
                    ("camera.jpg", "image/jpeg")
                } else {
                    ("camera.png", "image/png")
                }
            );
        } else if voice {
            assert_eq!(part.kind, v2::AttachmentKind::Voice as i32);
            assert_eq!(part.photo_metadata_stripped, None);
            assert_eq!(part.filename, "voice.webm");
            assert_eq!(part.mime_type, "audio/webm");
        } else {
            assert_eq!(part.kind, v2::AttachmentKind::File as i32);
            assert_eq!(part.photo_metadata_stripped, None);
            assert_eq!(part.filename, "camera.untrusted-extension");
            assert_eq!(part.mime_type, "application/octet-stream");
        }
        let incoming = f
            .bob
            .epoch_attachment_download_plan(id.clone())
            .await
            .unwrap();
        use super::super::super::attachment_download::tests::{append, begin, finish};
        let mut run = begin(&f.bob, incoming).await;
        append(&f.bob, &mut run, &ciphertext).await.unwrap();
        let cached = finish(&f.bob, run).await.unwrap();
        let file = f.bob.open_cached_epoch_attachment(cached).await.unwrap();
        assert_eq!(file.as_bytes(), expected);
        if clean {
            let marker = b"PRIVATE-CAMERA-GPS-LOCATION";
            assert!(!file.as_bytes().windows(marker.len()).any(|b| b == marker));
        }
        drop(file);
        // Already promoted data remains immutable and cannot become a new photo.
        assert!(matches!(
            w.new_epoch_photo_input(CHAT.into(), id.clone()).await,
            Err(CryptoError::Conflict)
        ));
        assert!(matches!(
            w.promote_epoch_attachment(id).await.unwrap(),
            EpochAttachmentPromotion::Queued(_)
        ));
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn native_photo_input_admission_binds_client_length_and_releases_store_before_source_is_copied()
 {
    let _serial = SERIAL.lock().await;
    let (f, a, _) = prepared().await;
    let owner = launch(f.alice, &Arc::new(Probe::default()));
    let other = launch(f.bob, &Arc::new(Probe::default()));
    let c = owner.client();
    let b = other.client();
    phase(&c, AccountPhase::Running).await;
    phase(&b, AccountPhase::Running).await;
    let id = uuid::Uuid::new_v4().to_string();
    let bytes = photo();
    let len = bytes.len();
    for len in [0, crate::compression::MAX_INPUT_BYTES + 1] {
        assert!(matches!(
            c.admit_photo(CHAT, &id, len),
            Err(AccountError::Invalid)
        ));
    }
    let admission = c.admit_photo(CHAT, &id, len).unwrap();
    assert!(matches!(
        c.admit_photo(CHAT, &id, len),
        Err(AccountError::Busy)
    ));
    assert!(matches!(
        b.admit_photo(CHAT, &id, len),
        Err(AccountError::Busy)
    ));
    let input = |bytes| PhotoSource {
        bytes,
        filename: Zeroizing::new("photo.png".into()),
        caption: Zeroizing::new(String::new()),
        choice: PhotoChoice::default(),
    };
    assert!(matches!(
        b.prepare_admitted_photo(admission, input(photo())).await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    let admission = c.admit_photo(CHAT, &id, len + 1).unwrap();
    assert!(matches!(
        c.prepare_admitted_photo(admission, input(photo())).await,
        Err(AccountError::Invalid)
    ));
    let admission = c.admit_photo(CHAT, &id, len).unwrap();
    owner.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    assert!(matches!(
        c.prepare_admitted_photo(admission, input(bytes)).await,
        Err(AccountError::Stopped)
    ));
    assert_eq!(
        w.epoch_attachment_input_state(CHAT.into(), id.clone())
            .await
            .unwrap(),
        crate::encryption::signal_state::EpochAttachmentInputState::Vacant
    );
    drop(b.admit_photo(CHAT, &id, len).unwrap());
    w.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}
