//! IPC framing and dispatch fences with a gated backend; no actual WebView/DPAPI.
use super::*;
use std::{
    sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};
use tauri::ipc::{InvokeResponseBody, IpcResponse};
use tokio::sync::Notify;
const CHAT: &str = "11111111-1111-4111-8111-111111111111";
const MESSAGE: &str = "22222222-2222-4222-8222-222222222222";
const REQUEST: &str = "33333333-3333-4333-8333-333333333333";

#[cfg(feature = "experimental-e2ee")]
#[test]
fn codec_failures_are_typed_and_do_not_expose_native_error_strings() {
    use rust_processor::{
        compression::PhotoPreparationError as P,
        encryption::{
            account_owner::AccountError as A, photo_preparation::PhotoAttachmentError as E,
        },
        errors::CompressionError as C,
    };
    for (error, expected) in [
        (E::Metadata, Reason::Invalid),
        (E::MediaTooLarge, Reason::PhotoOutputTooLarge),
        (
            E::Preparation(P::OriginalNeedsConfirmation),
            Reason::PhotoOriginalConsent,
        ),
        (
            E::Preparation(P::AnimationNeedsOriginal),
            Reason::PhotoAnimationOriginal,
        ),
        (
            E::Preparation(P::MultipageNeedsOriginal),
            Reason::PhotoMultipageOriginal,
        ),
        (
            E::Preparation(P::Codec(C::DecodeFailed("PRIVATE-CODEC-PATH".into()))),
            Reason::PhotoDecode,
        ),
        (
            E::Preparation(P::Codec(C::EncodeFailed("PRIVATE-CODEC-PATH".into()))),
            Reason::PhotoEncode,
        ),
        (
            E::Preparation(P::Codec(C::ImageTooLarge)),
            Reason::PhotoDimensions,
        ),
        (
            E::Preparation(P::Codec(C::UnsupportedFormat)),
            Reason::PhotoUnsupported,
        ),
    ] {
        let reason = photo_error(A::Photo(error));
        assert_eq!(reason, expected);
        assert!(!serde_json::to_string(&reason).unwrap().contains("PRIVATE"));
    }
    assert_eq!(
        photo_error(A::Crypto(
            rust_processor::encryption::signal_state::CryptoError::Conflict
        )),
        Reason::Conflict
    );
}
fn metadata() -> serde_json::Value {
    serde_json::json!({"epoch":7,"context":2,"requestId":REQUEST,"conversationId":CHAT,"messageId":MESSAGE,"filename":"private-camera.png","caption":"PRIVATE-CAPTION","choice":{"mode":"clean","quality":null,"maxDimension":0}})
}
fn frame(value: &serde_json::Value, source: &[u8]) -> Vec<u8> {
    let meta = serde_json::to_vec(value).unwrap();
    let mut bytes = (meta.len() as u32).to_le_bytes().to_vec();
    bytes.extend(meta);
    bytes.extend(source);
    bytes
}
fn input() -> AttachmentFrame {
    serde_json::from_value(metadata()).unwrap()
}
fn query() -> AttachmentTarget {
    AttachmentTarget {
        conversation_id: CHAT.into(),
        message_id: MESSAGE.into(),
    }
}
struct Fake {
    context: AtomicU64,
    active: AtomicBool,
    calls: AtomicUsize,
    blocked: AtomicBool,
    entered: Notify,
    release: Notify,
}
impl Fake {
    fn new() -> Self {
        Self {
            context: AtomicU64::new(2),
            active: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
            blocked: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}
impl Backend for Fake {
    fn ready(&self, context: u64) -> Result<(), Reason> {
        if context != self.context.load(Ordering::SeqCst) {
            return Err(Reason::Stale);
        }
        if !self.active.load(Ordering::SeqCst) {
            return Err(Reason::NotReady);
        }
        Ok(())
    }
    async fn prepare(
        &self,
        frame: AttachmentFrame,
        source: &[u8],
    ) -> Result<AttachmentView, Reason> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        assert_eq!(source, b"source");
        self.entered.notify_one();
        if self.blocked.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        Ok(AttachmentView {
            conversation_id: frame.conversation_id,
            message_id: frame.message_id,
            state: InputState::Prepared {
                crypto_epoch: "9007199254740993".into(),
            },
        })
    }
    async fn lookup(&self, _: u64, q: AttachmentTarget) -> Result<AttachmentView, Reason> {
        Ok(AttachmentView {
            conversation_id: q.conversation_id,
            message_id: q.message_id,
            state: InputState::Vacant,
        })
    }
}
#[test]
fn binary_photo_frame_bounds_choices_and_private_fields_are_strict() {
    let wire = frame(&metadata(), b"source");
    let (decoded, source) = decode(&wire).unwrap();
    assert_eq!(source, b"source");
    assert_eq!(decoded.context, 2);
    for data in [
        vec![],
        vec![1, 2, 3],
        0u32.to_le_bytes().to_vec(),
        ((MAX_METADATA + 1) as u32).to_le_bytes().to_vec(),
        vec![100, 0, 0, 0, 1, 2],
    ] {
        assert_eq!(decode(&data).err(), Some(Reason::Invalid));
    }
    for extra in [
        "accountId",
        "origin",
        "path",
        "key",
        "descriptor",
        "photoMetadataStripped",
        "cryptoEpoch",
    ] {
        let mut meta = metadata();
        meta[extra] = "forged".into();
        assert_eq!(
            decode(&frame(&meta, b"source")).err(),
            Some(Reason::Invalid)
        );
    }
    for choice in [
        serde_json::json!({"mode":"clean","quality":0,"maxDimension":0}),
        serde_json::json!({"mode":"clean","quality":101,"maxDimension":0}),
        serde_json::json!({"mode":"clean","quality":null,"maxDimension":63}),
    ] {
        let mut meta = metadata();
        meta["choice"] = choice;
        assert_eq!(
            decode(&frame(&meta, b"source")).err(),
            Some(Reason::PhotoChoiceInvalid)
        );
    }
    let mut meta = metadata();
    meta["choice"] = serde_json::json!({"mode":"original","metadataWarningAcknowledged":false});
    assert_eq!(
        decode(&frame(&meta, b"source")).err(),
        Some(Reason::PhotoOriginalConsent)
    );
    meta["choice"]["metadataWarningAcknowledged"] = true.into();
    assert!(decode(&frame(&meta, b"source")).is_ok());
    meta["choice"]["quality"] = 80.into();
    assert_eq!(
        decode(&frame(&meta, b"source")).err(),
        Some(Reason::Invalid)
    );
}
#[test]
fn photo_frame_uses_utf8_and_size_limits_without_copying_source() {
    assert_eq!(
        input().validate(MAX_PHOTO_SOURCE + 1),
        Err(Reason::PhotoSourceTooLarge)
    );
    assert_eq!(input().validate(0), Err(Reason::Invalid));
    let mut meta = metadata();
    meta["filename"] = "я".repeat(128).into();
    assert_eq!(
        decode(&frame(&meta, b"source")).err(),
        Some(Reason::Invalid)
    );
    meta = metadata();
    meta["caption"] = "\n".repeat(2000).into();
    let bytes = frame(&meta, b"source");
    let (_, source) = decode(&bytes).unwrap();
    assert_eq!(source.as_ptr(), bytes[bytes.len() - 6..].as_ptr());
    meta["caption"] = "я".repeat(1001).into();
    assert_eq!(
        decode(&frame(&meta, b"source")).err(),
        Some(Reason::Invalid)
    );
    meta = metadata();
    meta["context"] = 9_007_199_254_740_992u64.into();
    assert_eq!(
        decode(&frame(&meta, b"source")).err(),
        Some(Reason::Invalid)
    );
}

#[test]
fn file_frame_requires_canonical_epoch_and_keeps_raw_bytes() {
    let mut meta = metadata();
    meta["choice"] = serde_json::json!({"mode":"file","expectedEpoch":"9007199254740993"});
    let raw = frame(&meta, b"source");
    let (decoded, source) = decode(&raw).unwrap();
    assert_eq!(source, b"source");
    assert!(matches!(
        decoded.choice,
        Choice::File {
            expected_epoch: 9_007_199_254_740_993
        }
    ));
    assert!(decoded.validate(MAX_FILE_SOURCE).is_ok());
    assert_eq!(
        decoded.validate(MAX_FILE_SOURCE + 1),
        Err(Reason::FileSourceTooLarge)
    );
    for invalid in [
        serde_json::json!(0),
        serde_json::json!("01"),
        serde_json::json!("9223372036854775808"),
    ] {
        meta["choice"]["expectedEpoch"] = invalid;
        assert!(decode(&frame(&meta, b"source")).is_err());
    }
    meta["choice"]["expectedEpoch"] = "0".into();
    meta["choice"]["mimeType"] = "text/html".into();
    assert!(decode(&frame(&meta, b"source")).is_err());
}

#[test]
fn voice_frame_applies_media_cap_and_rejects_declared_mime() {
    let mut meta = metadata();
    meta["choice"] = serde_json::json!({"mode":"voice","expectedEpoch":"0"});
    let raw = frame(&meta, b"source");
    let (decoded, _) = decode(&raw).unwrap();
    assert!(decoded.validate(MAX_VOICE_SOURCE).is_ok());
    assert_eq!(
        decoded.validate(MAX_VOICE_SOURCE + 1),
        Err(Reason::VoiceSourceTooLarge)
    );
    meta["choice"]["mimeType"] = "audio/ogg".into();
    assert!(decode(&frame(&meta, b"source")).is_err());
}
#[tokio::test]
async fn private_photo_reply_has_only_bound_public_identity_and_native_reply_lease() {
    let admission = Admission::default();
    let backend = Fake::new();
    let response = execute_with(
        &admission,
        &backend,
        input(),
        b"source",
        || Ok(()),
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    let InvokeResponseBody::Json(raw) = response.body().unwrap() else {
        panic!("expected metadata JSON");
    };
    assert!(
        raw.len() < 4096
            && !raw.contains("PRIVATE")
            && !raw.contains("camera")
            && !raw.contains("source")
    );
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v.as_object().unwrap().len(), 3);
    assert_eq!(v["attachment"].as_object().unwrap().len(), 4);
    assert_eq!(v["attachment"]["cryptoEpoch"], "9007199254740993");
    assert!(matches!(
        admission.begin(2, REQUEST.into()),
        Err(Reason::Busy)
    ));
    admission
        .acknowledge(2, REQUEST, v["replyLease"].as_str().unwrap())
        .unwrap();
    drop(admission.begin(2, REQUEST.into()).unwrap());
    let result = lookup_with(&admission, &backend, 2, REQUEST.into(), query(), || Ok(()))
        .await
        .unwrap();
    let InvokeResponseBody::Json(raw) = result.body().unwrap() else {
        panic!()
    };
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v["attachment"]["kind"], "vacant");
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    let outgoing = AttachmentView {
        conversation_id: CHAT.into(),
        message_id: MESSAGE.into(),
        state: InputState::Outgoing {
            crypto_epoch: "9007199254740993".into(),
            stored_on_server: true,
            sealed_unsent: false,
        },
    };
    let v = serde_json::to_value(outgoing).unwrap();
    assert_eq!(v.as_object().unwrap().len(), 6);
    assert_eq!(v["cryptoEpoch"], "9007199254740993");
    assert_eq!(v["storedOnServer"], true);
}
#[tokio::test]
async fn photo_dispatch_rejects_wrong_page_context_or_readiness_before_backend() {
    let admission = Admission::default();
    let backend = Fake::new();
    assert_eq!(
        execute_with(
            &admission,
            &backend,
            input(),
            b"source",
            || Err(Reason::Stale),
            Duration::from_secs(1)
        )
        .await
        .err(),
        Some(Reason::Stale)
    );
    backend.context.store(3, Ordering::SeqCst);
    assert_eq!(
        execute_with(
            &admission,
            &backend,
            input(),
            b"source",
            || Ok(()),
            Duration::from_secs(1)
        )
        .await
        .err(),
        Some(Reason::Stale)
    );
    backend.context.store(2, Ordering::SeqCst);
    backend.active.store(false, Ordering::SeqCst);
    assert_eq!(
        execute_with(
            &admission,
            &backend,
            input(),
            b"source",
            || Ok(()),
            Duration::from_secs(1)
        )
        .await
        .err(),
        Some(Reason::NotReady)
    );
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn photo_late_page_or_account_change_discards_reply_and_releases_admission() {
    for account in [false, true] {
        let admission = Admission::default();
        let backend = Fake::new();
        backend.blocked.store(true, Ordering::SeqCst);
        let stale = AtomicBool::new(false);
        let request = execute_with(
            &admission,
            &backend,
            input(),
            b"source",
            || {
                if stale.load(Ordering::SeqCst) {
                    Err(Reason::Stale)
                } else {
                    Ok(())
                }
            },
            Duration::from_secs(1),
        );
        let steer = async {
            backend.entered.notified().await;
            if account {
                backend.context.store(3, Ordering::SeqCst);
            } else {
                stale.store(true, Ordering::SeqCst);
            }
            backend.release.notify_one();
        };
        let (result, ()) = tokio::join!(request, steer);
        assert_eq!(result.err(), Some(Reason::Stale));
        drop(admission.begin(2, REQUEST.into()).unwrap());
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn photo_cancel_timeout_and_abandoned_callback_never_replay_input() {
    let admission = Admission::default();
    let backend = Fake::new();
    backend.blocked.store(true, Ordering::SeqCst);
    let request = execute_with(
        &admission,
        &backend,
        input(),
        b"source",
        || Ok(()),
        Duration::from_secs(1),
    );
    let cancel = async {
        backend.entered.notified().await;
        admission.cancel(2, REQUEST).unwrap();
    };
    let (result, ()) = tokio::join!(request, cancel);
    assert_eq!(result.err(), Some(Reason::Stale));
    assert_eq!(
        execute_with(
            &admission,
            &backend,
            input(),
            b"source",
            || Ok(()),
            Duration::from_millis(1)
        )
        .await
        .err(),
        Some(Reason::NativeWait)
    );
    backend.entered.notified().await;
    {
        let request = execute_with(
            &admission,
            &backend,
            input(),
            b"source",
            || Ok(()),
            Duration::from_secs(1),
        );
        tokio::pin!(request);
        tokio::select! { _=backend.entered.notified()=>{}, _=&mut request=>panic!("callback must remain pending") }
    }
    drop(admission.begin(2, REQUEST.into()).unwrap());
    assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
}
#[cfg(not(feature = "experimental-e2ee"))]
#[tokio::test]
async fn default_build_has_no_photo_or_attachment_mutation_backend() {
    let state = DesktopE2ee {
        manager: super::super::SessionManager::new(),
        admission: Default::default(),
        cursors: Default::default(),
    };
    let mut frame = input();
    frame.context = 1;
    assert_eq!(
        execute(&state, frame, b"source", || Ok(())).await.err(),
        Some(Reason::NotReady)
    );
    assert_eq!(
        lookup(&state, 1, REQUEST.into(), query(), || Ok(()))
            .await
            .err(),
        Some(Reason::NotReady)
    );
}
