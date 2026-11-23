//! Local-only presentation of a verified attachment. The renderer can suggest a
//! basename; only the system picker supplies a destination.
use super::{DesktopE2ee, Reason};
use serde::Deserialize;
use tauri::{WebviewWindow, ipc::Response};

#[derive(Deserialize)]
#[cfg_attr(feature = "experimental-e2ee", derive(serde::Serialize))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Target {
    generation_id: Option<String>,
    conversation_id: String,
    message_id: String,
}
impl Target {
    fn validate(&self) -> Result<(), Reason> {
        use super::admission::valid_id;
        if self.generation_id.as_deref().is_none_or(valid_id)
            && valid_id(&self.conversation_id)
            && valid_id(&self.message_id)
        {
            Ok(())
        } else {
            Err(Reason::Invalid)
        }
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Operation {
    Preview,
    Listen,
    Export { suggested_name: String },
}
impl Operation {
    fn validate(&self) -> Result<(), Reason> {
        match self {
            Self::Export { suggested_name } if suggested_name.len() > 255 => Err(Reason::Invalid),
            _ => Ok(()),
        }
    }
}

#[cfg(feature = "experimental-e2ee")]
pub(super) const MAX_PNG: usize = 1_200_000;
#[cfg(feature = "experimental-e2ee")]
#[derive(serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum Outcome {
    Preview {
        #[serde(serialize_with = "super::pages::serialize_text")]
        png: zeroize::Zeroizing<String>,
    },
    Voice {
        #[serde(rename = "mimeType")]
        mime_type: &'static str,
        #[serde(skip)]
        file: rust_processor::encryption::signal_state::EpochAttachmentFile,
    },
    #[cfg(windows)]
    Exported,
    #[cfg(windows)]
    Cancelled,
}
#[cfg(feature = "experimental-e2ee")]
#[derive(serde::Serialize)]
struct Reply<'a> {
    context: u64,
    target: &'a Target,
    result: Outcome,
}

pub(crate) async fn execute(
    window: &WebviewWindow,
    state: &DesktopE2ee,
    context: u64,
    request_id: String,
    target: Target,
    operation: Operation,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    target.validate()?;
    operation.validate()?;
    #[cfg(feature = "experimental-e2ee")]
    {
        use std::time::Duration;
        let archive = target.generation_id.is_some();
        let session = state.manager.view_cancellation(context, archive)?;
        let guard = state.admission.begin(context, request_id)?;
        let cancel = guard.cancel.child_token();
        let _cancel_on_drop = cancel.clone().drop_guard();
        let permit = state.manager.local_file_permit()?;
        let work = async {
            #[cfg(windows)]
            let destination = if let Operation::Export { suggested_name } = &operation {
                let selected = save::choose(window, suggested_name, &cancel, &session).await?;
                let Some(path) = selected else {
                    return Ok(Outcome::Cancelled);
                };
                check_page()?;
                Some(path)
            } else {
                None
            };
            #[cfg(not(windows))]
            {
                let _ = window;
                if matches!(operation, Operation::Export { .. }) {
                    return Err(Reason::UnsupportedPlatform);
                }
            }
            let open = state.manager.open_local_attachment(
                context,
                target.generation_id.as_deref(),
                &target.conversation_id,
                &target.message_id,
            );
            let file = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Reason::Stale),
                r = tokio::time::timeout(Duration::from_secs(10), open) => r.map_err(|_| Reason::NativeWait)??,
            };
            check_page()?;
            let check = {
                let cancel = cancel.clone();
                let session = session.clone();
                move || {
                    if cancel.is_cancelled() || session.is_cancelled() {
                        Err(Reason::Stale)
                    } else {
                        Ok(())
                    }
                }
            };
            let job = tauri::async_runtime::spawn_blocking(move || {
                let _permit = permit;
                check()?;
                #[cfg(windows)]
                if let Some(path) = destination {
                    save::write_copy(&path, file.as_bytes(), check)?;
                    return Ok(Outcome::Exported);
                }
                if matches!(operation, Operation::Listen) {
                    use rust_processor::{
                        encryption::voice_format::VoiceFormat, proto::e2ee_v2::AttachmentKind,
                    };
                    if file.kind() != AttachmentKind::Voice {
                        return Err(Reason::VoiceFormat);
                    }
                    let mime_type = VoiceFormat::detect(file.as_bytes())
                        .ok_or(Reason::VoiceFormat)?
                        .mime();
                    check()?;
                    return Ok(Outcome::Voice { mime_type, file });
                }
                let png = preview(file.kind(), file.as_bytes())?;
                check()?;
                Ok(Outcome::Preview { png })
            });
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(Reason::Stale),
                _ = session.cancelled() => Err(Reason::Stale),
                r = tokio::time::timeout(Duration::from_secs(20), job) => r.map_err(|_| Reason::NativeWait)?.map_err(|_| Reason::Runtime)?,
            }
        };
        let result = work.await?;
        check_page()?;
        state.manager.ensure_views_ready(context, archive)?;
        let reply = Reply {
            context,
            target: &target,
            result,
        };
        match &reply.result {
            Outcome::Preview { .. } => guard.deliver_preview(context, &reply),
            Outcome::Voice { file, .. } => guard.deliver_audio(context, &reply, file.as_bytes()),
            #[cfg(windows)]
            _ => guard.deliver(context, &reply),
        }
    }
    #[cfg(not(feature = "experimental-e2ee"))]
    {
        let _ = (window, request_id);
        state.manager.ensure_dialog_ready(context)?;
        Err(Reason::NotReady)
    }
}

#[cfg(feature = "experimental-e2ee")]
fn preview(
    kind: rust_processor::proto::e2ee_v2::AttachmentKind,
    bytes: &[u8],
) -> Result<zeroize::Zeroizing<String>, Reason> {
    use base64::Engine;
    use rust_processor::{
        compression::{PhotoChoice, PhotoPreparationError, prepare_photo},
        proto::e2ee_v2::AttachmentKind,
    };
    if kind != AttachmentKind::Photo
        || bytes.len() > rust_processor::encryption::attachment_payload::MAX_MEDIA_BYTES
    {
        return Err(Reason::AttachmentPreview);
    }
    // Preserve detail when it fits in the existing preview IPC budget.
    for max_dimension in [1920, 960, 384] {
        let photo = match prepare_photo(
            bytes,
            PhotoChoice::Clean {
                quality: None,
                max_dimension,
            },
        ) {
            Ok(photo) => photo,
            Err(PhotoPreparationError::OutputTooLarge) => continue,
            Err(_) => return Err(Reason::AttachmentPreview),
        };
        let png = zeroize::Zeroizing::new(photo.into_bytes());
        if png.len() <= MAX_PNG {
            return Ok(zeroize::Zeroizing::new(
                base64::engine::general_purpose::STANDARD.encode(png.as_slice()),
            ));
        }
    }
    Err(Reason::AttachmentPreview)
}

#[cfg(all(feature = "experimental-e2ee", any(windows, test)))]
#[path = "local_files_save.rs"]
mod save;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_and_operation_reject_renderer_paths_and_invalid_scope() {
        let id = "11111111-1111-4111-8111-111111111111";
        let target = serde_json::json!({"generationId":null,"conversationId":id,"messageId":id});
        assert!(
            serde_json::from_value::<Target>(target.clone())
                .unwrap()
                .validate()
                .is_ok()
        );
        let mut wrong = target.clone();
        wrong["generationId"] = "invalid".into();
        assert_eq!(
            serde_json::from_value::<Target>(wrong).unwrap().validate(),
            Err(Reason::Invalid)
        );
        let mut path = target;
        path["path"] = "C:/arbitrary.txt".into();
        assert!(serde_json::from_value::<Target>(path).is_err());
        assert!(serde_json::from_value::<Operation>(serde_json::json!({"action":"export","suggestedName":"file.txt","path":"C:/file.txt"})).is_err());
    }

    #[cfg(feature = "experimental-e2ee")]
    #[test]
    fn preview_reencodes_a_bounded_raster_and_rejects_nonphotos() {
        use base64::{Engine, engine::general_purpose::STANDARD};
        use rust_processor::proto::e2ee_v2::AttachmentKind;
        let original = include_bytes!("../../icons/icon.png");
        assert_eq!(
            preview(AttachmentKind::File, original).err(),
            Some(Reason::AttachmentPreview)
        );
        assert_eq!(
            preview(AttachmentKind::Photo, b"<svg></svg>").err(),
            Some(Reason::AttachmentPreview)
        );
        let encoded = preview(AttachmentKind::Photo, original).unwrap();
        let png = STANDARD.decode(encoded.as_bytes()).unwrap();
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        assert!(png.len() <= MAX_PNG);
        assert_eq!(&png[16..24], &original[16..24]);
        assert_ne!(png.as_slice(), original);
    }
}
