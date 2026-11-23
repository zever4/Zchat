//! Small metadata frame + binary source; no JSON byte arrays, renderer paths,
//! private descriptors, caller-selected account or asserted metadata-clean flag.
#[cfg(any(feature = "experimental-e2ee", test))]
use super::admission::Admission;
use super::{AttachmentTarget, DesktopE2ee, Reason};
use serde::Deserialize;
#[cfg(any(feature = "experimental-e2ee", test))]
use serde::Serialize;
use tauri::ipc::Response;
use zeroize::Zeroizing;

pub(crate) const MAX_METADATA: usize = 16 * 1024;
const MAX_PHOTO_SOURCE: usize = rust_processor::compression::MAX_INPUT_BYTES;
const MAX_FILE_SOURCE: usize = 50 * 1024 * 1024;
const MAX_VOICE_SOURCE: usize = 10 * 1024 * 1024;
const MAX_JS: u64 = 9_007_199_254_740_991;

#[derive(Deserialize)]
#[serde(
    tag = "mode",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Choice {
    Voice {
        #[cfg_attr(not(feature = "experimental-e2ee"), allow(dead_code))]
        #[serde(deserialize_with = "input_epoch")]
        expected_epoch: u64,
    },
    File {
        #[cfg_attr(not(feature = "experimental-e2ee"), allow(dead_code))]
        #[serde(deserialize_with = "input_epoch")]
        expected_epoch: u64,
    },
    Clean {
        quality: Option<u8>,
        max_dimension: u32,
    },
    Original {
        metadata_warning_acknowledged: bool,
    },
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AttachmentFrame {
    pub epoch: u64,
    pub context: u64,
    pub request_id: String,
    pub conversation_id: String,
    pub message_id: String,
    #[serde(deserialize_with = "private_string")]
    filename: Zeroizing<String>,
    #[serde(deserialize_with = "private_string")]
    caption: Zeroizing<String>,
    choice: Choice,
}
fn private_string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Zeroizing<String>, D::Error> {
    String::deserialize(d).map(Zeroizing::new)
}
fn input_epoch<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    let value = String::deserialize(d)?;
    super::recovery::epoch(&value).map_err(|_| serde::de::Error::custom("invalid attachment epoch"))
}
impl AttachmentFrame {
    fn validate(&self, bytes: usize) -> Result<(), Reason> {
        match self.choice {
            Choice::Voice { .. } if bytes > MAX_VOICE_SOURCE => {
                return Err(Reason::VoiceSourceTooLarge);
            }
            Choice::File { .. } if bytes > MAX_FILE_SOURCE => {
                return Err(Reason::FileSourceTooLarge);
            }
            Choice::Clean { .. } | Choice::Original { .. } if bytes > MAX_PHOTO_SOURCE => {
                return Err(Reason::PhotoSourceTooLarge);
            }
            _ => {}
        }
        if bytes == 0
            || self.epoch > MAX_JS
            || self.context > MAX_JS
            || !super::admission::valid_id(&self.request_id)
            || !super::admission::valid_id(&self.conversation_id)
            || !super::admission::valid_id(&self.message_id)
            || self.filename.is_empty()
            || self.filename.len() > 255
            || self.caption.len() > 2000
        {
            return Err(Reason::Invalid);
        }
        match self.choice {
            Choice::File { .. } | Choice::Voice { .. } => Ok(()),
            Choice::Clean {
                quality,
                max_dimension,
            } if quality.is_none_or(|q| (1..=100).contains(&q))
                && (max_dimension == 0 || (64..=12000).contains(&max_dimension)) =>
            {
                Ok(())
            }
            Choice::Clean { .. } => Err(Reason::PhotoChoiceInvalid),
            Choice::Original {
                metadata_warning_acknowledged: true,
            } => Ok(()),
            Choice::Original { .. } => Err(Reason::PhotoOriginalConsent),
        }
    }
}

pub(crate) fn decode(data: &[u8]) -> Result<(AttachmentFrame, &[u8]), Reason> {
    if data.len() > 4 + MAX_METADATA + MAX_FILE_SOURCE {
        return Err(Reason::FileSourceTooLarge);
    }
    let prefix: [u8; 4] = data
        .get(..4)
        .ok_or(Reason::Invalid)?
        .try_into()
        .map_err(|_| Reason::Invalid)?;
    let length = u32::from_le_bytes(prefix) as usize;
    if length == 0 || length > MAX_METADATA {
        return Err(Reason::Invalid);
    }
    let split = 4 + length;
    let frame: AttachmentFrame = serde_json::from_slice(data.get(4..split).ok_or(Reason::Invalid)?)
        .map_err(|_| Reason::Invalid)?;
    let source = data.get(split..).ok_or(Reason::Invalid)?;
    frame.validate(source.len())?;
    Ok((frame, source))
}

#[cfg(any(feature = "experimental-e2ee", test))]
#[derive(Debug, Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum InputState {
    Vacant,
    Prepared {
        crypto_epoch: String,
    },
    Outgoing {
        crypto_epoch: String,
        stored_on_server: bool,
        sealed_unsent: bool,
    },
}
#[cfg(any(feature = "experimental-e2ee", test))]
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentView {
    conversation_id: String,
    message_id: String,
    #[serde(flatten)]
    state: InputState,
}
#[cfg(any(feature = "experimental-e2ee", test))]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Reply {
    context: u64,
    attachment: AttachmentView,
}

#[cfg(any(feature = "experimental-e2ee", test))]
trait Backend: Sync {
    fn ready(&self, context: u64) -> Result<(), Reason>;
    fn prepare(
        &self,
        frame: AttachmentFrame,
        source: &[u8],
    ) -> impl std::future::Future<Output = Result<AttachmentView, Reason>> + Send;
    fn lookup(
        &self,
        context: u64,
        query: AttachmentTarget,
    ) -> impl std::future::Future<Output = Result<AttachmentView, Reason>> + Send;
}
#[cfg(any(feature = "experimental-e2ee", test))]
impl Backend for super::SessionManager {
    fn ready(&self, context: u64) -> Result<(), Reason> {
        self.ensure_dialog_ready(context)
    }
    async fn prepare(
        &self,
        frame: AttachmentFrame,
        source: &[u8],
    ) -> Result<AttachmentView, Reason> {
        #[cfg(feature = "experimental-e2ee")]
        {
            let context = frame.context;
            let (client, cancel) = self.dialog_client(context)?;
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Reason::Stale),
                r = prepare_native(&client, frame, source) => r,
            };
            self.snapshot(context)?;
            if result.is_ok() {
                self.ensure_dialog_ready(context)?;
            }
            result
        }
        #[cfg(not(feature = "experimental-e2ee"))]
        {
            let _ = source;
            self.ensure_dialog_ready(frame.context)?;
            Err(Reason::NotReady)
        }
    }
    async fn lookup(
        &self,
        context: u64,
        query: AttachmentTarget,
    ) -> Result<AttachmentView, Reason> {
        #[cfg(feature = "experimental-e2ee")]
        {
            let (client, cancel) = self.dialog_client(context)?;
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Reason::Stale),
                r = client.attachment_input_state(&query.conversation_id, &query.message_id) => r.map_err(super::factory::account_error),
            };
            self.snapshot(context)?;
            let state = result?;
            self.ensure_dialog_ready(context)?;
            Ok(view(query, state))
        }
        #[cfg(not(feature = "experimental-e2ee"))]
        {
            let _ = query;
            self.ensure_dialog_ready(context)?;
            Err(Reason::NotReady)
        }
    }
}

pub(crate) async fn execute(
    state: &DesktopE2ee,
    frame: AttachmentFrame,
    source: &[u8],
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    #[cfg(any(feature = "experimental-e2ee", test))]
    {
        execute_with(
            &state.admission,
            &state.manager,
            frame,
            source,
            check_page,
            std::time::Duration::from_secs(30),
        )
        .await
    }
    #[cfg(not(any(feature = "experimental-e2ee", test)))]
    {
        check_page()?;
        frame.validate(source.len())?;
        state.manager.ensure_dialog_ready(frame.context)?;
        Err(Reason::NotReady)
    }
}
#[cfg(any(feature = "experimental-e2ee", test))]
async fn execute_with(
    admission: &Admission,
    backend: &impl Backend,
    frame: AttachmentFrame,
    source: &[u8],
    check_page: impl Fn() -> Result<(), Reason>,
    timeout: std::time::Duration,
) -> Result<Response, Reason> {
    check_page()?;
    frame.validate(source.len())?;
    let context = frame.context;
    backend.ready(context)?;
    let guard = admission.begin(context, frame.request_id.clone())?;
    let attachment = tokio::select! {
        biased;
        _ = guard.cancel.cancelled() => return Err(Reason::Stale),
        r = tokio::time::timeout(timeout, backend.prepare(frame, source)) => r.map_err(|_| Reason::NativeWait)??,
    };
    check_page()?;
    backend.ready(context)?;
    guard.deliver(
        context,
        &Reply {
            context,
            attachment,
        },
    )
}
pub(crate) async fn lookup(
    state: &DesktopE2ee,
    context: u64,
    request_id: String,
    query: AttachmentTarget,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    #[cfg(any(feature = "experimental-e2ee", test))]
    {
        lookup_with(
            &state.admission,
            &state.manager,
            context,
            request_id,
            query,
            check_page,
        )
        .await
    }
    #[cfg(not(any(feature = "experimental-e2ee", test)))]
    {
        let _ = request_id;
        check_page()?;
        query.validate()?;
        state.manager.ensure_dialog_ready(context)?;
        Err(Reason::NotReady)
    }
}
#[cfg(any(feature = "experimental-e2ee", test))]
async fn lookup_with(
    admission: &Admission,
    backend: &impl Backend,
    context: u64,
    request_id: String,
    query: AttachmentTarget,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    query.validate()?;
    backend.ready(context)?;
    let guard = admission.begin(context, request_id)?;
    let attachment = tokio::select! {
        biased;
        _ = guard.cancel.cancelled() => return Err(Reason::Stale),
        r = tokio::time::timeout(std::time::Duration::from_secs(30), backend.lookup(context, query)) => r.map_err(|_| Reason::NativeWait)??,
    };
    check_page()?;
    backend.ready(context)?;
    guard.deliver(
        context,
        &Reply {
            context,
            attachment,
        },
    )
}

#[cfg(feature = "experimental-e2ee")]
fn view(
    query: AttachmentTarget,
    state: rust_processor::encryption::signal_state::EpochAttachmentInputState,
) -> AttachmentView {
    use rust_processor::encryption::signal_state::EpochAttachmentInputState as S;
    let state = match state {
        S::Vacant => InputState::Vacant,
        S::Prepared { crypto_epoch } => InputState::Prepared {
            crypto_epoch: crypto_epoch.to_string(),
        },
        S::Outgoing {
            crypto_epoch,
            stored_on_server,
            sealed_unsent,
        } => InputState::Outgoing {
            crypto_epoch: crypto_epoch.to_string(),
            stored_on_server,
            sealed_unsent,
        },
    };
    AttachmentView {
        conversation_id: query.conversation_id,
        message_id: query.message_id,
        state,
    }
}
#[cfg(feature = "experimental-e2ee")]
async fn prepare_native(
    client: &rust_processor::encryption::account_owner::AccountClient,
    frame: AttachmentFrame,
    source: &[u8],
) -> Result<AttachmentView, Reason> {
    use rust_processor::{
        compression::PhotoChoice,
        encryption::{
            account_owner::{FileSource, PhotoSource},
            signal_state::{EpochAttachmentInputState as S, EpochAttachmentPreparation as P},
        },
    };
    let admission = match frame.choice {
        Choice::Voice { expected_epoch } => client.admit_voice(
            &frame.conversation_id,
            &frame.message_id,
            source.len(),
            expected_epoch,
        ),
        Choice::File { expected_epoch } => client.admit_file(
            &frame.conversation_id,
            &frame.message_id,
            source.len(),
            expected_epoch,
        ),
        _ => client.admit_photo(&frame.conversation_id, &frame.message_id, source.len()),
    }
    .map_err(super::factory::account_error)?;
    // Resolve an uncertain UUID BEFORE copying source bytes as well as before
    // codec work. The worker rechecks vacancy again at dispatch and at COMMIT.
    if client
        .attachment_input_state(&frame.conversation_id, &frame.message_id)
        .await
        .map_err(super::factory::account_error)?
        != S::Vacant
    {
        return Err(Reason::Conflict);
    }
    client
        .ensure_attachment_catalog(&frame.conversation_id)
        .await
        .map_err(super::factory::account_error)?;
    let choice = match frame.choice {
        Choice::File { .. } | Choice::Voice { .. } => None,
        Choice::Clean {
            quality,
            max_dimension,
        } => Some(PhotoChoice::Clean {
            quality,
            max_dimension,
        }),
        Choice::Original {
            metadata_warning_acknowledged,
        } => Some(PhotoChoice::Original {
            metadata_warning_acknowledged,
        }),
    };
    let bytes = Zeroizing::new(source.to_vec());
    let result = if let Some(choice) = choice {
        client
            .prepare_admitted_photo(
                admission,
                PhotoSource {
                    bytes,
                    filename: frame.filename,
                    caption: frame.caption,
                    choice,
                },
            )
            .await
    } else if matches!(frame.choice, Choice::Voice { .. }) {
        client
            .prepare_admitted_voice(admission, bytes, frame.caption)
            .await
    } else {
        client
            .prepare_admitted_file(
                admission,
                FileSource {
                    bytes,
                    filename: frame.filename,
                    caption: frame.caption,
                },
            )
            .await
    };
    match result.map_err(photo_error)? {
        P::Prepared(plan) => Ok(AttachmentView {
            conversation_id: plan.conversation_id().into(),
            message_id: plan.message_id().into(),
            state: InputState::Prepared {
                crypto_epoch: plan.crypto_epoch().to_string(),
            },
        }),
        P::CatalogRequired => Err(Reason::NotReady),
    }
}
#[cfg(feature = "experimental-e2ee")]
fn photo_error(error: rust_processor::encryption::account_owner::AccountError) -> Reason {
    use rust_processor::{
        compression::PhotoPreparationError as P,
        encryption::{
            account_owner::AccountError as A, photo_preparation::PhotoAttachmentError as E,
        },
        errors::CompressionError as C,
    };
    match error {
        A::VoiceFormat => Reason::VoiceFormat,
        A::Photo(E::Metadata) => Reason::Invalid,
        A::Photo(E::MediaTooLarge) | A::Photo(E::Preparation(P::OutputTooLarge)) => {
            Reason::PhotoOutputTooLarge
        }
        A::Photo(E::Preparation(P::OriginalNeedsConfirmation)) => Reason::PhotoOriginalConsent,
        A::Photo(E::Preparation(P::AnimationNeedsOriginal)) => Reason::PhotoAnimationOriginal,
        A::Photo(E::Preparation(P::MultipageNeedsOriginal)) => Reason::PhotoMultipageOriginal,
        A::Photo(E::Preparation(P::Codec(c))) => match c {
            C::InputTooLarge => Reason::PhotoSourceTooLarge,
            C::ImageTooLarge => Reason::PhotoDimensions,
            C::InvalidQuality(_) | C::InvalidMaxDimension(_) => Reason::PhotoChoiceInvalid,
            C::UnsupportedFormat | C::DetectionFailed(_) => Reason::PhotoUnsupported,
            C::DecodeFailed(_) | C::IoError(_) => Reason::PhotoDecode,
            C::EncodeFailed(_) => Reason::PhotoEncode,
        },
        other => super::factory::account_error(other),
    }
}

#[cfg(test)]
#[path = "photo_tests.rs"]
mod tests;
