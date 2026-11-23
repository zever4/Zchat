//! Private attachment message format and key-free history presentation.
//! No upload, file ACK, active-scope authority or durable blob preparation.
use prost::Message;
use zeroize::{Zeroize, Zeroizing};

use super::{
    attachment::{AttachmentBinding, AttachmentDescriptor},
    contract::{ContractError, MAX_TEXT_BYTES},
};
use crate::proto::e2ee_v2::{self as v2, AttachmentKind, AttachmentMessage};

pub const PAYLOAD_FORMAT: u32 = 3;
pub const MAX_MEDIA_BYTES: usize = 10 * 1024 * 1024;
pub const MAX_FILENAME_BYTES: usize = 255;

/// Native preparation input, with no file key, path, account or epoch authority.
/// The future desktop boundary must choose/sanitize the bytes before submitting.
pub struct AttachmentDraft {
    pub kind: AttachmentKind,
    pub filename: Zeroizing<String>,
    pub mime_type: Zeroizing<String>,
    pub caption: Zeroizing<String>,
    pub photo_metadata_stripped: Option<bool>,
}

/// Copies only public ciphertext identity, never the file key/private metadata.
/// Shape helper, not a server upload/COMMIT proof or active-epoch permission.
pub(crate) fn blob_reference(a: &AttachmentMessage) -> Result<v2::BlobRef, ContractError> {
    let d =
        AttachmentDescriptor::decode_from_signal(&a.private_descriptor).map_err(|_| invalid())?;
    let r = v2::BlobRef {
        format_version: 1,
        blob_id: a.blob_id.clone(),
        ciphertext_bytes: d.blob_len() as u64,
        sha256: d.blob_digest().to_vec(),
    };
    super::epoch_contract::validate_blob_ref(&r)?;
    Ok(r)
}

impl AttachmentDraft {
    #[cfg(feature = "compression")]
    pub(super) fn file(filename: Zeroizing<String>, caption: Zeroizing<String>) -> Self {
        Self {
            kind: AttachmentKind::File,
            filename,
            mime_type: Zeroizing::new("application/octet-stream".into()),
            caption,
            photo_metadata_stripped: None,
        }
    }

    pub(crate) fn validate(&self, size: usize) -> Result<(), ContractError> {
        description(
            self.kind as i32,
            &self.filename,
            &self.mime_type,
            &self.caption,
            self.photo_metadata_stripped,
            size,
        )
    }
    pub(crate) fn matches(&self, a: &AttachmentMessage) -> bool {
        self.kind as i32 == a.kind
            && self.filename.as_str() == a.filename
            && self.mime_type.as_str() == a.mime_type
            && self.caption.as_str() == a.caption
            && self.photo_metadata_stripped == a.photo_metadata_stripped
    }
    pub(crate) fn into_message(
        mut self,
        blob: String,
        descriptor: &AttachmentDescriptor,
    ) -> AttachmentMessage {
        AttachmentMessage {
            format_version: 1,
            blob_id: blob,
            kind: self.kind as i32,
            private_descriptor: descriptor.encode_for_signal().to_vec(),
            filename: std::mem::take(&mut *self.filename),
            mime_type: std::mem::take(&mut *self.mime_type),
            caption: std::mem::take(&mut *self.caption),
            photo_metadata_stripped: self.photo_metadata_stripped,
        }
    }
}

// prost's default Debug would print the file key through EncryptedPayload too.
impl std::fmt::Debug for AttachmentMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AttachmentMessage { <private fields redacted> }")
    }
}
impl Drop for AttachmentMessage {
    fn drop(&mut self) {
        self.private_descriptor.zeroize();
        self.filename.zeroize();
        self.mime_type.zeroize();
        self.caption.zeroize();
    }
}

fn invalid() -> ContractError {
    ContractError::Invalid("private attachment payload")
}

fn safe_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_FILENAME_BYTES
        && value.trim() == value
        && value != "."
        && value != ".."
        && !value.ends_with('.')
        && !value.chars().any(|c| {
            c.is_control()
                || matches!(c, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*')
                || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
        })
}

/// Structural validation only, called after Signal authentication. The descriptor
/// must later be matched to the downloaded blob and its route by attachment::open.
pub fn validate(p: &v2::EncryptedPayload, a: &AttachmentMessage) -> Result<(), ContractError> {
    if p.format_version != PAYLOAD_FORMAT || a.format_version != 1 || a.encoded_len() > 4096 {
        return Err(invalid());
    }
    AttachmentBinding::new(
        &p.conversation_id,
        p.crypto_epoch.ok_or_else(invalid)?,
        &p.message_id,
        &a.blob_id,
        p.sender.as_ref().ok_or_else(invalid)?,
        p.recipient.as_ref().ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    let descriptor =
        AttachmentDescriptor::decode_from_signal(&a.private_descriptor).map_err(|_| invalid())?;
    description(
        a.kind,
        &a.filename,
        &a.mime_type,
        &a.caption,
        a.photo_metadata_stripped,
        descriptor.plaintext_len(),
    )
}

fn description(
    kind: i32,
    filename: &str,
    mime_type: &str,
    caption: &str,
    photo_metadata_stripped: Option<bool>,
    size: usize,
) -> Result<(), ContractError> {
    if !safe_name(filename)
        || caption.len() > MAX_TEXT_BYTES
        || size == 0
        || size > super::attachment::MAX_PLAINTEXT_BYTES
    {
        return Err(invalid());
    }
    let allowed = match AttachmentKind::try_from(kind) {
        Ok(AttachmentKind::Photo) => {
            size <= MAX_MEDIA_BYTES
                && photo_metadata_stripped.is_some()
                && matches!(
                    mime_type,
                    "image/jpeg" | "image/png" | "image/gif" | "image/webp"
                )
        }
        Ok(AttachmentKind::File) => {
            photo_metadata_stripped.is_none() && mime_type == "application/octet-stream"
        }
        Ok(AttachmentKind::Voice) => {
            size <= MAX_MEDIA_BYTES
                && photo_metadata_stripped.is_none()
                && matches!(mime_type, "audio/ogg" | "audio/webm")
        }
        _ => false,
    };
    if !allowed {
        return Err(invalid());
    }
    Ok(())
}

/// Metadata for a future native presentation adapter: deliberately no file key,
/// secret descriptor, raw protobuf or blob contents. No Debug/Serialize/Clone.
pub struct AttachmentSummary {
    pub blob_id: String,
    pub kind: AttachmentKind,
    pub filename: Zeroizing<String>,
    pub mime_type: Zeroizing<String>,
    pub caption: Zeroizing<String>,
    pub plaintext_len: usize,
    pub photo_metadata_stripped: Option<bool>,
}

pub(crate) fn summary(mut a: AttachmentMessage) -> Result<AttachmentSummary, ContractError> {
    // Call only after full payload validation. Recheck secret descriptor shape
    // here, so malformed bytes cannot become a plausible presentation record.
    let descriptor =
        AttachmentDescriptor::decode_from_signal(&a.private_descriptor).map_err(|_| invalid())?;
    Ok(AttachmentSummary {
        blob_id: std::mem::take(&mut a.blob_id),
        kind: AttachmentKind::try_from(a.kind).map_err(|_| invalid())?,
        filename: Zeroizing::new(std::mem::take(&mut a.filename)),
        mime_type: Zeroizing::new(std::mem::take(&mut a.mime_type)),
        caption: Zeroizing::new(std::mem::take(&mut a.caption)),
        plaintext_len: descriptor.plaintext_len(),
        photo_metadata_stripped: a.photo_metadata_stripped,
    })
}

#[cfg(test)]
#[path = "attachment_payload_tests.rs"]
mod tests;
