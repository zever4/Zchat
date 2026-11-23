//! Bounded attachment format, independent of Signal ratchets and blob transport.
//!
//! No IPC, file IO, upload, metadata sanitization or persistence is performed here.
//! A worker must save the exact blob AND descriptor before attempting delivery;
//! retry must load those bytes, never call `seal` again. The descriptor is secret
//! and belongs only in protected native storage / authenticated Signal plaintext.
use aes_gcm_siv::{
    Aes256GcmSiv, Nonce,
    aead::{AeadInOut, KeyInit},
};
use rand::{TryRngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant};
use zeroize::Zeroizing;

use crate::proto::e2ee::DeviceRef;

pub const MAX_PLAINTEXT_BYTES: usize = 50 * 1024 * 1024;
pub const BLOB_OVERHEAD: usize = 8 + 12 + 16;
pub const MAX_BLOB_BYTES: usize = MAX_PLAINTEXT_BYTES + BLOB_OVERHEAD;
// Domain + version + suite (AES-256-GCM-SIV), followed by nonce and ciphertext/tag.
const HEADER: &[u8; 8] = b"ZCATT\0\x01\x01";
const NONCE_END: usize = HEADER.len() + 12;
const DESCRIPTOR_BYTES: usize = 1 + 32 + 32 + 8;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AttachmentError {
    #[error("invalid attachment binding")]
    Binding,
    #[error("attachment size outside supported bounds")]
    Size,
    #[error("invalid private attachment descriptor")]
    Descriptor,
    #[error("attachment authentication failed")]
    Authentication,
    #[error("attachment protection unavailable")]
    Protection,
}
type Result<T> = std::result::Result<T, AttachmentError>;

/// Validated wire binding, NOT a permission, active epoch or native COMMIT proof.
/// The caller must obtain these fields from its authenticated/protected scope.
pub struct AttachmentBinding {
    aad: Vec<u8>,
}

impl AttachmentBinding {
    pub fn new(
        conversation: &str,
        epoch: u64,
        message: &str,
        blob: &str,
        sender: &DeviceRef,
        recipient: &DeviceRef,
    ) -> Result<Self> {
        if epoch > i64::MAX as u64
            || sender.user_id == recipient.user_id
            || sender.generation_id == recipient.generation_id
        {
            return Err(AttachmentError::Binding);
        }
        let mut aad = b"zchat.e2ee.attachment.aad.v1\0".to_vec();
        aad.extend(HEADER);
        for id in [conversation, message, blob] {
            append_uuid(&mut aad, id)?;
        }
        aad.extend(epoch.to_be_bytes());
        for device in [sender, recipient] {
            if device.device_id != 1 {
                return Err(AttachmentError::Binding);
            }
            append_uuid(&mut aad, &device.user_id)?;
            aad.extend(device.device_id.to_be_bytes());
            append_uuid(&mut aad, &device.generation_id)?;
        }
        Ok(Self { aad })
    }
}

fn append_uuid(out: &mut Vec<u8>, id: &str) -> Result<()> {
    let parsed = Uuid::parse_str(id).map_err(|_| AttachmentError::Binding)?;
    if parsed.get_version_num() != 4
        || parsed.get_variant() != Variant::RFC4122
        || parsed.to_string() != id
    {
        return Err(AttachmentError::Binding);
    }
    out.extend(parsed.as_bytes());
    Ok(())
}

// Deliberately no Debug/Clone/serde or key getter. Encoding below is exclusively
// for protected storage and the encrypted Signal payload, never a blob HTTP body.
pub struct AttachmentDescriptor {
    key: Zeroizing<[u8; 32]>,
    digest: [u8; 32],
    plaintext_len: usize,
}

impl AttachmentDescriptor {
    pub fn plaintext_len(&self) -> usize {
        self.plaintext_len
    }

    pub fn blob_len(&self) -> usize {
        self.plaintext_len + BLOB_OVERHEAD
    }

    /// Public ciphertext hash, also suitable for exact upload/download identity.
    pub fn blob_digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Secret versioned bytes, NOT a renderer DTO or server-visible descriptor.
    pub fn encode_for_signal(&self) -> Zeroizing<[u8; DESCRIPTOR_BYTES]> {
        let mut bytes = Zeroizing::new([0; DESCRIPTOR_BYTES]);
        bytes[0] = 1;
        bytes[1..33].copy_from_slice(self.key.as_ref());
        bytes[33..65].copy_from_slice(&self.digest);
        bytes[65..].copy_from_slice(&(self.plaintext_len as u64).to_be_bytes());
        bytes
    }

    /// Invoke after authenticated Signal decrypt / protected local read. Parsing
    /// alone grants no authority to fetch a blob or expose its plaintext.
    pub fn decode_from_signal(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != DESCRIPTOR_BYTES || bytes[0] != 1 {
            return Err(AttachmentError::Descriptor);
        }
        let size = u64::from_be_bytes(bytes[65..].try_into().unwrap());
        validate_size(size)?;
        let mut key = Zeroizing::new([0; 32]);
        key.copy_from_slice(&bytes[1..33]);
        Ok(Self {
            key,
            digest: bytes[33..65].try_into().unwrap(),
            plaintext_len: size as usize,
        })
    }
}

/// An in-memory preparation only. This is NOT a durable outbox/upload receipt.
pub struct PreparedAttachment {
    blob: Vec<u8>,
    descriptor: AttachmentDescriptor,
}

impl PreparedAttachment {
    pub fn blob(&self) -> &[u8] {
        &self.blob
    }

    pub fn descriptor(&self) -> &AttachmentDescriptor {
        &self.descriptor
    }
}

fn validate_size(size: u64) -> Result<()> {
    if size == 0 || size > MAX_PLAINTEXT_BYTES as u64 {
        return Err(AttachmentError::Size);
    }
    Ok(())
}

/// Each NEW preparation gets a separate CSPRNG key and nonce. Photo sanitization
/// and optional compression must happen BEFORE this function. Generic file bytes
/// are retained exactly; this layer never attempts to inspect EXIF/MIME/content.
pub fn seal(
    binding: &AttachmentBinding,
    mut plaintext: Zeroizing<Vec<u8>>,
) -> Result<PreparedAttachment> {
    validate_size(plaintext.len() as u64)?;
    let size = plaintext.len();
    let mut key = Zeroizing::new([0; 32]);
    let mut nonce = [0; 12];
    OsRng
        .try_fill_bytes(key.as_mut())
        .and_then(|()| OsRng.try_fill_bytes(&mut nonce))
        .map_err(|_| AttachmentError::Protection)?;
    let cipher =
        Aes256GcmSiv::new_from_slice(key.as_ref()).map_err(|_| AttachmentError::Protection)?;
    cipher
        .encrypt_in_place(&Nonce::from(nonce), &binding.aad, &mut *plaintext)
        .map_err(|_| AttachmentError::Protection)?;
    let mut blob = Vec::with_capacity(size + BLOB_OVERHEAD);
    blob.extend(HEADER);
    blob.extend(nonce);
    blob.append(&mut plaintext);
    let digest = Sha256::digest(&blob).into();
    Ok(PreparedAttachment {
        blob,
        descriptor: AttachmentDescriptor {
            key,
            digest,
            plaintext_len: size,
        },
    })
}

/// No bytes are returned until the size, ciphertext hash AND AEAD tag/binding
/// have been checked. Callers must bound downloads to MAX_BLOB_BYTES before read.
/// Whole-file authentication means this format has no partial/range playback.
pub fn open(
    binding: &AttachmentBinding,
    descriptor: &AttachmentDescriptor,
    blob: &[u8],
) -> Result<Zeroizing<Vec<u8>>> {
    if blob.len() != descriptor.blob_len()
        || blob.len() > MAX_BLOB_BYTES
        || !blob.starts_with(HEADER)
        || <[u8; 32]>::from(Sha256::digest(blob)) != descriptor.digest
    {
        return Err(AttachmentError::Authentication);
    }
    let nonce: [u8; 12] = blob[HEADER.len()..NONCE_END].try_into().unwrap();
    let cipher = Aes256GcmSiv::new_from_slice(descriptor.key.as_ref())
        .map_err(|_| AttachmentError::Protection)?;
    let mut plaintext = Zeroizing::new(blob[NONCE_END..].to_vec());
    cipher
        .decrypt_in_place(&Nonce::from(nonce), &binding.aad, &mut *plaintext)
        .map_err(|_| AttachmentError::Authentication)?;
    Ok(plaintext)
}

#[cfg(test)]
#[path = "attachment_tests.rs"]
mod tests;
