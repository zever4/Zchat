//! Epoch data boundary: text/control2, attachment3. No enabled RPC/storage repair.
use libsignal_protocol::{IdentityKey, PublicKey, kem};
use prost::Message;
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant};

use super::contract::{ContractError, MAX_REQUEST_BYTES, MAX_TEXT_BYTES, MESSAGE_RETENTION_MS};
use super::recovery_contract::validate_epoch_version_binding;
use crate::proto::{e2ee as common, e2ee_v2 as v2};

fn invalid(field: &'static str) -> ContractError {
    ContractError::Invalid(field)
}
fn uuid4(s: &str) -> bool {
    s.len() == 36
        && Uuid::parse_str(s).is_ok_and(|id| {
            id.get_version_num() == 4 && id.get_variant() == Variant::RFC4122 && id.to_string() == s
        })
}
fn epoch(value: Option<u64>) -> Result<u64, ContractError> {
    value
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or(invalid("required crypto epoch"))
}
fn device(d: Option<&common::DeviceRef>) -> Result<&common::DeviceRef, ContractError> {
    d.filter(|d| uuid4(&d.user_id) && uuid4(&d.generation_id) && d.device_id == 1)
        .ok_or(invalid("v2 device reference"))
}
fn context(c: Option<&common::ClientContext>) -> Result<&common::ClientContext, ContractError> {
    let c = c
        .filter(|c| uuid4(&c.device_generation_id))
        .ok_or(invalid("v2 device context"))?;
    validate_epoch_version_binding(c.protocol_version, 0, 0)?;
    Ok(c)
}
fn participants(
    a: Option<&common::DeviceRef>,
    b: Option<&common::DeviceRef>,
) -> Result<(), ContractError> {
    let a = device(a)?;
    let b = device(b)?;
    if a.user_id == b.user_id || a.generation_id == b.generation_id {
        return Err(invalid("v2 participant binding"));
    }
    Ok(())
}
fn ec_public(key: &[u8]) -> bool {
    key.len() == 33 && key[0] == 5
}

// Kept separate from strict2 API/acquisition/recovery-control validation.
pub(crate) fn framing_version(version: u32) -> Result<(), ContractError> {
    if matches!(version, 2 | 3) {
        Ok(())
    } else {
        Err(ContractError::UnsupportedVersion)
    }
}

pub fn validate_blob_ref(r: &v2::BlobRef) -> Result<(), ContractError> {
    if r.format_version != 1 {
        return Err(ContractError::UnsupportedVersion);
    }
    if !uuid4(&r.blob_id)
        || r.ciphertext_bytes <= super::attachment::BLOB_OVERHEAD as u64
        || r.ciphertext_bytes > super::attachment::MAX_BLOB_BYTES as u64
        || r.sha256.len() != 32
    {
        return Err(invalid("public blob reference"));
    }
    Ok(())
}

pub fn validate_envelope(e: &v2::Envelope) -> Result<(), ContractError> {
    epoch(e.crypto_epoch)?;
    framing_version(e.protocol_version)?;
    match (e.protocol_version, &e.blob) {
        (3, Some(r)) => validate_blob_ref(r)?,
        (2, None) => {}
        _ => return Err(invalid("envelope/blob version binding")),
    }
    if e.encoded_len() > MAX_REQUEST_BYTES
        || !uuid4(&e.message_id)
        || !uuid4(&e.conversation_id)
        || e.crypto_suite != common::CryptoSuite::SignalPq1 as i32
        || e.ciphertext.is_empty()
        || !matches!(
            common::CiphertextType::try_from(e.message_type),
            Ok(common::CiphertextType::PrekeySignal | common::CiphertextType::Signal)
        )
    {
        return Err(invalid("v2 envelope fields"));
    }
    participants(e.sender.as_ref(), e.recipient.as_ref())
}

/// Expected epoch comes from the authorized conversation/retained epoch catalog,
/// never from the same untrusted envelope. Does not create an authorized scope.
pub fn validate_envelope_at_epoch(e: &v2::Envelope, expected: u64) -> Result<(), ContractError> {
    validate_envelope(e)?;
    if epoch(Some(expected))? != e.crypto_epoch.unwrap() {
        return Err(invalid("data epoch binding"));
    }
    Ok(())
}

/// Bound raw bytes BEFORE prost decoding. Prost ignores unknown fields; critical
/// semantics require explicit protocol/format versions, never unknown-field flags.
pub fn decode_envelope(bytes: &[u8]) -> Result<v2::Envelope, ContractError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(invalid("v2 envelope wire size"));
    }
    let e = v2::Envelope::decode(bytes).map_err(|_| invalid("v2 envelope wire"))?;
    validate_envelope(&e)?;
    Ok(e)
}

pub fn validate_send(r: &v2::SendMessageRequest) -> Result<(), ContractError> {
    let c = context(r.context.as_ref())?;
    let e = r.envelope.as_ref().ok_or(invalid("v2 Send envelope"))?;
    validate_envelope(e)?;
    if r.encoded_len() > MAX_REQUEST_BYTES
        || e.sender.as_ref().unwrap().generation_id != c.device_generation_id
    {
        return Err(invalid("v2 Send binding"));
    }
    Ok(())
}

pub fn envelope_frame(e: &v2::Envelope) -> Result<Vec<u8>, ContractError> {
    validate_envelope(e)?;
    let mut b = b"zchat.e2ee.envelope.v2\0".to_vec();
    if e.protocol_version == 3 {
        b = b"zchat.e2ee.envelope.v3\0".to_vec();
    }
    b.extend(e.protocol_version.to_be_bytes());
    b.extend((e.crypto_suite as u32).to_be_bytes());
    b.extend(e.message_id.as_bytes());
    b.extend(e.conversation_id.as_bytes());
    b.extend(e.crypto_epoch.unwrap().to_be_bytes());
    for d in [e.sender.as_ref().unwrap(), e.recipient.as_ref().unwrap()] {
        b.extend(d.user_id.as_bytes());
        b.extend(d.device_id.to_be_bytes());
        b.extend(d.generation_id.as_bytes());
    }
    b.extend((e.message_type as u32).to_be_bytes());
    if let Some(r) = &e.blob {
        b.extend(r.format_version.to_be_bytes());
        b.extend(r.blob_id.as_bytes());
        b.extend(r.ciphertext_bytes.to_be_bytes());
        b.extend(&r.sha256);
    }
    b.extend((e.ciphertext.len() as u32).to_be_bytes());
    b.extend(&e.ciphertext);
    Ok(b)
}

pub fn envelope_digest(e: &v2::Envelope) -> Result<[u8; 32], ContractError> {
    Ok(Sha256::digest(envelope_frame(e)?).into())
}

/// Derives exact Send data for a typed control fence; not a native COMMIT proof.
pub fn send_binding(e: &v2::Envelope) -> Result<common::EpochSendBinding, ContractError> {
    let digest = envelope_digest(e)?;
    Ok(common::EpochSendBinding {
        protocol_version: e.protocol_version,
        conversation_id: e.conversation_id.clone(),
        epoch: e.crypto_epoch.unwrap(),
        message_id: e.message_id.clone(),
        sender: e.sender.clone(),
        recipient: e.recipient.clone(),
        envelope_digest: digest.to_vec(),
    })
}

pub fn validate_receipt(
    r: &v2::SendMessageRequest,
    receipt: &v2::SendReceipt,
) -> Result<(), ContractError> {
    validate_send(r)?;
    let e = r.envelope.as_ref().unwrap();
    if epoch(receipt.crypto_epoch)? != e.crypto_epoch.unwrap()
        || receipt.message_id != e.message_id
        || receipt.envelope_digest != envelope_digest(e)?
        || receipt.stored_at_unix_ms <= 0
        || receipt.stored_at_unix_ms.checked_add(MESSAGE_RETENTION_MS)
            != Some(receipt.expires_at_unix_ms)
    {
        return Err(invalid("v2 persistence receipt"));
    }
    Ok(())
}

/// Invoke only AFTER authenticated Signal decrypt in native. This function does
/// not decrypt/authenticate ciphertext, obtain manual consent or create a scope.
pub fn validate_payload_binding(
    p: &v2::EncryptedPayload,
    e: &v2::Envelope,
) -> Result<(), ContractError> {
    validate_envelope(e)?;
    if epoch(p.crypto_epoch)? != e.crypto_epoch.unwrap()
        || p.message_id != e.message_id
        || p.conversation_id != e.conversation_id
        || p.sender != e.sender
        || p.recipient != e.recipient
        || p.created_at_unix_ms < 0
        || p.encoded_len() > MAX_REQUEST_BYTES
    {
        return Err(invalid("v2 authenticated payload binding"));
    }
    match p.content.as_ref() {
        Some(v2::encrypted_payload::Content::Text(t))
            if e.protocol_version == 2
                && p.format_version == 2
                && !t.text.is_empty()
                && t.text.len() <= MAX_TEXT_BYTES => {}
        Some(v2::encrypted_payload::Content::SessionConfirmation(c))
            if e.protocol_version == 2
                && p.format_version == 2
                && uuid4(&c.in_reply_to_message_id)
                && c.in_reply_to_message_id != p.message_id
                && epoch(c.crypto_epoch)? == p.crypto_epoch.unwrap() => {}
        Some(v2::encrypted_payload::Content::Attachment(a)) => {
            super::attachment_payload::validate(p, a)?;
            let reference = super::attachment_payload::blob_reference(a)?;
            if e.protocol_version != 3 || e.blob.as_ref() != Some(&reference) {
                return Err(invalid("private attachment/public reference binding"));
            }
        }
        _ => return Err(invalid("unsupported v2 payload")),
    }
    Ok(())
}

/// Classification only after validate_payload_binding. Confirmation is control,
/// while text and a private attachment descriptor are user-visible history.
pub(crate) fn user_message(p: &v2::EncryptedPayload) -> bool {
    matches!(
        p.content,
        Some(
            v2::encrypted_payload::Content::Text(_) | v2::encrypted_payload::Content::Attachment(_)
        )
    )
}

pub fn validate_acquisition(r: &v2::AcquireBundleRequest) -> Result<(), ContractError> {
    let c = context(r.context.as_ref())?;
    epoch(r.crypto_epoch)?;
    let peer = device(r.expected_peer.as_ref())?;
    if !uuid4(&r.acquisition_id)
        || !uuid4(&r.conversation_id)
        || r.encoded_len() > MAX_REQUEST_BYTES
        || peer.generation_id == c.device_generation_id
    {
        return Err(invalid("v2 acquisition binding"));
    }
    Ok(())
}

/// Public shape/binding only. No private keys or protocol1 fallback.
pub fn validate_acquired_bundle(
    r: &v2::AcquireBundleRequest,
    b: &v2::AcquiredBundle,
) -> Result<(), ContractError> {
    validate_acquisition(r)?;
    let value = epoch(b.crypto_epoch)?;
    validate_epoch_version_binding(b.protocol_version, value, epoch(r.crypto_epoch)?)?;
    let d = b.device.as_ref().ok_or(invalid("v2 bundle device"))?;
    if b.acquisition_id != r.acquisition_id
        || b.conversation_id != r.conversation_id
        || d.reference != r.expected_peer
        || !ec_public(&d.identity_key)
        || !(1..=16383).contains(&d.registration_id)
        || !d.public_material_ready
        || d.public_material_revision == 0
        || d.public_material_revision > i64::MAX as u64
        || b.not_after_unix_ms <= 0
        || b.encoded_len() > MAX_REQUEST_BYTES
    {
        return Err(invalid("v2 bundle binding"));
    }
    let s = b.signed_pre_key.as_ref().ok_or(invalid("v2 signed key"))?;
    let pq = b.kyber_pre_key.as_ref().ok_or(invalid("v2 PQ key"))?;
    if !ec_public(&s.public_key)
        || s.signature.len() != 64
        || s.created_at_unix_ms < 0
        || pq.public_key.len() != 1569
        || pq.public_key[0] != 8
        || pq.signature.len() != 64
        || b.one_time_pre_key
            .as_ref()
            .is_some_and(|k| !ec_public(&k.public_key))
    {
        return Err(invalid("v2 prekey shape"));
    }
    Ok(())
}

/// Also check the retained conversation identity/registration and real libsignal
/// public-key signatures before a future owner bootstraps the epoch. Does NOT
/// enforce lease against current time, consume keys or activate a session.
pub fn verify_pinned_bundle(
    r: &v2::AcquireBundleRequest,
    b: &v2::AcquiredBundle,
    pin: &IdentityKey,
    registration: u32,
) -> Result<(), ContractError> {
    validate_acquired_bundle(r, b)?;
    let d = b.device.as_ref().unwrap();
    if d.identity_key != pin.serialize().as_ref() || d.registration_id != registration {
        return Err(invalid("v2 bundle identity pin"));
    }
    let s = b.signed_pre_key.as_ref().unwrap();
    let pq = b.kyber_pre_key.as_ref().unwrap();
    PublicKey::deserialize(&s.public_key).map_err(|_| invalid("v2 signed key encoding"))?;
    kem::PublicKey::deserialize(&pq.public_key).map_err(|_| invalid("v2 PQ key encoding"))?;
    if let Some(ec) = &b.one_time_pre_key {
        PublicKey::deserialize(&ec.public_key).map_err(|_| invalid("v2 EC key encoding"))?;
    }
    if !pin
        .public_key()
        .verify_signature(&s.public_key, &s.signature)
        || !pin
            .public_key()
            .verify_signature(&pq.public_key, &pq.signature)
    {
        return Err(invalid("v2 prekey signature"));
    }
    Ok(())
}
