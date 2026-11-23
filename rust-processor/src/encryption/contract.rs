//! V1 client boundary. Call payload validation only AFTER libsignal decrypt.
//! This module neither decrypts nor replaces identity/signature verification.
use crate::proto::e2ee::{
    AckDisposition, AcknowledgeMessagesRequest, CiphertextType, Conversation,
    ConversationCloseReason, ConversationMode, ConversationState, CryptoSuite, DeviceRef,
    EncryptedPayload, Envelope, GetMessageStatusesRequest, MessageState, MessageStatuses,
    PendingMessages, SendMessageRequest, SendReceipt, encrypted_payload,
};
use prost::Message;
use sha2::{Digest, Sha256};
use uuid::{Uuid, Variant};

pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_REQUEST_BYTES: usize = 48 << 10;
pub const MAX_RESPONSE_BYTES: usize = 64 << 10;
pub const MAX_INBOX_MESSAGES: usize = 32;
pub const MAX_ACKNOWLEDGEMENTS: usize = 100;
pub const MAX_TEXT_BYTES: usize = 2000;
pub const MESSAGE_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ContractError {
    #[error("unsupported E2EE protocol version")]
    UnsupportedVersion,
    // Reasons are static: never log plaintext, credentials or private state.
    #[error("invalid E2EE contract: {0}")]
    Invalid(&'static str),
}

fn uuid4(value: &str) -> bool {
    value.len() == 36
        && Uuid::parse_str(value).is_ok_and(|id| {
            id.get_version_num() == 4
                && id.get_variant() == Variant::RFC4122
                && id.to_string() == value
        })
}

fn validate_device(device: Option<&DeviceRef>) -> Result<(), ContractError> {
    let device = device.ok_or(ContractError::Invalid("missing device"))?;
    if !uuid4(&device.user_id)
        || !uuid4(&device.generation_id)
        || !(1..=127).contains(&device.device_id)
    {
        return Err(ContractError::Invalid("device reference"));
    }
    Ok(())
}

/// Validate both active and archived responses. Retirement can clear readiness,
/// but cannot change the original identity, registration or device references.
pub fn validate_conversation(value: &Conversation) -> Result<(), ContractError> {
    let own = value.own_device.as_ref();
    let peer = value.peer_device.as_ref();
    validate_device(own)?;
    validate_device(peer.and_then(|d| d.reference.as_ref()))?;
    let own = own.unwrap();
    let peer = peer.unwrap();
    let reference = peer.reference.as_ref().unwrap();
    if value.encoded_len() > 2048
        || !uuid4(&value.conversation_id)
        || value.mode != ConversationMode::SignalPq1 as i32
        || own.device_id != 1
        || reference.device_id != 1
        || own.user_id == reference.user_id
        || own.generation_id == reference.generation_id
        || !(1..=16383).contains(&peer.registration_id)
        || peer.identity_key.len() != 33
        || peer.identity_key.first() != Some(&5)
        || peer.public_material_revision == 0
    {
        return Err(ContractError::Invalid("conversation binding"));
    }
    match ConversationState::try_from(value.state) {
        Ok(ConversationState::Active)
            if peer.public_material_ready
                && value.close_reason == 0
                && value.closed_generation_id.is_empty()
                && value.closed_at_unix_ms == 0 => {}
        Ok(ConversationState::Closed)
            if value.close_reason == ConversationCloseReason::DeviceReplaced as i32
                && value.closed_at_unix_ms > 0
                && (value.closed_generation_id == own.generation_id
                    || value.closed_generation_id == reference.generation_id) => {}
        _ => return Err(ContractError::Invalid("conversation state")),
    }
    Ok(())
}

pub fn validate_envelope(envelope: &Envelope) -> Result<(), ContractError> {
    if envelope.protocol_version != PROTOCOL_VERSION {
        return Err(ContractError::UnsupportedVersion);
    }
    if envelope.encoded_len() > MAX_REQUEST_BYTES
        || !uuid4(&envelope.message_id)
        || !uuid4(&envelope.conversation_id)
        || envelope.crypto_suite != CryptoSuite::SignalPq1 as i32
        || envelope.ciphertext.is_empty()
        || !matches!(
            CiphertextType::try_from(envelope.message_type),
            Ok(CiphertextType::PrekeySignal | CiphertextType::Signal)
        )
    {
        return Err(ContractError::Invalid("envelope fields"));
    }
    validate_device(envelope.sender.as_ref())?;
    validate_device(envelope.recipient.as_ref())?;
    if envelope.sender.as_ref().map(|d| &d.user_id)
        == envelope.recipient.as_ref().map(|d| &d.user_id)
    {
        return Err(ContractError::Invalid("private recipient"));
    }
    Ok(())
}

pub fn validate_send(request: &SendMessageRequest) -> Result<(), ContractError> {
    let context = request
        .context
        .as_ref()
        .ok_or(ContractError::Invalid("send context"))?;
    let envelope = request
        .envelope
        .as_ref()
        .ok_or(ContractError::Invalid("send envelope"))?;
    validate_envelope(envelope)?;
    if context.protocol_version != PROTOCOL_VERSION
        || request.encoded_len() > MAX_REQUEST_BYTES
        || envelope
            .sender
            .as_ref()
            .is_none_or(|sender| sender.generation_id != context.device_generation_id)
    {
        return Err(ContractError::Invalid("send binding"));
    }
    Ok(())
}

/// A receipt confirms server persistence only. An expired exact replay remains
/// valid evidence of that past COMMIT; do not extend it relative to local time.
pub fn validate_send_receipt(
    request: &SendMessageRequest,
    receipt: &SendReceipt,
) -> Result<(), ContractError> {
    validate_send(request)?;
    let envelope = request.envelope.as_ref().unwrap();
    if receipt.message_id != envelope.message_id
        || receipt.envelope_digest != envelope_digest(envelope)?
        || receipt.stored_at_unix_ms <= 0
        || receipt.stored_at_unix_ms.checked_add(MESSAGE_RETENTION_MS)
            != Some(receipt.expires_at_unix_ms)
    {
        return Err(ContractError::Invalid("persistence receipt"));
    }
    Ok(())
}

/// Transport validation only. The worker still checks conversation bindings,
/// Signal authentication and local durability before any recipient ACK.
pub fn validate_pending(page: &PendingMessages, own: &DeviceRef) -> Result<(), ContractError> {
    validate_device(Some(own))?;
    if page.encoded_len() > MAX_RESPONSE_BYTES || page.messages.len() > MAX_INBOX_MESSAGES {
        return Err(ContractError::Invalid("inbox bounds"));
    }
    let mut previous = 0;
    let mut ids = std::collections::HashSet::new();
    for delivery in &page.messages {
        let e = delivery
            .envelope
            .as_ref()
            .ok_or(ContractError::Invalid("missing delivery"))?;
        validate_envelope(e)?;
        if delivery.sequence <= previous
            || delivery.expires_at_unix_ms <= 0
            || delivery.envelope_digest != envelope_digest(e)?
            || e.recipient.as_ref() != Some(own)
            || own.device_id != 1
            || e.sender.as_ref().is_none_or(|sender| sender.device_id != 1)
            || !ids.insert(&e.message_id)
        {
            return Err(ContractError::Invalid("delivery binding"));
        }
        previous = delivery.sequence;
    }
    Ok(())
}

pub fn validate_status_request(request: &GetMessageStatusesRequest) -> Result<(), ContractError> {
    let context = request
        .context
        .as_ref()
        .ok_or(ContractError::Invalid("status context"))?;
    let mut ids = std::collections::HashSet::new();
    if context.protocol_version != PROTOCOL_VERSION
        || !uuid4(&context.device_generation_id)
        || request.messages.is_empty()
        || request.messages.len() > MAX_ACKNOWLEDGEMENTS
        || request.encoded_len() > MAX_REQUEST_BYTES
        || request.messages.iter().any(|r| {
            !uuid4(&r.message_id) || r.envelope_digest.len() != 32 || !ids.insert(&r.message_id)
        })
    {
        return Err(ContractError::Invalid("status request"));
    }
    Ok(())
}

pub fn validate_statuses(
    request: &GetMessageStatusesRequest,
    response: &MessageStatuses,
) -> Result<(), ContractError> {
    validate_status_request(request)?;
    if response.encoded_len() > MAX_RESPONSE_BYTES
        || response.messages.len() != request.messages.len()
    {
        return Err(ContractError::Invalid("status bounds"));
    }
    for (reference, value) in request.messages.iter().zip(&response.messages) {
        let receipt = value
            .receipt
            .as_ref()
            .ok_or(ContractError::Invalid("status receipt"))?;
        if receipt.message_id != reference.message_id
            || receipt.envelope_digest != reference.envelope_digest
            || receipt.stored_at_unix_ms <= 0
            || receipt.stored_at_unix_ms.checked_add(MESSAGE_RETENTION_MS)
                != Some(receipt.expires_at_unix_ms)
            || value.observed_at_unix_ms < receipt.stored_at_unix_ms
        {
            return Err(ContractError::Invalid("status receipt binding"));
        }
        match MessageState::try_from(value.state) {
            Ok(MessageState::Pending) if value.observed_at_unix_ms == receipt.stored_at_unix_ms => {
            }
            Ok(MessageState::Expired)
                if value.observed_at_unix_ms >= receipt.expires_at_unix_ms => {}
            Ok(
                MessageState::RecipientStored
                | MessageState::RecipientRejected
                | MessageState::RecipientReplaced,
            ) => {}
            _ => return Err(ContractError::Invalid("status state")),
        }
    }
    // Expiry is not final evidence of non-delivery. A later matching ACK may
    // refine it to Stored/Rejected without recreating ciphertext or its deadline.
    Ok(())
}

pub fn validate_acknowledgements(
    request: &AcknowledgeMessagesRequest,
) -> Result<(), ContractError> {
    let context = request
        .context
        .as_ref()
        .ok_or(ContractError::Invalid("ACK context"))?;
    let mut ids = std::collections::HashSet::new();
    if context.protocol_version != PROTOCOL_VERSION
        || !uuid4(&context.device_generation_id)
        || request.messages.is_empty()
        || request.messages.len() > MAX_ACKNOWLEDGEMENTS
        || request.encoded_len() > MAX_REQUEST_BYTES
        || request.messages.iter().any(|r| {
            !uuid4(&r.message_id)
                || r.envelope_digest.len() != 32
                || !ids.insert(&r.message_id)
                || !matches!(
                    AckDisposition::try_from(r.disposition),
                    Ok(AckDisposition::Stored | AckDisposition::Rejected)
                )
        })
    {
        return Err(ContractError::Invalid("ACK request"));
    }
    Ok(())
}

pub fn validate_payload_binding(
    payload: &EncryptedPayload,
    envelope: &Envelope,
) -> Result<(), ContractError> {
    validate_envelope(envelope)?;
    if payload.format_version != PROTOCOL_VERSION
        || payload.message_id != envelope.message_id
        || payload.conversation_id != envelope.conversation_id
        || payload.sender != envelope.sender
        || payload.recipient != envelope.recipient
        || payload.created_at_unix_ms < 0
    {
        return Err(ContractError::Invalid("authenticated payload binding"));
    }
    match payload.content.as_ref() {
        Some(encrypted_payload::Content::Text(text))
            if !text.text.is_empty() && text.text.len() <= MAX_TEXT_BYTES => {}
        Some(encrypted_payload::Content::SessionConfirmation(reply))
            if uuid4(&reply.in_reply_to_message_id)
                && reply.in_reply_to_message_id != payload.message_id => {}
        _ => return Err(ContractError::Invalid("unsupported encrypted payload")),
    }
    Ok(())
}

/// SHA256 of the explicitly framed v1 fields, not protobuf serialization.
/// See proto/fixtures/README.md. A matching hash is not authentication or a MAC.
pub fn envelope_digest(envelope: &Envelope) -> Result<[u8; 32], ContractError> {
    validate_envelope(envelope)?;
    let mut hash = Sha256::new();
    hash.update(b"zchat.e2ee.envelope.v1\0");
    hash.update(envelope.protocol_version.to_be_bytes());
    hash.update((envelope.crypto_suite as u32).to_be_bytes());
    hash.update(envelope.message_id.as_bytes());
    hash.update(envelope.conversation_id.as_bytes());
    for device in [&envelope.sender, &envelope.recipient] {
        let device = device
            .as_ref()
            .ok_or(ContractError::Invalid("missing device"))?;
        hash.update(device.user_id.as_bytes());
        hash.update(device.device_id.to_be_bytes());
        hash.update(device.generation_id.as_bytes());
    }
    hash.update((envelope.message_type as u32).to_be_bytes());
    hash.update((envelope.ciphertext.len() as u32).to_be_bytes());
    hash.update(&envelope.ciphertext);
    Ok(hash.finalize().into())
}
