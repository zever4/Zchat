//! One durable reply for each authenticated PreKey text, in the receive TX.
//! This is a Signal session confirmation, never a delivery/read status.
use super::*;
use crate::proto::e2ee::{SessionConfirmation, encrypted_payload};
use uuid::Uuid;

const PREFIX: &str = "session-confirmation:";
#[derive(Message)]
struct ConfirmationJournal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    message_id: String,
}
fn key(scope: &SessionScope, incoming_id: &str) -> CryptoResult<RecordKey> {
    RecordKey::new(incoming_id, RecordKind::Control, "uuid")?;
    Ok(scope.key(RecordKind::Control, &format!("{PREFIX}{incoming_id}"))?)
}
fn check_saved(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    incoming: &Envelope,
    saved: ConfirmationJournal,
) -> CryptoResult<()> {
    let incoming_id = incoming.message_id.as_str();
    if saved.format != 1 || saved.message_id == incoming_id {
        return Err(CryptoError::Invalid);
    }
    RecordKey::new(&saved.message_id, RecordKind::Control, "uuid")?;
    let envelope = tx
        .get(&RecordKey::new(
            &own.generation_id,
            RecordKind::Outbox,
            &saved.message_id,
        )?)?
        .ok_or(CryptoError::Invalid)?;
    let envelope = Envelope::decode(envelope.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let payload = tx
        .get(&RecordKey::new(
            &own.generation_id,
            RecordKind::History,
            &saved.message_id,
        )?)?
        .ok_or(CryptoError::Invalid)?;
    let payload = EncryptedPayload::decode(payload.as_slice()).map_err(|_| CryptoError::Invalid)?;
    validate_payload_binding(&payload, &envelope).map_err(|_| CryptoError::Invalid)?;
    if payload.message_id != saved.message_id
        || payload.conversation_id != incoming.conversation_id
        || payload.sender.as_ref() != Some(own)
        || payload.recipient != incoming.sender
        || !matches!(payload.content, Some(encrypted_payload::Content::SessionConfirmation(ref reply))
            if reply.in_reply_to_message_id == incoming_id)
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
/// ACK recovery also checks the durable link: a deleted/corrupt reply must not
/// be silently regenerated or hidden by removing the server's incoming copy.
pub(super) fn validate_existing(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    incoming: &Envelope,
) -> CryptoResult<()> {
    let key = RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("{PREFIX}{}", incoming.message_id),
    )?;
    let bytes = tx.get(&key)?.ok_or(CryptoError::Invalid)?;
    let saved = ConfirmationJournal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    check_saved(tx, own, incoming, saved)
}
pub(super) fn prepare(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    scope: &SessionScope,
    envelope: &Envelope,
    payload: &EncryptedPayload,
) -> CryptoResult<bool> {
    // Ordinary Signal texts need no reply. Even a PreKey confirmation must
    // never generate another confirmation (avoids automatic ping-pong).
    if envelope.message_type != CiphertextType::PrekeySignal as i32
        || !matches!(payload.content, Some(encrypted_payload::Content::Text(_)))
    {
        return Ok(false);
    }
    let journal = key(scope, &payload.message_id)?;
    if let Some(bytes) = tx.get(&journal)? {
        let saved =
            ConfirmationJournal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
        check_saved(tx, &scope.own, envelope, saved)?;
        return Ok(true);
    }
    // Receiving a previously accepted message is still allowed after peer
    // replacement, but it must not create new outbound crypto state/content.
    match scope.check_sending(tx) {
        Ok(()) => (),
        Err(CryptoError::ConversationClosed) => return Ok(false),
        Err(error) => return Err(error),
    }
    let message_id = Uuid::new_v4().to_string();
    let reply = EncryptedPayload {
        format_version: 1,
        message_id: message_id.clone(),
        conversation_id: scope.conversation.clone(),
        sender: Some(scope.own.clone()),
        recipient: Some(scope.peer.clone()),
        created_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CryptoError::Invalid)?
            .as_millis()
            .try_into()
            .map_err(|_| CryptoError::Invalid)?,
        content: Some(encrypted_payload::Content::SessionConfirmation(
            SessionConfirmation {
                in_reply_to_message_id: payload.message_id.clone(),
            },
        )),
    };
    // UUID, ratchet mutation, history, exact ciphertext and link commit together.
    encrypt_message(tx, runtime, scope, &reply)?;
    tx.put(
        &journal,
        &ConfirmationJournal {
            format: 1,
            message_id,
        }
        .encode_to_vec(),
    )?;
    Ok(true)
}
