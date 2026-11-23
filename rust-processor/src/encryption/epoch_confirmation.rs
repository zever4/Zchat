//! One protected reply per authenticated PreKey user message, in the receive TX.
//! Signal session confirmation is neither a delivery status nor a read receipt.
use super::{outgoing, *};
use crate::encryption::epoch_contract as data;

#[derive(Message)]
struct Link {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    reply_id: String,
    #[prost(bytes = "vec", tag = "3")]
    incoming_digest: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    reply_digest: Vec<u8>,
}
fn key(s: &EpochScope, incoming: &str) -> CryptoResult<RecordKey> {
    Ok(RecordKey::new(
        &s.own.generation_id,
        RecordKind::Control,
        &format!("session-confirmation:{}", outgoing::record_id(s, incoming)?),
    )?)
}
fn eligible(e: &v2::Envelope, p: &v2::EncryptedPayload) -> bool {
    e.message_type == CiphertextType::PrekeySignal as i32 && data::user_message(p)
}
pub(super) fn check_absent(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    incoming: &str,
) -> CryptoResult<()> {
    if tx.get(&key(s, incoming)?)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
// Protected cohort only: explicit index seeding may call this before SQL
// projections exist. Normal reads/ACK paths also check the reply projections.
pub(super) fn validate(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    e: &v2::Envelope,
    p: &v2::EncryptedPayload,
    required: bool,
    history_only: bool,
) -> CryptoResult<Option<EpochOutgoingPlan>> {
    let bytes = tx.get(&key(s, &e.message_id)?)?;
    if !required {
        if bytes.is_some() {
            return Err(CryptoError::Conflict);
        }
        return Ok(None);
    }
    if !eligible(e, p) {
        return Err(CryptoError::Invalid);
    }
    let bytes = bytes.ok_or(CryptoError::Invalid)?;
    let l = Link::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if l.format != FORMAT
        || l.reply_id == e.message_id
        || l.incoming_digest != data::envelope_digest(e).map_err(|_| CryptoError::Invalid)?
    {
        return Err(CryptoError::Conflict);
    }
    let id = outgoing::record_id(s, &l.reply_id)?;
    let plan = if history_only {
        outgoing::view_plan(tx, &s.own, &s.scope, &id)?
    } else {
        outgoing::plan(tx, &s.own, &s.scope, &id)?
    };
    let reply = plan
        .request()
        .envelope
        .as_ref()
        .ok_or(CryptoError::Invalid)?;
    let bytes = tx
        .get(&outgoing::record_key(&s.own, RecordKind::History, &id)?)?
        .ok_or(CryptoError::Invalid)?;
    let payload =
        v2::EncryptedPayload::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if l.reply_digest != data::envelope_digest(reply).map_err(|_| CryptoError::Invalid)?
        || !matches!(payload.content, Some(v2::encrypted_payload::Content::SessionConfirmation(ref c))
            if c.in_reply_to_message_id == e.message_id && c.crypto_epoch == Some(s.epoch))
    {
        return Err(CryptoError::Conflict);
    }
    Ok(Some(plan))
}
pub(super) fn prepare(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    s: &EpochScope,
    e: &v2::Envelope,
    p: &v2::EncryptedPayload,
) -> CryptoResult<bool> {
    let link = key(s, &e.message_id)?;
    // Only the fresh receive branch may prepare a reply. Never repair a lost
    // link or append a reply to an already committed receive on replay.
    if tx.get(&link)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    if !eligible(e, p) {
        return Ok(false); // No reply to a confirmation, even a PreKey one.
    }
    match bootstrap::sending(s) {
        Ok(()) => (),
        Err(CryptoError::ConversationClosed | CryptoError::EpochSendingFrozen) => return Ok(false),
        Err(error) => return Err(error),
    }
    let reply = v2::EncryptedPayload {
        format_version: FORMAT,
        message_id: uuid::Uuid::new_v4().to_string(),
        conversation_id: s.conversation.clone(),
        sender: Some(s.own.clone()),
        recipient: Some(s.peer.clone()),
        created_at_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| CryptoError::Invalid)?
            .as_millis()
            .try_into()
            .map_err(|_| CryptoError::Invalid)?,
        content: Some(v2::encrypted_payload::Content::SessionConfirmation(
            v2::SessionConfirmation {
                in_reply_to_message_id: e.message_id.clone(),
                crypto_epoch: Some(s.epoch),
            },
        )),
        crypto_epoch: Some(s.epoch),
    };
    let plan = outgoing::encrypt_message(tx, runtime, s, &reply)?;
    tx.put(
        &link,
        &Link {
            format: FORMAT,
            reply_id: reply.message_id,
            incoming_digest: data::envelope_digest(e)
                .map_err(|_| CryptoError::Invalid)?
                .to_vec(),
            reply_digest: data::envelope_digest(plan.request().envelope.as_ref().unwrap())
                .map_err(|_| CryptoError::Invalid)?
                .to_vec(),
        }
        .encode_to_vec(),
    )?;
    Ok(true)
}

#[cfg(test)]
#[path = "epoch_confirmation_tests.rs"]
mod tests;
