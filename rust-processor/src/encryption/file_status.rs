//! Public sender-file metadata shapes, not authenticated observations/cache or
//! filesystem proofs. Receipt/ACK/expiry/replacement remain independent facts.
use super::{
    contract::{ContractError, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, MESSAGE_RETENTION_MS},
    epoch_contract, epoch_rpc, recovery_contract,
};
use crate::proto::e2ee_v2 as v2;
use prost::Message;
use std::collections::BTreeSet;

pub const MAX_FILE_STATUSES: usize = 32;
pub const FILE_STATUS_FORMAT: u32 = 1;
pub const FILE_RETENTION_MS: i64 = 720 * 60 * 60 * 1000;
fn invalid() -> ContractError {
    ContractError::Invalid("file status binding or facts")
}
pub fn validate_request(r: &v2::GetFileStatusesRequest) -> Result<(), ContractError> {
    epoch_rpc::validate_context(r.context.as_ref())?;
    if r.files.is_empty()
        || r.files.len() > MAX_FILE_STATUSES
        || r.encoded_len() > MAX_REQUEST_BYTES
    {
        return Err(invalid());
    }
    let generation = &r.context.as_ref().unwrap().device_generation_id;
    let mut sender = None;
    let mut ids = BTreeSet::new();
    for reference in &r.files {
        let b = reference.binding.as_ref().ok_or_else(invalid)?;
        recovery_contract::validate_send_binding(b)?;
        epoch_contract::validate_blob_ref(reference.blob.as_ref().ok_or_else(invalid)?)?;
        let own = b.sender.as_ref().unwrap();
        if b.protocol_version != 3
            || &own.generation_id != generation
            || sender.is_some_and(|s| s != &own.user_id)
            || !ids.insert(&b.message_id)
        {
            return Err(invalid());
        }
        sender = Some(&own.user_id);
    }
    Ok(())
}
pub fn validate_response(
    r: &v2::GetFileStatusesRequest,
    response: &v2::FileStatuses,
) -> Result<(), ContractError> {
    validate_request(r)?;
    if response.files.len() != r.files.len() || response.encoded_len() > MAX_RESPONSE_BYTES {
        return Err(invalid());
    }
    for (reference, s) in r.files.iter().zip(&response.files) {
        let b = reference.binding.as_ref().unwrap();
        let receipt = s.receipt.as_ref().ok_or_else(invalid)?;
        let stored = receipt.stored_at_unix_ms;
        let deadline = stored.checked_add(FILE_RETENTION_MS).ok_or_else(invalid)?;
        let message_deadline = stored
            .checked_add(MESSAGE_RETENTION_MS)
            .ok_or_else(invalid)?;
        if s.format_version != FILE_STATUS_FORMAT
            || s.reference.as_ref() != Some(reference)
            || receipt.crypto_epoch != Some(b.epoch)
            || receipt.message_id != b.message_id
            || receipt.envelope_digest != b.envelope_digest
            || stored <= 0
            || receipt.expires_at_unix_ms != message_deadline
            || s.expires_at_unix_ms != deadline
            || s.observed_at_unix_ms < stored
            || s.observed_at_unix_ms != response.files[0].observed_at_unix_ms
        {
            return Err(invalid());
        }
        for at in [s.acknowledged_at_unix_ms, s.recipient_replaced_at_unix_ms]
            .into_iter()
            .flatten()
        {
            if at < stored || at > s.observed_at_unix_ms {
                return Err(invalid());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "file_status_tests.rs"]
mod tests;
