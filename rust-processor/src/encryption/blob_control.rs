//! Public metadata boundary, never a Signal/file key or a message receipt.
use super::{
    contract::{ContractError, MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES},
    epoch_contract, epoch_rpc,
};
use crate::proto::{e2ee as common, e2ee_v2 as v2};
use prost::Message;

pub const STAGING_LEASE_MS: i64 = 3_600_000;
fn invalid() -> ContractError {
    ContractError::Invalid("blob control binding")
}
/// Compact authenticated metadata observation for this EXACT attachment Send.
/// Not a receipt, unsent fence or authority to renew a lease/reset Signal.
pub fn validate_send_failure(
    r: &v2::SendMessageRequest,
    f: &v2::BlobSendFailure,
) -> Result<(), ContractError> {
    epoch_contract::validate_send(r)?;
    if r.envelope.as_ref().unwrap().protocol_version != 3
        || f.encoded_len() > MAX_RESPONSE_BYTES
        || !matches!(
            f.reason(),
            v2::BlobSendFailureReason::NotReady | v2::BlobSendFailureReason::LeaseExpired
        )
        || f.binding.as_ref() != Some(&epoch_contract::send_binding(r.envelope.as_ref().unwrap())?)
    {
        return Err(invalid());
    }
    Ok(())
}
pub fn validate_request(r: &v2::BlobLeaseRequest) -> Result<(), ContractError> {
    epoch_rpc::validate_context(r.context.as_ref())?;
    // Validation-only envelope; NEVER sent or used as Signal ciphertext.
    epoch_contract::validate_envelope(&v2::Envelope {
        protocol_version: 3,
        crypto_suite: common::CryptoSuite::SignalPq1 as i32,
        message_id: r.message_id.clone(),
        conversation_id: r.conversation_id.clone(),
        sender: r.sender.clone(),
        recipient: r.recipient.clone(),
        message_type: common::CiphertextType::Signal as i32,
        ciphertext: vec![1],
        crypto_epoch: r.crypto_epoch,
        blob: r.blob.clone(),
    })?;
    if r.encoded_len() > MAX_REQUEST_BYTES
        || !epoch_rpc::uuid4(&r.operation_id)
        || r.context.as_ref().unwrap().device_generation_id
            != r.sender.as_ref().unwrap().generation_id
        || r.expected_revision >= i64::MAX as u64
        || if r.expected_revision == 0 {
            !r.expected_attempt_id.is_empty()
        } else {
            !epoch_rpc::uuid4(&r.expected_attempt_id)
        }
    {
        return Err(invalid());
    }
    Ok(())
}
pub fn validate_lease(r: &v2::BlobLeaseRequest, g: &v2::BlobLease) -> Result<(), ContractError> {
    validate_request(r)?;
    if g.encoded_len() > MAX_RESPONSE_BYTES
        || g.message_id != r.message_id
        || g.operation_id != r.operation_id
        || !epoch_rpc::uuid4(&g.attempt_id)
        || g.revision != r.expected_revision + 1
        || g.reserved_at_unix_ms <= 0
        || g.reserved_at_unix_ms.checked_add(STAGING_LEASE_MS) != Some(g.expires_at_unix_ms)
        || g.observed_at_unix_ms < g.reserved_at_unix_ms
        || g.expired != (g.observed_at_unix_ms >= g.expires_at_unix_ms)
        || phase(g.state).is_none()
    {
        return Err(invalid());
    }
    Ok(())
}
pub fn validate_failure(
    r: &v2::BlobLeaseRequest,
    f: &v2::BlobFailure,
    observe: bool,
) -> Result<(), ContractError> {
    validate_request(r)?;
    if f.encoded_len() > MAX_RESPONSE_BYTES
        || f.request.as_ref() != Some(r)
        || failure_code(f.reason, observe).is_none()
    {
        return Err(invalid());
    }
    Ok(())
}
pub(crate) fn failure_code(reason: i32, observe: bool) -> Option<tonic::Code> {
    use v2::BlobFailureReason as R;
    Some(match R::try_from(reason).ok()? {
        R::OperationNotObserved if observe => tonic::Code::NotFound,
        R::CasChanged => tonic::Code::Aborted,
        R::ActiveLease | R::StorageUnavailable => tonic::Code::FailedPrecondition,
        R::BindingConflict => tonic::Code::AlreadyExists,
        R::Capacity => tonic::Code::ResourceExhausted,
        _ => return None,
    })
}
pub(crate) fn phase(state: i32) -> Option<u8> {
    use v2::BlobLeaseState as S;
    Some(match S::try_from(state).ok()? {
        S::Reserved => 0,
        S::Writing => 1,
        S::Ready => 2,
        S::Linked => 3,
        S::Deleting => 4,
        S::Reclaimed => 5,
        _ => return None,
    })
}
pub(crate) fn renewable(g: &v2::BlobLease) -> bool {
    use v2::BlobLeaseState as S;
    g.state != S::Linked as i32
        && (g.expired || matches!(S::try_from(g.state), Ok(S::Deleting | S::Reclaimed)))
}
