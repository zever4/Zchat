//! Explicit deletion of terminal outgoing spool, never of its preparation,
//! history/key/Signal/outbox/receipt/fence metadata. No unknown-message eviction.
use super::*;

#[derive(Message, PartialEq)]
#[prost(skip_debug)]
struct Removed {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(bytes = "vec", tag = "2")]
    preparation_pin: Vec<u8>,
    #[prost(message, optional, tag = "3")]
    binding: Option<crate::proto::e2ee::EpochSendBinding>,
    #[prost(message, optional, tag = "4")]
    blob: Option<v2::BlobRef>,
    #[prost(message, optional, tag = "5")]
    receipt: Option<v2::SendReceipt>,
    #[prost(message, optional, tag = "6")]
    sealed: Option<v2::EpochSealedFailure>,
}
fn key(own: &DeviceRef, message: &str) -> CryptoResult<RecordKey> {
    if !epoch_rpc::uuid4(message) {
        return Err(CryptoError::Invalid);
    }
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("attachment-spool-removed:{message}"),
    )?)
}
pub(in crate::encryption::signal_state::epoch_state) fn is_removed(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    message: &str,
) -> CryptoResult<bool> {
    Ok(tx.get(&key(own, message)?)?.is_some())
}
// Call only with a fully validated outgoing plan. Receipt and sealed-unsent
// fence are immutable terminal facts; later status/ACK refinement is independent.
fn expected(j: &Journal, p: &EpochOutgoingPlan) -> CryptoResult<Removed> {
    if p.needs_send() {
        return Err(CryptoError::AttachmentSendPending);
    }
    let e = p.request().envelope.as_ref().ok_or(CryptoError::Invalid)?;
    if j.promoted_digest.is_none() || e.protocol_version != 3 || e.blob.is_none() {
        return Err(CryptoError::Conflict);
    }
    Ok(Removed {
        format: 1,
        preparation_pin: digest(
            b"zchat.e2ee.attachment-spool-removed.v1\0",
            &Zeroizing::new(j.encode_to_vec()),
        )
        .to_vec(),
        binding: Some(data::send_binding(e).map_err(|_| CryptoError::Invalid)?),
        blob: e.blob.clone(),
        receipt: p.receipt().cloned(),
        sealed: p.sealed_failure().cloned(),
    })
}
/// Called AFTER the ordinary outgoing reader validated every receipt/signature/
/// recovery pin/fence/chain. No recursion into that reader, no file allocation.
pub(in crate::encryption::signal_state::epoch_state) fn validate(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    p: &EpochOutgoingPlan,
) -> CryptoResult<()> {
    let e = p.request().envelope.as_ref().ok_or(CryptoError::Invalid)?;
    let Some(bytes) = tx.get(&key(own, &e.message_id)?)? else {
        return Ok(());
    };
    let marker = Removed::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let j = load_retained_journal(tx, own, &e.message_id)?.ok_or(CryptoError::Conflict)?;
    if marker != expected(&j, p)? {
        return Err(CryptoError::Conflict);
    }
    tx.check_removed_attachment(&super::key(own, &e.message_id)?)?;
    Ok(())
}
pub(in crate::encryption::signal_state::epoch_state) fn remove(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    conversation: &str,
    message: &str,
) -> CryptoResult<()> {
    let (j, p) = archived::cohort(tx, own, store, conversation, message)?;
    let marker = expected(&j, &p)?;
    if is_removed(tx, own, message)? {
        return Ok(());
    } // Fresh full validation in cohort.
    drop(archived::open_bytes(tx, own, &j)?);
    let a = part(j.payload.as_ref().ok_or(CryptoError::Invalid)?)?;
    let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor)
        .map_err(|_| CryptoError::Invalid)?;
    tx.put(&key(own, message)?, &marker.encode_to_vec())?;
    tx.remove_known_attachment(
        &super::key(own, message)?,
        &a.blob_id,
        descriptor.blob_len(),
        descriptor.blob_digest(),
    )?;
    Ok(())
}
