//! Local-only reading of promoted outgoing files. Never expose a preparation,
//! upload or Send capability through the archive API, even for pending outbox.
use super::*;

pub(super) fn cohort(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    conversation: &str,
    message: &str,
) -> CryptoResult<(Journal, EpochOutgoingPlan)> {
    recovery_indexes::require_ready(tx, own)?;
    let link = outgoing::load_link(tx, own, message)?;
    if link.outgoing != Some(true) || link.attachment_preparation.is_none() {
        return Err(CryptoError::Conflict);
    }
    // The history-only reader validates outbox/envelope/promotion pin, receipt,
    // status/fence, chain and projections even after own closure/replacement.
    let plan = outgoing::view_plan(tx, own, store, &link.id)?;
    outgoing::check_projections(tx, own, &link.id, plan.needs_status_poll())?;
    let journal =
        load_retained_journal(tx, own, message)?.ok_or(CryptoError::MessageUnavailable)?;
    let payload = journal.payload.as_ref().ok_or(CryptoError::Invalid)?;
    history::check(tx, own, &link.id, payload)?;
    if payload.conversation_id != conversation || payload.message_id != message {
        return Err(CryptoError::Conflict);
    }
    Ok((journal, plan))
}

pub(super) fn open_bytes(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    journal: &Journal,
) -> CryptoResult<Zeroizing<Vec<u8>>> {
    let payload = journal.payload.as_ref().ok_or(CryptoError::Invalid)?;
    let a = part(payload)?;
    let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor)
        .map_err(|_| CryptoError::Invalid)?;
    let binding = attachment::AttachmentBinding::new(
        &payload.conversation_id,
        payload.crypto_epoch.ok_or(CryptoError::Invalid)?,
        &payload.message_id,
        &a.blob_id,
        own,
        payload.recipient.as_ref().ok_or(CryptoError::Invalid)?,
    )
    .map_err(|_| CryptoError::Invalid)?;
    let ciphertext = tx.attachment_blob(
        &key(own, &payload.message_id)?,
        &a.blob_id,
        descriptor.blob_len(),
        descriptor.blob_digest(),
        true,
    )?;
    let plaintext =
        attachment::open(&binding, &descriptor, &ciphertext).map_err(|_| CryptoError::Invalid)?;
    if Sha256::digest(plaintext.as_slice()).as_slice() != journal.source_digest {
        return Err(CryptoError::Conflict);
    }
    Ok(plaintext)
}

pub(in crate::encryption::signal_state::epoch_state) fn open(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    conversation: &str,
    message: &str,
) -> CryptoResult<(Zeroizing<Vec<u8>>, v2::AttachmentKind)> {
    let (journal, _) = cohort(tx, own, store, conversation, message)?;
    // cohort's view_plan freshly validates any retirement marker and absence
    // of all artifacts before this becomes a distinct unavailable-copy result.
    if retirement::is_removed(tx, own, message)? {
        return Err(CryptoError::AttachmentRemoved);
    }
    let payload = journal.payload.as_ref().ok_or(CryptoError::Invalid)?;
    let kind =
        v2::AttachmentKind::try_from(part(payload)?.kind).map_err(|_| CryptoError::Invalid)?;
    Ok((open_bytes(tx, own, &journal)?, kind))
}
