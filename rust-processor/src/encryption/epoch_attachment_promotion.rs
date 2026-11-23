//! Protected preparation -> Signal/outbox bridge. No network or file-cache ACK.
//! Retained readers validate the whole metadata cohort without opening the blob.
use super::*;

pub enum EpochAttachmentPromotion {
    Queued(Box<EpochOutgoingPlan>),
    // No local Signal acceptance. Existing acquisition/recovery gates still apply.
    BootstrapRequired,
}

fn ready(j: &Journal) -> CryptoResult<()> {
    let lease = j.lease.as_ref().ok_or(CryptoError::Conflict)?;
    if j.promoted_digest.is_some()
        || j.linked_seen != Some(false)
        || lease.expired
        || lease.state != v2::BlobLeaseState::Ready as i32
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}

// The prepared-only encryption entry rechecks authority, rather than taking a
// boolean exception to the generic path's reservation guard.
pub(in crate::encryption::signal_state::epoch_state) fn check_promotion(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    payload: &v2::EncryptedPayload,
) -> CryptoResult<()> {
    let j =
        load_journal(tx, &s.own, &payload.message_id)?.ok_or(CryptoError::MessageUnavailable)?;
    if j.payload.as_ref() != Some(payload) {
        return Err(CryptoError::Conflict);
    }
    let actual = current(tx, &s.own, &s.scope, &payload.conversation_id)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    if payload.crypto_epoch != Some(actual.epoch)
        || payload.sender.as_ref() != Some(&actual.own)
        || payload.recipient.as_ref() != Some(&actual.peer)
    {
        return Err(CryptoError::Conflict);
    }
    ready(&j)?;
    // Bounded streaming verification; no retained whole-file buffer.
    let _ = local_plan(tx, &s.own, &s.scope, &j)?;
    Ok(())
}

// Called only by the prepared-only encryption entry, before its final cohort
// read and the surrounding transaction COMMIT. The journal is never deleted.
pub(in crate::encryption::signal_state::epoch_state) fn mark_promoted(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    payload: &v2::EncryptedPayload,
    envelope: &v2::Envelope,
) -> CryptoResult<()> {
    let mut j = load_retained_journal(tx, &s.own, &payload.message_id)?
        .ok_or(CryptoError::MessageUnavailable)?;
    if j.payload.as_ref() != Some(payload) {
        return Err(CryptoError::Conflict);
    }
    ready(&j)?;
    data::validate_payload_binding(payload, envelope).map_err(|_| CryptoError::Invalid)?;
    let digest = data::envelope_digest(envelope).map_err(|_| CryptoError::Invalid)?;
    let mut link = outgoing::load_link(tx, &s.own, &payload.message_id)?;
    if link.outgoing != Some(true) || link.digest != digest || link.attachment_preparation.is_some()
    {
        return Err(CryptoError::Conflict);
    }
    link.attachment_preparation = Some(control::cohort_pin(&j).to_vec());
    j.promoted_digest = Some(digest.to_vec());
    tx.put(
        &key(&s.own, &payload.message_id)?,
        &Zeroizing::new(j.encode_to_vec()),
    )?;
    tx.put(
        &outgoing::link_key(&s.own, &payload.message_id)?,
        &link.encode_to_vec(),
    )?;
    Ok(())
}

// Used by ALL outgoing/history/settlement readers, including frozen/archived
// scopes. No blob/chunk lookup: expiry/loss must not hide a receipt or fence.
pub(in crate::encryption::signal_state::epoch_state) fn validate_cohort(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    link: &outgoing::Link,
    envelope: &v2::Envelope,
    payload: &v2::EncryptedPayload,
) -> CryptoResult<()> {
    let j = load_retained_journal(tx, own, &envelope.message_id)?;
    match (&link.attachment_preparation, j) {
        (None, None) => Ok(()), // Existing text/confirmation and generic core frames.
        (Some(pin), Some(j)) => {
            if envelope.protocol_version != 3
                || j.payload.as_ref() != Some(payload)
                || j.promoted_digest.as_deref() != Some(link.digest.as_slice())
                || pin.as_slice() != control::cohort_pin(&j)
                || j.operation.as_ref().and_then(|r| r.blob.as_ref()) != envelope.blob.as_ref()
            {
                return Err(CryptoError::Conflict);
            }
            Ok(())
        }
        _ => Err(CryptoError::Conflict),
    }
}

pub(super) fn retained_plan(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    message: &str,
    j: &Journal,
) -> CryptoResult<Option<EpochOutgoingPlan>> {
    let linked = tx.get(&outgoing::link_key(own, message)?)?.is_some();
    match (j.promoted_digest.is_some(), linked) {
        (false, false) => Ok(None),
        (true, true) => {
            let link = outgoing::load_link(tx, own, message)?;
            if link.attachment_preparation.is_none() {
                return Err(CryptoError::Conflict);
            }
            let p = outgoing::plan(tx, own, store, &link.id)?;
            outgoing::check_projections(tx, own, &link.id, p.needs_status_poll())?;
            Ok(Some(p))
        }
        _ => Err(CryptoError::Conflict),
    }
}

impl Engine {
    pub(super) fn attachment_outgoing(
        &mut self,
        message: &str,
    ) -> CryptoResult<Option<EpochOutgoingPlan>> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            match load_retained_journal(tx, own, message)? {
                Some(j) => retained_plan(tx, own, &store, message, &j),
                None => {
                    if tx.get(&outgoing::link_key(own, message)?)?.is_some() {
                        return Err(CryptoError::Conflict);
                    }
                    Ok(None)
                }
            }
        })
    }

    pub(super) fn promote_attachment(
        &mut self,
        message: &str,
        expected: Option<&EpochBlobControlPlan>,
    ) -> CryptoResult<EpochAttachmentPromotion> {
        let own = &self.own;
        let store = self.store.scope().clone();
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let j =
                load_retained_journal(tx, own, message)?.ok_or(CryptoError::MessageUnavailable)?;
            if let Some(plan) = expected {
                control::check_plan(plan, &j, own, &store)?;
            }
            // Resolve a past local COMMIT before looking at current send/file
            // availability. Never encrypt the same UUID again after recovery.
            if let Some(p) = retained_plan(tx, own, &store, message, &j)? {
                return Ok(EpochAttachmentPromotion::Queued(Box::new(p)));
            }
            if expected.is_some_and(|p| j.operation.as_ref() != Some(p.request())) {
                return Err(CryptoError::Conflict);
            }
            ready(&j)?;
            let p = j.payload.as_ref().ok_or(CryptoError::Invalid)?;
            let s = current(tx, own, &store, &p.conversation_id)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let plan =
                outgoing::encrypt_prepared_attachment(tx, runtime, &s, p).map_err(|e| match e {
                    CryptoError::Signal(SignalProtocolError::SessionNotFound(_)) => {
                        CryptoError::EpochRecoveryRequired
                    }
                    other => other,
                })?;
            Ok(match plan {
                Some(p) => EpochAttachmentPromotion::Queued(Box::new(p)),
                None => EpochAttachmentPromotion::BootstrapRequired,
            })
        })
    }
}

impl CryptoCoordinator {
    /// Commit one Signal message/history/outbox/cohort. Ready is a saved DB
    /// observation, not permanent server acceptance; Send must still settle it.
    pub async fn promote_epoch_attachment(
        &self,
        message: String,
    ) -> CryptoResult<EpochAttachmentPromotion> {
        if !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Promote(
                message, None, permit, reply,
            )))
        })
        .await
    }

    /// Account-internal Ready handoff. Bind both the immutable preparation and
    /// its exact current operation before creating a NEW Signal outbox. An old
    /// committed outbox still wins over changed lease availability.
    pub(in crate::encryption) async fn promote_epoch_attachment_if(
        &self,
        plan: EpochBlobControlPlan,
    ) -> CryptoResult<EpochAttachmentPromotion> {
        let message = plan.request().message_id.clone();
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Promote(
                message,
                Some(Box::new(plan)),
                permit,
                reply,
            )))
        })
        .await
    }

    /// Metadata-only exact outbox recovery, including frozen/sealed scopes.
    /// Does not take the file-buffer permit, bootstrap, upload, repair or renew.
    pub async fn epoch_attachment_outgoing(
        &self,
        message: String,
    ) -> CryptoResult<Option<EpochOutgoingPlan>> {
        if !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Outgoing(
                message, reply,
            )))
        })
        .await
    }
}
