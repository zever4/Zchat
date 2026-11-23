//! A sealed fence resolves one EXACT unknown Send. It is never a receipt, peer
//! ACK, permission to reencrypt, or a substitute for persisted signed acceptance.
use super::*;
fn anchor_key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    parse_id(id)?;
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("delivery-sealed:{id}"),
    )?)
}
pub(super) fn validate(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    pins: &EpochConversation,
    id: &str,
    r: &v2::SendMessageRequest,
    j: &Journal,
) -> CryptoResult<()> {
    let marker = tx.get(&anchor_key(&s.own, id)?)?;
    let Some(f) = &j.sealed else {
        if marker.is_some() {
            return Err(CryptoError::Invalid);
        }
        return Ok(());
    };
    if j.receipt.is_some() || j.status.is_some() || j.removal.is_some() || j.revision.is_some() {
        return Err(CryptoError::Conflict);
    }
    pins.verify_sealed(r, f).map_err(|_| CryptoError::Invalid)?;
    recovery::require_accepted_outcome(
        tx,
        &s.own,
        pins,
        f.recovery.as_ref().ok_or(CryptoError::Invalid)?,
    )?;
    if marker.as_ref().map(|b| b.as_slice())
        != Some(digest(b"zchat.e2ee.outgoing-sealed.v2\0", &j.encode_to_vec()).as_slice())
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
impl Engine {
    pub(super) fn accept_epoch_send_fence(
        &mut self,
        o: EpochObservation<EpochOutgoingPlan, v2::EpochSealedFailure>,
    ) -> CryptoResult<()> {
        let p = o.request();
        self.check_epoch_owner(&p.scope)?;
        if o.store_scope() != self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let e = p.request.envelope.as_ref().ok_or(CryptoError::Invalid)?;
            let id = record_id(&p.scope, &e.message_id)?;
            let saved = plan(tx, own, &store, &id)?;
            check_projections(tx, own, &id, saved.needs_status_poll())?;
            if saved.request != p.request || saved.receipt.is_some() {
                return Err(CryptoError::Conflict);
            }
            saved
                .pins
                .verify_sealed(&saved.request, o.response())
                .map_err(|_| CryptoError::Invalid)?;
            recovery::require_accepted_outcome(
                tx,
                own,
                &saved.pins,
                o.response().recovery.as_ref().ok_or(CryptoError::Invalid)?,
            )?;
            if let Some(old) = saved.sealed {
                if old != *o.response() {
                    return Err(CryptoError::Conflict);
                }
                return Ok(());
            }
            let j = Journal {
                format: FORMAT,
                receipt: None,
                status: None,
                removal: None,
                revision: None,
                sequence: saved.sequence,
                chain_digest: saved.chain_digest,
                sealed: Some(o.response().clone()),
            };
            let bytes = j.encode_to_vec();
            tx.put(&journal_key(own, &id)?, &bytes)?;
            tx.put(
                &anchor_key(own, &id)?,
                &digest(b"zchat.e2ee.outgoing-sealed.v2\0", &bytes),
            )?;
            tx.project_recovery(&record_key(own, RecordKind::Outbox, &id)?, false)?;
            Ok(())
        })
    }
}
impl CryptoCoordinator {
    pub async fn accept_epoch_send_fence(
        &self,
        o: EpochObservation<EpochOutgoingPlan, v2::EpochSealedFailure>,
    ) -> CryptoResult<()> {
        if o.response().encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::Fence(
                Box::new(o),
                r,
            )))
        })
        .await
    }
}
