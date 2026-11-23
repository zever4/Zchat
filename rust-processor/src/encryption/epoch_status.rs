//! Monotonic protected v2 delivery facts. Status polling never mutates Signal,
//! deletes ciphertext, settles an unknown Send fence or infers expiry locally.
use super::*;
use crate::encryption::{epoch_rpc, native::EpochStatuses};
use crate::proto::e2ee::MessageState;

#[derive(Message)]
struct Anchor {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(bytes = "vec", tag = "3")]
    digest: Vec<u8>,
}
fn anchor_key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    parse_id(id)?;
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("delivery-progress:{id}"),
    )?)
}
fn is_ack(s: &v2::MessageStatus) -> bool {
    matches!(
        MessageState::try_from(s.state),
        Ok(MessageState::RecipientStored | MessageState::RecipientRejected)
    )
}
fn is_removal(s: &v2::MessageStatus) -> bool {
    matches!(
        MessageState::try_from(s.state),
        Ok(MessageState::Expired | MessageState::RecipientReplaced)
    )
}
pub(super) fn pending(s: Option<&v2::MessageStatus>) -> bool {
    !s.is_some_and(|s| is_ack(s) || s.state == MessageState::RecipientReplaced as i32)
}
fn validate_fact(r: &v2::SendMessageRequest, s: &v2::MessageStatus) -> CryptoResult<()> {
    epoch_rpc::validate_statuses(
        std::slice::from_ref(r),
        &v2::MessageStatuses {
            messages: vec![s.clone()],
        },
    )
    .map_err(|_| CryptoError::Invalid)
}
pub(super) fn validate(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    r: &v2::SendMessageRequest,
    j: &Journal,
) -> CryptoResult<()> {
    let anchor = tx.get(&anchor_key(own, id)?)?;
    let Some(s) = &j.status else {
        if j.revision.is_some() || j.removal.is_some() || anchor.is_some() {
            return Err(CryptoError::Invalid);
        }
        return Ok(());
    };
    validate_fact(r, s)?;
    if j.receipt.is_none() || j.receipt != s.receipt {
        return Err(CryptoError::Conflict);
    }
    match &j.removal {
        Some(m) => {
            validate_fact(r, m)?;
            if !is_removal(m) || m.receipt != j.receipt || (!is_ack(s) && s != m) {
                return Err(CryptoError::Conflict);
            }
        }
        None if is_removal(s) => return Err(CryptoError::Invalid),
        None => (),
    }
    let revision = j
        .revision
        .filter(|v| *v > 0 && *v <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    let bytes = anchor.ok_or(CryptoError::Invalid)?;
    let a = Anchor::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if a.format != FORMAT
        || a.revision != revision
        || a.digest != digest(b"zchat.e2ee.outgoing-status.v2\0", &j.encode_to_vec())
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
fn save(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    j: &mut Journal,
) -> CryptoResult<()> {
    let revision = j
        .revision
        .unwrap_or(0)
        .checked_add(1)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    j.revision = Some(revision);
    let bytes = j.encode_to_vec();
    tx.put(&journal_key(own, id)?, &bytes)?;
    tx.put(
        &anchor_key(own, id)?,
        &Anchor {
            format: FORMAT,
            revision,
            digest: digest(b"zchat.e2ee.outgoing-status.v2\0", &bytes).to_vec(),
        }
        .encode_to_vec(),
    )?;
    tx.project_recovery(
        &record_key(own, RecordKind::Outbox, id)?,
        pending(j.status.as_ref()),
    )?;
    Ok(())
}
fn merge(j: &mut Journal, new: &v2::MessageStatus) -> CryptoResult<()> {
    let receipt = new.receipt.as_ref().ok_or(CryptoError::Invalid)?;
    if j.receipt.as_ref().is_some_and(|old| old != receipt) {
        return Err(CryptoError::Conflict);
    }
    j.receipt = Some(receipt.clone());
    if is_removal(new) {
        if j.removal.as_ref().is_some_and(|old| old != new) {
            return Err(CryptoError::Conflict);
        }
        j.removal = Some(new.clone());
    }
    if let Some(old) = &j.status {
        if old.state == new.state {
            if old != new {
                return Err(CryptoError::Conflict);
            }
            return Ok(());
        }
        if is_ack(old) {
            if is_ack(new) {
                return Err(CryptoError::Conflict);
            }
            return Ok(()); // ACK wins, but a newly observed removal was retained above.
        }
        if new.state == MessageState::Pending as i32 {
            return Ok(());
        }
        if is_removal(old) && is_removal(new) {
            return Err(CryptoError::Conflict);
        }
    }
    j.status = Some(new.clone());
    Ok(())
}
impl Engine {
    pub(super) fn accept_epoch_statuses(
        &mut self,
        b: EpochStatuses,
    ) -> CryptoResult<Vec<EpochOutgoingPlan>> {
        let plans = b.request();
        let response = b.response();
        if b.store_scope() != self.store.scope()
            || plans.is_empty()
            || plans.len() > epoch_rpc::MAX_PREPARED_STATUS_PLANS
        {
            return Err(CryptoError::Invalid);
        }
        for p in plans {
            self.check_epoch_owner(&p.scope)?;
        }
        let sends: Vec<_> = plans.iter().map(|p| p.request.clone()).collect();
        epoch_rpc::validate_statuses(&sends, response).map_err(|_| CryptoError::Invalid)?;
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let mut result = Vec::with_capacity(plans.len());
            for (p, s) in plans.iter().zip(&response.messages) {
                let e = p.request.envelope.as_ref().ok_or(CryptoError::Invalid)?;
                let id = record_id(&p.scope, &e.message_id)?;
                let saved = plan(tx, own, &store, &id)?;
                check_projections(tx, own, &id, saved.needs_status_poll())?;
                if saved.request != p.request || saved.sealed.is_some() {
                    return Err(CryptoError::Conflict);
                }
                let mut j = Journal {
                    format: FORMAT,
                    receipt: saved.receipt.clone(),
                    status: saved.status.clone(),
                    removal: saved.removal.clone(),
                    revision: saved.revision,
                    sequence: saved.sequence,
                    chain_digest: saved.chain_digest.clone(),
                    sealed: None,
                };
                merge(&mut j, s)?;
                if j.receipt != saved.receipt
                    || j.status != saved.status
                    || j.removal != saved.removal
                {
                    save(tx, own, &id, &mut j)?;
                }
                file_progress::track_receipt(
                    tx,
                    own,
                    &id,
                    &saved.request,
                    j.receipt.as_ref().ok_or(CryptoError::Invalid)?,
                    saved.receipt.is_none(),
                )?;
                result.push(plan(tx, own, &store, &id)?);
            }
            Ok(result)
        })
    }
}
impl CryptoCoordinator {
    /// All matched observations commit as one batch. A stale plan's progress is
    /// never authority: re-read current protected facts before merging each row.
    pub async fn accept_epoch_statuses(
        &self,
        b: EpochStatuses,
    ) -> CryptoResult<Vec<EpochOutgoingPlan>> {
        if b.request().is_empty()
            || b.request().len() > epoch_rpc::MAX_PREPARED_STATUS_PLANS
            || b.response().encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES
        {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::Statuses(
                Box::new(b),
                r,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_status_tests.rs"]
mod tests;
