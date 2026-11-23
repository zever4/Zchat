//! Explicit trusted-native rejection. Never called as an error fallback. The
//! authenticated server delivery is retained without decrypting/changing Signal.
use super::*;

/// A trusted owner's explicit local reason, not a classification of arbitrary
/// transport/storage/Busy errors or proof that ciphertext is unrecoverable.
/// No renderer deserializer, prototype alias or automatic error conversion.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum EpochQuarantineReason {
    InvalidCiphertext = 1,
    PayloadBinding = 2,
    UntrustedIdentity = 3,
}
impl EpochQuarantineReason {
    pub(super) fn parse(value: i32) -> CryptoResult<Self> {
        match value {
            1 => Ok(Self::InvalidCiphertext),
            2 => Ok(Self::PayloadBinding),
            3 => Ok(Self::UntrustedIdentity),
            _ => Err(CryptoError::Invalid),
        }
    }
}
/// Read-only protected ciphertext/reason snapshot, never permission to ACK or
/// decrypt again. Rejection/disposition is immutable for this message UUID.
pub struct EpochQuarantinedMessage {
    delivery: v2::Delivery,
    reason: EpochQuarantineReason,
}
impl EpochQuarantinedMessage {
    pub fn delivery(&self) -> &v2::Delivery {
        &self.delivery
    }
    pub fn reason(&self) -> EpochQuarantineReason {
        self.reason
    }
}
impl Engine {
    pub(super) fn quarantine_epoch(
        &mut self,
        s: EpochScope,
        observed: EpochDelivery,
        reason: EpochQuarantineReason,
    ) -> CryptoResult<EpochDurableAck> {
        self.check_epoch_owner(&s)?;
        check_observation(&s, &observed)?;
        let d = observed.response();
        let e = d.envelope.as_ref().ok_or(CryptoError::Invalid)?;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, &s.own)?;
            if attachment_prepare::reserved(tx, &s.own, &e.message_id)? {
                return Err(CryptoError::Conflict);
            }
            let s = bootstrap::current(tx, &s)?;
            let id = record_id(&s, &e.message_id)?;
            let inbox = record_key(&s.own, RecordKind::Inbox, &id)?;
            if tx.get(&inbox)?.is_some() {
                let (state, proof, j) = load_message(tx, &s.own, &s.scope, &id)?;
                if !matches!(state, IncomingState::Quarantined(old) if old == reason)
                    || j.delivery.as_ref() != Some(d)
                {
                    return Err(CryptoError::Conflict);
                }
                check_projections(tx, &proof)?;
                return Ok(proof);
            }
            for k in [
                record_key(&s.own, RecordKind::History, &id)?,
                record_key(&s.own, RecordKind::Outbox, &id)?,
                link_key(&s.own, &e.message_id)?,
                journal_key(&s.own, &id)?,
                marker_key(&s.own, &id)?,
            ] {
                if tx.get(&k)?.is_some() {
                    return Err(CryptoError::Conflict);
                }
            }
            confirmation::check_absent(tx, &s, &e.message_id)?;
            bootstrap::check_traffic(tx, &s)?;
            bootstrap::observe_quarantine(tx, &s)?;
            tx.put(&inbox, &e.encode_to_vec())?;
            save_link(tx, &s.own, &id, e, false)?;
            let journal = journal_key(&s.own, &id)?;
            tx.put(
                &journal,
                &Journal {
                    format: FORMAT,
                    delivery: Some(d.clone()),
                    acknowledged: Some(false),
                    confirmation_required: Some(false),
                    quarantine_reason: Some(reason as i32),
                }
                .encode_to_vec(),
            )?;
            tx.project_recovery(&inbox, false)?;
            tx.project_recovery(&journal, true)?;
            let (_, proof, _) = load_message(tx, &s.own, &s.scope, &id)?;
            check_projections(tx, &proof)?;
            Ok(proof) // Escapes the worker only after the outer COMMIT succeeds.
        })
    }
    pub(super) fn epoch_quarantined_message(
        &mut self,
        uuid: &str,
    ) -> CryptoResult<EpochQuarantinedMessage> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let link = load_link(tx, own, uuid)?;
            let (state, proof, j) = load_message(tx, own, &store, &link.id)?;
            let IncomingState::Quarantined(reason) = state else {
                return Err(CryptoError::Conflict);
            };
            check_projections(tx, &proof)?;
            Ok(EpochQuarantinedMessage {
                delivery: j.delivery.ok_or(CryptoError::Invalid)?,
                reason,
            })
        })
    }
}
impl CryptoCoordinator {
    /// Explicit trusted-owner action, not an automatic decrypt-error handler.
    /// Full ciphertext/metadata and Rejected proof commit together; never Stored.
    pub async fn quarantine_epoch(
        &self,
        s: EpochScope,
        d: EpochDelivery,
        reason: EpochQuarantineReason,
    ) -> CryptoResult<EpochDurableAck> {
        if d.response().encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Incoming(IncomingCommand::Quarantine(
                Box::new((s, d, reason)),
                r,
            )))
        })
        .await
    }
    pub async fn epoch_quarantined_message(
        &self,
        uuid: String,
    ) -> CryptoResult<EpochQuarantinedMessage> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Incoming(IncomingCommand::Quarantined(
                uuid, r,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_quarantine_tests.rs"]
mod tests;
