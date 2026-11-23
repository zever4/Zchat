//! Expiry is an exact authenticated remote assertion. Local time/generic errors
//! never authorize a new nonce. Renew only unused bootstrap on the account lane.
use super::*;
use crate::encryption::native::EpochExpiredAcquisition;

impl Engine {
    pub(super) fn renew_epoch_acquisition(
        &mut self,
        expired: EpochExpiredAcquisition,
    ) -> CryptoResult<EpochAcquisitionPlan> {
        let expected = expired.request();
        self.check_epoch_owner(&expected.scope)?;
        if expired.store_scope() != self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        epoch_rpc::validate_expired_acquisition(&expected.request, expired.response())
            .map_err(|_| CryptoError::Invalid)?;
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, &expected.scope.own)?;
            let s = current(tx, &expected.scope)?;
            sending(&s)?;
            check_traffic(tx, &s)?;
            let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let pins = pins(&c, s.own_identity)?;
            pins.check_acquire(&expected.request)
                .map_err(|_| CryptoError::Invalid)?;
            let mut j = read(tx, &s, &pins)?.ok_or(CryptoError::Conflict)?;
            let request = j.request.as_ref().ok_or(CryptoError::Invalid)?;
            let mut same = expected.request.clone();
            same.acquisition_id = request.acquisition_id.clone();
            if same != *request {
                return Err(CryptoError::Conflict);
            }
            let exists = session(tx, runtime, &s)?;
            if j.applied.is_some() && !exists {
                return Err(CryptoError::Invalid);
            }
            if exists {
                return Err(CryptoError::SessionExists);
            }
            if j.applied.is_some() || j.has_message_state != Some(false) || has_traffic(tx, &s)? {
                return Err(CryptoError::Conflict);
            }
            if request != &expected.request {
                if j.replaces_request.as_ref() != Some(&expected.request) {
                    return Err(CryptoError::Conflict);
                }
                return plan(s, pins, &j); // SAME committed successor, no mutation.
            }
            let mut request = request.clone();
            request.acquisition_id = uuid::Uuid::new_v4().to_string();
            j.replaces_request = Some(expected.request.clone());
            j.request = Some(request);
            save(tx, &s, &j)?;
            plan(s, pins, &j) // Escapes only after the transaction COMMIT succeeds.
        })
    }
}
impl CryptoCoordinator {
    /// Persist a new unused nonce exactly once after authoritative v2 expiry.
    /// Never resets an existing/missing ratchet or any traffic/private-key state.
    pub async fn renew_epoch_acquisition(
        &self,
        expired: EpochExpiredAcquisition,
    ) -> CryptoResult<EpochAcquisitionPlan> {
        if expired.request().request().encoded_len()
            > crate::encryption::contract::MAX_REQUEST_BYTES
            || expired.response().encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES
        {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Bootstrap(BootstrapCommand::Renew(
                Box::new(expired),
                r,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_renewal_tests.rs"]
mod tests;
