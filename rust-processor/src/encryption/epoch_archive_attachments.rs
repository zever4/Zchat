//! Read-only incoming cache / outgoing spool in known local generations. No
//! network plan/ACK proof is returned; old workers/ratchets are never resumed.
use super::*;

impl Engine {
    pub(in crate::encryption::signal_state) fn open_archive_attachment(
        &mut self,
        generation: &str,
        conversation: &str,
        message: &str,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> CryptoResult<EpochAttachmentFile> {
        if !epoch_rpc::uuid4(generation)
            || !epoch_rpc::uuid4(conversation)
            || !epoch_rpc::uuid4(message)
        {
            return Err(CryptoError::Invalid);
        }
        let active = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let own = views::archive_own(tx, active, generation)?;
            if outgoing::load_link(tx, &own, message)?.outgoing == Some(true) {
                let (bytes, kind) =
                    attachment_prepare::archived::open(tx, &own, &store, conversation, message)?;
                return Ok(EpochAttachmentFile {
                    bytes,
                    kind,
                    _permit: permit,
                });
            }
            let (cohort, payload) = cache_cohort(tx, &own, &store, message, true)?;
            if cohort.binding.conversation_id != conversation {
                return Err(CryptoError::Conflict);
            }
            require_cached(
                &load_bound_cache(tx, &own, &cohort.binding, &cohort.blob, &cohort.payload_pin)?
                    .ok_or(CryptoError::MessageUnavailable)?,
            )?;
            Ok(EpochAttachmentFile {
                bytes: open_bound_cache(tx, &own, &cohort.binding, &cohort.blob, &payload)?,
                kind: attachment_kind(&payload)?,
                _permit: permit,
            })
        })
    }
}
