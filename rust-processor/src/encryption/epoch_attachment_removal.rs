//! Explicit retirement of a verified, positively acknowledged local incoming
//! copy. No unknown-data eviction, history/key removal or remote ACK revocation.
use super::*;

pub(super) fn key(own: &DeviceRef, message: &str) -> CryptoResult<RecordKey> {
    if !epoch_rpc::uuid4(message) {
        return Err(CryptoError::Invalid);
    }
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("attachment-removed:{message}"),
    )?)
}

impl Engine {
    pub(super) fn remove_local_attachment(
        &mut self,
        generation: &str,
        conversation: &str,
        message: &str,
    ) -> CryptoResult<()> {
        let active = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let own = views::archive_own(tx, active, generation)?;
            if outgoing::load_link(tx, &own, message)?.outgoing == Some(true) {
                return attachment_prepare::retirement::remove(
                    tx,
                    &own,
                    &store,
                    conversation,
                    message,
                );
            }
            let (cohort, payload) = cache_cohort(tx, &own, &store, message, true)?;
            if cohort.binding.conversation_id != conversation {
                return Err(CryptoError::Conflict);
            }
            let mut j =
                load_bound_cache(tx, &own, &cohort.binding, &cohort.blob, &cohort.payload_pin)?
                    .ok_or(CryptoError::MessageUnavailable)?;
            if j.format == 3 {
                return Ok(());
            } // Valid protected marker + no artifacts.
            if j.verified != Some(true) {
                return Err(CryptoError::MessageUnavailable);
            }
            if j.file_acknowledged_at_unix_ms.is_none_or(|at| at <= 0) {
                return Err(CryptoError::AttachmentAckPending);
            }
            // Intentionally NOT just the verified flag or a caller's UUID. Any
            // corruption aborts the entire transaction without deleting evidence.
            drop(open_bound_cache(
                tx,
                &own,
                &cohort.binding,
                &cohort.blob,
                &payload,
            )?);
            j.format = 3;
            tx.put(&key(&own, message)?, &j.encode_to_vec())?;
            tx.remove_known_download(
                &super::key(&own, message)?,
                &cohort.blob.blob_id,
                cohort.blob.ciphertext_bytes as usize,
            )?;
            Ok(())
        })
    }
}
