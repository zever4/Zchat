//! Explicit native text submission. UUID, history, ratchet, ciphertext and its
//! indexes commit together. No network, renderer-provided scope or timestamp.
use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EpochTextQueued {
    pub message_id: String,
    pub conversation_id: String,
    pub crypto_epoch: u64,
    pub created_at_unix_ms: i64,
    pub stored_on_server: bool,
    pub sealed_unsent: bool,
}
pub enum EpochTextPreparation {
    Queued(EpochTextQueued),
    // No local acceptance yet. Only an explicit Send may arrange bootstrap.
    BootstrapRequired,
}
fn queued(p: &EpochOutgoingPlan, created: i64) -> CryptoResult<EpochTextPreparation> {
    let e = p.request().envelope.as_ref().ok_or(CryptoError::Invalid)?;
    Ok(EpochTextPreparation::Queued(EpochTextQueued {
        message_id: e.message_id.clone(),
        conversation_id: e.conversation_id.clone(),
        crypto_epoch: e.crypto_epoch.ok_or(CryptoError::Invalid)?,
        created_at_unix_ms: created,
        stored_on_server: p.receipt().is_some(),
        sealed_unsent: p.sealed_failure().is_some(),
    }))
}
impl Engine {
    pub(super) fn prepare_epoch_text(
        &mut self,
        chat: &str,
        uuid: &str,
        text: &str,
    ) -> CryptoResult<EpochTextPreparation> {
        if !epoch_rpc::uuid4(chat)
            || !epoch_rpc::uuid4(uuid)
            || text.is_empty()
            || text.len() > crate::encryption::contract::MAX_TEXT_BYTES
        {
            return Err(CryptoError::Invalid);
        }
        let own = &self.own;
        let store = self.store.scope().clone();
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            if attachment_prepare::reserved(tx, own, uuid)? {
                return Err(CryptoError::Conflict);
            }
            if let Some((p, created)) = outgoing::existing_text(tx, own, &store, chat, uuid, text)?
            {
                return queued(&p, created);
            }
            let c = conversation_lifecycle::load(tx, own, chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            if c.state == ConversationState::Closed as i32 {
                return Err(CryptoError::ConversationClosed);
            }
            let (public, registration) = identity(tx, own)?;
            let Some(loaded) = load(tx, own, &c, public, registration)? else {
                return Ok(EpochTextPreparation::BootstrapRequired);
            };
            let s = scope(&loaded, &c, store)?;
            bootstrap::sending(&s)?;
            bootstrap::check_traffic(tx, &s)?;
            if !bootstrap::session(tx, runtime, &s)? {
                // Existing traffic/applied acquisition is checked again by the
                // bootstrap worker; absence never permits a silent session reset.
                return Ok(EpochTextPreparation::BootstrapRequired);
            }
            let created = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| CryptoError::Invalid)?
                .as_millis()
                .try_into()
                .map_err(|_| CryptoError::Invalid)?;
            let mut payload = v2::EncryptedPayload {
                format_version: 2,
                message_id: uuid.into(),
                conversation_id: chat.into(),
                sender: Some(s.own.clone()),
                recipient: Some(s.peer.clone()),
                created_at_unix_ms: created,
                content: Some(v2::encrypted_payload::Content::Text(
                    crate::proto::e2ee::TextMessage { text: text.into() },
                )),
                crypto_epoch: Some(s.epoch),
            };
            let result =
                outgoing::encrypt_message(tx, runtime, &s, &payload).map_err(|e| match e {
                    // Pinned Signal can expire an unconfirmed session after 30d.
                    // This is an explicit recovery condition, never permission to
                    // reset/re-bootstrap it or report successful local acceptance.
                    CryptoError::Signal(SignalProtocolError::SessionNotFound(_)) => {
                        CryptoError::EpochRecoveryRequired
                    }
                    other => other,
                });
            if let Some(v2::encrypted_payload::Content::Text(t)) = payload.content.take() {
                drop(Zeroizing::new(t.text));
            }
            queued(&result?, created)
        })
    }
}
impl CryptoCoordinator {
    pub async fn prepare_epoch_text(
        &self,
        chat: String,
        message: String,
        text: Zeroizing<String>,
    ) -> CryptoResult<EpochTextPreparation> {
        self.request(|r| Command::Epoch(EpochCommand::Text(Box::new((chat, message, text)), r)))
            .await
    }
}

#[cfg(test)]
#[path = "epoch_text_tests.rs"]
mod tests;
