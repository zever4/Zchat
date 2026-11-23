//! Read-only bounded scheduling over the existing protected conversation index.
//! Empty pages never authorize reset, activation, deletion or unfreezing.
use super::*;
#[derive(Clone)]
pub struct EpochConversationCursor {
    scope: StoreScope,
    own: DeviceRef,
    after: String,
}
pub struct EpochConversationPage {
    pub scopes: Vec<EpochScope>,
    pub next: Option<EpochConversationCursor>,
}
pub(super) fn cursor(s: &EpochScope) -> EpochConversationCursor {
    EpochConversationCursor {
        scope: s.scope.clone(),
        own: s.own.clone(),
        after: format!("conversation:{}", s.conversation),
    }
}
impl Engine {
    pub(super) fn active_epoch_scope(&mut self, chat: &str) -> CryptoResult<Option<EpochScope>> {
        prefix(chat)?;
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let c = conversation_lifecycle::load(tx, own, chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let (public, registration) = identity(tx, own)?;
            load(tx, own, &c, public, registration)?
                .map(|l| scope(&l, &c, store))
                .transpose()
        })
    }
    pub(super) fn epoch_conversation_page(
        &mut self,
        c: Option<EpochConversationCursor>,
    ) -> CryptoResult<EpochConversationPage> {
        let own = &self.own;
        let store = self.store.scope().clone();
        if c.as_ref()
            .is_some_and(|c| c.scope != store || c.own != *own)
        {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            let rows = tx.record_page(
                &own.generation_id,
                RecordKind::Control,
                "conversation:",
                c.as_ref().map(|c| c.after.as_str()),
            )?;
            let next = if rows.len() == 32 {
                Some(EpochConversationCursor {
                    scope: store.clone(),
                    own: own.clone(),
                    after: rows.last().unwrap().0.clone(),
                })
            } else {
                None
            };
            let (public, registration) = identity(tx, own)?;
            let mut scopes = Vec::new();
            for (id, bytes) in rows {
                let chat = id
                    .strip_prefix("conversation:")
                    .ok_or(CryptoError::Invalid)?;
                prefix(chat)?;
                let c = Conversation::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
                conversation_lifecycle::validate(&c, own)?;
                if c.conversation_id != chat {
                    return Err(CryptoError::Conflict);
                }
                if let Some(l) = load(tx, own, &c, public, registration)? {
                    scopes.push(scope(&l, &c, store.clone())?);
                }
            }
            Ok(EpochConversationPage { scopes, next })
        })
    }
}
impl CryptoCoordinator {
    pub async fn active_epoch_scope(&self, chat: String) -> CryptoResult<Option<EpochScope>> {
        self.request(|r| Command::Epoch(EpochCommand::Active(chat, r)))
            .await
    }
    pub async fn epoch_conversation_page(
        &self,
        c: Option<EpochConversationCursor>,
    ) -> CryptoResult<EpochConversationPage> {
        self.request(|r| Command::Epoch(EpochCommand::Conversations(c, r)))
            .await
    }
}
