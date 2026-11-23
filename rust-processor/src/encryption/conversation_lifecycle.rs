//! The authoritative protected conversation index, shared by Create/Get/incoming.
//! A delayed active response cannot reopen a closed dialogue. Scopes are minted
//! from committed index state, never from renderer-supplied protobuf values.
use super::*;
use crate::encryption::protected::StoreScope;
use crate::proto::e2ee::GetConversationRequest;

#[derive(Clone)]
pub struct ConversationLookup {
    own: DeviceRef,
    request: GetConversationRequest,
    store_scope: StoreScope,
}
impl ConversationLookup {
    pub fn request(&self) -> &GetConversationRequest {
        &self.request
    }
    pub(in crate::encryption) fn own(&self) -> &DeviceRef {
        &self.own
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.store_scope
    }
}
pub struct VerifiedConversationLookup {
    attempt: ConversationLookup,
    response: Conversation,
}
impl VerifiedConversationLookup {
    pub(in crate::encryption) fn from_rpc(
        attempt: ConversationLookup,
        response: Conversation,
    ) -> Self {
        Self { attempt, response }
    }
}

pub(super) fn key(own: &DeviceRef, id: &str) -> Result<RecordKey, StoreError> {
    RecordKey::new(id, RecordKind::Control, "uuid")?;
    RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("conversation:{id}"),
    )
}
pub(super) fn validate(value: &Conversation, own: &DeviceRef) -> CryptoResult<()> {
    validate_conversation(value).map_err(|_| CryptoError::Invalid)?;
    if value.own_device.as_ref() != Some(own) {
        return Err(CryptoError::Invalid);
    }
    IdentityKey::try_from(
        value
            .peer_device
            .as_ref()
            .ok_or(CryptoError::Invalid)?
            .identity_key
            .as_slice(),
    )?;
    Ok(())
}
pub(super) fn load(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<Option<Conversation>> {
    let Some(bytes) = tx.get(&key(own, id)?)? else {
        return Ok(None);
    };
    if bytes.len() > 2048 {
        return Err(CryptoError::Invalid);
    }
    let value = Conversation::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    validate(&value, own)?;
    if value.conversation_id != id {
        return Err(CryptoError::Conflict);
    }
    Ok(Some(value))
}
pub(super) fn merge(old: &Conversation, new: &Conversation) -> CryptoResult<Conversation> {
    let a = old.peer_device.as_ref().ok_or(CryptoError::Invalid)?;
    let b = new.peer_device.as_ref().ok_or(CryptoError::Invalid)?;
    if old.conversation_id != new.conversation_id
        || old.mode != new.mode
        || old.own_device != new.own_device
        || a.reference != b.reference
        || a.identity_key != b.identity_key
        || a.registration_id != b.registration_id
    {
        return Err(CryptoError::Conflict);
    }
    if old.state == ConversationState::Closed as i32 {
        if new.state == ConversationState::Closed as i32
            && (old.close_reason != new.close_reason
                || old.closed_generation_id != new.closed_generation_id
                || old.closed_at_unix_ms != new.closed_at_unix_ms)
        {
            return Err(CryptoError::Conflict);
        }
        return Ok(old.clone());
    }
    if new.state == ConversationState::Active as i32
        && b.public_material_revision < a.public_material_revision
    {
        return Ok(old.clone());
    }
    Ok(new.clone())
}
pub(super) fn save(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    value: &Conversation,
) -> CryptoResult<Conversation> {
    validate(value, own)?;
    // A first Get index must also respect an identity already pinned by Signal
    // in this generation, including a pin established in another conversation.
    let binding = SessionScope::build(value, None)?;
    if let Some(bytes) = tx.get(&binding.peer_key(RecordKind::PeerIdentity, false)?)?
        && IdentityKey::try_from(bytes.as_slice())? != binding.peer_identity
    {
        return Err(CryptoError::Conflict);
    }
    let old = load(tx, own, &value.conversation_id)?;
    let result = match &old {
        Some(old) => merge(old, value)?,
        None => value.clone(),
    };
    if old.as_ref() != Some(&result) {
        tx.put(&key(own, &result.conversation_id)?, &result.encode_to_vec())?;
    }
    Ok(result)
}
fn binding(scope: &SessionScope, value: &Conversation) -> CryptoResult<()> {
    let peer = value.peer_device.as_ref().ok_or(CryptoError::Invalid)?;
    if value.own_device.as_ref() != Some(&scope.own)
        || peer.reference.as_ref() != Some(&scope.peer)
        || peer.identity_key != scope.peer_identity.serialize().as_ref()
        || peer.registration_id != scope.peer_registration
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
impl SessionScope {
    fn indexed(&self, tx: &mut ProtectedTransaction<'_>) -> CryptoResult<Option<Conversation>> {
        let value = load(tx, &self.own, &self.conversation)?;
        if let Some(value) = &value {
            binding(self, value)?;
        }
        if value.is_none() {
            // Historical raw scopes exist only in crypto-unit fixtures. Normal
            // builds have no public/raw scope constructor and require the index.
            #[cfg(test)]
            if self.store_scope.is_none() {
                return Ok(None);
            }
            return Err(CryptoError::ConversationUnavailable);
        }
        Ok(value)
    }
    pub(super) fn check_sending(&self, tx: &mut ProtectedTransaction<'_>) -> CryptoResult<()> {
        super::epoch_state::reject_prototype_scope(tx, &self.own, &self.conversation)?;
        if self.receive_only {
            return Err(CryptoError::ConversationClosed);
        }
        if let Some(value) = self.indexed(tx)? {
            if value.closed_generation_id == self.own.generation_id {
                return Err(CryptoError::ArchivedGeneration);
            }
            if value.state == ConversationState::Closed as i32 {
                return Err(CryptoError::ConversationClosed);
            }
        }
        Ok(())
    }
    pub(super) fn check_receiving(&self, tx: &mut ProtectedTransaction<'_>) -> CryptoResult<()> {
        super::epoch_state::reject_prototype_scope(tx, &self.own, &self.conversation)?;
        if let Some(value) = self.indexed(tx)?
            && value.closed_generation_id == self.own.generation_id
        {
            return Err(CryptoError::ArchivedGeneration);
        }
        Ok(())
    }
}
impl Engine {
    pub(super) fn prepare_lookup(&mut self, id: &str) -> CryptoResult<ConversationLookup> {
        key(&self.own, id)?;
        Ok(ConversationLookup {
            own: self.own.clone(),
            store_scope: self.store.scope().clone(),
            request: GetConversationRequest {
                context: Some(ClientContext {
                    protocol_version: 1,
                    device_generation_id: self.own.generation_id.clone(),
                }),
                conversation_id: id.into(),
            },
        })
    }
    pub(super) fn accept_lookup(
        &mut self,
        verified: VerifiedConversationLookup,
    ) -> CryptoResult<Conversation> {
        let VerifiedConversationLookup { attempt, response } = verified;
        if attempt.own != self.own
            || &attempt.store_scope != self.store.scope()
            || attempt.request.context.as_ref()
                != Some(&ClientContext {
                    protocol_version: 1,
                    device_generation_id: self.own.generation_id.clone(),
                })
            || attempt.request.conversation_id != response.conversation_id
        {
            return Err(CryptoError::Conflict);
        }
        let own = &self.own;
        self.store.transaction_with(|tx| save(tx, own, &response))
    }
    pub(super) fn conversation_scope(&mut self, id: &str) -> CryptoResult<SessionScope> {
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let value = load(tx, own, id)?.ok_or(CryptoError::ConversationUnavailable)?;
            SessionScope::from_index(&value, store_scope)
        })
    }
}
impl CryptoCoordinator {
    /// Get is read-only on the server, so no new operation UUID/journal is needed.
    pub async fn prepare_conversation_lookup(
        &self,
        id: String,
    ) -> CryptoResult<ConversationLookup> {
        self.request(|r| Command::PrepareLookup(id, r)).await
    }
    pub async fn accept_conversation_lookup(
        &self,
        proof: VerifiedConversationLookup,
    ) -> CryptoResult<Conversation> {
        self.request(|r| Command::AcceptLookup(proof, r)).await
    }
    /// Only committed, authenticated index state yields a usable opaque scope.
    pub async fn conversation_scope(&self, id: String) -> CryptoResult<SessionScope> {
        self.request(|r| Command::ConversationScope(id, r)).await
    }
}
