//! Account-worker journal for creation. The peer slot is a pointer to an exact
//! immutable request; old requests/results survive explicit new-dialogue actions.
use super::*;
use crate::encryption::protected::StoreScope;
use crate::proto::e2ee::CreateConversationRequest;

#[derive(Clone)]
pub struct ConversationCreation {
    own: DeviceRef,
    request: CreateConversationRequest,
    store_scope: StoreScope,
}
impl ConversationCreation {
    pub fn request(&self) -> &CreateConversationRequest {
        &self.request
    }
    pub(in crate::encryption) fn own(&self) -> &DeviceRef {
        &self.own
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.store_scope
    }
}

/// Cannot be constructed from renderer/protobuf claims. The RPC adapter supplies
/// the exact attempt it sent and a strictly validated authenticated response.
pub struct VerifiedConversationCreation {
    attempt: ConversationCreation,
    response: Conversation,
}
impl VerifiedConversationCreation {
    pub(in crate::encryption) fn from_rpc(
        attempt: ConversationCreation,
        response: Conversation,
    ) -> Self {
        Self { attempt, response }
    }
}

#[derive(Message)]
struct CreationJournal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    own: Option<DeviceRef>,
    #[prost(message, optional, tag = "3")]
    request: Option<CreateConversationRequest>,
    #[prost(message, optional, tag = "4")]
    response: Option<Conversation>,
    #[prost(string, tag = "5")]
    previous_request_id: String,
}

fn key(own: &DeviceRef, prefix: &str, id: &str) -> Result<RecordKey, StoreError> {
    RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("{prefix}:{id}"),
    )
}
fn request_key(own: &DeviceRef, id: &str) -> Result<RecordKey, StoreError> {
    RecordKey::new(id, RecordKind::Control, "uuid")?;
    key(own, "create", id)
}
fn conversation_key(own: &DeviceRef, id: &str) -> Result<RecordKey, StoreError> {
    conversation_lifecycle::key(own, id)
}

fn head_key(own: &DeviceRef, peer: i64) -> Result<RecordKey, StoreError> {
    key(own, "create-peer", &peer.to_string())
}
fn load_head(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    peer: i64,
) -> CryptoResult<Option<String>> {
    tx.get(&head_key(own, peer)?)?
        .map(|value| {
            let id = String::from_utf8(value.to_vec()).map_err(|_| CryptoError::Invalid)?;
            request_key(own, &id)?;
            Ok(id)
        })
        .transpose()
}
fn validate_response(value: &Conversation, own: &DeviceRef) -> CryptoResult<()> {
    conversation_lifecycle::validate(value, own)
}

fn load_journal(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    peer: i64,
    id: &str,
) -> CryptoResult<CreationJournal> {
    let bytes = tx
        .get(&request_key(own, id)?)?
        .ok_or(CryptoError::Invalid)?;
    if bytes.len() > 4096 {
        return Err(CryptoError::Invalid);
    }
    let mut journal =
        CreationJournal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let request = journal.request.as_ref().ok_or(CryptoError::Invalid)?;
    if journal.format != 1
        || journal.own.as_ref() != Some(own)
        || request.creation_request_id != id
        || request.peer_user_id != peer
        || peer <= 0
        || request.context.as_ref()
            != Some(&ClientContext {
                protocol_version: 1,
                device_generation_id: own.generation_id.clone(),
            })
    {
        return Err(CryptoError::Conflict);
    }
    if !journal.previous_request_id.is_empty() {
        request_key(own, &journal.previous_request_id)?;
        if journal.previous_request_id == id {
            return Err(CryptoError::Conflict);
        }
    }
    if let Some(result) = &journal.response {
        validate_response(result, own)?;
        let index = conversation_lifecycle::load(tx, own, &result.conversation_id)?
            .ok_or(CryptoError::Invalid)?;
        journal.response = Some(conversation_lifecycle::merge(&index, result)?);
    }
    Ok(journal)
}
fn attempt(
    journal: &CreationJournal,
    store_scope: &StoreScope,
) -> CryptoResult<ConversationCreation> {
    Ok(ConversationCreation {
        own: journal.own.clone().ok_or(CryptoError::Invalid)?,
        request: journal.request.clone().ok_or(CryptoError::Invalid)?,
        store_scope: store_scope.clone(),
    })
}

impl Engine {
    /// Resolve a UI-selected CLOSED chat to its protected creation journal. The
    /// renderer never supplies a request UUID. Only the head or its immediate
    /// predecessor may match: stale consent cannot advance a later peer slot.
    pub(super) fn prepare_conversation_after_closed(
        &mut self,
        peer: i64,
        chat: &str,
    ) -> CryptoResult<ConversationCreation> {
        if peer <= 0 {
            return Err(CryptoError::Invalid);
        }
        conversation_key(&self.own, chat)?;
        let own = &self.own;
        let previous = self.store.transaction_with(|tx| {
            let head = load_head(tx, own, peer)?.ok_or(CryptoError::Conflict)?;
            let journal = load_journal(tx, own, peer, &head)?;
            let matches = |j: &CreationJournal| {
                j.response.as_ref().is_some_and(|r| {
                    r.conversation_id == chat && r.state == ConversationState::Closed as i32
                })
            };
            // Prefer an existing successor even if its reply also names the
            // selected closed chat. Never advance again on an ambiguous retry.
            if !journal.previous_request_id.is_empty() {
                let previous = load_journal(tx, own, peer, &journal.previous_request_id)?;
                if matches(&previous) {
                    return Ok(journal.previous_request_id);
                }
            }
            if matches(&journal) {
                return Ok(head);
            }
            Err(CryptoError::Conflict)
        })?;
        // Both transactions run synchronously within ONE serial worker command;
        // no RPC/other command interleaves. The writer below rechecks the head,
        // closed result/index and predecessor CAS before advancing or replaying.
        self.prepare_conversation(peer, Some(&previous))
    }

    pub(super) fn prepare_conversation(
        &mut self,
        peer: i64,
        previous: Option<&str>,
    ) -> CryptoResult<ConversationCreation> {
        if peer <= 0 {
            return Err(CryptoError::Invalid);
        }
        if let Some(id) = previous {
            request_key(&self.own, id)?;
        }
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            if let Some(id) = load_head(tx, own, peer)? {
                let journal = load_journal(tx, own, peer, &id)?;
                match previous {
                    None => return attempt(&journal, &store_scope),
                    Some(previous) if journal.previous_request_id == previous => {
                        return attempt(&journal, &store_scope);
                    }
                    Some(previous) if previous == id => {
                        let result = journal.response.as_ref().ok_or(CryptoError::Conflict)?;
                        if result.state != ConversationState::Closed as i32 {
                            return Err(CryptoError::Conflict);
                        }
                        let index = tx
                            .get(&conversation_key(own, &result.conversation_id)?)?
                            .ok_or(CryptoError::Invalid)?;
                        let indexed = Conversation::decode(index.as_slice())
                            .map_err(|_| CryptoError::Invalid)?;
                        if indexed != *result {
                            return Err(CryptoError::Conflict);
                        }
                    }
                    Some(_) => return Err(CryptoError::Conflict),
                }
            } else if previous.is_some() {
                return Err(CryptoError::Conflict);
            }
            let id = uuid::Uuid::new_v4().to_string();
            let journal = CreationJournal {
                format: 1,
                own: Some(own.clone()),
                request: Some(CreateConversationRequest {
                    context: Some(ClientContext {
                        protocol_version: 1,
                        device_generation_id: own.generation_id.clone(),
                    }),
                    creation_request_id: id.clone(),
                    peer_user_id: peer,
                }),
                response: None,
                previous_request_id: previous.unwrap_or_default().into(),
            };
            tx.put(&request_key(own, &id)?, &journal.encode_to_vec())?;
            tx.put(&head_key(own, peer)?, id.as_bytes())?;
            attempt(&journal, &store_scope)
        })
    }

    pub(super) fn accept_conversation(
        &mut self,
        verified: VerifiedConversationCreation,
    ) -> CryptoResult<Conversation> {
        let VerifiedConversationCreation { attempt, response } = verified;
        if attempt.own != self.own || &attempt.store_scope != self.store.scope() {
            return Err(CryptoError::Invalid);
        }
        validate_response(&response, &self.own)?;
        let own = &self.own;
        let req = &attempt.request;
        self.store.transaction_with(|tx| {
            if load_head(tx, own, req.peer_user_id)?.as_deref() != Some(&req.creation_request_id) {
                return Err(CryptoError::Conflict);
            }
            let mut journal = load_journal(tx, own, req.peer_user_id, &req.creation_request_id)?;
            if journal.request.as_ref() != Some(req) {
                return Err(CryptoError::Conflict);
            }
            let mut result = match &journal.response {
                Some(old) => conversation_lifecycle::merge(old, &response)?,
                None => response,
            };
            result = conversation_lifecycle::save(tx, own, &result)?;
            journal.response = Some(result.clone());
            tx.put(
                &request_key(own, &req.creation_request_id)?,
                &journal.encode_to_vec(),
            )?;
            Ok(result)
        })
    }
}

#[cfg(test)]
#[path = "conversation_creation_tests.rs"]
mod tests;
