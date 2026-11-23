//! Durable delivery metadata owned by the same account worker as Signal state.
//! Opaque plans/proofs cross the trusted native boundary; no raw renderer ACK.
use super::super::{
    contract::{
        envelope_digest, validate_acknowledgements, validate_pending, validate_send,
        validate_send_receipt, validate_statuses,
    },
    protected::StoreScope,
};
use super::*;
use crate::proto::e2ee::{
    AckDisposition, AcknowledgeMessagesRequest, Acknowledgement, Delivery,
    GetMessageStatusesRequest, MessageReference, MessageState, MessageStatus, MessageStatuses,
    PendingMessages, SendMessageRequest, SendReceipt,
};

const OUTGOING: &str = "delivery-out:";
const INCOMING: &str = "delivery-in:";

// Ciphertext remains in Outbox/Inbox. These are encrypted Control values, so
// Master keys/record AAD stay unchanged. Schema3 adds recovery projections only.
// No local/private-state GC.
#[derive(Message)]
struct OutgoingJournal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    receipt: Option<SendReceipt>,
    #[prost(message, optional, tag = "3")]
    status: Option<MessageStatus>,
}
#[derive(Message)]
struct IncomingJournal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    acknowledgement: Option<Acknowledgement>,
    #[prost(int64, tag = "3")]
    sequence: i64,
    #[prost(int64, tag = "4")]
    expires_at_unix_ms: i64,
    #[prost(int32, tag = "5")]
    quarantine_reason: i32,
    #[prost(bool, tag = "6")]
    acknowledged: bool,
    // Additive encrypted journal field, not a SQLite/wire schema change.
    #[prost(bool, tag = "7")]
    confirmation_required: bool,
}

/// A worker snapshot, not permission to encrypt again. Read-only references
/// prevent callers from changing the exact persisted request inside this plan.
#[derive(Clone)]
pub struct OutgoingPlan {
    request: SendMessageRequest,
    store_scope: StoreScope,
    progress: OutgoingProgress,
}
impl OutgoingPlan {
    pub fn request(&self) -> &SendMessageRequest {
        &self.request
    }
    pub fn progress(&self) -> &OutgoingProgress {
        &self.progress
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.store_scope
    }
}
#[derive(Clone, Default)]
pub struct OutgoingProgress {
    receipt: Option<SendReceipt>,
    status: Option<MessageStatus>,
}
impl OutgoingProgress {
    pub fn receipt(&self) -> Option<&SendReceipt> {
        self.receipt.as_ref()
    }
    pub fn status(&self) -> Option<&MessageStatus> {
        self.status.as_ref()
    }
}
pub struct OutboxPage {
    pub messages: Vec<OutgoingPlan>,
    /// Last pending UUID. Resume by key even if earlier items became terminal.
    pub next: Option<String>,
}
pub struct AckPage {
    pub acknowledgements: Vec<DurableAck>,
    pub next: Option<String>,
}

pub struct VerifiedSendReceipt {
    plan: OutgoingPlan,
    receipt: SendReceipt,
}
impl VerifiedSendReceipt {
    pub(in crate::encryption) fn from_rpc(plan: OutgoingPlan, receipt: SendReceipt) -> Self {
        Self { plan, receipt }
    }
}
pub struct VerifiedMessageStatuses {
    plans: Vec<OutgoingPlan>,
    response: MessageStatuses,
}
impl VerifiedMessageStatuses {
    pub(in crate::encryption) fn from_rpc(
        plans: Vec<OutgoingPlan>,
        response: MessageStatuses,
    ) -> Self {
        Self { plans, response }
    }
}

/// Created only after the worker transaction commits a matching inbox/history or
/// explicit quarantine. No serialization/Debug/public constructor or mutable fields.
#[derive(Clone)]
pub struct DurableAck {
    store_scope: StoreScope,
    request: AcknowledgeMessagesRequest,
}
impl DurableAck {
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.store_scope
    }
    pub(in crate::encryption) fn request(&self) -> &AcknowledgeMessagesRequest {
        &self.request
    }
}
pub struct VerifiedAck {
    proof: DurableAck,
}
impl VerifiedAck {
    pub(in crate::encryption) fn from_rpc(proof: DurableAck) -> Self {
        Self { proof }
    }
}

/// Explicit trusted-native decision, never an automatic reaction to a transport,
/// storage, Busy or generic decrypt error. The full ciphertext remains protected.
#[derive(Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum QuarantineReason {
    InvalidCiphertext = 1,
    PayloadBinding = 2,
    UntrustedIdentity = 3,
}

fn message_key(own: &DeviceRef, kind: RecordKind, id: &str) -> CryptoResult<RecordKey> {
    RecordKey::new(id, RecordKind::Control, "uuid")?;
    Ok(RecordKey::new(&own.generation_id, kind, id)?)
}
fn journal_key(own: &DeviceRef, prefix: &str, id: &str) -> CryptoResult<RecordKey> {
    message_key(own, RecordKind::Control, id)?;
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("{prefix}{id}"),
    )?)
}
fn context(own: &DeviceRef) -> ClientContext {
    ClientContext {
        protocol_version: 1,
        device_generation_id: own.generation_id.clone(),
    }
}
fn check_owner(tx: &ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<()> {
    if tx.account_id().to_string() != own.user_id || own.device_id != 1 {
        return Err(CryptoError::Invalid);
    };
    Ok(())
}
fn load_envelope(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    kind: RecordKind,
    id: &str,
) -> CryptoResult<Envelope> {
    let bytes = tx
        .get(&message_key(own, kind, id)?)?
        .ok_or(CryptoError::MessageUnavailable)?;
    let envelope = Envelope::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    validate_envelope(&envelope).map_err(|_| CryptoError::Invalid)?;
    if envelope.message_id != id {
        return Err(CryptoError::Invalid);
    };
    Ok(envelope)
}
fn check_history(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    envelope: &Envelope,
) -> CryptoResult<()> {
    let bytes = tx
        .get(&message_key(
            own,
            RecordKind::History,
            &envelope.message_id,
        )?)?
        .ok_or(CryptoError::Invalid)?;
    let payload = EncryptedPayload::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    validate_payload_binding(&payload, envelope).map_err(|_| CryptoError::Invalid)
}
fn outgoing(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<SendMessageRequest> {
    check_owner(tx, own)?;
    let envelope = load_envelope(tx, own, RecordKind::Outbox, id)?;
    if envelope.sender.as_ref() != Some(own)
        || envelope.recipient.as_ref().is_none_or(|d| d.device_id != 1)
    {
        return Err(CryptoError::Invalid);
    }
    check_history(tx, own, &envelope)?;
    let request = SendMessageRequest {
        context: Some(context(own)),
        envelope: Some(envelope),
    };
    validate_send(&request).map_err(|_| CryptoError::Invalid)?;
    Ok(request)
}
fn status_request(
    requests: impl Iterator<Item = SendMessageRequest>,
    own: &DeviceRef,
) -> CryptoResult<GetMessageStatusesRequest> {
    let mut refs = Vec::new();
    for request in requests {
        let envelope = request.envelope.ok_or(CryptoError::Invalid)?;
        refs.push(MessageReference {
            message_id: envelope.message_id.clone(),
            envelope_digest: envelope_digest(&envelope)
                .map_err(|_| CryptoError::Invalid)?
                .to_vec(),
        })
    }
    Ok(GetMessageStatusesRequest {
        context: Some(context(own)),
        messages: refs,
    })
}
fn load_outgoing(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    request: &SendMessageRequest,
) -> CryptoResult<OutgoingJournal> {
    let id = &request
        .envelope
        .as_ref()
        .ok_or(CryptoError::Invalid)?
        .message_id;
    let Some(bytes) = tx.get(&journal_key(own, OUTGOING, id)?)? else {
        return Ok(OutgoingJournal {
            format: 1,
            ..Default::default()
        });
    };
    let journal = OutgoingJournal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let receipt = journal.receipt.as_ref().ok_or(CryptoError::Invalid)?;
    if journal.format != 1 {
        return Err(CryptoError::Invalid);
    }
    validate_send_receipt(request, receipt).map_err(|_| CryptoError::Invalid)?;
    if let Some(status) = journal.status.as_ref() {
        if status.receipt.as_ref() != Some(receipt) {
            return Err(CryptoError::Invalid);
        }
        validate_statuses(
            &status_request(std::iter::once(request.clone()), own)?,
            &MessageStatuses {
                messages: vec![status.clone()],
            },
        )
        .map_err(|_| CryptoError::Invalid)?;
    }
    Ok(journal)
}
fn plan(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    scope: &StoreScope,
    id: &str,
) -> CryptoResult<OutgoingPlan> {
    let request = outgoing(tx, own, id)?;
    let journal = load_outgoing(tx, own, &request)?;
    if recovery_indexes::ready(tx, own)? {
        tx.check_recovery_projection(
            &message_key(own, RecordKind::Outbox, id)?,
            !terminal(&OutgoingProgress {
                receipt: journal.receipt.clone(),
                status: journal.status.clone(),
            }),
        )?;
    }
    Ok(OutgoingPlan {
        request,
        store_scope: scope.clone(),
        progress: OutgoingProgress {
            receipt: journal.receipt,
            status: journal.status,
        },
    })
}
fn accept_receipt(journal: &mut OutgoingJournal, receipt: &SendReceipt) -> CryptoResult<()> {
    if journal.receipt.as_ref().is_some_and(|old| old != receipt) {
        return Err(CryptoError::Conflict);
    }
    journal.receipt = Some(receipt.clone());
    Ok(())
}
fn is_ack(state: i32) -> bool {
    matches!(
        MessageState::try_from(state),
        Ok(MessageState::RecipientStored | MessageState::RecipientRejected)
    )
}
fn merge_status(old: Option<&MessageStatus>, new: &MessageStatus) -> CryptoResult<MessageStatus> {
    let Some(old) = old else {
        return Ok(new.clone());
    };
    if old.receipt != new.receipt {
        return Err(CryptoError::Conflict);
    }
    if old.state == new.state {
        if old != new {
            return Err(CryptoError::Conflict);
        };
        return Ok(old.clone());
    }
    if is_ack(old.state) {
        if is_ack(new.state) {
            return Err(CryptoError::Conflict);
        };
        return Ok(old.clone());
    }
    if is_ack(new.state) || old.state == MessageState::Pending as i32 {
        return Ok(new.clone());
    }
    if new.state == MessageState::Pending as i32 {
        return Ok(old.clone());
    }
    // Removal reasons are immutable server facts; Expired/Replaced cannot turn
    // into each other. ACK is the only allowed refinement after either one.
    Err(CryptoError::Conflict)
}
fn check_delivery(own: &DeviceRef, delivery: &Delivery) -> CryptoResult<()> {
    validate_pending(
        &PendingMessages {
            messages: vec![delivery.clone()],
        },
        own,
    )
    .map_err(|_| CryptoError::Invalid)
}
fn load_incoming(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<Option<IncomingJournal>> {
    let Some(bytes) = tx.get(&journal_key(own, INCOMING, id)?)? else {
        return Ok(None);
    };
    let saved = IncomingJournal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let ack = saved.acknowledgement.as_ref().ok_or(CryptoError::Invalid)?;
    validate_acknowledgements(&AcknowledgeMessagesRequest {
        context: Some(context(own)),
        messages: vec![ack.clone()],
    })
    .map_err(|_| CryptoError::Invalid)?;
    let envelope = load_envelope(tx, own, RecordKind::Inbox, id)?;
    check_delivery(
        own,
        &Delivery {
            sequence: saved.sequence,
            expires_at_unix_ms: saved.expires_at_unix_ms,
            envelope: Some(envelope.clone()),
            envelope_digest: ack.envelope_digest.clone(),
        },
    )?;
    if saved.format != 1 || ack.message_id != id {
        return Err(CryptoError::Invalid);
    }
    match AckDisposition::try_from(ack.disposition) {
        Ok(AckDisposition::Stored) if saved.quarantine_reason == 0 => {
            check_history(tx, own, &envelope)?;
            if saved.confirmation_required {
                session_confirmation::validate_existing(tx, own, &envelope)?;
            }
        }
        Ok(AckDisposition::Rejected) if (1..=3).contains(&saved.quarantine_reason) => {
            if saved.confirmation_required {
                return Err(CryptoError::Invalid);
            }
            if tx
                .get(&message_key(own, RecordKind::History, id)?)?
                .is_some()
            {
                return Err(CryptoError::Invalid);
            }
        }
        _ => return Err(CryptoError::Invalid),
    }
    if recovery_indexes::ready(tx, own)? {
        tx.check_recovery_projection(&journal_key(own, INCOMING, id)?, !saved.acknowledged)?;
    }
    Ok(Some(saved))
}
fn acknowledgement(
    own: &DeviceRef,
    journal: &IncomingJournal,
) -> CryptoResult<AcknowledgeMessagesRequest> {
    Ok(AcknowledgeMessagesRequest {
        context: Some(context(own)),
        messages: vec![
            journal
                .acknowledgement
                .clone()
                .ok_or(CryptoError::Invalid)?,
        ],
    })
}
fn prepare_incoming(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    delivery: &Delivery,
    reason: Option<QuarantineReason>,
) -> CryptoResult<AcknowledgeMessagesRequest> {
    check_owner(tx, own)?;
    recovery_indexes::require_ready(tx, own)?;
    check_delivery(own, delivery)?;
    let envelope = delivery.envelope.as_ref().ok_or(CryptoError::Invalid)?;
    let id = &envelope.message_id;
    if let Some(old) = load_incoming(tx, own, id)? {
        let ack = old.acknowledgement.as_ref().ok_or(CryptoError::Invalid)?;
        if old.sequence != delivery.sequence
            || old.expires_at_unix_ms != delivery.expires_at_unix_ms
            || ack.envelope_digest != delivery.envelope_digest
            || old.quarantine_reason != reason.map_or(0, |v| v as i32)
        {
            return Err(CryptoError::Conflict);
        }
        return acknowledgement(own, &old);
    }
    if reason.is_some() {
        // Cannot reinterpret an already decrypted message or overwrite a local
        // outbox with the same UUID. No Signal/session/prekey mutation here.
        for kind in [RecordKind::Inbox, RecordKind::Outbox, RecordKind::History] {
            if tx.get(&message_key(own, kind, id)?)?.is_some() {
                return Err(CryptoError::Conflict);
            }
        }
        tx.put(
            &message_key(own, RecordKind::Inbox, id)?,
            &envelope.encode_to_vec(),
        )?;
        tx.project_recovery(&message_key(own, RecordKind::Inbox, id)?, false)?;
        recovery_indexes::observe(tx, own, envelope)?;
    } else {
        if load_envelope(tx, own, RecordKind::Inbox, id)? != *envelope {
            return Err(CryptoError::Conflict);
        }
        check_history(tx, own, envelope)?;
    }
    let saved = IncomingJournal {
        format: 1,
        acknowledgement: Some(Acknowledgement {
            message_id: id.clone(),
            envelope_digest: delivery.envelope_digest.clone(),
            disposition: if reason.is_some() {
                AckDisposition::Rejected as i32
            } else {
                AckDisposition::Stored as i32
            },
        }),
        sequence: delivery.sequence,
        expires_at_unix_ms: delivery.expires_at_unix_ms,
        quarantine_reason: reason.map_or(0, |v| v as i32),
        acknowledged: false,
        confirmation_required: false,
    };
    tx.put(&journal_key(own, INCOMING, id)?, &saved.encode_to_vec())?;
    tx.project_recovery(&journal_key(own, INCOMING, id)?, true)?;
    acknowledgement(own, &saved)
}

pub(super) fn check_outgoing_id(
    tx: &mut ProtectedTransaction<'_>,
    scope: &SessionScope,
    id: &str,
) -> CryptoResult<()> {
    if tx.get(&journal_key(&scope.own, INCOMING, id)?)?.is_some() {
        return Err(CryptoError::Conflict);
    };
    Ok(())
}
pub(super) fn guard_decrypt(
    tx: &mut ProtectedTransaction<'_>,
    scope: &SessionScope,
    envelope: &Envelope,
) -> CryptoResult<()> {
    if let Some(saved) = load_incoming(tx, &scope.own, &envelope.message_id)? {
        if load_envelope(tx, &scope.own, RecordKind::Inbox, &envelope.message_id)? != *envelope {
            return Err(CryptoError::Conflict);
        }
        if saved.quarantine_reason != 0 {
            return Err(CryptoError::Quarantined);
        }
    };
    Ok(())
}

fn terminal(progress: &OutgoingProgress) -> bool {
    progress.status.as_ref().is_some_and(|s| {
        matches!(
            MessageState::try_from(s.state),
            Ok(MessageState::RecipientStored
                | MessageState::RecipientRejected
                | MessageState::RecipientReplaced)
        )
    })
}
pub(super) fn index_outgoing(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<()> {
    let request = outgoing(tx, own, id)?;
    let journal = load_outgoing(tx, own, &request)?;
    let progress = OutgoingProgress {
        receipt: journal.receipt,
        status: journal.status,
    };
    tx.project_recovery(
        &message_key(own, RecordKind::Outbox, id)?,
        !terminal(&progress),
    )?;
    Ok(())
}
pub(super) fn index_incoming(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<()> {
    let saved = load_incoming(tx, own, id)?.ok_or(CryptoError::Invalid)?;
    tx.project_recovery(&journal_key(own, INCOMING, id)?, !saved.acknowledged)?;
    Ok(())
}

impl Engine {
    fn outgoing_plan(&mut self, id: &str) -> CryptoResult<OutgoingPlan> {
        let scope = self.store.scope().clone();
        let own = &self.own;
        self.store.transaction_with(|tx| plan(tx, own, &scope, id))
    }
    fn outbox_page(&mut self, after: Option<&str>) -> CryptoResult<OutboxPage> {
        let own = &self.own;
        let scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            check_owner(tx, own)?;
            if let Some(id) = after {
                message_key(own, RecordKind::Outbox, id)?;
            }
            recovery_indexes::require_ready(tx, own)?;
            let rows =
                tx.prototype_recovery_page(&own.generation_id, RecordKind::Outbox, "", after)?;
            let next = if rows.len() == 32 {
                rows.last().cloned()
            } else {
                None
            };
            let mut messages = Vec::new();
            for id in rows {
                let message = plan(tx, own, &scope, &id)?;
                if terminal(&message.progress) {
                    return Err(CryptoError::Invalid);
                }
                messages.push(message);
            }
            Ok(OutboxPage { messages, next })
        })
    }
    fn accept_send_receipt(
        &mut self,
        proof: VerifiedSendReceipt,
    ) -> CryptoResult<OutgoingProgress> {
        if proof.plan.store_scope != *self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        let own = &self.own;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let id = &proof.receipt.message_id;
            let request = outgoing(tx, own, id)?;
            if request != proof.plan.request {
                return Err(CryptoError::Conflict);
            }
            validate_send_receipt(&request, &proof.receipt).map_err(|_| CryptoError::Invalid)?;
            let mut journal = load_outgoing(tx, own, &request)?;
            accept_receipt(&mut journal, &proof.receipt)?;
            tx.put(&journal_key(own, OUTGOING, id)?, &journal.encode_to_vec())?;
            index_outgoing(tx, own, id)?;
            Ok(OutgoingProgress {
                receipt: journal.receipt,
                status: journal.status,
            })
        })
    }
    fn accept_message_statuses(
        &mut self,
        proof: VerifiedMessageStatuses,
    ) -> CryptoResult<Vec<OutgoingProgress>> {
        let own = &self.own;
        let scope = self.store.scope().clone();
        let request = status_request(proof.plans.iter().map(|p| p.request.clone()), own)?;
        validate_statuses(&request, &proof.response).map_err(|_| CryptoError::Invalid)?;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let mut progress = Vec::new();
            for (saved, value) in proof.plans.iter().zip(&proof.response.messages) {
                if saved.store_scope != scope {
                    return Err(CryptoError::Conflict);
                }
                let receipt = value.receipt.as_ref().ok_or(CryptoError::Invalid)?;
                let request = outgoing(tx, own, &receipt.message_id)?;
                if request != saved.request {
                    return Err(CryptoError::Conflict);
                }
                validate_send_receipt(&request, receipt).map_err(|_| CryptoError::Invalid)?;
                let mut journal = load_outgoing(tx, own, &request)?;
                accept_receipt(&mut journal, receipt)?;
                journal.status = Some(merge_status(journal.status.as_ref(), value)?);
                tx.put(
                    &journal_key(own, OUTGOING, &receipt.message_id)?,
                    &journal.encode_to_vec(),
                )?;
                index_outgoing(tx, own, &receipt.message_id)?;
                progress.push(OutgoingProgress {
                    receipt: journal.receipt,
                    status: journal.status,
                });
            }
            Ok(progress)
        })
    }
    fn prepare_ack(
        &mut self,
        scope: &SessionScope,
        delivery: &Delivery,
        reason: Option<QuarantineReason>,
    ) -> CryptoResult<DurableAck> {
        self.check(scope)?;
        check_delivery(&self.own, delivery)?;
        let envelope = delivery.envelope.as_ref().ok_or(CryptoError::Invalid)?;
        if envelope.conversation_id != scope.conversation
            || envelope.sender.as_ref() != Some(&scope.peer)
        {
            return Err(CryptoError::Invalid);
        }
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        let request = self.store.transaction_with(|tx| {
            scope.check_receiving(tx)?;
            prepare_incoming(tx, own, delivery, reason)
        })?;
        Ok(DurableAck {
            store_scope,
            request,
        })
    }
    fn receive_delivery(
        &mut self,
        scope: &SessionScope,
        delivery: &Delivery,
        confirm_session: bool,
    ) -> CryptoResult<(EncryptedPayload, DurableAck)> {
        self.check(scope)?;
        check_delivery(&self.own, delivery)?;
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        let runtime = &self.runtime;
        let (payload, request) = self.store.transaction_with(|tx| {
            let payload = decrypt_message(
                tx,
                runtime,
                scope,
                delivery.envelope.as_ref().ok_or(CryptoError::Invalid)?,
            )?;
            let request = prepare_incoming(tx, own, delivery, None)?;
            if confirm_session {
                let required = session_confirmation::prepare(
                    tx,
                    runtime,
                    scope,
                    delivery.envelope.as_ref().ok_or(CryptoError::Invalid)?,
                    &payload,
                )?;
                if required {
                    let mut journal =
                        load_incoming(tx, own, &payload.message_id)?.ok_or(CryptoError::Invalid)?;
                    if !journal.confirmation_required {
                        journal.confirmation_required = true;
                        tx.put(
                            &journal_key(own, INCOMING, &payload.message_id)?,
                            &journal.encode_to_vec(),
                        )?;
                        tx.project_recovery(
                            &journal_key(own, INCOMING, &payload.message_id)?,
                            !journal.acknowledged,
                        )?;
                    }
                }
            }
            Ok::<_, CryptoError>((payload, request))
        })?;
        Ok((
            payload,
            DurableAck {
                store_scope,
                request,
            },
        ))
    }
    fn pending_acknowledgements(&mut self, after: Option<&str>) -> CryptoResult<AckPage> {
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            check_owner(tx, own)?;
            let cursor = after
                .map(|id| journal_key(own, INCOMING, id).map(|_| format!("{INCOMING}{id}")))
                .transpose()?;
            recovery_indexes::require_ready(tx, own)?;
            let rows = tx.prototype_recovery_page(
                &own.generation_id,
                RecordKind::Control,
                INCOMING,
                cursor.as_deref(),
            )?;
            let next = if rows.len() == 32 {
                rows.last()
                    .and_then(|id| id.strip_prefix(INCOMING))
                    .map(str::to_string)
            } else {
                None
            };
            let mut acknowledgements = Vec::new();
            for id in rows {
                let id = id.strip_prefix(INCOMING).ok_or(CryptoError::Invalid)?;
                let saved = load_incoming(tx, own, id)?.ok_or(CryptoError::Invalid)?;
                if saved.acknowledged {
                    return Err(CryptoError::Invalid);
                }
                acknowledgements.push(DurableAck {
                    store_scope: store_scope.clone(),
                    request: acknowledgement(own, &saved)?,
                });
            }
            Ok(AckPage {
                acknowledgements,
                next,
            })
        })
    }
    fn mark_acknowledged(&mut self, proof: VerifiedAck) -> CryptoResult<()> {
        if proof.proof.store_scope != *self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        let own = &self.own;
        let request = proof.proof.request;
        validate_acknowledgements(&request).map_err(|_| CryptoError::Invalid)?;
        if request.context.as_ref() != Some(&context(own)) || request.messages.len() != 1 {
            return Err(CryptoError::Invalid);
        }
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            check_owner(tx, own)?;
            let ack = &request.messages[0];
            let mut saved =
                load_incoming(tx, own, &ack.message_id)?.ok_or(CryptoError::MessageUnavailable)?;
            if saved.acknowledgement.as_ref() != Some(ack) {
                return Err(CryptoError::Conflict);
            }
            if !saved.acknowledged {
                saved.acknowledged = true;
                tx.put(
                    &journal_key(own, INCOMING, &ack.message_id)?,
                    &saved.encode_to_vec(),
                )?;
                tx.project_recovery(&journal_key(own, INCOMING, &ack.message_id)?, false)?;
            }
            Ok(())
        })
    }
}

pub(super) enum DeliveryCommand {
    Plan(String, Reply<OutgoingPlan>),
    OutboxPage(Option<String>, Reply<OutboxPage>),
    Receipt(VerifiedSendReceipt, Reply<OutgoingProgress>),
    Statuses(VerifiedMessageStatuses, Reply<Vec<OutgoingProgress>>),
    Ack(
        SessionScope,
        Delivery,
        Option<QuarantineReason>,
        Reply<DurableAck>,
    ),
    Receive(
        SessionScope,
        Delivery,
        bool,
        Reply<(EncryptedPayload, DurableAck)>,
    ),
    AckPage(Option<String>, Reply<AckPage>),
    MarkAck(VerifiedAck, Reply<()>),
}
impl Engine {
    pub(super) fn handle_delivery(&mut self, command: DeliveryCommand) {
        match command {
            DeliveryCommand::Plan(id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.outgoing_plan(&id));
                }
            }
            DeliveryCommand::OutboxPage(after, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.outbox_page(after.as_deref()));
                }
            }
            DeliveryCommand::Receipt(proof, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_send_receipt(proof));
                }
            }
            DeliveryCommand::Statuses(proof, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_message_statuses(proof));
                }
            }
            DeliveryCommand::Ack(scope, delivery, reason, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_ack(&scope, &delivery, reason));
                }
            }
            DeliveryCommand::Receive(scope, delivery, confirm_session, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.receive_delivery(&scope, &delivery, confirm_session));
                }
            }
            DeliveryCommand::AckPage(after, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.pending_acknowledgements(after.as_deref()));
                }
            }
            DeliveryCommand::MarkAck(proof, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.mark_acknowledged(proof));
                }
            }
        }
    }
}
impl CryptoCoordinator {
    pub async fn outgoing_plan(&self, id: String) -> CryptoResult<OutgoingPlan> {
        self.request(|r| Command::Delivery(DeliveryCommand::Plan(id, r)))
            .await
    }
    pub async fn outbox_page(&self, after: Option<String>) -> CryptoResult<OutboxPage> {
        self.request(|r| Command::Delivery(DeliveryCommand::OutboxPage(after, r)))
            .await
    }
    pub async fn accept_send_receipt(
        &self,
        proof: VerifiedSendReceipt,
    ) -> CryptoResult<OutgoingProgress> {
        self.request(|r| Command::Delivery(DeliveryCommand::Receipt(proof, r)))
            .await
    }
    pub async fn accept_message_statuses(
        &self,
        proof: VerifiedMessageStatuses,
    ) -> CryptoResult<Vec<OutgoingProgress>> {
        self.request(|r| Command::Delivery(DeliveryCommand::Statuses(proof, r)))
            .await
    }
    pub async fn prepare_ack(
        &self,
        scope: SessionScope,
        delivery: Delivery,
    ) -> CryptoResult<DurableAck> {
        check_delivery(&scope.own, &delivery)?;
        self.request(|r| Command::Delivery(DeliveryCommand::Ack(scope, delivery, None, r)))
            .await
    }
    pub async fn quarantine_delivery(
        &self,
        scope: SessionScope,
        delivery: Delivery,
        reason: QuarantineReason,
    ) -> CryptoResult<DurableAck> {
        check_delivery(&scope.own, &delivery)?;
        self.request(|r| Command::Delivery(DeliveryCommand::Ack(scope, delivery, Some(reason), r)))
            .await
    }
    /// One COMMIT covers Signal state, plaintext history, inbox and ACK journal.
    /// Neither plaintext nor proof leaves the worker on failure/unknown COMMIT.
    pub async fn receive_delivery(
        &self,
        scope: SessionScope,
        delivery: Delivery,
    ) -> CryptoResult<(EncryptedPayload, DurableAck)> {
        check_delivery(&scope.own, &delivery)?;
        self.request(|r| Command::Delivery(DeliveryCommand::Receive(scope, delivery, false, r)))
            .await
    }
    /// Native runner receive: the same COMMIT also persists any automatic Signal
    /// confirmation, its UUID/link/history and exact outbox ciphertext. A crash
    /// cannot leave a stored incoming message without its required reply.
    pub async fn receive_for_delivery(
        &self,
        scope: SessionScope,
        delivery: Delivery,
    ) -> CryptoResult<(EncryptedPayload, DurableAck)> {
        check_delivery(&scope.own, &delivery)?;
        self.request(|r| Command::Delivery(DeliveryCommand::Receive(scope, delivery, true, r)))
            .await
    }
    pub async fn pending_acknowledgements(&self, after: Option<String>) -> CryptoResult<AckPage> {
        self.request(|r| Command::Delivery(DeliveryCommand::AckPage(after, r)))
            .await
    }
    pub async fn mark_acknowledged(&self, proof: VerifiedAck) -> CryptoResult<()> {
        self.request(|r| Command::Delivery(DeliveryCommand::MarkAck(proof, r)))
            .await
    }
}
