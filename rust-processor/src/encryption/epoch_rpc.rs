//! V2 RPC boundaries. Observations never activate scopes, unfreeze sending or ACK
//! plaintext. Unknown prost fields are discarded: critical changes need a version bump.
use std::collections::HashSet;

use libsignal_protocol::IdentityKey;
use prost::Message;

use super::{
    contract::{
        self, ContractError, MAX_ACKNOWLEDGEMENTS, MAX_INBOX_MESSAGES, MAX_REQUEST_BYTES,
        MAX_RESPONSE_BYTES,
    },
    epoch_contract as data,
    recovery_contract::{self as control, VerifiedRecoveryOutcome},
};
use crate::proto::{e2ee as common, e2ee_v2 as v2};

// Owner batches use the same32-row budget as protected pending pages. The raw
// wire boundary retains its separately advertised100-reference limit.
pub(super) const MAX_PREPARED_STATUS_PLANS: usize = 32;

pub(super) fn validate_expired_acquisition(
    r: &v2::AcquireBundleRequest,
    failure: &v2::AcquisitionExpiredFailure,
) -> Result<(), ContractError> {
    data::validate_acquisition(r)?;
    if failure.encoded_len() > super::contract::MAX_RESPONSE_BYTES
        || failure.request.as_ref() != Some(r)
    {
        return Err(invalid("exact v2 acquisition expiry binding"));
    }
    Ok(())
}

fn invalid(field: &'static str) -> ContractError {
    ContractError::Invalid(field)
}
pub(super) fn uuid4(value: &str) -> bool {
    use uuid::{Uuid, Variant};
    value.len() == 36
        && Uuid::parse_str(value).is_ok_and(|id| {
            id.get_version_num() == 4
                && id.get_variant() == Variant::RFC4122
                && id.to_string() == value
        })
}
fn epoch(value: Option<u64>) -> Result<u64, ContractError> {
    value
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or(invalid("required v2 epoch/revision"))
}
pub fn validate_context(c: Option<&common::ClientContext>) -> Result<(), ContractError> {
    let c = c
        .filter(|c| uuid4(&c.device_generation_id))
        .ok_or(invalid("v2 context"))?;
    control::validate_epoch_version_binding(c.protocol_version, 0, 0)
}

/// Retained native routing/identity pins, NOT a session, epoch head or UI input.
/// The future protected owner supplies both pins; server metadata cannot replace them.
#[derive(Clone)]
pub struct EpochConversation {
    conversation: common::Conversation,
    own_pin: IdentityKey,
    peer_pin: IdentityKey,
}
impl EpochConversation {
    pub fn new(
        conversation: common::Conversation,
        own_pin: IdentityKey,
        peer_pin: IdentityKey,
    ) -> Result<Self, ContractError> {
        contract::validate_conversation(&conversation)?;
        if conversation.peer_device.as_ref().unwrap().identity_key != peer_pin.serialize().as_ref()
            || conversation
                .peer_device
                .as_ref()
                .unwrap()
                .public_material_revision
                > i64::MAX as u64
        {
            return Err(invalid("retained peer identity"));
        }
        Ok(Self {
            conversation,
            own_pin,
            peer_pin,
        })
    }
    pub fn conversation(&self) -> &common::Conversation {
        &self.conversation
    }
    pub(super) fn own(&self) -> &common::DeviceRef {
        self.conversation.own_device.as_ref().unwrap()
    }
    pub(super) fn peer(&self) -> &common::DeviceRef {
        self.conversation
            .peer_device
            .as_ref()
            .unwrap()
            .reference
            .as_ref()
            .unwrap()
    }
    pub(super) fn check_send(&self, r: &v2::SendMessageRequest) -> Result<(), ContractError> {
        data::validate_send(r)?;
        let e = r.envelope.as_ref().unwrap();
        if e.conversation_id != self.conversation.conversation_id
            || e.sender.as_ref() != Some(self.own())
            || e.recipient.as_ref() != Some(self.peer())
        {
            return Err(invalid("retained send conversation"));
        }
        Ok(())
    }
    pub(super) fn check_acquire(&self, r: &v2::AcquireBundleRequest) -> Result<(), ContractError> {
        data::validate_acquisition(r)?;
        if r.conversation_id != self.conversation.conversation_id
            || r.expected_peer.as_ref() != Some(self.peer())
            || r.context.as_ref().unwrap().device_generation_id != self.own().generation_id
        {
            return Err(invalid("retained acquisition conversation"));
        }
        Ok(())
    }
    pub(super) fn verify_bundle(
        &self,
        r: &v2::AcquireBundleRequest,
        b: &v2::AcquiredBundle,
    ) -> Result<(), ContractError> {
        self.check_acquire(r)?;
        data::verify_pinned_bundle(
            r,
            b,
            &self.peer_pin,
            self.conversation
                .peer_device
                .as_ref()
                .unwrap()
                .registration_id,
        )
    }
    fn pins<'a>(
        &'a self,
        p: &common::SignedRecoveryControl,
    ) -> Result<(&'a IdentityKey, &'a IdentityKey), ContractError> {
        control::validate_signed(p)?;
        let c = p.control.as_ref().unwrap();
        if c.conversation_id != self.conversation.conversation_id {
            return Err(invalid("retained recovery conversation"));
        }
        let a = &c.initiator.as_ref().unwrap().device;
        let b = &c.peer.as_ref().unwrap().device;
        let pins = if a.as_ref() == Some(self.own()) && b.as_ref() == Some(self.peer()) {
            (&self.own_pin, &self.peer_pin)
        } else if a.as_ref() == Some(self.peer()) && b.as_ref() == Some(self.own()) {
            (&self.peer_pin, &self.own_pin)
        } else {
            return Err(invalid("retained recovery participants"));
        };
        if c.initiator.as_ref().unwrap().identity_fingerprint
            != control::identity_fingerprint(pins.0)
            || c.peer.as_ref().unwrap().identity_fingerprint
                != control::identity_fingerprint(pins.1)
        {
            return Err(invalid("retained recovery identity fingerprints"));
        }
        Ok(pins)
    }
    pub(super) fn verify_proposal(
        &self,
        proposal: &common::SignedRecoveryControl,
    ) -> Result<(), ContractError> {
        let (a, _) = self.pins(proposal)?;
        if proposal.control.as_ref().unwrap().action != common::RecoveryAction::Propose as i32 {
            return Err(invalid("recovery proposal action"));
        }
        control::verify_control(proposal, a)
    }
    pub(super) fn verify_record<'a>(
        &self,
        r: &'a v2::RecoveryRecord,
        expected: Option<&common::SignedRecoveryControl>,
    ) -> Result<VerifiedRecoveryOutcome<'a>, ContractError> {
        let o = r
            .outcome
            .as_ref()
            .ok_or(invalid("recovery record outcome"))?;
        let p = o
            .proposal
            .as_ref()
            .ok_or(invalid("recovery record proposal"))?;
        let (a, b) = self.pins(p)?;
        let verified = control::verify_outcome(o, expected.unwrap_or(p), a, b)?;
        let pending = o.state == common::RecoveryState::Pending as i32;
        if r.encoded_len() > MAX_RESPONSE_BYTES
            || r.created_at_unix_ms <= 0
            || r.expires_at_unix_ms <= r.created_at_unix_ms
            || pending != r.resolved_at_unix_ms.is_none()
            || r.resolved_at_unix_ms
                .is_some_and(|at| at < r.created_at_unix_ms)
            || o.state == common::RecoveryState::Expired as i32
                && r.resolved_at_unix_ms
                    .is_none_or(|at| at < r.expires_at_unix_ms)
        {
            return Err(invalid("recovery record timestamps"));
        }
        Ok(verified)
    }
    pub(super) fn verify_command(
        &self,
        p: &common::SignedRecoveryControl,
        command: &common::SignedRecoveryControl,
        propose: bool,
    ) -> Result<(), ContractError> {
        let (a, b) = self.pins(p)?;
        if p.control.as_ref().unwrap().action != common::RecoveryAction::Propose as i32 {
            return Err(invalid("recovery proposal action"));
        }
        control::verify_control(p, a)?;
        control::validate_signed(command)?;
        let c = command.control.as_ref().unwrap();
        let action =
            common::RecoveryAction::try_from(c.action).map_err(|_| invalid("recovery action"))?;
        if propose {
            if command != p || action != common::RecoveryAction::Propose {
                return Err(invalid("proposal exact frame"));
            }
        } else {
            if !matches!(
                action,
                common::RecoveryAction::Accept
                    | common::RecoveryAction::Decline
                    | common::RecoveryAction::Cancel
            ) {
                return Err(invalid("decision action"));
            }
            let mut expected = p.control.clone().unwrap();
            expected.action = c.action;
            expected.request_id = c.request_id.clone();
            expected.proposal_digest =
                control::control_digest(p.control.as_ref().unwrap())?.to_vec();
            if &expected != c {
                return Err(invalid("decision exact proposal"));
            }
        }
        let (actor, pin) = if matches!(
            action,
            common::RecoveryAction::Accept | common::RecoveryAction::Decline
        ) {
            (&c.peer, b)
        } else {
            (&c.initiator, a)
        };
        if actor.as_ref().and_then(|p| p.device.as_ref()) != Some(self.own()) {
            return Err(invalid("recovery command owner"));
        }
        control::verify_control(command, pin)
    }
    pub(super) fn verify_sealed(
        &self,
        r: &v2::SendMessageRequest,
        failure: &v2::EpochSealedFailure,
    ) -> Result<(), ContractError> {
        self.check_send(r)?;
        let o = failure
            .recovery
            .as_ref()
            .ok_or(invalid("sealed transition"))?;
        let p = o.proposal.as_ref().ok_or(invalid("sealed proposal"))?;
        let (a, b) = self.pins(p)?;
        let accepted = control::verify_outcome(o, p, a, b)?;
        let binding = data::send_binding(r.envelope.as_ref().unwrap())?;
        control::validate_fence(
            failure.fence.as_ref().ok_or(invalid("sealed fence"))?,
            &binding,
            &accepted,
        )
    }
}

pub fn validate_capabilities(c: &v2::Capabilities) -> Result<(), ContractError> {
    let l = c.limits.as_ref().ok_or(invalid("v2 limits"))?;
    if c.protocol_version != 2
        || c.max_active_devices != 1
        || c.crypto_suites != [common::CryptoSuite::SignalPq1 as i32]
        || c.encoded_len() > MAX_RESPONSE_BYTES
        || l.max_request_bytes != MAX_REQUEST_BYTES as u32
        || l.max_response_bytes != MAX_RESPONSE_BYTES as u32
        || l.max_text_bytes != contract::MAX_TEXT_BYTES as u32
        || l.max_inbox_messages != MAX_INBOX_MESSAGES as u32
        || l.max_acknowledgements != MAX_ACKNOWLEDGEMENTS as u32
        || l.max_keys_per_kind_per_upload != 100
    {
        return Err(invalid("v2 capabilities"));
    }
    Ok(())
}
pub fn validate_pending(
    page: &v2::PendingMessages,
    own: &common::DeviceRef,
) -> Result<(), ContractError> {
    if !uuid4(&own.user_id)
        || !uuid4(&own.generation_id)
        || own.device_id != 1
        || page.messages.len() > MAX_INBOX_MESSAGES
        || page.encoded_len() > MAX_RESPONSE_BYTES
    {
        return Err(invalid("v2 inbox bounds"));
    }
    let mut sequence = 0;
    let mut ids = HashSet::new();
    for row in &page.messages {
        let e = row.envelope.as_ref().ok_or(invalid("v2 inbox envelope"))?;
        data::validate_envelope(e)?;
        if row.sequence <= sequence
            || e.recipient.as_ref() != Some(own)
            || row.expires_at_unix_ms <= 0
            || row.envelope_digest != data::envelope_digest(e)?
            || !ids.insert(&e.message_id)
        {
            return Err(invalid("v2 inbox binding/order"));
        }
        sequence = row.sequence;
    }
    Ok(())
}
pub fn status_request(
    context: common::ClientContext,
    sends: &[v2::SendMessageRequest],
) -> Result<common::GetMessageStatusesRequest, ContractError> {
    validate_context(Some(&context))?;
    if sends.is_empty() || sends.len() > MAX_ACKNOWLEDGEMENTS {
        return Err(invalid("v2 status batch"));
    }
    let mut ids = HashSet::new();
    let mut messages = Vec::with_capacity(sends.len());
    for r in sends {
        data::validate_send(r)?;
        let e = r.envelope.as_ref().unwrap();
        if r.context.as_ref() != Some(&context) || !ids.insert(&e.message_id) {
            return Err(invalid("v2 status request binding"));
        }
        messages.push(common::MessageReference {
            message_id: e.message_id.clone(),
            envelope_digest: data::envelope_digest(e)?.to_vec(),
        });
    }
    let r = common::GetMessageStatusesRequest {
        context: Some(context),
        messages,
    };
    if r.encoded_len() > MAX_REQUEST_BYTES {
        return Err(invalid("v2 status request bytes"));
    }
    Ok(r)
}
pub fn validate_statuses(
    sends: &[v2::SendMessageRequest],
    result: &v2::MessageStatuses,
) -> Result<(), ContractError> {
    let context = sends
        .first()
        .and_then(|r| r.context.clone())
        .ok_or(invalid("v2 status requests"))?;
    status_request(context, sends)?;
    if result.messages.len() != sends.len() || result.encoded_len() > MAX_RESPONSE_BYTES {
        return Err(invalid("v2 status response batch"));
    }
    for (r, m) in sends.iter().zip(&result.messages) {
        let receipt = m.receipt.as_ref().ok_or(invalid("v2 status receipt"))?;
        data::validate_receipt(r, receipt)?;
        if m.observed_at_unix_ms < receipt.stored_at_unix_ms {
            return Err(invalid("v2 status time"));
        }
        match common::MessageState::try_from(m.state) {
            Ok(common::MessageState::Pending)
                if m.observed_at_unix_ms == receipt.stored_at_unix_ms => {}
            Ok(common::MessageState::Expired)
                if m.observed_at_unix_ms >= receipt.expires_at_unix_ms => {}
            Ok(
                common::MessageState::RecipientStored
                | common::MessageState::RecipientRejected
                | common::MessageState::RecipientReplaced,
            ) => {}
            _ => return Err(invalid("v2 status state")),
        }
    }
    Ok(())
}
pub(super) fn validate_ack(r: &common::AcknowledgeMessagesRequest) -> Result<(), ContractError> {
    validate_context(r.context.as_ref())?;
    if r.messages.is_empty()
        || r.messages.len() > MAX_ACKNOWLEDGEMENTS
        || r.encoded_len() > MAX_REQUEST_BYTES
    {
        return Err(invalid("v2 ACK batch"));
    }
    let mut ids = HashSet::new();
    for m in &r.messages {
        if !uuid4(&m.message_id)
            || m.envelope_digest.len() != 32
            || !matches!(
                common::AckDisposition::try_from(m.disposition),
                Ok(common::AckDisposition::Stored | common::AckDisposition::Rejected)
            )
            || !ids.insert(&m.message_id)
        {
            return Err(invalid("v2 ACK binding"));
        }
    }
    Ok(())
}

pub fn validate_recovery_page(
    r: &v2::GetPendingEpochRecoveriesRequest,
    page: &v2::PendingEpochRecoveries,
) -> Result<(), ContractError> {
    validate_context(r.context.as_ref())?;
    if !(1..=32).contains(&r.limit)
        || !r.after_conversation_id.is_empty() && !uuid4(&r.after_conversation_id)
        || page.proposals.len() > r.limit as usize
        || page.encoded_len() > MAX_RESPONSE_BYTES
    {
        return Err(invalid("recovery discovery bounds"));
    }
    let mut previous = r.after_conversation_id.as_str();
    let mut ids = HashSet::new();
    for p in &page.proposals {
        if !uuid4(&p.conversation_id)
            || !uuid4(&p.proposal_id)
            || p.conversation_id.as_str() <= previous
            || !ids.insert(&p.proposal_id)
            || p.expected_epoch >= i64::MAX as u64
            || p.expected_revision >= i64::MAX as u64
            || p.created_at_unix_ms <= 0
            || p.expires_at_unix_ms <= p.created_at_unix_ms
        {
            return Err(invalid("recovery discovery reference"));
        }
        previous = &p.conversation_id;
    }
    if !page.next_conversation_id.is_empty()
        && (page.proposals.len() != r.limit as usize
            || page.next_conversation_id != previous
            || !uuid4(&page.next_conversation_id))
    {
        return Err(invalid("recovery discovery cursor"));
    }
    Ok(())
}
pub fn validate_catalog(
    r: &v2::GetConversationEpochsRequest,
    page: &v2::ConversationEpochs,
    pins: &EpochConversation,
) -> Result<(), ContractError> {
    validate_context(r.context.as_ref())?;
    if !(1..=32).contains(&r.limit)
        || r.context.as_ref().unwrap().device_generation_id != pins.own().generation_id
        || r.conversation_id != pins.conversation.conversation_id
        || r.after_epoch.is_some_and(|e| e > i64::MAX as u64)
        || page.protocol_version != 2
        || page.encoded_len() > MAX_RESPONSE_BYTES
    {
        return Err(invalid("epoch catalog request/bounds"));
    }
    let head = epoch(page.active_epoch)?;
    let revision = epoch(page.epoch_revision)?;
    let c = page
        .conversation
        .as_ref()
        .ok_or(invalid("epoch conversation"))?;
    contract::validate_conversation(c)?;
    let p = c.peer_device.as_ref().unwrap();
    let saved = &pins.conversation;
    let saved_peer = saved.peer_device.as_ref().unwrap();
    if c.conversation_id != r.conversation_id
        || c.own_device != saved.own_device
        || p.reference != saved_peer.reference
        || p.identity_key != saved_peer.identity_key
        || p.registration_id != saved_peer.registration_id
        || p.public_material_revision < saved_peer.public_material_revision
        || p.public_material_revision > i64::MAX as u64
        || saved.state == common::ConversationState::Closed as i32
            && (c.state != saved.state
                || c.close_reason != saved.close_reason
                || c.closed_generation_id != saved.closed_generation_id
                || c.closed_at_unix_ms != saved.closed_at_unix_ms)
    {
        return Err(invalid("retained catalog conversation"));
    }
    let first = match r.after_epoch {
        Some(e) if e >= head => None,
        Some(e) => Some(e + 1),
        None => Some(0),
    };
    let count = first.map_or(0, |first| (head - first + 1).min(r.limit as u64) as usize);
    if page.epochs.len() != count {
        return Err(invalid("epoch catalog page length"));
    }
    let mut previous: Option<&v2::EpochRecord> = None;
    for (i, e) in page.epochs.iter().enumerate() {
        let value = epoch(e.epoch)?;
        let rev = epoch(e.revision)?;
        if value != first.unwrap() + i as u64
            || rev > revision
            || e.created_at_unix_ms <= 0
            || value == head && rev != revision
        {
            return Err(invalid("epoch catalog order/head"));
        }
        if value == 0 {
            if rev != 0 || !e.created_by_proposal_id.is_empty() || e.bootstrap_device.is_some() {
                return Err(invalid("initial epoch metadata"));
            }
        } else if !uuid4(&e.created_by_proposal_id)
            || e.bootstrap_device.as_ref() != Some(pins.own())
                && e.bootstrap_device.as_ref() != Some(pins.peer())
        {
            return Err(invalid("epoch bootstrap metadata"));
        }
        if value < head {
            if !e.sealed
                || !uuid4(&e.sealed_by_proposal_id)
                || e.sealed_at_unix_ms
                    .is_none_or(|at| at < e.created_at_unix_ms)
            {
                return Err(invalid("sealed epoch metadata"));
            }
        } else if e.sealed || e.sealed_at_unix_ms.is_some() || !e.sealed_by_proposal_id.is_empty() {
            return Err(invalid("active epoch metadata"));
        }
        if let Some(prev) = previous
            && (prev.revision.unwrap().checked_add(1) != Some(rev)
                || prev.sealed_by_proposal_id != e.created_by_proposal_id
                || prev.sealed_at_unix_ms != Some(e.created_at_unix_ms))
        {
            return Err(invalid("epoch transition links"));
        }
        previous = Some(e);
    }
    let expected_next = page
        .epochs
        .last()
        .and_then(|e| e.epoch.filter(|e| *e < head));
    if page.next_after_epoch != expected_next {
        return Err(invalid("epoch catalog cursor"));
    }
    Ok(())
}

#[cfg(test)]
#[path = "epoch_rpc_tests.rs"]
pub(crate) mod tests;
