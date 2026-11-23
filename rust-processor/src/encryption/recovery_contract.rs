//! SESSION1 control contract, not an enabled recovery RPC or epoch state machine.
//! Never replace persisted signatures on retry or derive a fence from transport errors.
use libsignal_protocol::{IdentityKey, IdentityKeyPair};
use prost::Message;
use sha2::{Digest, Sha256};

use super::contract::ContractError;
use crate::proto::e2ee::{
    CryptoSuite, DeviceRef, EpochSealedFence, EpochSendBinding, RecoveryAction, RecoveryControl,
    RecoveryOutcome, RecoveryParticipant, RecoveryState, SignedRecoveryControl,
};

pub const RECOVERY_PROTOCOL_VERSION: u32 = 2;
pub const MAX_RECOVERY_BYTES: usize = 4096;

fn invalid(field: &'static str) -> ContractError {
    ContractError::Invalid(field)
}
fn version(v: u32) -> Result<(), ContractError> {
    if v == RECOVERY_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ContractError::UnsupportedVersion)
    }
}
fn uuid4(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|id| {
        id.get_version_num() == 4
            && id.get_variant() == uuid::Variant::RFC4122
            && id.to_string() == value
    })
}
fn validate_device(d: Option<&DeviceRef>) -> Result<(), ContractError> {
    if d.is_some_and(|d| uuid4(&d.user_id) && uuid4(&d.generation_id) && d.device_id == 1) {
        Ok(())
    } else {
        Err(invalid("recovery device reference"))
    }
}
fn participant(p: Option<&RecoveryParticipant>) -> bool {
    p.is_some_and(|p| {
        validate_device(p.device.as_ref()).is_ok()
            && p.device.as_ref().is_some_and(|d| d.device_id == 1)
            && p.identity_fingerprint.len() == 32
    })
}

pub fn validate_control(c: &RecoveryControl) -> Result<(), ContractError> {
    version(c.protocol_version)?;
    if c.format_version != 1
        || c.crypto_suite != CryptoSuite::SignalPq1 as i32
        || !uuid4(&c.conversation_id)
        || !uuid4(&c.request_id)
        || !uuid4(&c.proposal_id)
        || c.expected_epoch >= i64::MAX as u64
        || c.expected_revision >= i64::MAX as u64
        || !participant(c.initiator.as_ref())
        || !participant(c.peer.as_ref())
        || c.encoded_len() > MAX_RECOVERY_BYTES
    {
        return Err(invalid("recovery fields"));
    }
    let a = c.initiator.as_ref().unwrap().device.as_ref().unwrap();
    let b = c.peer.as_ref().unwrap().device.as_ref().unwrap();
    if a.user_id == b.user_id || a.generation_id == b.generation_id {
        return Err(invalid("recovery participants"));
    }
    match RecoveryAction::try_from(c.action) {
        Ok(RecoveryAction::Propose)
            if c.request_id == c.proposal_id && c.proposal_digest.is_empty() =>
        {
            Ok(())
        }
        Ok(RecoveryAction::Accept | RecoveryAction::Decline | RecoveryAction::Cancel)
            if c.request_id != c.proposal_id && c.proposal_digest.len() == 32 =>
        {
            Ok(())
        }
        _ => Err(invalid("recovery action binding")),
    }
}

/// Explicit signed frame; protobuf wire ordering is deliberately irrelevant.
pub fn control_frame(c: &RecoveryControl) -> Result<Vec<u8>, ContractError> {
    validate_control(c)?;
    let mut b = b"zchat.e2ee.epoch-recovery.v1\0".to_vec();
    let role = if matches!(
        RecoveryAction::try_from(c.action),
        Ok(RecoveryAction::Accept | RecoveryAction::Decline)
    ) {
        2u32
    } else {
        1u32
    };
    for v in [
        c.protocol_version,
        c.format_version,
        c.crypto_suite as u32,
        c.action as u32,
        role,
    ] {
        b.extend(v.to_be_bytes());
    }
    for s in [&c.conversation_id, &c.request_id, &c.proposal_id] {
        b.extend(s.as_bytes());
    }
    b.extend(c.expected_epoch.to_be_bytes());
    b.extend(c.expected_revision.to_be_bytes());
    for p in [c.initiator.as_ref().unwrap(), c.peer.as_ref().unwrap()] {
        let d = p.device.as_ref().unwrap();
        b.extend(d.user_id.as_bytes());
        b.extend(d.device_id.to_be_bytes());
        b.extend(d.generation_id.as_bytes());
        b.extend((p.identity_fingerprint.len() as u32).to_be_bytes());
        b.extend(&p.identity_fingerprint);
    }
    b.extend((c.proposal_digest.len() as u32).to_be_bytes());
    b.extend(&c.proposal_digest);
    Ok(b)
}

pub fn control_digest(c: &RecoveryControl) -> Result<[u8; 32], ContractError> {
    Ok(Sha256::digest(control_frame(c)?).into())
}

pub fn identity_fingerprint(identity: &IdentityKey) -> [u8; 32] {
    Sha256::digest(identity.serialize()).into()
}

pub fn validate_signed(s: &SignedRecoveryControl) -> Result<(), ContractError> {
    if s.signature.len() != 64 || s.encoded_len() > MAX_RECOVERY_BYTES {
        return Err(invalid("signed recovery control"));
    }
    validate_control(s.control.as_ref().ok_or(invalid("recovery control"))?)
}

fn actor(c: &RecoveryControl) -> &RecoveryParticipant {
    if matches!(
        RecoveryAction::try_from(c.action),
        Ok(RecoveryAction::Accept | RecoveryAction::Decline)
    ) {
        c.peer.as_ref().unwrap()
    } else {
        c.initiator.as_ref().unwrap()
    }
}

/// Core-only signing primitive. Future owner must persist exact bytes/signature
/// BEFORE RPC and require explicit local intent/manual peer acceptance.
pub fn sign_control(
    c: RecoveryControl,
    identity: &IdentityKeyPair,
) -> Result<SignedRecoveryControl, ContractError> {
    let frame = control_frame(&c)?;
    if actor(&c).identity_fingerprint != identity_fingerprint(identity.identity_key()) {
        return Err(invalid("recovery signing identity"));
    }
    let signature = identity
        .private_key()
        .calculate_signature(&frame, &mut rand::rng())
        .map_err(|_| invalid("recovery signature"))?
        .to_vec();
    Ok(SignedRecoveryControl {
        control: Some(c),
        signature,
    })
}

/// Uses libsignal's identity curve API, not standard Ed25519. The caller supplies
/// the retained PINNED identity; a key bundled by an untrusted reply is insufficient.
pub fn verify_control(
    s: &SignedRecoveryControl,
    pinned: &IdentityKey,
) -> Result<(), ContractError> {
    validate_signed(s)?;
    let c = s.control.as_ref().unwrap();
    if actor(c).identity_fingerprint != identity_fingerprint(pinned)
        || !pinned
            .public_key()
            .verify_signature(&control_frame(c)?, &s.signature)
    {
        return Err(invalid("recovery signature or identity"));
    }
    Ok(())
}

fn same_context(a: &RecoveryControl, b: &RecoveryControl) -> bool {
    a.protocol_version == b.protocol_version
        && a.format_version == b.format_version
        && a.crypto_suite == b.crypto_suite
        && a.conversation_id == b.conversation_id
        && a.proposal_id == b.proposal_id
        && a.expected_epoch == b.expected_epoch
        && a.expected_revision == b.expected_revision
        && a.initiator == b.initiator
        && a.peer == b.peer
}

pub fn validate_outcome(
    o: &RecoveryOutcome,
    expected: &SignedRecoveryControl,
) -> Result<(), ContractError> {
    version(o.protocol_version)?;
    if o.encoded_len() > MAX_RECOVERY_BYTES {
        return Err(invalid("recovery outcome size"));
    }
    validate_signed(expected)?;
    let p = expected.control.as_ref().unwrap();
    if p.action != RecoveryAction::Propose as i32 || o.proposal.as_ref() != Some(expected) {
        return Err(invalid("recovery outcome proposal"));
    }
    let action = match RecoveryState::try_from(o.state) {
        Ok(RecoveryState::Accepted) => Some(RecoveryAction::Accept),
        Ok(RecoveryState::Declined) => Some(RecoveryAction::Decline),
        Ok(RecoveryState::Cancelled) => Some(RecoveryAction::Cancel),
        Ok(RecoveryState::Pending | RecoveryState::Expired | RecoveryState::Superseded) => None,
        _ => return Err(invalid("recovery state")),
    };
    if let Some(action) = action {
        let d = o
            .decision
            .as_ref()
            .ok_or(invalid("missing recovery decision"))?;
        validate_signed(d)?;
        let c = d.control.as_ref().unwrap();
        if c.action != action as i32
            || !same_context(p, c)
            || c.proposal_digest != control_digest(p)?
        {
            return Err(invalid("recovery decision binding"));
        }
    } else if o.decision.is_some() {
        return Err(invalid("unexpected recovery decision"));
    }
    if o.state == RecoveryState::Accepted as i32 {
        if o.new_epoch != p.expected_epoch + 1 || o.epoch_revision != p.expected_revision + 1 {
            return Err(invalid("recovery CAS result"));
        }
    } else if o.new_epoch != 0 || o.epoch_revision != 0 {
        return Err(invalid("unexpected recovery CAS result"));
    }
    Ok(())
}

/// Signature/context verification only, NOT evidence of authenticated server
/// COMMIT, manual user consent or a durable native transition. Cannot mint scopes.
#[derive(Debug)]
pub struct VerifiedRecoveryOutcome<'a> {
    outcome: &'a RecoveryOutcome,
}

pub fn verify_outcome<'a>(
    o: &'a RecoveryOutcome,
    expected: &SignedRecoveryControl,
    initiator_pin: &IdentityKey,
    peer_pin: &IdentityKey,
) -> Result<VerifiedRecoveryOutcome<'a>, ContractError> {
    validate_outcome(o, expected)?;
    let p = expected.control.as_ref().unwrap();
    // Both pins are checked even for Pending/Expired and initiator-only Cancel.
    if p.initiator.as_ref().unwrap().identity_fingerprint != identity_fingerprint(initiator_pin)
        || p.peer.as_ref().unwrap().identity_fingerprint != identity_fingerprint(peer_pin)
    {
        return Err(invalid("recovery participant pins"));
    }
    verify_control(expected, initiator_pin)?;
    if let Some(d) = &o.decision {
        let pin = if d.control.as_ref().unwrap().action == RecoveryAction::Cancel as i32 {
            initiator_pin
        } else {
            peer_pin
        };
        verify_control(d, pin)?;
    }
    Ok(VerifiedRecoveryOutcome { outcome: o })
}

pub fn validate_send_binding(b: &EpochSendBinding) -> Result<(), ContractError> {
    super::epoch_contract::framing_version(b.protocol_version)?;
    if !uuid4(&b.conversation_id)
        || !uuid4(&b.message_id)
        || b.epoch > i64::MAX as u64
        || b.envelope_digest.len() != 32
    {
        return Err(invalid("epoch Send fields"));
    }
    validate_device(b.sender.as_ref())?;
    validate_device(b.recipient.as_ref())?;
    let a = b.sender.as_ref().unwrap();
    let c = b.recipient.as_ref().unwrap();
    if a.device_id != 1
        || c.device_id != 1
        || a.user_id == c.user_id
        || a.generation_id == c.generation_id
    {
        return Err(invalid("epoch Send participants"));
    }
    Ok(())
}

pub fn validate_fence(
    f: &EpochSealedFence,
    expected: &EpochSendBinding,
    accepted: &VerifiedRecoveryOutcome<'_>,
) -> Result<(), ContractError> {
    validate_send_binding(expected)?;
    let o = accepted.outcome;
    if f.binding.as_ref() != Some(expected) || o.state != RecoveryState::Accepted as i32 {
        return Err(invalid("epoch fence"));
    }
    let p = o.proposal.as_ref().unwrap().control.as_ref().unwrap();
    let a = &p.initiator.as_ref().unwrap().device;
    let b = &p.peer.as_ref().unwrap().device;
    let participants = (&expected.sender == a && &expected.recipient == b)
        || (&expected.sender == b && &expected.recipient == a);
    if !participants
        || expected.conversation_id != p.conversation_id
        || expected.epoch != p.expected_epoch
        || f.proposal_digest != control_digest(p)?
        || f.next_epoch != o.new_epoch
        || f.epoch_revision != o.epoch_revision
    {
        return Err(invalid("epoch fence binding"));
    }
    Ok(())
}

/// V2-only routing boundary. Epoch0 is the first session, not a legacy fallback.
pub fn validate_epoch_version_binding(
    protocol: u32,
    epoch: u64,
    authoritative_epoch: u64,
) -> Result<(), ContractError> {
    if epoch > i64::MAX as u64
        || authoritative_epoch > i64::MAX as u64
        || epoch != authoritative_epoch
    {
        return Err(invalid("data epoch binding"));
    }
    if protocol == RECOVERY_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(ContractError::UnsupportedVersion)
    }
}
