//! Protected recovery journals and the root-bound, bounded current-head index.
use super::*;

const MAX_JOURNALS: u32 = 256;
#[derive(Message)]
pub(super) struct Head {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(uint64, optional, tag = "2")]
    pub(super) epoch: Option<u64>,
    #[prost(message, optional, tag = "3")]
    pub(super) own: Option<Reference>,
    #[prost(message, optional, tag = "4")]
    pub(super) peer: Option<Reference>,
    #[prost(uint32, optional, tag = "5")]
    pub(super) count: Option<u32>,
    #[prost(message, optional, tag = "6")]
    pub(super) activation: Option<transition::Progress>,
}
#[derive(Message)]
pub(super) struct Reference {
    #[prost(string, tag = "1")]
    pub(super) id: String,
    #[prost(bytes = "vec", tag = "2")]
    hash: Vec<u8>,
}
#[derive(Message)]
pub(super) struct Journal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    pub(super) proposal: Option<common::SignedRecoveryControl>,
    #[prost(bool, optional, tag = "3")]
    pub(super) own_intent: Option<bool>,
    #[prost(message, optional, tag = "4")]
    pub(super) decision: Option<common::SignedRecoveryControl>,
    #[prost(message, optional, tag = "5")]
    pub(super) observed: Option<v2::RecoveryRecord>,
    #[prost(uint64, tag = "6")]
    revision: u64,
}
impl Journal {
    pub(super) fn new(p: common::SignedRecoveryControl, own: bool) -> Self {
        Self {
            format: FORMAT,
            proposal: Some(p),
            own_intent: Some(own),
            decision: None,
            observed: None,
            revision: 0,
        }
    }
    pub(super) fn proposal(&self) -> CryptoResult<&common::SignedRecoveryControl> {
        self.proposal.as_ref().ok_or(CryptoError::Invalid)
    }
    pub(super) fn control(&self) -> CryptoResult<&common::RecoveryControl> {
        self.proposal()?
            .control
            .as_ref()
            .ok_or(CryptoError::Invalid)
    }
    pub(super) fn state(&self) -> Option<common::RecoveryState> {
        self.observed
            .as_ref()
            .and_then(|r| r.outcome.as_ref())
            .and_then(|o| common::RecoveryState::try_from(o.state).ok())
    }
    pub(super) fn terminal(&self) -> bool {
        self.state()
            .is_some_and(|s| s != common::RecoveryState::Pending)
    }
    pub(super) fn freezes(&self) -> bool {
        let local = self.own_intent == Some(true)
            || self
                .decision
                .as_ref()
                .and_then(|d| d.control.as_ref())
                .is_some_and(|c| c.action == common::RecoveryAction::Accept as i32);
        local
            && !matches!(
                self.state(),
                Some(
                    common::RecoveryState::Declined
                        | common::RecoveryState::Cancelled
                        | common::RecoveryState::Expired
                )
            )
    }
}
pub(super) fn head_key(own: &DeviceRef, chat: &str, epoch: u64) -> CryptoResult<RecordKey> {
    key(own, chat, &format!("recovery-head:{epoch:016x}"))
}
pub(super) fn journal_key(own: &DeviceRef, chat: &str, id: &str) -> CryptoResult<RecordKey> {
    if !epoch_rpc::uuid4(id) {
        return Err(CryptoError::Invalid);
    }
    key(own, chat, &format!("recovery:{id}"))
}
fn anchor_key(own: &DeviceRef, chat: &str, id: &str) -> CryptoResult<RecordKey> {
    if !epoch_rpc::uuid4(id) {
        return Err(CryptoError::Invalid);
    }
    key(own, chat, &format!("recovery-anchor:{id}"))
}
fn hash(bytes: &[u8]) -> [u8; 32] {
    digest(b"zchat.e2ee.recovery-journal.v2\0", bytes)
}
fn head_hash(bytes: &[u8]) -> [u8; 32] {
    digest(b"zchat.e2ee.recovery-head.v2\0", bytes)
}

pub(in crate::encryption::signal_state::epoch_state) fn initialize(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    chat: &str,
    epoch: u64,
) -> CryptoResult<Vec<u8>> {
    let h = Head {
        format: FORMAT,
        epoch: Some(epoch),
        own: None,
        peer: None,
        count: Some(0),
        activation: None,
    };
    let bytes = h.encode_to_vec();
    tx.put(&head_key(own, chat, epoch)?, &bytes)?;
    Ok(head_hash(&bytes).to_vec())
}

pub(super) fn load_journal(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    pins: &EpochConversation,
    id: &str,
) -> CryptoResult<Option<Journal>> {
    let chat = &pins.conversation().conversation_id;
    let bytes = tx.get(&journal_key(own, chat, id)?)?;
    let anchor = tx.get(&anchor_key(own, chat, id)?)?;
    let (bytes, anchor) = match (bytes, anchor) {
        (None, None) => return Ok(None),
        (Some(b), Some(a)) => (b, a),
        _ => return Err(CryptoError::Invalid),
    };
    if bytes.len() > 16384 || anchor.as_slice() != hash(&bytes) {
        return Err(CryptoError::Conflict);
    }
    let j = Journal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let p = j.proposal()?;
    let c = j.control()?;
    let is_own = c.initiator.as_ref().and_then(|p| p.device.as_ref()) == Some(own);
    if j.format != FORMAT
        || j.revision == 0
        || j.revision > i64::MAX as u64
        || j.own_intent != Some(is_own)
        || c.proposal_id != id
    {
        return Err(CryptoError::Conflict);
    }
    pins.verify_proposal(p).map_err(|_| CryptoError::Invalid)?;
    if !is_own && j.observed.is_none() {
        return Err(CryptoError::Invalid);
    }
    if let Some(d) = &j.decision {
        pins.verify_command(p, d, false)
            .map_err(|_| CryptoError::Invalid)?;
    }
    if let Some(r) = &j.observed {
        pins.verify_record(r, Some(p))
            .map_err(|_| CryptoError::Invalid)?;
        check_local_consent(&j, r, own)?;
    }
    Ok(Some(j))
}

pub(super) fn check_local_consent(
    j: &Journal,
    r: &v2::RecoveryRecord,
    own: &DeviceRef,
) -> CryptoResult<()> {
    let o = r.outcome.as_ref().ok_or(CryptoError::Invalid)?;
    if let Some(d) = &o.decision {
        let c = d.control.as_ref().ok_or(CryptoError::Invalid)?;
        let actor = if c.action == common::RecoveryAction::Cancel as i32 {
            &c.initiator
        } else {
            &c.peer
        };
        if actor.as_ref().and_then(|p| p.device.as_ref()) == Some(own)
            && j.decision.as_ref() != Some(d)
        {
            return Err(CryptoError::Conflict);
        }
    }
    if o.state == common::RecoveryState::Accepted as i32
        && j.own_intent != Some(true)
        && j.decision.as_ref() != o.decision.as_ref()
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}

pub(super) fn read_head(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    root: &Root,
    pins: &EpochConversation,
) -> CryptoResult<(Head, bool)> {
    let chat = &pins.conversation().conversation_id;
    let bytes = tx
        .get(&head_key(
            own,
            chat,
            root.active_epoch.ok_or(CryptoError::Invalid)?,
        )?)?
        .ok_or(CryptoError::Invalid)?;
    if bytes.len() > 16384 || root.recovery_digest.as_slice() != head_hash(&bytes) {
        return Err(CryptoError::Conflict);
    }
    let h = Head::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if h.format != FORMAT
        || h.epoch != root.active_epoch
        || h.count.is_none_or(|n| n > MAX_JOURNALS)
        || (h.own.is_some() as u32 + h.peer.is_some() as u32) > h.count.unwrap_or(0)
    {
        return Err(CryptoError::Conflict);
    }
    let mut freezes = false;
    for (reference, is_own) in [(&h.own, true), (&h.peer, false)] {
        if let Some(r) = reference {
            let j = load_journal(tx, own, pins, &r.id)?.ok_or(CryptoError::Invalid)?;
            if r.hash.as_slice() != hash(&j.encode_to_vec())
                || j.own_intent != Some(is_own)
                || Some(j.control()?.expected_epoch) != root.active_epoch
                || Some(j.control()?.expected_revision) != root.revision
            {
                return Err(CryptoError::Conflict);
            }
            freezes |= j.freezes();
        }
    }
    if let Some(p) = &h.activation {
        let id = transition::accepted_id(p)?;
        if ![&h.own, &h.peer].into_iter().flatten().any(|r| r.id == id) {
            return Err(CryptoError::Conflict);
        }
        transition::validate_progress(tx, own, root, pins, p)?;
    }
    if freezes && root.send_frozen != Some(true) {
        return Err(CryptoError::Conflict);
    }
    Ok((h, freezes))
}
pub(in crate::encryption::signal_state::epoch_state) fn validate_head(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    root: &Root,
    pins: &EpochConversation,
) -> CryptoResult<()> {
    read_head(tx, own, root, pins).map(|_| ())
}

pub(super) fn save_journal(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    chat: &str,
    h: &mut Head,
    j: &mut Journal,
    fresh: bool,
) -> CryptoResult<()> {
    if fresh {
        let count = h.count.ok_or(CryptoError::Invalid)?;
        if count >= MAX_JOURNALS {
            return Err(CryptoError::EpochRecoveryCapacity);
        }
        h.count = Some(count + 1);
    }
    j.revision = j
        .revision
        .checked_add(1)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    let id = j.control()?.proposal_id.clone();
    let bytes = j.encode_to_vec();
    let digest = hash(&bytes).to_vec();
    tx.put(&journal_key(own, chat, &id)?, &bytes)?;
    tx.put(&anchor_key(own, chat, &id)?, &digest)?;
    let r = Some(Reference { id, hash: digest });
    if j.own_intent == Some(true) {
        h.own = r;
    } else {
        h.peer = r;
    }
    Ok(())
}

pub(super) fn save_head(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    pins: &EpochConversation,
    root: &mut Root,
    h: &Head,
    previously_frozen_by_recovery: bool,
) -> CryptoResult<()> {
    let chat = &pins.conversation().conversation_id;
    let mut freezes = false;
    for r in [&h.own, &h.peer].into_iter().flatten() {
        freezes |= load_journal(tx, own, pins, &r.id)?
            .ok_or(CryptoError::Invalid)?
            .freezes();
    }
    root.send_frozen =
        Some(freezes || (root.send_frozen == Some(true) && !previously_frozen_by_recovery));
    let bytes = h.encode_to_vec();
    root.recovery_digest = head_hash(&bytes).to_vec();
    tx.put(
        &head_key(own, chat, root.active_epoch.ok_or(CryptoError::Invalid)?)?,
        &bytes,
    )?;
    save_root(tx, own, root)
}

pub(super) fn initialize_next(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    chat: &str,
    epoch: u64,
    count: Option<u32>,
) -> CryptoResult<Vec<u8>> {
    if count.is_none_or(|n| n > MAX_JOURNALS) {
        return Err(CryptoError::Invalid);
    }
    let h = Head {
        format: FORMAT,
        epoch: Some(epoch),
        own: None,
        peer: None,
        count,
        activation: None,
    };
    let bytes = h.encode_to_vec();
    tx.put(&head_key(own, chat, epoch)?, &bytes)?;
    Ok(head_hash(&bytes).to_vec())
}
