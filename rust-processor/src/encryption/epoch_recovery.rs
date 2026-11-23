//! Signed recovery intent/decision lane. Exact bytes and send freeze precede RPC;
//! an accepted observation is retained before the separate epoch activation step.
use super::*;
use crate::{encryption::recovery_contract as control, proto::e2ee as common};
#[path = "epoch_recovery_state.rs"]
mod state;
#[path = "epoch_transition.rs"]
mod transition;
use state::*;
#[path = "epoch_recovery_view.rs"]
mod view;
pub(super) use state::{initialize, validate_head};
pub use transition::{EpochTransitionProgress, EpochTransitionQuery};
pub use view::EpochRecoveryOverview;

/// Only an explicit trusted-owner command may select a decision. Observing a
/// proposal never implies consent. A pending decision cannot be silently changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochRecoveryDecision {
    Accept,
    Decline,
    Cancel,
}
impl EpochRecoveryDecision {
    fn action(self) -> common::RecoveryAction {
        match self {
            Self::Accept => common::RecoveryAction::Accept,
            Self::Decline => common::RecoveryAction::Decline,
            Self::Cancel => common::RecoveryAction::Cancel,
        }
    }
}
#[derive(Clone)]
pub struct EpochRecoveryIntent {
    scope: EpochScope,
    pins: EpochConversation,
    proposal: common::SignedRecoveryControl,
    request: v2::RecoveryCommandRequest,
}
impl EpochRecoveryIntent {
    pub fn request(&self) -> &v2::RecoveryCommandRequest {
        &self.request
    }
    pub fn proposal(&self) -> &common::SignedRecoveryControl {
        &self.proposal
    }
    pub(in crate::encryption) fn pins(&self) -> &EpochConversation {
        &self.pins
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope.scope
    }
}
#[derive(Clone)]
pub struct EpochRecoveryQuery {
    scope: EpochScope,
    pins: EpochConversation,
    expected: Option<common::SignedRecoveryControl>,
    request: v2::GetEpochRecoveryRequest,
}
impl EpochRecoveryQuery {
    pub fn request(&self) -> &v2::GetEpochRecoveryRequest {
        &self.request
    }
    pub(in crate::encryption) fn expected(&self) -> Option<&common::SignedRecoveryControl> {
        self.expected.as_ref()
    }
    pub(in crate::encryption) fn pins(&self) -> &EpochConversation {
        &self.pins
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope.scope
    }
}
/// Read-only local status, never a transition/consent capability.
pub struct EpochRecoverySnapshot {
    proposal_id: String,
    state: Option<common::RecoveryState>,
    own_proposal: bool,
    decision: Option<EpochRecoveryDecision>,
    frozen: bool,
    retired_by: Option<String>,
}
impl EpochRecoverySnapshot {
    pub fn proposal_id(&self) -> &str {
        &self.proposal_id
    }
    pub fn state(&self) -> Option<common::RecoveryState> {
        self.state
    }
    pub fn own_proposal(&self) -> bool {
        self.own_proposal
    }
    pub fn decision(&self) -> Option<EpochRecoveryDecision> {
        self.decision
    }
    pub fn retired_by(&self) -> Option<&str> {
        self.retired_by.as_deref()
    }
    pub fn sending_frozen(&self) -> bool {
        self.frozen
    }
}
fn snapshot(
    j: &Journal,
    root: &Root,
    retired_by: Option<&str>,
) -> CryptoResult<EpochRecoverySnapshot> {
    let decision = j
        .decision
        .as_ref()
        .and_then(|d| d.control.as_ref())
        .map(|c| match common::RecoveryAction::try_from(c.action) {
            Ok(common::RecoveryAction::Accept) => Ok(EpochRecoveryDecision::Accept),
            Ok(common::RecoveryAction::Decline) => Ok(EpochRecoveryDecision::Decline),
            Ok(common::RecoveryAction::Cancel) => Ok(EpochRecoveryDecision::Cancel),
            _ => Err(CryptoError::Invalid),
        })
        .transpose()?;
    Ok(EpochRecoverySnapshot {
        proposal_id: j.control()?.proposal_id.clone(),
        state: j.state(),
        own_proposal: j.own_intent == Some(true),
        decision,
        frozen: root.send_frozen.ok_or(CryptoError::Invalid)?,
        retired_by: retired_by.map(str::to_owned),
    })
}
fn context(own: &DeviceRef) -> ClientContext {
    ClientContext {
        protocol_version: 2,
        device_generation_id: own.generation_id.clone(),
    }
}
fn loaded(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
) -> CryptoResult<(Loaded, EpochScope, EpochConversation)> {
    let s = bootstrap::current(tx, s)?;
    recovery_indexes::require_ready(tx, &s.own)?;
    let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    let (public, registration) = identity(tx, &s.own)?;
    let l = archive::load_at(tx, &s.own, &c, public, registration, s.epoch)?;
    let pins = pins(&c, public)?;
    Ok((l, s, pins))
}
fn sign(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    c: common::RecoveryControl,
) -> CryptoResult<common::SignedRecoveryControl> {
    let bytes = tx
        .get(&RecordKey::new(
            &s.own.generation_id,
            RecordKind::Identity,
            "local",
        )?)?
        .ok_or(CryptoError::Invalid)?;
    if bytes.len() < 6 || bytes[0] != 1 {
        return Err(CryptoError::Invalid);
    }
    let pair = IdentityKeyPair::try_from(&bytes[5..])?;
    if pair.identity_key() != &s.own_identity {
        return Err(CryptoError::Conflict);
    }
    control::sign_control(c, &pair).map_err(|_| CryptoError::Invalid)
}
fn intent(
    s: EpochScope,
    pins: EpochConversation,
    j: &Journal,
    decision: bool,
) -> CryptoResult<EpochRecoveryIntent> {
    let p = j.proposal()?.clone();
    let command = if decision {
        j.decision.clone().ok_or(CryptoError::Invalid)?
    } else {
        p.clone()
    };
    pins.verify_command(&p, &command, !decision)
        .map_err(|_| CryptoError::Invalid)?;
    Ok(EpochRecoveryIntent {
        request: v2::RecoveryCommandRequest {
            context: Some(context(&s.own)),
            command: Some(command),
        },
        scope: s,
        pins,
        proposal: p,
    })
}
pub(super) fn require_accepted_outcome(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    pins: &EpochConversation,
    o: &common::RecoveryOutcome,
) -> CryptoResult<v2::RecoveryRecord> {
    let id = &o
        .proposal
        .as_ref()
        .and_then(|p| p.control.as_ref())
        .ok_or(CryptoError::Invalid)?
        .proposal_id;
    let j = load_journal(tx, own, pins, id)?.ok_or(CryptoError::Invalid)?;
    if j.state() != Some(common::RecoveryState::Accepted)
        || j.observed.as_ref().and_then(|r| r.outcome.as_ref()) != Some(o)
    {
        return Err(CryptoError::Conflict);
    }
    j.observed.ok_or(CryptoError::Invalid)
}
pub(super) fn require_accepted(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    pins: &EpochConversation,
    r: &v2::RecoveryRecord,
) -> CryptoResult<()> {
    let o = r.outcome.as_ref().ok_or(CryptoError::Invalid)?;
    let id = &o
        .proposal
        .as_ref()
        .and_then(|p| p.control.as_ref())
        .ok_or(CryptoError::Invalid)?
        .proposal_id;
    let j = load_journal(tx, own, pins, id)?.ok_or(CryptoError::Invalid)?;
    if j.state() != Some(common::RecoveryState::Accepted) || j.observed.as_ref() != Some(r) {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
fn bound(root: &Root, j: &Journal) -> CryptoResult<()> {
    if Some(j.control()?.expected_epoch) != root.active_epoch
        || Some(j.control()?.expected_revision) != root.revision
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}

fn merge_record(j: &mut Journal, r: &v2::RecoveryRecord, own: &DeviceRef) -> CryptoResult<bool> {
    check_local_consent(j, r, own)?;
    if let Some(old) = &j.observed {
        if old.created_at_unix_ms != r.created_at_unix_ms
            || old.expires_at_unix_ms != r.expires_at_unix_ms
        {
            return Err(CryptoError::Conflict);
        }
        if old == r {
            return Ok(false);
        }
        if j.terminal() {
            if r.outcome
                .as_ref()
                .is_some_and(|o| o.state == common::RecoveryState::Pending as i32)
            {
                return Ok(false);
            }
            return Err(CryptoError::Conflict);
        }
    }
    j.observed = Some(r.clone());
    Ok(true)
}

pub(in crate::encryption::signal_state) enum RecoveryCommand {
    Resume(Box<EpochScope>, String, Reply<Option<EpochRecoveryIntent>>),
    Transition(transition::TransitionCommand),
    Current(Box<EpochScope>, Reply<Vec<EpochRecoverySnapshot>>),
    Overview(String, Reply<EpochRecoveryOverview>),
    ProposeChecked(Box<EpochScope>, Option<String>, Reply<EpochRecoveryIntent>),
    Propose(Box<EpochScope>, Reply<EpochRecoveryIntent>),
    Decide(
        Box<EpochScope>,
        String,
        EpochRecoveryDecision,
        Reply<EpochRecoveryIntent>,
    ),
    Query(Box<EpochScope>, String, Reply<EpochRecoveryQuery>),
    ObserveCommand(
        Box<EpochObservation<EpochRecoveryIntent, v2::RecoveryRecord>>,
        Reply<EpochRecoverySnapshot>,
    ),
    ObserveQuery(
        Box<EpochObservation<EpochRecoveryQuery, v2::RecoveryRecord>>,
        Reply<EpochRecoverySnapshot>,
    ),
    Snapshot(Box<EpochScope>, String, Reply<EpochRecoverySnapshot>),
}
impl Engine {
    pub(super) fn handle_epoch_recovery(&mut self, c: RecoveryCommand) {
        match c {
            RecoveryCommand::Resume(s, id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.resume_epoch_recovery(*s, &id));
                }
            }
            RecoveryCommand::Transition(c) => self.handle_epoch_transition(c),
            RecoveryCommand::Current(s, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.current_epoch_recoveries(*s));
                }
            }
            RecoveryCommand::Overview(chat, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_recovery_overview(&chat));
                }
            }
            RecoveryCommand::ProposeChecked(s, own, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_epoch_recovery(*s, Some(own)));
                }
            }
            RecoveryCommand::Propose(s, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_epoch_recovery(*s, None));
                }
            }
            RecoveryCommand::Decide(s, id, d, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_epoch_recovery_decision(*s, &id, d));
                }
            }
            RecoveryCommand::Query(s, id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_epoch_recovery_query(*s, &id));
                }
            }
            RecoveryCommand::ObserveCommand(o, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_epoch_recovery_result(*o));
                }
            }
            RecoveryCommand::ObserveQuery(o, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_epoch_recovery_query(*o));
                }
            }
            RecoveryCommand::Snapshot(s, id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_recovery_snapshot(*s, &id));
                }
            }
        }
    }
    fn resume_epoch_recovery(
        &mut self,
        s: EpochScope,
        id: &str,
    ) -> CryptoResult<Option<EpochRecoveryIntent>> {
        self.check_epoch_owner(&s)?;
        self.store.transaction_with(|tx| {
            // Resume is strictly read-only, including incomplete/corrupt stores.
            // Do not let the shared explicit-operation helper seed empty indexes.
            if !recovery_indexes::ready(tx, &s.own)? {
                return Err(CryptoError::RecoveryIndexesNotReady);
            }
            let (l, s, pins) = loaded(tx, &s)?;
            let (h, _) = read_head(tx, &s.own, &l.root, &pins)?;
            if ![&h.own, &h.peer].into_iter().flatten().any(|r| r.id == id) {
                return Err(CryptoError::Conflict);
            }
            let j = load_journal(tx, &s.own, &pins, id)?.ok_or(CryptoError::Invalid)?;
            bound(&l.root, &j)?;
            if j.terminal() || s.sealed || (j.own_intent != Some(true) && j.decision.is_none()) {
                return Ok(None);
            }
            // An own Cancel can commit while Propose is still offline/unknown.
            // Reconcile the saved proposal FIRST; sending Decide to a missing
            // server record would otherwise retry forever. Both signed commands
            // already exist; this read never creates or replaces either intent.
            let decision =
                j.decision.is_some() && (j.own_intent != Some(true) || j.observed.is_some());
            intent(s, pins, &j, decision).map(Some)
        })
    }
    fn current_epoch_recoveries(
        &mut self,
        s: EpochScope,
    ) -> CryptoResult<Vec<EpochRecoverySnapshot>> {
        self.check_epoch_owner(&s)?;
        self.store.transaction_with(|tx| {
            let (l, s, pins) = loaded(tx, &s)?;
            let (head, _) = read_head(tx, &s.own, &l.root, &pins)?;
            [&head.own, &head.peer]
                .into_iter()
                .flatten()
                .map(|r| {
                    let j = load_journal(tx, &s.own, &pins, &r.id)?.ok_or(CryptoError::Invalid)?;
                    bound(&l.root, &j)?;
                    snapshot(&j, &l.root, s.retired_by.as_deref())
                })
                .collect()
        })
    }
    fn prepare_epoch_recovery(
        &mut self,
        s: EpochScope,
        expected_own: Option<Option<String>>,
    ) -> CryptoResult<EpochRecoveryIntent> {
        self.check_epoch_owner(&s)?;
        self.store.transaction_with(|tx| {
            let (mut l, s, pins) = loaded(tx, &s)?;
            if s.sealed {
                return Err(CryptoError::EpochRecoveryRequired);
            }
            let (mut h, frozen) = read_head(tx, &s.own, &l.root, &pins)?;
            // Compare inside the signing/freeze transaction. An old renderer
            // intent must not create another proposal after a previous one ended.
            if expected_own.as_ref().is_some_and(|expected| {
                expected.as_deref() != h.own.as_ref().map(|r| r.id.as_str())
            }) {
                return Err(CryptoError::Conflict);
            }
            if let Some(r) = &h.own {
                let j = load_journal(tx, &s.own, &pins, &r.id)?.ok_or(CryptoError::Invalid)?;
                if !j.terminal() || j.freezes() {
                    return intent(s, pins, &j, false);
                }
            }
            bootstrap::sending(&s)?;
            let id = uuid::Uuid::new_v4().to_string();
            let participant =
                |device: &DeviceRef, identity: &IdentityKey| common::RecoveryParticipant {
                    device: Some(device.clone()),
                    identity_fingerprint: control::identity_fingerprint(identity).to_vec(),
                };
            let c = common::RecoveryControl {
                protocol_version: 2,
                format_version: 1,
                crypto_suite: 1,
                action: common::RecoveryAction::Propose as i32,
                conversation_id: s.conversation.clone(),
                request_id: id.clone(),
                proposal_id: id,
                expected_epoch: s.epoch,
                expected_revision: l.root.revision.ok_or(CryptoError::Invalid)?,
                initiator: Some(participant(&s.own, &s.own_identity)),
                peer: Some(participant(&s.peer, &s.peer_identity)),
                proposal_digest: vec![],
            };
            let mut j = Journal::new(sign(tx, &s, c)?, true);
            save_journal(tx, &s.own, &s.conversation, &mut h, &mut j, true)?;
            save_head(tx, &s.own, &pins, &mut l.root, &h, frozen)?;
            intent(s, pins, &j, false)
        })
    }
    fn prepare_epoch_recovery_decision(
        &mut self,
        s: EpochScope,
        id: &str,
        decision: EpochRecoveryDecision,
    ) -> CryptoResult<EpochRecoveryIntent> {
        self.check_epoch_owner(&s)?;
        self.store.transaction_with(|tx| {
            let (mut l, s, pins) = loaded(tx, &s)?;
            if s.sealed {
                return Err(CryptoError::EpochRecoveryRequired);
            }
            let (mut h, frozen) = read_head(tx, &s.own, &l.root, &pins)?;
            let mut j = load_journal(tx, &s.own, &pins, id)?.ok_or(CryptoError::Invalid)?;
            bound(&l.root, &j)?;
            let reference = if j.own_intent == Some(true) {
                &h.own
            } else {
                &h.peer
            };
            if reference.as_ref().is_none_or(|r| r.id != id) {
                return Err(CryptoError::Conflict);
            }
            if let Some(d) = &j.decision {
                if d.control
                    .as_ref()
                    .is_none_or(|c| c.action != decision.action() as i32)
                {
                    return Err(CryptoError::Conflict);
                }
                return intent(s, pins, &j, true);
            }
            if j.terminal() || s.receive_only || (s.send_frozen && !frozen) {
                return Err(CryptoError::Conflict);
            }
            if (decision == EpochRecoveryDecision::Cancel) != (j.own_intent == Some(true)) {
                return Err(CryptoError::Invalid);
            }
            if j.own_intent != Some(true) && j.state() != Some(common::RecoveryState::Pending) {
                return Err(CryptoError::Conflict);
            }
            let mut c = j.control()?.clone();
            c.action = decision.action() as i32;
            c.request_id = uuid::Uuid::new_v4().to_string();
            c.proposal_digest = control::control_digest(j.control()?)
                .map_err(|_| CryptoError::Invalid)?
                .to_vec();
            j.decision = Some(sign(tx, &s, c)?);
            save_journal(tx, &s.own, &s.conversation, &mut h, &mut j, false)?;
            save_head(tx, &s.own, &pins, &mut l.root, &h, frozen)?;
            intent(s, pins, &j, true)
        })
    }
    fn prepare_epoch_recovery_query(
        &mut self,
        s: EpochScope,
        id: &str,
    ) -> CryptoResult<EpochRecoveryQuery> {
        self.check_epoch_owner(&s)?;
        if !epoch_rpc::uuid4(id) {
            return Err(CryptoError::Invalid);
        }
        self.store.transaction_with(|tx| {
            let (_, s, pins) = loaded(tx, &s)?;
            let expected = load_journal(tx, &s.own, &pins, id)?
                .map(|j| j.proposal().cloned())
                .transpose()?;
            Ok(EpochRecoveryQuery {
                request: v2::GetEpochRecoveryRequest {
                    context: Some(context(&s.own)),
                    conversation_id: s.conversation.clone(),
                    proposal_id: id.into(),
                },
                scope: s,
                pins,
                expected,
            })
        })
    }
    fn accept_epoch_recovery_result(
        &mut self,
        o: EpochObservation<EpochRecoveryIntent, v2::RecoveryRecord>,
    ) -> CryptoResult<EpochRecoverySnapshot> {
        let p = o.request();
        self.check_epoch_owner(&p.scope)?;
        if o.store_scope() != self.store.scope() || p.request.context != Some(context(&self.own)) {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            let (_, s, pins) = loaded(tx, &p.scope)?;
            let id = p
                .proposal
                .control
                .as_ref()
                .ok_or(CryptoError::Invalid)?
                .proposal_id
                .as_str();
            let j = load_journal(tx, &s.own, &pins, id)?.ok_or(CryptoError::Invalid)?;
            if j.proposal()? != &p.proposal
                || (p.request.command.as_ref() != j.proposal.as_ref()
                    && p.request.command.as_ref() != j.decision.as_ref())
            {
                return Err(CryptoError::Conflict);
            }
            observe(tx, &s, &pins, id, Some(&p.proposal), o.response())
        })
    }
    fn accept_epoch_recovery_query(
        &mut self,
        o: EpochObservation<EpochRecoveryQuery, v2::RecoveryRecord>,
    ) -> CryptoResult<EpochRecoverySnapshot> {
        let p = o.request();
        self.check_epoch_owner(&p.scope)?;
        if o.store_scope() != self.store.scope()
            || p.request.context != Some(context(&self.own))
            || p.request.conversation_id != p.scope.conversation
        {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            let (_, s, pins) = loaded(tx, &p.scope)?;
            observe(
                tx,
                &s,
                &pins,
                &p.request.proposal_id,
                p.expected.as_ref(),
                o.response(),
            )
        })
    }
    fn epoch_recovery_snapshot(
        &mut self,
        s: EpochScope,
        id: &str,
    ) -> CryptoResult<EpochRecoverySnapshot> {
        self.check_epoch_owner(&s)?;
        self.store.transaction_with(|tx| {
            let (l, s, pins) = loaded(tx, &s)?;
            let j = load_journal(tx, &s.own, &pins, id)?.ok_or(CryptoError::Invalid)?;
            bound(&l.root, &j)?;
            snapshot(&j, &l.root, s.retired_by.as_deref())
        })
    }
}
fn observe(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    pins: &EpochConversation,
    id: &str,
    expected: Option<&common::SignedRecoveryControl>,
    r: &v2::RecoveryRecord,
) -> CryptoResult<EpochRecoverySnapshot> {
    if r.encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES {
        return Err(CryptoError::Invalid);
    }
    pins.verify_record(r, expected)
        .map_err(|_| CryptoError::Invalid)?;
    let p = r
        .outcome
        .as_ref()
        .and_then(|o| o.proposal.as_ref())
        .ok_or(CryptoError::Invalid)?;
    if p.control.as_ref().is_none_or(|c| c.proposal_id != id) {
        return Err(CryptoError::Conflict);
    }
    let (mut l, _, _) = loaded(tx, s)?;
    let (mut h, frozen) = read_head(tx, &s.own, &l.root, pins)?;
    let saved = load_journal(tx, &s.own, pins, id)?;
    let fresh = saved.is_none();
    let mut j = if let Some(j) = saved {
        j
    } else {
        // Local proposals and acceptance signatures cannot be adopted from RPC.
        if p.control
            .as_ref()
            .and_then(|c| c.initiator.as_ref())
            .and_then(|p| p.device.as_ref())
            == Some(&s.own)
        {
            return Err(CryptoError::Conflict);
        }
        Journal::new(p.clone(), false)
    };
    if j.proposal()? != p {
        return Err(CryptoError::Conflict);
    }
    bound(&l.root, &j)?;
    if s.sealed {
        if fresh {
            return Err(CryptoError::Conflict);
        }
        check_local_consent(&j, r, &s.own)?;
        if r.outcome
            .as_ref()
            .is_some_and(|o| o.state == common::RecoveryState::Accepted as i32)
            && (s.retired_by.as_deref() != Some(id) || j.observed.as_ref() != Some(r))
        {
            return Err(CryptoError::Conflict);
        }
        return snapshot(&j, &l.root, s.retired_by.as_deref());
    }
    if !merge_record(&mut j, r, &s.own)? {
        return snapshot(&j, &l.root, s.retired_by.as_deref());
    }
    let reference = if j.own_intent == Some(true) {
        &h.own
    } else {
        &h.peer
    };
    if let Some(old) = reference
        && old.id != id
    {
        let old = load_journal(tx, &s.own, pins, &old.id)?.ok_or(CryptoError::Invalid)?;
        if !old.terminal() || old.freezes() || !fresh {
            return Err(CryptoError::Conflict);
        }
    }
    save_journal(tx, &s.own, &s.conversation, &mut h, &mut j, fresh)?;
    save_head(tx, &s.own, pins, &mut l.root, &h, frozen)?;
    snapshot(&j, &l.root, s.retired_by.as_deref())
}

impl CryptoCoordinator {
    /// Reload only previously committed bytes. This command never signs, creates
    /// a proposal/decision, changes freeze, or adopts an external own signature.
    pub async fn resume_epoch_recovery(
        &self,
        s: EpochScope,
        id: String,
    ) -> CryptoResult<Option<EpochRecoveryIntent>> {
        if !epoch_rpc::uuid4(&id) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Resume(
                Box::new(s),
                id,
                r,
            )))
        })
        .await
    }
    /// At most the current own and peer proposal. Reconnect can recover their
    /// exact IDs/decisions without scanning retained journals or message history.
    pub async fn current_epoch_recoveries(
        &self,
        s: EpochScope,
    ) -> CryptoResult<Vec<EpochRecoverySnapshot>> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Current(
                Box::new(s),
                r,
            )))
        })
        .await
    }
    /// Current owner/peer references and root flags in ONE protected read TX.
    /// No bootstrap or network lookup when a known dialog has no epoch yet.
    pub async fn epoch_recovery_overview(
        &self,
        chat: String,
    ) -> CryptoResult<EpochRecoveryOverview> {
        prefix(&chat)?;
        self.request(|r| Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Overview(chat, r))))
            .await
    }
    /// Conditional explicit proposal. A conflict requires a new observation and
    /// user intent; never retry by silently replacing the expected reference.
    pub async fn prepare_epoch_recovery_if(
        &self,
        s: EpochScope,
        expected_own: Option<String>,
    ) -> CryptoResult<EpochRecoveryIntent> {
        if expected_own
            .as_deref()
            .is_some_and(|id| !epoch_rpc::uuid4(id))
        {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::ProposeChecked(
                Box::new(s),
                expected_own,
                r,
            )))
        })
        .await
    }
    pub async fn prepare_epoch_recovery(&self, s: EpochScope) -> CryptoResult<EpochRecoveryIntent> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Propose(
                Box::new(s),
                r,
            )))
        })
        .await
    }
    pub async fn prepare_epoch_recovery_decision(
        &self,
        s: EpochScope,
        id: String,
        d: EpochRecoveryDecision,
    ) -> CryptoResult<EpochRecoveryIntent> {
        if !epoch_rpc::uuid4(&id) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Decide(
                Box::new(s),
                id,
                d,
                r,
            )))
        })
        .await
    }
    pub async fn prepare_epoch_recovery_query(
        &self,
        s: EpochScope,
        id: String,
    ) -> CryptoResult<EpochRecoveryQuery> {
        if !epoch_rpc::uuid4(&id) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Query(
                Box::new(s),
                id,
                r,
            )))
        })
        .await
    }
    pub async fn accept_epoch_recovery_result(
        &self,
        o: EpochObservation<EpochRecoveryIntent, v2::RecoveryRecord>,
    ) -> CryptoResult<EpochRecoverySnapshot> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::ObserveCommand(
                Box::new(o),
                r,
            )))
        })
        .await
    }
    pub async fn accept_epoch_recovery_query(
        &self,
        o: EpochObservation<EpochRecoveryQuery, v2::RecoveryRecord>,
    ) -> CryptoResult<EpochRecoverySnapshot> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::ObserveQuery(
                Box::new(o),
                r,
            )))
        })
        .await
    }
    pub async fn epoch_recovery_snapshot(
        &self,
        s: EpochScope,
        id: String,
    ) -> CryptoResult<EpochRecoverySnapshot> {
        if !epoch_rpc::uuid4(&id) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Snapshot(
                Box::new(s),
                id,
                r,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_recovery_tests.rs"]
mod tests;
