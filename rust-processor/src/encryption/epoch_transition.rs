//! Explicit accepted transition. Network catalogue and both retained signed intents
//! precede a bounded authenticated outbox audit. Activation is one final SQLite TX.
use super::*;
const STEP: usize = 32;
#[derive(Clone)]
pub struct EpochTransitionQuery {
    scope: EpochScope,
    pins: EpochConversation,
    accepted: v2::RecoveryRecord,
    request: v2::GetConversationEpochsRequest,
}
impl EpochTransitionQuery {
    pub fn request(&self) -> &v2::GetConversationEpochsRequest {
        &self.request
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope.scope
    }
    pub(in crate::encryption) fn pins(&self) -> &EpochConversation {
        &self.pins
    }
}
/// Read-only status. A zero remaining count is not itself an activation capability.
pub struct EpochTransitionProgress {
    from_epoch: u64,
    to_epoch: u64,
    remaining: u64,
    total: u64,
    blocked_message: Option<String>,
    activated: bool,
}
impl EpochTransitionProgress {
    pub fn from_epoch(&self) -> u64 {
        self.from_epoch
    }
    pub fn to_epoch(&self) -> u64 {
        self.to_epoch
    }
    pub fn remaining(&self) -> u64 {
        self.remaining
    }
    pub fn total(&self) -> u64 {
        self.total
    }
    pub fn blocked_message(&self) -> Option<&str> {
        self.blocked_message.as_deref()
    }
    pub fn activated(&self) -> bool {
        self.activated
    }
}
#[derive(Message)]
pub(super) struct Progress {
    #[prost(message, optional, tag = "1")]
    accepted: Option<v2::RecoveryRecord>,
    #[prost(message, optional, tag = "2")]
    catalog: Option<v2::ConversationEpochs>,
    #[prost(uint64, optional, tag = "3")]
    total: Option<u64>,
    #[prost(bytes = "vec", tag = "4")]
    tail: Vec<u8>,
    #[prost(uint64, optional, tag = "5")]
    next: Option<u64>,
    #[prost(bytes = "vec", tag = "6")]
    hash: Vec<u8>,
}
fn control(r: &v2::RecoveryRecord) -> CryptoResult<&common::RecoveryControl> {
    r.outcome
        .as_ref()
        .and_then(|o| o.proposal.as_ref())
        .and_then(|p| p.control.as_ref())
        .ok_or(CryptoError::Invalid)
}
pub(super) fn accepted_id(p: &Progress) -> CryptoResult<&str> {
    Ok(&control(p.accepted.as_ref().ok_or(CryptoError::Invalid)?)?.proposal_id)
}
fn status(
    p: &Progress,
    blocked_message: Option<String>,
    activated: bool,
) -> CryptoResult<EpochTransitionProgress> {
    let c = control(p.accepted.as_ref().ok_or(CryptoError::Invalid)?)?;
    Ok(EpochTransitionProgress {
        from_epoch: c.expected_epoch,
        to_epoch: c.expected_epoch + 1,
        remaining: p.next.ok_or(CryptoError::Invalid)?,
        total: p.total.ok_or(CryptoError::Invalid)?,
        blocked_message,
        activated,
    })
}
fn validate_catalogue(
    pins: &EpochConversation,
    accepted: &v2::RecoveryRecord,
    page: &v2::ConversationEpochs,
) -> CryptoResult<()> {
    let c = control(accepted)?;
    pins.verify_record(accepted, None)
        .map_err(|_| CryptoError::Invalid)?;
    let o = accepted.outcome.as_ref().ok_or(CryptoError::Invalid)?;
    epoch_rpc::validate_catalog(
        &archive::catalog_request(pins.own(), &c.conversation_id, c.expected_epoch, 2),
        page,
        pins,
    )
    .map_err(|_| CryptoError::Invalid)?;
    if o.state != common::RecoveryState::Accepted as i32
        || page.active_epoch != Some(o.new_epoch)
        || page.epoch_revision != Some(o.epoch_revision)
        || page.epochs.len() != 2
        || page.next_after_epoch.is_some()
    {
        return Err(CryptoError::Conflict);
    }
    let (old, new) = (&page.epochs[0], &page.epochs[1]);
    if old.epoch != Some(c.expected_epoch)
        || old.revision != Some(c.expected_revision)
        || !old.sealed
        || old.sealed_by_proposal_id != c.proposal_id
        || old.sealed_at_unix_ms != accepted.resolved_at_unix_ms
        || new.epoch != Some(o.new_epoch)
        || new.created_by_proposal_id != c.proposal_id
        || new.bootstrap_device != c.initiator.as_ref().and_then(|p| p.device.clone())
        || Some(new.created_at_unix_ms) != accepted.resolved_at_unix_ms
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
pub(super) fn validate_progress(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    root: &Root,
    pins: &EpochConversation,
    p: &Progress,
) -> CryptoResult<()> {
    let r = p.accepted.as_ref().ok_or(CryptoError::Invalid)?;
    require_accepted(tx, own, pins, r)?;
    let c = control(r)?;
    let page = p.catalog.as_ref().ok_or(CryptoError::Invalid)?;
    let historical = page.conversation.as_ref().ok_or(CryptoError::Invalid)?;
    // This protected snapshot is evidence of a past transition, not a fresh RPC
    // trying to reopen a subsequently closed chat or roll back published keys.
    conversation_lifecycle::merge(historical, pins.conversation())?;
    let historical_pins = super::super::pins(
        historical,
        IdentityKey::try_from(root.own_public.as_slice())?,
    )?;
    validate_catalogue(&historical_pins, r, page)?;
    chain::validate_tail(p.total, &p.tail)?;
    chain::validate_tail(p.next, &p.hash)?;
    if root.active_epoch != Some(c.expected_epoch)
        || root.revision != Some(c.expected_revision)
        || root.send_frozen != Some(true)
        || p.total != root.outgoing_count
        || p.tail != root.outgoing_head
        || p.next > p.total
        || (p.next == p.total && p.hash != p.tail)
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
fn same_transition_catalog(
    old: &v2::ConversationEpochs,
    new: &v2::ConversationEpochs,
) -> CryptoResult<()> {
    if old.protocol_version != new.protocol_version
        || old.active_epoch != new.active_epoch
        || old.epoch_revision != new.epoch_revision
        || old.epochs != new.epochs
        || old.next_after_epoch != new.next_after_epoch
    {
        return Err(CryptoError::Conflict);
    }
    conversation_lifecycle::merge(
        old.conversation.as_ref().ok_or(CryptoError::Invalid)?,
        new.conversation.as_ref().ok_or(CryptoError::Invalid)?,
    )?;
    Ok(())
}
fn eligible(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    id: &str,
) -> CryptoResult<(
    Loaded,
    EpochScope,
    EpochConversation,
    Head,
    v2::RecoveryRecord,
)> {
    let (l, s, pins) = loaded(tx, s)?;
    if s.receive_only {
        return Err(CryptoError::ConversationClosed);
    }
    if s.sealed {
        return Err(CryptoError::EpochRecoveryRequired);
    }
    let (h, _) = read_head(tx, &s.own, &l.root, &pins)?;
    if ![&h.own, &h.peer].into_iter().flatten().any(|r| r.id == id) {
        return Err(CryptoError::Conflict);
    }
    let j = load_journal(tx, &s.own, &pins, id)?.ok_or(CryptoError::Invalid)?;
    bound(&l.root, &j)?;
    if j.state() != Some(common::RecoveryState::Accepted) || !j.freezes() || !s.send_frozen {
        return Err(CryptoError::Conflict);
    }
    let r = j.observed.ok_or(CryptoError::Invalid)?;
    Ok((l, s, pins, h, r))
}
pub(in crate::encryption::signal_state) enum TransitionCommand {
    Prepare(Box<EpochScope>, String, Reply<EpochTransitionQuery>),
    Observe(
        Box<EpochObservation<EpochTransitionQuery, v2::ConversationEpochs>>,
        Reply<EpochTransitionProgress>,
    ),
    Advance(Box<EpochScope>, String, Reply<EpochTransitionProgress>),
}
impl Engine {
    pub(super) fn handle_epoch_transition(&mut self, c: TransitionCommand) {
        match c {
            TransitionCommand::Prepare(s, id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_epoch_transition(*s, &id));
                }
            }
            TransitionCommand::Observe(o, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_epoch_transition_catalog(*o));
                }
            }
            TransitionCommand::Advance(s, id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.advance_epoch_transition(*s, &id));
                }
            }
        }
    }
    fn prepare_epoch_transition(
        &mut self,
        s: EpochScope,
        id: &str,
    ) -> CryptoResult<EpochTransitionQuery> {
        self.check_epoch_owner(&s)?;
        self.store.transaction_with(|tx| {
            let (_, s, pins, _, accepted) = eligible(tx, &s, id)?;
            Ok(EpochTransitionQuery {
                request: archive::catalog_request(&s.own, &s.conversation, s.epoch, 2),
                scope: s,
                pins,
                accepted,
            })
        })
    }
    fn accept_epoch_transition_catalog(
        &mut self,
        o: EpochObservation<EpochTransitionQuery, v2::ConversationEpochs>,
    ) -> CryptoResult<EpochTransitionProgress> {
        let q = o.request();
        self.check_epoch_owner(&q.scope)?;
        if o.store_scope() != self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        let id = &control(&q.accepted)?.proposal_id;
        if q.request
            != archive::catalog_request(&q.scope.own, &q.scope.conversation, q.scope.epoch, 2)
        {
            return Err(CryptoError::Conflict);
        }
        // Even a post-COMMIT retry must carry a correctly bound catalogue.
        validate_catalogue(&q.pins, &q.accepted, o.response())?;
        self.store.transaction_with(|tx| {
            if let Some(done) = completed(tx, &q.scope, id, Some(o.response()))? {
                return Ok(done);
            }
            let (mut l, s, pins, mut h, accepted) = eligible(tx, &q.scope, id)?;
            if q.accepted != accepted
                || q.request != archive::catalog_request(&s.own, &s.conversation, s.epoch, 2)
            {
                return Err(CryptoError::Conflict);
            }
            validate_catalogue(&pins, &accepted, o.response())?;
            let mut unsealed = o.response().epochs[0].clone();
            unsealed.sealed = false;
            unsealed.sealed_by_proposal_id.clear();
            unsealed.sealed_at_unix_ms = None;
            if l.row.epoch.as_ref() != Some(&unsealed) {
                return Err(CryptoError::Conflict);
            }
            conversation_lifecycle::merge(
                l.root.conversation.as_ref().ok_or(CryptoError::Invalid)?,
                o.response()
                    .conversation
                    .as_ref()
                    .ok_or(CryptoError::Invalid)?,
            )?;
            if o.response()
                .conversation
                .as_ref()
                .is_none_or(|c| c.state != ConversationState::Active as i32)
            {
                return Err(CryptoError::ConversationClosed);
            }
            if let Some(p) = &h.activation {
                if p.accepted.as_ref() != Some(&accepted) {
                    return Err(CryptoError::Conflict);
                }
                same_transition_catalog(
                    p.catalog.as_ref().ok_or(CryptoError::Invalid)?,
                    o.response(),
                )?;
                return status(p, None, false);
            }
            let p = Progress {
                accepted: Some(accepted),
                catalog: Some(o.response().clone()),
                total: l.root.outgoing_count,
                tail: l.root.outgoing_head.clone(),
                next: l.root.outgoing_count,
                hash: l.root.outgoing_head.clone(),
            };
            let result = status(&p, None, false)?;
            h.activation = Some(p);
            save_head(tx, &s.own, &pins, &mut l.root, &h, true)?;
            Ok(result)
        })
    }
    fn advance_epoch_transition(
        &mut self,
        s: EpochScope,
        id: &str,
    ) -> CryptoResult<EpochTransitionProgress> {
        self.check_epoch_owner(&s)?;
        self.store.transaction_with(|tx| {
            if let Some(done) = completed(tx, &s, id, None)? {
                return Ok(done);
            }
            let (mut l, s, pins, mut h, accepted) = eligible(tx, &s, id)?;
            let mut p = h.activation.take().ok_or(CryptoError::Invalid)?;
            let previous_next = p.next;
            if p.accepted.as_ref() != Some(&accepted) {
                return Err(CryptoError::Conflict);
            }
            let mut blocked = None;
            for _ in 0..STEP {
                let n = p.next.ok_or(CryptoError::Invalid)?;
                if n == 0 {
                    break;
                }
                let entry = chain::read(tx, &s, n)?;
                if chain::hash(&entry).as_slice() != p.hash {
                    return Err(CryptoError::Conflict);
                }
                let record = outgoing::record_id(&s, &entry.message)?;
                let plan = outgoing::plan(tx, &s.own, &s.scope, &record)?;
                outgoing::check_projections(tx, &s.own, &record, plan.needs_status_poll())?;
                if plan.request().envelope.as_ref().is_none_or(|e| {
                    crate::encryption::epoch_contract::envelope_digest(e)
                        .ok()
                        .as_ref()
                        .map(|h| h.as_slice())
                        != Some(entry.envelope_digest.as_slice())
                }) {
                    return Err(CryptoError::Conflict);
                }
                if plan.needs_send() {
                    blocked = Some(entry.message);
                    break;
                }
                p.next = Some(n - 1);
                p.hash = entry.previous;
            }
            let done = p.next == Some(0);
            let result = status(&p, blocked, done)?;
            if !done && p.next == previous_next {
                return Ok(result);
            }
            h.activation = Some(p);
            save_head(tx, &s.own, &pins, &mut l.root, &h, true)?;
            if done {
                activate(tx, &s, &pins, l, h, &accepted)?;
            }
            Ok(result)
        })
    }
}
// Repeating a committed transition never requires reconstructing old permissions.
fn completed(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    id: &str,
    expected_catalog: Option<&v2::ConversationEpochs>,
) -> CryptoResult<Option<EpochTransitionProgress>> {
    let (l, actual, _) = loaded(tx, s)?;
    if !actual.sealed {
        return Ok(None);
    }
    if actual.retired_by.as_deref() != Some(id) {
        return Err(CryptoError::Conflict);
    }
    let (h, _) = read_head(
        tx,
        &s.own,
        &l.root,
        &pins(
            l.root.conversation.as_ref().ok_or(CryptoError::Invalid)?,
            s.own_identity,
        )?,
    )?;
    let p = h.activation.as_ref().ok_or(CryptoError::Invalid)?;
    if let Some(page) = expected_catalog {
        same_transition_catalog(p.catalog.as_ref().ok_or(CryptoError::Invalid)?, page)?;
    }
    if p.next != Some(0)
        || control(p.accepted.as_ref().ok_or(CryptoError::Invalid)?)?.proposal_id != id
    {
        return Err(CryptoError::Conflict);
    }
    Ok(Some(status(p, None, true)?))
}
fn activate(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    pins: &EpochConversation,
    mut l: Loaded,
    h: Head,
    accepted: &v2::RecoveryRecord,
) -> CryptoResult<()> {
    let p = h.activation.as_ref().ok_or(CryptoError::Invalid)?;
    validate_progress(tx, &s.own, &l.root, pins, p)?;
    if p.next != Some(0) || p.hash != chain::EMPTY {
        return Err(CryptoError::Conflict);
    }
    let catalog = p.catalog.as_ref().ok_or(CryptoError::Invalid)?;
    let new = catalog.epochs[1].clone();
    let next = new.epoch.ok_or(CryptoError::Invalid)?;
    reject_future_state(tx, s, next)?;
    l.row.epoch = Some(catalog.epochs[0].clone());
    l.row.sealed_by = Some(accepted.clone());
    l.row.outgoing_count = l.root.outgoing_count;
    l.row.outgoing_head = l.root.outgoing_head.clone();
    l.row.recovery_digest = l.root.recovery_digest.clone();
    archive::save(tx, &s.own, &l.row)?;
    let row = EpochRow {
        format: FORMAT,
        conversation_id: s.conversation.clone(),
        own: Some(s.own.clone()),
        peer: Some(s.peer.clone()),
        epoch: Some(new),
        created_by: Some(accepted.clone()),
        sealed_by: None,
        outgoing_count: None,
        outgoing_head: vec![],
        recovery_digest: vec![],
    };
    archive::save(tx, &s.own, &row)?;
    l.root.active_epoch = Some(next);
    l.root.revision = row.epoch.as_ref().and_then(|e| e.revision);
    l.root.row_digest = digest(b"zchat.e2ee.epoch-row.v2\0", &row.encode_to_vec()).to_vec();
    l.root.outgoing_count = Some(0);
    l.root.outgoing_head = chain::EMPTY.to_vec();
    l.root.send_frozen = Some(false);
    l.root.recovery_digest = initialize_next(tx, &s.own, &s.conversation, next, h.count)?;
    l.root.conversation = Some(conversation_lifecycle::merge(
        l.root.conversation.as_ref().ok_or(CryptoError::Invalid)?,
        catalog.conversation.as_ref().ok_or(CryptoError::Invalid)?,
    )?);
    save_root(tx, &s.own, &mut l.root)
}
fn reject_future_state(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    next: u64,
) -> CryptoResult<()> {
    for suffix in [
        format!("row:{next:016x}"),
        format!("row-anchor:{next:016x}"),
        format!("recovery-head:{next:016x}"),
        format!("acquire:{next:016x}"),
        format!("traffic:{next:016x}"),
        format!("quarantine-traffic:{next:016x}"),
    ] {
        if tx.get(&key(&s.own, &s.conversation, &suffix)?)?.is_some() {
            return Err(CryptoError::Conflict);
        }
    }
    let prefix = format!("v2:{}:{next:016x}:", s.conversation);
    for kind in [
        RecordKind::Session,
        RecordKind::Acquisition,
        RecordKind::History,
        RecordKind::Outbox,
        RecordKind::Inbox,
    ] {
        if tx.has_record_prefix(&s.own.generation_id, kind, &prefix)? {
            return Err(CryptoError::Conflict);
        }
    }
    for p in [
        format!("epoch-v2:{}:outgoing-chain:{next:016x}:", s.conversation),
        format!("delivery-out:{prefix}"),
        format!("delivery-in:{prefix}"),
        format!("delivery-progress:{prefix}"),
        format!("delivery-acked:{prefix}"),
        format!("delivery-sealed:{prefix}"),
        format!("session-confirmation:{prefix}"),
    ] {
        if tx.has_record_prefix(&s.own.generation_id, RecordKind::Control, &p)? {
            return Err(CryptoError::Conflict);
        }
    }
    Ok(())
}
impl CryptoCoordinator {
    pub async fn prepare_epoch_transition(
        &self,
        s: EpochScope,
        id: String,
    ) -> CryptoResult<EpochTransitionQuery> {
        if !epoch_rpc::uuid4(&id) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Transition(
                TransitionCommand::Prepare(Box::new(s), id, r),
            )))
        })
        .await
    }
    pub async fn accept_epoch_transition_catalog(
        &self,
        o: EpochObservation<EpochTransitionQuery, v2::ConversationEpochs>,
    ) -> CryptoResult<EpochTransitionProgress> {
        if o.response().encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Transition(
                TransitionCommand::Observe(Box::new(o), r),
            )))
        })
        .await
    }
    pub async fn advance_epoch_transition(
        &self,
        s: EpochScope,
        id: String,
    ) -> CryptoResult<EpochTransitionProgress> {
        if !epoch_rpc::uuid4(&id) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Recovery(RecoveryCommand::Transition(
                TransitionCommand::Advance(Box::new(s), id, r),
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_transition_tests.rs"]
mod tests;
