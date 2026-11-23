//! Durable v2 bootstrap on the existing account lane. No network in a TX and
//! no public raw encrypt/decrypt: those require atomic v2 delivery journals.
use super::*;

#[path = "epoch_renewal.rs"]
mod renewal;

/// A request committed before RPC, bound to one protected account/epoch. Only
/// the owner can mint it. Applied means reuse the ratchet, never bootstrap again.
#[derive(Clone)]
pub struct EpochAcquisitionPlan {
    scope: EpochScope,
    pins: EpochConversation,
    request: v2::AcquireBundleRequest,
    applied: bool,
}
impl EpochAcquisitionPlan {
    pub fn request(&self) -> &v2::AcquireBundleRequest {
        &self.request
    }
    pub fn applied(&self) -> bool {
        self.applied
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope.scope
    }
    pub(in crate::encryption) fn pins(&self) -> &EpochConversation {
        &self.pins
    }
}

#[derive(Message)]
struct Journal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    request: Option<v2::AcquireBundleRequest>,
    #[prost(bytes = "vec", tag = "3")]
    peer_identity: Vec<u8>,
    #[prost(uint32, tag = "4")]
    peer_registration: u32,
    #[prost(message, optional, tag = "5")]
    applied: Option<v2::AcquiredBundle>,
    // Future data mutations must set this in their SAME transaction. It cannot
    // be inferred from a negative pending page or cleared when delivery settles.
    #[prost(bool, optional, tag = "6")]
    has_message_state: Option<bool>,
    // Immediate predecessor only; exact retries return the current nonce. This
    // never imports a prototype request or changes a crypto epoch/session slot.
    #[prost(message, optional, tag = "7")]
    replaces_request: Option<v2::AcquireBundleRequest>,
}
fn journal_key(s: &EpochScope) -> CryptoResult<RecordKey> {
    Ok(RecordKey::new(
        &s.own.generation_id,
        RecordKind::Acquisition,
        &s.acquisition_id,
    )?)
}
fn anchor_key(s: &EpochScope) -> CryptoResult<RecordKey> {
    key(
        &s.own,
        &s.conversation,
        &format!("acquire:{:016x}", s.epoch),
    )
}
fn save(tx: &mut ProtectedTransaction<'_>, s: &EpochScope, j: &Journal) -> CryptoResult<()> {
    let bytes = j.encode_to_vec();
    tx.put(&journal_key(s)?, &bytes)?;
    tx.put(
        &anchor_key(s)?,
        &digest(b"zchat.e2ee.acquisition-anchor.v2\0", &bytes),
    )?;
    Ok(())
}
fn read(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    pins: &EpochConversation,
) -> CryptoResult<Option<Journal>> {
    let bytes = tx.get(&journal_key(s)?)?;
    let anchor = tx.get(&anchor_key(s)?)?;
    let (bytes, anchor) = match (bytes, anchor) {
        (None, None) => return Ok(None),
        (Some(b), Some(a)) => (b, a),
        _ => return Err(CryptoError::Invalid),
    };
    if bytes.len() > 12288
        || anchor.as_slice() != digest(b"zchat.e2ee.acquisition-anchor.v2\0", &bytes)
    {
        return Err(CryptoError::Invalid);
    }
    let j = Journal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let r = j.request.as_ref().ok_or(CryptoError::Invalid)?;
    if j.format != FORMAT
        || j.has_message_state.is_none()
        || j.peer_identity != s.peer_identity.serialize().as_ref()
        || j.peer_registration != s.peer_registration
        || r.crypto_epoch != Some(s.epoch)
    {
        return Err(CryptoError::Conflict);
    }
    pins.check_acquire(r).map_err(|_| CryptoError::Invalid)?;
    if let Some(previous) = &j.replaces_request {
        pins.check_acquire(previous)
            .map_err(|_| CryptoError::Invalid)?;
        let mut same = previous.clone();
        same.acquisition_id = r.acquisition_id.clone();
        if previous.acquisition_id == r.acquisition_id || same != *r {
            return Err(CryptoError::Conflict);
        }
    }
    if j.has_message_state == Some(true) && !traffic_seen(tx, s)? && !quarantine_seen(tx, s)? {
        return Err(CryptoError::Invalid);
    }
    if let Some(b) = &j.applied {
        pins.verify_bundle(r, b).map_err(|_| CryptoError::Invalid)?;
    }
    Ok(Some(j))
}

// Always reload the root, identity and common closure within the mutation TX.
// Scope flags cached before a later freeze/closure grant no authority.
pub(super) fn current(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
) -> CryptoResult<EpochScope> {
    let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    let (own_public, registration) = identity(tx, &s.own)?;
    let state = archive::load_at(tx, &s.own, &c, own_public, registration, s.epoch)?;
    let actual = scope(&state, &c, s.scope.clone())?;
    if s.epoch != actual.epoch
        || s.own != actual.own
        || s.peer != actual.peer
        || s.own_identity != actual.own_identity
        || s.peer_identity != actual.peer_identity
        || s.peer_registration != actual.peer_registration
        || s.session_id != actual.session_id
        || s.acquisition_id != actual.acquisition_id
    {
        return Err(CryptoError::Conflict);
    }
    Ok(actual)
}
pub(super) fn sending(s: &EpochScope) -> CryptoResult<()> {
    if s.receive_only {
        return Err(CryptoError::ConversationClosed);
    }
    if s.send_frozen || s.sealed {
        return Err(CryptoError::EpochSendingFrozen);
    }
    Ok(())
}

fn outbound_bootstrap(s: &EpochScope) -> CryptoResult<()> {
    sending(s)?;
    if s.epoch > 0 && s.bootstrap_device.as_ref() != Some(&s.own) {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}

#[derive(Message)]
struct Traffic {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    conversation: String,
    #[prost(uint64, optional, tag = "3")]
    epoch: Option<u64>,
    #[prost(message, optional, tag = "4")]
    own: Option<DeviceRef>,
    #[prost(message, optional, tag = "5")]
    peer: Option<DeviceRef>,
}
// Quarantine is sticky activity but does not itself create a Signal session.
// This binding also remembers whether real crypto traffic was observed; it
// advances together with the existing positive crypto marker, never backwards.
#[derive(Message)]
struct QuarantineTraffic {
    #[prost(message, optional, tag = "1")]
    binding: Option<Traffic>,
    #[prost(bool, optional, tag = "2")]
    signal_observed: Option<bool>,
}
fn traffic_binding(s: &EpochScope) -> Traffic {
    Traffic {
        format: FORMAT,
        conversation: s.conversation.clone(),
        epoch: Some(s.epoch),
        own: Some(s.own.clone()),
        peer: Some(s.peer.clone()),
    }
}
fn check_binding(t: &Traffic, s: &EpochScope) -> CryptoResult<()> {
    if t.format != FORMAT
        || t.conversation != s.conversation
        || t.epoch != Some(s.epoch)
        || t.own.as_ref() != Some(&s.own)
        || t.peer.as_ref() != Some(&s.peer)
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
fn quarantine_key(s: &EpochScope) -> CryptoResult<RecordKey> {
    key(
        &s.own,
        &s.conversation,
        &format!("quarantine-traffic:{:016x}", s.epoch),
    )
}
fn quarantine_progress(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
) -> CryptoResult<Option<bool>> {
    let Some(bytes) = tx.get(&quarantine_key(s)?)? else {
        return Ok(None);
    };
    let q = QuarantineTraffic::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    check_binding(q.binding.as_ref().ok_or(CryptoError::Invalid)?, s)?;
    Ok(Some(q.signal_observed.ok_or(CryptoError::Invalid)?))
}
fn save_quarantine_traffic(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    crypto: bool,
) -> CryptoResult<()> {
    tx.put(
        &quarantine_key(s)?,
        &QuarantineTraffic {
            binding: Some(traffic_binding(s)),
            signal_observed: Some(crypto),
        }
        .encode_to_vec(),
    )?;
    Ok(())
}
pub(super) fn quarantine_seen(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
) -> CryptoResult<bool> {
    let Some(crypto) = quarantine_progress(tx, s)? else {
        return Ok(false);
    };
    if crypto != traffic_seen(tx, s)? {
        return Err(CryptoError::Invalid);
    }
    Ok(true)
}
fn traffic_key(s: &EpochScope) -> CryptoResult<RecordKey> {
    key(
        &s.own,
        &s.conversation,
        &format!("traffic:{:016x}", s.epoch),
    )
}
pub(super) fn traffic_seen(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
) -> CryptoResult<bool> {
    let Some(bytes) = tx.get(&traffic_key(s)?)? else {
        return Ok(false);
    };
    let t = Traffic::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    check_binding(&t, s)?;
    Ok(true)
}
pub(super) fn check_traffic(tx: &mut ProtectedTransaction<'_>, s: &EpochScope) -> CryptoResult<()> {
    let crypto = traffic_seen(tx, s)?;
    let quarantined = quarantine_seen(tx, s)?;
    // Quarantine never writes History/Outbox. Retained crypto source rows must
    // therefore still have the positive crypto marker, even if an old valid
    // quarantine-only marker was replayed. Fixed indexed presence probes, no scan.
    if !crypto {
        let prefix = format!("v2:{}:{:016x}:", s.conversation, s.epoch);
        for kind in [RecordKind::History, RecordKind::Outbox] {
            if tx.has_record_prefix(&s.own.generation_id, kind, &prefix)? {
                return Err(CryptoError::Invalid);
            }
        }
    }
    if !crypto && !quarantined && has_traffic(tx, s)? {
        return Err(CryptoError::Invalid);
    }
    Ok(())
}
/// Same transaction as the first message mutation; also works for a future
/// inbound-created ratchet without inventing an outbound acquisition request.
pub(super) fn observe_traffic(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
) -> CryptoResult<()> {
    let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    let p = pins(&c, s.own_identity)?;
    let j = read(tx, s, &p)?;
    let quarantined = quarantine_progress(tx, s)?;
    let crypto = traffic_seen(tx, s)?;
    if quarantined.is_some_and(|old| old != crypto) {
        return Err(CryptoError::Invalid);
    }
    if !crypto {
        tx.put(&traffic_key(s)?, &traffic_binding(s).encode_to_vec())?;
    }
    if quarantined == Some(false) {
        save_quarantine_traffic(tx, s, true)?;
    }
    if let Some(mut j) = j
        && j.has_message_state != Some(true)
    {
        j.has_message_state = Some(true);
        save(tx, s, &j)?;
    }
    Ok(())
}
pub(super) fn observe_quarantine(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
) -> CryptoResult<()> {
    let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    let p = pins(&c, s.own_identity)?;
    let j = read(tx, s, &p)?;
    // No Signal View, decrypt, prekey consumption, ratchet reset or nonce change.
    let crypto = traffic_seen(tx, s)?;
    let old = quarantine_progress(tx, s)?;
    if old.is_some_and(|old| old != crypto) {
        return Err(CryptoError::Invalid);
    }
    if old.is_none() {
        save_quarantine_traffic(tx, s, crypto)?;
    }
    if let Some(mut j) = j
        && j.has_message_state != Some(true)
    {
        j.has_message_state = Some(true);
        save(tx, s, &j)?;
    }
    Ok(())
}
pub(super) fn check_receiving(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    session_exists: bool,
) -> CryptoResult<()> {
    check_traffic(tx, s)?;
    if !session_exists && s.epoch > 0 && s.bootstrap_device.as_ref() != Some(&s.peer) {
        // Only the accepted proposer may initiate a fresh recovered session.
        return Err(CryptoError::Conflict);
    }
    let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    let p = pins(&c, s.own_identity)?;
    let j = read(tx, s, &p)?;
    if !session_exists && (traffic_seen(tx, s)? || j.is_some_and(|j| j.applied.is_some())) {
        return Err(CryptoError::Invalid);
    }
    Ok(())
}
fn has_traffic(tx: &mut ProtectedTransaction<'_>, s: &EpochScope) -> CryptoResult<bool> {
    if traffic_seen(tx, s)? || quarantine_seen(tx, s)? {
        return Ok(true);
    }
    let prefix = format!("v2:{}:{:016x}:", s.conversation, s.epoch);
    for kind in [RecordKind::History, RecordKind::Outbox, RecordKind::Inbox] {
        if tx.has_record_prefix(&s.own.generation_id, kind, &prefix)? {
            return Ok(true);
        }
    }
    Ok(false)
}
pub(super) fn session(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    s: &EpochScope,
) -> CryptoResult<bool> {
    let view = View::epoch(tx, s)?;
    let record = runtime.block_on(view.load_session(&address(&s.peer)))?;
    let pin = runtime.block_on(view.get_identity(&address(&s.peer)))?;
    if pin.is_some_and(|p| p != s.peer_identity) {
        return Err(CryptoError::Conflict);
    }
    if let Some(record) = &record
        && (pin != Some(s.peer_identity)
            || record.local_identity_key_bytes()? != s.own_identity.serialize().as_ref()
            || record.remote_identity_key_bytes()?.as_deref()
                != Some(s.peer_identity.serialize().as_ref())
            || record.remote_registration_id()? != s.peer_registration
            || record.local_registration_id()?
                != runtime.block_on(view.get_local_registration_id())?)
    {
        return Err(CryptoError::Invalid);
    }
    let exists = record.is_some();
    Ok(exists)
}
fn plan(s: EpochScope, pins: EpochConversation, j: &Journal) -> CryptoResult<EpochAcquisitionPlan> {
    Ok(EpochAcquisitionPlan {
        scope: s,
        pins,
        request: j.request.clone().ok_or(CryptoError::Invalid)?,
        applied: j.applied.is_some(),
    })
}

pub(in crate::encryption::signal_state) enum BootstrapCommand {
    Renew(
        Box<crate::encryption::native::EpochExpiredAcquisition>,
        Reply<EpochAcquisitionPlan>,
    ),
    Prepare(Box<EpochScope>, Reply<EpochAcquisitionPlan>),
    Apply(
        Box<EpochObservation<EpochAcquisitionPlan, v2::AcquiredBundle>>,
        Reply<()>,
    ),
}
impl Engine {
    pub(super) fn handle_epoch_bootstrap(&mut self, c: BootstrapCommand) {
        match c {
            BootstrapCommand::Renew(proof, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.renew_epoch_acquisition(*proof));
                }
            }
            BootstrapCommand::Prepare(s, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_epoch_acquisition(*s));
                }
            }
            BootstrapCommand::Apply(b, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.bootstrap_epoch(*b));
                }
            }
        }
    }
    pub(super) fn check_epoch_owner(&self, s: &EpochScope) -> CryptoResult<()> {
        if s.own != self.own || &s.scope != self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        Ok(())
    }
    fn prepare_epoch_acquisition(&mut self, s: EpochScope) -> CryptoResult<EpochAcquisitionPlan> {
        self.check_epoch_owner(&s)?;
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            let s = current(tx, &s)?;
            let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let pins = pins(&c, s.own_identity)?;
            let j = read(tx, &s, &pins)?;
            let exists = session(tx, runtime, &s)?;
            if let Some(j) = j {
                if j.applied.is_some() {
                    if !exists {
                        return Err(CryptoError::Invalid);
                    }
                    return plan(s, pins, &j);
                }
                outbound_bootstrap(&s)?;
                if exists {
                    return Err(CryptoError::SessionExists);
                }
                if j.has_message_state == Some(true) || has_traffic(tx, &s)? {
                    return Err(CryptoError::Conflict);
                }
                return plan(s, pins, &j);
            }
            outbound_bootstrap(&s)?;
            if exists {
                return Err(CryptoError::SessionExists);
            }
            if has_traffic(tx, &s)? {
                return Err(CryptoError::Conflict);
            }
            let j = Journal {
                format: FORMAT,
                request: Some(v2::AcquireBundleRequest {
                    context: Some(ClientContext {
                        protocol_version: 2,
                        device_generation_id: s.own.generation_id.clone(),
                    }),
                    conversation_id: s.conversation.clone(),
                    acquisition_id: uuid::Uuid::new_v4().to_string(),
                    expected_peer: Some(s.peer.clone()),
                    crypto_epoch: Some(s.epoch),
                }),
                peer_identity: s.peer_identity.serialize().to_vec(),
                peer_registration: s.peer_registration,
                applied: None,
                has_message_state: Some(false),
                replaces_request: None,
            };
            pins.check_acquire(j.request.as_ref().unwrap())
                .map_err(|_| CryptoError::Invalid)?;
            save(tx, &s, &j)?;
            plan(s, pins, &j)
        })
    }
    fn bootstrap_epoch(
        &mut self,
        b: EpochObservation<EpochAcquisitionPlan, v2::AcquiredBundle>,
    ) -> CryptoResult<()> {
        let p = b.request();
        let bundle = b.response();
        self.check_epoch_owner(&p.scope)?;
        if b.store_scope() != self.store.scope() || bundle.encoded_len() > 8192 {
            return Err(CryptoError::Invalid);
        }
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            let s = current(tx, &p.scope)?;
            let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let pins = pins(&c, s.own_identity)?;
            let mut j = read(tx, &s, &pins)?.ok_or(CryptoError::Invalid)?;
            if j.request.as_ref() != Some(&p.request) {
                return Err(CryptoError::Conflict);
            }
            pins.verify_bundle(&p.request, bundle)
                .map_err(|_| CryptoError::Invalid)?;
            let exists = session(tx, runtime, &s)?;
            if let Some(old) = &j.applied {
                if old != bundle {
                    return Err(CryptoError::Conflict);
                }
                if !exists {
                    return Err(CryptoError::Invalid);
                }
                return Ok(()); // Exact applied replay never resets or extends the lease.
            }
            outbound_bootstrap(&s)?;
            if exists {
                return Err(CryptoError::SessionExists);
            }
            if j.has_message_state == Some(true) || has_traffic(tx, &s)? {
                return Err(CryptoError::Conflict);
            }
            let signal = signal_bundle(&s, bundle)?;
            let now = SystemTime::now();
            let now_ms = now
                .duration_since(UNIX_EPOCH)
                .map_err(|_| CryptoError::Invalid)?
                .as_millis()
                .try_into()
                .map_err(|_| CryptoError::Invalid)?;
            check_acquisition_deadline(bundle.not_after_unix_ms, now_ms)?;
            let view = View::epoch(tx, &s)?;
            runtime.block_on(process_prekey_bundle(
                &address(&s.peer),
                &address(&s.own),
                &mut view.clone(),
                &mut view.clone(),
                &signal,
                now,
                &mut rand::rng(),
            ))?;
            drop(view);
            j.applied = Some(bundle.clone());
            save(tx, &s, &j)?;
            Ok(())
        })
    }
}
fn signal_bundle(s: &EpochScope, b: &v2::AcquiredBundle) -> CryptoResult<PreKeyBundle> {
    let signed = b.signed_pre_key.as_ref().ok_or(CryptoError::Invalid)?;
    let pq = b.kyber_pre_key.as_ref().ok_or(CryptoError::Invalid)?;
    let ec = b
        .one_time_pre_key
        .as_ref()
        .map(|p| {
            Ok::<_, SignalProtocolError>((
                PreKeyId::from(p.key_id),
                PublicKey::try_from(p.public_key.as_slice())?,
            ))
        })
        .transpose()?;
    Ok(PreKeyBundle::new(
        s.peer_registration,
        DeviceId::new(1).expect("valid constant"),
        ec,
        signed.key_id.into(),
        PublicKey::try_from(signed.public_key.as_slice()).map_err(SignalProtocolError::from)?,
        signed.signature.clone(),
        pq.key_id.into(),
        kem::PublicKey::deserialize(&pq.public_key)?,
        pq.signature.clone(),
        s.peer_identity,
    )?)
}
impl CryptoCoordinator {
    /// Persist before network; repeat this call after timeout/reopen for SAME nonce.
    pub async fn prepare_epoch_acquisition(
        &self,
        s: EpochScope,
    ) -> CryptoResult<EpochAcquisitionPlan> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Bootstrap(BootstrapCommand::Prepare(
                Box::new(s),
                r,
            )))
        })
        .await
    }
    /// Only a native observation of a protected request can enter bootstrap.
    /// No ratchet or applied proof is visible before the SQLite COMMIT succeeds.
    pub async fn bootstrap_epoch(
        &self,
        b: EpochObservation<EpochAcquisitionPlan, v2::AcquiredBundle>,
    ) -> CryptoResult<()> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Bootstrap(BootstrapCommand::Apply(
                Box::new(b),
                r,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_bootstrap_tests.rs"]
mod tests;
