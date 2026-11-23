//! Protected v2 receive and ACK. No RPC in a transaction; plaintext and ACK proof
//! leave the existing account lane only after all receive state commits together.
use super::outgoing::{link_key, load_link, parse_id, record_id, record_key, save_link};
use super::*;
use crate::{
    encryption::{epoch_contract as data, native::EpochDelivery},
    proto::e2ee as common,
};

#[path = "epoch_quarantine.rs"]
mod quarantine;
pub use quarantine::{EpochQuarantineReason, EpochQuarantinedMessage};

enum IncomingState {
    Stored(Box<v2::EncryptedPayload>),
    Quarantined(EpochQuarantineReason),
}
impl IncomingState {
    fn plaintext(self) -> CryptoResult<v2::EncryptedPayload> {
        match self {
            Self::Stored(p) => Ok(*p),
            Self::Quarantined(_) => Err(CryptoError::Quarantined),
        }
    }
}

/// Unforgeable by the renderer: only a committed protected receive/quarantine/reload can
/// mint it. No Deserialize/Debug/raw constructor or conversion from v1 proof.
#[derive(Clone)]
pub struct EpochDurableAck {
    scope: StoreScope,
    own: DeviceRef,
    id: String,
    request: common::AcknowledgeMessagesRequest,
    acknowledged: bool,
    // A volatile observation attached only to the result of a new receive.
    new_message: bool,
    // Freshly reloaded protected reply classification, used only by the local
    // projection check. A network proof's cached copy never authorizes a write.
    confirmation_projection: Option<(String, bool)>,
}
impl EpochDurableAck {
    pub(in crate::encryption) fn cursor(&self) -> EpochAckCursor {
        EpochAckCursor {
            scope: self.scope.clone(),
            own: self.own.clone(),
            after: format!("delivery-in:{}", self.id),
        }
    }
    pub fn message_id(&self) -> &str {
        &self.request.messages[0].message_id
    }
    pub fn acknowledged(&self) -> bool {
        self.acknowledged
    }
    pub(in crate::encryption) fn new_message(&self) -> bool {
        self.new_message
    }
    pub fn rejected(&self) -> bool {
        self.request.messages[0].disposition == common::AckDisposition::Rejected as i32
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope
    }
    pub(in crate::encryption) fn own_device(&self) -> &DeviceRef {
        &self.own
    }
    pub(in crate::encryption) fn request(&self) -> &common::AcknowledgeMessagesRequest {
        &self.request
    }
}
#[derive(Clone)]
pub struct EpochAckCursor {
    scope: StoreScope,
    own: DeviceRef,
    after: String,
}
pub struct EpochAckPage {
    pub acknowledgements: Vec<EpochDurableAck>,
    pub next: Option<EpochAckCursor>,
}
#[derive(Message)]
struct Journal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    delivery: Option<v2::Delivery>,
    #[prost(bool, optional, tag = "3")]
    acknowledged: Option<bool>,
    #[prost(bool, optional, tag = "4")]
    confirmation_required: Option<bool>,
    #[prost(int32, optional, tag = "5")]
    quarantine_reason: Option<i32>,
}
fn journal_key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    parse_id(id)?;
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("delivery-in:{id}"),
    )?)
}
fn marker_key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    parse_id(id)?;
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("delivery-acked:{id}"),
    )?)
}
fn check_delivery(own: &DeviceRef, d: &v2::Delivery) -> CryptoResult<()> {
    epoch_rpc::validate_pending(
        &v2::PendingMessages {
            messages: vec![d.clone()],
        },
        own,
    )
    .map_err(|_| CryptoError::Invalid)
}
fn request(
    own: &DeviceRef,
    d: &v2::Delivery,
    rejected: bool,
) -> CryptoResult<common::AcknowledgeMessagesRequest> {
    check_delivery(own, d)?;
    Ok(common::AcknowledgeMessagesRequest {
        context: Some(ClientContext {
            protocol_version: 2,
            device_generation_id: own.generation_id.clone(),
        }),
        messages: vec![common::Acknowledgement {
            message_id: d.envelope.as_ref().unwrap().message_id.clone(),
            envelope_digest: d.envelope_digest.clone(),
            disposition: if rejected {
                common::AckDisposition::Rejected
            } else {
                common::AckDisposition::Stored
            } as i32,
        }],
    })
}
fn check_envelope(s: &EpochScope, e: &v2::Envelope) -> CryptoResult<()> {
    data::validate_envelope_at_epoch(e, s.epoch).map_err(|_| CryptoError::Invalid)?;
    if e.conversation_id != s.conversation
        || e.sender.as_ref() != Some(&s.peer)
        || e.recipient.as_ref() != Some(&s.own)
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
fn check_observation(s: &EpochScope, observed: &EpochDelivery) -> CryptoResult<()> {
    if observed.store_scope() != &s.scope
        || observed.request().context
            != Some(ClientContext {
                protocol_version: 2,
                device_generation_id: s.own.generation_id.clone(),
            })
    {
        return Err(CryptoError::Conflict);
    }
    check_delivery(&s.own, observed.response())?;
    check_envelope(
        s,
        observed
            .response()
            .envelope
            .as_ref()
            .ok_or(CryptoError::Invalid)?,
    )
}
fn load_message(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
) -> CryptoResult<(IncomingState, EpochDurableAck, Journal)> {
    load_message_with(tx, own, store, id, false)
}
fn load_message_with(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
    history_only: bool,
) -> CryptoResult<(IncomingState, EpochDurableAck, Journal)> {
    let s = outgoing::load_scope_with(tx, own, store, id, history_only)?;
    let (_, _, uuid) = parse_id(id)?;
    let l = load_link(tx, own, uuid)?;
    if l.outgoing != Some(false) || l.id != id {
        return Err(CryptoError::Conflict);
    }
    bootstrap::check_traffic(tx, &s)?;
    let b = tx
        .get(&record_key(own, RecordKind::Inbox, id)?)?
        .ok_or(CryptoError::Invalid)?;
    let e = data::decode_envelope(&b).map_err(|_| CryptoError::Invalid)?;
    check_envelope(&s, &e)?;
    if data::envelope_digest(&e)
        .map_err(|_| CryptoError::Invalid)?
        .as_slice()
        != l.digest
    {
        return Err(CryptoError::Conflict);
    }
    let b = tx
        .get(&journal_key(own, id)?)?
        .ok_or(CryptoError::Invalid)?;
    let j = Journal::decode(b.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if j.format != FORMAT
        || j.acknowledged.is_none()
        || j.confirmation_required.is_none()
        || j.quarantine_reason.is_none()
    {
        return Err(CryptoError::Invalid);
    }
    let d = j.delivery.as_ref().ok_or(CryptoError::Invalid)?;
    check_delivery(own, d)?;
    if d.envelope.as_ref() != Some(&e) {
        return Err(CryptoError::Conflict);
    }
    let acknowledged = j.acknowledged.unwrap();
    let marker = tx.get(&marker_key(own, id)?)?;
    match (acknowledged, marker) {
        (false, None) => (),
        (true, Some(b))
            if b.as_slice() == digest(b"zchat.e2ee.ack-commit.v2\0", &j.encode_to_vec()) => {}
        _ => return Err(CryptoError::Invalid),
    }
    let history = tx.get(&record_key(own, RecordKind::History, id)?)?;
    let (state, confirmation_projection) = if j.quarantine_reason == Some(0) {
        if !bootstrap::traffic_seen(tx, &s)? {
            return Err(CryptoError::Invalid);
        }
        let bytes = history.ok_or(CryptoError::Invalid)?;
        let p = v2::EncryptedPayload::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
        data::validate_payload_binding(&p, &e).map_err(|_| CryptoError::Invalid)?;
        let projection = confirmation::validate(
            tx,
            &s,
            &e,
            &p,
            j.confirmation_required.unwrap(),
            history_only,
        )?
        .map(|plan| {
            Ok::<_, CryptoError>((
                record_id(&s, &plan.request().envelope.as_ref().unwrap().message_id)?,
                plan.needs_status_poll(),
            ))
        })
        .transpose()?;
        (IncomingState::Stored(Box::new(p)), projection)
    } else {
        let reason = EpochQuarantineReason::parse(j.quarantine_reason.unwrap())?;
        if history.is_some()
            || j.confirmation_required != Some(false)
            || !bootstrap::quarantine_seen(tx, &s)?
            || tx.get(&record_key(own, RecordKind::Outbox, id)?)?.is_some()
        {
            return Err(CryptoError::Conflict);
        }
        confirmation::check_absent(tx, &s, &e.message_id)?;
        (IncomingState::Quarantined(reason), None)
    };
    let rejected = matches!(state, IncomingState::Quarantined(_));
    let proof = EpochDurableAck {
        scope: store.clone(),
        own: own.clone(),
        id: id.into(),
        request: request(own, d, rejected)?,
        acknowledged,
        new_message: false,
        confirmation_projection,
    };
    Ok((state, proof, j))
}
fn check_projections(
    tx: &mut ProtectedTransaction<'_>,
    proof: &EpochDurableAck,
) -> CryptoResult<()> {
    let own = &proof.own;
    let id = &proof.id;
    if !proof.rejected() {
        tx.check_recovery_projection(&record_key(own, RecordKind::History, id)?, false)?;
    }
    tx.check_recovery_projection(&record_key(own, RecordKind::Inbox, id)?, false)?;
    tx.check_recovery_projection(&journal_key(own, id)?, !proof.acknowledged)?;
    if let Some((reply, pending)) = &proof.confirmation_projection {
        outgoing::check_projections(tx, own, reply, *pending)?;
    }
    Ok(())
}
/// Saved receive cohort before key-free UI conversion. File work separately
/// checks the current own-device lifecycle; this does not grant file ACK.
pub(super) fn view_payload(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
) -> CryptoResult<(v2::EncryptedPayload, v2::Envelope)> {
    let (value, proof, j) = load_message_with(tx, own, store, id, true)?;
    check_projections(tx, &proof)?;
    let p = value.plaintext()?;
    let e = j
        .delivery
        .and_then(|d| d.envelope)
        .ok_or(CryptoError::Invalid)?;
    Ok((p, e))
}
pub(super) fn index_data_record(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    kind: RecordKind,
    id: &str,
    bytes: &[u8],
) -> CryptoResult<()> {
    if !matches!(kind, RecordKind::Inbox | RecordKind::History) {
        return Err(CryptoError::Invalid);
    }
    let (p, proof, j) = load_message(tx, own, store, id)?;
    if kind == RecordKind::Inbox
        && data::decode_envelope(bytes).map_err(|_| CryptoError::Invalid)?
            != *j.delivery.as_ref().unwrap().envelope.as_ref().unwrap()
        || kind == RecordKind::History
            && !matches!(&p, IncomingState::Stored(value) if v2::EncryptedPayload::decode(bytes).is_ok_and(|actual| &actual == value.as_ref()))
    {
        return Err(CryptoError::Conflict);
    }
    if !proof.rejected() {
        tx.project_recovery(&record_key(own, RecordKind::History, id)?, false)?;
    }
    tx.project_recovery(&record_key(own, RecordKind::Inbox, id)?, false)?;
    tx.project_recovery(&journal_key(own, id)?, !proof.acknowledged)?;
    Ok(())
}
pub(in crate::encryption::signal_state) fn index_ack_record(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    key: &str,
) -> CryptoResult<()> {
    let id = key
        .strip_prefix("delivery-in:")
        .ok_or(CryptoError::Invalid)?;
    let (_, proof, _) = load_message(tx, own, store, id)?;
    tx.project_recovery(&journal_key(own, id)?, !proof.acknowledged)?;
    Ok(())
}
pub(in crate::encryption::signal_state) enum IncomingCommand {
    Quarantine(
        Box<(EpochScope, EpochDelivery, EpochQuarantineReason)>,
        Reply<EpochDurableAck>,
    ),
    Quarantined(String, Reply<EpochQuarantinedMessage>),
    Receive(
        Box<(EpochScope, EpochDelivery)>,
        Reply<(v2::EncryptedPayload, EpochDurableAck)>,
    ),
    Ack(String, Reply<EpochDurableAck>),
    History(String, Reply<v2::EncryptedPayload>),
    Page(Option<EpochAckCursor>, Reply<EpochAckPage>),
    Accepted(
        Box<EpochObservation<EpochDurableAck, common::Empty>>,
        Reply<()>,
    ),
}
impl Engine {
    pub(super) fn handle_epoch_incoming(&mut self, c: IncomingCommand) {
        match c {
            IncomingCommand::Quarantine(b, r) => {
                if !r.is_closed() {
                    let (s, d, reason) = *b;
                    let _ = r.send(self.quarantine_epoch(s, d, reason));
                }
            }
            IncomingCommand::Quarantined(id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_quarantined_message(&id));
                }
            }
            IncomingCommand::Receive(b, r) => {
                if !r.is_closed() {
                    let (s, d) = *b;
                    let _ = r.send(self.receive_epoch(s, d));
                }
            }
            IncomingCommand::Ack(id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_ack(&id));
                }
            }
            IncomingCommand::History(id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_incoming_message(&id));
                }
            }
            IncomingCommand::Page(c, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_ack_page(c));
                }
            }
            IncomingCommand::Accepted(b, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_epoch_ack(*b));
                }
            }
        }
    }
    fn receive_epoch(
        &mut self,
        s: EpochScope,
        observed: EpochDelivery,
    ) -> CryptoResult<(v2::EncryptedPayload, EpochDurableAck)> {
        self.check_epoch_owner(&s)?;
        check_observation(&s, &observed)?;
        let d = observed.response();
        check_delivery(&s.own, d)?;
        let e = d.envelope.as_ref().ok_or(CryptoError::Invalid)?;
        check_envelope(&s, e)?;
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, &s.own)?;
            if attachment_prepare::reserved(tx, &s.own, &e.message_id)? {
                return Err(CryptoError::Conflict);
            }
            let s = bootstrap::current(tx, &s)?;
            let id = record_id(&s, &e.message_id)?;
            let inbox = record_key(&s.own, RecordKind::Inbox, &id)?;
            let history = record_key(&s.own, RecordKind::History, &id)?;
            if tx.get(&inbox)?.is_some() {
                let (p, proof, j) = load_message(tx, &s.own, &s.scope, &id)?;
                if j.delivery.as_ref() != Some(d) {
                    return Err(CryptoError::Conflict);
                }
                check_projections(tx, &proof)?;
                return Ok((p.plaintext()?, proof));
            }
            if tx.get(&history)?.is_some()
                || tx.get(&link_key(&s.own, &e.message_id)?)?.is_some()
                || tx.get(&journal_key(&s.own, &id)?)?.is_some()
                || tx.get(&marker_key(&s.own, &id)?)?.is_some()
            {
                return Err(CryptoError::Conflict);
            }
            let exists = bootstrap::session(tx, runtime, &s)?;
            bootstrap::check_receiving(tx, &s, exists)?;
            let encrypted = match CiphertextType::try_from(e.message_type) {
                Ok(CiphertextType::PrekeySignal) => {
                    let message = PreKeySignalMessage::try_from(e.ciphertext.as_slice())?;
                    if message.registration_id() != s.peer_registration {
                        return Err(CryptoError::Conflict);
                    }
                    CiphertextMessage::PreKeySignalMessage(message)
                }
                Ok(CiphertextType::Signal) => CiphertextMessage::SignalMessage(
                    SignalMessage::try_from(e.ciphertext.as_slice())?,
                ),
                _ => return Err(CryptoError::Invalid),
            };
            let view = View::epoch(tx, &s)?;
            let plaintext = Zeroizing::new(runtime.block_on(message_decrypt(
                &encrypted,
                &address(&s.peer),
                &address(&s.own),
                &mut view.clone(),
                &mut view.clone(),
                &mut view.clone(),
                &view.clone(),
                &mut view.clone(),
                &mut rand::rng(),
            ))?);
            drop(view);
            let p = v2::EncryptedPayload::decode(plaintext.as_slice())
                .map_err(|_| CryptoError::Invalid)?;
            data::validate_payload_binding(&p, e).map_err(|_| CryptoError::Invalid)?;
            if !bootstrap::session(tx, runtime, &s)? {
                return Err(CryptoError::Invalid);
            }
            bootstrap::observe_traffic(tx, &s)?;
            tx.put(&history, &plaintext)?;
            tx.put(&inbox, &e.encode_to_vec())?;
            save_link(tx, &s.own, &id, e, false)?;
            tx.project_recovery(&history, false)?;
            tx.project_recovery(&inbox, false)?;
            let confirmation_required = confirmation::prepare(tx, runtime, &s, e, &p)?;
            tx.put(
                &journal_key(&s.own, &id)?,
                &Journal {
                    format: FORMAT,
                    delivery: Some(d.clone()),
                    acknowledged: Some(false),
                    confirmation_required: Some(confirmation_required),
                    quarantine_reason: Some(0),
                }
                .encode_to_vec(),
            )?;
            tx.project_recovery(&journal_key(&s.own, &id)?, true)?;
            history::index(tx, &s.own, &id, &p)?;
            let (p, mut proof, _) = load_message(tx, &s.own, &s.scope, &id)?;
            let plaintext = p.plaintext()?;
            proof.new_message = matches!(
                plaintext.content,
                Some(
                    v2::encrypted_payload::Content::Text(_)
                        | v2::encrypted_payload::Content::Attachment(_)
                )
            );
            Ok((plaintext, proof))
        })
    }
    fn epoch_ack(&mut self, uuid: &str) -> CryptoResult<EpochDurableAck> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let l = load_link(tx, own, uuid)?;
            let (_, proof, _) = load_message(tx, own, &store, &l.id)?;
            check_projections(tx, &proof)?;
            Ok(proof)
        })
    }
    fn epoch_incoming_message(&mut self, uuid: &str) -> CryptoResult<v2::EncryptedPayload> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let l = load_link(tx, own, uuid)?;
            let (p, proof, _) = load_message(tx, own, &store, &l.id)?;
            check_projections(tx, &proof)?;
            p.plaintext()
        })
    }
    fn epoch_ack_page(&mut self, c: Option<EpochAckCursor>) -> CryptoResult<EpochAckPage> {
        let own = &self.own;
        let store = self.store.scope().clone();
        if c.as_ref()
            .is_some_and(|c| c.own != *own || c.scope != store)
        {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let ids = tx.recovery_page(
                &own.generation_id,
                RecordKind::Control,
                "delivery-in:v2:",
                c.as_ref().map(|c| c.after.as_str()),
            )?;
            let next = if ids.len() == 32 {
                Some(EpochAckCursor {
                    scope: store.clone(),
                    own: own.clone(),
                    after: ids.last().unwrap().clone(),
                })
            } else {
                None
            };
            let mut acknowledgements = Vec::with_capacity(ids.len());
            for key in ids {
                let id = key
                    .strip_prefix("delivery-in:")
                    .ok_or(CryptoError::Invalid)?;
                let (_, p, _) = load_message(tx, own, &store, id)?;
                check_projections(tx, &p)?;
                if p.acknowledged {
                    return Err(CryptoError::Invalid);
                }
                acknowledgements.push(p);
            }
            Ok(EpochAckPage {
                acknowledgements,
                next,
            })
        })
    }
    fn accept_epoch_ack(
        &mut self,
        b: EpochObservation<EpochDurableAck, common::Empty>,
    ) -> CryptoResult<()> {
        let p = b.request();
        let own = &self.own;
        let store = self.store.scope().clone();
        if b.store_scope() != &store || p.scope != store || p.own != *own {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let (_, saved, mut j) = load_message(tx, own, &store, &p.id)?;
            check_projections(tx, &saved)?;
            if p.request != saved.request {
                return Err(CryptoError::Conflict);
            }
            if saved.acknowledged {
                return Ok(());
            }
            j.acknowledged = Some(true);
            let bytes = j.encode_to_vec();
            tx.put(&journal_key(own, &p.id)?, &bytes)?;
            tx.put(
                &marker_key(own, &p.id)?,
                &digest(b"zchat.e2ee.ack-commit.v2\0", &bytes),
            )?;
            tx.project_recovery(&journal_key(own, &p.id)?, false)?;
            Ok(())
        })
    }
}
impl CryptoCoordinator {
    pub async fn receive_epoch(
        &self,
        s: EpochScope,
        d: EpochDelivery,
    ) -> CryptoResult<(v2::EncryptedPayload, EpochDurableAck)> {
        if d.response().encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Incoming(IncomingCommand::Receive(
                Box::new((s, d)),
                r,
            )))
        })
        .await
    }
    pub async fn epoch_ack(&self, uuid: String) -> CryptoResult<EpochDurableAck> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| Command::Epoch(EpochCommand::Incoming(IncomingCommand::Ack(uuid, r))))
            .await
    }
    pub async fn epoch_incoming_message(&self, uuid: String) -> CryptoResult<v2::EncryptedPayload> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| Command::Epoch(EpochCommand::Incoming(IncomingCommand::History(uuid, r))))
            .await
    }
    pub async fn epoch_ack_page(&self, c: Option<EpochAckCursor>) -> CryptoResult<EpochAckPage> {
        self.request(|r| Command::Epoch(EpochCommand::Incoming(IncomingCommand::Page(c, r))))
            .await
    }
    pub async fn accept_epoch_ack(
        &self,
        b: EpochObservation<EpochDurableAck, common::Empty>,
    ) -> CryptoResult<()> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Incoming(IncomingCommand::Accepted(
                Box::new(b),
                r,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_incoming_tests.rs"]
pub(super) mod tests;
