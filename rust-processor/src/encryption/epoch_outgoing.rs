//! V2 outgoing ledger. A ciphertext/plan leaves the account lane only AFTER
//! ratchet+history+outbox+binding+sticky traffic+projection commit together.
use super::*;
use crate::encryption::epoch_contract as data;

#[path = "epoch_fence.rs"]
mod fence;
#[path = "epoch_file_progress.rs"]
mod file_progress;
#[path = "epoch_status.rs"]
mod status;
use file_progress::commands::FileProgressCommand;
pub use file_progress::{
    EpochFileProgress, EpochFileStatusCursor, EpochFileStatusPage, FileStatusSeedProgress,
};

/// Exact persisted Send, not permission to encrypt again or change epochs.
#[derive(Clone)]
pub struct EpochOutgoingPlan {
    scope: EpochScope,
    pins: EpochConversation,
    request: v2::SendMessageRequest,
    receipt: Option<v2::SendReceipt>,
    status: Option<v2::MessageStatus>,
    removal: Option<v2::MessageStatus>,
    revision: Option<u64>,
    sequence: Option<u64>,
    chain_digest: Vec<u8>,
    sealed: Option<v2::EpochSealedFailure>,
}
impl EpochOutgoingPlan {
    pub(in crate::encryption) fn cursor(&self) -> EpochOutboxCursor {
        let e = self
            .request
            .envelope
            .as_ref()
            .expect("validated worker plan");
        EpochOutboxCursor {
            scope: self.scope.scope.clone(),
            own: self.scope.own.clone(),
            after: format!(
                "v2:{}:{:016x}:{}",
                e.conversation_id,
                e.crypto_epoch.expect("v2 epoch"),
                e.message_id
            ),
        }
    }
    pub fn request(&self) -> &v2::SendMessageRequest {
        &self.request
    }
    pub fn receipt(&self) -> Option<&v2::SendReceipt> {
        self.receipt.as_ref()
    }
    pub fn status(&self) -> Option<&v2::MessageStatus> {
        self.status.as_ref()
    }
    /// Previously observed removal remains visible even after a late peer ACK.
    /// Absence is not proof that no unobserved server removal ever occurred.
    pub fn removal(&self) -> Option<&v2::MessageStatus> {
        self.removal.as_ref()
    }
    pub fn sealed_failure(&self) -> Option<&v2::EpochSealedFailure> {
        self.sealed.as_ref()
    }
    pub fn needs_send(&self) -> bool {
        self.receipt.is_none() && self.sealed.is_none()
    }
    pub fn needs_status_poll(&self) -> bool {
        self.sealed.is_none() && status::pending(self.status.as_ref())
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope.scope
    }
    pub(in crate::encryption) fn pins(&self) -> &EpochConversation {
        &self.pins
    }
}
#[derive(Clone)]
pub struct EpochOutboxCursor {
    scope: StoreScope,
    own: DeviceRef,
    after: String,
}
pub struct EpochOutboxPage {
    pub messages: Vec<EpochOutgoingPlan>,
    pub next: Option<EpochOutboxCursor>,
}
#[derive(Message)]
struct Journal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    receipt: Option<v2::SendReceipt>,
    #[prost(message, optional, tag = "3")]
    status: Option<v2::MessageStatus>,
    #[prost(message, optional, tag = "4")]
    removal: Option<v2::MessageStatus>,
    #[prost(uint64, optional, tag = "5")]
    revision: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    sequence: Option<u64>,
    #[prost(bytes = "vec", tag = "7")]
    chain_digest: Vec<u8>,
    #[prost(message, optional, tag = "8")]
    sealed: Option<v2::EpochSealedFailure>,
}
// Global per-generation UUID binding: reusing a UUID in a different chat/epoch
// must fail before consuming the ratchet. This is a protected point lookup.
#[derive(Message)]
pub(super) struct Link {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    pub(super) id: String,
    #[prost(message, optional, tag = "3")]
    own: Option<DeviceRef>,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) digest: Vec<u8>,
    #[prost(bool, optional, tag = "5")]
    pub(super) outgoing: Option<bool>,
    #[prost(bytes = "vec", optional, tag = "6")]
    pub(super) attachment_preparation: Option<Vec<u8>>,
}
pub(super) fn link_key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    if !epoch_rpc::uuid4(id) {
        return Err(CryptoError::Invalid);
    }
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("message-v2:{id}"),
    )?)
}
pub(super) fn record_id(s: &EpochScope, id: &str) -> CryptoResult<String> {
    if !epoch_rpc::uuid4(id) {
        return Err(CryptoError::Invalid);
    }
    Ok(format!("v2:{}:{:016x}:{id}", s.conversation, s.epoch))
}
pub(super) fn record_key(own: &DeviceRef, kind: RecordKind, id: &str) -> CryptoResult<RecordKey> {
    parse_id(id)?;
    Ok(RecordKey::new(&own.generation_id, kind, id)?)
}
fn journal_key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    parse_id(id)?;
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("delivery-out:{id}"),
    )?)
}
pub(super) fn parse_id(id: &str) -> CryptoResult<(&str, u64, &str)> {
    let parts: Vec<_> = id.split(':').collect();
    if parts.len() != 4
        || parts[0] != "v2"
        || !epoch_rpc::uuid4(parts[1])
        || parts[2].len() != 16
        || !epoch_rpc::uuid4(parts[3])
    {
        return Err(CryptoError::Invalid);
    }
    let epoch = u64::from_str_radix(parts[2], 16).map_err(|_| CryptoError::Invalid)?;
    if epoch > i64::MAX as u64 || format!("{epoch:016x}") != parts[2] {
        return Err(CryptoError::Invalid);
    }
    Ok((parts[1], epoch, parts[3]))
}
fn envelope(
    s: &EpochScope,
    id: &str,
    ciphertext: Vec<u8>,
    kind: CiphertextType,
    blob: Option<v2::BlobRef>,
) -> v2::Envelope {
    v2::Envelope {
        protocol_version: if blob.is_some() { 3 } else { 2 },
        crypto_suite: CryptoSuite::SignalPq1 as i32,
        message_id: id.into(),
        conversation_id: s.conversation.clone(),
        sender: Some(s.own.clone()),
        recipient: Some(s.peer.clone()),
        message_type: kind as i32,
        ciphertext,
        crypto_epoch: Some(s.epoch),
        blob,
    }
}
pub(super) fn load_link(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<Link> {
    let b = tx
        .get(&link_key(own, id)?)?
        .ok_or(CryptoError::MessageUnavailable)?;
    let l = Link::decode(b.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if l.format != FORMAT
        || l.outgoing.is_none()
        || l.own.as_ref() != Some(own)
        || l.digest.len() != 32
        || l.attachment_preparation
            .as_ref()
            .is_some_and(|p| p.len() != 32 || l.outgoing != Some(true))
        || parse_id(&l.id)?.2 != id
    {
        return Err(CryptoError::Conflict);
    }
    Ok(l)
}
fn read_link(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef, id: &str) -> CryptoResult<Link> {
    let l = load_link(tx, own, id)?;
    if l.outgoing != Some(true) {
        return Err(CryptoError::Conflict);
    }
    Ok(l)
}
// Resolve UUID BEFORE selecting the current epoch or generating a new timestamp.
// A retry of an old committed text never re-encrypts/rebases it after recovery.
pub(super) fn existing_text(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    chat: &str,
    uuid: &str,
    text: &str,
) -> CryptoResult<Option<(EpochOutgoingPlan, i64)>> {
    if tx.get(&link_key(own, uuid)?)?.is_none() {
        return Ok(None);
    }
    let link = read_link(tx, own, uuid)?;
    if parse_id(&link.id)?.0 != chat {
        return Err(CryptoError::Conflict);
    }
    let saved = plan(tx, own, store, &link.id)?;
    check_projections(tx, own, &link.id, saved.needs_status_poll())?;
    let bytes = tx
        .get(&record_key(own, RecordKind::History, &link.id)?)?
        .ok_or(CryptoError::Invalid)?;
    let mut payload =
        v2::EncryptedPayload::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let Some(v2::encrypted_payload::Content::Text(old)) = payload.content.take() else {
        return Err(CryptoError::Conflict);
    };
    let old = Zeroizing::new(old.text);
    if old.as_str() != text {
        return Err(CryptoError::Conflict);
    }
    Ok(Some((saved, payload.created_at_unix_ms)))
}
pub(super) fn view_payload(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
) -> CryptoResult<(v2::EncryptedPayload, MessageProgress)> {
    let p = view_plan(tx, own, store, id)?;
    check_projections(tx, own, id, p.needs_status_poll())?;
    let bytes = tx
        .get(&record_key(own, RecordKind::History, id)?)?
        .ok_or(CryptoError::Invalid)?;
    let payload =
        v2::EncryptedPayload::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    Ok((
        payload,
        MessageProgress {
            stored_on_server: p.receipt().is_some(),
            state: p
                .status()
                .map(|s| {
                    crate::proto::e2ee::MessageState::try_from(s.state)
                        .map_err(|_| CryptoError::Invalid)
                })
                .transpose()?,
            removal: p
                .removal()
                .map(|s| {
                    crate::proto::e2ee::MessageState::try_from(s.state)
                        .map_err(|_| CryptoError::Invalid)
                })
                .transpose()?,
            sealed_unsent: p.sealed_failure().is_some(),
        },
    ))
}
pub(super) fn save_link(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    e: &v2::Envelope,
    outgoing: bool,
) -> CryptoResult<()> {
    tx.put(
        &link_key(own, &e.message_id)?,
        &Link {
            format: FORMAT,
            id: id.into(),
            own: Some(own.clone()),
            digest: data::envelope_digest(e)
                .map_err(|_| CryptoError::Invalid)?
                .to_vec(),
            outgoing: Some(outgoing),
            attachment_preparation: None,
        }
        .encode_to_vec(),
    )?;
    Ok(())
}

pub(super) fn load_scope_with(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
    history_only: bool,
) -> CryptoResult<EpochScope> {
    let (chat, epoch, _) = parse_id(id)?;
    let c =
        conversation_lifecycle::load(tx, own, chat)?.ok_or(CryptoError::ConversationUnavailable)?;
    let (identity, registration) = identity(tx, own)?;
    let loaded = archive::load_at(tx, own, &c, identity, registration, epoch)?;
    let s = scope_with(&loaded, &c, store.clone(), history_only)?;
    if s.epoch != epoch {
        return Err(CryptoError::EpochRecoveryRequired);
    }
    Ok(s)
}
pub(super) fn plan(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
) -> CryptoResult<EpochOutgoingPlan> {
    plan_with(tx, own, store, id, false)
}
// Never returned by a worker command. Used only to validate retained plaintext
// and service-reply cohorts, even after own-device closure.
pub(super) fn view_plan(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
) -> CryptoResult<EpochOutgoingPlan> {
    plan_with(tx, own, store, id, true)
}
fn plan_with(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
    history_only: bool,
) -> CryptoResult<EpochOutgoingPlan> {
    let s = load_scope_with(tx, own, store, id, history_only)?;
    let (_, _, uuid) = parse_id(id)?;
    let l = read_link(tx, own, uuid)?;
    if l.id != id || !bootstrap::traffic_seen(tx, &s)? {
        return Err(CryptoError::Conflict);
    }
    let b = tx
        .get(&record_key(own, RecordKind::Outbox, id)?)?
        .ok_or(CryptoError::Invalid)?;
    let e = data::decode_envelope(&b).map_err(|_| CryptoError::Invalid)?;
    if e != envelope(
        &s,
        uuid,
        e.ciphertext.clone(),
        CiphertextType::try_from(e.message_type).map_err(|_| CryptoError::Invalid)?,
        e.blob.clone(),
    ) || data::envelope_digest(&e)
        .map_err(|_| CryptoError::Invalid)?
        .as_slice()
        != l.digest
    {
        return Err(CryptoError::Conflict);
    }
    let b = tx
        .get(&record_key(own, RecordKind::History, id)?)?
        .ok_or(CryptoError::Invalid)?;
    let p = v2::EncryptedPayload::decode(b.as_slice()).map_err(|_| CryptoError::Invalid)?;
    data::validate_payload_binding(&p, &e).map_err(|_| CryptoError::Invalid)?;
    attachment_prepare::promotion::validate_cohort(tx, own, &l, &e, &p)?;
    let b = tx
        .get(&journal_key(own, id)?)?
        .ok_or(CryptoError::Invalid)?;
    let j = Journal::decode(b.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if j.format != FORMAT {
        return Err(CryptoError::Invalid);
    }
    let c = conversation_lifecycle::load(tx, own, &s.conversation)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    let pins = pins(&c, s.own_identity)?;
    let r = v2::SendMessageRequest {
        context: Some(ClientContext {
            protocol_version: 2,
            device_generation_id: own.generation_id.clone(),
        }),
        envelope: Some(e),
    };
    pins.check_send(&r).map_err(|_| CryptoError::Invalid)?;
    if let Some(receipt) = &j.receipt {
        data::validate_receipt(&r, receipt).map_err(|_| CryptoError::Invalid)?;
    }
    chain::validate_message(
        tx,
        &s,
        r.envelope.as_ref().unwrap(),
        j.sequence,
        &j.chain_digest,
    )?;
    status::validate(tx, own, id, &r, &j)?;
    fence::validate(tx, &s, &pins, id, &r, &j)?;
    let result = EpochOutgoingPlan {
        scope: s,
        pins,
        request: r,
        receipt: j.receipt,
        status: j.status,
        removal: j.removal,
        revision: j.revision,
        sequence: j.sequence,
        chain_digest: j.chain_digest,
        sealed: j.sealed,
    };
    attachment_prepare::retirement::validate(tx, own, &result)?;
    Ok(result)
}
pub(super) fn check_projections(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    pending: bool,
) -> CryptoResult<()> {
    tx.check_recovery_projection(&record_key(own, RecordKind::History, id)?, false)?;
    tx.check_recovery_projection(&record_key(own, RecordKind::Outbox, id)?, pending)?;
    Ok(())
}
/// Existing explicit ≤32-source-row seeder routes by the v2 record namespace,
/// never by attempting a v1 decode. History routing checks the UUID direction.
pub(in crate::encryption::signal_state) fn index_data_record(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    kind: RecordKind,
    id: &str,
    bytes: &[u8],
) -> CryptoResult<()> {
    let (_, _, uuid) = parse_id(id)?;
    if kind == RecordKind::Inbox
        || (kind == RecordKind::History && load_link(tx, own, uuid)?.outgoing == Some(false))
    {
        return super::incoming::index_data_record(tx, own, store, kind, id, bytes);
    }
    if !matches!(kind, RecordKind::Outbox | RecordKind::History) {
        return Err(CryptoError::Invalid);
    }
    let p = plan(tx, own, store, id)?;
    if kind == RecordKind::Outbox
        && data::decode_envelope(bytes).map_err(|_| CryptoError::Invalid)?
            != *p.request.envelope.as_ref().unwrap()
    {
        return Err(CryptoError::Conflict);
    }
    tx.project_recovery(&record_key(own, RecordKind::History, id)?, false)?;
    tx.project_recovery(
        &record_key(own, RecordKind::Outbox, id)?,
        p.needs_status_poll(),
    )?;
    Ok(())
}

// Shared by explicit sends and automatic confirmations. The caller owns the
// existing transaction; no nested TX/network and no result before its COMMIT.
pub(super) fn encrypt_message(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    s: &EpochScope,
    p: &v2::EncryptedPayload,
) -> CryptoResult<EpochOutgoingPlan> {
    // Generic sends/confirmations cannot consume a reserved attachment UUID.
    if attachment_prepare::reserved(tx, &s.own, &p.message_id)? {
        return Err(CryptoError::Conflict);
    }
    encrypt_message_inner(tx, runtime, s, p, |_, _| Ok(()))
}

// There is no boolean bypass: this entry requires the exact protected prepared
// cohort and a durable Ready observation, reloaded inside the SAME transaction.
pub(super) fn encrypt_prepared_attachment(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    s: &EpochScope,
    p: &v2::EncryptedPayload,
) -> CryptoResult<Option<EpochOutgoingPlan>> {
    attachment_prepare::promotion::check_promotion(tx, s, p)?;
    bootstrap::check_traffic(tx, s)?;
    if !bootstrap::session(tx, runtime, s)? {
        return Ok(None);
    }
    encrypt_message_inner(tx, runtime, s, p, |tx, envelope| {
        attachment_prepare::promotion::mark_promoted(tx, s, p, envelope)
    })
    .map(Some)
}

fn encrypt_message_inner(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    s: &EpochScope,
    p: &v2::EncryptedPayload,
    finish: impl FnOnce(&mut ProtectedTransaction<'_>, &v2::Envelope) -> CryptoResult<()>,
) -> CryptoResult<EpochOutgoingPlan> {
    let blob = match p.content.as_ref() {
        Some(v2::encrypted_payload::Content::Attachment(a)) => Some(
            crate::encryption::attachment_payload::blob_reference(a)
                .map_err(|_| CryptoError::Invalid)?,
        ),
        _ => None,
    };
    data::validate_payload_binding(
        p,
        &envelope(
            s,
            &p.message_id,
            vec![1],
            CiphertextType::Signal,
            blob.clone(),
        ),
    )
    .map_err(|_| CryptoError::Invalid)?;
    recovery_indexes::require_ready(tx, &s.own)?;
    let s = bootstrap::current(tx, s)?;
    let id = record_id(&s, &p.message_id)?;
    let history = record_key(&s.own, RecordKind::History, &id)?;
    let outbox = record_key(&s.own, RecordKind::Outbox, &id)?;
    if tx.get(&outbox)?.is_some() {
        let saved = plan(tx, &s.own, &s.scope, &id)?;
        check_projections(tx, &s.own, &id, saved.needs_status_poll())?;
        let old = tx.get(&history)?.ok_or(CryptoError::Invalid)?;
        if v2::EncryptedPayload::decode(old.as_slice()).map_err(|_| CryptoError::Invalid)? != *p {
            return Err(CryptoError::Conflict);
        }
        return Ok(saved);
    }
    if tx.get(&history)?.is_some()
        || tx.get(&link_key(&s.own, &p.message_id)?)?.is_some()
        || tx.get(&journal_key(&s.own, &id)?)?.is_some()
    {
        return Err(CryptoError::Conflict);
    }
    bootstrap::sending(&s)?;
    bootstrap::check_traffic(tx, &s)?;
    if !bootstrap::session(tx, runtime, &s)? {
        return Err(CryptoError::Invalid);
    }
    let view = View::epoch(tx, &s)?;
    let plaintext = Zeroizing::new(p.encode_to_vec());
    let encrypted = runtime.block_on(message_encrypt(
        &plaintext,
        &address(&s.peer),
        &address(&s.own),
        &mut view.clone(),
        &mut view.clone(),
        SystemTime::now(),
        &mut rand::rng(),
    ))?;
    drop(view);
    let kind = match &encrypted {
        CiphertextMessage::PreKeySignalMessage(_) => CiphertextType::PrekeySignal,
        CiphertextMessage::SignalMessage(_) => CiphertextType::Signal,
        _ => return Err(CryptoError::Invalid),
    };
    let e = envelope(
        &s,
        &p.message_id,
        encrypted.serialize().to_vec(),
        kind,
        blob,
    );
    data::validate_payload_binding(p, &e).map_err(|_| CryptoError::Invalid)?;
    bootstrap::observe_traffic(tx, &s)?;
    tx.put(&history, &plaintext)?;
    tx.put(&outbox, &e.encode_to_vec())?;
    let (sequence, chain_digest) = chain::append(tx, &s, &e)?;
    tx.put(
        &journal_key(&s.own, &id)?,
        &Journal {
            format: FORMAT,
            receipt: None,
            status: None,
            removal: None,
            revision: None,
            sequence: Some(sequence),
            chain_digest,
            sealed: None,
        }
        .encode_to_vec(),
    )?;
    save_link(tx, &s.own, &id, &e, true)?;
    tx.project_recovery(&history, false)?;
    tx.project_recovery(&outbox, true)?;
    history::index(tx, &s.own, &id, p)?;
    finish(tx, &e)?;
    plan(tx, &s.own, &s.scope, &id)
}

pub(in crate::encryption::signal_state) enum OutgoingCommand {
    FileProgress(FileProgressCommand),
    Fence(
        Box<EpochObservation<EpochOutgoingPlan, v2::EpochSealedFailure>>,
        Reply<()>,
    ),
    Statuses(
        Box<crate::encryption::native::EpochStatuses>,
        Reply<Vec<EpochOutgoingPlan>>,
    ),
    Encrypt(
        Box<(EpochScope, v2::EncryptedPayload)>,
        Reply<EpochOutgoingPlan>,
    ),
    Plan(String, Reply<EpochOutgoingPlan>),
    Page(Option<EpochOutboxCursor>, Reply<EpochOutboxPage>),
    Receipt(
        Box<EpochObservation<EpochOutgoingPlan, v2::SendReceipt>>,
        Reply<()>,
    ),
}
impl Engine {
    pub(super) fn handle_epoch_outgoing(&mut self, c: OutgoingCommand) {
        match c {
            OutgoingCommand::FileProgress(c) => self.handle_file_progress(c),
            OutgoingCommand::Fence(o, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_epoch_send_fence(*o));
                }
            }
            OutgoingCommand::Statuses(b, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_epoch_statuses(*b));
                }
            }
            OutgoingCommand::Encrypt(args, r) => {
                if !r.is_closed() {
                    let (s, p) = *args;
                    let _ = r.send(self.encrypt_epoch(s, p));
                }
            }
            OutgoingCommand::Plan(id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_outgoing_plan(&id));
                }
            }
            OutgoingCommand::Page(cursor, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_outbox_page(cursor));
                }
            }
            OutgoingCommand::Receipt(b, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_epoch_send_receipt(*b));
                }
            }
        }
    }
    fn encrypt_epoch(
        &mut self,
        s: EpochScope,
        p: v2::EncryptedPayload,
    ) -> CryptoResult<EpochOutgoingPlan> {
        self.check_epoch_owner(&s)?;
        let runtime = &self.runtime;
        self.store
            .transaction_with(|tx| encrypt_message(tx, runtime, &s, &p))
    }
    fn epoch_outgoing_plan(&mut self, uuid: &str) -> CryptoResult<EpochOutgoingPlan> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let l = read_link(tx, own, uuid)?;
            let p = plan(tx, own, &store, &l.id)?;
            check_projections(tx, own, &l.id, p.needs_status_poll())?;
            Ok(p)
        })
    }
    fn epoch_outbox_page(&mut self, c: Option<EpochOutboxCursor>) -> CryptoResult<EpochOutboxPage> {
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
                RecordKind::Outbox,
                "v2:",
                c.as_ref().map(|c| c.after.as_str()),
            )?;
            let next = if ids.len() == 32 {
                Some(EpochOutboxCursor {
                    scope: store.clone(),
                    own: own.clone(),
                    after: ids.last().unwrap().clone(),
                })
            } else {
                None
            };
            let mut messages = Vec::with_capacity(ids.len());
            for id in ids {
                let p = plan(tx, own, &store, &id)?;
                check_projections(tx, own, &id, p.needs_status_poll())?;
                if !p.needs_status_poll() {
                    return Err(CryptoError::Invalid);
                }
                messages.push(p);
            }
            Ok(EpochOutboxPage { messages, next })
        })
    }
    fn accept_epoch_send_receipt(
        &mut self,
        b: EpochObservation<EpochOutgoingPlan, v2::SendReceipt>,
    ) -> CryptoResult<()> {
        let p = b.request();
        self.check_epoch_owner(&p.scope)?;
        if b.store_scope() != self.store.scope() {
            return Err(CryptoError::Conflict);
        }
        let store = self.store.scope().clone();
        let own = &self.own;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let e = p.request.envelope.as_ref().ok_or(CryptoError::Invalid)?;
            let id = record_id(&p.scope, &e.message_id)?;
            let saved = plan(tx, own, &store, &id)?;
            check_projections(tx, own, &id, saved.needs_status_poll())?;
            if saved.request != p.request || saved.sealed.is_some() {
                return Err(CryptoError::Conflict);
            }
            data::validate_receipt(&saved.request, b.response())
                .map_err(|_| CryptoError::Invalid)?;
            if let Some(old) = &saved.receipt {
                if old != b.response() {
                    return Err(CryptoError::Conflict);
                }
                return file_progress::track_receipt(tx, own, &id, &saved.request, old, false);
            }
            tx.put(
                &journal_key(own, &id)?,
                &Journal {
                    format: FORMAT,
                    receipt: Some(b.response().clone()),
                    status: None,
                    removal: None,
                    revision: None,
                    sequence: saved.sequence,
                    chain_digest: saved.chain_digest,
                    sealed: None,
                }
                .encode_to_vec(),
            )?;
            file_progress::track_receipt(tx, own, &id, &saved.request, b.response(), true)?;
            // Server acceptance stops sending, but still needs status polling.
            Ok(())
        })
    }
}
impl CryptoCoordinator {
    pub async fn encrypt_epoch(
        &self,
        s: EpochScope,
        p: v2::EncryptedPayload,
    ) -> CryptoResult<EpochOutgoingPlan> {
        if p.encoded_len() > crate::encryption::contract::MAX_REQUEST_BYTES {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::Encrypt(
                Box::new((s, p)),
                r,
            )))
        })
        .await
    }
    pub async fn epoch_outgoing_plan(&self, uuid: String) -> CryptoResult<EpochOutgoingPlan> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::Plan(uuid, r))))
            .await
    }
    pub async fn epoch_outbox_page(
        &self,
        c: Option<EpochOutboxCursor>,
    ) -> CryptoResult<EpochOutboxPage> {
        self.request(|r| Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::Page(c, r))))
            .await
    }
    pub async fn accept_epoch_send_receipt(
        &self,
        b: EpochObservation<EpochOutgoingPlan, v2::SendReceipt>,
    ) -> CryptoResult<()> {
        self.request(|r| {
            Command::Epoch(EpochCommand::Outgoing(OutgoingCommand::Receipt(
                Box::new(b),
                r,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_outgoing_tests.rs"]
pub(super) mod tests;
