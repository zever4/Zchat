//! Initial protected v2 catalogue. All access runs on the existing account lane.
//! Opening a DB never seeds this state; only an authenticated epoch0 observation
//! can initialize it. Signed recovery freezes sending; an explicit bounded audit
//! precedes atomic epoch activation/archive. V2 data always rechecks this catalogue.
use super::*;
use crate::{
    encryption::{
        epoch_rpc::{self, EpochConversation},
        native::EpochObservation,
    },
    proto::e2ee_v2 as v2,
};

#[path = "epoch_bootstrap.rs"]
mod bootstrap;
pub use bootstrap::EpochAcquisitionPlan;

#[path = "epoch_outgoing.rs"]
mod outgoing;
pub(super) use outgoing::index_data_record;
pub use outgoing::{
    EpochFileProgress, EpochFileStatusCursor, EpochFileStatusPage, FileStatusSeedProgress,
};
pub use outgoing::{EpochOutboxCursor, EpochOutboxPage, EpochOutgoingPlan};

#[path = "epoch_text.rs"]
mod text;
pub use text::{EpochTextPreparation, EpochTextQueued};

#[path = "epoch_attachment_prepare.rs"]
mod attachment_prepare;
pub use attachment_prepare::{
    EpochAttachmentBlob, EpochAttachmentInputState, EpochAttachmentPlan,
    EpochAttachmentPreparation, EpochAttachmentPromotion, EpochAttachmentRetryPlan,
    EpochBlobControlPlan, EpochBlobProgress, EpochBlobUpload,
};

#[path = "epoch_attachment_download.rs"]
mod attachment_download;
#[path = "epoch_history.rs"]
mod history;
pub use attachment_download::{
    AttachmentCacheState, AttachmentDownloadError, EpochAttachmentCache,
    EpochAttachmentDownloadPlan, EpochAttachmentFile, EpochDurableFileAck, EpochFileAckCursor,
    EpochFileAckPage,
};
pub use history::HistorySeedProgress;
#[path = "epoch_views.rs"]
mod views;
pub use views::{
    HistoryItem, MessageProgress, MessageViewStatus, PendingAttachment, ViewArchive, ViewCursor,
    ViewDialog, ViewPage, ViewRequest, ViewSegment,
};

#[path = "epoch_incoming.rs"]
mod incoming;
pub(super) use incoming::index_ack_record;
pub use incoming::{
    EpochAckCursor, EpochAckPage, EpochDurableAck, EpochQuarantineReason, EpochQuarantinedMessage,
};

#[path = "epoch_confirmation.rs"]
mod confirmation;

#[cfg(test)]
#[path = "epoch_attachment_tests.rs"]
mod attachment_tests;

#[cfg(test)]
#[path = "epoch_file_status_transport_tests.rs"]
mod file_status_transport_tests;

#[path = "epoch_recovery.rs"]
mod recovery;
pub use recovery::{
    EpochRecoveryDecision, EpochRecoveryIntent, EpochRecoveryOverview, EpochRecoveryQuery,
    EpochRecoverySnapshot, EpochTransitionProgress, EpochTransitionQuery,
};

#[path = "epoch_archive.rs"]
mod archive;
#[path = "epoch_outgoing_chain.rs"]
mod chain;
#[path = "epoch_dispatch.rs"]
mod dispatch;
pub use dispatch::{EpochConversationCursor, EpochConversationPage};

const FORMAT: u32 = 2;

/// Opaque local snapshot for a read RPC, not a signed recovery intent. A stale
/// response cannot overwrite a changed root or create a replacement session.
#[derive(Clone)]
pub struct EpochCatalogQuery {
    own: DeviceRef,
    scope: StoreScope,
    pins: EpochConversation,
    own_public: Vec<u8>,
    request: v2::GetConversationEpochsRequest,
    expected: Option<[u8; 32]>,
}
impl EpochCatalogQuery {
    pub fn request(&self) -> &v2::GetConversationEpochsRequest {
        &self.request
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope
    }
    pub(in crate::encryption) fn pins(&self) -> &EpochConversation {
        &self.pins
    }
}

/// Committed epoch locator. No public constructor/Deserialize and no v1 proof
/// conversion. Future crypto operations must recheck the current protected root,
/// closure and freeze; a cached token alone does not authorize a Send or bootstrap.
#[derive(Clone)]
pub struct EpochScope {
    conversation: String,
    epoch: u64,
    own: DeviceRef,
    peer: DeviceRef,
    own_identity: IdentityKey,
    peer_identity: IdentityKey,
    peer_registration: u32,
    scope: StoreScope,
    receive_only: bool,
    send_frozen: bool,
    session_id: String,
    acquisition_id: String,
    sealed: bool,
    bootstrap_device: Option<DeviceRef>,
    retired_by: Option<String>,
    outgoing_count: u64,
}
impl EpochScope {
    pub fn conversation_id(&self) -> &str {
        &self.conversation
    }
    pub fn crypto_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn own_device(&self) -> &DeviceRef {
        &self.own
    }
    pub fn peer_device(&self) -> &DeviceRef {
        &self.peer
    }
    pub fn own_identity(&self) -> IdentityKey {
        self.own_identity
    }
    pub fn peer_identity(&self) -> IdentityKey {
        self.peer_identity
    }
    pub fn peer_registration(&self) -> u32 {
        self.peer_registration
    }
    pub fn store_scope(&self) -> &StoreScope {
        &self.scope
    }
    pub fn receive_only(&self) -> bool {
        self.receive_only
    }
    pub fn sealed(&self) -> bool {
        self.sealed
    }
    pub fn sending_frozen(&self) -> bool {
        self.send_frozen
    }
    /// A scheduling hint only; bootstrap reloads current state inside its TX.
    pub fn bootstrap_initiator(&self) -> bool {
        self.epoch > 0 && self.bootstrap_device.as_ref() == Some(&self.own)
    }
    pub(in crate::encryption) fn conversation_cursor(&self) -> EpochConversationCursor {
        dispatch::cursor(self)
    }
    pub fn session_record_id(&self) -> &str {
        &self.session_id
    }
    pub fn acquisition_record_id(&self) -> &str {
        &self.acquisition_id
    }
}

#[derive(Message)]
struct Root {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    conversation: Option<Conversation>,
    #[prost(bytes = "vec", tag = "3")]
    own_public: Vec<u8>,
    #[prost(uint32, tag = "4")]
    own_registration: u32,
    #[prost(uint64, optional, tag = "5")]
    active_epoch: Option<u64>,
    #[prost(uint64, optional, tag = "6")]
    revision: Option<u64>,
    #[prost(uint64, tag = "7")]
    local_revision: u64,
    #[prost(bytes = "vec", tag = "8")]
    row_digest: Vec<u8>,
    #[prost(bool, optional, tag = "9")]
    send_frozen: Option<bool>,
    #[prost(bytes = "vec", tag = "10")]
    recovery_digest: Vec<u8>,
    #[prost(uint64, optional, tag = "11")]
    outgoing_count: Option<u64>,
    #[prost(bytes = "vec", tag = "12")]
    outgoing_head: Vec<u8>,
}
#[derive(Message)]
struct Anchor {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(uint64, tag = "2")]
    local_revision: u64,
    #[prost(bytes = "vec", tag = "3")]
    root_digest: Vec<u8>,
}
#[derive(Message, PartialEq)]
struct EpochRow {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    conversation_id: String,
    #[prost(message, optional, tag = "3")]
    own: Option<DeviceRef>,
    #[prost(message, optional, tag = "4")]
    peer: Option<DeviceRef>,
    #[prost(message, optional, tag = "5")]
    epoch: Option<v2::EpochRecord>,
    #[prost(message, optional, tag = "6")]
    created_by: Option<v2::RecoveryRecord>,
    #[prost(message, optional, tag = "7")]
    sealed_by: Option<v2::RecoveryRecord>,
    #[prost(uint64, optional, tag = "8")]
    outgoing_count: Option<u64>,
    #[prost(bytes = "vec", tag = "9")]
    outgoing_head: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
    recovery_digest: Vec<u8>,
}
struct Loaded {
    root: Root,
    row: EpochRow,
    digest: [u8; 32],
}

fn prefix(chat: &str) -> CryptoResult<String> {
    if !epoch_rpc::uuid4(chat) {
        return Err(CryptoError::Invalid);
    }
    Ok(format!("epoch-v2:{chat}:"))
}
fn key(own: &DeviceRef, chat: &str, suffix: &str) -> CryptoResult<RecordKey> {
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("{}{suffix}", prefix(chat)?),
    )?)
}
#[cfg(test)]
fn row_key(own: &DeviceRef, chat: &str) -> CryptoResult<RecordKey> {
    archive::row_key(own, chat, 0)
}
fn save_root(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    root: &mut Root,
) -> CryptoResult<()> {
    let chat = root
        .conversation
        .as_ref()
        .ok_or(CryptoError::Invalid)?
        .conversation_id
        .clone();
    root.local_revision = root
        .local_revision
        .checked_add(1)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    let bytes = root.encode_to_vec();
    let anchor = Anchor {
        format: FORMAT,
        local_revision: root.local_revision,
        root_digest: digest(b"zchat.e2ee.epoch-root.v2\0", &bytes).to_vec(),
    };
    tx.put(&key(own, &chat, "head")?, &bytes)?;
    tx.put(&key(own, &chat, "anchor")?, &anchor.encode_to_vec())?;
    Ok(())
}
fn digest(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(domain);
    h.update(bytes);
    h.finalize().into()
}

// Fixed indexed probes, not a history scan. Future v2 data records share the
// opaque chat prefix; residual state cannot be silently treated as a fresh epoch0.
fn has_v2_state(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    chat: &str,
) -> CryptoResult<bool> {
    if tx.has_record_prefix(&own.generation_id, RecordKind::Control, &prefix(chat)?)? {
        return Ok(true);
    }
    for p in [
        "delivery-out:",
        "delivery-in:",
        "delivery-acked:",
        "delivery-progress:",
        "session-confirmation:",
    ] {
        if tx.has_record_prefix(
            &own.generation_id,
            RecordKind::Control,
            &format!("{p}v2:{chat}:"),
        )? {
            return Ok(true);
        }
    }
    let prefix = format!("v2:{chat}:");
    for kind in [
        RecordKind::Session,
        RecordKind::Acquisition,
        RecordKind::History,
        RecordKind::Outbox,
        RecordKind::Inbox,
    ] {
        if tx.has_record_prefix(&own.generation_id, kind, &prefix)? {
            return Ok(true);
        }
    }
    Ok(false)
}
pub(super) fn reject_prototype_scope(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    chat: &str,
) -> CryptoResult<()> {
    if has_v2_state(tx, own, chat)? {
        return Err(CryptoError::DataProtocolConflict);
    }
    Ok(())
}
fn reject_old_state(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    c: &Conversation,
) -> CryptoResult<()> {
    // Reuse the existing key locators ONLY to reject prototype evidence. Never
    // parse/adopt its ratchet, acquisition, ciphertext, or use a v1 hash as epoch0.
    let old = SessionScope::build(c, None)?;
    if tx.get(&old.peer_key(RecordKind::Session, true)?)?.is_some()
        || tx.get(&old.acquisition_key()?)?.is_some()
        || recovery_indexes::has_traffic(tx, own, &c.conversation_id)?
    {
        return Err(CryptoError::DataProtocolConflict);
    }
    Ok(())
}
fn identity(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
) -> CryptoResult<(IdentityKey, u32)> {
    if tx.account_id().to_string() != own.user_id || own.device_id != 1 {
        return Err(CryptoError::Invalid);
    }
    let bytes = tx
        .get(&RecordKey::new(
            &own.generation_id,
            RecordKind::Identity,
            "local",
        )?)?
        .ok_or(CryptoError::Invalid)?;
    if bytes.len() < 6 || bytes[0] != 1 {
        return Err(CryptoError::Invalid);
    }
    let registration =
        u32::from_be_bytes(bytes[1..5].try_into().map_err(|_| CryptoError::Invalid)?);
    if !(1..=16383).contains(&registration) {
        return Err(CryptoError::Invalid);
    }
    Ok((
        *IdentityKeyPair::try_from(&bytes[5..])?.identity_key(),
        registration,
    ))
}
fn pins(c: &Conversation, own_public: IdentityKey) -> CryptoResult<EpochConversation> {
    let peer = c.peer_device.as_ref().ok_or(CryptoError::Invalid)?;
    EpochConversation::new(
        c.clone(),
        own_public,
        IdentityKey::try_from(peer.identity_key.as_slice())?,
    )
    .map_err(|_| CryptoError::Invalid)
}
fn request(own: &DeviceRef, chat: &str) -> v2::GetConversationEpochsRequest {
    v2::GetConversationEpochsRequest {
        context: Some(ClientContext {
            protocol_version: 2,
            device_generation_id: own.generation_id.clone(),
        }),
        conversation_id: chat.into(),
        after_epoch: None,
        limit: 1,
    }
}
fn catalogue(root: &Root, row: &EpochRow) -> v2::ConversationEpochs {
    v2::ConversationEpochs {
        protocol_version: 2,
        conversation: root.conversation.clone(),
        active_epoch: root.active_epoch,
        epoch_revision: root.revision,
        epochs: row.epoch.clone().into_iter().collect(),
        next_after_epoch: None,
    }
}
fn load(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    c: &Conversation,
    own_public: IdentityKey,
    registration: u32,
) -> CryptoResult<Option<Loaded>> {
    let chat = &c.conversation_id;
    let bytes = tx.get(&key(own, chat, "head")?)?;
    let anchor = tx.get(&key(own, chat, "anchor")?)?;
    let (bytes, anchor) = match (bytes, anchor) {
        (None, None) if !has_v2_state(tx, own, chat)? => return Ok(None),
        (Some(b), Some(a)) => (b, a),
        _ => return Err(CryptoError::Invalid),
    };
    if bytes.len() > 4096 || anchor.len() > 128 {
        return Err(CryptoError::Invalid);
    }
    let root = Root::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let anchor = Anchor::decode(anchor.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let epoch = root
        .active_epoch
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    let row = archive::read(tx, own, c, own_public, epoch, epoch)?;
    let hash = digest(b"zchat.e2ee.epoch-root.v2\0", &bytes);
    if root.format != FORMAT
        || anchor.format != FORMAT
        || row.format != FORMAT
        || root.send_frozen.is_none()
        || root.local_revision == 0
        || root.local_revision > i64::MAX as u64
        || anchor.local_revision != root.local_revision
        || anchor.root_digest != hash
        || root.row_digest != digest(b"zchat.e2ee.epoch-row.v2\0", &row.encode_to_vec())
        || root.own_public != own_public.serialize().as_ref()
        || root.own_registration != registration
        || row.conversation_id != *chat
        || row.own.as_ref() != Some(own)
        || row.peer.as_ref() != c.peer_device.as_ref().and_then(|d| d.reference.as_ref())
    {
        return Err(CryptoError::Conflict);
    }
    let saved = root.conversation.as_ref().ok_or(CryptoError::Invalid)?;
    conversation_lifecycle::validate(saved, own)?;
    conversation_lifecycle::merge(saved, c)?;
    if saved.conversation_id != *chat {
        return Err(CryptoError::Conflict);
    }
    if root.revision != row.epoch.as_ref().and_then(|r| r.revision) {
        return Err(CryptoError::Conflict);
    }
    chain::validate_root(tx, own, &root)?;
    let pins = pins(saved, own_public)?;
    epoch_rpc::validate_catalog(
        &archive::catalog_request(own, chat, epoch, 1),
        &catalogue(&root, &row),
        &pins,
    )
    .map_err(|_| CryptoError::Invalid)?;
    recovery::validate_head(tx, own, &root, &pins)?;
    reject_old_state(tx, own, c)?;
    Ok(Some(Loaded {
        root,
        row,
        digest: hash,
    }))
}
fn slot_id(
    domain: &[u8],
    chat: &str,
    epoch: u64,
    own: &DeviceRef,
    peer: &DeviceRef,
) -> CryptoResult<String> {
    if !epoch_rpc::uuid4(chat)
        || epoch > i64::MAX as u64
        || own.device_id != 1
        || peer.device_id != 1
        || own.user_id == peer.user_id
        || own.generation_id == peer.generation_id
        || [
            &own.user_id,
            &own.generation_id,
            &peer.user_id,
            &peer.generation_id,
        ]
        .iter()
        .any(|id| !epoch_rpc::uuid4(id))
    {
        return Err(CryptoError::Invalid);
    }
    let mut h = Sha256::new();
    h.update(domain);
    h.update(2u32.to_be_bytes());
    h.update(1u32.to_be_bytes());
    h.update(chat.as_bytes());
    h.update(epoch.to_be_bytes());
    for d in [own, peer] {
        h.update(d.user_id.as_bytes());
        h.update(d.generation_id.as_bytes());
        h.update(d.device_id.to_be_bytes());
    }
    let hash: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Ok(format!("v2:{chat}:{epoch:016x}:{hash}"))
}
fn scope(
    loaded: &Loaded,
    current: &Conversation,
    store_scope: StoreScope,
) -> CryptoResult<EpochScope> {
    scope_with(loaded, current, store_scope, false)
}
// Presentation-only scopes stay inside cohort validators; no public read page
// can return one. All mutation paths retain scope()'s own-closure rejection.
fn scope_with(
    loaded: &Loaded,
    current: &Conversation,
    store_scope: StoreScope,
    history_only: bool,
) -> CryptoResult<EpochScope> {
    let c = conversation_lifecycle::merge(
        loaded
            .root
            .conversation
            .as_ref()
            .ok_or(CryptoError::Invalid)?,
        current,
    )?;
    let own = c.own_device.clone().ok_or(CryptoError::Invalid)?;
    let peer = c.peer_device.as_ref().ok_or(CryptoError::Invalid)?;
    let reference = peer.reference.clone().ok_or(CryptoError::Invalid)?;
    if !history_only && c.closed_generation_id == own.generation_id {
        return Err(CryptoError::ArchivedGeneration);
    }
    Ok(EpochScope {
        conversation: c.conversation_id.clone(),
        epoch: loaded.root.active_epoch.ok_or(CryptoError::Invalid)?,
        session_id: slot_id(
            b"zchat.e2ee.signal-session.v2\0",
            &c.conversation_id,
            loaded.root.active_epoch.ok_or(CryptoError::Invalid)?,
            &own,
            &reference,
        )?,
        acquisition_id: slot_id(
            b"zchat.e2ee.acquisition-journal.v2\0",
            &c.conversation_id,
            loaded.root.active_epoch.ok_or(CryptoError::Invalid)?,
            &own,
            &reference,
        )?,
        own,
        peer: reference,
        own_identity: IdentityKey::try_from(loaded.root.own_public.as_slice())?,
        peer_identity: IdentityKey::try_from(peer.identity_key.as_slice())?,
        peer_registration: peer.registration_id,
        scope: store_scope,
        receive_only: history_only || c.state == ConversationState::Closed as i32,
        send_frozen: loaded.root.send_frozen.ok_or(CryptoError::Invalid)? || history_only,
        outgoing_count: loaded.root.outgoing_count.ok_or(CryptoError::Invalid)?,
        sealed: loaded
            .row
            .epoch
            .as_ref()
            .ok_or(CryptoError::Invalid)?
            .sealed,
        retired_by: loaded
            .row
            .epoch
            .as_ref()
            .filter(|e| e.sealed)
            .map(|e| e.sealed_by_proposal_id.clone()),
        bootstrap_device: loaded
            .row
            .epoch
            .as_ref()
            .ok_or(CryptoError::Invalid)?
            .bootstrap_device
            .clone(),
    })
}

pub(super) enum EpochCommand {
    AttachmentDownload(attachment_download::DownloadCommand),
    SeedHistory(Reply<HistorySeedProgress>),
    ReadViews(uuid::Uuid, ViewRequest, Reply<ViewPage>),
    Text(
        Box<(String, String, Zeroizing<String>)>,
        Reply<EpochTextPreparation>,
    ),
    Attachment(attachment_prepare::AttachmentCommand),
    Active(String, Reply<Option<EpochScope>>),
    Conversations(
        Option<EpochConversationCursor>,
        Reply<EpochConversationPage>,
    ),
    Recovery(recovery::RecoveryCommand),
    Incoming(incoming::IncomingCommand),
    Outgoing(outgoing::OutgoingCommand),
    Bootstrap(bootstrap::BootstrapCommand),
    PrepareInitial(String, Reply<EpochCatalogQuery>),
    AcceptInitial(
        Box<EpochObservation<EpochCatalogQuery, v2::ConversationEpochs>>,
        Reply<EpochScope>,
    ),
    Scope(String, u64, Reply<EpochScope>),
}
impl Engine {
    pub(super) fn handle_epoch(&mut self, command: EpochCommand) {
        match command {
            EpochCommand::AttachmentDownload(command) => self.handle_attachment_download(command),
            EpochCommand::Attachment(command) => self.handle_attachment_prepare(command),
            EpochCommand::SeedHistory(r) => {
                if !r.is_closed() {
                    let _ = r.send(self.seed_history_index());
                }
            }
            EpochCommand::ReadViews(instance, query, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.read_views(instance, query));
                }
            }
            EpochCommand::Text(args, r) => {
                if !r.is_closed() {
                    let (chat, id, text) = *args;
                    let _ = r.send(self.prepare_epoch_text(&chat, &id, &text));
                }
            }
            EpochCommand::Active(id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.active_epoch_scope(&id));
                }
            }
            EpochCommand::Conversations(c, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_conversation_page(c));
                }
            }
            EpochCommand::Recovery(command) => self.handle_epoch_recovery(command),
            EpochCommand::Incoming(command) => self.handle_epoch_incoming(command),
            EpochCommand::Outgoing(command) => self.handle_epoch_outgoing(command),
            EpochCommand::Bootstrap(command) => self.handle_epoch_bootstrap(command),
            EpochCommand::PrepareInitial(id, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.prepare_initial_epoch(&id));
                }
            }
            EpochCommand::AcceptInitial(value, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.accept_initial_epoch(*value));
                }
            }
            EpochCommand::Scope(id, epoch, r) => {
                if !r.is_closed() {
                    let _ = r.send(self.epoch_scope(&id, epoch));
                }
            }
        }
    }
    fn prepare_initial_epoch(&mut self, chat: &str) -> CryptoResult<EpochCatalogQuery> {
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        prefix(chat)?;
        self.store.transaction_with(|tx| {
            let c = conversation_lifecycle::load(tx, own, chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let (own_public, registration) = identity(tx, own)?;
            reject_old_state(tx, own, &c)?;
            let saved = load(tx, own, &c, own_public, registration)?;
            let c = if let Some(saved) = &saved {
                conversation_lifecycle::merge(saved.root.conversation.as_ref().unwrap(), &c)?
            } else {
                c
            };
            if c.closed_generation_id == own.generation_id {
                return Err(CryptoError::ArchivedGeneration);
            }
            Ok(EpochCatalogQuery {
                own: own.clone(),
                scope: store_scope,
                pins: pins(&c, own_public)?,
                own_public: own_public.serialize().to_vec(),
                request: request(own, chat),
                expected: saved.map(|s| s.digest),
            })
        })
    }
    fn accept_initial_epoch(
        &mut self,
        observed: EpochObservation<EpochCatalogQuery, v2::ConversationEpochs>,
    ) -> CryptoResult<EpochScope> {
        let query = observed.request();
        let response = observed.response();
        if observed.store_scope() != self.store.scope()
            || &query.scope != self.store.scope()
            || query.own != self.own
            || query.request != request(&self.own, &query.request.conversation_id)
        {
            return Err(CryptoError::Conflict);
        }
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let chat = &query.request.conversation_id;
            let c = conversation_lifecycle::load(tx, own, chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let (own_public, registration) = identity(tx, own)?;
            if query.own_public != own_public.serialize().as_ref() {
                return Err(CryptoError::Conflict);
            }
            conversation_lifecycle::merge(query.pins.conversation(), &c)?;
            epoch_rpc::validate_catalog(&query.request, response, &query.pins)
                .map_err(|_| CryptoError::Invalid)?;
            if response.active_epoch != Some(0) || response.epoch_revision != Some(0) {
                return Err(CryptoError::EpochRecoveryRequired);
            }
            reject_old_state(tx, own, &c)?;
            let old = load(tx, own, &c, own_public, registration)?;
            if old.as_ref().map(|s| s.digest) != query.expected {
                return Err(CryptoError::Conflict);
            }
            let mut value = conversation_lifecycle::merge(
                response.conversation.as_ref().ok_or(CryptoError::Invalid)?,
                &c,
            )?;
            if let Some(old) = &old {
                value =
                    conversation_lifecycle::merge(old.root.conversation.as_ref().unwrap(), &value)?;
            }
            if value.closed_generation_id == own.generation_id {
                return Err(CryptoError::ArchivedGeneration);
            }
            let row = EpochRow {
                format: FORMAT,
                conversation_id: chat.clone(),
                own: Some(own.clone()),
                peer: value.peer_device.as_ref().and_then(|d| d.reference.clone()),
                epoch: response.epochs.first().cloned(),
                created_by: None,
                sealed_by: None,
                outgoing_count: None,
                outgoing_head: vec![],
                recovery_digest: vec![],
            };
            if old.as_ref().is_some_and(|s| s.row != row) {
                return Err(CryptoError::Conflict);
            }
            if let Some(old) = old
                .as_ref()
                .filter(|old| old.root.conversation.as_ref() == Some(&value))
            {
                return scope(old, &value, store_scope);
            }
            let root = Root {
                format: FORMAT,
                conversation: Some(value.clone()),
                own_public: own_public.serialize().to_vec(),
                own_registration: registration,
                active_epoch: Some(0),
                revision: Some(0),
                local_revision: old
                    .as_ref()
                    .map_or(Some(1), |old| old.root.local_revision.checked_add(1))
                    .filter(|v| *v <= i64::MAX as u64)
                    .ok_or(CryptoError::Invalid)?,
                row_digest: digest(b"zchat.e2ee.epoch-row.v2\0", &row.encode_to_vec()).to_vec(),
                send_frozen: Some(
                    old.as_ref()
                        .is_some_and(|s| s.root.send_frozen == Some(true)),
                ),
                outgoing_count: Some(old.as_ref().map_or(0, |o| o.root.outgoing_count.unwrap())),
                outgoing_head: old
                    .as_ref()
                    .map_or_else(|| chain::EMPTY.to_vec(), |o| o.root.outgoing_head.clone()),
                recovery_digest: match &old {
                    Some(old) => old.root.recovery_digest.clone(),
                    None => recovery::initialize(tx, own, chat, 0)?,
                },
            };
            let bytes = root.encode_to_vec();
            let hash = digest(b"zchat.e2ee.epoch-root.v2\0", &bytes);
            let anchor = Anchor {
                format: FORMAT,
                local_revision: root.local_revision,
                root_digest: hash.to_vec(),
            };
            if old.is_none() {
                archive::save(tx, own, &row)?;
            }
            tx.put(&key(own, chat, "head")?, &bytes)?;
            tx.put(&key(own, chat, "anchor")?, &anchor.encode_to_vec())?;
            scope(
                &Loaded {
                    root,
                    row,
                    digest: hash,
                },
                &value,
                store_scope,
            )
        })
    }
    fn epoch_scope(&mut self, chat: &str, epoch: u64) -> CryptoResult<EpochScope> {
        prefix(chat)?;
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let c = conversation_lifecycle::load(tx, own, chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let (identity, registration) = identity(tx, own)?;
            let state = archive::load_at(tx, own, &c, identity, registration, epoch)?;
            scope(&state, &c, store_scope)
        })
    }
}
impl CryptoCoordinator {
    pub async fn prepare_initial_epoch(&self, chat: String) -> CryptoResult<EpochCatalogQuery> {
        prefix(&chat)?;
        self.request(|r| Command::Epoch(EpochCommand::PrepareInitial(chat, r)))
            .await
    }
    pub async fn accept_initial_epoch(
        &self,
        observed: EpochObservation<EpochCatalogQuery, v2::ConversationEpochs>,
    ) -> CryptoResult<EpochScope> {
        self.request(|r| Command::Epoch(EpochCommand::AcceptInitial(Box::new(observed), r)))
            .await
    }
    pub async fn epoch_scope(&self, chat: String, epoch: u64) -> CryptoResult<EpochScope> {
        prefix(&chat)?;
        self.request(|r| Command::Epoch(EpochCommand::Scope(chat, epoch, r)))
            .await
    }
}

#[cfg(test)]
#[path = "account_owner_tests.rs"]
mod owner_tests;
#[cfg(test)]
#[path = "epoch_runner_tests.rs"]
mod runner_tests;
#[cfg(test)]
#[path = "epoch_state_tests.rs"]
mod tests;
