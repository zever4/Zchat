//! Transaction-bound Signal stores with separate prototype/v2 session slots. Only the dedicated account worker mutates
//! protocol state; the renderer/network never receives a generic store interface.
//! Registration preparation/publication RPC progress and server delivery are separate.
use super::{
    contract::{validate_conversation, validate_envelope, validate_payload_binding},
    native::ExpiredAcquisition,
    prekey_maintenance::{InventoryRefill, RefillPolicy, VerifiedPreKeyInventory},
    protected::{
        ProtectedStore, ProtectedTransaction, RecordKey, RecordKind, StoreError, StoreScope,
    },
};
#[cfg(test)]
use crate::proto::e2ee::ConversationMode;
use crate::proto::e2ee::{
    AcquireBundleRequest, AcquiredBundle, CiphertextType, ClientContext, Conversation,
    ConversationState, CryptoSuite, DeviceRef, EncryptedPayload, Envelope,
};
use async_trait::async_trait;
use libsignal_protocol::{
    CiphertextMessage, DeviceId, Direction, GenericSignedPreKey, IdentityChange, IdentityKey,
    IdentityKeyPair, IdentityKeyStore, KyberPreKeyId, KyberPreKeyRecord, KyberPreKeyStore,
    PreKeyBundle, PreKeyId, PreKeyRecord, PreKeySignalMessage, PreKeyStore, ProtocolAddress,
    PublicKey, SessionRecord, SessionStore, SignalMessage, SignalProtocolError, SignedPreKeyId,
    SignedPreKeyRecord, SignedPreKeyStore, kem, message_decrypt, message_encrypt,
    process_prekey_bundle,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    rc::Rc,
    sync::mpsc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;
use zeroize::Zeroizing;

#[path = "archive_owner.rs"]
mod archive_owner;
pub use archive_owner::{ArchiveClient, ArchiveOwner};

#[path = "conversation_creation.rs"]
mod conversation_creation;
pub use conversation_creation::{ConversationCreation, VerifiedConversationCreation};

#[path = "conversation_lifecycle.rs"]
mod conversation_lifecycle;
pub use conversation_lifecycle::{ConversationLookup, VerifiedConversationLookup};

#[path = "epoch_state.rs"]
mod epoch_state;
pub use epoch_state::{
    AttachmentCacheState, AttachmentDownloadError, EpochAckCursor, EpochAckPage,
    EpochAcquisitionPlan, EpochAttachmentBlob, EpochAttachmentCache, EpochAttachmentDownloadPlan,
    EpochAttachmentFile, EpochAttachmentInputState, EpochAttachmentPlan,
    EpochAttachmentPreparation, EpochAttachmentPromotion, EpochAttachmentRetryPlan,
    EpochBlobControlPlan, EpochBlobProgress, EpochBlobUpload, EpochCatalogQuery,
    EpochConversationCursor, EpochConversationPage, EpochDurableAck, EpochDurableFileAck,
    EpochFileAckCursor, EpochFileAckPage, EpochFileProgress, EpochFileStatusCursor,
    EpochFileStatusPage, EpochOutboxCursor, EpochOutboxPage, EpochOutgoingPlan,
    EpochQuarantineReason, EpochQuarantinedMessage, EpochRecoveryDecision, EpochRecoveryIntent,
    EpochRecoveryOverview, EpochRecoveryQuery, EpochRecoverySnapshot, EpochScope,
    EpochTextPreparation, EpochTextQueued, EpochTransitionProgress, EpochTransitionQuery,
    FileStatusSeedProgress, HistoryItem, HistorySeedProgress, MessageProgress, MessageViewStatus,
    PendingAttachment, ViewArchive, ViewCursor, ViewDialog, ViewPage, ViewRequest, ViewSegment,
};

#[path = "delivery.rs"]
mod delivery;
pub use delivery::{
    AckPage, DurableAck, OutboxPage, OutgoingPlan, OutgoingProgress, QuarantineReason, VerifiedAck,
    VerifiedMessageStatuses, VerifiedSendReceipt,
};

#[path = "session_confirmation.rs"]
mod session_confirmation;

#[path = "recovery_indexes.rs"]
mod recovery_indexes;
pub use recovery_indexes::RecoveryIndexPage;

#[path = "prekey_state.rs"]
mod prekey_state;
pub use prekey_state::{PreKeyRedactionCursor, PreKeyRedactionPage};

#[path = "prekey_ids.rs"]
mod prekey_ids;
pub use prekey_ids::{PreKeyIdNamespace, PreKeyIdReservation, PreKeyIdSeedPage};

#[path = "prekey_refill.rs"]
pub(super) mod prekey_refill;
pub use prekey_refill::{
    PreKeyPublicationPlan, PreKeyRefillPlan, PreKeyRefillUpload, PreKeyRotationPlan,
    VerifiedPreKeyRefillUpload,
};

type SignalResult<T> = Result<T, SignalProtocolError>;
pub type CryptoResult<T> = Result<T, CryptoError>;

const ACQUISITION_LEASE_MS: i64 = 86_400_000;
// Only the future-horizon sanity check tolerates clock skew. Do not add this
// allowance to the absolute expiry check or persist a longer server deadline.
const ACQUISITION_CLOCK_SKEW_MS: i64 = 60_000;
fn check_acquisition_deadline(not_after_ms: i64, now_ms: i64) -> CryptoResult<()> {
    if not_after_ms <= now_ms {
        return Err(CryptoError::Expired);
    }
    if not_after_ms > now_ms.saturating_add(ACQUISITION_LEASE_MS + ACQUISITION_CLOCK_SKEW_MS) {
        return Err(CryptoError::AcquisitionClockSkew);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("protected history index is still initializing")]
    HistoryIndexNotReady,
    #[error("protected crypto storage failed")]
    Storage(#[from] StoreError),
    #[error("signal operation failed")]
    Signal(#[from] SignalProtocolError),
    #[error("invalid crypto operation binding or persisted state")]
    Invalid,
    #[error("operation identifier conflicts with saved data")]
    Conflict,
    #[error("bootstrap acquisition has expired")]
    Expired,
    #[error("acquisition deadline is inconsistent with the local clock")]
    AcquisitionClockSkew,
    #[error("prekey ID initialization has not completed")]
    PreKeyIdsNotReady,
    #[error("recovery index migration has not completed")]
    RecoveryIndexesNotReady,
    #[error("sender file status index initialization has not completed")]
    FileStatusIndexNotReady,
    #[error("prekey ID namespace exhausted; IDs cannot be reused")]
    PreKeyIdsExhausted,
    #[error("this generation is archived; use local history without restarting its crypto worker")]
    ArchivedGeneration,
    #[error("the conversation is closed; explicitly create a new dialogue")]
    ConversationClosed,
    #[error("a Signal session already exists; do not bootstrap it again")]
    SessionExists,
    #[error("no durable local message exists for this operation")]
    MessageUnavailable,
    #[error("the local attachment copy was explicitly removed")]
    AttachmentRemoved,
    #[error("local attachment removal awaits a confirmed file acknowledgement")]
    AttachmentAckPending,
    #[error("local attachment removal awaits a terminal send result")]
    AttachmentSendPending,
    #[error("no authenticated local conversation index exists for this operation")]
    ConversationUnavailable,
    #[error("the epoch transition requires signed recovery and protected local intent")]
    EpochRecoveryRequired,
    #[error(
        "this conversation contains state from a different data protocol; automatic conversion is forbidden"
    )]
    DataProtocolConflict,
    #[error("epoch sending is frozen pending explicit signed recovery")]
    EpochSendingFrozen,
    #[error("retained recovery journal capacity reached")]
    EpochRecoveryCapacity,
    #[error("message is durably quarantined; no automatic decrypt or disposition change")]
    Quarantined,
    #[error("crypto worker queue is full")]
    Busy,
    #[error("crypto worker stopped")]
    Stopped,
}

/// Opaque binding minted from the committed authenticated conversation index.
/// A cached active scope cannot override a later durable closure.
#[derive(Clone)]
pub struct SessionScope {
    conversation: String,
    own: DeviceRef,
    peer: DeviceRef,
    peer_identity: IdentityKey,
    peer_registration: u32,
    store_scope: Option<StoreScope>,
    receive_only: bool,
}

impl SessionScope {
    #[cfg(test)]
    pub(in crate::encryption) fn from_conversation(
        conversation: &Conversation,
    ) -> CryptoResult<Self> {
        if conversation.state != ConversationState::Active as i32 {
            return Err(CryptoError::Invalid);
        }
        Self::build(conversation, None)
    }
    fn from_index(conversation: &Conversation, store_scope: StoreScope) -> CryptoResult<Self> {
        let scope = Self::build(conversation, Some(store_scope))?;
        if conversation.closed_generation_id == scope.own.generation_id {
            return Err(CryptoError::ArchivedGeneration);
        }
        Ok(scope)
    }
    fn build(conversation: &Conversation, store_scope: Option<StoreScope>) -> CryptoResult<Self> {
        validate_conversation(conversation).map_err(|_| CryptoError::Invalid)?;
        let own = conversation
            .own_device
            .clone()
            .ok_or(CryptoError::Invalid)?;
        let device = conversation
            .peer_device
            .as_ref()
            .ok_or(CryptoError::Invalid)?;
        Ok(Self {
            conversation: conversation.conversation_id.clone(),
            own,
            peer: device.reference.clone().ok_or(CryptoError::Invalid)?,
            peer_identity: IdentityKey::try_from(device.identity_key.as_slice())?,
            peer_registration: device.registration_id,
            receive_only: conversation.state == ConversationState::Closed as i32,
            store_scope,
        })
    }

    fn envelope(&self, id: &str, ciphertext: Vec<u8>, kind: CiphertextType) -> Envelope {
        Envelope {
            protocol_version: 1,
            crypto_suite: CryptoSuite::SignalPq1 as i32,
            message_id: id.into(),
            conversation_id: self.conversation.clone(),
            sender: Some(self.own.clone()),
            recipient: Some(self.peer.clone()),
            message_type: kind as i32,
            ciphertext,
        }
    }

    fn address(&self) -> ProtocolAddress {
        address(&self.peer)
    }
    fn check_owner(&self, tx: &ProtectedTransaction<'_>) -> CryptoResult<()> {
        if tx.account_id().to_string() != self.own.user_id {
            return Err(CryptoError::Invalid);
        }
        Ok(())
    }
    fn key(&self, kind: RecordKind, id: &str) -> Result<RecordKey, StoreError> {
        RecordKey::new(&self.own.generation_id, kind, id)
    }
    fn peer_key(&self, kind: RecordKind, session: bool) -> Result<RecordKey, StoreError> {
        peer_record_key(
            &self.own,
            &self.peer,
            kind,
            session.then_some(self.conversation.as_str()),
        )
    }

    fn acquisition_key(&self) -> Result<RecordKey, StoreError> {
        acquisition_key_for(&self.own, &self.conversation, &self.peer)
    }

    fn check_bundle(&self, bundle: &AcquiredBundle) -> CryptoResult<()> {
        if bundle.encoded_len() > 8192
            || bundle.conversation_id != self.conversation
            || bundle.device.as_ref().and_then(|d| d.reference.as_ref()) != Some(&self.peer)
            || bundle.device.as_ref().is_none_or(|d| {
                d.identity_key != self.peer_identity.serialize().as_ref()
                    || d.registration_id != self.peer_registration
                    || !d.public_material_ready
                    || d.public_material_revision == 0
            })
        {
            return Err(CryptoError::Invalid);
        }
        Ok(())
    }
}

// Generation-wide trust metadata is shared by both data protocols. Only the
// retired prototype session key includes its chat under this historical domain;
// v2 sessions always use EpochScope's separate, epoch-bound locator.
fn peer_record_key(
    own: &DeviceRef,
    peer: &DeviceRef,
    kind: RecordKind,
    prototype_chat: Option<&str>,
) -> Result<RecordKey, StoreError> {
    let mut h = Sha256::new();
    h.update(b"zchat.e2ee.signal-view.v1\0");
    if let Some(chat) = prototype_chat {
        h.update(chat.as_bytes());
    }
    h.update(peer.user_id.as_bytes());
    h.update(peer.generation_id.as_bytes());
    h.update(peer.device_id.to_be_bytes());
    let id: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    RecordKey::new(&own.generation_id, kind, &id)
}

fn acquisition_key_for(
    own: &DeviceRef,
    conversation: &str,
    peer: &DeviceRef,
) -> Result<RecordKey, StoreError> {
    RecordKey::new(conversation, RecordKind::Control, "uuid")?;
    RecordKey::new(&peer.user_id, RecordKind::Control, "uuid")?;
    RecordKey::new(&peer.generation_id, RecordKind::Control, "uuid")?;
    let mut h = Sha256::new();
    h.update(b"zchat.e2ee.acquire-journal.v1\0");
    h.update(conversation.as_bytes());
    h.update(peer.user_id.as_bytes());
    h.update(peer.generation_id.as_bytes());
    h.update(peer.device_id.to_be_bytes());
    let digest: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
    RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("acquire:{digest}"),
    )
}

#[derive(Message)]
struct AcquisitionJournal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    request: Option<AcquireBundleRequest>,
    #[prost(bytes = "vec", tag = "3")]
    peer_identity: Vec<u8>,
    #[prost(uint32, tag = "4")]
    peer_registration: u32,
    // One-step idempotence after a renewal COMMIT/lost local response. Older
    // snapshots decode as empty; no schema/key migration or private GC is needed.
    #[prost(string, tag = "5")]
    replaces_acquisition_id: String,
    #[prost(uint32, tag = "6")]
    activity_guard_version: u32,
    #[prost(bool, tag = "7")]
    has_message_state: bool,
}

/// A durable request for the trusted native RPC layer. Applied plans must not
/// fetch another bundle or invoke process_prekey_bundle a second time.
pub struct AcquisitionPlan {
    pub request: AcquireBundleRequest,
    pub applied: bool,
}
fn load_acquisition(
    tx: &mut ProtectedTransaction<'_>,
    scope: &SessionScope,
) -> CryptoResult<Option<AcquireBundleRequest>> {
    Ok(load_acquisition_journal(tx, scope)?.and_then(|journal| journal.request))
}
fn validate_acquisition_journal(
    saved: &AcquisitionJournal,
    own: &DeviceRef,
    conversation: &str,
    peer: &DeviceRef,
) -> CryptoResult<()> {
    let request = saved.request.as_ref().ok_or(CryptoError::Invalid)?;
    if saved.format != 1
        || request.conversation_id != conversation
        || request.expected_peer.as_ref() != Some(peer)
        || request.context.as_ref()
            != Some(&ClientContext {
                protocol_version: 1,
                device_generation_id: own.generation_id.clone(),
            })
    {
        return Err(CryptoError::Conflict);
    }
    RecordKey::new(&request.acquisition_id, RecordKind::Acquisition, "id")?;
    if !saved.replaces_acquisition_id.is_empty() {
        RecordKey::new(
            &saved.replaces_acquisition_id,
            RecordKind::Acquisition,
            "id",
        )?;
        if saved.replaces_acquisition_id == request.acquisition_id {
            return Err(CryptoError::Conflict);
        }
    }
    if saved.activity_guard_version > 1
        || peer.device_id != 1
        || !(1..=16383).contains(&saved.peer_registration)
    {
        return Err(CryptoError::Invalid);
    }
    IdentityKey::try_from(saved.peer_identity.as_slice())?;
    Ok(())
}
fn load_acquisition_journal(
    tx: &mut ProtectedTransaction<'_>,
    scope: &SessionScope,
) -> CryptoResult<Option<AcquisitionJournal>> {
    let Some(bytes) = tx.get(&scope.acquisition_key()?)? else {
        return Ok(None);
    };
    let saved = AcquisitionJournal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    validate_acquisition_journal(&saved, &scope.own, &scope.conversation, &scope.peer)?;
    if saved.peer_identity != scope.peer_identity.serialize().as_ref()
        || saved.peer_registration != scope.peer_registration
    {
        return Err(CryptoError::Conflict);
    }
    if recovery_indexes::ready(tx, &scope.own)? {
        if saved.activity_guard_version == 0 {
            return Err(CryptoError::Invalid);
        }
        tx.check_recovery_projection(&scope.acquisition_key()?, false)?;
    }

    Ok(Some(saved))
}

fn address(reference: &DeviceRef) -> ProtocolAddress {
    // SessionScope enforces MVP device 1; other references are checked on worker start.
    ProtocolAddress::new(
        reference.user_id.clone(),
        DeviceId::new(1).expect("constant valid device"),
    )
}
#[derive(Debug, thiserror::Error)]
#[error("protected storage callback failed")]
struct CallbackFailure;
fn storage_error(_e: StoreError) -> SignalProtocolError {
    // rusqlite/io errors can contain !UnwindSafe dynamic sources; do not pass
    // them through libsignal's UnwindSafe callback boundary or expose values.
    SignalProtocolError::ApplicationCallbackError("protected state", Box::new(CallbackFailure))
}
fn invalid() -> SignalProtocolError {
    SignalProtocolError::InvalidArgument("invalid protected protocol state".into())
}

#[derive(Clone)]
struct View<'a, 'db> {
    tx: Rc<RefCell<&'a mut ProtectedTransaction<'db>>>,
    own: DeviceRef,
    peer_address: ProtocolAddress,
    peer_identity: IdentityKey,
    identity_key: RecordKey,
    session_key: RecordKey,
}
impl<'a, 'db> View<'a, 'db> {
    fn prototype(
        tx: &'a mut ProtectedTransaction<'db>,
        scope: &SessionScope,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            tx: Rc::new(RefCell::new(tx)),
            own: scope.own.clone(),
            peer_address: scope.address(),
            peer_identity: scope.peer_identity,
            identity_key: scope.peer_key(RecordKind::PeerIdentity, false)?,
            session_key: scope.peer_key(RecordKind::Session, true)?,
        })
    }
    fn epoch(
        tx: &'a mut ProtectedTransaction<'db>,
        scope: &EpochScope,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            tx: Rc::new(RefCell::new(tx)),
            own: scope.own_device().clone(),
            peer_address: address(scope.peer_device()),
            peer_identity: scope.peer_identity(),
            identity_key: peer_record_key(
                scope.own_device(),
                scope.peer_device(),
                RecordKind::PeerIdentity,
                None,
            )?,
            session_key: RecordKey::new(
                &scope.own_device().generation_id,
                RecordKind::Session,
                scope.session_record_id(),
            )?,
        })
    }
    fn record_key(&self, kind: RecordKind, id: &str) -> SignalResult<RecordKey> {
        RecordKey::new(&self.own.generation_id, kind, id).map_err(storage_error)
    }
    fn finish<T>(&self, result: SignalResult<T>) -> SignalResult<T> {
        if result.is_err() {
            self.tx.borrow_mut().abort();
        }
        result
    }
    fn apply<T>(
        &self,
        f: impl FnOnce(&mut ProtectedTransaction<'_>) -> SignalResult<T>,
    ) -> SignalResult<T> {
        let mut tx = self.tx.borrow_mut();
        let result = f(&mut tx);
        if result.is_err() {
            tx.abort();
        }
        result
    }
    fn check(&self, peer: &ProtocolAddress) -> SignalResult<()> {
        if peer != &self.peer_address {
            return Err(invalid());
        }
        Ok(())
    }
    fn key(&self, kind: RecordKind, id: u32) -> SignalResult<RecordKey> {
        self.record_key(kind, &id.to_string())
    }
    fn load(
        &self,
        kind: RecordKind,
        id: u32,
        missing: SignalProtocolError,
    ) -> SignalResult<Zeroizing<Vec<u8>>> {
        self.apply(|tx| {
            let bytes = tx
                .get(&self.key(kind, id)?)
                .map_err(storage_error)?
                .ok_or(missing)?;
            if !matches!(
                prekey_state::decode(kind, id, &bytes).map_err(prekey_error)?,
                prekey_state::Saved::Live { .. }
            ) {
                return Err(match kind {
                    RecordKind::PqPreKey => SignalProtocolError::InvalidKyberPreKeyId,
                    _ => SignalProtocolError::InvalidPreKeyId,
                });
            }
            Ok(bytes)
        })
    }
    fn save(&self, kind: RecordKind, id: u32, bytes: &[u8]) -> SignalResult<()> {
        self.apply(|tx| {
            prekey_state::save(tx, &self.own.generation_id, kind, id, bytes, None)
                .map_err(prekey_error)
        })
    }
}

fn prekey_error(error: CryptoError) -> SignalProtocolError {
    match error {
        CryptoError::Storage(error) => storage_error(error),
        CryptoError::Signal(error) => error,
        _ => invalid(),
    }
}

#[async_trait(?Send)]
impl IdentityKeyStore for View<'_, '_> {
    async fn get_identity_key_pair(&self) -> SignalResult<IdentityKeyPair> {
        self.apply(|tx| {
            let bytes = tx
                .get(&self.record_key(RecordKind::Identity, "local")?)
                .map_err(storage_error)?
                .ok_or_else(invalid)?;
            if bytes.len() < 6 || bytes[0] != 1 {
                return Err(invalid());
            }
            IdentityKeyPair::try_from(&bytes[5..])
        })
    }
    async fn get_local_registration_id(&self) -> SignalResult<u32> {
        self.apply(|tx| {
            let bytes = tx
                .get(&self.record_key(RecordKind::Identity, "local")?)
                .map_err(storage_error)?
                .ok_or_else(invalid)?;
            if bytes.len() < 6 || bytes[0] != 1 {
                return Err(invalid());
            }
            Ok(u32::from_be_bytes(
                bytes[1..5].try_into().map_err(|_| invalid())?,
            ))
        })
    }
    async fn get_identity(&self, peer: &ProtocolAddress) -> SignalResult<Option<IdentityKey>> {
        self.apply(|tx| {
            self.check(peer)?;
            tx.get(&self.identity_key)
                .map_err(storage_error)?
                .map(|b| IdentityKey::try_from(b.as_slice()))
                .transpose()
        })
    }
    async fn is_trusted_identity(
        &self,
        peer: &ProtocolAddress,
        identity: &IdentityKey,
        _: Direction,
    ) -> SignalResult<bool> {
        self.finish(self.check(peer))?;
        if identity != &self.peer_identity {
            return Ok(false);
        }
        Ok(self
            .get_identity(peer)
            .await?
            .is_none_or(|old| old == *identity))
    }
    async fn save_identity(
        &mut self,
        peer: &ProtocolAddress,
        identity: &IdentityKey,
    ) -> SignalResult<IdentityChange> {
        let old = self.get_identity(peer).await?;
        self.apply(|tx| {
            if identity != &self.peer_identity || old.is_some_and(|old| old != *identity) {
                return Err(SignalProtocolError::UntrustedIdentity(peer.clone()));
            }
            tx.put(&self.identity_key, &identity.serialize())
                .map_err(storage_error)?;
            Ok(IdentityChange::NewOrUnchanged)
        })
    }
}

#[async_trait(?Send)]
impl SessionStore for View<'_, '_> {
    async fn load_session(&self, peer: &ProtocolAddress) -> SignalResult<Option<SessionRecord>> {
        self.apply(|tx| {
            self.check(peer)?;
            tx.get(&self.session_key)
                .map_err(storage_error)?
                .map(|b| SessionRecord::deserialize(&b))
                .transpose()
        })
    }
    async fn store_session(
        &mut self,
        peer: &ProtocolAddress,
        record: &SessionRecord,
    ) -> SignalResult<()> {
        self.apply(|tx| {
            self.check(peer)?;
            let bytes = Zeroizing::new(record.serialize()?);
            tx.put(&self.session_key, &bytes).map_err(storage_error)
        })
    }
}
#[async_trait(?Send)]
impl PreKeyStore for View<'_, '_> {
    async fn get_pre_key(&self, id: PreKeyId) -> SignalResult<PreKeyRecord> {
        let bytes = self.load(
            RecordKind::EcPreKey,
            id.into(),
            SignalProtocolError::InvalidPreKeyId,
        )?;
        self.finish((|| {
            let record = PreKeyRecord::deserialize(&bytes[3..])?;
            if record.id()? != id || bytes[2] != 0 {
                return Err(invalid());
            }
            Ok(record)
        })())
    }
    async fn save_pre_key(&mut self, id: PreKeyId, record: &PreKeyRecord) -> SignalResult<()> {
        self.finish((|| {
            if record.id()? != id {
                return Err(invalid());
            }
            self.save(
                RecordKind::EcPreKey,
                id.into(),
                &Zeroizing::new(record.serialize()?),
            )
        })())
    }
    async fn remove_pre_key(&mut self, id: PreKeyId) -> SignalResult<()> {
        let bytes = self.load(
            RecordKind::EcPreKey,
            id.into(),
            SignalProtocolError::InvalidPreKeyId,
        )?;
        self.apply(|tx| {
            tx.put(
                &self.key(RecordKind::EcPreKey, id.into())?,
                &prekey_state::tombstone(RecordKind::EcPreKey, id.into(), &bytes[3..]),
            )
            .map_err(storage_error)
        })
    }
}
#[async_trait(?Send)]
impl SignedPreKeyStore for View<'_, '_> {
    async fn get_signed_pre_key(&self, id: SignedPreKeyId) -> SignalResult<SignedPreKeyRecord> {
        let bytes = self.load(
            RecordKind::SignedPreKey,
            id.into(),
            SignalProtocolError::InvalidSignedPreKeyId,
        )?;
        self.finish((|| {
            let record = SignedPreKeyRecord::deserialize(&bytes[3..])?;
            if record.id()? != id || bytes[1] != 0 || bytes[2] != 0 {
                return Err(invalid());
            }
            Ok(record)
        })())
    }
    async fn save_signed_pre_key(
        &mut self,
        id: SignedPreKeyId,
        record: &SignedPreKeyRecord,
    ) -> SignalResult<()> {
        self.finish((|| {
            if record.id()? != id {
                return Err(invalid());
            }
            self.save(
                RecordKind::SignedPreKey,
                id.into(),
                &Zeroizing::new(record.serialize()?),
            )
        })())
    }
}
#[async_trait(?Send)]
impl KyberPreKeyStore for View<'_, '_> {
    async fn get_kyber_pre_key(&self, id: KyberPreKeyId) -> SignalResult<KyberPreKeyRecord> {
        let bytes = self.load(
            RecordKind::PqPreKey,
            id.into(),
            SignalProtocolError::InvalidKyberPreKeyId,
        )?;
        self.finish((|| {
            let record = KyberPreKeyRecord::deserialize(&bytes[3..])?;
            if record.id()? != id || (bytes[2] == 1 && bytes[1] != 0) {
                return Err(invalid());
            }
            Ok(record)
        })())
    }
    async fn save_kyber_pre_key(
        &mut self,
        id: KyberPreKeyId,
        record: &KyberPreKeyRecord,
    ) -> SignalResult<()> {
        self.finish((|| {
            if record.id()? != id {
                return Err(invalid());
            }
            self.save(
                RecordKind::PqPreKey,
                id.into(),
                &Zeroizing::new(record.serialize()?),
            )
        })())
    }
    async fn mark_kyber_pre_key_used(
        &mut self,
        id: KyberPreKeyId,
        ec: SignedPreKeyId,
        base: &PublicKey,
    ) -> SignalResult<()> {
        let bytes = self.load(
            RecordKind::PqPreKey,
            id.into(),
            SignalProtocolError::InvalidKyberPreKeyId,
        )?;
        self.apply(|tx| {
            if bytes[2] == 1 {
                let mut h = Sha256::new();
                h.update(b"zchat.e2ee.pq-use.v1\0");
                h.update(u32::from(id).to_be_bytes());
                h.update(u32::from(ec).to_be_bytes());
                h.update(base.serialize());
                let digest: String = h.finalize().iter().map(|b| format!("{b:02x}")).collect();
                let usage = self.record_key(RecordKind::Control, &format!("pq-use:{digest}"))?;
                if tx.get(&usage).map_err(storage_error)?.is_some() {
                    return Err(SignalProtocolError::DuplicatedMessage(id.into(), ec.into()));
                }
                tx.put(&usage, &[1]).map_err(storage_error)?;
            } else {
                tx.put(
                    &self.key(RecordKind::PqPreKey, id.into())?,
                    &prekey_state::tombstone(RecordKind::PqPreKey, id.into(), &bytes[3..]),
                )
                .map_err(storage_error)?;
            }
            Ok(())
        })
    }
}

/// Material must have been durably prepared before registration/publication. This
/// layer installs supplied material; it never generates/replenishes keys on reopen.
/// Initial registration preparation/bind is implemented by DeviceProvisioning,
/// which shares this install transaction with its persisted publication plan.
pub struct DeviceMaterial {
    pub identity: IdentityKeyPair,
    pub registration_id: u32,
    pub credential: Zeroizing<[u8; 32]>,
    pub signed: SignedPreKeyRecord,
    pub ec: Vec<PreKeyRecord>,
    pub pq: Vec<(KyberPreKeyRecord, bool)>,
}

/// Install supplied keys in the caller's existing transaction; registration bind
/// and the publication plan must commit with these private records.
pub(super) fn install_material(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    material: DeviceMaterial,
) -> CryptoResult<()> {
    if tx.account_id().to_string() != own.user_id || own.device_id != 1 {
        return Err(CryptoError::Invalid);
    }
    if material.ec.len() > 100
        || material.pq.len() > 100
        || !material.pq.iter().any(|(_, lr)| *lr)
        || material.pq.iter().filter(|(_, lr)| *lr).count() != 1
    {
        return Err(CryptoError::Invalid);
    }
    let generation = own.generation_id.clone();
    let identity_key = RecordKey::new(&generation, RecordKind::Identity, "local")?;
    let mut identity = Zeroizing::new(vec![1]);
    identity.extend_from_slice(&material.registration_id.to_be_bytes());
    identity.extend_from_slice(&material.identity.serialize());
    let credential = RecordKey::new(&generation, RecordKind::DeviceCredential, "local")?;
    let mut records = vec![
        (identity_key, identity, None),
        (
            credential,
            Zeroizing::new(material.credential.to_vec()),
            None,
        ),
    ];
    records.push((
        RecordKey::new(
            &generation,
            RecordKind::SignedPreKey,
            &u32::from(material.signed.id()?).to_string(),
        )?,
        Zeroizing::new([&[1, 0, 0], material.signed.serialize()?.as_slice()].concat()),
        Some((RecordKind::SignedPreKey, u32::from(material.signed.id()?))),
    ));
    if !material.identity.public_key().verify_signature(
        &material.signed.public_key()?.serialize(),
        &material.signed.signature()?,
    ) {
        return Err(CryptoError::Invalid);
    }
    for ec in &material.ec {
        records.push((
            RecordKey::new(
                &generation,
                RecordKind::EcPreKey,
                &u32::from(ec.id()?).to_string(),
            )?,
            Zeroizing::new([&[1, 0, 0], ec.serialize()?.as_slice()].concat()),
            Some((RecordKind::EcPreKey, u32::from(ec.id()?))),
        ));
    }
    for (pq, lr) in &material.pq {
        if pq.public_key()?.serialize().len() != 1569
            || !material
                .identity
                .public_key()
                .verify_signature(&pq.public_key()?.serialize(), &pq.signature()?)
        {
            return Err(CryptoError::Invalid);
        }
        records.push((
            RecordKey::new(
                &generation,
                RecordKind::PqPreKey,
                &u32::from(pq.id()?).to_string(),
            )?,
            Zeroizing::new([&[1, 0, u8::from(*lr)], pq.serialize()?.as_slice()].concat()),
            Some((RecordKind::PqPreKey, u32::from(pq.id()?))),
        ));
    }
    // Reject duplicate IDs before writing, including within one supplied batch.
    let mut ids = std::collections::HashSet::new();
    for (key, value, prekey) in &records {
        if !ids.insert(key.clone()) {
            return Err(CryptoError::Invalid);
        }
        if let Some((kind, id)) = prekey {
            prekey_state::save(
                tx,
                &generation,
                *kind,
                *id,
                &value[3..],
                Some(value[2] == 1),
            )?;
        } else if let Some(old) = tx.get(key)? {
            if old.as_slice() != value.as_slice() {
                return Err(CryptoError::Conflict);
            }
        } else {
            tx.put(key, value)?;
        }
    }
    Ok(())
}

struct Engine {
    store: ProtectedStore,
    own: DeviceRef,
    runtime: tokio::runtime::Runtime,
}
impl Engine {
    fn check(&self, scope: &SessionScope) -> CryptoResult<()> {
        if scope.own != self.own {
            return Err(CryptoError::Invalid);
        }
        if scope.store_scope.as_ref() == Some(self.store.scope()) {
            return Ok(());
        }
        #[cfg(test)]
        if scope.store_scope.is_none() {
            return Ok(());
        }
        Err(CryptoError::Invalid)
    }

    fn install(&mut self, material: DeviceMaterial) -> CryptoResult<()> {
        self.store
            .transaction_with(|tx| install_material(tx, &self.own, material))
    }
    fn prepare_acquisition(&mut self, scope: &SessionScope) -> CryptoResult<AcquisitionPlan> {
        self.check(scope)?;
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, &scope.own)?;
            scope.check_owner(tx)?;
            scope.check_sending(tx)?;
            let saved = load_acquisition(tx, scope)?;
            let applied = if let Some(request) = &saved {
                if let Some(bytes) =
                    tx.get(&scope.key(RecordKind::Acquisition, &request.acquisition_id)?)?
                {
                    let bundle = AcquiredBundle::decode(bytes.as_slice())
                        .map_err(|_| CryptoError::Invalid)?;
                    scope.check_bundle(&bundle)?;
                    if bundle.acquisition_id != request.acquisition_id {
                        return Err(CryptoError::Conflict);
                    }
                    true
                } else {
                    false
                }
            } else {
                false
            };
            let view = View::prototype(tx, scope)?;
            let exists = runtime.block_on(async {
                view.get_identity_key_pair().await?;
                let exists = view.load_session(&scope.address()).await?.is_some();
                if applied
                    && (!exists
                        || view.get_identity(&scope.address()).await? != Some(scope.peer_identity))
                {
                    return Err(invalid());
                }
                Ok::<_, SignalProtocolError>(exists)
            })?;
            drop(view);
            if exists && !applied {
                return Err(CryptoError::SessionExists);
            }
            if let Some(request) = saved {
                return Ok(AcquisitionPlan { request, applied });
            }
            if recovery_indexes::has_traffic(tx, &scope.own, &scope.conversation)? {
                return Err(CryptoError::Conflict);
            }
            let request = AcquireBundleRequest {
                context: Some(ClientContext {
                    protocol_version: 1,
                    device_generation_id: scope.own.generation_id.clone(),
                }),
                conversation_id: scope.conversation.clone(),
                acquisition_id: uuid::Uuid::new_v4().to_string(),
                expected_peer: Some(scope.peer.clone()),
            };
            let journal = AcquisitionJournal {
                format: 1,
                request: Some(request.clone()),
                peer_identity: scope.peer_identity.serialize().to_vec(),
                peer_registration: scope.peer_registration,
                replaces_acquisition_id: String::new(),
                activity_guard_version: 1,
                has_message_state: false,
            };
            tx.put(&scope.acquisition_key()?, &journal.encode_to_vec())?;
            tx.project_recovery(&scope.acquisition_key()?, false)?;
            Ok(AcquisitionPlan {
                request,
                applied: false,
            })
        })
    }
    fn renew_acquisition(
        &mut self,
        scope: &SessionScope,
        expired: &ExpiredAcquisition,
    ) -> CryptoResult<AcquisitionPlan> {
        self.check(scope)?;
        let expected = expired.request();
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, &scope.own)?;
            scope.check_owner(tx)?;
            scope.check_sending(tx)?;
            let mut journal = load_acquisition_journal(tx, scope)?.ok_or(CryptoError::Conflict)?;
            let current = journal.request.as_ref().ok_or(CryptoError::Invalid)?;
            let mut same_binding = expected.clone();
            same_binding.acquisition_id = current.acquisition_id.clone();
            if same_binding != *current {
                return Err(CryptoError::Conflict);
            }
            if current.acquisition_id != expected.acquisition_id
                && journal.replaces_acquisition_id != expected.acquisition_id
            {
                return Err(CryptoError::Conflict);
            }
            // The worker serializes this check with bootstrap/decrypt/encrypt.
            // An inbound session wins even if its creation raced the expiry RPC.
            let view = View::prototype(tx, scope)?;
            runtime.block_on(view.get_identity_key_pair())?;
            if runtime
                .block_on(view.load_session(&scope.address()))?
                .is_some()
            {
                return Err(CryptoError::SessionExists);
            }
            if runtime
                .block_on(view.get_identity(&scope.address()))?
                .is_some_and(|identity| identity != scope.peer_identity)
            {
                return Err(CryptoError::Conflict);
            }
            drop(view);
            for id in [&current.acquisition_id, &expected.acquisition_id] {
                if tx.get(&scope.key(RecordKind::Acquisition, id)?)?.is_some() {
                    return Err(CryptoError::Conflict);
                }
            }
            // Sticky evidence lives in the mandatory acquisition journal, so a
            // missing ratchet never authorizes bootstrap over retained messages.
            if journal.activity_guard_version != 1
                || journal.has_message_state
                || recovery_indexes::has_traffic(tx, &scope.own, &scope.conversation)?
            {
                return Err(CryptoError::Conflict);
            }
            if current.acquisition_id != expected.acquisition_id {
                return Ok(AcquisitionPlan {
                    request: current.clone(),
                    applied: false,
                });
            }
            let mut request = current.clone();
            request.acquisition_id = uuid::Uuid::new_v4().to_string();
            journal.replaces_acquisition_id = expected.acquisition_id.clone();
            journal.request = Some(request.clone());
            tx.put(&scope.acquisition_key()?, &journal.encode_to_vec())?;
            tx.project_recovery(&scope.acquisition_key()?, false)?;
            Ok(AcquisitionPlan {
                request,
                applied: false,
            })
        })
    }
    fn bootstrap(&mut self, scope: &SessionScope, bundle: &AcquiredBundle) -> CryptoResult<()> {
        self.check(scope)?;
        scope.check_bundle(bundle)?;
        // Validate canonical UUID without pretending it is a device generation.
        RecordKey::new(&bundle.acquisition_id, RecordKind::Acquisition, "id")?;
        let receipt = scope.key(RecordKind::Acquisition, &bundle.acquisition_id)?;
        let runtime = &self.runtime;
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, &scope.own)?;
            scope.check_owner(tx)?;
            scope.check_sending(tx)?;
            let request = load_acquisition(tx, scope)?.ok_or(CryptoError::Invalid)?;
            if request.acquisition_id != bundle.acquisition_id {
                return Err(CryptoError::Conflict);
            }
            if let Some(old) = tx.get(&receipt)? {
                let saved =
                    AcquiredBundle::decode(old.as_slice()).map_err(|_| CryptoError::Invalid)?;
                if saved != *bundle {
                    return Err(CryptoError::Conflict);
                }
                // A receipt cannot repair lost/corrupted session or trust state.
                let view = View::prototype(tx, scope)?;
                runtime.block_on(async {
                    if view.load_session(&scope.address()).await?.is_none()
                        || view.get_identity(&scope.address()).await? != Some(scope.peer_identity)
                    {
                        return Err(invalid());
                    }
                    Ok(())
                })?;
                return Ok(());
            }
            // An incoming prekey message may already have established a session
            // while this RPC was in flight. Never overwrite that ratchet.
            let view = View::prototype(tx, scope)?;
            if runtime
                .block_on(view.load_session(&scope.address()))?
                .is_some()
            {
                return Err(CryptoError::SessionExists);
            }
            drop(view);
            if recovery_indexes::has_traffic(tx, &scope.own, &scope.conversation)?
                || load_acquisition_journal(tx, scope)?.is_some_and(|j| j.has_message_state)
            {
                return Err(CryptoError::Conflict);
            }
            let signed = bundle.signed_pre_key.as_ref().ok_or(CryptoError::Invalid)?;
            let pq = bundle.kyber_pre_key.as_ref().ok_or(CryptoError::Invalid)?;
            if signed.public_key.len() != 33
                || signed.signature.len() != 64
                || signed.created_at_unix_ms < 0
                || pq.public_key.len() != 1569
                || pq.signature.len() != 64
            {
                return Err(CryptoError::Invalid);
            }
            let ec = bundle
                .one_time_pre_key
                .as_ref()
                .map(|p| {
                    if p.public_key.len() != 33 || p.public_key.first() != Some(&5) {
                        return Err(invalid());
                    }
                    Ok::<_, SignalProtocolError>((
                        PreKeyId::from(p.key_id),
                        PublicKey::try_from(p.public_key.as_slice())?,
                    ))
                })
                .transpose()?;
            let signal = PreKeyBundle::new(
                scope.peer_registration,
                DeviceId::new(1).expect("valid constant"),
                ec,
                signed.key_id.into(),
                PublicKey::try_from(signed.public_key.as_slice())
                    .map_err(SignalProtocolError::from)?,
                signed.signature.clone(),
                pq.key_id.into(),
                kem::PublicKey::deserialize(&pq.public_key)?,
                pq.signature.clone(),
                scope.peer_identity,
            )?;
            // Check after lock acquisition, immediately before fresh bootstrap.
            // Exact applied replay above never repeats process_prekey_bundle.
            let now = SystemTime::now();
            let now_ms: i64 = now
                .duration_since(UNIX_EPOCH)
                .map_err(|_| CryptoError::Invalid)?
                .as_millis()
                .try_into()
                .map_err(|_| CryptoError::Invalid)?;
            check_acquisition_deadline(bundle.not_after_unix_ms, now_ms)?;
            let view = View::prototype(tx, scope)?;
            runtime.block_on(process_prekey_bundle(
                &scope.address(),
                &address(&scope.own),
                &mut view.clone(),
                &mut view.clone(),
                &signal,
                now,
                &mut rand::rng(),
            ))?;
            drop(view);
            tx.put(&receipt, &bundle.encode_to_vec())?;
            Ok(())
        })
    }
    fn encrypt(
        &mut self,
        scope: &SessionScope,
        payload: &EncryptedPayload,
    ) -> CryptoResult<Envelope> {
        self.check(scope)?;
        let runtime = &self.runtime;
        self.store
            .transaction_with(|tx| encrypt_message(tx, runtime, scope, payload))
    }
    fn decrypt(
        &mut self,
        scope: &SessionScope,
        envelope: &Envelope,
    ) -> CryptoResult<EncryptedPayload> {
        self.check(scope)?;
        let runtime = &self.runtime;
        self.store
            .transaction_with(|tx| decrypt_message(tx, runtime, scope, envelope))
    }
}

// Shared transaction primitive: confirmation encryption must commit together
// with the incoming ratchet/history/ACK, without a nested SQLite transaction.
fn encrypt_message(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    scope: &SessionScope,
    payload: &EncryptedPayload,
) -> CryptoResult<Envelope> {
    encrypt_message_at(tx, runtime, scope, payload, SystemTime::now())
}

// Keep the time input private to the transaction primitive. Production callers
// always use the wall clock above; tests can exercise libsignal's age boundary
// without changing the OS clock, persisted session bytes or protocol limits.
fn encrypt_message_at(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    scope: &SessionScope,
    payload: &EncryptedPayload,
    now: SystemTime,
) -> CryptoResult<Envelope> {
    recovery_indexes::require_ready(tx, &scope.own)?;
    let mut envelope = scope.envelope(&payload.message_id, vec![1], CiphertextType::Signal);
    validate_payload_binding(payload, &envelope).map_err(|_| CryptoError::Invalid)?;
    scope.check_owner(tx)?;
    scope.check_sending(tx)?;
    let history = scope.key(RecordKind::History, &payload.message_id)?;
    let outbox = scope.key(RecordKind::Outbox, &payload.message_id)?;
    delivery::check_outgoing_id(tx, scope, &payload.message_id)?;
    if let Some(old) = tx.get(&outbox)? {
        let saved = Envelope::decode(old.as_slice()).map_err(|_| CryptoError::Invalid)?;
        let old_payload = tx.get(&history)?.ok_or(CryptoError::Invalid)?;
        if EncryptedPayload::decode(old_payload.as_slice()).map_err(|_| CryptoError::Invalid)?
            != *payload
        {
            return Err(CryptoError::Conflict);
        }
        validate_payload_binding(payload, &saved).map_err(|_| CryptoError::Invalid)?;
        return Ok(saved);
    }
    if tx.get(&history)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    let view = View::prototype(tx, scope)?;
    let plaintext = Zeroizing::new(payload.encode_to_vec());
    let ciphertext = runtime.block_on(message_encrypt(
        &plaintext,
        &scope.address(),
        &address(&scope.own),
        &mut view.clone(),
        &mut view.clone(),
        now,
        &mut rand::rng(),
    ))?;
    envelope.message_type = match &ciphertext {
        CiphertextMessage::PreKeySignalMessage(_) => CiphertextType::PrekeySignal as i32,
        CiphertextMessage::SignalMessage(_) => CiphertextType::Signal as i32,
        _ => return Err(CryptoError::Invalid),
    };
    envelope.ciphertext = ciphertext.serialize().to_vec();
    validate_envelope(&envelope).map_err(|_| CryptoError::Invalid)?;
    drop(view);
    tx.put(&history, &plaintext)?;
    tx.put(&outbox, &envelope.encode_to_vec())?;
    tx.project_recovery(&history, false)?;
    tx.project_recovery(&outbox, true)?;
    recovery_indexes::observe(tx, &scope.own, &envelope)?;
    Ok(envelope)
}

fn decrypt_message(
    tx: &mut ProtectedTransaction<'_>,
    runtime: &tokio::runtime::Runtime,
    scope: &SessionScope,
    envelope: &Envelope,
) -> CryptoResult<EncryptedPayload> {
    recovery_indexes::require_ready(tx, &scope.own)?;
    validate_envelope(envelope).map_err(|_| CryptoError::Invalid)?;
    if envelope.conversation_id != scope.conversation
        || envelope.sender.as_ref() != Some(&scope.peer)
        || envelope.recipient.as_ref() != Some(&scope.own)
    {
        return Err(CryptoError::Invalid);
    }

    scope.check_owner(tx)?;
    scope.check_receiving(tx)?;
    delivery::guard_decrypt(tx, scope, envelope)?;
    let history = scope.key(RecordKind::History, &envelope.message_id)?;
    let inbox = scope.key(RecordKind::Inbox, &envelope.message_id)?;
    if let Some(old) = tx.get(&inbox)? {
        if Envelope::decode(old.as_slice()).map_err(|_| CryptoError::Invalid)? != *envelope {
            return Err(CryptoError::Conflict);
        }
        let saved = tx.get(&history)?.ok_or(CryptoError::Invalid)?;
        let payload =
            EncryptedPayload::decode(saved.as_slice()).map_err(|_| CryptoError::Invalid)?;
        validate_payload_binding(&payload, envelope).map_err(|_| CryptoError::Invalid)?;
        return Ok(payload);
    }
    if tx.get(&history)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    let ciphertext = match CiphertextType::try_from(envelope.message_type) {
        Ok(CiphertextType::PrekeySignal) => CiphertextMessage::PreKeySignalMessage(
            PreKeySignalMessage::try_from(envelope.ciphertext.as_slice())?,
        ),
        Ok(CiphertextType::Signal) => CiphertextMessage::SignalMessage(SignalMessage::try_from(
            envelope.ciphertext.as_slice(),
        )?),
        _ => return Err(CryptoError::Invalid),
    };
    let view = View::prototype(tx, scope)?;
    let plaintext = Zeroizing::new(runtime.block_on(message_decrypt(
        &ciphertext,
        &scope.address(),
        &address(&scope.own),
        &mut view.clone(),
        &mut view.clone(),
        &mut view.clone(),
        &view.clone(),
        &mut view.clone(),
        &mut rand::rng(),
    ))?);
    let payload =
        EncryptedPayload::decode(plaintext.as_slice()).map_err(|_| CryptoError::Invalid)?;
    validate_payload_binding(&payload, envelope).map_err(|_| CryptoError::Invalid)?;
    drop(view);
    tx.put(&history, &plaintext)?;
    tx.put(&inbox, &envelope.encode_to_vec())?;
    tx.project_recovery(&history, false)?;
    tx.project_recovery(&inbox, false)?;
    recovery_indexes::observe(tx, &scope.own, envelope)?;
    Ok(payload)
}

type Reply<T> = oneshot::Sender<CryptoResult<T>>;
enum Command {
    Epoch(epoch_state::EpochCommand),
    PrepareLookup(String, Reply<ConversationLookup>),
    AcceptLookup(VerifiedConversationLookup, Reply<Conversation>),
    ConversationScope(String, Reply<SessionScope>),
    Delivery(delivery::DeliveryCommand),
    Install(DeviceMaterial, Reply<()>),
    RedactPreKeys(Option<PreKeyRedactionCursor>, Reply<PreKeyRedactionPage>),
    SeedPreKeyIds(Reply<PreKeyIdSeedPage>),
    SeedRecoveryIndexes(Reply<RecoveryIndexPage>),
    ReservePreKeyIds(String, PreKeyIdNamespace, u32, Reply<PreKeyIdReservation>),
    PreparePreKeyRefill(u32, u32, Option<String>, Reply<PreKeyRefillPlan>),
    PreparePreKeyRotation(bool, bool, Option<String>, Reply<PreKeyRotationPlan>),
    CurrentPreKeyPublication(Reply<Option<PreKeyPublicationPlan>>),
    RefillFromInventory(
        VerifiedPreKeyInventory,
        RefillPolicy,
        Reply<InventoryRefill>,
    ),
    MaterializePreKeyRefill(String, Reply<PreKeyRefillPlan>),
    NextPreKeyRefillUpload(Reply<Option<PreKeyRefillUpload>>),
    AcceptPreKeyRefillUpload(VerifiedPreKeyRefillUpload, Reply<()>),
    PrepareConversation(i64, Option<String>, Reply<ConversationCreation>),
    PrepareConversationAfterClosed(i64, String, Reply<ConversationCreation>),
    AcceptConversation(VerifiedConversationCreation, Reply<Conversation>),
    PrepareAcquisition(SessionScope, Reply<AcquisitionPlan>),
    RenewAcquisition(SessionScope, ExpiredAcquisition, Reply<AcquisitionPlan>),
    Bootstrap(SessionScope, AcquiredBundle, Reply<()>),
    Encrypt(SessionScope, EncryptedPayload, Reply<Envelope>),
    Decrypt(SessionScope, Envelope, Reply<EncryptedPayload>),
}
/// One bounded worker per server/account. SQLite and !Send Signal futures stay
/// on this OS thread, outside the UI/network async executors. No network in a TX.
pub struct CryptoCoordinator {
    attachment_budget: std::sync::Arc<tokio::sync::Semaphore>,
    view_instance: uuid::Uuid,
    sender: mpsc::SyncSender<Command>,
    stopped: oneshot::Receiver<()>,
}
impl CryptoCoordinator {
    pub async fn start(store: ProtectedStore, own: DeviceRef) -> CryptoResult<Self> {
        if own.user_id != store.account_id().to_string() || own.device_id != 1 {
            return Err(CryptoError::Invalid);
        }
        RecordKey::new(&own.generation_id, RecordKind::Identity, "local")?;
        let (sender, receiver) = mpsc::sync_channel::<Command>(8);
        let (ready_tx, ready) = oneshot::channel();
        let (stopped_tx, stopped) = oneshot::channel();
        std::thread::Builder::new()
            .name("zchat-crypto".into())
            .spawn(move || {
                {
                    let runtime = match tokio::runtime::Builder::new_current_thread().build() {
                        Ok(runtime) => runtime,
                        Err(_) => {
                            let _ = ready_tx.send(Err(CryptoError::Stopped));
                            return;
                        }
                    };
                    let mut engine = Engine {
                        store,
                        own,
                        runtime,
                    };
                    let permitted = engine.store.transaction_with(|tx| {
                        if tx
                            .get(&RecordKey::new(
                                &engine.own.generation_id,
                                RecordKind::Control,
                                super::provisioning::ARCHIVED_DEVICE_RECORD,
                            )?)?
                            .is_some()
                        {
                            return Err(CryptoError::ArchivedGeneration);
                        }
                        Ok(())
                    });
                    if let Err(error) = permitted {
                        drop(engine); // Release the owner lock before reporting failure.
                        let _ = ready_tx.send(Err(error));
                        return;
                    }
                    if ready_tx.send(Ok(())).is_ok() {
                        while let Ok(command) = receiver.recv() {
                            match command {
                                Command::Epoch(command) => engine.handle_epoch(command),
                                Command::PrepareLookup(id, r) => {
                                    if !r.is_closed() {
                                        let _ = r.send(engine.prepare_lookup(&id));
                                    }
                                }
                                Command::AcceptLookup(proof, r) => {
                                    if !r.is_closed() {
                                        let _ = r.send(engine.accept_lookup(proof));
                                    }
                                }
                                Command::ConversationScope(id, r) => {
                                    if !r.is_closed() {
                                        let _ = r.send(engine.conversation_scope(&id));
                                    }
                                }
                                Command::Delivery(command) => engine.handle_delivery(command),
                                Command::Install(material, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.install(material));
                                    }
                                }
                                Command::RedactPreKeys(after, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.redact_consumed_prekeys(after));
                                    }
                                }
                                Command::SeedRecoveryIndexes(reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.seed_recovery_indexes());
                                    }
                                }
                                Command::SeedPreKeyIds(reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.seed_prekey_ids());
                                    }
                                }
                                Command::ReservePreKeyIds(operation, namespace, count, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(
                                            engine.reserve_prekey_ids(&operation, namespace, count),
                                        );
                                    }
                                }
                                Command::PreparePreKeyRefill(ec, pq, previous, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.prepare_prekey_refill(
                                            ec,
                                            pq,
                                            previous.as_deref(),
                                        ));
                                    }
                                }
                                Command::MaterializePreKeyRefill(id, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.materialize_prekey_refill(&id));
                                    }
                                }
                                Command::PreparePreKeyRotation(
                                    signed,
                                    last_resort,
                                    previous,
                                    reply,
                                ) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.prepare_prekey_rotation(
                                            signed,
                                            last_resort,
                                            previous.as_deref(),
                                        ));
                                    }
                                }
                                Command::CurrentPreKeyPublication(reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.current_prekey_publication());
                                    }
                                }
                                Command::RefillFromInventory(proof, policy, reply) => {
                                    if !reply.is_closed() {
                                        let _ =
                                            reply.send(engine.refill_from_inventory(proof, policy));
                                    }
                                }
                                Command::NextPreKeyRefillUpload(reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.next_prekey_refill_upload());
                                    }
                                }
                                Command::AcceptPreKeyRefillUpload(proof, reply) => {
                                    if !reply.is_closed() {
                                        let _ =
                                            reply.send(engine.accept_prekey_refill_upload(proof));
                                    }
                                }
                                Command::PrepareConversation(peer, previous, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(
                                            engine.prepare_conversation(peer, previous.as_deref()),
                                        );
                                    }
                                }
                                Command::AcceptConversation(verified, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.accept_conversation(verified));
                                    }
                                }
                                Command::PrepareConversationAfterClosed(peer, chat, reply) => {
                                    if !reply.is_closed() {
                                        let _ =
                                            reply
                                                .send(engine.prepare_conversation_after_closed(
                                                    peer, &chat,
                                                ));
                                    }
                                }
                                Command::PrepareAcquisition(scope, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.prepare_acquisition(&scope));
                                    }
                                }
                                Command::RenewAcquisition(scope, expired, reply) => {
                                    if !reply.is_closed() {
                                        let _ =
                                            reply.send(engine.renew_acquisition(&scope, &expired));
                                    }
                                }
                                Command::Bootstrap(scope, bundle, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.bootstrap(&scope, &bundle));
                                    }
                                }
                                Command::Encrypt(scope, payload, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.encrypt(&scope, &payload));
                                    }
                                }
                                Command::Decrypt(scope, envelope, reply) => {
                                    if !reply.is_closed() {
                                        let _ = reply.send(engine.decrypt(&scope, &envelope));
                                    }
                                }
                            }
                        }
                    }
                } // Drop connection/keys/runtime before shutdown acknowledgement.
                let _ = stopped_tx.send(());
            })
            .map_err(StoreError::from)?;
        ready.await.map_err(|_| CryptoError::Stopped)??;
        Ok(Self {
            attachment_budget: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
            sender,
            stopped,
            view_instance: uuid::Uuid::new_v4(),
        })
    }
    async fn request<T>(&self, command: impl FnOnce(Reply<T>) -> Command) -> CryptoResult<T> {
        let (reply, result) = oneshot::channel();
        self.sender.try_send(command(reply)).map_err(|e| match e {
            mpsc::TrySendError::Full(_) => CryptoError::Busy,
            mpsc::TrySendError::Disconnected(_) => CryptoError::Stopped,
        })?;
        result.await.map_err(|_| CryptoError::Stopped)?
    }
    pub async fn install_prepared(&self, material: DeviceMaterial) -> CryptoResult<()> {
        if material.ec.len() > 100 || material.pq.len() > 100 {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| Command::Install(material, r)).await
    }
    /// Explicit bounded upgrade of already-consumed v1 EC/PQ records. Call until
    /// next=None; restart from None is safe. Never invoked implicitly on open.
    pub async fn redact_consumed_prekeys(
        &self,
        after: Option<PreKeyRedactionCursor>,
    ) -> CryptoResult<PreKeyRedactionPage> {
        self.request(|r| Command::RedactPreKeys(after, r)).await
    }
    /// Explicit, restartable initialization: at most 32 prekey records per TX.
    /// Call until ready before reserving. Opening a store never scans or repairs it.
    pub async fn seed_prekey_ids(&self) -> CryptoResult<PreKeyIdSeedPage> {
        self.request(Command::SeedPreKeyIds).await
    }
    /// Explicit migration, <=32 authoritative records per transaction. Opening
    /// a store adds only empty schema/indexes; never scans/decrypts old history.
    pub async fn seed_recovery_indexes(&self) -> CryptoResult<RecoveryIndexPage> {
        self.request(Command::SeedRecoveryIndexes).await
    }
    /// Reserve before generating private material. The native caller must retain
    /// the same operation UUID/count/namespace across uncertain local replies.
    /// Lost operations burn IDs; this API never releases or regenerates them.
    pub async fn reserve_prekey_ids(
        &self,
        operation: String,
        namespace: PreKeyIdNamespace,
        count: u32,
    ) -> CryptoResult<PreKeyIdReservation> {
        self.request(|r| Command::ReservePreKeyIds(operation, namespace, count, r))
            .await
    }
    /// Resume the current refill; never starts another one after a lost reply.
    pub async fn prepare_prekey_refill(&self, ec: u32, pq: u32) -> CryptoResult<PreKeyRefillPlan> {
        self.request(|r| Command::PreparePreKeyRefill(ec, pq, None, r))
            .await
    }
    /// Explicit CAS advancement after the displayed previous refill completed.
    pub async fn prepare_next_prekey_refill(
        &self,
        previous: String,
        ec: u32,
        pq: u32,
    ) -> CryptoResult<PreKeyRefillPlan> {
        self.request(|r| Command::PreparePreKeyRefill(ec, pq, Some(previous), r))
            .await
    }
    pub async fn materialize_prekey_refill(
        &self,
        operation: String,
    ) -> CryptoResult<PreKeyRefillPlan> {
        self.request(|r| Command::MaterializePreKeyRefill(operation, r))
            .await
    }
    /// Reuses the same publication slot/revision as EC/PQ refill. No key GC.
    pub async fn prepare_prekey_rotation(
        &self,
        signed: bool,
        last_resort: bool,
    ) -> CryptoResult<PreKeyRotationPlan> {
        self.request(|r| Command::PreparePreKeyRotation(signed, last_resort, None, r))
            .await
    }
    pub async fn prepare_next_prekey_rotation(
        &self,
        previous: String,
        signed: bool,
        last_resort: bool,
    ) -> CryptoResult<PreKeyRotationPlan> {
        self.request(|r| Command::PreparePreKeyRotation(signed, last_resort, Some(previous), r))
            .await
    }
    /// Read the shared slot independently of old request counts or policy.
    pub async fn current_prekey_publication(&self) -> CryptoResult<Option<PreKeyPublicationPlan>> {
        self.request(Command::CurrentPreKeyPublication).await
    }
    /// Inventory revision/pointers, shared-slot CAS, quotas and new ID reservations
    /// are checked/committed in one protected transaction. No remote rebasing.
    pub async fn refill_from_inventory(
        &self,
        proof: VerifiedPreKeyInventory,
        policy: RefillPolicy,
    ) -> CryptoResult<InventoryRefill> {
        self.request(|r| Command::RefillFromInventory(proof, policy, r))
            .await
    }
    pub async fn next_prekey_refill_upload(&self) -> CryptoResult<Option<PreKeyRefillUpload>> {
        self.request(Command::NextPreKeyRefillUpload).await
    }
    pub async fn accept_prekey_refill_upload(
        &self,
        proof: VerifiedPreKeyRefillUpload,
    ) -> CryptoResult<()> {
        self.request(|r| Command::AcceptPreKeyRefillUpload(proof, r))
            .await
    }
    /// Commit a peer-bound request before RPC; ordinary retries reuse its UUID,
    /// including when the original conversation has since closed.
    pub async fn prepare_conversation_creation(
        &self,
        peer_user_id: i64,
    ) -> CryptoResult<ConversationCreation> {
        self.request(|r| Command::PrepareConversation(peer_user_id, None, r))
            .await
    }
    /// Explicit new-dialogue action. Only a durably known closed result permits
    /// advancing the peer slot; a lost local reply retries with the same old ID.
    pub async fn prepare_new_conversation_creation(
        &self,
        peer_user_id: i64,
        closed_request_id: String,
    ) -> CryptoResult<ConversationCreation> {
        self.request(|r| Command::PrepareConversation(peer_user_id, Some(closed_request_id), r))
            .await
    }
    /// Narrow native action: derive the saved request from a known CLOSED chat.
    /// A retry after COMMIT/reopen reuses its direct successor, never a new UUID.
    pub async fn prepare_conversation_after_closed(
        &self,
        peer_user_id: i64,
        closed_conversation_id: String,
    ) -> CryptoResult<ConversationCreation> {
        self.request(|r| {
            Command::PrepareConversationAfterClosed(peer_user_id, closed_conversation_id, r)
        })
        .await
    }
    /// Only the authenticated native transport can mint this opaque receipt.
    /// Return a usable result after its journal/index transaction commits.
    pub async fn accept_conversation_creation(
        &self,
        verified: VerifiedConversationCreation,
    ) -> CryptoResult<Conversation> {
        self.request(|r| Command::AcceptConversation(verified, r))
            .await
    }
    pub async fn bootstrap(&self, scope: SessionScope, bundle: AcquiredBundle) -> CryptoResult<()> {
        if bundle.encoded_len() > 8192 {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| Command::Bootstrap(scope, bundle, r)).await
    }
    /// Commit the request before RPC. Reuse it on timeout/disconnect/restart;
    /// applied=true means skip the RPC/bootstrap, not generate another UUID.
    pub async fn prepare_acquisition(&self, scope: SessionScope) -> CryptoResult<AcquisitionPlan> {
        self.request(|r| Command::PrepareAcquisition(scope, r))
            .await
    }
    /// Explicit local CAS after server-confirmed expiry. Commits the new request
    /// before RPC, never reuses consumed keys or modifies session/message records.
    pub async fn renew_acquisition(
        &self,
        scope: SessionScope,
        expired: ExpiredAcquisition,
    ) -> CryptoResult<AcquisitionPlan> {
        self.request(|r| Command::RenewAcquisition(scope, expired, r))
            .await
    }
    pub async fn encrypt(
        &self,
        scope: SessionScope,
        payload: EncryptedPayload,
    ) -> CryptoResult<Envelope> {
        validate_payload_binding(
            &payload,
            &scope.envelope(&payload.message_id, vec![1], CiphertextType::Signal),
        )
        .map_err(|_| CryptoError::Invalid)?;
        self.request(|r| Command::Encrypt(scope, payload, r)).await
    }
    pub async fn decrypt(
        &self,
        scope: SessionScope,
        envelope: Envelope,
    ) -> CryptoResult<EncryptedPayload> {
        validate_envelope(&envelope).map_err(|_| CryptoError::Invalid)?;
        self.request(|r| Command::Decrypt(scope, envelope, r)).await
    }
    pub async fn shutdown(self) -> CryptoResult<()> {
        drop(self.sender);
        self.stopped.await.map_err(|_| CryptoError::Stopped)
    }
}

#[cfg(test)]
#[path = "signal_state_tests.rs"]
mod tests;
