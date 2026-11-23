//! Durable ciphertext preparation, not Signal outbox/server acceptance. All blob
//! chunks and the private journal commit atomically on the existing account lane.
use super::*;
use crate::encryption::{
    attachment::{self, AttachmentDescriptor},
    attachment_payload::{self, AttachmentDraft},
    epoch_contract as data,
};
use tokio::sync::OwnedSemaphorePermit;

#[cfg(feature = "compression")]
#[path = "epoch_photo_prepare.rs"]
mod photo;

#[path = "epoch_attachment_control.rs"]
mod control;
pub use control::{EpochBlobControlPlan, EpochBlobProgress, EpochBlobUpload};

#[path = "epoch_attachment_promotion.rs"]
pub(super) mod promotion;
pub use promotion::EpochAttachmentPromotion;

#[path = "epoch_attachment_retry.rs"]
mod retry;
pub use retry::EpochAttachmentRetryPlan;

#[path = "epoch_attachment_archive.rs"]
pub(super) mod archived;

#[path = "epoch_attachment_retirement.rs"]
pub(super) mod retirement;

#[derive(Message)]
#[prost(skip_debug)]
struct Journal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    payload: Option<v2::EncryptedPayload>,
    #[prost(bytes = "vec", tag = "3")]
    source_digest: Vec<u8>,
    // Required in format2. Minted with ciphertext BEFORE any network call.
    #[prost(message, optional, tag = "4")]
    operation: Option<v2::BlobLeaseRequest>,
    #[prost(message, optional, tag = "5")]
    lease: Option<v2::BlobLease>,
    #[prost(message, optional, tag = "6")]
    replaces: Option<control::Predecessor>,
    #[prost(bool, optional, tag = "7")]
    linked_seen: Option<bool>,
    // Present only after the SAME TX commits Signal/history/outbox and this
    // journal. Not a server receipt or permission to open/upload the file.
    #[prost(bytes = "vec", optional, tag = "8")]
    promoted_digest: Option<Vec<u8>>,
}
impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AttachmentPreparation { <private fields redacted> }")
    }
}
impl Drop for Journal {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        self.source_digest.zeroize();
    }
}

/// Immutable local cohort proof only, not permission to send or an upload receipt.
/// No raw constructor/Debug/serde/key or source-fingerprint getter.
pub struct EpochAttachmentPlan {
    own: DeviceRef,
    store: StoreScope,
    message: String,
    conversation: String,
    epoch: u64,
    blob: String,
    bytes: usize,
    digest: [u8; 32],
    journal_pin: [u8; 32],
}
impl EpochAttachmentPlan {
    pub fn message_id(&self) -> &str {
        &self.message
    }
    pub fn conversation_id(&self) -> &str {
        &self.conversation
    }
    pub fn crypto_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn blob_id(&self) -> &str {
        &self.blob
    }
    pub fn blob_len(&self) -> usize {
        self.bytes
    }
    pub fn blob_digest(&self) -> &[u8; 32] {
        &self.digest
    }
}
pub enum EpochAttachmentPreparation {
    Prepared(Box<EpochAttachmentPlan>),
    // No key, blob, journal or Signal session has been generated.
    CatalogRequired,
}

/// Read-only local outcome for an attachment UUID. No file/private metadata,
/// active-epoch authority or instruction to re-encrypt absent input.
#[derive(Debug, PartialEq, Eq)]
pub enum EpochAttachmentInputState {
    Vacant,
    Prepared {
        crypto_epoch: u64,
    },
    Outgoing {
        crypto_epoch: u64,
        stored_on_server: bool,
        sealed_unsent: bool,
    },
}
/// Bounded ciphertext buffer. Its permit remains held until this buffer drops,
/// including when the requesting future is abandoned after native work starts.
pub struct EpochAttachmentBlob {
    bytes: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}
impl EpochAttachmentBlob {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}
pub(in crate::encryption::signal_state) struct Input {
    chat: String,
    message: String,
    draft: AttachmentDraft,
    plaintext: Zeroizing<Vec<u8>>,
    // New native input stays in the authenticated epoch found at preflight.
    // None preserves the existing trusted-Rust retained-input API.
    expected: Option<EpochScope>,
    _permit: OwnedSemaphorePermit,
}
pub(in crate::encryption::signal_state) enum AttachmentCommand {
    InputState(String, String, Reply<EpochAttachmentInputState>),
    Candidates(Reply<Vec<String>>),
    Manifest(Reply<Vec<String>>),
    Prepare(Box<Input>, Reply<EpochAttachmentPreparation>),
    #[cfg(feature = "compression")]
    NewInput(String, String, Option<u64>, Reply<Option<EpochScope>>),
    Lookup(
        String,
        OwnedSemaphorePermit,
        Reply<Option<EpochAttachmentPlan>>,
    ),
    Read(
        Box<EpochAttachmentPlan>,
        OwnedSemaphorePermit,
        Reply<EpochAttachmentBlob>,
    ),
    Control(Box<control::ControlCommand>, OwnedSemaphorePermit),
    Upload(String, OwnedSemaphorePermit, Reply<EpochBlobUpload>),
    Promote(
        String,
        Option<Box<EpochBlobControlPlan>>,
        OwnedSemaphorePermit,
        Reply<EpochAttachmentPromotion>,
    ),
    Outgoing(String, Reply<Option<EpochOutgoingPlan>>),
    Retry(Box<retry::RetryCommand>, OwnedSemaphorePermit),
}
pub(super) fn key(own: &DeviceRef, message: &str) -> CryptoResult<RecordKey> {
    if !epoch_rpc::uuid4(message) {
        return Err(CryptoError::Invalid);
    }
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("attachment-prepare:{message}"),
    )?)
}
pub(super) fn reserved(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    message: &str,
) -> CryptoResult<bool> {
    Ok(tx.get(&key(own, message)?)?.is_some())
}
fn part(p: &v2::EncryptedPayload) -> CryptoResult<&v2::AttachmentMessage> {
    match p.content.as_ref() {
        Some(v2::encrypted_payload::Content::Attachment(a)) => Ok(a),
        _ => Err(CryptoError::Invalid),
    }
}
fn check_payload(p: &v2::EncryptedPayload, own: &DeviceRef) -> CryptoResult<()> {
    if p.sender.as_ref() != Some(own) {
        return Err(CryptoError::Conflict);
    }
    data::validate_payload_binding(
        p,
        &v2::Envelope {
            protocol_version: 3,
            crypto_suite: CryptoSuite::SignalPq1 as i32,
            message_id: p.message_id.clone(),
            conversation_id: p.conversation_id.clone(),
            sender: p.sender.clone(),
            recipient: p.recipient.clone(),
            message_type: CiphertextType::Signal as i32,
            ciphertext: vec![1],
            crypto_epoch: p.crypto_epoch,
            blob: Some(
                attachment_payload::blob_reference(part(p)?).map_err(|_| CryptoError::Invalid)?,
            ),
        },
    )
    .map_err(|_| CryptoError::Invalid)
}
fn load_journal(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    message: &str,
) -> CryptoResult<Option<Journal>> {
    let Some(j) = load_retained_journal(tx, own, message)? else {
        return Ok(None);
    };
    if j.promoted_digest.is_some() || tx.get(&outgoing::link_key(own, message)?)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    Ok(Some(j))
}
// Decode retained preparation metadata without granting prepromotion authority.
// Outgoing readers must separately verify both sides of the promotion cohort.
fn load_retained_journal(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    message: &str,
) -> CryptoResult<Option<Journal>> {
    let Some(bytes) = tx.get(&key(own, message)?)? else {
        return Ok(None);
    };
    let journal = Journal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if journal.format != 2 || journal.source_digest.len() != 32 {
        return Err(CryptoError::Invalid);
    }
    let p = journal.payload.as_ref().ok_or(CryptoError::Invalid)?;
    if p.message_id != message
        || journal
            .promoted_digest
            .as_ref()
            .is_some_and(|d| d.len() != 32)
    {
        return Err(CryptoError::Conflict);
    }
    check_payload(p, own)?;
    control::validate_journal(&journal)?;
    Ok(Some(journal))
}
fn local_plan(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    j: &Journal,
) -> CryptoResult<EpochAttachmentPlan> {
    let p = j.payload.as_ref().ok_or(CryptoError::Invalid)?;
    let a = part(p)?;
    let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor)
        .map_err(|_| CryptoError::Invalid)?;
    tx.attachment_blob(
        &key(own, &p.message_id)?,
        &a.blob_id,
        descriptor.blob_len(),
        descriptor.blob_digest(),
        false,
    )?;
    Ok(EpochAttachmentPlan {
        own: own.clone(),
        store: store.clone(),
        message: p.message_id.clone(),
        conversation: p.conversation_id.clone(),
        epoch: p.crypto_epoch.unwrap(),
        blob: a.blob_id.clone(),
        bytes: descriptor.blob_len(),
        digest: *descriptor.blob_digest(),
        journal_pin: digest(
            b"zchat.e2ee.attachment.prepare.v1\0",
            &Zeroizing::new(j.encode_to_vec()),
        ),
    })
}
fn current(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    chat: &str,
) -> CryptoResult<Option<EpochScope>> {
    let c =
        conversation_lifecycle::load(tx, own, chat)?.ok_or(CryptoError::ConversationUnavailable)?;
    if c.state == ConversationState::Closed as i32 {
        return Err(CryptoError::ConversationClosed);
    }
    let (public, registration) = identity(tx, own)?;
    let Some(loaded) = load(tx, own, &c, public, registration)? else {
        return Ok(None);
    };
    let s = scope(&loaded, &c, store.clone())?;
    bootstrap::sending(&s)?;
    Ok(Some(s))
}

/// List durable prepromotion records in UUID order. The bounded manifest has
/// at most 64 entries for the store. Never read ciphertext chunks in this view.
pub(super) fn pending_page(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    chat: &str,
    after: Option<&str>,
) -> CryptoResult<(Vec<PendingAttachment>, Option<String>)> {
    recovery_indexes::require_ready(tx, own)?;
    let conversation =
        conversation_lifecycle::load(tx, own, chat)?.ok_or(CryptoError::ConversationUnavailable)?;
    let mut items = Vec::new();
    let mut wire_bytes = 0;
    for id in tx.attachment_candidates(&own.generation_id)? {
        if after.is_some_and(|after| id.as_str() <= after) {
            continue;
        }
        let mut journal = load_retained_journal(tx, own, &id)?.ok_or(CryptoError::Invalid)?;
        let payload = journal.payload.as_mut().ok_or(CryptoError::Invalid)?;
        if payload.conversation_id != chat {
            continue;
        }
        let linked = tx.get(&outgoing::link_key(own, &id)?)?.is_some();
        match (journal.promoted_digest.is_some(), linked) {
            (true, true) => continue, // Promoted records belong to history.
            (false, false) => (),
            _ => return Err(CryptoError::Conflict),
        }
        if payload.recipient.as_ref()
            != conversation
                .peer_device
                .as_ref()
                .and_then(|p| p.reference.as_ref())
        {
            return Err(CryptoError::Conflict);
        }
        let Some(v2::encrypted_payload::Content::Attachment(file)) = payload.content.take() else {
            return Err(CryptoError::Invalid);
        };
        let attachment = attachment_payload::summary(file).map_err(|_| CryptoError::Invalid)?;
        tx.check_attachment_manifest(
            &key(own, &id)?,
            &attachment.blob_id,
            attachment.plaintext_len + attachment::BLOB_OVERHEAD,
        )?;
        let cost = 1024 + views::attachment_wire_cost(&attachment);
        if items.len() == views::ITEMS || wire_bytes + cost > views::HISTORY_WIRE_BUDGET {
            let last: &PendingAttachment = items.last().ok_or(CryptoError::Invalid)?;
            let next = last.message_id.clone();
            return Ok((items, Some(next)));
        }
        wire_bytes += cost;
        items.push(PendingAttachment {
            message_id: id,
            crypto_epoch: payload.crypto_epoch.ok_or(CryptoError::Invalid)?,
            created_at_unix_ms: payload.created_at_unix_ms,
            attachment,
        });
    }
    Ok((items, None))
}

impl Engine {
    fn attachment_input_state(
        &mut self,
        chat: &str,
        message: &str,
    ) -> CryptoResult<EpochAttachmentInputState> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            conversation_lifecycle::load(tx, own, chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let Some(j) = load_retained_journal(tx, own, message)? else {
                if tx.get(&outgoing::link_key(own, message)?)?.is_some() {
                    return Err(CryptoError::Conflict);
                }
                tx.require_empty_attachment_slot(&key(own, message)?)?;
                return Ok(EpochAttachmentInputState::Vacant);
            };
            let payload = j.payload.as_ref().ok_or(CryptoError::Invalid)?;
            if payload.conversation_id != chat {
                return Err(CryptoError::Conflict);
            }
            let epoch = payload.crypto_epoch.ok_or(CryptoError::Invalid)?;
            if let Some(p) = promotion::retained_plan(tx, own, &store, message, &j)? {
                return Ok(EpochAttachmentInputState::Outgoing {
                    crypto_epoch: epoch,
                    stored_on_server: p.receipt().is_some(),
                    sealed_unsent: p.sealed_failure().is_some(),
                });
            }
            // Validate the retained unpromoted cohort without keeping a whole
            // file buffer; incomplete/corrupt bytes are never reported Vacant.
            local_plan(tx, own, &store, &j)?;
            Ok(EpochAttachmentInputState::Prepared {
                crypto_epoch: epoch,
            })
        })
    }
    fn prepare_attachment(&mut self, input: Input) -> CryptoResult<EpochAttachmentPreparation> {
        let Input {
            chat,
            message,
            draft,
            plaintext,
            expected,
            _permit,
        } = input;
        if let Some(s) = &expected {
            self.check_epoch_owner(s)?;
        }
        draft
            .validate(plaintext.len())
            .map_err(|_| CryptoError::Invalid)?;
        let source = Zeroizing::new(<[u8; 32]>::from(Sha256::digest(plaintext.as_slice())));
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            // New input is separate from a retained retry. Reject identical new
            // input under an occupied UUID, before creating/replacing anything.
            if expected.is_some()
                && (load_retained_journal(tx, own, &message)?.is_some()
                    || tx.get(&outgoing::link_key(own, &message)?)?.is_some())
            {
                return Err(CryptoError::Conflict);
            }
            if let Some(j) = load_journal(tx, own, &message)? {
                let p = j.payload.as_ref().unwrap();
                if p.conversation_id != chat
                    || j.source_digest.as_slice() != source.as_slice()
                    || !draft.matches(part(p)?)
                {
                    return Err(CryptoError::Conflict);
                }
                return Ok(EpochAttachmentPreparation::Prepared(Box::new(local_plan(
                    tx, own, &store, &j,
                )?)));
            }
            if tx.get(&outgoing::link_key(own, &message)?)?.is_some() {
                return Err(CryptoError::Conflict);
            }
            tx.require_empty_attachment_slot(&key(own, &message)?)?;
            let s = if let Some(expected) = &expected {
                if expected.conversation != chat {
                    return Err(CryptoError::Conflict);
                }
                let actual = bootstrap::current(tx, expected)?;
                bootstrap::sending(&actual)?;
                actual
            } else {
                let Some(s) = current(tx, own, &store, &chat)? else {
                    return Ok(EpochAttachmentPreparation::CatalogRequired);
                };
                s
            };
            tx.attachment_capacity(plaintext.len() + attachment::BLOB_OVERHEAD)?;
            let blob = uuid::Uuid::new_v4().to_string();
            let binding = attachment::AttachmentBinding::new(
                &chat, s.epoch, &message, &blob, &s.own, &s.peer,
            )
            .map_err(|_| CryptoError::Invalid)?;
            let file = attachment::seal(&binding, plaintext).map_err(|_| CryptoError::Invalid)?;
            let created = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| CryptoError::Invalid)?
                .as_millis()
                .try_into()
                .map_err(|_| CryptoError::Invalid)?;
            let p = v2::EncryptedPayload {
                format_version: attachment_payload::PAYLOAD_FORMAT,
                message_id: message.clone(),
                conversation_id: chat,
                sender: Some(s.own),
                recipient: Some(s.peer),
                created_at_unix_ms: created,
                crypto_epoch: Some(s.epoch),
                content: Some(v2::encrypted_payload::Content::Attachment(
                    draft.into_message(blob.clone(), file.descriptor()),
                )),
            };
            check_payload(&p, own)?;
            let operation = control::initial_request(&p)?;
            let j = Journal {
                format: 2,
                payload: Some(p),
                source_digest: source.to_vec(),
                operation: Some(operation),
                lease: None,
                replaces: None,
                linked_seen: Some(false),
                promoted_digest: None,
            };
            let k = key(own, &message)?;
            tx.put(&k, &Zeroizing::new(j.encode_to_vec()))?;
            tx.put_attachment_blob(&k, &blob, file.blob())?;
            Ok(EpochAttachmentPreparation::Prepared(Box::new(local_plan(
                tx, own, &store, &j,
            )?)))
        })
    }
    fn attachment_lookup(&mut self, message: &str) -> CryptoResult<Option<EpochAttachmentPlan>> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            load_journal(tx, own, message)?
                .map(|j| local_plan(tx, own, &store, &j))
                .transpose()
        })
    }
    fn attachment_read(
        &mut self,
        plan: EpochAttachmentPlan,
        permit: OwnedSemaphorePermit,
    ) -> CryptoResult<EpochAttachmentBlob> {
        let own = &self.own;
        let store = self.store.scope().clone();
        if plan.own != *own || plan.store != store {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let j = load_journal(tx, own, &plan.message)?.ok_or(CryptoError::MessageUnavailable)?;
            let pin = digest(
                b"zchat.e2ee.attachment.prepare.v1\0",
                &Zeroizing::new(j.encode_to_vec()),
            );
            if pin != plan.journal_pin {
                return Err(CryptoError::Conflict);
            }
            let p = j.payload.as_ref().unwrap();
            let s = current(tx, own, &store, &p.conversation_id)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            if s.epoch != p.crypto_epoch.unwrap()
                || p.sender.as_ref() != Some(&s.own)
                || p.recipient.as_ref() != Some(&s.peer)
            {
                return Err(CryptoError::Conflict);
            }
            let bytes = tx.attachment_blob(
                &key(own, &plan.message)?,
                &plan.blob,
                plan.bytes,
                &plan.digest,
                true,
            )?;
            Ok(EpochAttachmentBlob {
                bytes,
                _permit: permit,
            })
        })
    }
    pub(super) fn handle_attachment_prepare(&mut self, command: AttachmentCommand) {
        match command {
            AttachmentCommand::InputState(chat, message, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.attachment_input_state(&chat, &message));
                }
            }
            #[cfg(feature = "compression")]
            AttachmentCommand::NewInput(chat, message, epoch, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.new_attachment_input(&chat, &message, epoch));
                }
            }
            AttachmentCommand::Manifest(reply) => {
                if !reply.is_closed() {
                    let own = &self.own;
                    let result = self.store.transaction_with(|tx| {
                        recovery_indexes::require_ready(tx, own)?;
                        Ok::<_, CryptoError>(tx.attachment_candidates(&own.generation_id)?)
                    });
                    let _ = reply.send(result);
                }
            }
            AttachmentCommand::Candidates(reply) => {
                if !reply.is_closed() {
                    let own = &self.own;
                    let result = self.store.transaction_with(|tx| {
                        recovery_indexes::require_ready(tx, own)?;
                        let mut ids = Vec::new();
                        for id in tx.attachment_candidates(&own.generation_id)? {
                            if tx.get(&outgoing::link_key(own, &id)?)?.is_none() {
                                ids.push(id);
                            }
                        }
                        Ok::<_, CryptoError>(ids)
                    });
                    let _ = reply.send(result);
                }
            }
            AttachmentCommand::Upload(message, permit, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.attachment_upload(&message, permit));
                }
            }
            AttachmentCommand::Prepare(input, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.prepare_attachment(*input));
                }
            }
            AttachmentCommand::Lookup(message, permit, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_lookup(&message);
                    // A plan contains no retained blob buffer. Release BEFORE
                    // publishing it, so an immediate read cannot spuriously Busy.
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            AttachmentCommand::Read(plan, permit, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.attachment_read(*plan, permit));
                }
            }
            AttachmentCommand::Control(command, permit) => {
                self.handle_attachment_control(*command, permit)
            }
            AttachmentCommand::Retry(command, permit) => {
                self.handle_attachment_retry(*command, permit)
            }
            AttachmentCommand::Promote(message, expected, permit, reply) => {
                if !reply.is_closed() {
                    let result = self.promote_attachment(&message, expected.as_deref());
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            AttachmentCommand::Outgoing(message, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.attachment_outgoing(&message));
                }
            }
        }
    }
}
impl CryptoCoordinator {
    pub async fn epoch_attachment_input_state(
        &self,
        chat: String,
        message: String,
    ) -> CryptoResult<EpochAttachmentInputState> {
        if !epoch_rpc::uuid4(&chat) || !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::InputState(
                chat, message, reply,
            )))
        })
        .await
    }
    /// Bounded scheduling IDs including promoted files, not upload authority.
    pub(in crate::encryption) async fn epoch_attachment_manifest(
        &self,
    ) -> CryptoResult<Vec<String>> {
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Manifest(reply)))
        })
        .await
    }
    /// Bounded manifest hints, never authorization or a lease. Does not take the
    /// file-buffer permit, so selection cannot block other account commands.
    pub(in crate::encryption) async fn epoch_attachment_candidates(
        &self,
    ) -> CryptoResult<Vec<String>> {
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Candidates(
                reply,
            )))
        })
        .await
    }
    pub(super) fn attachment_permit(&self) -> CryptoResult<OwnedSemaphorePermit> {
        self.attachment_budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| CryptoError::Busy)
    }
    pub async fn prepare_epoch_attachment(
        &self,
        chat: String,
        message: String,
        draft: AttachmentDraft,
        plaintext: Zeroizing<Vec<u8>>,
    ) -> CryptoResult<EpochAttachmentPreparation> {
        if !epoch_rpc::uuid4(&chat) || !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        draft
            .validate(plaintext.len())
            .map_err(|_| CryptoError::Invalid)?;
        let input = Input {
            chat,
            message,
            draft,
            plaintext,
            expected: None,
            _permit: self.attachment_permit()?,
        };
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Prepare(
                Box::new(input),
                reply,
            )))
        })
        .await
    }
    /// Local consistency recovery only; may return a now-frozen/closed old plan.
    /// Reading bytes for upload separately revalidates the current lifecycle.
    pub async fn prepared_epoch_attachment(
        &self,
        message: String,
    ) -> CryptoResult<Option<EpochAttachmentPlan>> {
        if !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Lookup(
                message, permit, reply,
            )))
        })
        .await
    }
    /// Still not permanent network permission. Future native transport must bind
    /// its session/cancellation; Signal Send requires a separate fresh mutation.
    pub async fn prepared_epoch_attachment_blob(
        &self,
        plan: EpochAttachmentPlan,
    ) -> CryptoResult<EpochAttachmentBlob> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Read(
                Box::new(plan),
                permit,
                reply,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_attachment_prepare_tests.rs"]
mod tests;
