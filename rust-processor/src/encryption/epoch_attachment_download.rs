//! Protected incoming history -> bounded ciphertext staging -> verified cache.
//! No renderer keys, automatic account scheduling, server file ACK or eviction.
use super::*;
use crate::encryption::{
    attachment::{self, AttachmentBinding, AttachmentDescriptor},
    blob_download::EpochBlobDownloadTransport,
    epoch_contract as data,
    native::{
        BlobCiphertextChunk, BlobCiphertextSink, BlobGetResponse, BlobReadError, BlobReadRequest,
        BlobSinkError,
    },
};
use std::sync::Arc;
use tokio::sync::OwnedSemaphorePermit;

#[path = "epoch_attachment_file_ack.rs"]
mod file_ack;
pub use file_ack::{EpochDurableFileAck, EpochFileAckCursor, EpochFileAckPage};
#[path = "epoch_archive_attachments.rs"]
mod archived;
#[path = "epoch_attachment_removal.rs"]
mod removal;

/// Minted ONLY from a complete authenticated incoming cohort. No raw/renderer
/// constructor, private descriptor, Debug or serde. Rechecked on every mutation.
#[derive(Clone)]
pub struct EpochAttachmentDownloadPlan {
    own: DeviceRef,
    store: StoreScope,
    id: String,
    binding: crate::proto::e2ee::EpochSendBinding,
    blob: v2::BlobRef,
    payload_pin: [u8; 32],
}
impl EpochAttachmentDownloadPlan {
    pub fn message_id(&self) -> &str {
        &self.binding.message_id
    }
    pub fn conversation_id(&self) -> &str {
        &self.binding.conversation_id
    }
    pub fn crypto_epoch(&self) -> u64 {
        self.binding.epoch
    }
    pub fn blob_len(&self) -> usize {
        self.blob.ciphertext_bytes as usize
    }
    pub fn read_request(&self) -> BlobReadRequest {
        BlobReadRequest::new(self.store.clone(), self.binding.clone(), self.blob.clone())
            .expect("validated protected incoming plan")
    }
}
/// Local verified-cache locator, NOT a server file-ACK proof. Reopening checks
/// the full protected history/journal/chunks/AEAD again before returning bytes.
pub struct EpochAttachmentCache {
    plan: EpochAttachmentDownloadPlan,
}
impl EpochAttachmentCache {
    pub fn message_id(&self) -> &str {
        self.plan.message_id()
    }
}
/// Native plaintext only; the SAME file-buffer permit outlives the returned
/// buffer. No Debug/Clone/Serialize or generic renderer export.
pub struct EpochAttachmentFile {
    bytes: Zeroizing<Vec<u8>>,
    kind: v2::AttachmentKind,
    _permit: Arc<OwnedSemaphorePermit>,
}
impl EpochAttachmentFile {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn kind(&self) -> v2::AttachmentKind {
        self.kind
    }
}
#[derive(Debug, thiserror::Error)]
pub enum AttachmentDownloadError {
    #[error("protected attachment cache operation failed")]
    Crypto(#[from] CryptoError),
    #[error("attachment HTTP download failed")]
    Http(#[from] BlobReadError),
}

#[derive(Message)]
#[prost(skip_debug)]
struct Journal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(bytes = "vec", tag = "2")]
    payload_pin: Vec<u8>,
    #[prost(message, optional, tag = "3")]
    binding: Option<crate::proto::e2ee::EpochSendBinding>,
    #[prost(message, optional, tag = "4")]
    blob: Option<v2::BlobRef>,
    #[prost(string, tag = "5")]
    run: String,
    #[prost(uint64, optional, tag = "6")]
    offset: Option<u64>,
    #[prost(bool, optional, tag = "7")]
    verified: Option<bool>,
    // Format2: None while incomplete, Some(0) pending, positive original
    // server ACK time after a validated response. Format3 removal markers retain
    // that positive time with no manifest/chunks. Never inferred from a locator.
    #[prost(int64, optional, tag = "8")]
    file_acknowledged_at_unix_ms: Option<i64>,
}
// A run owns the account's ONE whole-file memory permit even between commands.
// Queued commands retain an Arc after a caller/future disappears.
#[derive(Clone)]
pub(in crate::encryption::signal_state) struct Run {
    plan: EpochAttachmentDownloadPlan,
    run: String,
    offset: usize,
    _permit: Arc<OwnedSemaphorePermit>,
}
pub(in crate::encryption::signal_state) enum Start {
    Download(Run),
    Cached(EpochAttachmentCache),
}
pub(in crate::encryption::signal_state) enum DownloadCommand {
    FileAck(file_ack::FileAckCommand),
    Plan(String, Reply<EpochAttachmentDownloadPlan>),
    Cached(String, Reply<EpochAttachmentCache>),
    Joined(Reply<()>),
    Begin(
        Box<EpochAttachmentDownloadPlan>,
        Arc<OwnedSemaphorePermit>,
        Reply<Start>,
    ),
    Append(
        Box<Run>,
        BlobCiphertextChunk,
        Reply<(usize, BlobCiphertextChunk)>,
    ),
    Finish(
        Box<Run>,
        Box<BlobGetResponse>,
        BlobCiphertextChunk,
        Reply<EpochAttachmentCache>,
    ),
    Open(
        Box<EpochAttachmentCache>,
        Arc<OwnedSemaphorePermit>,
        Reply<EpochAttachmentFile>,
    ),
    OpenArchive(
        Option<String>,
        String,
        String,
        Arc<OwnedSemaphorePermit>,
        Reply<EpochAttachmentFile>,
    ),
    Remove(String, String, String, Arc<OwnedSemaphorePermit>, Reply<()>),
    #[cfg(test)]
    Barrier(
        std::sync::mpsc::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ),
}
fn key(own: &DeviceRef, uuid: &str) -> CryptoResult<RecordKey> {
    if !epoch_rpc::uuid4(uuid) {
        return Err(CryptoError::Invalid);
    }
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Control,
        &format!("attachment-download:{uuid}"),
    )?)
}
fn part(p: &v2::EncryptedPayload) -> CryptoResult<&v2::AttachmentMessage> {
    match p.content.as_ref() {
        Some(v2::encrypted_payload::Content::Attachment(a)) => Ok(a),
        _ => Err(CryptoError::Invalid),
    }
}
fn attachment_kind(p: &v2::EncryptedPayload) -> CryptoResult<v2::AttachmentKind> {
    v2::AttachmentKind::try_from(part(p)?.kind).map_err(|_| CryptoError::Invalid)
}
// Private cohort data, not a download plan or ACK capability. Archive reads use
// this directly and never mint a plan whose read_request could reach the server.
struct CacheCohort {
    id: String,
    binding: crate::proto::e2ee::EpochSendBinding,
    blob: v2::BlobRef,
    payload_pin: [u8; 32],
}
fn cache_cohort(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    uuid: &str,
    history_only: bool,
) -> CryptoResult<(CacheCohort, v2::EncryptedPayload)> {
    recovery_indexes::require_ready(tx, own)?;
    if !history_only
        && tx
            .get(&RecordKey::new(
                &own.generation_id,
                RecordKind::Control,
                crate::encryption::provisioning::ARCHIVED_DEVICE_RECORD,
            )?)?
            .is_some()
    {
        return Err(CryptoError::ArchivedGeneration);
    }
    let link = outgoing::load_link(tx, own, uuid)?;
    if link.outgoing != Some(false) {
        return Err(CryptoError::Conflict);
    }
    // Only private archive reads allow own closure. Network/file-ACK paths keep
    // the existing current-own checks; neither path repairs missing cohorts.
    outgoing::load_scope_with(tx, own, store, &link.id, history_only)?;
    let (p, e) = incoming::view_payload(tx, own, store, &link.id)?;
    history::check(tx, own, &link.id, &p)?;
    let a = part(&p)?;
    crate::encryption::attachment_payload::validate(&p, a).map_err(|_| CryptoError::Invalid)?;
    let binding = data::send_binding(&e).map_err(|_| CryptoError::Invalid)?;
    let blob = e.blob.ok_or(CryptoError::Invalid)?;
    if e.protocol_version != 3 || p.recipient.as_ref() != Some(own) {
        return Err(CryptoError::Conflict);
    }
    let cohort = CacheCohort {
        id: link.id,
        binding,
        blob,
        payload_pin: payload_pin(&p),
    };
    Ok((cohort, p))
}
fn payload_pin(p: &v2::EncryptedPayload) -> [u8; 32] {
    digest(
        b"zchat.e2ee.attachment-incoming.v1\0",
        &Zeroizing::new(p.encode_to_vec()),
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttachmentCacheState {
    Missing,
    Saved,
    Removed,
}

// The history loader already authenticated this incoming payload and envelope.
// This is a read of the committed journal/manifest, not a grant to open bytes.
pub(super) fn history_cache_state(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    p: &v2::EncryptedPayload,
    e: &v2::Envelope,
) -> CryptoResult<AttachmentCacheState> {
    let binding = data::send_binding(e).map_err(|_| CryptoError::Invalid)?;
    let blob = e.blob.as_ref().ok_or(CryptoError::Invalid)?;
    let journal = load_bound_cache(tx, own, &binding, blob, &payload_pin(p))?;
    Ok(match journal {
        Some(j) if j.format == 3 => AttachmentCacheState::Removed,
        Some(j) if j.verified == Some(true) => AttachmentCacheState::Saved,
        _ => AttachmentCacheState::Missing,
    })
}
fn snapshot(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    uuid: &str,
) -> CryptoResult<(EpochAttachmentDownloadPlan, v2::EncryptedPayload)> {
    let (c, p) = cache_cohort(tx, own, store, uuid, false)?;
    Ok((
        EpochAttachmentDownloadPlan {
            own: own.clone(),
            store: store.clone(),
            id: c.id,
            binding: c.binding,
            blob: c.blob,
            payload_pin: c.payload_pin,
        },
        p,
    ))
}
fn fresh(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    expected: &EpochAttachmentDownloadPlan,
) -> CryptoResult<v2::EncryptedPayload> {
    if expected.own != *own || expected.store != *store {
        return Err(CryptoError::Conflict);
    }
    let (actual, payload) = snapshot(tx, own, store, expected.message_id())?;
    if actual.id != expected.id
        || actual.binding != expected.binding
        || actual.blob != expected.blob
        || actual.payload_pin != expected.payload_pin
    {
        return Err(CryptoError::Conflict);
    }
    Ok(payload)
}
fn load(
    tx: &mut ProtectedTransaction<'_>,
    plan: &EpochAttachmentDownloadPlan,
) -> CryptoResult<Option<Journal>> {
    load_bound_cache(tx, &plan.own, &plan.binding, &plan.blob, &plan.payload_pin)
}
fn load_bound_cache(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    binding: &crate::proto::e2ee::EpochSendBinding,
    blob: &v2::BlobRef,
    payload_pin: &[u8; 32],
) -> CryptoResult<Option<Journal>> {
    let key = key(own, &binding.message_id)?;
    let live = tx.get(&key)?;
    let removed = tx.get(&removal::key(own, &binding.message_id)?)?;
    if live.is_some() && removed.is_some() {
        return Err(CryptoError::Conflict);
    }
    tx.check_download_manifest(
        &key,
        &blob.blob_id,
        blob.ciphertext_bytes as usize,
        live.is_some(),
    )?;
    let retired = removed.is_some();
    let bytes = live.or(removed);
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    let j = Journal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if j.format != if retired { 3 } else { 2 }
        || j.payload_pin.as_slice() != payload_pin
        || j.binding.as_ref() != Some(binding)
        || j.blob.as_ref() != Some(blob)
        || !epoch_rpc::uuid4(&j.run)
        || j.offset.is_none_or(|n| n > blob.ciphertext_bytes)
        || j.verified.is_none()
        || j.verified == Some(true) && j.offset != Some(blob.ciphertext_bytes)
        || match (j.verified, j.file_acknowledged_at_unix_ms) {
            (Some(false), None) => false,
            (Some(true), Some(at)) if at >= 0 => false,
            _ => true,
        }
        || retired
            && (j.verified != Some(true) || j.file_acknowledged_at_unix_ms.is_none_or(|at| at <= 0))
    {
        return Err(CryptoError::Conflict);
    }
    Ok(Some(j))
}
fn require_cached(j: &Journal) -> CryptoResult<()> {
    if j.format == 3 {
        return Err(CryptoError::AttachmentRemoved);
    }
    if j.verified != Some(true) {
        return Err(CryptoError::MessageUnavailable);
    }
    Ok(())
}
fn current(tx: &mut ProtectedTransaction<'_>, run: &Run) -> CryptoResult<Journal> {
    let j = load(tx, &run.plan)?.ok_or(CryptoError::MessageUnavailable)?;
    if j.run != run.run || j.offset != Some(run.offset as u64) || j.verified != Some(false) {
        return Err(CryptoError::Conflict);
    }
    Ok(j)
}
fn open(
    tx: &mut ProtectedTransaction<'_>,
    plan: &EpochAttachmentDownloadPlan,
    p: &v2::EncryptedPayload,
) -> CryptoResult<Zeroizing<Vec<u8>>> {
    open_bound_cache(tx, &plan.own, &plan.binding, &plan.blob, p)
}
fn open_bound_cache(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    send: &crate::proto::e2ee::EpochSendBinding,
    blob: &v2::BlobRef,
    p: &v2::EncryptedPayload,
) -> CryptoResult<Zeroizing<Vec<u8>>> {
    let a = part(p)?;
    let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor)
        .map_err(|_| CryptoError::Invalid)?;
    let binding = AttachmentBinding::new(
        &p.conversation_id,
        send.epoch,
        &p.message_id,
        &a.blob_id,
        p.sender.as_ref().ok_or(CryptoError::Invalid)?,
        own,
    )
    .map_err(|_| CryptoError::Invalid)?;
    let bytes = tx.collect_download(
        &key(own, &send.message_id)?,
        blob.ciphertext_bytes as usize,
        &blob.sha256,
    )?;
    // At most one ciphertext + one Zeroizing plaintext buffer (<=~100MiB for
    // the 50MiB format cap). Reuse audited AEAD; never invent streaming crypto.
    attachment::open(&binding, &descriptor, &bytes).map_err(|_| CryptoError::Invalid)
}
impl Engine {
    pub(super) fn handle_attachment_download(&mut self, command: DownloadCommand) {
        match command {
            DownloadCommand::Remove(generation, chat, uuid, permit, reply) => {
                if !reply.is_closed() {
                    let result = self.remove_local_attachment(&generation, &chat, &uuid);
                    drop(permit); // Retain file admission through COMMIT/rollback.
                    let _ = reply.send(result);
                }
            }
            DownloadCommand::OpenArchive(generation, chat, uuid, permit, reply) => {
                if !reply.is_closed() {
                    let generation = generation.unwrap_or_else(|| self.own.generation_id.clone());
                    let _ =
                        reply.send(self.open_archive_attachment(&generation, &chat, &uuid, permit));
                }
            }
            DownloadCommand::FileAck(command) => self.handle_file_ack(command),
            DownloadCommand::Joined(reply) => {
                let _ = reply.send(Ok(()));
            }
            DownloadCommand::Cached(uuid, reply) => {
                if !reply.is_closed() {
                    let own = &self.own;
                    let store = self.store.scope().clone();
                    let result = self.store.transaction_with(|tx| {
                        let (plan, _) = snapshot(tx, own, &store, &uuid)?;
                        require_cached(&load(tx, &plan)?.ok_or(CryptoError::MessageUnavailable)?)?;
                        // A metadata-only locator is not file/ACK authority;
                        // open_cached_epoch_attachment rechecks all bytes/AEAD.
                        Ok(EpochAttachmentCache { plan })
                    });
                    let _ = reply.send(result);
                }
            }
            #[cfg(test)]
            DownloadCommand::Barrier(release, entered) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
            DownloadCommand::Plan(uuid, reply) => {
                if !reply.is_closed() {
                    let own = &self.own;
                    let store = self.store.scope().clone();
                    let result = self
                        .store
                        .transaction_with(|tx| snapshot(tx, own, &store, &uuid).map(|(p, _)| p));
                    let _ = reply.send(result);
                }
            }
            DownloadCommand::Begin(plan, permit, reply) => {
                if !reply.is_closed() {
                    let result = self.begin_download(*plan, permit);
                    let _ = reply.send(result);
                }
            }
            DownloadCommand::Append(run, chunk, reply) => {
                if !reply.is_closed() {
                    let result = self
                        .append_download(&run, chunk.as_bytes())
                        .map(|n| (n, chunk));
                    let _ = reply.send(result);
                }
            }
            DownloadCommand::Finish(run, response, last, reply) => {
                if reply.is_closed() {
                    drop(run);
                    drop(response);
                    drop(last);
                } else {
                    let result = self.finish_download(*run, *response);
                    drop(last); // Includes COMMIT/rollback after the waiting future drops.
                    let _ = reply.send(result);
                }
            }
            DownloadCommand::Open(cache, permit, reply) => {
                if !reply.is_closed() {
                    let own = &self.own;
                    let store = self.store.scope().clone();
                    let plan = &cache.plan;
                    let result = self.store.transaction_with(|tx| {
                        let p = fresh(tx, own, &store, plan)?;
                        require_cached(&load(tx, plan)?.ok_or(CryptoError::MessageUnavailable)?)?;
                        Ok(EpochAttachmentFile {
                            bytes: open(tx, plan, &p)?,
                            kind: attachment_kind(&p)?,
                            _permit: permit,
                        })
                    });
                    let _ = reply.send(result);
                }
            }
        }
    }
    fn begin_download(
        &mut self,
        plan: EpochAttachmentDownloadPlan,
        permit: Arc<OwnedSemaphorePermit>,
    ) -> CryptoResult<Start> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let p = fresh(tx, own, &store, &plan)?;
            let old = load(tx, &plan)?;
            if old.as_ref().is_some_and(|j| j.format == 3) {
                return Err(CryptoError::AttachmentRemoved);
            }
            if old.as_ref().is_some_and(|j| j.verified == Some(true)) {
                drop(open(tx, &plan, &p)?); // A locator is not integrity authority.
                return Ok(Start::Cached(EpochAttachmentCache { plan }));
            }
            let key = key(own, plan.message_id())?;
            if old.is_none() {
                tx.create_download_manifest(&key, &plan.blob.blob_id, plan.blob_len())?;
            } else {
                tx.reset_download_chunks(&key)?;
            }
            let run = uuid::Uuid::new_v4().to_string();
            tx.put(
                &key,
                &Journal {
                    format: 2,
                    payload_pin: plan.payload_pin.to_vec(),
                    binding: Some(plan.binding.clone()),
                    blob: Some(plan.blob.clone()),
                    run: run.clone(),
                    offset: Some(0),
                    verified: Some(false),
                    file_acknowledged_at_unix_ms: None,
                }
                .encode_to_vec(),
            )?;
            Ok(Start::Download(Run {
                plan,
                run,
                offset: 0,
                _permit: permit,
            }))
        })
    }
    fn append_download(&mut self, run: &Run, bytes: &[u8]) -> CryptoResult<usize> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            fresh(tx, own, &store, &run.plan)?;
            let mut j = current(tx, run)?;
            let key = key(own, run.plan.message_id())?;
            tx.append_download_chunk(&key, run.plan.blob_len(), run.offset, bytes)?;
            let offset = run.offset + bytes.len();
            j.offset = Some(offset as u64);
            tx.put(&key, &j.encode_to_vec())?;
            Ok(offset)
        })
    }
    fn finish_download(
        &mut self,
        run: Run,
        response: BlobGetResponse,
    ) -> CryptoResult<EpochAttachmentCache> {
        let own = &self.own;
        let store = self.store.scope().clone();
        let plan = &run.plan;
        let r = response.request();
        if r.store_scope() != &plan.store || r.binding() != &plan.binding || r.blob() != &plan.blob
        {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            let p = fresh(tx, own, &store, plan)?;
            let mut j = current(tx, &run)?;
            if run.offset != plan.blob_len() {
                return Err(CryptoError::Invalid);
            }
            drop(open(tx, plan, &p)?);
            j.verified = Some(true);
            j.file_acknowledged_at_unix_ms = Some(0);
            tx.put(&key(own, plan.message_id())?, &j.encode_to_vec())?;
            // Only the surrounding successful COMMIT may publish this locator.
            Ok(EpochAttachmentCache { plan: plan.clone() })
        })
    }
}
struct Sink<'a> {
    worker: &'a CryptoCoordinator,
    run: Run,
    last: Option<BlobCiphertextChunk>,
    error: Option<CryptoError>,
}
#[async_trait]
impl BlobCiphertextSink for Sink<'_> {
    async fn write(&mut self, chunk: BlobCiphertextChunk) -> Result<(), BlobSinkError> {
        let result = self
            .worker
            .request(|reply| {
                Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Append(
                    Box::new(self.run.clone()),
                    chunk,
                    reply,
                )))
            })
            .await;
        match result {
            Ok((offset, chunk)) => {
                self.run.offset = offset;
                self.last = Some(chunk);
                Ok(())
            }
            Err(e) => {
                self.error = Some(e);
                Err(BlobSinkError)
            }
        }
    }
}
impl CryptoCoordinator {
    pub(in crate::encryption) async fn join_attachment_work(&self) -> CryptoResult<()> {
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Joined(r)))
        })
        .await
    }
    pub async fn cached_epoch_attachment(
        &self,
        uuid: String,
    ) -> CryptoResult<EpochAttachmentCache> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Cached(
                uuid, r,
            )))
        })
        .await
    }
    pub async fn epoch_attachment_download_plan(
        &self,
        uuid: String,
    ) -> CryptoResult<EpochAttachmentDownloadPlan> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Plan(
                uuid, r,
            )))
        })
        .await
    }
    /// Explicit native operation, not an account poller/IPC command. Same HTTP
    /// transport as PUT; no new credentials/URL/client or per-file spawned task.
    pub async fn download_epoch_attachment<T: EpochBlobDownloadTransport>(
        &self,
        http: &mut T,
        plan: EpochAttachmentDownloadPlan,
    ) -> Result<EpochAttachmentCache, AttachmentDownloadError> {
        let permit = Arc::new(self.attachment_permit()?);
        let start = self
            .request(|r| {
                Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Begin(
                    Box::new(plan),
                    permit,
                    r,
                )))
            })
            .await?;
        let run = match start {
            Start::Cached(cache) => return Ok(cache),
            Start::Download(run) => run,
        };
        let request = run.plan.read_request();
        let mut sink = Sink {
            worker: self,
            run,
            last: None,
            error: None,
        };
        let response = match http.get(request, &mut sink).await {
            Ok(r) => r,
            Err(e) => {
                return Err(match sink.error.take() {
                    Some(e) => e.into(),
                    None => e.into(),
                });
            }
        };
        // The sink's LAST chunk retains the HTTP admission through final SQLite
        // verification/COMMIT. Queued/running writes own their chunks separately.
        let last = sink.last.take().ok_or(CryptoError::Invalid)?;
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Finish(
                Box::new(sink.run.clone()),
                Box::new(response),
                last,
                r,
            )))
        })
        .await
        .map_err(Into::into)
    }
    pub async fn open_cached_epoch_attachment(
        &self,
        cache: EpochAttachmentCache,
    ) -> CryptoResult<EpochAttachmentFile> {
        let permit = Arc::new(self.attachment_permit()?);
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Open(
                Box::new(cache),
                permit,
                r,
            )))
        })
        .await
    }
    /// Local read-only saved incoming/outgoing file in a known own-generation archive.
    /// No download/FILEACK plan, network fallback, mutation or key export.
    pub async fn open_archived_epoch_attachment(
        &self,
        generation: String,
        conversation: String,
        message: String,
    ) -> CryptoResult<EpochAttachmentFile> {
        self.open_local_epoch_attachment(Some(generation), conversation, message)
            .await
    }
    pub(in crate::encryption) async fn open_local_epoch_attachment(
        &self,
        generation: Option<String>,
        conversation: String,
        message: String,
    ) -> CryptoResult<EpochAttachmentFile> {
        if generation
            .as_deref()
            .is_some_and(|id| !epoch_rpc::uuid4(id))
            || !epoch_rpc::uuid4(&conversation)
            || !epoch_rpc::uuid4(&message)
        {
            return Err(CryptoError::Invalid);
        }
        let permit = Arc::new(self.attachment_permit()?);
        self.request(|reply| {
            Command::Epoch(EpochCommand::AttachmentDownload(
                DownloadCommand::OpenArchive(generation, conversation, message, permit, reply),
            ))
        })
        .await
    }
    /// Explicit native deletion of a known verified copy with durable FILEACK
    /// (incoming) or terminal Send evidence (outgoing). History/receipt survive;
    /// bytes cannot be resurrected
    /// by an old locator/plan. Read-only ArchiveOwner has no deletion API.
    pub async fn remove_local_epoch_attachment(
        &self,
        generation: String,
        conversation: String,
        message: String,
    ) -> CryptoResult<()> {
        if !epoch_rpc::uuid4(&generation)
            || !epoch_rpc::uuid4(&conversation)
            || !epoch_rpc::uuid4(&message)
        {
            return Err(CryptoError::Invalid);
        }
        let permit = Arc::new(self.attachment_permit()?);
        self.request(|reply| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Remove(
                generation,
                conversation,
                message,
                permit,
                reply,
            )))
        })
        .await
    }
}

#[cfg(test)]
#[path = "epoch_attachment_download_tests.rs"]
pub(super) mod tests;
