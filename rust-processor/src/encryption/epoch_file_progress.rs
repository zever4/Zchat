//! Sender FILE progress is independent of descriptor/message delivery. Only
//! authenticated native observations add facts; no client-clock expiry or GC.
use super::*;
use crate::encryption::{file_status as contract, native::EpochFileStatuses};

const JOURNALS: &str = "file-progress:";
const ANCHORS: &str = "file-progress-anchor:";
const PENDING: &str = "file-progress-pending:";
const INDEX: &str = "file-progress-index:v1";
const INDEX_ANCHOR: &str = "file-progress-index-anchor:v1";
const DOMAIN: &[u8] = b"zchat.e2ee.file-progress.v1\0";
const INDEX_DOMAIN: &[u8] = b"zchat.e2ee.file-progress-index.v1\0";

#[derive(Clone, PartialEq, Message)]
struct FileJournal {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    reference: Option<v2::FileStatusReference>,
    #[prost(message, optional, tag = "3")]
    receipt: Option<v2::SendReceipt>,
    #[prost(int64, optional, tag = "4")]
    observed_at: Option<i64>,
    #[prost(int64, optional, tag = "5")]
    acknowledged_at: Option<i64>,
    #[prost(int64, optional, tag = "6")]
    replaced_at: Option<i64>,
    #[prost(uint64, tag = "7")]
    revision: u64,
}
#[derive(PartialEq, Message)]
struct FileAnchor {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(uint64, tag = "2")]
    revision: u64,
    #[prost(bytes = "vec", tag = "3")]
    digest: Vec<u8>,
}
#[derive(Message)]
struct Index {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    after: String,
    #[prost(bool, optional, tag = "3")]
    ready: Option<bool>,
    #[prost(uint64, tag = "4")]
    revision: u64,
}

/// Local metadata facts, never proof of server filesystem removal or of a
/// verified local file. No private descriptor, plaintext or key is exposed.
#[derive(Clone)]
pub struct EpochFileProgress(FileJournal);
impl EpochFileProgress {
    pub fn message_id(&self) -> &str {
        &self.receipt().message_id
    }
    pub fn receipt(&self) -> &v2::SendReceipt {
        self.0.receipt.as_ref().expect("validated journal")
    }
    pub fn expires_at_unix_ms(&self) -> i64 {
        self.receipt().stored_at_unix_ms + contract::FILE_RETENTION_MS
    }
    pub fn observed_at_unix_ms(&self) -> Option<i64> {
        self.0.observed_at
    }
    pub fn acknowledged_at_unix_ms(&self) -> Option<i64> {
        self.0.acknowledged_at
    }
    pub fn recipient_replaced_at_unix_ms(&self) -> Option<i64> {
        self.0.replaced_at
    }
    /// Authenticated server time passed the logical deadline. Not physical GC.
    pub fn deadline_observed(&self) -> bool {
        self.0
            .observed_at
            .is_some_and(|at| at >= self.expires_at_unix_ms())
    }
    pub fn needs_poll(&self) -> bool {
        self.0.acknowledged_at.is_none() && self.0.replaced_at.is_none()
    }
    pub fn revision(&self) -> u64 {
        self.0.revision
    }
}
fn key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    Ok(RecordKey::new(&own.generation_id, RecordKind::Control, id)?)
}
fn row_key(own: &DeviceRef, prefix: &str, id: &str) -> CryptoResult<RecordKey> {
    parse_id(id)?;
    key(own, &format!("{prefix}{id}"))
}
fn anchor(bytes: &[u8], revision: u64, domain: &[u8]) -> CryptoResult<FileAnchor> {
    if revision == 0 || revision > i64::MAX as u64 {
        return Err(CryptoError::Invalid);
    }
    Ok(FileAnchor {
        format: 1,
        revision,
        digest: digest(domain, bytes).to_vec(),
    })
}
fn reference(r: &v2::SendMessageRequest) -> CryptoResult<v2::FileStatusReference> {
    let e = r.envelope.as_ref().ok_or(CryptoError::Invalid)?;
    if e.protocol_version != 3 {
        return Err(CryptoError::Invalid);
    }
    Ok(v2::FileStatusReference {
        binding: Some(data::send_binding(e).map_err(|_| CryptoError::Invalid)?),
        blob: e.blob.clone(),
    })
}
fn request(own: &DeviceRef, files: Vec<v2::FileStatusReference>) -> v2::GetFileStatusesRequest {
    v2::GetFileStatusesRequest {
        context: Some(ClientContext {
            protocol_version: 2,
            device_generation_id: own.generation_id.clone(),
        }),
        files,
    }
}
fn load_index(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<Option<Index>> {
    match (
        tx.get(&key(own, INDEX)?)?,
        tx.get(&key(own, INDEX_ANCHOR)?)?,
    ) {
        (None, None) => {
            for prefix in [JOURNALS, ANCHORS, PENDING] {
                if tx.has_record_prefix(&own.generation_id, RecordKind::Control, prefix)? {
                    return Err(CryptoError::Invalid);
                }
            }
            Ok(None)
        }
        (Some(b), Some(a)) => {
            let s = Index::decode(b.as_slice()).map_err(|_| CryptoError::Invalid)?;
            let a = FileAnchor::decode(a.as_slice()).map_err(|_| CryptoError::Invalid)?;
            if s.format != 1 || s.ready.is_none() || a != anchor(&b, s.revision, INDEX_DOMAIN)? {
                return Err(CryptoError::Conflict);
            }
            if !s.after.is_empty() {
                parse_id(&s.after)?;
            }
            Ok(Some(s))
        }
        _ => Err(CryptoError::Invalid),
    }
}
fn save_index(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    s: &mut Index,
) -> CryptoResult<()> {
    s.revision = s.revision.checked_add(1).ok_or(CryptoError::Invalid)?;
    let b = s.encode_to_vec();
    tx.put(&key(own, INDEX)?, &b)?;
    tx.put(
        &key(own, INDEX_ANCHOR)?,
        &anchor(&b, s.revision, INDEX_DOMAIN)?.encode_to_vec(),
    )?;
    Ok(())
}
fn index(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<Index> {
    if let Some(s) = load_index(tx, own)? {
        return Ok(s);
    }
    let mut s = Index {
        format: 1,
        after: String::new(),
        ready: Some(false),
        revision: 0,
    };
    save_index(tx, own, &mut s)?;
    Ok(s)
}
fn current_owner(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<()> {
    recovery_indexes::require_ready(tx, own)?;
    if tx
        .get(&key(
            own,
            crate::encryption::provisioning::ARCHIVED_DEVICE_RECORD,
        )?)?
        .is_some()
    {
        return Err(CryptoError::ArchivedGeneration);
    }
    Ok(())
}
fn require_ready(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<()> {
    current_owner(tx, own)?;
    if !load_index(tx, own)?.is_some_and(|s| s.ready == Some(true)) {
        return Err(CryptoError::FileStatusIndexNotReady);
    }
    Ok(())
}
fn load(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    send: &v2::SendMessageRequest,
    receipt: &v2::SendReceipt,
) -> CryptoResult<Option<FileJournal>> {
    let journal = tx.get(&row_key(own, JOURNALS, id)?)?;
    let a = tx.get(&row_key(own, ANCHORS, id)?)?;
    let pending = tx.get(&row_key(own, PENDING, id)?)?;
    let Some(b) = journal else {
        if a.is_some() || pending.is_some() {
            return Err(CryptoError::Invalid);
        }
        return Ok(None);
    };
    let j = FileJournal::decode(b.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let a = FileAnchor::decode(a.ok_or(CryptoError::Invalid)?.as_slice())
        .map_err(|_| CryptoError::Invalid)?;
    if j.format != 1
        || j.reference != Some(reference(send)?)
        || j.receipt.as_ref() != Some(receipt)
        || a != anchor(&b, j.revision, DOMAIN)?
    {
        return Err(CryptoError::Conflict);
    }
    data::validate_receipt(send, receipt).map_err(|_| CryptoError::Invalid)?;
    let r = request(own, vec![reference(send)?]);
    contract::validate_response(
        &r,
        &v2::FileStatuses {
            files: vec![v2::FileStatus {
                format_version: 1,
                reference: j.reference.clone(),
                receipt: j.receipt.clone(),
                observed_at_unix_ms: j.observed_at.unwrap_or(receipt.stored_at_unix_ms),
                expires_at_unix_ms: receipt
                    .stored_at_unix_ms
                    .checked_add(contract::FILE_RETENTION_MS)
                    .ok_or(CryptoError::Invalid)?,
                acknowledged_at_unix_ms: j.acknowledged_at,
                recipient_replaced_at_unix_ms: j.replaced_at,
            }],
        },
    )
    .map_err(|_| CryptoError::Invalid)?;
    if j.observed_at.is_none() && (j.acknowledged_at.is_some() || j.replaced_at.is_some()) {
        return Err(CryptoError::Invalid);
    }
    if EpochFileProgress(j.clone()).needs_poll() {
        let p = FileAnchor::decode(pending.ok_or(CryptoError::Invalid)?.as_slice())
            .map_err(|_| CryptoError::Invalid)?;
        if p != a {
            return Err(CryptoError::Conflict);
        }
    } else if pending.is_some() {
        return Err(CryptoError::Conflict);
    }
    Ok(Some(j))
}
fn save(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    j: &mut FileJournal,
) -> CryptoResult<()> {
    j.revision = j.revision.checked_add(1).ok_or(CryptoError::Invalid)?;
    let b = j.encode_to_vec();
    let a = anchor(&b, j.revision, DOMAIN)?.encode_to_vec();
    tx.put(&row_key(own, JOURNALS, id)?, &b)?;
    tx.put(&row_key(own, ANCHORS, id)?, &a)?;
    let pending = row_key(own, PENDING, id)?;
    if EpochFileProgress(j.clone()).needs_poll() {
        tx.put(&pending, &a)?;
    } else {
        tx.remove_unindexed_control(&pending)?;
    }
    Ok(())
}
// Called in EVERY receipt-introducing transaction. Already accepted retries may
// backfill only while explicit seeding is incomplete, never repair a ready index.
pub(super) fn track_receipt(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    send: &v2::SendMessageRequest,
    receipt: &v2::SendReceipt,
    newly_accepted: bool,
) -> CryptoResult<()> {
    if send
        .envelope
        .as_ref()
        .ok_or(CryptoError::Invalid)?
        .protocol_version
        != 3
    {
        return Ok(());
    }
    let s = index(tx, own)?;
    if load(tx, own, id, send, receipt)?.is_none() {
        if !newly_accepted && s.ready == Some(true) {
            return Err(CryptoError::Invalid);
        }
        let mut j = FileJournal {
            format: 1,
            reference: Some(reference(send)?),
            receipt: Some(receipt.clone()),
            observed_at: None,
            acknowledged_at: None,
            replaced_at: None,
            revision: 0,
        };
        // Validate the immutable shape before any protected write.
        data::validate_receipt(send, receipt).map_err(|_| CryptoError::Invalid)?;
        contract::validate_request(&request(own, vec![reference(send)?]))
            .map_err(|_| CryptoError::Invalid)?;
        receipt
            .stored_at_unix_ms
            .checked_add(contract::FILE_RETENTION_MS)
            .ok_or(CryptoError::Invalid)?;
        save(tx, own, id, &mut j)?;
    }
    Ok(())
}

#[path = "epoch_file_progress_commands.rs"]
pub(super) mod commands;
pub use commands::{EpochFileStatusCursor, EpochFileStatusPage, FileStatusSeedProgress};

#[cfg(test)]
#[path = "epoch_file_progress_tests.rs"]
mod tests;
