//! Ciphertext-only incoming staging/cache. Private state is an AEAD-protected
//! record; these bounded tables never contain file keys or plaintext files.
use super::*;
use crate::encryption::attachment::{BLOB_OVERHEAD, MAX_BLOB_BYTES};
use attachments::CHUNK_BYTES;

pub(super) const TABLES: &str = "CREATE TABLE protected_attachment_downloads (
    record_key TEXT PRIMARY KEY,
    generation BLOB NOT NULL CHECK(length(generation)=16),
    kind INTEGER NOT NULL CHECK(kind=13),
    record_id TEXT NOT NULL,
    message_id TEXT NOT NULL CHECK(length(message_id)=36),
    blob_id TEXT NOT NULL CHECK(length(blob_id)=36),
    ciphertext_size INTEGER NOT NULL CHECK(ciphertext_size BETWEEN 37 AND 52428836),
    CHECK(record_id='attachment-download:' || message_id),
    CHECK(record_key=lower(hex(generation)) || ':13:' || record_id),
    UNIQUE(generation,kind,record_id), UNIQUE(generation,blob_id),
    FOREIGN KEY(generation,kind,record_id) REFERENCES protected_records(generation,kind,record_id) DEFERRABLE INITIALLY DEFERRED
    ) WITHOUT ROWID;
    CREATE TABLE protected_attachment_download_chunks (
    record_key TEXT NOT NULL,
    chunk_index INTEGER NOT NULL CHECK(chunk_index BETWEEN 0 AND 200),
    payload BLOB NOT NULL CHECK(length(payload) BETWEEN 1 AND 262144),
    PRIMARY KEY(record_key,chunk_index),
    FOREIGN KEY(record_key) REFERENCES protected_attachment_downloads(record_key) DEFERRABLE INITIALLY DEFERRED
    ) WITHOUT ROWID;";

fn reference(key: &RecordKey) -> StoreResult<String> {
    if key.kind != RecordKind::Control {
        return Err(StoreError::InvalidScope);
    }
    canonical_uuid(
        key.id
            .strip_prefix("attachment-download:")
            .ok_or(StoreError::InvalidScope)?,
    )?;
    Ok(format!("{}:13:{}", key.generation.simple(), key.id))
}

fn size(size: usize) -> StoreResult<()> {
    if size <= BLOB_OVERHEAD || size > MAX_BLOB_BYTES {
        return Err(StoreError::RecordTooLarge);
    }
    Ok(())
}
impl ProtectedTransaction<'_> {
    pub(crate) fn check_download_manifest(
        &mut self,
        key: &RecordKey,
        blob: &str,
        bytes: usize,
        present: bool,
    ) -> StoreResult<()> {
        let result = (|| {
            let reference = reference(key)?;
            canonical_uuid(blob)?;
            size(bytes)?;
            let actual: Option<(Option<String>, i64)> = self.connection.query_row(
                "SELECT CASE WHEN length(blob_id)=36 THEN blob_id ELSE NULL END,ciphertext_size FROM protected_attachment_downloads WHERE record_key=?1
                    AND generation=?2 AND kind=13 AND record_id=?3",
                params![reference, key.generation.as_bytes().as_slice(), key.id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            ).optional()?;
            if match actual {
                Some((b, n)) => !present || b.as_deref() != Some(blob) || n != bytes as i64,
                None => present,
            } {
                return Err(StoreError::Format);
            }
            if !present && self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM protected_attachment_download_chunks WHERE record_key=?1)",
                [reference], |r|r.get::<_,bool>(0),
            )? {
                return Err(StoreError::Format);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    pub(crate) fn create_download_manifest(
        &mut self,
        key: &RecordKey,
        blob: &str,
        bytes: usize,
    ) -> StoreResult<()> {
        let result = (|| {
            self.check_download_manifest(key, blob, bytes, false)?;
            self.attachment_capacity(bytes)?;
            self.connection.execute(
                "INSERT INTO protected_attachment_downloads VALUES(?1,?2,13,?3,?4,?5,?6)",
                params![
                    reference(key)?,
                    key.generation.as_bytes().as_slice(),
                    key.id,
                    key.id.strip_prefix("attachment-download:").unwrap(),
                    blob,
                    bytes as i64
                ],
            )?;
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    /// Only the authenticated domain journal may authorize resetting incomplete
    /// staging. Verified files and unknown/corrupt journals are never evicted.
    pub(crate) fn reset_download_chunks(&mut self, key: &RecordKey) -> StoreResult<()> {
        let result = (|| {
            self.connection.execute(
                "DELETE FROM protected_attachment_download_chunks WHERE record_key=?1",
                [reference(key)?],
            )?;
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    /// Domain authorization and complete hash/AEAD verification must precede this
    /// call in the SAME transaction. The caller also persists a protected removal
    /// marker; no standalone cleanup by UUID or swallowed partial SQL error.
    pub(crate) fn remove_known_download(
        &mut self,
        key: &RecordKey,
        blob: &str,
        bytes: usize,
    ) -> StoreResult<()> {
        let result = (|| {
            self.check_download_manifest(key, blob, bytes, true)?;
            let reference = reference(key)?;
            self.connection.execute(
                "DELETE FROM protected_attachment_download_chunks WHERE record_key=?1",
                [&reference],
            )?;
            if self.connection.execute(
                "DELETE FROM protected_attachment_downloads WHERE record_key=?1",
                [reference],
            )? != 1
            {
                return Err(StoreError::Format);
            }
            self.remove_unindexed_control(key)
        })();
        self.failed |= result.is_err();
        result
    }
    /// Normalize arbitrary HTTP fragmentation into at most 201 SQLite chunks.
    /// A transaction appends <=256KiB and rewrites at most one partial tail.
    pub(crate) fn append_download_chunk(
        &mut self,
        key: &RecordKey,
        total: usize,
        offset: usize,
        bytes: &[u8],
    ) -> StoreResult<()> {
        let result = (|| {
            size(total)?;
            if bytes.is_empty()
                || bytes.len() > CHUNK_BYTES
                || offset > total
                || bytes.len() > total - offset
            {
                return Err(StoreError::Format);
            }
            let reference = reference(key)?;
            let index = offset / CHUNK_BYTES;
            let tail = offset % CHUNK_BYTES;
            let mut joined = if tail == 0 {
                Vec::new()
            } else {
                let prior: Option<Vec<u8>> = self.connection.query_row(
                    "SELECT CASE WHEN length(payload)=?3 THEN payload ELSE NULL END
                     FROM protected_attachment_download_chunks WHERE record_key=?1 AND chunk_index=?2",
                    params![reference,index as i64,tail as i64], |r| r.get(0))?;
                prior.ok_or(StoreError::Format)?
            };
            joined.extend(bytes);
            for (i, part) in joined.chunks(CHUNK_BYTES).enumerate() {
                if i == 0 && tail != 0 {
                    if self.connection.execute("UPDATE protected_attachment_download_chunks SET payload=?3 WHERE record_key=?1 AND chunk_index=?2",
                        params![reference,index as i64,part])? != 1 { return Err(StoreError::Format); }
                } else {
                    self.connection.execute(
                        "INSERT INTO protected_attachment_download_chunks VALUES(?1,?2,?3)",
                        params![reference, (index + i) as i64, part],
                    )?;
                }
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    /// One explicitly bounded whole-file allocation for the existing AEAD API.
    /// The caller holds the same account file-buffer permit as preparations.
    pub(crate) fn collect_download(
        &mut self,
        key: &RecordKey,
        total: usize,
        expected: &[u8],
    ) -> StoreResult<Vec<u8>> {
        let result = (|| {
            size(total)?;
            if expected.len() != 32 {
                return Err(StoreError::Format);
            }
            let count = total.div_ceil(CHUNK_BYTES);
            let mut q = self.connection.prepare(
                "SELECT chunk_index,CASE WHEN length(payload) BETWEEN 1 AND 262144 THEN payload ELSE NULL END
                 FROM protected_attachment_download_chunks WHERE record_key=?1 ORDER BY chunk_index LIMIT ?2")?;
            let mut rows = q.query(params![reference(key)?, (count + 1) as i64])?;
            let mut out = Vec::with_capacity(total);
            let mut seen = 0;
            while let Some(row) = rows.next()? {
                let index: i64 = row.get(0)?;
                let part: Option<Vec<u8>> = row.get(1)?;
                let part = part.ok_or(StoreError::Format)?;
                if seen >= count
                    || index != seen as i64
                    || part.len() != (total - seen * CHUNK_BYTES).min(CHUNK_BYTES)
                {
                    return Err(StoreError::Format);
                }
                out.extend(part);
                seen += 1;
            }
            if seen != count || Sha256::digest(&out).as_slice() != expected {
                return Err(StoreError::Protection);
            }
            Ok(out)
        })();
        self.failed |= result.is_err();
        result
    }
}

#[cfg(test)]
#[path = "protected_download_tests.rs"]
mod tests;
