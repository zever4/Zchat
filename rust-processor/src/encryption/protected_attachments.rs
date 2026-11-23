//! Ciphertext-only local spool. The secret preparation journal and all chunks
//! publish in ONE protected-store transaction; no separate filesystem commit.
use super::*;
use crate::encryption::attachment::{BLOB_OVERHEAD, MAX_BLOB_BYTES};

pub(crate) const CHUNK_BYTES: usize = 256 * 1024;
pub(crate) const MAX_BLOBS: usize = 64;
pub(crate) const MAX_BYTES: usize = 256 * 1024 * 1024;
const MAX_CHUNKS: usize = MAX_BLOB_BYTES.div_ceil(CHUNK_BYTES);

pub(super) const TABLES: &str = "CREATE TABLE protected_attachment_blobs (
    record_key TEXT PRIMARY KEY,
    generation BLOB NOT NULL CHECK(length(generation)=16),
    kind INTEGER NOT NULL CHECK(kind=13),
    record_id TEXT NOT NULL,
    message_id TEXT NOT NULL CHECK(length(message_id)=36),
    blob_id TEXT NOT NULL CHECK(length(blob_id)=36),
    ciphertext_size INTEGER NOT NULL CHECK(ciphertext_size BETWEEN 37 AND 52428836),
    chunk_count INTEGER NOT NULL CHECK(chunk_count BETWEEN 1 AND 201),
    CHECK(record_id='attachment-prepare:' || message_id),
    CHECK(record_key=lower(hex(generation)) || ':13:' || record_id),
    UNIQUE(generation,kind,record_id), UNIQUE(generation,blob_id),
    FOREIGN KEY(generation,kind,record_id) REFERENCES protected_records(generation,kind,record_id) DEFERRABLE INITIALLY DEFERRED
    ) WITHOUT ROWID;
    CREATE TABLE protected_attachment_chunks (
    record_key TEXT NOT NULL,
    chunk_index INTEGER NOT NULL CHECK(chunk_index BETWEEN 0 AND 200),
    payload BLOB NOT NULL CHECK(length(payload) BETWEEN 1 AND 262144),
    PRIMARY KEY(record_key,chunk_index),
    FOREIGN KEY(record_key) REFERENCES protected_attachment_blobs(record_key) DEFERRABLE INITIALLY DEFERRED
    ) WITHOUT ROWID;";

fn reference(key: &RecordKey) -> StoreResult<String> {
    if key.kind != RecordKind::Control {
        return Err(StoreError::InvalidScope);
    }
    canonical_uuid(
        key.id
            .strip_prefix("attachment-prepare:")
            .ok_or(StoreError::InvalidScope)?,
    )?;
    Ok(format!("{}:13:{}", key.generation.simple(), key.id))
}

impl ProtectedTransaction<'_> {
    /// A retired spool retains its protected preparation, with no manifest,
    /// chunks or attachment_ref. Never treat partially missing data as retired.
    pub(crate) fn check_removed_attachment(&mut self, key: &RecordKey) -> StoreResult<()> {
        let result = (|| {
            let reference = reference(key)?;
            let valid: bool = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id=?3 AND attachment_ref IS NULL)
                 AND NOT EXISTS(SELECT 1 FROM protected_attachment_blobs WHERE record_key=?4)
                 AND NOT EXISTS(SELECT 1 FROM protected_attachment_chunks WHERE record_key=?4)",
                params![key.generation.as_bytes().as_slice(),key.kind as u8,key.id,reference],|r|r.get(0),
            )?;
            if !valid {
                return Err(StoreError::Format);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    /// The domain must validate terminal delivery evidence and full file AEAD
    /// before calling this in the SAME transaction as its protected marker.
    pub(crate) fn remove_known_attachment(
        &mut self,
        key: &RecordKey,
        blob: &str,
        bytes: usize,
        hash: &[u8; 32],
    ) -> StoreResult<()> {
        let result = (|| {
            self.attachment_blob(key, blob, bytes, hash, false)?;
            let reference = reference(key)?;
            if self.connection.execute(
                "UPDATE protected_records SET attachment_ref=NULL WHERE generation=?1 AND kind=?2 AND record_id=?3 AND attachment_ref=?4",
                params![key.generation.as_bytes().as_slice(),key.kind as u8,key.id,reference],
            )? != 1 { return Err(StoreError::Format); }
            self.connection.execute(
                "DELETE FROM protected_attachment_chunks WHERE record_key=?1",
                [&reference],
            )?;
            if self.connection.execute(
                "DELETE FROM protected_attachment_blobs WHERE record_key=?1",
                [reference],
            )? != 1
            {
                return Err(StoreError::Format);
            }
            self.check_removed_attachment(key)
        })();
        self.failed |= result.is_err();
        result
    }
    /// Selection hints only. Bound the ENTIRE manifest before filtering the
    /// current generation, so a corrupt foreign namespace cannot cause a scan.
    pub(crate) fn attachment_candidates(&mut self, generation: &str) -> StoreResult<Vec<String>> {
        let result = (|| {
            let own = canonical_uuid(generation)?;
            let mut q = self.connection.prepare("SELECT CASE WHEN length(generation)=16 THEN generation ELSE NULL END,CASE WHEN length(message_id)=36 THEN message_id ELSE NULL END FROM protected_attachment_blobs ORDER BY record_key LIMIT 65")?;
            let mut rows = q.query([])?;
            let mut result = Vec::new();
            let mut count = 0;
            while let Some(row) = rows.next()? {
                count += 1;
                if count > MAX_BLOBS {
                    return Err(StoreError::Format);
                }
                let generation: Option<Vec<u8>> = row.get(0)?;
                let message: Option<String> = row.get(1)?;
                let generation = generation.ok_or(StoreError::Format)?;
                let message = message.ok_or(StoreError::Format)?;
                canonical_uuid(&message)?;
                if generation.as_slice() == own.as_bytes() {
                    result.push(message);
                }
            }
            result.sort();
            Ok(result)
        })();
        self.failed |= result.is_err();
        result
    }

    /// Indexed absence proof for a UUID whose private journal is missing.
    /// Partial artifacts must never turn a damaged input into a vacant slot.
    pub(crate) fn require_empty_attachment_slot(&mut self, key: &RecordKey) -> StoreResult<()> {
        let result = (|| {
            let reference = reference(key)?;
            let occupied: bool = self.connection.query_row("SELECT EXISTS(SELECT 1 FROM protected_attachment_blobs WHERE record_key=?1) OR EXISTS(SELECT 1 FROM protected_attachment_chunks WHERE record_key=?1)", [&reference], |r| r.get(0))?;
            if occupied {
                return Err(StoreError::Format);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    /// Bounded aggregate over the dedicated manifest table, not history/chunks.
    /// Logical quota includes all generations. SQLite pages/journal overhead is
    /// additional; a full filesystem still fails the surrounding transaction.
    pub(crate) fn attachment_capacity(&mut self, bytes: usize) -> StoreResult<()> {
        let result = (|| {
            let (count, used): (i64, i64) = self.connection.query_row(
                "SELECT count(*),coalesce(sum(ciphertext_size),0) FROM (
                    SELECT ciphertext_size FROM (SELECT ciphertext_size FROM protected_attachment_blobs LIMIT 65)
                    UNION ALL SELECT ciphertext_size FROM (SELECT ciphertext_size FROM protected_attachment_downloads LIMIT 65))",
                [], |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if count < 0 || used < 0 {
                return Err(StoreError::Format);
            }
            if count >= MAX_BLOBS as i64 || bytes > MAX_BYTES || used > (MAX_BYTES - bytes) as i64 {
                return Err(StoreError::Capacity);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }

    pub(crate) fn put_attachment_blob(
        &mut self,
        key: &RecordKey,
        blob_id: &str,
        bytes: &[u8],
    ) -> StoreResult<()> {
        let result = (|| {
            let reference = reference(key)?;
            canonical_uuid(blob_id)?;
            if bytes.len() <= BLOB_OVERHEAD || bytes.len() > MAX_BLOB_BYTES {
                return Err(StoreError::RecordTooLarge);
            }
            self.attachment_capacity(bytes.len())?;
            let count = bytes.len().div_ceil(CHUNK_BYTES);
            let message = key.id.strip_prefix("attachment-prepare:").unwrap();
            self.connection.execute(
                "INSERT INTO protected_attachment_blobs VALUES(?1,?2,13,?3,?4,?5,?6,?7)",
                params![
                    reference,
                    key.generation.as_bytes().as_slice(),
                    key.id,
                    message,
                    blob_id,
                    bytes.len() as i64,
                    count as i64
                ],
            )?;
            {
                let mut insert = self
                    .connection
                    .prepare("INSERT INTO protected_attachment_chunks VALUES(?1,?2,?3)")?;
                for (index, chunk) in bytes.chunks(CHUNK_BYTES).enumerate() {
                    insert.execute(params![reference, index as i64, chunk])?;
                }
            }
            if self.connection.execute(
                "UPDATE protected_records SET attachment_ref=?4 WHERE generation=?1 AND kind=?2 AND record_id=?3",
                params![key.generation.as_bytes().as_slice(), key.kind as u8, key.id, reference],
            )? != 1 {
                return Err(StoreError::Format);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }

    /// Check the manifest against the authenticated journal. A presentation
    /// read can use this without reading chunks or asserting file integrity.
    pub(crate) fn check_attachment_manifest(
        &mut self,
        key: &RecordKey,
        blob_id: &str,
        size: usize,
    ) -> StoreResult<()> {
        let result = (|| {
            let reference = reference(key)?;
            canonical_uuid(blob_id)?;
            if size <= BLOB_OVERHEAD || size > MAX_BLOB_BYTES {
                return Err(StoreError::RecordTooLarge);
            }
            let count = size.div_ceil(CHUNK_BYTES);
            if count > MAX_CHUNKS {
                return Err(StoreError::Format);
            }
            let valid: bool = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM protected_attachment_blobs b JOIN protected_records r ON r.generation=b.generation AND r.kind=b.kind AND r.record_id=b.record_id WHERE b.record_key=?1 AND r.attachment_ref=?1 AND b.generation=?2 AND b.kind=13 AND b.record_id=?3 AND b.blob_id=?4 AND b.ciphertext_size=?5 AND b.chunk_count=?6)",
                params![reference, key.generation.as_bytes().as_slice(), key.id, blob_id, size as i64, count as i64], |r| r.get(0),
            )?;
            if !valid {
                return Err(StoreError::Format);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }

    /// Verify the complete cohort/hash. Only a bounded chunk is allocated at a
    /// time unless collect=true is explicit.
    pub(crate) fn attachment_blob(
        &mut self,
        key: &RecordKey,
        blob_id: &str,
        size: usize,
        digest: &[u8; 32],
        collect: bool,
    ) -> StoreResult<Vec<u8>> {
        let result = (|| {
            self.check_attachment_manifest(key, blob_id, size)?;
            let reference = reference(key)?;
            let count = size.div_ceil(CHUNK_BYTES);
            let mut select = self.connection.prepare(
                "SELECT chunk_index,CASE WHEN length(payload) BETWEEN 1 AND 262144 THEN payload ELSE NULL END FROM protected_attachment_chunks WHERE record_key=?1 ORDER BY chunk_index LIMIT ?2",
            )?;
            let mut rows = select.query(params![reference, (count + 1) as i64])?;
            let mut hash = Sha256::new();
            let mut output = if collect {
                Vec::with_capacity(size)
            } else {
                Vec::new()
            };
            let mut seen = 0;
            while let Some(row) = rows.next()? {
                let index: usize = row
                    .get::<_, i64>(0)?
                    .try_into()
                    .map_err(|_| StoreError::Format)?;
                let bytes: Option<Vec<u8>> = row.get(1)?;
                let bytes = bytes.ok_or(StoreError::Protection)?;
                if index != seen
                    || index >= count
                    || bytes.len() != (size - index * CHUNK_BYTES).min(CHUNK_BYTES)
                {
                    return Err(StoreError::Format);
                }
                hash.update(&bytes);
                if collect {
                    output.extend(bytes);
                }
                seen += 1;
            }
            if seen != count || <[u8; 32]>::from(hash.finalize()) != *digest {
                return Err(StoreError::Protection);
            }
            Ok(output)
        })();
        self.failed |= result.is_err();
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const GENERATION: &str = "33333333-3333-4333-8333-333333333333";
    const REPLACEMENT: &str = "44444444-4444-4444-8444-444444444444";
    fn scope() -> StoreScope {
        StoreScope::new(
            "https://chat.example.test",
            "11111111-1111-4111-8111-111111111111",
        )
        .unwrap()
    }
    fn connect(root: &Path, scope: StoreScope, create: bool) -> ProtectedStore {
        super::super::test_open(root, scope, create)
    }
    fn record(generation: &str, n: usize) -> RecordKey {
        RecordKey::new(
            generation,
            RecordKind::Control,
            &format!("attachment-prepare:00000000-0000-4000-8000-{n:012x}"),
        )
        .unwrap()
    }
    #[test]
    fn explicit_spool_removal_preserves_journal_frees_shared_quota_and_poisons_swallowed_failure() {
        let root = tempfile::tempdir().unwrap();
        let mut s = connect(root.path(), scope(), true);
        let first = record(GENERATION, 0);
        let blob = Uuid::new_v4().to_string();
        let bytes = [7; 37];
        let hash: [u8; 32] = Sha256::digest(bytes).into();
        for n in 0..MAX_BLOBS {
            let k = record(GENERATION, n);
            let id = if n == 0 {
                blob.clone()
            } else {
                Uuid::new_v4().to_string()
            };
            s.transaction(|tx| {
                tx.put(&k, b"domain-validated journal placeholder")?;
                tx.put_attachment_blob(&k, &id, &bytes)
            })
            .unwrap();
        }
        // Storage only; full domain permission/AEAD and terminal proof are
        // exercised by the worker tests. A swallowed error must poison COMMIT.
        assert!(
            s.transaction(|tx| {
                let _ = tx.remove_known_attachment(&first, &blob, bytes.len(), &[0; 32]);
                tx.remove_known_attachment(&first, &blob, bytes.len(), &hash)
            })
            .is_err()
        );
        assert_eq!(
            s.transaction(|tx| tx.attachment_blob(&first, &blob, bytes.len(), &hash, true))
                .unwrap(),
            bytes
        );
        let ciphertext = s
            .connection
            .query_row::<Vec<u8>, _, _>(
                "SELECT payload FROM protected_records WHERE record_id=?1",
                [&first.id],
                |r| r.get(0),
            )
            .unwrap();
        s.transaction(|tx| tx.remove_known_attachment(&first, &blob, bytes.len(), &hash))
            .unwrap();
        assert_eq!(
            s.connection
                .query_row::<Vec<u8>, _, _>(
                    "SELECT payload FROM protected_records WHERE record_id=?1",
                    [&first.id],
                    |r| r.get(0)
                )
                .unwrap(),
            ciphertext
        );
        s.transaction(|tx| tx.check_removed_attachment(&first))
            .unwrap();
        let incoming = RecordKey::new(
            REPLACEMENT,
            RecordKind::Control,
            "attachment-download:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        )
        .unwrap();
        s.transaction(|tx| {
            tx.put(&incoming, b"new incoming journal")?;
            tx.create_download_manifest(&incoming, &Uuid::new_v4().to_string(), 37)
        })
        .unwrap();
    }
    #[test]
    fn attachment_quota_counts_all_generations_and_never_publishes_partial_overflow() {
        let root = tempfile::tempdir().unwrap();
        let mut store = connect(root.path(), scope(), true);
        store
            .transaction(|tx| tx.attachment_capacity(MAX_BYTES))
            .unwrap();
        assert!(matches!(
            store.transaction(|tx| tx.attachment_capacity(MAX_BYTES + 1)),
            Err(StoreError::Capacity)
        ));
        for n in 0..MAX_BLOBS {
            let key = record(if n % 2 == 0 { GENERATION } else { REPLACEMENT }, n);
            store
                .transaction(|tx| {
                    tx.put(&key, b"test protected descriptor journal")?;
                    tx.put_attachment_blob(&key, &Uuid::new_v4().to_string(), &[7; 37])
                })
                .unwrap();
        }
        assert_eq!(
            store
                .connection
                .query_row::<i64, _, _>(
                    "SELECT count(DISTINCT generation) FROM protected_attachment_blobs",
                    [],
                    |r| r.get(0)
                )
                .unwrap(),
            2
        );
        let extra = record(GENERATION, MAX_BLOBS);
        assert!(matches!(
            store.transaction(|tx| {
                tx.put(&extra, b"must roll back")?;
                tx.put_attachment_blob(&extra, &Uuid::new_v4().to_string(), &[7; 37])
            }),
            Err(StoreError::Capacity)
        ));
        assert!(store.transaction(|tx| tx.get(&extra)).unwrap().is_none());
        assert_eq!(
            store
                .connection
                .query_row::<i64, _, _>(
                    "SELECT count(*) FROM protected_attachment_chunks",
                    [],
                    |r| r.get(0)
                )
                .unwrap(),
            MAX_BLOBS as i64
        );
    }
    #[test]
    fn sqlite_full_during_chunk_write_rolls_back_blob_and_private_journal() {
        let root = tempfile::tempdir().unwrap();
        let mut store = connect(root.path(), scope(), true);
        let pages: i64 = store
            .connection
            .pragma_query_value(None, "page_count", |r| r.get(0))
            .unwrap();
        let limit: i64 = store
            .connection
            .pragma_query_value(None, "max_page_count", |r| r.get(0))
            .unwrap();
        store
            .connection
            .pragma_update(None, "max_page_count", pages + 8)
            .unwrap();
        let key = record(GENERATION, 3);
        let result = store.transaction(|tx| {
            tx.put(&key, b"must not outlive a failed blob write")?;
            tx.put_attachment_blob(&key, &Uuid::new_v4().to_string(), &vec![7; 600 * 1024])
        });
        assert!(
            matches!(result,Err(StoreError::Database(rusqlite::Error::SqliteFailure(ref e,_))) if e.code==rusqlite::ErrorCode::DiskFull)
        );
        assert!(store.transaction(|tx| tx.get(&key)).unwrap().is_none());
        for table in ["protected_attachment_blobs", "protected_attachment_chunks"] {
            assert_eq!(
                store
                    .connection
                    .query_row::<i64, _, _>(&format!("SELECT count(*) FROM {table}"), [], |r| r
                        .get(0))
                    .unwrap(),
                0
            );
        }
        store
            .connection
            .pragma_update(None, "max_page_count", limit)
            .unwrap();
        store
            .transaction(|tx| {
                tx.put(&key, b"explicit retry after capacity is restored")?;
                tx.put_attachment_blob(&key, &Uuid::new_v4().to_string(), &vec![7; 600 * 1024])
            })
            .unwrap();
    }

    #[test]
    fn attachment_byte_budget_and_swallowed_fault_abort_the_whole_transaction() {
        let root = tempfile::tempdir().unwrap();
        let mut store = connect(root.path(), scope(), true);
        let key = record(GENERATION, 1);
        store
            .transaction(|tx| {
                tx.put(&key, b"journal")?;
                tx.put_attachment_blob(&key, &Uuid::new_v4().to_string(), &[7; 37])
            })
            .unwrap();
        store
            .transaction(|tx| tx.attachment_capacity(MAX_BYTES - 37))
            .unwrap();
        assert!(matches!(
            store.transaction(|tx| tx.attachment_capacity(MAX_BYTES - 36)),
            Err(StoreError::Capacity)
        ));
        let extra = record(GENERATION, 2);
        assert!(matches!(
            store.transaction(|tx| {
                tx.put(&extra, b"must roll back")?;
                assert!(tx.attachment_capacity(MAX_BYTES).is_err());
                Ok(())
            }),
            Err(StoreError::Aborted)
        ));
        assert!(store.transaction(|tx| tx.get(&extra)).unwrap().is_none());
    }
}
