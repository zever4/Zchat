//! Protected persistence foundation for the v1 coordinator, independent of legacy Stores.
//!
//! One database and DPAPI-wrapped random key per server/account; generations coexist
//! as disjoint record namespaces so replacement does not erase readable archives.
//! Only encrypted values reach SQLite, including its rollback journal. Schema,
//! opaque record IDs, generation IDs, pending flags, sizes and access patterns are NOT encrypted.
//! This does not prevent deletion/replay of older valid records or a whole database,
//! or malware running as the Windows user. It provides no monotonic rollback anchor.
//! No automatic import, repair, identity generation, network access or GC occurs here.

use aes_gcm_siv::{
    Aes256GcmSiv, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use rand::{TryRngCore, rngs::OsRng};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    time::Duration,
};
use url::Url;
use uuid::{Uuid, Variant};
use zeroize::Zeroizing;

const FORMAT: i64 = 1;
// Version 6 fences pre-retirement writers: they cannot interpret removed-cache
// markers and must not reopen an upgraded store. Record/key framing is unchanged.
const SCHEMA: i64 = 6;
const APPLICATION_ID: i64 = 0x5a434531; // ZCE1
const FRAME: u8 = 1;
const OVERHEAD: usize = 1 + 12 + 16;
pub const MAX_RECORD_BYTES: usize = 1 << 20;
const KEY_CHECK: &[u8] = b"zchat protected store key check v1";

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("invalid local store scope or record key")]
    InvalidScope,
    #[error("protected E2EE storage currently requires Windows")]
    UnsupportedPlatform,
    #[error("this account store already has a crypto owner")]
    Busy,
    #[error("local storage I/O failed")]
    Io(#[from] std::io::Error),
    #[error("local storage database operation failed")]
    Database(#[from] rusqlite::Error),
    #[error("unsupported or inconsistent local storage format")]
    Format,
    #[error("local data protection failed; existing state has been preserved")]
    Protection,
    #[error("protected record exceeds its size limit")]
    RecordTooLarge,
    #[error("local attachment storage capacity reached")]
    Capacity,
    #[error("local state transaction has been aborted")]
    Aborted,
}
pub type StoreResult<T> = Result<T, StoreError>;

/// Build from the trusted native server configuration and authenticated account UUID,
/// never from a renderer-supplied filesystem path, email, JWT or display name.
#[derive(Clone, PartialEq, Eq)]
pub struct StoreScope {
    namespace: [u8; 32],
    account: Uuid,
}

impl StoreScope {
    pub fn new(server_origin: &str, account_id: &str) -> StoreResult<Self> {
        let url = Url::parse(server_origin).map_err(|_| StoreError::InvalidScope)?;
        let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if url.host_str().is_none()
            || !(url.scheme() == "https" || (url.scheme() == "http" && local))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return Err(StoreError::InvalidScope);
        }
        let account = canonical_uuid(account_id)?;
        let origin = url.origin().ascii_serialization();
        let mut hash = Sha256::new();
        hash.update(b"zchat.e2ee.store.scope.v1\0");
        hash.update((origin.len() as u32).to_be_bytes());
        hash.update(origin.as_bytes());
        hash.update(account.as_bytes());
        Ok(Self {
            namespace: hash.finalize().into(),
            account,
        })
    }

    pub fn database_path(&self, app_data: &Path) -> PathBuf {
        let directory: String = self.namespace.iter().map(|b| format!("{b:02x}")).collect();
        app_data
            .join("e2ee-v1")
            .join(directory)
            .join("state.sqlite")
    }
}

fn canonical_uuid(value: &str) -> StoreResult<Uuid> {
    let id = Uuid::parse_str(value).map_err(|_| StoreError::InvalidScope)?;
    if id.get_version_num() != 4 || id.get_variant() != Variant::RFC4122 || id.to_string() != value
    {
        return Err(StoreError::InvalidScope);
    }
    Ok(id)
}

/// Account records exist before a server has assigned any device generation.
#[derive(Clone, Copy)]
#[repr(u8)]
pub enum AccountKind {
    Registration = 1,
    ActiveDevice = 2,
}

pub struct AccountKey {
    kind: AccountKind,
    id: String,
}
impl AccountKey {
    pub fn new(kind: AccountKind, id: &str) -> StoreResult<Self> {
        if id.is_empty()
            || id.len() > 128
            || !id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b':' | b'.'))
        {
            return Err(StoreError::InvalidScope);
        }
        Ok(Self {
            kind,
            id: id.into(),
        })
    }
    fn aad(&self, namespace: &[u8; 32]) -> Vec<u8> {
        let mut aad = b"zchat.e2ee.store.account.v1\0".to_vec();
        aad.extend_from_slice(namespace);
        aad.push(self.kind as u8);
        aad.extend_from_slice(&(self.id.len() as u16).to_be_bytes());
        aad.extend_from_slice(self.id.as_bytes());
        aad
    }
}

const ACCOUNT_TABLE: &str = "CREATE TABLE protected_account_records (
    kind INTEGER NOT NULL CHECK(kind BETWEEN 1 AND 2),
    record_id TEXT NOT NULL CHECK(length(record_id) BETWEEN 1 AND 128),
    payload BLOB NOT NULL CHECK(length(payload) BETWEEN 29 AND 1048605),
    PRIMARY KEY(kind,record_id)) WITHOUT ROWID;";

/// Protocol values, including public plans and history, use the protected path.
/// SQL recovery projections contain only opaque IDs and pending flags.
/// Record IDs must be opaque UUIDs/hashes or fixed internal labels, never message text.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum RecordKind {
    Identity = 1,
    DeviceCredential,
    EcPreKey,
    SignedPreKey,
    PqPreKey,
    PeerIdentity,
    Session,
    Publication,
    Acquisition,
    History,
    Outbox,
    Inbox,
    Control,
}

// Empty on upgrade: CREATE INDEX never builds over retained ciphertext/history.
// Mutual deferred FKs keep each projection attached to its authoritative row.
const RECOVERY_TABLE: &str = "CREATE TABLE protected_recovery (
    record_key TEXT PRIMARY KEY,
    generation BLOB NOT NULL, kind INTEGER NOT NULL, record_id TEXT NOT NULL,
    pending INTEGER CHECK(pending IN (0,1)),
    CHECK(record_key=lower(hex(generation)) || ':' || kind || ':' || record_id),
    UNIQUE(generation,kind,record_id),
    FOREIGN KEY(generation,kind,record_id) REFERENCES protected_records(generation,kind,record_id) DEFERRABLE INITIALLY DEFERRED
    ) WITHOUT ROWID;
    CREATE INDEX protected_recovery_pending ON protected_recovery(generation,kind,record_id) WHERE pending=1;
    CREATE INDEX protected_recovery_unfinished ON protected_recovery(generation) WHERE pending IS NULL;";

fn recovery_record(key: &RecordKey) -> bool {
    matches!(
        key.kind,
        RecordKind::History | RecordKind::Outbox | RecordKind::Inbox
    ) || key.kind == RecordKind::Control
        && (key.id.starts_with("acquire:") || key.id.starts_with("delivery-in:"))
}
fn recovery_reference(key: &RecordKey) -> String {
    format!("{}:{}:{}", key.generation.simple(), key.kind as u8, key.id)
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct RecordKey {
    generation: Uuid,
    kind: RecordKind,
    id: String,
}

impl RecordKey {
    pub fn new(generation: &str, kind: RecordKind, opaque_id: &str) -> StoreResult<Self> {
        if opaque_id.is_empty()
            || opaque_id.len() > 128
            || !opaque_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b':' | b'.'))
        {
            return Err(StoreError::InvalidScope);
        }
        Ok(Self {
            generation: canonical_uuid(generation)?,
            kind,
            id: opaque_id.into(),
        })
    }

    fn aad(&self, namespace: &[u8; 32]) -> Vec<u8> {
        let mut aad = b"zchat.e2ee.store.record.v1\0".to_vec();
        aad.extend_from_slice(namespace);
        aad.extend_from_slice(self.generation.as_bytes());
        aad.push(self.kind as u8);
        aad.extend_from_slice(&(self.id.len() as u16).to_be_bytes());
        aad.extend_from_slice(self.id.as_bytes());
        aad
    }
}

// Deliberately private: production callers cannot install a plaintext key protector.
trait KeyProtector {
    fn wrap(&self, key: &[u8], namespace: &[u8; 32]) -> StoreResult<Vec<u8>>;
    fn unwrap(&self, wrapped: &[u8], namespace: &[u8; 32]) -> StoreResult<Zeroizing<Vec<u8>>>;
}

#[cfg(test)]
struct TestRecordExit {
    key: RecordKey,
    after_commit: bool,
    previous: Option<Zeroizing<Vec<u8>>>,
}

// Explicit unlock is necessary even if another descriptor inherited/duplicated
// this open file description. Close-only retirement may otherwise retain Busy.
struct OwnerLock(File, u32);
impl Drop for OwnerLock {
    fn drop(&mut self) {
        // Drop cannot report an OS error. Closing the handle remains the fallback;
        // an unsuccessful unlock keeps the lock (fail closed), never bypasses it.
        // A forked child's inherited guard must not unlock its live parent.
        if std::process::id() == self.1 {
            let _ = self.0.unlock();
        }
    }
}
/// Non-cloneable connection and lifetime OS file lock: one writer/crypto owner per
/// account, including across processes. Different accounts can be open together.
pub struct ProtectedStore {
    // Field declaration order is intentional: close SQLite BEFORE unlocking.
    connection: Connection,
    cipher: Aes256GcmSiv,
    scope: StoreScope,
    read_only: bool,
    _owner: OwnerLock,
    #[cfg(test)]
    exit_on_record: Option<TestRecordExit>,
}

impl ProtectedStore {
    pub(crate) fn scope(&self) -> &StoreScope {
        &self.scope
    }

    /// Explicit creation only. Existing, damaged or partly-created files are never reset.
    pub fn create(app_data: &Path, scope: StoreScope) -> StoreResult<Self> {
        Self::connect(app_data, scope, true, &dpapi::WindowsDpapi)
    }

    /// Opening never creates a new database/key. Logout/login/update use this path.
    pub fn open(app_data: &Path, scope: StoreScope) -> StoreResult<Self> {
        Self::connect(app_data, scope, false, &dpapi::WindowsDpapi)
    }

    fn connect(
        app_data: &Path,
        scope: StoreScope,
        create: bool,
        protector: &impl KeyProtector,
    ) -> StoreResult<Self> {
        // Refuse unsupported platforms before touching disk, including on create.
        dpapi::require_platform()?;
        Self::connect_protected(app_data, scope, create, protector)
    }

    fn connect_protected(
        app_data: &Path,
        scope: StoreScope,
        create: bool,
        protector: &impl KeyProtector,
    ) -> StoreResult<Self> {
        let path = scope.database_path(app_data);
        let directory = path.parent().ok_or(StoreError::InvalidScope)?;
        if create {
            fs::create_dir_all(directory)?;
        }
        let owner = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(directory.join("owner.lock"))?;
        match owner.try_lock() {
            Ok(()) => (),
            Err(std::fs::TryLockError::WouldBlock) => return Err(StoreError::Busy),
            Err(std::fs::TryLockError::Error(e)) => return Err(e.into()),
        }
        // Construct the unlock guard ONLY after successfully acquiring the lock.
        let owner = OwnerLock(owner, std::process::id());
        if create {
            // Atomic ownership claim, before SQLite opens without SQLITE_OPEN_CREATE.
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?
                .sync_all()?;
        }
        let mut connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_millis(250))?;
        connection.execute_batch(
            "PRAGMA trusted_schema=OFF; PRAGMA foreign_keys=ON;
             PRAGMA temp_store=MEMORY; PRAGMA synchronous=EXTRA;",
        )?;
        let cipher = if create {
            connection.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA secure_delete=ON;")?;
            let mut key = Zeroizing::new([0u8; 32]);
            random(key.as_mut())?;
            let wrapped = protector.wrap(key.as_ref(), &scope.namespace)?;
            if wrapped.is_empty() || wrapped.len() > 16384 {
                return Err(StoreError::Protection);
            }
            let cipher =
                Aes256GcmSiv::new_from_slice(key.as_ref()).map_err(|_| StoreError::Protection)?;
            let check = seal(&cipher, KEY_CHECK, &scope.namespace)?;
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch(
                "CREATE TABLE store_header (
                    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
                    version INTEGER NOT NULL CHECK(version=1),
                    namespace BLOB NOT NULL CHECK(length(namespace)=32),
                    wrapped_key BLOB NOT NULL CHECK(length(wrapped_key) BETWEEN 1 AND 16384),
                    key_check BLOB NOT NULL);
                 CREATE TABLE protected_records (
                    generation BLOB NOT NULL CHECK(length(generation)=16),
                    kind INTEGER NOT NULL CHECK(kind BETWEEN 1 AND 13),
                    record_id TEXT NOT NULL CHECK(length(record_id) BETWEEN 1 AND 128),
                    payload BLOB NOT NULL CHECK(length(payload) BETWEEN 29 AND 1048605),
                    recovery_ref TEXT REFERENCES protected_recovery(record_key) DEFERRABLE INITIALLY DEFERRED,
                    attachment_ref TEXT REFERENCES protected_attachment_blobs(record_key) DEFERRABLE INITIALLY DEFERRED,
                    PRIMARY KEY(generation,kind,record_id)) WITHOUT ROWID;
                 ",
            )?;
            tx.pragma_update(None, "application_id", APPLICATION_ID)?;
            tx.execute_batch(ACCOUNT_TABLE)?;
            tx.execute_batch(RECOVERY_TABLE)?;
            tx.execute_batch(attachments::TABLES)?;
            tx.execute_batch(downloads::TABLES)?;
            tx.pragma_update(None, "user_version", SCHEMA)?;
            tx.execute(
                "INSERT INTO store_header VALUES(1,?1,?2,?3,?4)",
                params![FORMAT, scope.namespace.as_slice(), wrapped, check],
            )?;
            tx.commit()?;
            cipher
        } else {
            let application_id: i64 =
                connection.pragma_query_value(None, "application_id", |r| r.get(0))?;
            let version: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
            if application_id != APPLICATION_ID || !(1..=SCHEMA).contains(&version) {
                return Err(StoreError::Format);
            }
            let (version, namespace, wrapped, check): (i64, Vec<u8>, Vec<u8>, Vec<u8>) = connection.query_row(
                "SELECT version,CASE WHEN length(namespace)=32 THEN namespace ELSE NULL END,
                    CASE WHEN length(wrapped_key) BETWEEN 1 AND 16384 THEN wrapped_key ELSE NULL END,
                    CASE WHEN length(key_check) BETWEEN 29 AND 256 THEN key_check ELSE NULL END
                 FROM store_header WHERE singleton=1", [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )?;
            if version != FORMAT || namespace != scope.namespace {
                return Err(StoreError::Format);
            }
            let key = protector.unwrap(&wrapped, &scope.namespace)?;
            let cipher = Aes256GcmSiv::new_from_slice(&key).map_err(|_| StoreError::Protection)?;
            if open(&cipher, &check, &scope.namespace)?.as_slice() != KEY_CHECK {
                return Err(StoreError::Protection);
            }
            let schema: i64 = connection.pragma_query_value(None, "user_version", |r| r.get(0))?;
            if schema == 1 {
                // Only authenticated protected-v1 stores migrate. No raw legacy
                // import, key generation or changes to generation ciphertext.
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute_batch(ACCOUNT_TABLE)?;
                tx.pragma_update(None, "user_version", 2)?;
                tx.commit()?;
            }
            if schema < 3 {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute_batch(RECOVERY_TABLE)?;
                tx.execute_batch("ALTER TABLE protected_records ADD COLUMN recovery_ref TEXT REFERENCES protected_recovery(record_key) DEFERRABLE INITIALLY DEFERRED;")?;
                tx.pragma_update(None, "user_version", 3)?;
                tx.commit()?;
            }
            if schema < 4 {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute_batch(attachments::TABLES)?;
                tx.execute_batch("ALTER TABLE protected_records ADD COLUMN attachment_ref TEXT REFERENCES protected_attachment_blobs(record_key) DEFERRABLE INITIALLY DEFERRED;")?;
                tx.pragma_update(None, "user_version", 4)?;
                tx.commit()?;
            }
            if schema < 5 {
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute_batch(downloads::TABLES)?;
                tx.pragma_update(None, "user_version", 5)?;
                tx.commit()?;
            }
            if schema < 6 {
                // Metadata-only, AFTER master-key verification. No new keys,
                // ciphertext rewrites, or deletion of existing download spools.
                let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.pragma_update(None, "user_version", SCHEMA)?;
                tx.commit()?;
            }
            connection
                .prepare("SELECT kind,record_id,payload FROM protected_account_records LIMIT 0")?;
            cipher
        };
        connection.execute_batch("PRAGMA journal_mode=DELETE; PRAGMA secure_delete=ON;")?;
        Ok(Self {
            connection,
            cipher,
            scope,
            read_only: false,
            _owner: owner,
            #[cfg(test)]
            exit_on_record: None,
        })
    }

    /// The closure is synchronous and must perform no network work. Signal adapters
    /// will share this transaction for ratchet/prekey/history/outbox mutation. Callers
    /// may release a result to the network only after this method returns success.
    /// No in-memory protocol cache is committed here; on error reload durable state.
    pub fn transaction<T>(
        &mut self,
        work: impl FnOnce(&mut ProtectedTransaction<'_>) -> StoreResult<T>,
    ) -> StoreResult<T> {
        self.transaction_with(work)
    }

    /// Preserve typed domain errors while retaining the same COMMIT boundary.
    pub fn transaction_with<T, E: From<StoreError>>(
        &mut self,
        work: impl FnOnce(&mut ProtectedTransaction<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        let behavior = if self.read_only {
            TransactionBehavior::Deferred
        } else {
            TransactionBehavior::Immediate
        };
        let tx = self
            .connection
            .transaction_with_behavior(behavior)
            .map_err(StoreError::from)?;
        let mut state = ProtectedTransaction {
            connection: &tx,
            cipher: &self.cipher,
            namespace: &self.scope.namespace,
            account: self.scope.account,
            failed: false,
        };
        let value = work(&mut state)?;
        if state.failed {
            return Err(StoreError::Aborted.into());
        }
        // Fault injection exists only in unit-test binaries and is used in a
        // separate process; production builds have no process-exit hook.
        #[cfg(test)]
        let exit_after_commit = match self.exit_on_record.as_ref() {
            Some(TestRecordExit {
                key,
                after_commit: after,
                previous,
            }) if state
                .get(key)?
                .is_some_and(|value| Some(&value) != previous.as_ref()) =>
            {
                if !after {
                    // Force dirty-page spill, rather than testing only RAM rollback.
                    for i in 0..100 {
                        let spill = RecordKey::new(
                            &key.generation.to_string(),
                            RecordKind::Control,
                            &format!("test-fault-spill-{i}"),
                        )?;
                        state.put(&spill, &vec![17; 48 * 1024])?;
                    }
                    std::process::exit(73);
                }
                true
            }
            _ => false,
        };
        tx.commit().map_err(StoreError::from)?;
        #[cfg(test)]
        if exit_after_commit {
            std::process::exit(74);
        }
        Ok(value)
    }

    /// One-way downgrade for an archive owner. SQLite rejects writes even if a
    /// future presentation helper accidentally tries to seed or repair data.
    pub(crate) fn into_read_only(mut self) -> StoreResult<Self> {
        self.connection.pragma_update(None, "query_only", true)?;
        self.read_only = true;
        Ok(self)
    }

    pub fn account_id(&self) -> Uuid {
        self.scope.account
    }

    #[cfg(test)]
    pub fn test_exit_on_record(&mut self, key: RecordKey, after_commit: bool) {
        self.exit_on_record = Some(TestRecordExit {
            key,
            after_commit,
            previous: None,
        });
    }

    /// Existing records require a change trigger, otherwise startup/read-only
    /// transactions would fire the crash hook before the tested mutation.
    #[cfg(test)]
    pub fn test_exit_on_record_change(
        &mut self,
        key: RecordKey,
        after_commit: bool,
    ) -> StoreResult<()> {
        let previous = self.transaction(|tx| tx.get(&key))?;
        self.exit_on_record = Some(TestRecordExit {
            key,
            after_commit,
            previous,
        });
        Ok(())
    }
}

/// No raw SQL or independent connection is exposed to the future Signal adapters.
pub struct ProtectedTransaction<'a> {
    connection: &'a Connection,
    cipher: &'a Aes256GcmSiv,
    namespace: &'a [u8; 32],
    account: Uuid,
    failed: bool,
}

impl ProtectedTransaction<'_> {
    pub fn get_account(&mut self, key: &AccountKey) -> StoreResult<Option<Zeroizing<Vec<u8>>>> {
        let result = (|| {
            let row:Option<Option<Vec<u8>>>=self.connection.query_row(
                "SELECT CASE WHEN length(payload) BETWEEN 29 AND 1048605 THEN payload ELSE NULL END
                 FROM protected_account_records WHERE kind=?1 AND record_id=?2",
                params![key.kind as u8,key.id],|r|r.get(0)).optional()?;
            match row {
                None => Ok(None),
                Some(None) => Err(StoreError::Protection),
                Some(Some(bytes)) => open(self.cipher, &bytes, &key.aad(self.namespace)).map(Some),
            }
        })();
        self.failed |= result.is_err();
        result
    }
    pub fn put_account(&mut self, key: &AccountKey, value: &[u8]) -> StoreResult<()> {
        let result = (|| {
            if value.len() > MAX_RECORD_BYTES {
                return Err(StoreError::RecordTooLarge);
            }
            let bytes = seal(self.cipher, value, &key.aad(self.namespace))?;
            self.connection.execute(
                "INSERT INTO protected_account_records VALUES(?1,?2,?3)
                ON CONFLICT(kind,record_id) DO UPDATE SET payload=excluded.payload",
                params![key.kind as u8, key.id, bytes],
            )?;
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    pub fn has_generation_records(&mut self) -> StoreResult<bool> {
        let result = self
            .connection
            .query_row("SELECT EXISTS(SELECT 1 FROM protected_records)", [], |r| {
                r.get(0)
            })
            .map_err(StoreError::from);
        self.failed |= result.is_err();
        result
    }
    pub(crate) fn has_generation(&mut self, generation: &str) -> StoreResult<bool> {
        let result = (|| {
            let id = canonical_uuid(generation)?;
            self.connection
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM protected_records WHERE generation=?1)",
                    [id.as_bytes().as_slice()],
                    |row| row.get(0),
                )
                .map_err(StoreError::from)
        })();
        self.failed |= result.is_err();
        result
    }
    pub fn account_id(&self) -> Uuid {
        self.account
    }

    /// Indexed bounded existence check. Never decrypt retained history merely
    /// to decide whether a missing domain root can be initialized.
    pub(crate) fn has_record_prefix(
        &mut self,
        generation: &str,
        kind: RecordKind,
        prefix: &str,
    ) -> StoreResult<bool> {
        let result = (|| {
            let key = RecordKey::new(generation, kind, prefix)?;
            self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id>=?3 AND record_id<?4)",
                params![key.generation.as_bytes().as_slice(), kind as u8, prefix, format!("{prefix}~")],
                |row|row.get(0),
            ).map_err(StoreError::from)
        })();
        self.failed |= result.is_err();
        result
    }

    /// A domain callback must poison its transaction if it catches an error.
    pub fn abort(&mut self) {
        self.failed = true;
    }
    pub fn get(&mut self, key: &RecordKey) -> StoreResult<Option<Zeroizing<Vec<u8>>>> {
        let result = (|| {
            let row: Option<Option<Vec<u8>>> = self.connection.query_row(
                "SELECT CASE WHEN length(payload) BETWEEN 29 AND 1048605 THEN payload ELSE NULL END
                 FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id=?3",
                params![key.generation.as_bytes().as_slice(), key.kind as u8, key.id],
                |r| r.get(0),
            ).optional()?;
            match row {
                None => Ok(None),
                Some(None) => Err(StoreError::Protection),
                Some(Some(bytes)) => open(self.cipher, &bytes, &key.aad(self.namespace)).map(Some),
            }
        })();
        self.failed |= result.is_err();
        result
    }

    /// Remove an auxiliary control row only. History, crypto records and SQL
    /// projected/cohort-linked rows cannot be deleted through this operation.
    pub(crate) fn remove_unindexed_control(&mut self, key: &RecordKey) -> StoreResult<()> {
        let result = (|| {
            if key.kind != RecordKind::Control || recovery_record(key) {
                return Err(StoreError::InvalidScope);
            }
            let removed = self.connection.execute(
                "DELETE FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id=?3 AND recovery_ref IS NULL AND attachment_ref IS NULL",
                params![key.generation.as_bytes().as_slice(), key.kind as u8, key.id],
            )?;
            if removed == 0 && self.get(key)?.is_some() {
                return Err(StoreError::Format);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }

    pub(crate) fn project_recovery(&mut self, key: &RecordKey, pending: bool) -> StoreResult<()> {
        let result = (|| {
            if !recovery_record(key) {
                return Err(StoreError::InvalidScope);
            }
            let reference = recovery_reference(key);
            self.connection.execute(
                "INSERT INTO protected_recovery VALUES(?1,?2,?3,?4,?5)
                ON CONFLICT(record_key) DO UPDATE SET pending=excluded.pending",
                params![
                    reference,
                    key.generation.as_bytes().as_slice(),
                    key.kind as u8,
                    key.id,
                    pending
                ],
            )?;
            if self.connection.execute("UPDATE protected_records SET recovery_ref=?4 WHERE generation=?1 AND kind=?2 AND record_id=?3",
                params![key.generation.as_bytes().as_slice(),key.kind as u8,key.id,reference])? != 1 { return Err(StoreError::Format); }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    pub(crate) fn check_recovery_projection(
        &mut self,
        key: &RecordKey,
        pending: bool,
    ) -> StoreResult<()> {
        let result = (|| {
            let valid: bool=self.connection.query_row("SELECT EXISTS(SELECT 1 FROM protected_records r JOIN protected_recovery p ON r.recovery_ref=p.record_key
                WHERE r.generation=?1 AND r.kind=?2 AND r.record_id=?3 AND p.record_key=?4 AND p.pending=?5)",
                params![key.generation.as_bytes().as_slice(),key.kind as u8,key.id,recovery_reference(key),pending],|r|r.get(0))?;
            if !valid {
                return Err(StoreError::Format);
            }
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
    pub(crate) fn unfinished_recovery(&mut self, generation: &str) -> StoreResult<bool> {
        let id = canonical_uuid(generation)?;
        Ok(self.connection.query_row("SELECT EXISTS(SELECT 1 FROM protected_recovery WHERE generation=?1 AND pending IS NULL)",
            [id.as_bytes().as_slice()], |r| r.get(0))?)
    }
    pub(crate) fn has_recovery_projection(&mut self, generation: &str) -> StoreResult<bool> {
        let id = canonical_uuid(generation)?;
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM protected_recovery WHERE generation=?1)",
            [id.as_bytes().as_slice()],
            |r| r.get(0),
        )?)
    }
    /// Fixed indexed existence probes, no decryption or retained-history scan.
    pub(crate) fn has_recovery_records(&mut self, generation: &str) -> StoreResult<bool> {
        for kind in [
            RecordKind::History,
            RecordKind::Outbox,
            RecordKind::Inbox,
            RecordKind::Control,
        ] {
            let prefixes: &[&str] = if kind == RecordKind::Control {
                &["acquire:", "delivery-in:"]
            } else {
                &[""]
            };
            let id = canonical_uuid(generation)?;
            for prefix in prefixes {
                let found: bool = self.connection.query_row(
                    "SELECT EXISTS(SELECT 1 FROM protected_records
                    WHERE generation=?1 AND kind=?2 AND record_id>=?3 AND record_id<?4)",
                    params![
                        id.as_bytes().as_slice(),
                        kind as u8,
                        prefix,
                        format!("{prefix}~")
                    ],
                    |r| r.get(0),
                )?;
                if found {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
    pub(crate) fn recovery_page(
        &mut self,
        generation: &str,
        kind: RecordKind,
        prefix: &str,
        after: Option<&str>,
    ) -> StoreResult<Vec<String>> {
        self.recovery_page_before(generation, kind, prefix, &format!("{prefix}~"), after)
    }
    /// Prototype UUID queues cannot consume v2's explicitly prefixed record IDs.
    /// UUIDs use only lowercase hex; 'g' is an exclusive indexed upper bound.
    pub(crate) fn prototype_recovery_page(
        &mut self,
        generation: &str,
        kind: RecordKind,
        prefix: &str,
        after: Option<&str>,
    ) -> StoreResult<Vec<String>> {
        self.recovery_page_before(generation, kind, prefix, &format!("{prefix}g"), after)
    }
    fn recovery_page_before(
        &mut self,
        generation: &str,
        kind: RecordKind,
        prefix: &str,
        upper: &str,
        after: Option<&str>,
    ) -> StoreResult<Vec<String>> {
        let generation_id = canonical_uuid(generation)?;
        if let Some(after) = after {
            RecordKey::new(generation, kind, after)?;
            if !after.starts_with(prefix) {
                return Err(StoreError::InvalidScope);
            }
        }
        let mut statement = self.connection.prepare(
            "SELECT record_id FROM protected_recovery WHERE generation=?1 AND kind=?2 AND pending=1
            AND record_id>=?3 AND record_id<?4 AND record_id>?5 ORDER BY record_id LIMIT 32",
        )?;
        let rows = statement.query_map(
            params![
                generation_id.as_bytes().as_slice(),
                kind as u8,
                prefix,
                upper,
                after.unwrap_or("")
            ],
            |r| r.get(0),
        )?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    /// Bounded internal recovery scan using the existing primary key. Values
    /// stay authenticated to their exact key; no raw connection leaves the owner.
    pub(crate) fn record_page(
        &mut self,
        generation: &str,
        kind: RecordKind,
        prefix: &str,
        after: Option<&str>,
    ) -> StoreResult<Vec<(String, Zeroizing<Vec<u8>>)>> {
        self.record_page_limited(
            generation,
            kind,
            prefix,
            after,
            false,
            (32, MAX_RECORD_BYTES),
        )
    }

    /// Indexed keyset paging with bounds checked before allocating/decrypting a
    /// value. UI reads use much smaller limits than the generic recovery scan.
    pub(crate) fn record_page_limited(
        &mut self,
        generation: &str,
        kind: RecordKind,
        prefix: &str,
        boundary: Option<&str>,
        descending: bool,
        limits: (usize, usize),
    ) -> StoreResult<Vec<(String, Zeroizing<Vec<u8>>)>> {
        let result = (|| {
            if limits.0 == 0 || limits.0 > 32 || limits.1 > MAX_RECORD_BYTES {
                return Err(StoreError::InvalidScope);
            }
            let generation_id = canonical_uuid(generation)?;
            if !prefix.is_empty() {
                RecordKey::new(generation, kind, prefix)?;
            }
            if let Some(after) = boundary {
                RecordKey::new(generation, kind, after)?;
                if !after.starts_with(prefix) {
                    return Err(StoreError::InvalidScope);
                }
            }
            let query = if descending {
                "SELECT record_id,CASE WHEN length(payload) BETWEEN 29 AND ?6 THEN payload ELSE NULL END
                 FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id>=?3 AND record_id<?4
                 AND record_id<?5 ORDER BY record_id DESC LIMIT ?7"
            } else {
                "SELECT record_id,CASE WHEN length(payload) BETWEEN 29 AND ?6 THEN payload ELSE NULL END
                 FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id>=?3 AND record_id<?4
                 AND record_id>?5 ORDER BY record_id LIMIT ?7"
            };
            let upper = format!("{prefix}~");
            let mut statement = self.connection.prepare(query)?;
            let mut rows = statement.query(params![
                generation_id.as_bytes().as_slice(),
                kind as u8,
                prefix,
                upper,
                boundary.unwrap_or(if descending { &upper } else { "" }),
                limits.1 as i64 + 29,
                limits.0 as i64,
            ])?;
            let mut result = Vec::new();
            while let Some(row) = rows.next()? {
                let id: String = row.get(0)?;
                let bytes: Option<Vec<u8>> = row.get(1)?;
                let key = RecordKey::new(generation, kind, &id)?;
                let plaintext = open(
                    self.cipher,
                    &bytes.ok_or(StoreError::Protection)?,
                    &key.aad(self.namespace),
                )?;
                result.push((id, plaintext));
            }
            Ok(result)
        })();
        self.failed |= result.is_err();
        result
    }

    /// One indexed seek per namespace, skipping all its messages/keys. Do not
    /// replace this with DISTINCT generation: that may scan retained records.
    /// Only IDs leave this helper; callers authenticate each archive marker.
    pub(crate) fn generation_page(
        &mut self,
        after: Option<&str>,
        limit: usize,
    ) -> StoreResult<Vec<String>> {
        let result = (|| {
            if limit == 0 || limit > 9 {
                return Err(StoreError::InvalidScope);
            }
            let mut boundary = match after {
                Some(id) => canonical_uuid(id)?.as_bytes().to_vec(),
                None => Vec::new(),
            };
            let mut query = self.connection.prepare(
                "SELECT generation FROM protected_records WHERE generation>?1 ORDER BY generation LIMIT 1",
            )?;
            let mut result = Vec::new();
            for _ in 0..limit {
                let next: Option<Vec<u8>> =
                    query.query_row([&boundary], |r| r.get(0)).optional()?;
                let Some(next) = next else { break };
                let id = Uuid::from_slice(&next)
                    .map_err(|_| StoreError::Format)?
                    .to_string();
                canonical_uuid(&id)?;
                result.push(id);
                boundary = next;
            }
            Ok(result)
        })();
        self.failed |= result.is_err();
        result
    }

    pub fn put(&mut self, key: &RecordKey, value: &[u8]) -> StoreResult<()> {
        let result = (|| {
            if value.len() > MAX_RECORD_BYTES {
                return Err(StoreError::RecordTooLarge);
            }
            let bytes = seal(self.cipher, value, &key.aad(self.namespace))?;
            let reference = if recovery_record(key) {
                Some(recovery_reference(key))
            } else {
                None
            };
            if let Some(reference) = &reference {
                // Any authoritative rewrite invalidates the projection until its
                // domain operation classifies it in THIS transaction. Never
                // silently inherit a terminal/pending flag from stale metadata.
                self.connection.execute(
                    "INSERT INTO protected_recovery VALUES(?1,?2,?3,?4,NULL)
                    ON CONFLICT(record_key) DO UPDATE SET pending=NULL",
                    params![
                        reference,
                        key.generation.as_bytes().as_slice(),
                        key.kind as u8,
                        key.id
                    ],
                )?;
            }
            self.connection.execute(
                "INSERT INTO protected_records(generation,kind,record_id,payload,recovery_ref) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(generation,kind,record_id) DO UPDATE SET payload=excluded.payload,recovery_ref=excluded.recovery_ref",
                params![
                    key.generation.as_bytes().as_slice(),
                    key.kind as u8,
                    key.id,
                    bytes,
                    reference
                ],
            )?;
            Ok(())
        })();
        self.failed |= result.is_err();
        result
    }
}

fn random(bytes: &mut [u8]) -> StoreResult<()> {
    OsRng
        .try_fill_bytes(bytes)
        .map_err(|_| StoreError::Protection)
}

fn seal(cipher: &Aes256GcmSiv, plaintext: &[u8], aad: &[u8]) -> StoreResult<Vec<u8>> {
    let mut nonce = [0u8; 12];
    random(&mut nonce)?;
    let ciphertext = cipher
        .encrypt(
            &Nonce::from(nonce),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| StoreError::Protection)?;
    let mut frame = Vec::with_capacity(OVERHEAD + plaintext.len());
    frame.push(FRAME);
    frame.extend_from_slice(&nonce);
    frame.extend_from_slice(&ciphertext);
    Ok(frame)
}

fn open(cipher: &Aes256GcmSiv, frame: &[u8], aad: &[u8]) -> StoreResult<Zeroizing<Vec<u8>>> {
    if frame.len() < OVERHEAD || frame[0] != FRAME {
        return Err(StoreError::Protection);
    }
    let nonce: [u8; 12] = frame[1..13]
        .try_into()
        .map_err(|_| StoreError::Protection)?;
    cipher
        .decrypt(
            &Nonce::from(nonce),
            Payload {
                msg: &frame[13..],
                aad,
            },
        )
        .map(Zeroizing::new)
        .map_err(|_| StoreError::Protection)
}

#[path = "protected_dpapi.rs"]
mod dpapi;

#[path = "protected_attachments.rs"]
mod attachments;
#[path = "protected_downloads.rs"]
mod downloads;

#[cfg(test)]
#[path = "protected_tests.rs"]
mod tests;

#[cfg(test)]
pub(crate) fn test_open(root: &Path, scope: StoreScope, create: bool) -> ProtectedStore {
    ProtectedStore::connect_protected(root, scope, create, &tests::TestProtector(42)).unwrap()
}
