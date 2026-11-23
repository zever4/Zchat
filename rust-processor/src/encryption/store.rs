//! Legacy plaintext prototype. Not used by v1 or Tauri; never auto-import it.
use super::{EncryptionError, Result};
use libsignal_protocol::{
    DeviceId, GenericSignedPreKey, IdentityKeyPair, KeyPair, KyberPreKeyId, KyberPreKeyRecord,
    KyberPreKeyStore, PreKeyId, PreKeyRecord, PreKeyStore, ProtocolAddress, SignalProtocolError,
    SignedPreKeyId, SignedPreKeyRecord, SignedPreKeyStore, Timestamp,
    error::Result as SignalResult, kem,
};
use rusqlite::{Connection, OptionalExtension, ToSql, params};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

type DbConnection = Arc<Mutex<Connection>>;

/// Responsible for user_info and indentity tables
pub struct SqliteIdentityStore {
    conn: DbConnection,
    identity: Option<IdentityKeyPair>,
    registration_id: u32,
}

impl SqliteIdentityStore {
    fn load_existing(&mut self) -> Result<bool> {
        let existing: Option<(Vec<u8>, u32)> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT identity_bytes,registration_id FROM identity WHERE id=1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((bytes, registration)) = existing {
            self.identity = Some(deserialize_identity(&bytes)?);
            self.registration_id = registration;
            return Ok(true);
        }
        Ok(false)
    }
    pub fn new(
        conn: DbConnection,
        identity: Option<IdentityKeyPair>,
        registration_id: u32,
    ) -> Self {
        Self {
            conn,
            identity,
            registration_id,
        }
    }

    pub fn set_user_info(&self, user_id: &str, device_id: u32, token: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO user_info (id, user_id, device_id, auth_token) 
             VALUES (1, ?, ?, ?)",
            [user_id, &device_id.to_string(), token],
        )?;
        Ok(())
    }

    pub fn get_user_info(&self) -> Result<Option<(String, u32, Option<String>)>> {
        let conn = self.conn.lock().unwrap();
        let row: Option<(String, u32, Option<String>)> = conn
            .query_row(
                "SELECT user_id, device_id, auth_token FROM user_info WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;

        Ok(row)
    }

    /// The local address must match the identity registered by this client.
    pub fn local_address(&self) -> Result<ProtocolAddress> {
        let (user_id, device_id, _) = self
            .get_user_info()?
            .ok_or(EncryptionError::MissingUserInfo)?;
        if user_id.trim().is_empty() {
            return Err(EncryptionError::MissingUserInfo);
        }
        let device_id =
            u8::try_from(device_id).map_err(|e| EncryptionError::InvalidDeviceId(e.to_string()))?;
        let device_id = DeviceId::new(device_id)
            .map_err(|e| EncryptionError::InvalidDeviceId(e.to_string()))?;
        Ok(ProtocolAddress::new(user_id, device_id))
    }

    /// initialize_identity returns `IdentityKeyPair` and `registration_id`.
    /// If no identity is stored, creates a new identity first
    pub fn initialize_identity(&mut self) -> Result<(IdentityKeyPair, u32)> {
        let conn = self.conn.lock().unwrap();

        let existing: Option<(Vec<u8>, u32)> = conn
            .query_row(
                "SELECT identity_bytes, registration_id FROM identity WHERE id = 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        if let Some((identity_bytes, reg_id)) = existing {
            let identity = deserialize_identity(&identity_bytes)?;
            self.identity = Some(identity);
            self.registration_id = reg_id;
            return Ok((identity, reg_id));
        }

        let mut rng = rand::rng();
        let identity = IdentityKeyPair::generate(&mut rng);
        let reg_id = rand::random::<u16>() as u32; // 0-65535
        let identity_bytes = serialize_identity(&identity);

        conn.execute(
            "INSERT INTO identity (id, identity_bytes, registration_id) VALUES (1, ?1, ?2)",
            [
                &identity_bytes as &dyn ToSql,
                &(reg_id as i64) as &dyn ToSql,
            ],
        )?;

        self.identity = Some(identity);
        self.registration_id = reg_id;
        Ok((identity, reg_id))
    }
}

pub struct SqlitePreKeyStore {
    conn: DbConnection,
}

impl SqlitePreKeyStore {
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }

    // One-time EC PreKeys
    pub async fn get_unused_pre_keys(
        &self,
        limit: usize,
    ) -> SignalResult<Vec<(PreKeyId, KeyPair)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, public_key, private_key FROM prekeys WHERE used = 0 LIMIT ?")
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

        let rows = stmt
            .query_map([limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u32,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

        let mut result = Vec::new();
        for row in rows {
            let (id, pub_bytes, priv_bytes) =
                row.map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            let key_pair = KeyPair::from_public_and_private(&pub_bytes, &priv_bytes)
                .map_err(|_| SignalProtocolError::InvalidKeyAgreement)?;

            result.push((PreKeyId::from(id), key_pair));
        }

        Ok(result)
    }

    pub async fn get_next_prekey_id(&self) -> SignalResult<u32> {
        let conn = self.conn.lock().unwrap();
        let max_id: i64 = conn
            .query_row("SELECT COALESCE(MAX(id), 0) FROM prekeys", [], |row| {
                row.get(0)
            })
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;
        Ok((max_id + 1) as u32)
    }
}

pub struct SqliteSignedPreKeyStore {
    conn: DbConnection,
}

impl SqliteSignedPreKeyStore {
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }

    /// Get last SignedPreKey by timestamp
    /// TODO: Key rotation
    pub async fn get_active_signed_pre_key(&self) -> SignalResult<SignedPreKeyRecord> {
        let conn = self.conn.lock().unwrap();

        let row: (u32, Vec<u8>, Vec<u8>, Vec<u8>, i64) = conn
            .query_row(
                "SELECT id, public_key, private_key, signature, timestamp
                FROM signed_prekeys
                ORDER BY timestamp DESC LIMIT 1",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .map_err(|_| SignalProtocolError::InvalidSignedPreKeyId)?;

        let (id, pub_bytes, priv_bytes, signature_bytes, ts) = row;

        let key_pair = KeyPair::from_public_and_private(&pub_bytes, &priv_bytes)
            .map_err(|_| SignalProtocolError::InvalidKeyAgreement)?;

        Ok(SignedPreKeyRecord::new(
            SignedPreKeyId::from(id),
            Timestamp::from_epoch_millis(ts as u64),
            &key_pair,
            &signature_bytes,
        ))
    }

    pub async fn get_next_signed_prekey_id(&self) -> SignalResult<u32> {
        let conn = self.conn.lock().unwrap();
        let max_id: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(id), 0) FROM signed_prekeys",
                [],
                |row| row.get(0),
            )
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;
        Ok((max_id + 1) as u32)
    }
}

pub struct SqliteKyberPreKeyStore {
    conn: DbConnection,
}

impl SqliteKyberPreKeyStore {
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }

    /// Saves last_resort kyber pre key with no distinction in ID between regular keys.
    /// DO NOT USE THIS FUNCTION when saving regular kyber prekeys.
    pub async fn save_last_resort_kyber_pre_key(
        &mut self,
        kyber_prekey_id: KyberPreKeyId,
        record: &KyberPreKeyRecord,
    ) -> SignalResult<()> {
        let conn = self.conn.lock().unwrap();
        let id: u32 = kyber_prekey_id.into();
        let record_bytes = record
            .serialize()
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("serialize: {}", e)))?;

        let is_last_resort: i64 = 1; // true

        conn.execute(
            "INSERT INTO kyber_pre_keys (id, record, is_last_resort) VALUES (?, ?, ?)",
            params![id as i64, record_bytes, is_last_resort],
        )
        .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

        Ok(())
    }

    pub async fn get_next_kyber_prekey_id(&self) -> SignalResult<u32> {
        let conn = self.conn.lock().unwrap();
        let max_id: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(id), 0) FROM kyber_pre_keys",
                [],
                |row| row.get(0),
            )
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;
        Ok((max_id + 1) as u32)
    }

    /// Returns `limit` unused one-time kyber pre keys.
    /// To get last resort key, use `get_last_resort_kyber_pre_key`
    pub async fn get_unused_kyber_pre_keys(
        &self,
        limit: usize,
    ) -> SignalResult<Vec<(KyberPreKeyId, KyberPreKeyRecord, bool)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, record, is_last_resort FROM kyber_pre_keys WHERE is_last_resort = 0 LIMIT ?")
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

        let rows = stmt
            .query_map([limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)? as u32,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, i64>(2)? != 0, // is_last_resort as bool
                ))
            })
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

        let mut result = Vec::new();
        for row in rows {
            let (id, record_bytes, is_last_resort) =
                row.map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            let record = KyberPreKeyRecord::deserialize(&record_bytes)
                .map_err(|_| SignalProtocolError::InvalidKyberPreKeyId)?;

            result.push((KyberPreKeyId::from(id), record, is_last_resort));
        }

        Ok(result)
    }

    /// Returns a single last resort kyber pre key
    pub async fn get_last_resort_kyber_pre_key(
        &self,
    ) -> SignalResult<Option<(KyberPreKeyId, KyberPreKeyRecord)>> {
        let conn = self.conn.lock().unwrap();

        let row: Option<(i64, Vec<u8>)> = conn
            .query_row(
                "SELECT id, record FROM kyber_pre_keys WHERE is_last_resort = 1 LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

        match row {
            Some((id, record_bytes)) => {
                let record = KyberPreKeyRecord::deserialize(&record_bytes)
                    .map_err(|_| SignalProtocolError::InvalidKyberPreKeyId)?;
                Ok(Some((KyberPreKeyId::from(id as u32), record)))
            }
            None => Ok(None),
        }
    }
}

pub struct SqliteSessionStore {
    conn: DbConnection,
}

impl SqliteSessionStore {
    pub fn new(conn: DbConnection) -> Self {
        Self { conn }
    }
}

pub struct Stores {
    pub identity: SqliteIdentityStore,
    pub prekey: SqlitePreKeyStore,
    pub signed_prekey: SqliteSignedPreKeyStore,
    pub kyber_prekey: SqliteKyberPreKeyStore,
    pub session: SqliteSessionStore,
    // pub sender: SqliteSenderKeyStore,
}

// TODO: impl SenderKeyStore (group chats)
//
// Implement method for creating last resort kyber prekeys.
//
// pub struct SqliteSenderKeyStore {
//     conn: DbConnection,
//     identity: Option<IdentityKeyPair>,
//     registration_id: u32,
// }

impl Stores {
    pub fn new(db_path: &str) -> Result<Self> {
        let connection: DbConnection = Arc::new(Mutex::new(Connection::open(db_path)?));

        let conn = connection.lock().unwrap();

        // Миграции
        conn.execute(
            "CREATE TABLE IF NOT EXISTS user_info (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                user_id TEXT NOT NULL,
                device_id INTEGER NOT NULL DEFAULT 1,
                auth_token TEXT,
                server_address TEXT
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS identity (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                identity_bytes BLOB NOT NULL,
                registration_id INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS sessions (
                address TEXT PRIMARY KEY,
                session_record BLOB NOT NULL,
                created_at INTEGER DEFAULT (strftime('%s', 'now')),
                last_used INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS prekeys (
                id INTEGER PRIMARY KEY,
                public_key BLOB,
                private_key BLOB NOT NULL,
                used BOOLEAN DEFAULT 0
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS signed_prekeys (
                id INTEGER PRIMARY KEY,
                public_key BLOB NOT NULL,
                private_key BLOB NOT NULL,
                signature BLOB NOT NULL,
                timestamp INTEGER NOT NULL
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS kyber_pre_keys (
                id INTEGER PRIMARY KEY,
                record BLOB NOT NULL,
                is_last_resort INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS kyber_pre_key_usage (
                kyber_prekey_id INTEGER NOT NULL,
                ec_prekey_id INTEGER NOT NULL,
                base_key BLOB NOT NULL,
                used_at INTEGER DEFAULT (strftime('%s', 'now')),
                PRIMARY KEY (kyber_prekey_id, ec_prekey_id, base_key)
                )",
            [],
        )?;

        conn.execute(
            "CREATE TABLE IF NOT EXISTS trusted_identities (
                address TEXT PRIMARY KEY,
                identity_key BLOB NOT NULL,
                first_seen INTEGER DEFAULT (strftime('%s', 'now')),
                replaced BOOLEAN DEFAULT 0
            )",
            [],
        )?;

        drop(conn);

        Ok(Self {
            identity: SqliteIdentityStore::new(connection.clone(), None, 0),
            prekey: SqlitePreKeyStore::new(connection.clone()),
            signed_prekey: SqliteSignedPreKeyStore::new(connection.clone()),
            kyber_prekey: SqliteKyberPreKeyStore::new(connection.clone()),
            session: SqliteSessionStore::new(connection),
        })
    }

    /// Generate initial keys if user has no identity in store.
    pub async fn ensure_keys_generated(&mut self) -> Result<()> {
        if self.identity.load_existing()? {
            let complete: bool = self.identity.conn.lock().unwrap().query_row(
                "SELECT EXISTS(SELECT 1 FROM signed_prekeys) AND EXISTS(SELECT 1 FROM kyber_pre_keys WHERE is_last_resort=1)",
                [], |r| r.get(0))?;
            if !complete {
                return Err(SignalProtocolError::InvalidState(
                    "legacy key initialization",
                    "incomplete existing state; refusing implicit regeneration".into(),
                )
                .into());
            }
            // Pool exhaustion is not a new installation or a key-rotation request.
            return Ok(());
        }
        let orphaned: bool = self.identity.conn.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM prekeys) OR EXISTS(SELECT 1 FROM signed_prekeys) OR EXISTS(SELECT 1 FROM kyber_pre_keys)",
            [], |r| r.get(0))?;
        if orphaned {
            return Err(SignalProtocolError::InvalidState(
                "legacy key initialization",
                "keys without identity; refusing regeneration".into(),
            )
            .into());
        }
        self.generate_initial_keys().await
    }

    pub async fn generate_initial_keys(&mut self) -> Result<()> {
        let mut csprng = rand::rng();
        let (identity, _) = self.identity.initialize_identity()?;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;

        // EC one-time prekeys
        let ec_next_id = self.prekey.get_next_prekey_id().await?;
        for i in 0..100 {
            let key_pair = KeyPair::generate(&mut csprng);
            let id = PreKeyId::from(ec_next_id + i as u32);
            let record = PreKeyRecord::new(id, &key_pair);
            self.prekey.save_pre_key(id, &record).await?;
        }

        // Signed prekey
        let signed_next_id = self.signed_prekey.get_next_signed_prekey_id().await?;
        let signed_pair = KeyPair::generate(&mut csprng);
        let signed_id = SignedPreKeyId::from(signed_next_id);
        let signature = identity
            .private_key()
            .calculate_signature(&signed_pair.public_key.serialize(), &mut csprng)
            .map_err(|e| EncryptionError::SignalCurve(e.to_string()))?;
        let signed_record = SignedPreKeyRecord::new(
            signed_id,
            Timestamp::from_epoch_millis(timestamp),
            &signed_pair,
            &signature,
        );
        self.signed_prekey
            .save_signed_pre_key(signed_id, &signed_record)
            .await?;

        // Kyber one-time
        let kyber_next_id = self.kyber_prekey.get_next_kyber_prekey_id().await?;
        for i in 0..100 {
            let id = KyberPreKeyId::from(kyber_next_id + i as u32);
            let kem_pair = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut csprng);
            let signature = identity
                .private_key()
                .calculate_signature(&kem_pair.public_key.serialize(), &mut csprng)
                .map_err(|e| EncryptionError::SignalCurve(e.to_string()))?;
            let record = KyberPreKeyRecord::new(
                id,
                Timestamp::from_epoch_millis(timestamp),
                &kem_pair,
                &signature,
            );
            self.kyber_prekey.save_kyber_pre_key(id, &record).await?;
        }

        // Kyber last-resort
        let last_resort_id =
            KyberPreKeyId::from(self.kyber_prekey.get_next_kyber_prekey_id().await?);
        let kem_pair = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut csprng);
        let signature = identity
            .private_key()
            .calculate_signature(&kem_pair.public_key.serialize(), &mut csprng)
            .map_err(|e| EncryptionError::SignalCurve(e.to_string()))?;
        let last_resort_record = KyberPreKeyRecord::new(
            last_resort_id,
            Timestamp::from_epoch_millis(timestamp),
            &kem_pair,
            &signature,
        );
        self.kyber_prekey
            .save_last_resort_kyber_pre_key(last_resort_id, &last_resort_record)
            .await?;

        Ok(())
    }
}

fn serialize_identity(identity: &IdentityKeyPair) -> Vec<u8> {
    identity.serialize().to_vec()
}

fn deserialize_identity(bytes: &[u8]) -> Result<IdentityKeyPair> {
    IdentityKeyPair::try_from(bytes).map_err(|_| {
        EncryptionError::Signal(SignalProtocolError::InvalidArgument(
            "invalid identity key".into(),
        ))
    })
}

mod traits {
    use super::*;
    use async_trait::async_trait;
    use libsignal_protocol::{
        Direction, GenericSignedPreKey, IdentityChange, IdentityKey, IdentityKeyPair,
        IdentityKeyStore, KeyPair, KyberPreKeyId, KyberPreKeyRecord, KyberPreKeyStore, PreKeyId,
        PreKeyRecord, PreKeyStore, ProtocolAddress, PublicKey, SessionRecord, SessionStore,
        SignalProtocolError, SignedPreKeyId, SignedPreKeyRecord, SignedPreKeyStore, Timestamp,
        error::Result as SignalResult,
    };
    use rusqlite::{ToSql, params};

    // impl ProtocolStore for SqliteProtocolStore {}

    #[async_trait(?Send)]
    impl IdentityKeyStore for SqliteIdentityStore {
        async fn get_identity_key_pair(&self) -> SignalResult<IdentityKeyPair> {
            self.identity.ok_or_else(|| {
                SignalProtocolError::InvalidState("no identity", "add identity".to_string())
            })
        }

        async fn get_local_registration_id(&self) -> SignalResult<u32> {
            Ok(self.registration_id)
        }

        async fn save_identity(
            &mut self,
            address: &ProtocolAddress,
            identity: &IdentityKey,
        ) -> SignalResult<IdentityChange> {
            let conn = self.conn.lock().unwrap();
            let addr = format!("{}:{}", address.name(), address.device_id());

            let existing: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT identity_key FROM trusted_identities WHERE address = ?",
                    [&addr as &dyn ToSql],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db error: {}", e)))?;

            let new_key_bytes = identity.serialize().to_vec();

            if let Some(old_bytes) = existing {
                if old_bytes == new_key_bytes {
                    return Ok(IdentityChange::NewOrUnchanged);
                }

                return Err(SignalProtocolError::UntrustedIdentity(address.clone()));
            }

            conn.execute(
                "INSERT INTO trusted_identities (address, identity_key, first_seen) VALUES (?, ?, strftime('%s', 'now'))",
                [&addr as &dyn ToSql, &new_key_bytes as &dyn ToSql],
            ).map_err(|e| SignalProtocolError::InvalidArgument(format!("db error: {}", e)))?;

            Ok(IdentityChange::NewOrUnchanged)
        }

        async fn is_trusted_identity(
            &self,
            address: &ProtocolAddress,
            identity: &IdentityKey,
            _direction: Direction,
        ) -> SignalResult<bool> {
            let conn = self.conn.lock().unwrap();
            let addr = format!("{}:{}", address.name(), address.device_id());

            let existing: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT identity_key FROM trusted_identities WHERE address = ?",
                    [&addr as &dyn ToSql],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db error: {}", e)))?;

            match existing {
                None => Ok(true),
                Some(stored) => Ok(stored == identity.serialize().to_vec()),
            }
        }

        async fn get_identity(
            &self,
            address: &ProtocolAddress,
        ) -> SignalResult<Option<IdentityKey>> {
            let conn = self.conn.lock().unwrap();
            let addr = format!("{}:{}", address.name(), address.device_id());

            let bytes: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT identity_key FROM trusted_identities WHERE address = ?",
                    [&addr as &dyn ToSql],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db error: {}", e)))?;

            match bytes {
                Some(b) => {
                    let key = IdentityKey::try_from(b.as_slice()).map_err(|_| {
                        SignalProtocolError::InvalidArgument("invalid identity".into())
                    })?;
                    Ok(Some(key))
                }
                None => Ok(None),
            }
        }
    }

    #[async_trait(?Send)]
    impl SessionStore for SqliteSessionStore {
        async fn load_session(
            &self,
            address: &ProtocolAddress,
        ) -> SignalResult<Option<SessionRecord>> {
            let conn = self.conn.lock().unwrap();
            let addr = format!("{}:{}", address.name(), address.device_id());

            let bytes: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT session_record FROM sessions WHERE address = ?",
                    [&addr],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db error: {}", e)))?;

            match bytes {
                Some(b) => {
                    let record = SessionRecord::deserialize(&b)?;
                    Ok(Some(record))
                }
                None => Ok(None),
            }
        }

        async fn store_session(
            &mut self,
            address: &ProtocolAddress,
            record: &SessionRecord,
        ) -> SignalResult<()> {
            let conn = self.conn.lock().unwrap();
            let addr = format!("{}:{}", address.name(), address.device_id());

            conn.execute(
                "INSERT INTO sessions (address, session_record, created_at, last_used) 
                VALUES (?, ?, strftime('%s', 'now'), strftime('%s', 'now'))
                ON CONFLICT(address) DO UPDATE SET 
                session_record = excluded.session_record,
                last_used = strftime('%s', 'now')",
                [&addr as &dyn ToSql, &record.serialize()? as &dyn ToSql],
            )
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db error: {}", e)))?;

            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl PreKeyStore for SqlitePreKeyStore {
        async fn get_pre_key(&self, prekey_id: PreKeyId) -> SignalResult<PreKeyRecord> {
            let conn = self.conn.lock().unwrap();
            let id_val: u32 = prekey_id.into();

            let bytes: Option<(Vec<u8>, Vec<u8>)> = conn
                .query_row(
                    "SELECT public_key, private_key FROM prekeys WHERE id = ? AND used = 0",
                    [id_val as i64],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            let (pub_bytes, priv_bytes) = bytes.ok_or(SignalProtocolError::InvalidPreKeyId)?;

            let key_pair = KeyPair::from_public_and_private(&pub_bytes, &priv_bytes)
                .map_err(|_| SignalProtocolError::InvalidKeyAgreement)?;

            Ok(PreKeyRecord::new(prekey_id, &key_pair))
        }

        async fn save_pre_key(
            &mut self,
            prekey_id: PreKeyId,
            record: &PreKeyRecord,
        ) -> SignalResult<()> {
            let conn = self.conn.lock().unwrap();
            let id_val: u32 = prekey_id.into();

            conn.execute(
            "INSERT OR REPLACE INTO prekeys (id, public_key, private_key, used) VALUES (?, ?, ?, 0)",
            [
                &(id_val as i64) as &dyn ToSql,
                &record.key_pair()?.public_key.serialize() as &dyn ToSql,
                &record.key_pair()?.private_key.serialize() as &dyn ToSql,
            ],
        ).map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            Ok(())
        }

        async fn remove_pre_key(&mut self, prekey_id: PreKeyId) -> SignalResult<()> {
            let conn = self.conn.lock().unwrap();
            let id_val: u32 = prekey_id.into();

            // Soft delete
            conn.execute("UPDATE prekeys SET used = 1 WHERE id = ?", [id_val as i64])
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl SignedPreKeyStore for SqliteSignedPreKeyStore {
        async fn get_signed_pre_key(
            &self,
            signed_prekey_id: SignedPreKeyId,
        ) -> SignalResult<SignedPreKeyRecord> {
            let conn = self.conn.lock().unwrap();
            let prekey_id: u32 = signed_prekey_id.into();

            let bytes: Option<(Vec<u8>, Vec<u8>, Vec<u8>, i64)> = conn
                .query_row(
                    "SELECT public_key, private_key, signature, timestamp 
                 FROM signed_prekeys WHERE id = ?",
                    [prekey_id as i64],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                )
                .optional()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            let (pub_bytes, priv_bytes, sig_bytes, ts) =
                bytes.ok_or(SignalProtocolError::InvalidSignedPreKeyId)?;

            // Восстанавливаем KeyPair и SignedPreKeyRecord
            let key_pair = KeyPair::from_public_and_private(&pub_bytes, &priv_bytes)
                .map_err(|_| SignalProtocolError::InvalidKeyAgreement)?;

            let record = SignedPreKeyRecord::new(
                signed_prekey_id,
                Timestamp::from_epoch_millis(ts as u64),
                &key_pair,
                &sig_bytes,
            );

            Ok(record)
        }

        async fn save_signed_pre_key(
            &mut self,
            signed_prekey_id: SignedPreKeyId,
            record: &SignedPreKeyRecord,
        ) -> SignalResult<()> {
            let conn = self.conn.lock().unwrap();
            let prekey_id: u32 = signed_prekey_id.into();

            conn.execute(
                "INSERT INTO signed_prekeys 
             (id, public_key, private_key, signature, timestamp) 
             VALUES (?, ?, ?, ?, ?)",
                [
                    &(prekey_id as i64) as &dyn ToSql,
                    &record.public_key()?.serialize() as &dyn ToSql,
                    &record.private_key()?.serialize() as &dyn ToSql,
                    &record.signature()?.to_vec() as &dyn ToSql,
                    &(record.timestamp()?.epoch_millis() as i64) as &dyn ToSql,
                ],
            )
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            Ok(())
        }
    }

    #[async_trait(?Send)]
    impl KyberPreKeyStore for SqliteKyberPreKeyStore {
        async fn get_kyber_pre_key(
            &self,
            kyber_prekey_id: KyberPreKeyId,
        ) -> SignalResult<KyberPreKeyRecord> {
            let conn = self.conn.lock().unwrap();
            let id: u32 = kyber_prekey_id.into();

            let record_bytes: Option<Vec<u8>> = conn
                .query_row(
                    "SELECT record FROM kyber_pre_keys WHERE id = ?",
                    [id as i64],
                    |row| row.get(0),
                )
                .optional()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            let bytes = record_bytes.ok_or(SignalProtocolError::InvalidKyberPreKeyId)?;

            KyberPreKeyRecord::deserialize(&bytes)
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("deserialize: {}", e)))
        }

        async fn save_kyber_pre_key(
            &mut self,
            kyber_prekey_id: KyberPreKeyId,
            record: &KyberPreKeyRecord,
        ) -> SignalResult<()> {
            let conn = self.conn.lock().unwrap();
            let id: u32 = kyber_prekey_id.into();
            let record_bytes = record
                .serialize()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("serialize: {}", e)))?;

            // По умолчанию one-time (0), last-resort определяется отдельно если нужно
            let is_last_resort: i64 = 0;

            conn.execute(
                "INSERT INTO kyber_pre_keys (id, record, is_last_resort) VALUES (?, ?, ?)",
                params![id as i64, record_bytes, is_last_resort],
            )
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db: {}", e)))?;

            Ok(())
        }

        async fn mark_kyber_pre_key_used(
            &mut self,
            kyber_prekey_id: KyberPreKeyId,
            ec_prekey_id: SignedPreKeyId,
            base_key: &PublicKey,
        ) -> SignalResult<()> {
            let mut conn = self.conn.lock().unwrap();
            let kyber_id: u32 = kyber_prekey_id.into();
            let ec_id: u32 = ec_prekey_id.into();
            let base_key_bytes = base_key.serialize();

            let tx = conn.transaction().map_err(|e| {
                SignalProtocolError::InvalidArgument(format!("db transaction: {}", e))
            })?;

            // Проверяем дубль
            let exists: i64 = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM kyber_pre_key_usage 
                WHERE kyber_prekey_id = ? AND ec_prekey_id = ? AND base_key = ?)",
                    [
                        &(kyber_id as i64) as &dyn ToSql,
                        &(ec_id as i64) as &dyn ToSql,
                        &base_key_bytes as &dyn ToSql,
                    ],
                    |row| row.get(0),
                )
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db query: {}", e)))?;

            if exists != 0 {
                return Err(SignalProtocolError::DuplicatedMessage(kyber_id, ec_id));
            }

            // Проверяем is_last_resort
            let is_last_resort: bool = {
                let mut stmt = tx
                    .prepare("SELECT is_last_resort FROM kyber_pre_keys WHERE id = ?")
                    .map_err(|e| {
                        SignalProtocolError::InvalidArgument(format!("db prepare: {}", e))
                    })?;

                let value: i64 = stmt
                    .query_row([kyber_id as i64], |row| row.get(0))
                    .map_err(|e| {
                        SignalProtocolError::InvalidArgument(format!("db query: {}", e))
                    })?;

                value != 0
            };

            if is_last_resort {
                // Логируем использование
                tx.execute(
                "INSERT OR REPLACE INTO kyber_pre_key_usage (kyber_prekey_id, ec_prekey_id, base_key) VALUES (?, ?, ?)",
                params![kyber_id as i64, ec_id as i64, &base_key_bytes],
            )
            .map_err(|e| SignalProtocolError::InvalidArgument(format!("db insert: {}", e)))?;
            } else {
                // One-time — удаляем
                tx.execute("DELETE FROM kyber_pre_keys WHERE id = ?", [kyber_id as i64])
                    .map_err(|e| {
                        SignalProtocolError::InvalidArgument(format!("db delete: {}", e))
                    })?;
            }

            tx.commit()
                .map_err(|e| SignalProtocolError::InvalidArgument(format!("db commit: {}", e)))?;

            Ok(())
        }
    }
}
