//! Durable initial registration and publication, before any native RPC adapter.
//!
//! This synchronous owner must run off the UI thread. It owns the account store
//! until it hands the same connection/lock to the Signal worker. No network call
//! belongs inside these transactions. Replacement is a separate explicit flow;
//! an existing/unmanaged generation is never adopted or overwritten here.
use super::{
    protected::{
        AccountKey, AccountKind, ProtectedStore, ProtectedTransaction, RecordKey, RecordKind,
    },
    publication::{prepare_upload_batches, validate_upload, validate_upload_receipt},
    signal_state::{CryptoError, CryptoResult, DeviceMaterial, install_material},
};
use crate::proto::e2ee::{
    ClientContext, DeviceRef, KyberPreKey, OneTimePreKey, OwnDeviceState, RegisterDeviceRequest,
    SignedPreKey, UploadPreKeysRequest, UploadReceipt,
};
use libsignal_protocol::{
    GenericSignedPreKey, IdentityKeyPair, KeyPair, KyberPreKeyRecord, PreKeyRecord,
    SignedPreKeyRecord, Timestamp, kem,
};
use prost::Message;
use rand::{Rng, TryRngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::{Uuid, Variant};
use zeroize::{Zeroize, Zeroizing};

const INITIAL_KEYS: u32 = 32;
const PLAN_ID: &str = "initial-plan";
pub(crate) const ARCHIVED_DEVICE_RECORD: &str = "archived-device";

#[path = "replacement.rs"]
mod replacement;
pub use replacement::ReplacementAttempt;
pub(crate) use replacement::read_archived_device;

// Private protobuf is a local versioned record, never an RPC/IPC message.
// Suppress prost's generated Debug, which would print private material.
#[derive(Message)]
#[prost(skip_debug)]
struct Pending {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, optional, tag = "2")]
    request: Option<RegisterDeviceRequest>,
    #[prost(bytes = "vec", tag = "3")]
    identity: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    credential: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    signed: Vec<u8>,
    #[prost(bytes = "vec", repeated, tag = "6")]
    ec: Vec<Vec<u8>>,
    #[prost(bytes = "vec", repeated, tag = "7")]
    pq: Vec<Vec<u8>>,
    #[prost(bytes = "vec", tag = "8")]
    last_resort: Vec<u8>,
    #[prost(message, optional, tag = "9")]
    bound: Option<DeviceRef>,
    #[prost(string, tag = "10")]
    expected_generation_id: String, // Empty for Register; persisted CAS for Replace.
}
impl std::fmt::Debug for Pending {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Pending([redacted])")
    }
}
impl Pending {
    fn clear_private(&mut self) {
        self.identity.zeroize();
        self.credential.zeroize();
        self.signed.zeroize();
        for key in &mut self.ec {
            key.zeroize();
        }
        for key in &mut self.pq {
            key.zeroize();
        }
        self.ec.clear();
        self.pq.clear();
        self.last_resort.zeroize();
        self.identity.clear();
        self.credential.clear();
        self.signed.clear();
        self.last_resort.clear();
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.clear_private();
    }
}

#[derive(Message)]
struct Progress {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(message, repeated, tag = "2")]
    requests: Vec<UploadPreKeysRequest>,
    #[prost(message, repeated, tag = "3")]
    receipts: Vec<UploadReceipt>,
}

/// Native-only RPC input. Deliberately no Debug/serialization or renderer exposure.
pub struct RegistrationAttempt {
    pub request: RegisterDeviceRequest,
    pub bound_device: Option<DeviceRef>,
    credential: Zeroizing<[u8; 32]>,
}
impl RegistrationAttempt {
    /// Only the trusted native transport may put this secret in TLS RPC metadata.
    pub fn credential(&self) -> &[u8; 32] {
        &self.credential
    }
}

pub struct DeviceProvisioning {
    store: ProtectedStore,
}

/// A current observation, not a permanent server guarantee. None of these states
/// authorizes adopting remote keys or deleting local history. RPC authorization
/// still checks the active generation on every operation.
#[derive(Debug, PartialEq)]
pub enum Reconciliation {
    Unregistered,
    RegistrationPending,
    PublicationPending,
    Ready,
    ReplacementRequired {
        active_device: DeviceRef,
    },
    DeviceUnavailable {
        local_device: DeviceRef,
        active_device: Option<DeviceRef>,
    },
    ReplacementPending {
        request_id: String,
        expected_generation_id: String,
        active_device: Option<DeviceRef>,
    },
}

fn account_key(kind: AccountKind) -> CryptoResult<AccountKey> {
    Ok(AccountKey::new(kind, "initial")?)
}
fn plan_key(own: &DeviceRef) -> CryptoResult<RecordKey> {
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Publication,
        PLAN_ID,
    )?)
}
fn uuid4(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|id| {
        id.get_version_num() == 4 && id.get_variant() == Variant::RFC4122 && id.to_string() == value
    })
}
fn credential_hash(credential: &[u8]) -> Vec<u8> {
    let mut hash = Sha256::new();
    hash.update(b"zchat.e2ee.device.v1\0");
    hash.update(credential);
    hash.finalize().to_vec()
}
fn load_pending(tx: &mut ProtectedTransaction<'_>) -> CryptoResult<Option<Pending>> {
    load_pending_at(tx, &account_key(AccountKind::Registration)?)
}
fn load_pending_at(
    tx: &mut ProtectedTransaction<'_>,
    key: &AccountKey,
) -> CryptoResult<Option<Pending>> {
    let Some(bytes) = tx.get_account(key)? else {
        return Ok(None);
    };
    let pending = Pending::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let req = pending.request.as_ref().ok_or(CryptoError::Invalid)?;
    if pending.format != 1
        || req.protocol_version != 1
        || !uuid4(&req.registration_request_id)
        || req.registration_id == 0
        || req.registration_id > 16383
        || req.identity_key.len() != 33
        || req.identity_key[0] != 5
        || req.credential_hash.len() != 32
        || (!pending.expected_generation_id.is_empty() && !uuid4(&pending.expected_generation_id))
    {
        return Err(CryptoError::Invalid);
    }
    if let Some(own) = &pending.bound {
        check_owner(tx, own)?;
        if pending.expected_generation_id == own.generation_id
            || !pending.identity.is_empty()
            || !pending.credential.is_empty()
            || !pending.signed.is_empty()
            || !pending.ec.is_empty()
            || !pending.pq.is_empty()
            || !pending.last_resort.is_empty()
        {
            return Err(CryptoError::Invalid);
        }
    } else {
        // Parse the saved private identity and verify its public registration.
        let identity = IdentityKeyPair::try_from(pending.identity.as_slice())
            .map_err(|_| CryptoError::Invalid)?;
        if pending.credential.len() != 32
            || credential_hash(&pending.credential) != req.credential_hash
            || identity.public_key().serialize().as_ref() != req.identity_key
            || pending.ec.len() != INITIAL_KEYS as usize
            || pending.pq.len() != INITIAL_KEYS as usize
        {
            return Err(CryptoError::Invalid);
        }
        // Refuse a partial/corrupt prepared key set before returning an RPC input.
        material(&pending)?;
    }
    Ok(Some(pending))
}

fn validated_current(tx: &mut ProtectedTransaction<'_>) -> CryptoResult<Option<Pending>> {
    let pending = load_pending(tx)?;
    if let Some(saved) = &pending {
        attempt(tx, saved)?;
        if let Some(own) = &saved.bound {
            load_progress(tx, own)?;
            return Ok(pending);
        }
    }
    if tx
        .get_account(&account_key(AccountKind::ActiveDevice)?)?
        .is_some()
        || tx.has_generation_records()?
    {
        return Err(CryptoError::Conflict);
    }
    Ok(pending)
}
fn check_owner(tx: &ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<()> {
    if own.user_id != tx.account_id().to_string()
        || own.device_id != 1
        || !uuid4(&own.generation_id)
    {
        return Err(CryptoError::Invalid);
    }
    Ok(())
}
fn load_active(tx: &mut ProtectedTransaction<'_>) -> CryptoResult<OwnDeviceState> {
    let bytes = tx
        .get_account(&account_key(AccountKind::ActiveDevice)?)?
        .ok_or(CryptoError::Invalid)?;
    let active = OwnDeviceState::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if active.user_id != tx.account_id().to_string() {
        return Err(CryptoError::Invalid);
    }
    check_owner(
        tx,
        active
            .active_device
            .as_ref()
            .and_then(|d| d.reference.as_ref())
            .ok_or(CryptoError::Invalid)?,
    )?;
    Ok(active)
}
fn attempt(
    tx: &mut ProtectedTransaction<'_>,
    pending: &Pending,
) -> CryptoResult<RegistrationAttempt> {
    let request = pending.request.clone().ok_or(CryptoError::Invalid)?;
    let credential = if let Some(own) = &pending.bound {
        let active = load_active(tx)?;
        let device = active.active_device.as_ref().ok_or(CryptoError::Invalid)?;
        if active
            .active_device
            .as_ref()
            .and_then(|d| d.reference.as_ref())
            != Some(own)
            || device.identity_key != request.identity_key
            || device.registration_id != request.registration_id
        {
            return Err(CryptoError::Conflict);
        }
        let identity = tx
            .get(&RecordKey::new(
                &own.generation_id,
                RecordKind::Identity,
                "local",
            )?)?
            .ok_or(CryptoError::Invalid)?;
        if identity.len() < 6
            || identity[0] != 1
            || identity[1..5] != request.registration_id.to_be_bytes()
            || IdentityKeyPair::try_from(&identity[5..])?
                .public_key()
                .serialize()
                .as_ref()
                != request.identity_key
        {
            return Err(CryptoError::Invalid);
        }
        tx.get(&RecordKey::new(
            &own.generation_id,
            RecordKind::DeviceCredential,
            "local",
        )?)?
        .ok_or(CryptoError::Invalid)?
    } else {
        Zeroizing::new(pending.credential.clone())
    };
    let credential: Zeroizing<[u8; 32]> = Zeroizing::new(
        credential
            .as_slice()
            .try_into()
            .map_err(|_| CryptoError::Invalid)?,
    );
    if credential_hash(credential.as_ref()) != request.credential_hash {
        return Err(CryptoError::Invalid);
    }
    Ok(RegistrationAttempt {
        request,
        bound_device: pending.bound.clone(),
        credential,
    })
}
fn material(p: &Pending) -> CryptoResult<DeviceMaterial> {
    let request = p.request.as_ref().ok_or(CryptoError::Invalid)?;
    let mut pq: Vec<_> =
        p.pq.iter()
            .map(|b| KyberPreKeyRecord::deserialize(b).map(|k| (k, false)))
            .collect::<Result<_, _>>()?;
    pq.push((KyberPreKeyRecord::deserialize(&p.last_resort)?, true));
    let keys = DeviceMaterial {
        identity: IdentityKeyPair::try_from(p.identity.as_slice())
            .map_err(|_| CryptoError::Invalid)?,
        registration_id: request.registration_id,
        credential: Zeroizing::new(
            p.credential
                .as_slice()
                .try_into()
                .map_err(|_| CryptoError::Invalid)?,
        ),
        signed: SignedPreKeyRecord::deserialize(&p.signed)?,
        ec: p
            .ec
            .iter()
            .map(|b| PreKeyRecord::deserialize(b))
            .collect::<Result<_, _>>()?,
        pq,
    };
    if !keys.identity.public_key().verify_signature(
        &keys.signed.public_key()?.serialize(),
        &keys.signed.signature()?,
    ) {
        return Err(CryptoError::Invalid);
    }
    keys.signed.key_pair()?;
    let mut ec_ids = HashSet::new();
    for key in &keys.ec {
        if !ec_ids.insert(u32::from(key.id()?)) {
            return Err(CryptoError::Invalid);
        }
        key.key_pair()?;
    }
    let mut pq_ids = HashSet::new();
    for (key, _) in &keys.pq {
        key.key_pair()?;
        if !pq_ids.insert(u32::from(key.id()?))
            || key.public_key()?.serialize().len() != 1569
            || !keys
                .identity
                .public_key()
                .verify_signature(&key.public_key()?.serialize(), &key.signature()?)
        {
            return Err(CryptoError::Invalid);
        }
    }
    Ok(keys)
}
fn generate() -> CryptoResult<Pending> {
    let mut rng = rand::rng();
    let identity = IdentityKeyPair::generate(&mut rng);
    let mut credential = Zeroizing::new(vec![0; 32]);
    OsRng
        .try_fill_bytes(&mut credential)
        .map_err(|_| super::protected::StoreError::Protection)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CryptoError::Invalid)?
        .as_millis();
    let now = u64::try_from(now)
        .ok()
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    let timestamp = Timestamp::from_epoch_millis(now);
    let pair = KeyPair::generate(&mut rng);
    let signature = identity
        .private_key()
        .calculate_signature(&pair.public_key.serialize(), &mut rng)
        .map_err(|_| CryptoError::Invalid)?;
    let signed = Zeroizing::new(
        SignedPreKeyRecord::new(1.into(), timestamp, &pair, &signature).serialize()?,
    );
    let mut pending = Pending {
        format: 1,
        request: Some(RegisterDeviceRequest {
            protocol_version: 1,
            registration_request_id: Uuid::new_v4().to_string(),
            registration_id: rng.random_range(1..=16383),
            identity_key: identity.public_key().serialize().to_vec(),
            credential_hash: credential_hash(&credential),
        }),
        identity: identity.serialize().into_vec(),
        credential: credential.to_vec(),
        signed: signed.to_vec(),
        ec: vec![],
        pq: vec![],
        last_resort: vec![],
        bound: None,
        expected_generation_id: String::new(),
    };
    for id in 1..=INITIAL_KEYS {
        pending
            .ec
            .push(PreKeyRecord::new(id.into(), &KeyPair::generate(&mut rng)).serialize()?);
    }
    for id in 1..=INITIAL_KEYS + 1 {
        let pair = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut rng);
        let signature = identity
            .private_key()
            .calculate_signature(&pair.public_key.serialize(), &mut rng)
            .map_err(|_| CryptoError::Invalid)?;
        let bytes = KyberPreKeyRecord::new(id.into(), timestamp, &pair, &signature).serialize()?;
        if id == INITIAL_KEYS + 1 {
            pending.last_resort = bytes
        } else {
            pending.pq.push(bytes)
        }
    }
    Ok(pending)
}
fn public_plan(own: &DeviceRef, m: &DeviceMaterial) -> CryptoResult<Progress> {
    let signed = SignedPreKey {
        key_id: m.signed.id()?.into(),
        public_key: m.signed.public_key()?.serialize().to_vec(),
        signature: m.signed.signature()?,
        created_at_unix_ms: m.signed.timestamp()?.epoch_millis() as i64,
    };
    let ec =
        m.ec.iter()
            .map(|k| {
                Ok(OneTimePreKey {
                    key_id: k.id()?.into(),
                    public_key: k.public_key()?.serialize().to_vec(),
                })
            })
            .collect::<CryptoResult<Vec<_>>>()?;
    let pq =
        m.pq.iter()
            .map(|(k, lr)| {
                Ok(KyberPreKey {
                    key_id: k.id()?.into(),
                    public_key: k.public_key()?.serialize().to_vec(),
                    signature: k.signature()?,
                    is_last_resort: *lr,
                })
            })
            .collect::<CryptoResult<Vec<_>>>()?;
    let requests = prepare_upload_batches(
        ClientContext {
            protocol_version: 1,
            device_generation_id: own.generation_id.clone(),
        },
        0,
        Some(signed),
        ec,
        pq,
    )
    .map_err(|_| CryptoError::Invalid)?;
    Ok(Progress {
        format: 1,
        requests,
        receipts: vec![],
    })
}
fn load_progress(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<Progress> {
    let bytes = tx.get(&plan_key(own)?)?.ok_or(CryptoError::Invalid)?;
    let progress = Progress::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if progress.format != 1
        || progress.requests.is_empty()
        || progress.requests.len() > 8
        || progress.receipts.len() > progress.requests.len()
    {
        return Err(CryptoError::Invalid);
    }
    let mut ids = HashSet::new();
    for (i, request) in progress.requests.iter().enumerate() {
        validate_upload(request).map_err(|_| CryptoError::Invalid)?;
        if request
            .context
            .as_ref()
            .is_none_or(|c| c.device_generation_id != own.generation_id)
            || request.expected_revision != i as u64
            || !ids.insert(&request.batch_id)
        {
            return Err(CryptoError::Invalid);
        }
        if let Some(receipt) = progress.receipts.get(i) {
            validate_upload_receipt(request, receipt).map_err(|_| CryptoError::Invalid)?;
        }
    }
    Ok(progress)
}

pub(super) fn mark_publication(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    request: &UploadPreKeysRequest,
    receipt: &UploadReceipt,
) -> CryptoResult<()> {
    let mut active = load_active(tx)?;
    let device = active.active_device.as_mut().ok_or(CryptoError::Invalid)?;
    if device.reference.as_ref() != Some(own)
        || device.public_material_revision != request.expected_revision
    {
        return Err(CryptoError::Conflict);
    }
    let mut mark = |kind: &str, id: u32| -> CryptoResult<()> {
        tx.put(
            &RecordKey::new(
                &own.generation_id,
                RecordKind::Control,
                &format!("published:{kind}:{id}"),
            )?,
            &receipt.encode_to_vec(),
        )?;
        Ok(())
    };
    for key in &request.one_time_pre_keys {
        mark("ec", key.key_id)?;
    }
    for key in &request.kyber_pre_keys {
        mark("pq", key.key_id)?;
    }
    if let Some(key) = &request.signed_pre_key {
        mark("signed", key.key_id)?;
    }
    if let Some(key) = &request.signed_pre_key {
        tx.put(
            &RecordKey::new(&own.generation_id, RecordKind::Control, "active-signed")?,
            &key.key_id.to_be_bytes(),
        )?;
    }
    if let Some(key) = request.kyber_pre_keys.iter().find(|k| k.is_last_resort) {
        tx.put(
            &RecordKey::new(
                &own.generation_id,
                RecordKind::Control,
                "active-last-resort",
            )?,
            &key.key_id.to_be_bytes(),
        )?;
    }
    device.public_material_revision = receipt.publication_revision;
    device.public_material_ready = tx
        .get(&RecordKey::new(
            &own.generation_id,
            RecordKind::Control,
            "active-signed",
        )?)?
        .is_some()
        && tx
            .get(&RecordKey::new(
                &own.generation_id,
                RecordKind::Control,
                "active-last-resort",
            )?)?
            .is_some();
    tx.put_account(
        &account_key(AccountKind::ActiveDevice)?,
        &active.encode_to_vec(),
    )?;
    Ok(())
}

pub(super) fn check_publication_revision(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    revision: u64,
) -> CryptoResult<()> {
    let active = load_active(tx)?;
    let device = active.active_device.ok_or(CryptoError::Invalid)?;
    if device.reference.as_ref() != Some(own) || device.public_material_revision != revision {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
pub(super) fn initial_revision(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
) -> CryptoResult<u64> {
    let p = load_progress(tx, own)?;
    if p.receipts.len() != p.requests.len() {
        return Err(CryptoError::Conflict);
    }
    Ok(p.receipts.len() as u64)
}
fn publication_progress(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    p: &Progress,
) -> CryptoResult<super::signal_state::prekey_refill::PublicationProgress> {
    if p.receipts.len() != p.requests.len() {
        return Ok(super::signal_state::prekey_refill::PublicationProgress {
            revision: p.receipts.len() as u64,
            complete: false,
            may_be_in_flight: true,
        });
    }
    super::signal_state::prekey_refill::progress(tx, own, p.receipts.len() as u64)
}

impl DeviceProvisioning {
    pub fn new(store: ProtectedStore) -> Self {
        Self { store }
    }

    pub(crate) fn scope(&self) -> &super::protected::StoreScope {
        self.store.scope()
    }

    /// Read-only reconciliation. In particular, GetOwnDevice is not a Register
    /// receipt: a matching pending identity still needs the exact Register replay
    /// to prove the saved request/credential before binding any generation.
    pub fn reconcile(&mut self, remote: &OwnDeviceState) -> CryptoResult<Reconciliation> {
        super::native::validate_own(remote).map_err(|_| CryptoError::Invalid)?;
        self.store.transaction_with(|tx| {
            if remote.user_id != tx.account_id().to_string() {
                return Err(CryptoError::Conflict);
            }
            let remote_device = remote.active_device.as_ref();
            let remote_ref = remote_device.and_then(|d| d.reference.clone());
            let current = validated_current(tx)?;
            if let Some(replacement) = replacement::load(tx)? {
                if replacement.bound.is_none() {
                    return Ok(Reconciliation::ReplacementPending {
                        request_id: replacement
                            .request
                            .as_ref()
                            .ok_or(CryptoError::Invalid)?
                            .registration_request_id
                            .clone(),
                        expected_generation_id: replacement.expected_generation_id.clone(),
                        active_device: remote_ref,
                    });
                }
                replacement::check_current(&replacement, current.as_ref())?;
            }
            let Some(pending) = current else {
                if tx
                    .get_account(&account_key(AccountKind::ActiveDevice)?)?
                    .is_some()
                    || tx.has_generation_records()?
                {
                    return Err(CryptoError::Conflict);
                }
                return Ok(match remote_ref {
                    Some(active_device) => Reconciliation::ReplacementRequired { active_device },
                    None => Reconciliation::Unregistered,
                });
            };
            let saved = attempt(tx, &pending)?;
            let Some(own) = &pending.bound else {
                if tx
                    .get_account(&account_key(AccountKind::ActiveDevice)?)?
                    .is_some()
                    || tx.has_generation_records()?
                {
                    return Err(CryptoError::Conflict);
                }
                return Ok(match remote_device {
                    Some(d)
                        if d.identity_key != saved.request.identity_key
                            || d.registration_id != saved.request.registration_id =>
                    {
                        Reconciliation::ReplacementRequired {
                            active_device: remote_ref.ok_or(CryptoError::Invalid)?,
                        }
                    }
                    _ => Reconciliation::RegistrationPending,
                });
            };
            // Validate the saved publication journal even if the device is revoked.
            let progress = load_progress(tx, own)?;
            if remote_ref.as_ref() != Some(own) {
                return Ok(Reconciliation::DeviceUnavailable {
                    local_device: own.clone(),
                    active_device: remote_ref,
                });
            }
            let device = remote_device.ok_or(CryptoError::Invalid)?;
            if device.identity_key != saved.request.identity_key
                || device.registration_id != saved.request.registration_id
            {
                return Err(CryptoError::Conflict);
            }
            let local = load_active(tx)?.active_device.ok_or(CryptoError::Invalid)?;
            let publication = publication_progress(tx, own, &progress)?;
            let committed = publication.revision;
            let complete = publication.complete;
            // At most the next sequential upload can have committed without its
            // local receipt. Never trust GetOwn's revision to skip that receipt.
            if local.public_material_revision != committed
                || device.public_material_revision < committed
                || device.public_material_revision
                    > committed + u64::from(publication.may_be_in_flight)
                || (device.public_material_revision == committed
                    && device.public_material_ready != local.public_material_ready)
                || (local.public_material_ready && !device.public_material_ready)
            {
                return Err(CryptoError::Conflict);
            }
            if complete {
                if !local.public_material_ready || !device.public_material_ready {
                    return Err(CryptoError::Conflict);
                }
                Ok(Reconciliation::Ready)
            } else {
                Ok(Reconciliation::PublicationPending)
            }
        })
    }

    /// Idempotent across restarts and lost Register responses. Returns only after
    /// the generated private keys, credential and request UUID have committed.
    pub fn prepare_registration(&mut self) -> CryptoResult<RegistrationAttempt> {
        self.store.transaction_with(|tx| {
            if replacement::load(tx)?.is_some_and(|p| p.bound.is_none()) {
                return Err(CryptoError::Conflict);
            }
            if let Some(pending) = load_pending(tx)? {
                return attempt(tx, &pending);
            }
            if tx
                .get_account(&account_key(AccountKind::ActiveDevice)?)?
                .is_some()
                || tx.has_generation_records()?
            {
                return Err(CryptoError::Conflict);
            }
            let pending = generate()?;
            tx.put_account(
                &account_key(AccountKind::Registration)?,
                &Zeroizing::new(pending.encode_to_vec()),
            )?;
            attempt(tx, &pending)
        })
    }

    /// Correlate the authenticated response with the persisted Register request.
    /// Install private material, bind the generation and persist every upload ID
    /// atomically. A response from another/newer installation is never adopted.
    pub fn bind_registration(
        &mut self,
        request_id: &str,
        receipt: &OwnDeviceState,
    ) -> CryptoResult<()> {
        self.store.transaction_with(|tx| {
            let mut pending = load_pending(tx)?.ok_or(CryptoError::Invalid)?;
            let request = pending.request.as_ref().ok_or(CryptoError::Invalid)?;
            if !pending.expected_generation_id.is_empty() {
                return Err(CryptoError::Conflict); // Replace response is not a Register receipt.
            }
            let device = receipt.active_device.as_ref().ok_or(CryptoError::Invalid)?;
            let own = device.reference.as_ref().ok_or(CryptoError::Invalid)?;
            check_owner(tx, own)?;
            if request.registration_request_id != request_id
                || receipt.user_id != own.user_id
                || device.identity_key != request.identity_key
                || device.registration_id != request.registration_id
            {
                return Err(CryptoError::Conflict);
            }
            if let Some(bound) = &pending.bound {
                if bound != own {
                    return Err(CryptoError::Conflict);
                }
                // Register replay may contain newer readiness/revision. Only a
                // verified upload receipt advances the saved local progress.
                attempt(tx, &pending)?;
                load_progress(tx, own)?;
                return Ok(());
            }
            if device.public_material_ready
                || device.public_material_revision != 0
                || tx
                    .get_account(&account_key(AccountKind::ActiveDevice)?)?
                    .is_some()
                || tx.has_generation_records()?
            {
                return Err(CryptoError::Conflict);
            }
            let keys = material(&pending)?;
            let progress = public_plan(own, &keys)?;
            install_material(tx, own, keys)?;
            tx.put(&plan_key(own)?, &progress.encode_to_vec())?;
            tx.put_account(
                &account_key(AccountKind::ActiveDevice)?,
                &receipt.encode_to_vec(),
            )?;
            pending.bound = Some(own.clone());
            pending.clear_private();
            tx.put_account(
                &account_key(AccountKind::Registration)?,
                &Zeroizing::new(pending.encode_to_vec()),
            )?;
            Ok(())
        })
    }

    pub fn next_upload(&mut self) -> CryptoResult<Option<UploadPreKeysRequest>> {
        self.store.transaction_with(|tx| {
            let active = load_active(tx)?;
            let own = active
                .active_device
                .as_ref()
                .and_then(|d| d.reference.as_ref())
                .ok_or(CryptoError::Invalid)?;
            let progress = load_progress(tx, own)?;
            if let Some(request) = progress.requests.get(progress.receipts.len()) {
                return Ok(Some(request.clone()));
            }
            super::signal_state::prekey_refill::next_upload(tx, own)
        })
    }

    /// An exact acknowledged upload authenticates a bound installation without
    /// replaying password-protected Replace on every ordinary login/update.
    pub(crate) fn publication_probe(&mut self) -> CryptoResult<UploadPreKeysRequest> {
        self.store.transaction_with(|tx| {
            let active = load_active(tx)?;
            let own = active
                .active_device
                .as_ref()
                .and_then(|d| d.reference.as_ref())
                .ok_or(CryptoError::Invalid)?;
            let progress = load_progress(tx, own)?;
            if progress.receipts.len() != progress.requests.len() {
                return Err(CryptoError::Conflict);
            }
            if let Some(request) = super::signal_state::prekey_refill::probe(tx, own)? {
                return Ok(request);
            }
            progress
                .requests
                .last()
                .cloned()
                .ok_or(CryptoError::Invalid)
        })
    }

    /// Acknowledgement records publication separately from private-key use. An
    /// exact old receipt is harmless; a future/mismatched receipt cannot skip work.
    pub fn acknowledge_upload(&mut self, receipt: &UploadReceipt) -> CryptoResult<()> {
        self.store.transaction_with(|tx| {
            let active = load_active(tx)?;
            let own = active
                .active_device
                .as_ref()
                .and_then(|d| d.reference.as_ref())
                .ok_or(CryptoError::Invalid)?;
            let mut progress = load_progress(tx, own)?;
            if !progress
                .requests
                .iter()
                .any(|r| r.batch_id == receipt.batch_id)
            {
                return super::signal_state::prekey_refill::acknowledge(tx, own, receipt);
            }
            if let Some(old) = progress
                .receipts
                .iter()
                .find(|r| r.batch_id == receipt.batch_id)
            {
                return if old == receipt {
                    Ok(())
                } else {
                    Err(CryptoError::Conflict)
                };
            }
            let request = progress
                .requests
                .get(progress.receipts.len())
                .ok_or(CryptoError::Conflict)?;
            validate_upload_receipt(request, receipt).map_err(|_| CryptoError::Conflict)?;
            mark_publication(tx, own, request, receipt)?;
            progress.receipts.push(receipt.clone());
            tx.put(&plan_key(own)?, &progress.encode_to_vec())?;
            Ok(())
        })
    }

    /// Complete initial publication before moving the same store into the Signal
    /// worker. On failure retain ownership so the caller can resume/retry safely.
    pub fn ready_device(&mut self) -> CryptoResult<DeviceRef> {
        self.store.transaction_with(|tx| {
            let active = load_active(tx)?;
            let device = active.active_device.as_ref().ok_or(CryptoError::Invalid)?;
            let own = device.reference.as_ref().ok_or(CryptoError::Invalid)?;
            let progress = load_progress(tx, own)?;
            let publication = publication_progress(tx, own, &progress)?;
            if !device.public_material_ready
                || !publication.complete
                || device.public_material_revision != publication.revision
            {
                return Err(CryptoError::Invalid);
            }
            Ok(own.clone())
        })
    }

    /// Native lifecycle retains the store on error and calls ready_device first.
    /// No key regeneration occurs when handing ownership to CryptoCoordinator.
    pub fn into_store(self) -> ProtectedStore {
        self.store
    }

    pub(crate) fn into_archive_store(self) -> CryptoResult<Option<(ProtectedStore, DeviceRef)>> {
        let mut store = self.store.into_read_only()?;
        let own = store.transaction_with(|tx| {
            Ok::<_, CryptoError>(validated_current(tx)?.and_then(|p| p.bound.clone()))
        })?;
        Ok(own.map(|own| (store, own)))
    }
}

#[cfg(test)]
#[path = "provisioning_tests.rs"]
mod tests;
