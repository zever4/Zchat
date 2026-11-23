//! Public-only publication planning/receipts. The caller must persist the whole
//! plan before sending and advance it only after a verified receipt. This is
//! not a network client, private-key store, signature verifier or GC policy.
use super::contract::{ContractError, MAX_REQUEST_BYTES, PROTOCOL_VERSION};
use crate::proto::e2ee::{
    ClientContext, KyberPreKey, OneTimePreKey, SignedPreKey, UploadPreKeysRequest, UploadReceipt,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use uuid::{Uuid, Variant};

const MAX_KEYS_PER_BATCH: usize = 100;
const MAX_PLAN_KEYS_PER_KIND: usize = 4096;

fn uuid4(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|id| {
        value.len() == 36
            && id.get_version_num() == 4
            && id.get_variant() == Variant::RFC4122
            && id.to_string() == value
    })
}
fn ec_public(key: &[u8]) -> bool {
    key.len() == 33 && key[0] == 5
}

fn validate(req: &UploadPreKeysRequest, plan: bool) -> Result<(), ContractError> {
    let ctx = req
        .context
        .as_ref()
        .ok_or(ContractError::Invalid("publication context"))?;
    if ctx.protocol_version != PROTOCOL_VERSION {
        return Err(ContractError::UnsupportedVersion);
    }
    let max = if plan {
        MAX_PLAN_KEYS_PER_KIND
    } else {
        MAX_KEYS_PER_BATCH
    };
    if !uuid4(&ctx.device_generation_id)
        || !uuid4(&req.batch_id)
        || req.expected_revision >= i64::MAX as u64
        || (!plan && req.encoded_len() > MAX_REQUEST_BYTES)
        || req.one_time_pre_keys.len() > max
        || req.kyber_pre_keys.len() > max
        || (req.signed_pre_key.is_none()
            && req.one_time_pre_keys.is_empty()
            && req.kyber_pre_keys.is_empty())
    {
        return Err(ContractError::Invalid("publication fields"));
    }
    if req.signed_pre_key.as_ref().is_some_and(|k| {
        !ec_public(&k.public_key) || k.signature.len() != 64 || k.created_at_unix_ms < 0
    }) {
        return Err(ContractError::Invalid("signed prekey"));
    }
    let mut ids = HashSet::new();
    for k in &req.one_time_pre_keys {
        if !ec_public(&k.public_key) || !ids.insert(k.key_id) {
            return Err(ContractError::Invalid("EC prekey"));
        }
    }
    ids.clear();
    let mut last_resort = false;
    for k in &req.kyber_pre_keys {
        if k.public_key.len() != 1569
            || k.public_key[0] != 8
            || k.signature.len() != 64
            || !ids.insert(k.key_id)
            || (k.is_last_resort && last_resort)
        {
            return Err(ContractError::Invalid("Kyber prekey"));
        }
        last_resort |= k.is_last_resort;
    }
    Ok(())
}

pub fn validate_upload(req: &UploadPreKeysRequest) -> Result<(), ContractError> {
    validate(req, false)
}

/// SHA256 of the explicit publication.v1 frame, never protobuf serialization.
pub fn publication_digest(req: &UploadPreKeysRequest) -> Result<[u8; 32], ContractError> {
    validate_upload(req)?;
    let mut hash = Sha256::new();
    hash.update(b"zchat.e2ee.publication.v1\0");
    let ctx = req
        .context
        .as_ref()
        .ok_or(ContractError::Invalid("publication context"))?;
    hash.update(ctx.protocol_version.to_be_bytes());
    hash.update(ctx.device_generation_id.as_bytes());
    hash.update(req.batch_id.as_bytes());
    hash.update(req.expected_revision.to_be_bytes());
    if let Some(k) = &req.signed_pre_key {
        hash.update(1u32.to_be_bytes());
        hash.update(k.key_id.to_be_bytes());
        hash.update(&k.public_key);
        hash.update(&k.signature);
        hash.update((k.created_at_unix_ms as u64).to_be_bytes());
    } else {
        hash.update(0u32.to_be_bytes());
    }
    hash.update((req.one_time_pre_keys.len() as u32).to_be_bytes());
    for k in &req.one_time_pre_keys {
        hash.update(k.key_id.to_be_bytes());
        hash.update(&k.public_key);
    }
    hash.update((req.kyber_pre_keys.len() as u32).to_be_bytes());
    for k in &req.kyber_pre_keys {
        hash.update(k.key_id.to_be_bytes());
        hash.update(&k.public_key);
        hash.update(&k.signature);
        hash.update(u32::from(k.is_last_resort).to_be_bytes());
    }
    Ok(hash.finalize().into())
}

pub fn validate_upload_receipt(
    req: &UploadPreKeysRequest,
    receipt: &UploadReceipt,
) -> Result<(), ContractError> {
    let digest = publication_digest(req)?;
    if req.batch_id != receipt.batch_id
        || receipt.publication_revision != req.expected_revision + 1
        || receipt.batch_digest.as_slice() != digest
    {
        return Err(ContractError::Invalid("publication receipt"));
    }
    Ok(())
}

/// Create a bounded, ordered plan. Each successful batch advances the account's
/// publication CAS once. Resume exact saved requests; never regenerate IDs on
/// a network retry or mark private keys disposable after an upload receipt.
pub fn prepare_upload_batches(
    context: ClientContext,
    expected_revision: u64,
    signed: Option<SignedPreKey>,
    ec: Vec<OneTimePreKey>,
    pq: Vec<KyberPreKey>,
) -> Result<Vec<UploadPreKeysRequest>, ContractError> {
    let input = UploadPreKeysRequest {
        context: Some(context.clone()),
        batch_id: Uuid::new_v4().to_string(),
        expected_revision,
        signed_pre_key: signed,
        one_time_pre_keys: ec,
        kyber_pre_keys: pq,
    };
    validate(&input, true)?;
    let mut batches = vec![UploadPreKeysRequest {
        context: Some(context.clone()),
        batch_id: input.batch_id,
        expected_revision,
        signed_pre_key: input.signed_pre_key,
        ..Default::default()
    }];
    enum Key {
        Ec(OneTimePreKey),
        Pq(KyberPreKey),
    }
    // Put last-resort first so an initial publication can become ready promptly.
    let mut pq = input.kyber_pre_keys;
    pq.sort_by_key(|k| !k.is_last_resort);
    let keys = input
        .one_time_pre_keys
        .into_iter()
        .map(Key::Ec)
        .chain(pq.into_iter().map(Key::Pq));
    for key in keys {
        let current = batches
            .last_mut()
            .ok_or(ContractError::Invalid("publication plan"))?;
        match &key {
            Key::Ec(k) => current.one_time_pre_keys.push(k.clone()),
            Key::Pq(k) => current.kyber_pre_keys.push(k.clone()),
        }
        if current.encoded_len() <= MAX_REQUEST_BYTES
            && current.one_time_pre_keys.len() <= MAX_KEYS_PER_BATCH
            && current.kyber_pre_keys.len() <= MAX_KEYS_PER_BATCH
        {
            continue;
        }
        match &key {
            Key::Ec(_) => {
                current.one_time_pre_keys.pop();
            }
            Key::Pq(_) => {
                current.kyber_pre_keys.pop();
            }
        }
        let next = current
            .expected_revision
            .checked_add(1)
            .filter(|r| *r < i64::MAX as u64)
            .ok_or(ContractError::Invalid("publication revision exhausted"))?;
        let mut batch = UploadPreKeysRequest {
            context: Some(context.clone()),
            batch_id: Uuid::new_v4().to_string(),
            expected_revision: next,
            ..Default::default()
        };
        match key {
            Key::Ec(k) => batch.one_time_pre_keys.push(k),
            Key::Pq(k) => batch.kyber_pre_keys.push(k),
        }
        batches.push(batch);
    }
    for batch in &batches {
        validate_upload(batch)?;
    }
    Ok(batches)
}
