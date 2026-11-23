//! Rotation-specific checks within the shared durable publication journal.
use super::*;
use crate::proto::e2ee::SignedPreKey;

pub(super) fn validate_shape(p: &Plan) -> CryptoResult<()> {
    match p.format {
        1 if !p.rotate_signed
            && !p.rotate_last_resort
            && p.signed_operation.is_empty()
            && p.signed_id == 0 =>
        {
            counts(p.ec_count, p.pq_count)
        }
        2 if (p.rotate_signed || p.rotate_last_resort)
            && p.ec_count == 0
            && p.pq_count == 0
            && p.requests.len() <= 1 =>
        {
            Ok(())
        }
        _ => Err(CryptoError::Invalid),
    }
}

pub(super) fn generate_signed(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    p: &Plan,
    identity: &IdentityKeyPair,
    now: u64,
) -> CryptoResult<Option<SignedPreKey>> {
    if !p.rotate_signed {
        return Ok(None);
    }
    let key = RecordKey::new(
        &own.generation_id,
        RecordKind::SignedPreKey,
        &p.signed_id.to_string(),
    )?;
    if tx.get(&key)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    let mut rng = rand::rng();
    let pair = KeyPair::generate(&mut rng);
    let signature = identity
        .private_key()
        .calculate_signature(&pair.public_key.serialize(), &mut rng)
        .map_err(|_| CryptoError::Invalid)?;
    let record = SignedPreKeyRecord::new(
        p.signed_id.into(),
        Timestamp::from_epoch_millis(now),
        &pair,
        &signature,
    );
    let private = Zeroizing::new(record.serialize()?);
    let frame = Zeroizing::new([&[1, 0, 0], private.as_slice()].concat());
    prekey_state::decode(RecordKind::SignedPreKey, p.signed_id, &frame)?;
    tx.put(&key, &frame)?;
    Ok(Some(SignedPreKey {
        key_id: p.signed_id,
        public_key: record.public_key()?.serialize().to_vec(),
        signature: record.signature()?,
        created_at_unix_ms: now as i64,
    }))
}

// Retained signed/LR records must remain live with their original classification.
// A consumed PQ tombstone can never stand in for a reusable last-resort key.
pub(super) fn validate_request(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    p: &Plan,
    request: &UploadPreKeysRequest,
    identity: &IdentityKeyPair,
) -> CryptoResult<()> {
    if !request.one_time_pre_keys.is_empty()
        || request.signed_pre_key.is_some() != p.rotate_signed
        || request.kyber_pre_keys.len() != usize::from(p.rotate_last_resort)
    {
        return Err(CryptoError::Invalid);
    }
    if let Some(k) = &request.signed_pre_key {
        if k.key_id != p.signed_id
            || !identity
                .public_key()
                .verify_signature(&k.public_key, &k.signature)
        {
            return Err(CryptoError::Invalid);
        }
        let bytes = tx
            .get(&RecordKey::new(
                &own.generation_id,
                RecordKind::SignedPreKey,
                &k.key_id.to_string(),
            )?)?
            .ok_or(CryptoError::Invalid)?;
        let prekey_state::Saved::Live {
            record,
            last_resort: false,
        } = prekey_state::decode(RecordKind::SignedPreKey, k.key_id, &bytes)?
        else {
            return Err(CryptoError::Invalid);
        };
        let record = SignedPreKeyRecord::deserialize(record)?;
        if record.public_key()?.serialize().as_ref() != k.public_key
            || record.signature()? != k.signature
            || record.timestamp()?.epoch_millis() != k.created_at_unix_ms as u64
        {
            return Err(CryptoError::Invalid);
        }
    }
    if let Some(k) = request.kyber_pre_keys.first() {
        if k.key_id != p.pq_first
            || !k.is_last_resort
            || !identity
                .public_key()
                .verify_signature(&k.public_key, &k.signature)
        {
            return Err(CryptoError::Invalid);
        }
        let bytes = tx
            .get(&RecordKey::new(
                &own.generation_id,
                RecordKind::PqPreKey,
                &k.key_id.to_string(),
            )?)?
            .ok_or(CryptoError::Invalid)?;
        let prekey_state::Saved::Live {
            record,
            last_resort: true,
        } = prekey_state::decode(RecordKind::PqPreKey, k.key_id, &bytes)?
        else {
            return Err(CryptoError::Invalid);
        };
        let record = KyberPreKeyRecord::deserialize(record)?;
        if record.public_key()?.serialize().as_ref() != k.public_key
            || record.signature()? != k.signature
        {
            return Err(CryptoError::Invalid);
        }
    }
    Ok(())
}
