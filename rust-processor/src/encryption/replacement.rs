//! Explicit replacement journal, separate from ordinary registration/login. No
//! password is stored. Unknown outcomes reuse the same credential/material/CAS.
use super::*;

fn key() -> CryptoResult<AccountKey> {
    Ok(AccountKey::new(AccountKind::Registration, "replacement")?)
}
pub(super) fn load(tx: &mut ProtectedTransaction<'_>) -> CryptoResult<Option<Pending>> {
    let pending = load_pending_at(tx, &key()?)?;
    if pending
        .as_ref()
        .is_some_and(|p| p.expected_generation_id.is_empty())
    {
        return Err(CryptoError::Invalid);
    }
    Ok(pending)
}
pub(super) fn check_current(pending: &Pending, current: Option<&Pending>) -> CryptoResult<()> {
    let current = current.ok_or(CryptoError::Conflict)?;
    if pending.request != current.request
        || pending.bound != current.bound
        || pending.expected_generation_id != current.expected_generation_id
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}

/// Public immutable RPC material plus a native-only secret. No password, Debug,
/// serialization or renderer constructor. The persisted journal is authoritative.
pub struct ReplacementAttempt {
    request: RegisterDeviceRequest,
    expected_generation_id: String,
    credential: Zeroizing<[u8; 32]>,
}
impl ReplacementAttempt {
    pub fn request(&self) -> &RegisterDeviceRequest {
        &self.request
    }
    pub fn expected_generation_id(&self) -> &str {
        &self.expected_generation_id
    }
    pub(crate) fn credential(&self) -> &[u8; 32] {
        &self.credential
    }
}
fn replacement_attempt(
    tx: &mut ProtectedTransaction<'_>,
    pending: &Pending,
) -> CryptoResult<ReplacementAttempt> {
    let prepared = attempt(tx, pending)?;
    Ok(ReplacementAttempt {
        request: prepared.request,
        expected_generation_id: pending.expected_generation_id.clone(),
        credential: prepared.credential,
    })
}
fn archive_account(tx: &mut ProtectedTransaction<'_>, pending: &Pending) -> CryptoResult<()> {
    let id = &pending
        .request
        .as_ref()
        .ok_or(CryptoError::Invalid)?
        .registration_request_id;
    let archive = AccountKey::new(AccountKind::Registration, &format!("archived:{id}"))?;
    if tx.get_account(&archive)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    tx.put_account(&archive, &Zeroizing::new(pending.encode_to_vec()))?;
    Ok(())
}

#[derive(Message)]
struct ArchivedDevice {
    #[prost(message, optional, tag = "1")]
    previous: Option<OwnDeviceState>,
    #[prost(message, optional, tag = "2")]
    replaced_by: Option<DeviceRef>,
}

/// Read-only public locator. No credential, private journal, worker, adoption or
/// replacement authority is returned. The marker and identity must agree.
pub(crate) fn read_archived_device(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
) -> CryptoResult<DeviceRef> {
    check_owner(tx, own)?;
    let bytes = tx
        .get(&RecordKey::new(
            &own.generation_id,
            RecordKind::Control,
            ARCHIVED_DEVICE_RECORD,
        )?)?
        .ok_or(CryptoError::ArchivedGeneration)?;
    if bytes.len() > 2048 {
        return Err(CryptoError::Invalid);
    }
    let archive = ArchivedDevice::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let previous = archive.previous.ok_or(CryptoError::Invalid)?;
    super::super::native::validate_own(&previous).map_err(|_| CryptoError::Invalid)?;
    let device = previous.active_device.ok_or(CryptoError::Invalid)?;
    let replaced_by = archive.replaced_by.ok_or(CryptoError::Invalid)?;
    check_owner(tx, &replaced_by)?;
    if device.reference.as_ref() != Some(own)
        || previous.user_id != own.user_id
        || replaced_by == *own
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
        || identity[1..5] != device.registration_id.to_be_bytes()
        || IdentityKeyPair::try_from(&identity[5..])?
            .public_key()
            .serialize()
            .as_ref()
            != device.identity_key
    {
        return Err(CryptoError::Conflict);
    }
    Ok(replaced_by)
}

impl DeviceProvisioning {
    /// Explicit action using the displayed expected generation. Same CAS target
    /// resumes exact material. A different target requires the pending request ID
    /// as an additional local CAS: archive it rather than silently rebasing it.
    pub(crate) fn prepare_replacement(
        &mut self,
        expected: &str,
        superseded_request_id: Option<&str>,
    ) -> CryptoResult<ReplacementAttempt> {
        if !uuid4(expected) {
            return Err(CryptoError::Invalid);
        }
        self.store.transaction_with(|tx| {
            let current = validated_current(tx)?;
            if let Some(saved) = load(tx)? {
                if saved.bound.is_some() {
                    check_current(&saved, current.as_ref())?;
                }
                if saved.expected_generation_id == expected {
                    if superseded_request_id.is_some_and(|id| {
                        saved
                            .request
                            .as_ref()
                            .is_none_or(|r| r.registration_request_id != id)
                    }) {
                        return Err(CryptoError::Conflict);
                    }
                    return replacement_attempt(tx, &saved);
                }
                if saved.bound.is_none() {
                    if superseded_request_id
                        != saved
                            .request
                            .as_ref()
                            .map(|r| r.registration_request_id.as_str())
                    {
                        return Err(CryptoError::Conflict);
                    }
                    archive_account(tx, &saved)?;
                } else if superseded_request_id.is_some() {
                    return Err(CryptoError::Conflict);
                }
            } else if superseded_request_id.is_some() {
                return Err(CryptoError::Conflict);
            }
            let mut pending = generate()?;
            pending.expected_generation_id = expected.into();
            if current.as_ref().is_some_and(|p| {
                p.request.as_ref().is_some_and(|r| {
                    pending.request.as_ref().is_some_and(|new| {
                        new.identity_key == r.identity_key
                            || new.credential_hash == r.credential_hash
                    })
                })
            }) {
                return Err(CryptoError::Conflict);
            }
            tx.put_account(&key()?, &Zeroizing::new(pending.encode_to_vec()))?;
            replacement_attempt(tx, &pending)
        })
    }
    pub fn replacement_attempt(&mut self) -> CryptoResult<ReplacementAttempt> {
        self.store.transaction_with(|tx| {
            let current = validated_current(tx)?;
            let pending = load(tx)?.ok_or(CryptoError::Invalid)?;
            if pending.bound.is_some() {
                check_current(&pending, current.as_ref())?;
            }
            replacement_attempt(tx, &pending)
        })
    }
    pub(crate) fn bind_replacement(
        &mut self,
        request_id: &str,
        receipt: &OwnDeviceState,
    ) -> CryptoResult<()> {
        super::super::native::validate_own(receipt).map_err(|_| CryptoError::Invalid)?;
        self.store.transaction_with(|tx| {
            let current = validated_current(tx)?;
            let mut pending = load(tx)?.ok_or(CryptoError::Invalid)?;
            let request = pending.request.as_ref().ok_or(CryptoError::Invalid)?;
            let device = receipt.active_device.as_ref().ok_or(CryptoError::Invalid)?;
            let own = device.reference.as_ref().ok_or(CryptoError::Invalid)?;
            check_owner(tx, own)?;
            if request.registration_request_id != request_id
                || receipt.user_id != own.user_id
                || device.identity_key != request.identity_key
                || device.registration_id != request.registration_id
                || own.generation_id == pending.expected_generation_id
            {
                return Err(CryptoError::Conflict);
            }
            if let Some(bound) = &pending.bound {
                if bound != own {
                    return Err(CryptoError::Conflict);
                }
                check_current(&pending, current.as_ref())?;
                attempt(tx, &pending)?;
                load_progress(tx, own)?;
                return Ok(());
            }
            if device.public_material_ready
                || device.public_material_revision != 0
                || tx.has_generation(&own.generation_id)?
            {
                return Err(CryptoError::Conflict);
            }
            if let Some(previous) = &current {
                if let Some(old) = &previous.bound {
                    let archive = RecordKey::new(
                        &old.generation_id,
                        RecordKind::Control,
                        ARCHIVED_DEVICE_RECORD,
                    )?;
                    if tx.get(&archive)?.is_some() {
                        return Err(CryptoError::Conflict);
                    }
                    let previous = load_active(tx)?;
                    tx.put(
                        &archive,
                        &ArchivedDevice {
                            previous: Some(previous),
                            replaced_by: Some(own.clone()),
                        }
                        .encode_to_vec(),
                    )?;
                }
                // Keep the original journal too, including any unbound private
                // material from a previous unknown registration outcome.
                archive_account(tx, previous)?;
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
            let bytes = Zeroizing::new(pending.encode_to_vec());
            tx.put_account(&account_key(AccountKind::Registration)?, &bytes)?;
            tx.put_account(&key()?, &bytes)?;
            Ok(())
        })
    }
}

#[cfg(test)]
#[path = "replacement_tests.rs"]
mod tests;
