//! One durable publication at a time: EC/PQ refill or signed/LR rotation.
//! Private material lives only in prekey records, never in the replay journal.
use super::*;
use crate::encryption::prekey_maintenance::{
    InventoryRefill, RefillPolicy, VerifiedPreKeyInventory,
};
use crate::encryption::{provisioning, publication};
use crate::proto::e2ee::{KyberPreKey, OneTimePreKey, UploadPreKeysRequest, UploadReceipt};
use libsignal_protocol::{KeyPair, Timestamp};

const HEAD: &str = "prekey-refill:head:v1";
#[path = "prekey_rotation.rs"]
mod rotation;
#[derive(Message)]
struct Plan {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    operation: String,
    #[prost(string, tag = "3")]
    previous: String,
    #[prost(uint32, tag = "4")]
    ec_count: u32,
    #[prost(uint32, tag = "5")]
    pq_count: u32,
    #[prost(string, tag = "6")]
    ec_operation: String,
    #[prost(string, tag = "7")]
    pq_operation: String,
    #[prost(uint32, tag = "8")]
    ec_first: u32,
    #[prost(uint32, tag = "9")]
    pq_first: u32,
    #[prost(uint64, tag = "10")]
    base_revision: u64,
    #[prost(message, repeated, tag = "11")]
    requests: Vec<UploadPreKeysRequest>,
    #[prost(message, repeated, tag = "12")]
    receipts: Vec<UploadReceipt>,
    #[prost(string, tag = "13")]
    signed_operation: String,
    #[prost(uint32, tag = "14")]
    signed_id: u32,
    #[prost(bool, tag = "15")]
    rotate_signed: bool,
    #[prost(bool, tag = "16")]
    rotate_last_resort: bool,
}
impl Plan {
    fn complete(&self) -> bool {
        !self.requests.is_empty() && self.requests.len() == self.receipts.len()
    }
    fn view(&self) -> PreKeyRefillPlan {
        PreKeyRefillPlan {
            operation_id: self.operation.clone(),
            ec_count: self.ec_count,
            pq_count: self.pq_count,
            rotate_signed: self.rotate_signed,
            rotate_last_resort: self.rotate_last_resort,
            materialized: !self.requests.is_empty(),
            complete: self.complete(),
        }
    }
    fn revision(&self) -> u64 {
        self.base_revision + self.receipts.len() as u64
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreKeyPublicationPlan {
    pub operation_id: String,
    pub ec_count: u32,
    pub pq_count: u32,
    pub rotate_signed: bool,
    pub rotate_last_resort: bool,
    pub materialized: bool,
    pub complete: bool,
}
// Keep existing native refill callers on the same publication pipeline.
pub type PreKeyRefillPlan = PreKeyPublicationPlan;
pub type PreKeyRotationPlan = PreKeyPublicationPlan;
/// Only the protected owner can mint an upload tied to committed private keys.
#[derive(Clone)]
pub struct PreKeyRefillUpload {
    scope: StoreScope,
    own: DeviceRef,
    operation: String,
    request: UploadPreKeysRequest,
}
impl PreKeyRefillUpload {
    pub fn request(&self) -> &UploadPreKeysRequest {
        &self.request
    }
    pub(in crate::encryption) fn scope(&self) -> &StoreScope {
        &self.scope
    }
    pub(in crate::encryption) fn own(&self) -> &DeviceRef {
        &self.own
    }
}
pub struct VerifiedPreKeyRefillUpload {
    upload: PreKeyRefillUpload,
    receipt: UploadReceipt,
}
impl VerifiedPreKeyRefillUpload {
    pub(in crate::encryption) fn from_rpc(
        upload: PreKeyRefillUpload,
        receipt: UploadReceipt,
    ) -> Self {
        Self { upload, receipt }
    }
}
fn head_key(own: &DeviceRef) -> Result<RecordKey, StoreError> {
    RecordKey::new(&own.generation_id, RecordKind::Control, HEAD)
}
fn plan_key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    RecordKey::new(id, RecordKind::Control, "operation")?;
    Ok(RecordKey::new(
        &own.generation_id,
        RecordKind::Publication,
        &format!("refill:{id}"),
    )?)
}
fn counts(ec: u32, pq: u32) -> CryptoResult<()> {
    if ec > 100 || pq > 100 || ec + pq == 0 {
        return Err(CryptoError::Invalid);
    }
    Ok(())
}
fn identity(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<IdentityKeyPair> {
    let bytes = tx
        .get(&RecordKey::new(
            &own.generation_id,
            RecordKind::Identity,
            "local",
        )?)?
        .ok_or(CryptoError::Invalid)?;
    if bytes.len() < 6 || bytes[0] != 1 {
        return Err(CryptoError::Invalid);
    }
    Ok(IdentityKeyPair::try_from(&bytes[5..])?)
}
fn load(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef, id: &str) -> CryptoResult<Plan> {
    let bytes = tx.get(&plan_key(own, id)?)?.ok_or(CryptoError::Invalid)?;
    let p = Plan::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    rotation::validate_shape(&p)?;
    if p.operation != id
        || p.base_revision == 0
        || p.base_revision >= i64::MAX as u64 - 8
        || p.requests.len() > 8
        || p.receipts.len() > p.requests.len()
        || p.previous == p.operation
    {
        return Err(CryptoError::Invalid);
    }
    if !p.previous.is_empty() {
        plan_key(own, &p.previous)?;
    }
    for (count, first, operation, namespace) in [
        (
            p.ec_count,
            p.ec_first,
            &p.ec_operation,
            PreKeyIdNamespace::Ec,
        ),
        (
            p.pq_count + u32::from(p.rotate_last_resort),
            p.pq_first,
            &p.pq_operation,
            PreKeyIdNamespace::Pq,
        ),
        (
            u32::from(p.rotate_signed),
            p.signed_id,
            &p.signed_operation,
            PreKeyIdNamespace::Signed,
        ),
    ] {
        if count == 0 {
            if first != 0 || !operation.is_empty() {
                return Err(CryptoError::Invalid);
            }
        } else {
            let reservation =
                prekey_ids::reserved_in_tx(tx, &own.generation_id, operation, namespace, count)?
                    .ok_or(CryptoError::Invalid)?;
            if reservation.first != first {
                return Err(CryptoError::Invalid);
            }
        }
    }
    let mut reservations = std::collections::HashSet::new();
    for operation in [&p.ec_operation, &p.pq_operation, &p.signed_operation] {
        if !operation.is_empty() && !reservations.insert(operation) {
            return Err(CryptoError::Invalid);
        }
    }
    let mut ec = 0u64;
    let mut pq = 0u64;
    let identity = identity(tx, own)?;
    let mut batches = std::collections::HashSet::new();
    for (index, request) in p.requests.iter().enumerate() {
        publication::validate_upload(request).map_err(|_| CryptoError::Invalid)?;
        if request.context.as_ref()
            != Some(&ClientContext {
                protocol_version: 1,
                device_generation_id: own.generation_id.clone(),
            })
            || request.expected_revision != p.base_revision + index as u64
            || !batches.insert(&request.batch_id)
        {
            return Err(CryptoError::Invalid);
        }
        if p.format == 2 {
            rotation::validate_request(tx, own, &p, request, &identity)?;
            if let Some(receipt) = p.receipts.get(index) {
                publication::validate_upload_receipt(request, receipt)
                    .map_err(|_| CryptoError::Invalid)?;
            }
            continue;
        }
        if request.signed_pre_key.is_some() {
            return Err(CryptoError::Invalid);
        }
        for k in &request.one_time_pre_keys {
            if u64::from(k.key_id) != u64::from(p.ec_first) + ec || ec >= u64::from(p.ec_count) {
                return Err(CryptoError::Invalid);
            }
            ec += 1;
            let saved = tx
                .get(&RecordKey::new(
                    &own.generation_id,
                    RecordKind::EcPreKey,
                    &k.key_id.to_string(),
                )?)?
                .ok_or(CryptoError::Invalid)?;
            if let prekey_state::Saved::Live { record, .. } =
                prekey_state::decode(RecordKind::EcPreKey, k.key_id, &saved)?
                && PreKeyRecord::deserialize(record)?
                    .public_key()?
                    .serialize()
                    .as_ref()
                    != k.public_key
            {
                return Err(CryptoError::Invalid);
            }
        }
        for k in &request.kyber_pre_keys {
            if k.is_last_resort
                || u64::from(k.key_id) != u64::from(p.pq_first) + pq
                || pq >= u64::from(p.pq_count)
            {
                return Err(CryptoError::Invalid);
            }
            if !identity
                .public_key()
                .verify_signature(&k.public_key, &k.signature)
            {
                return Err(CryptoError::Invalid);
            }
            pq += 1;
            let saved = tx
                .get(&RecordKey::new(
                    &own.generation_id,
                    RecordKind::PqPreKey,
                    &k.key_id.to_string(),
                )?)?
                .ok_or(CryptoError::Invalid)?;
            if let prekey_state::Saved::Live {
                record,
                last_resort,
            } = prekey_state::decode(RecordKind::PqPreKey, k.key_id, &saved)?
            {
                let record = KyberPreKeyRecord::deserialize(record)?;
                if last_resort
                    || record.public_key()?.serialize().as_ref() != k.public_key
                    || record.signature()? != k.signature
                {
                    return Err(CryptoError::Invalid);
                }
            }
        }
        if let Some(receipt) = p.receipts.get(index) {
            publication::validate_upload_receipt(request, receipt)
                .map_err(|_| CryptoError::Invalid)?;
        }
    }
    if !p.requests.is_empty() && (ec != u64::from(p.ec_count) || pq != u64::from(p.pq_count)) {
        return Err(CryptoError::Invalid);
    }
    Ok(p)
}
fn load_head(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<Option<Plan>> {
    let Some(bytes) = tx.get(&head_key(own)?)? else {
        return Ok(None);
    };
    let id = std::str::from_utf8(&bytes).map_err(|_| CryptoError::Invalid)?;
    Ok(Some(load(tx, own, id)?))
}
fn save(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef, p: &Plan) -> CryptoResult<()> {
    tx.put(&plan_key(own, &p.operation)?, &p.encode_to_vec())?;
    Ok(())
}

pub(in crate::encryption) struct PublicationProgress {
    pub revision: u64,
    pub complete: bool,
    pub may_be_in_flight: bool,
}
pub(in crate::encryption) fn progress(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    initial: u64,
) -> CryptoResult<PublicationProgress> {
    let Some(p) = load_head(tx, own)? else {
        return Ok(PublicationProgress {
            revision: initial,
            complete: true,
            may_be_in_flight: false,
        });
    };
    if p.previous.is_empty() {
        if p.base_revision != initial {
            return Err(CryptoError::Invalid);
        }
    } else {
        let old = load(tx, own, &p.previous)?;
        if !old.complete() || old.revision() != p.base_revision || old.base_revision < initial {
            return Err(CryptoError::Invalid);
        }
    }
    Ok(PublicationProgress {
        revision: p.revision(),
        complete: p.complete(),
        may_be_in_flight: !p.requests.is_empty() && !p.complete(),
    })
}

// Reservation authority comes from the durable parent/child journal, never from
// a caller-supplied record or the generic install callback. All bytes commit with
// the public upload plan. A rollback exposes no upload and may regenerate only
// that never-committed material; a committed plan is always replayed unchanged.
pub(in crate::encryption) fn materialize(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
) -> CryptoResult<PreKeyRefillPlan> {
    let mut p = load_head(tx, own)?.ok_or(CryptoError::Conflict)?;
    if p.operation != id {
        return Err(CryptoError::Conflict);
    }
    if !p.requests.is_empty() {
        return Ok(p.view());
    }
    let identity = identity(tx, own)?;
    let now: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| CryptoError::Invalid)?
        .as_millis()
        .try_into()
        .map_err(|_| CryptoError::Invalid)?;
    if now > i64::MAX as u64 {
        return Err(CryptoError::Invalid);
    }
    let mut rng = rand::rng();
    let mut ec = vec![];
    let mut pq = vec![];
    for (namespace, first, count) in [
        (PreKeyIdNamespace::Ec, p.ec_first, p.ec_count),
        (
            PreKeyIdNamespace::Pq,
            p.pq_first,
            p.pq_count + u32::from(p.rotate_last_resort),
        ),
    ] {
        for offset in 0..count {
            let id = first.checked_add(offset).ok_or(CryptoError::Invalid)?;
            let kind = if namespace == PreKeyIdNamespace::Ec {
                RecordKind::EcPreKey
            } else {
                RecordKind::PqPreKey
            };
            let key = RecordKey::new(&own.generation_id, kind, &id.to_string())?;
            if tx.get(&key)?.is_some() {
                return Err(CryptoError::Conflict);
            }
            let private = if namespace == PreKeyIdNamespace::Ec {
                let key = PreKeyRecord::new(id.into(), &KeyPair::generate(&mut rng));
                ec.push(OneTimePreKey {
                    key_id: id,
                    public_key: key.public_key()?.serialize().to_vec(),
                });
                Zeroizing::new(key.serialize()?)
            } else {
                let pair = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut rng);
                let signature = identity
                    .private_key()
                    .calculate_signature(&pair.public_key.serialize(), &mut rng)
                    .map_err(|_| CryptoError::Invalid)?;
                let key = KyberPreKeyRecord::new(
                    id.into(),
                    Timestamp::from_epoch_millis(now),
                    &pair,
                    &signature,
                );
                pq.push(KyberPreKey {
                    key_id: id,
                    public_key: key.public_key()?.serialize().to_vec(),
                    signature: key.signature()?,
                    is_last_resort: p.rotate_last_resort,
                });
                Zeroizing::new(key.serialize()?)
            };
            let frame = Zeroizing::new(
                [
                    &[
                        1,
                        0,
                        u8::from(kind == RecordKind::PqPreKey && p.rotate_last_resort),
                    ],
                    private.as_slice(),
                ]
                .concat(),
            );
            prekey_state::decode(kind, id, &frame)?;
            tx.put(&key, &frame)?;
        }
    }
    let signed = rotation::generate_signed(tx, own, &p, &identity, now)?;
    p.requests = publication::prepare_upload_batches(
        ClientContext {
            protocol_version: 1,
            device_generation_id: own.generation_id.clone(),
        },
        p.base_revision,
        signed,
        ec,
        pq,
    )
    .map_err(|_| CryptoError::Invalid)?;
    save(tx, own, &p)?;
    Ok(p.view())
}
pub(in crate::encryption) fn next_upload(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
) -> CryptoResult<Option<UploadPreKeysRequest>> {
    let Some(p) = load_head(tx, own)? else {
        return Ok(None);
    };
    if p.requests.is_empty() {
        materialize(tx, own, &p.operation)?;
    }
    let p = load(tx, own, &p.operation)?;
    Ok(p.requests.get(p.receipts.len()).cloned())
}
pub(in crate::encryption) fn probe(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
) -> CryptoResult<Option<UploadPreKeysRequest>> {
    let Some(p) = load_head(tx, own)? else {
        return Ok(None);
    };
    if !p.complete() {
        return Err(CryptoError::Conflict);
    }
    Ok(p.requests.last().cloned())
}
pub(in crate::encryption) fn acknowledge(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    receipt: &UploadReceipt,
) -> CryptoResult<()> {
    let mut p = load_head(tx, own)?.ok_or(CryptoError::Conflict)?;
    if let Some(saved) = p.receipts.iter().find(|r| r.batch_id == receipt.batch_id) {
        return if saved == receipt {
            Ok(())
        } else {
            Err(CryptoError::Conflict)
        };
    }
    let request = p
        .requests
        .get(p.receipts.len())
        .ok_or(CryptoError::Conflict)?;
    publication::validate_upload_receipt(request, receipt).map_err(|_| CryptoError::Conflict)?;
    provisioning::mark_publication(tx, own, request, receipt)?;
    p.receipts.push(receipt.clone());
    save(tx, own, &p)
}

fn prepare_in_tx(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    ec: u32,
    pq: u32,
    signed: bool,
    last_resort: bool,
    previous: Option<&str>,
) -> CryptoResult<PreKeyPublicationPlan> {
    let initial = provisioning::initial_revision(tx, own)?;
    let publication = progress(tx, own, initial)?;
    provisioning::check_publication_revision(tx, own, publication.revision)?;
    let head = load_head(tx, own)?;
    if let Some(head) = &head {
        if previous.is_none() || previous == Some(head.previous.as_str()) {
            if head.ec_count != ec
                || head.pq_count != pq
                || head.rotate_signed != signed
                || head.rotate_last_resort != last_resort
            {
                return Err(CryptoError::Conflict);
            }
            return Ok(head.view());
        }
        if previous != Some(head.operation.as_str()) || !head.complete() {
            return Err(CryptoError::Conflict);
        }
    } else if previous.is_some() {
        return Err(CryptoError::Conflict);
    }
    let mut p = Plan {
        format: if signed || last_resort { 2 } else { 1 },
        operation: uuid::Uuid::new_v4().to_string(),
        previous: head.map(|h| h.operation).unwrap_or_default(),
        ec_count: ec,
        pq_count: pq,
        ec_operation: String::new(),
        pq_operation: String::new(),
        ec_first: 0,
        pq_first: 0,
        base_revision: publication.revision,
        requests: vec![],
        receipts: vec![],
        signed_operation: String::new(),
        signed_id: 0,
        rotate_signed: signed,
        rotate_last_resort: last_resort,
    };
    if p.base_revision >= i64::MAX as u64 - 8 {
        return Err(CryptoError::Invalid);
    }
    for (namespace, count, operation, first) in [
        (
            PreKeyIdNamespace::Ec,
            ec,
            &mut p.ec_operation,
            &mut p.ec_first,
        ),
        (
            PreKeyIdNamespace::Pq,
            pq + u32::from(last_resort),
            &mut p.pq_operation,
            &mut p.pq_first,
        ),
        (
            PreKeyIdNamespace::Signed,
            u32::from(signed),
            &mut p.signed_operation,
            &mut p.signed_id,
        ),
    ] {
        if count > 0 {
            *operation = uuid::Uuid::new_v4().to_string();
            *first =
                prekey_ids::reserve_in_tx(tx, &own.generation_id, operation, namespace, count)?
                    .first;
        }
    }
    save(tx, own, &p)?;
    tx.put(&head_key(own)?, p.operation.as_bytes())?;
    Ok(p.view())
}

impl Engine {
    pub(super) fn refill_from_inventory(
        &mut self,
        proof: VerifiedPreKeyInventory,
        policy: RefillPolicy,
    ) -> CryptoResult<InventoryRefill> {
        if !policy.valid() {
            return Err(CryptoError::Invalid);
        }
        let v = proof.value();
        if proof.scope() != self.store.scope() || v.own_device.as_ref() != Some(&self.own) {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            let initial = provisioning::initial_revision(tx, &self.own)?;
            let publication = progress(tx, &self.own, initial)?;
            provisioning::check_publication_revision(tx, &self.own, publication.revision)?;
            let head = load_head(tx, &self.own)?;
            // A concurrent explicit refill/rotation owns the shared slot. Resume
            // it even when its unknown remote COMMIT is ahead of this inventory.
            if let Some(p) = head.as_ref().filter(|p| !p.complete()) {
                return Ok(InventoryRefill::Publication(p.view()));
            }
            if !v.public_material_ready || v.publication_revision != publication.revision {
                return Err(CryptoError::Conflict);
            }
            for (name, expected) in [
                ("active-signed", v.active_signed_id),
                ("active-last-resort", v.active_last_resort_id),
            ] {
                let value = tx
                    .get(&RecordKey::new(
                        &self.own.generation_id,
                        RecordKind::Control,
                        name,
                    )?)?
                    .ok_or(CryptoError::Invalid)?;
                if Some(u32::from_be_bytes(
                    value
                        .as_slice()
                        .try_into()
                        .map_err(|_| CryptoError::Invalid)?,
                )) != expected
                {
                    return Err(CryptoError::Conflict);
                }
            }
            let Some((ec, pq)) = policy.amounts(v) else {
                return Ok(InventoryRefill::Healthy);
            };
            if ec + pq == 0 {
                return Ok(InventoryRefill::CapacityExhausted);
            }
            let previous = head.as_ref().map(|p| p.operation.as_str());
            Ok(InventoryRefill::Publication(prepare_in_tx(
                tx, &self.own, ec, pq, false, false, previous,
            )?))
        })
    }
    pub(super) fn prepare_prekey_refill(
        &mut self,
        ec: u32,
        pq: u32,
        previous: Option<&str>,
    ) -> CryptoResult<PreKeyRefillPlan> {
        counts(ec, pq)?;
        self.prepare_prekey_publication(ec, pq, false, false, previous)
    }
    pub(super) fn prepare_prekey_rotation(
        &mut self,
        signed: bool,
        last_resort: bool,
        previous: Option<&str>,
    ) -> CryptoResult<PreKeyRotationPlan> {
        if !signed && !last_resort {
            return Err(CryptoError::Invalid);
        }
        self.prepare_prekey_publication(0, 0, signed, last_resort, previous)
    }
    fn prepare_prekey_publication(
        &mut self,
        ec: u32,
        pq: u32,
        signed: bool,
        last_resort: bool,
        previous: Option<&str>,
    ) -> CryptoResult<PreKeyPublicationPlan> {
        if let Some(previous) = previous {
            plan_key(&self.own, previous)?;
        }
        self.store.transaction_with(|tx| {
            prepare_in_tx(tx, &self.own, ec, pq, signed, last_resort, previous)
        })
    }
    pub(super) fn current_prekey_publication(
        &mut self,
    ) -> CryptoResult<Option<PreKeyPublicationPlan>> {
        self.store.transaction_with(|tx| {
            let initial = provisioning::initial_revision(tx, &self.own)?;
            let publication = progress(tx, &self.own, initial)?;
            provisioning::check_publication_revision(tx, &self.own, publication.revision)?;
            Ok(load_head(tx, &self.own)?.map(|p| p.view()))
        })
    }
    pub(super) fn materialize_prekey_refill(&mut self, id: &str) -> CryptoResult<PreKeyRefillPlan> {
        self.store
            .transaction_with(|tx| materialize(tx, &self.own, id))
    }
    pub(super) fn next_prekey_refill_upload(&mut self) -> CryptoResult<Option<PreKeyRefillUpload>> {
        let scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let Some(p) = load_head(tx, &self.own)? else {
                return Ok(None);
            };
            if p.requests.is_empty() {
                return Err(CryptoError::Conflict);
            }
            Ok(p.requests
                .get(p.receipts.len())
                .cloned()
                .map(|request| PreKeyRefillUpload {
                    scope,
                    own: self.own.clone(),
                    operation: p.operation,
                    request,
                }))
        })
    }
    pub(super) fn accept_prekey_refill_upload(
        &mut self,
        proof: VerifiedPreKeyRefillUpload,
    ) -> CryptoResult<()> {
        if proof.upload.scope != *self.store.scope() || proof.upload.own != self.own {
            return Err(CryptoError::Conflict);
        }
        publication::validate_upload_receipt(&proof.upload.request, &proof.receipt)
            .map_err(|_| CryptoError::Conflict)?;
        self.store.transaction_with(|tx| {
            let p = load(tx, &self.own, &proof.upload.operation)?;
            if !p.requests.contains(&proof.upload.request) {
                return Err(CryptoError::Conflict);
            }
            if let Some(saved) = p
                .receipts
                .iter()
                .find(|r| r.batch_id == proof.receipt.batch_id)
            {
                return if saved == &proof.receipt {
                    Ok(())
                } else {
                    Err(CryptoError::Conflict)
                };
            }
            acknowledge(tx, &self.own, &proof.receipt)
        })
    }
}
