//! Durable upload intent/observation CAS on the existing per-account crypto lane.
//! No ratchet/outbox promotion, upload bytes, clock-based renewal or Send proof.
use super::*;
use crate::encryption::{blob_control as boundary, native::EpochBlobLease};

#[derive(Clone, Message)]
pub(super) struct Predecessor {
    #[prost(message, optional, tag = "1")]
    request: Option<v2::BlobLeaseRequest>,
    #[prost(message, optional, tag = "2")]
    lease: Option<v2::BlobLease>,
}
/// Committed protected intent plus a fresh lifecycle check. No constructor,
/// Debug, serde or private descriptor/key/source-fingerprint getter.
#[derive(Clone)]
pub struct EpochBlobControlPlan {
    scope: StoreScope,
    own: DeviceRef,
    request: v2::BlobLeaseRequest,
    cohort_pin: [u8; 32],
}
impl EpochBlobControlPlan {
    pub fn request(&self) -> &v2::BlobLeaseRequest {
        &self.request
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.scope
    }
    #[cfg(test)]
    pub(in crate::encryption) fn test_plan(
        scope: StoreScope,
        request: v2::BlobLeaseRequest,
    ) -> Self {
        Self {
            own: request.sender.clone().unwrap(),
            scope,
            request,
            cohort_pin: [42; 32],
        }
    }
}
/// Result of merging a validated observation in the protected transaction.
/// This is the saved state (possibly newer than the reply), never a Send receipt.
pub struct EpochBlobProgress {
    plan: EpochBlobControlPlan,
    lease: v2::BlobLease,
    linked_seen: bool,
}
impl EpochBlobProgress {
    pub fn plan(&self) -> &EpochBlobControlPlan {
        &self.plan
    }
    pub fn lease(&self) -> &v2::BlobLease {
        &self.lease
    }
    pub fn linked_seen(&self) -> bool {
        self.linked_seen
    }
}
/// One fresh transaction binds the saved current grant/request to exact cipher
/// bytes. No constructor/Debug/serde or private key/descriptor getter.
pub struct EpochBlobUpload {
    plan: EpochBlobControlPlan,
    grant: v2::BlobLease,
    blob: EpochAttachmentBlob,
}
impl EpochBlobUpload {
    pub fn plan(&self) -> &EpochBlobControlPlan {
        &self.plan
    }
    pub fn grant(&self) -> &v2::BlobLease {
        &self.grant
    }
    pub(in crate::encryption) fn bytes(&self) -> &[u8] {
        self.blob.as_bytes()
    }
    #[cfg(test)]
    pub(in crate::encryption) fn test_upload(
        plan: EpochBlobControlPlan,
        grant: v2::BlobLease,
        bytes: Vec<u8>,
    ) -> Self {
        let permit = std::sync::Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .unwrap();
        Self {
            plan,
            grant,
            blob: EpochAttachmentBlob {
                bytes,
                _permit: permit,
            },
        }
    }
}
pub(in crate::encryption::signal_state) enum ControlCommand {
    Plan(String, Reply<EpochBlobControlPlan>),
    Accept(Box<EpochBlobLease>, Reply<EpochBlobControlPlan>),
    AcceptProgress(Box<EpochBlobLease>, Reply<EpochBlobProgress>),
    Renew(Box<EpochBlobControlPlan>, Reply<EpochBlobControlPlan>),
}
fn payload_request(
    p: &v2::EncryptedPayload,
    operation: String,
) -> CryptoResult<v2::BlobLeaseRequest> {
    let r = v2::BlobLeaseRequest {
        context: Some(ClientContext {
            protocol_version: 2,
            device_generation_id: p
                .sender
                .as_ref()
                .ok_or(CryptoError::Invalid)?
                .generation_id
                .clone(),
        }),
        message_id: p.message_id.clone(),
        conversation_id: p.conversation_id.clone(),
        crypto_epoch: p.crypto_epoch,
        sender: p.sender.clone(),
        recipient: p.recipient.clone(),
        blob: Some(attachment_payload::blob_reference(part(p)?).map_err(|_| CryptoError::Invalid)?),
        operation_id: operation,
        expected_revision: 0,
        expected_attempt_id: String::new(),
    };
    boundary::validate_request(&r).map_err(|_| CryptoError::Invalid)?;
    Ok(r)
}
pub(super) fn initial_request(p: &v2::EncryptedPayload) -> CryptoResult<v2::BlobLeaseRequest> {
    payload_request(p, uuid::Uuid::new_v4().to_string())
}
fn check_request(p: &v2::EncryptedPayload, r: &v2::BlobLeaseRequest) -> CryptoResult<()> {
    boundary::validate_request(r).map_err(|_| CryptoError::Invalid)?;
    let mut original = r.clone();
    original.expected_revision = 0;
    original.expected_attempt_id.clear();
    if original != payload_request(p, r.operation_id.clone())? {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
pub(super) fn validate_journal(j: &Journal) -> CryptoResult<()> {
    let p = j.payload.as_ref().ok_or(CryptoError::Invalid)?;
    let r = j.operation.as_ref().ok_or(CryptoError::Invalid)?;
    let linked = j.linked_seen.ok_or(CryptoError::Invalid)?;
    check_request(p, r)?;
    match (r.expected_revision, &j.replaces) {
        (0, None) => {}
        (0, Some(_)) | (_, None) => return Err(CryptoError::Invalid),
        (_, Some(old)) => {
            let previous = old.request.as_ref().ok_or(CryptoError::Invalid)?;
            let lease = old.lease.as_ref().ok_or(CryptoError::Invalid)?;
            check_request(p, previous)?;
            boundary::validate_lease(previous, lease).map_err(|_| CryptoError::Invalid)?;
            if !boundary::renewable(lease)
                || r.expected_revision != lease.revision
                || r.expected_attempt_id != lease.attempt_id
                || r.operation_id == previous.operation_id
            {
                return Err(CryptoError::Conflict);
            }
        }
    }
    if let Some(g) = &j.lease {
        boundary::validate_lease(r, g).map_err(|_| CryptoError::Invalid)?;
        if g.state == v2::BlobLeaseState::Linked as i32 && !linked
            || linked && boundary::phase(g.state).unwrap() < 3
        {
            return Err(CryptoError::Invalid);
        }
    } else if linked {
        return Err(CryptoError::Invalid);
    }
    Ok(())
}
pub(super) fn cohort_pin(j: &Journal) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"zchat.e2ee.attachment.control-cohort.v1\0");
    hash.update(&j.source_digest);
    hash.update(Zeroizing::new(j.payload.as_ref().unwrap().encode_to_vec()).as_slice());
    hash.finalize().into()
}
pub(super) fn plan(j: &Journal, own: &DeviceRef, scope: &StoreScope) -> EpochBlobControlPlan {
    EpochBlobControlPlan {
        scope: scope.clone(),
        own: own.clone(),
        request: j.operation.as_ref().unwrap().clone(),
        cohort_pin: cohort_pin(j),
    }
}
fn progress(j: &Journal, own: &DeviceRef, scope: &StoreScope) -> EpochBlobProgress {
    EpochBlobProgress {
        plan: plan(j, own, scope),
        lease: j.lease.as_ref().unwrap().clone(),
        linked_seen: j.linked_seen.unwrap(),
    }
}
fn current_journal(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    message: &str,
) -> CryptoResult<Journal> {
    recovery_indexes::require_ready(tx, own)?;
    let j = load_journal(tx, own, message)?.ok_or(CryptoError::MessageUnavailable)?;
    let p = j.payload.as_ref().unwrap();
    let s =
        current(tx, own, store, &p.conversation_id)?.ok_or(CryptoError::ConversationUnavailable)?;
    if s.epoch != p.crypto_epoch.unwrap()
        || p.sender.as_ref() != Some(&s.own)
        || p.recipient.as_ref() != Some(&s.peer)
    {
        return Err(CryptoError::Conflict);
    }
    // Verify all chunks even for metadata-only requests. No repair/reseal.
    let _ = local_plan(tx, own, store, &j)?;
    Ok(j)
}
pub(super) fn check_plan(
    p: &EpochBlobControlPlan,
    j: &Journal,
    own: &DeviceRef,
    scope: &StoreScope,
) -> CryptoResult<()> {
    if p.own != *own || p.scope != *scope || p.cohort_pin != cohort_pin(j) {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
fn save(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef, j: &Journal) -> CryptoResult<()> {
    validate_journal(j)?;
    tx.put(
        &key(own, &j.payload.as_ref().unwrap().message_id)?,
        &Zeroizing::new(j.encode_to_vec()),
    )?;
    Ok(())
}
// These helpers mutate/read an ALREADY authorized journal. Ordinary preparation
// callers always use current_journal; retained callers have a separate exact
// outbox + authenticated failed Send + current lifecycle gate. No bool bypass.
pub(super) fn merge_lease(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    mut j: Journal,
    observed: EpochBlobLease,
) -> CryptoResult<EpochBlobProgress> {
    if observed.store_scope() != store {
        return Err(CryptoError::Conflict);
    }
    let p = observed.request();
    check_plan(p, &j, own, store)?;
    if j.operation.as_ref() != Some(&p.request) {
        return Err(CryptoError::Conflict);
    }
    let g = observed.response();
    boundary::validate_lease(&p.request, g).map_err(|_| CryptoError::Invalid)?;
    if let Some(old) = &j.lease {
        if old.attempt_id != g.attempt_id
            || old.revision != g.revision
            || old.reserved_at_unix_ms != g.reserved_at_unix_ms
            || old.expires_at_unix_ms != g.expires_at_unix_ms
        {
            return Err(CryptoError::Conflict);
        }
        let (a, b) = (
            boundary::phase(old.state).unwrap(),
            boundary::phase(g.state).unwrap(),
        );
        if b < a {
            if g.observed_at_unix_ms > old.observed_at_unix_ms {
                return Err(CryptoError::Conflict);
            }
            return Ok(progress(&j, own, store));
        }
        if g.observed_at_unix_ms < old.observed_at_unix_ms {
            if b > a {
                return Err(CryptoError::Conflict);
            }
            return Ok(progress(&j, own, store));
        }
        if g == old {
            return Ok(progress(&j, own, store));
        }
    }
    j.linked_seen = Some(j.linked_seen.unwrap() || g.state == v2::BlobLeaseState::Linked as i32);
    j.lease = Some(g.clone());
    save(tx, own, &j)?;
    Ok(progress(&j, own, store))
}
pub(super) fn renew_operation(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    mut j: Journal,
    p: EpochBlobControlPlan,
) -> CryptoResult<EpochBlobControlPlan> {
    check_plan(&p, &j, own, store)?;
    if j.operation.as_ref() != Some(&p.request) {
        if j.replaces.as_ref().and_then(|v| v.request.as_ref()) == Some(&p.request) {
            return Ok(plan(&j, own, store));
        }
        return Err(CryptoError::Conflict);
    }
    let g = j.lease.as_ref().ok_or(CryptoError::Conflict)?;
    if j.linked_seen != Some(false) || !boundary::renewable(g) {
        return Err(CryptoError::Conflict);
    }
    let mut successor = p.request.clone();
    successor.operation_id = uuid::Uuid::new_v4().to_string();
    successor.expected_revision = g.revision;
    successor.expected_attempt_id = g.attempt_id.clone();
    j.replaces = Some(Predecessor {
        request: Some(p.request),
        lease: j.lease.take(),
    });
    j.operation = Some(successor);
    save(tx, own, &j)?;
    Ok(plan(&j, own, store))
}
pub(super) fn upload_source(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    j: &Journal,
    permit: OwnedSemaphorePermit,
) -> CryptoResult<EpochBlobUpload> {
    let grant = j.lease.as_ref().ok_or(CryptoError::Conflict)?;
    if j.linked_seen != Some(false)
        || grant.expired
        || grant.state != v2::BlobLeaseState::Reserved as i32
    {
        return Err(CryptoError::Conflict);
    }
    let r = j.operation.as_ref().unwrap();
    let b = r.blob.as_ref().unwrap();
    let digest: [u8; 32] = b
        .sha256
        .as_slice()
        .try_into()
        .map_err(|_| CryptoError::Invalid)?;
    let bytes = tx.attachment_blob(
        &key(own, &j.payload.as_ref().unwrap().message_id)?,
        &b.blob_id,
        b.ciphertext_bytes as usize,
        &digest,
        true,
    )?;
    Ok(EpochBlobUpload {
        plan: plan(j, own, store),
        grant: grant.clone(),
        blob: EpochAttachmentBlob {
            bytes,
            _permit: permit,
        },
    })
}

impl Engine {
    pub(super) fn attachment_upload(
        &mut self,
        message: &str,
        permit: OwnedSemaphorePermit,
    ) -> CryptoResult<EpochBlobUpload> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let j = current_journal(tx, own, &store, message)?;
            upload_source(tx, own, &store, &j, permit)
        })
    }
    fn attachment_control_plan(&mut self, message: &str) -> CryptoResult<EpochBlobControlPlan> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let j = current_journal(tx, own, &store, message)?;
            Ok(plan(&j, own, &store))
        })
    }
    fn attachment_accept_lease(
        &mut self,
        observed: EpochBlobLease,
    ) -> CryptoResult<EpochBlobProgress> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            if observed.store_scope() != &store {
                return Err(CryptoError::Conflict);
            }
            let j = current_journal(tx, own, &store, &observed.request().request.message_id)?;
            merge_lease(tx, own, &store, j, observed)
        })
    }
    fn attachment_renew_control(
        &mut self,
        p: EpochBlobControlPlan,
    ) -> CryptoResult<EpochBlobControlPlan> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let j = current_journal(tx, own, &store, &p.request.message_id)?;
            renew_operation(tx, own, &store, j, p)
        })
    }
    pub(super) fn handle_attachment_control(
        &mut self,
        c: ControlCommand,
        permit: OwnedSemaphorePermit,
    ) {
        match c {
            ControlCommand::Plan(message, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_control_plan(&message);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            ControlCommand::Accept(observed, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_accept_lease(*observed).map(|p| p.plan);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            ControlCommand::AcceptProgress(observed, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_accept_lease(*observed);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            ControlCommand::Renew(plan, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_renew_control(*plan);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
        }
    }
}
impl CryptoCoordinator {
    pub async fn epoch_blob_upload(&self, message: String) -> CryptoResult<EpochBlobUpload> {
        if !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Upload(
                message, permit, reply,
            )))
        })
        .await
    }
    pub async fn epoch_attachment_control(
        &self,
        message: String,
    ) -> CryptoResult<EpochBlobControlPlan> {
        if !epoch_rpc::uuid4(&message) {
            return Err(CryptoError::Invalid);
        }
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Control(
                Box::new(ControlCommand::Plan(message, reply)),
                permit,
            )))
        })
        .await
    }
    pub async fn accept_epoch_blob_lease(
        &self,
        observed: EpochBlobLease,
    ) -> CryptoResult<EpochBlobControlPlan> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Control(
                Box::new(ControlCommand::Accept(Box::new(observed), reply)),
                permit,
            )))
        })
        .await
    }
    pub async fn accept_epoch_blob_progress(
        &self,
        observed: EpochBlobLease,
    ) -> CryptoResult<EpochBlobProgress> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Control(
                Box::new(ControlCommand::AcceptProgress(Box::new(observed), reply)),
                permit,
            )))
        })
        .await
    }
    /// Explicit CAS after a durable eligible DB observation, never local time
    /// or a generic error. Repeated predecessor returns exactly one successor.
    pub async fn renew_epoch_blob_control(
        &self,
        plan: EpochBlobControlPlan,
    ) -> CryptoResult<EpochBlobControlPlan> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Control(
                Box::new(ControlCommand::Renew(Box::new(plan), reply)),
                permit,
            )))
        })
        .await
    }
}
