//! Durable cache ACK intent. No RPC, renderer entry point or file removal.
use super::*;
use crate::encryption::{file_ack as contract, native::EpochObservation};

const PREFIX: &str = "attachment-download:";
const PAGE_SIZE: usize = 32;

/// Only a protected verified cache AND a fresh history/hash/AEAD read can mint
/// this proof. Public cache locators and pending-page UUIDs cannot substitute.
/// No raw constructor, Debug, serde, private file key or plaintext buffer.
#[derive(Clone)]
pub struct EpochDurableFileAck {
    plan: EpochAttachmentDownloadPlan,
    run: String,
    request: v2::AcknowledgeFilesRequest,
    acknowledged_at: Option<i64>,
}
impl EpochDurableFileAck {
    pub fn message_id(&self) -> &str {
        self.plan.message_id()
    }
    pub fn acknowledged_at_unix_ms(&self) -> Option<i64> {
        self.acknowledged_at
    }
    pub(in crate::encryption) fn request(&self) -> &v2::AcknowledgeFilesRequest {
        &self.request
    }
    pub(in crate::encryption) fn store_scope(&self) -> &StoreScope {
        &self.plan.store
    }
    pub(in crate::encryption) fn own_device(&self) -> &DeviceRef {
        &self.plan.own
    }
}

#[derive(Clone)]
pub struct EpochFileAckCursor {
    store: StoreScope,
    own: DeviceRef,
    after: String,
}
/// Bounded candidate UUIDs, NOT ACK proofs. A short/empty page can still have a
/// continuation because incomplete/completed caches also advance the scan.
pub struct EpochFileAckPage {
    pub messages: Vec<String>,
    pub next: Option<EpochFileAckCursor>,
}

pub(in crate::encryption::signal_state) enum FileAckCommand {
    Status(String, Reply<Option<i64>>),
    Pending(Option<EpochFileAckCursor>, Reply<EpochFileAckPage>),
    Prepare(
        String,
        Arc<OwnedSemaphorePermit>,
        Reply<EpochDurableFileAck>,
    ),
    Accept(
        Box<EpochObservation<EpochDurableFileAck, v2::FileAcknowledgements>>,
        Reply<()>,
    ),
}

fn current_owner(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<()> {
    recovery_indexes::require_ready(tx, own)?;
    if tx
        .get(&RecordKey::new(
            &own.generation_id,
            RecordKind::Control,
            crate::encryption::provisioning::ARCHIVED_DEVICE_RECORD,
        )?)?
        .is_some()
    {
        return Err(CryptoError::ArchivedGeneration);
    }
    Ok(())
}
fn request(plan: &EpochAttachmentDownloadPlan) -> v2::AcknowledgeFilesRequest {
    v2::AcknowledgeFilesRequest {
        context: Some(crate::proto::e2ee::ClientContext {
            protocol_version: 2,
            device_generation_id: plan.own.generation_id.clone(),
        }),
        files: vec![v2::FileAcknowledgement {
            binding: Some(plan.binding.clone()),
            blob: Some(plan.blob.clone()),
        }],
    }
}

impl Engine {
    pub(super) fn handle_file_ack(&mut self, command: FileAckCommand) {
        match command {
            FileAckCommand::Status(uuid, reply) => {
                if !reply.is_closed() {
                    let own = &self.own;
                    let store = self.store.scope().clone();
                    let result = self.store.transaction_with(|tx| {
                        let (plan, _) = snapshot(tx, own, &store, &uuid)?;
                        let j = load(tx, &plan)?.ok_or(CryptoError::MessageUnavailable)?;
                        if j.verified != Some(true) {
                            return Err(CryptoError::MessageUnavailable);
                        }
                        Ok(j.file_acknowledged_at_unix_ms.filter(|at| *at > 0))
                    });
                    let _ = reply.send(result);
                }
            }
            FileAckCommand::Pending(cursor, reply) => {
                if !reply.is_closed() {
                    let result = self.pending_file_acks(cursor);
                    let _ = reply.send(result);
                }
            }
            FileAckCommand::Prepare(uuid, permit, reply) => {
                if !reply.is_closed() {
                    let result = self.prepare_file_ack(&uuid);
                    // SAME whole-file permit is held through verification/COMMIT,
                    // even if the requesting future drops while this command runs.
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            FileAckCommand::Accept(observation, reply) => {
                if !reply.is_closed() {
                    let result = self.accept_file_ack(*observation);
                    let _ = reply.send(result);
                }
            }
        }
    }
    fn pending_file_acks(
        &mut self,
        cursor: Option<EpochFileAckCursor>,
    ) -> CryptoResult<EpochFileAckPage> {
        let own = &self.own;
        let store = self.store.scope().clone();
        if cursor
            .as_ref()
            .is_some_and(|c| c.store != store || c.own != *own)
        {
            return Err(CryptoError::Conflict);
        }
        self.store.transaction_with(|tx| {
            current_owner(tx, own)?;
            // Existing (generation,kind,record_id) PK: no retained history scan,
            // SQL projection, migration, or whole-file allocation in this page.
            let rows = tx.record_page_limited(
                &own.generation_id,
                RecordKind::Control,
                PREFIX,
                cursor.as_ref().map(|c| c.after.as_str()),
                false,
                (PAGE_SIZE, 4096),
            )?;
            let next = if rows.len() == PAGE_SIZE {
                Some(EpochFileAckCursor {
                    store: store.clone(),
                    own: own.clone(),
                    after: rows.last().unwrap().0.clone(),
                })
            } else {
                None
            };
            let mut messages = Vec::new();
            for (id, _) in rows {
                let uuid = id.strip_prefix(PREFIX).ok_or(CryptoError::Invalid)?;
                let (plan, _) = snapshot(tx, own, &store, uuid)?;
                let j = load(tx, &plan)?.ok_or(CryptoError::MessageUnavailable)?;
                if j.file_acknowledged_at_unix_ms == Some(0) {
                    messages.push(uuid.to_owned());
                }
            }
            Ok(EpochFileAckPage { messages, next })
        })
    }
    fn prepare_file_ack(&mut self, uuid: &str) -> CryptoResult<EpochDurableFileAck> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let (plan, p) = snapshot(tx, own, &store, uuid)?;
            let j = load(tx, &plan)?.ok_or(CryptoError::MessageUnavailable)?;
            require_cached(&j)?;
            drop(open(tx, &plan, &p)?); // Actual chunks/hash/descriptor/AAD/AEAD, not verified flag alone.
            let request = request(&plan);
            contract::validate_request(&request).map_err(|_| CryptoError::Invalid)?;
            Ok(EpochDurableFileAck {
                plan,
                run: j.run,
                request,
                acknowledged_at: j.file_acknowledged_at_unix_ms.filter(|at| *at > 0),
            })
        })
    }
    fn accept_file_ack(
        &mut self,
        observed: EpochObservation<EpochDurableFileAck, v2::FileAcknowledgements>,
    ) -> CryptoResult<()> {
        let proof = observed.request();
        let own = &self.own;
        let store = self.store.scope().clone();
        if observed.store_scope() != &store
            || proof.store_scope() != &store
            || proof.own_device() != own
        {
            return Err(CryptoError::Conflict);
        }
        contract::validate_receipts(proof.request(), observed.response())
            .map_err(|_| CryptoError::Invalid)?;
        let at = observed.response().files[0].acknowledged_at_unix_ms;
        self.store.transaction_with(|tx| {
            fresh(tx, own, &store, &proof.plan)?;
            let mut j = load(tx, &proof.plan)?.ok_or(CryptoError::MessageUnavailable)?;
            if j.verified != Some(true)
                || j.run != proof.run
                || request(&proof.plan) != proof.request
            {
                return Err(CryptoError::Conflict);
            }
            match j.file_acknowledged_at_unix_ms {
                Some(0) => {
                    j.file_acknowledged_at_unix_ms = Some(at);
                    tx.put(&key(own, proof.message_id())?, &j.encode_to_vec())?;
                    Ok(())
                }
                Some(saved) if saved == at => Ok(()),
                _ => Err(CryptoError::Conflict),
            }
        })
    }
}

impl CryptoCoordinator {
    /// Local metadata observation only, never a byte/AEAD or RPC ACK proof.
    pub async fn epoch_file_ack_status(&self, uuid: String) -> CryptoResult<Option<i64>> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::FileAck(
                FileAckCommand::Status(uuid, r),
            )))
        })
        .await
    }
    pub async fn pending_epoch_file_acks(
        &self,
        after: Option<EpochFileAckCursor>,
    ) -> CryptoResult<EpochFileAckPage> {
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::FileAck(
                FileAckCommand::Pending(after, r),
            )))
        })
        .await
    }
    pub async fn epoch_file_ack(&self, uuid: String) -> CryptoResult<EpochDurableFileAck> {
        if !epoch_rpc::uuid4(&uuid) {
            return Err(CryptoError::Invalid);
        }
        let permit = Arc::new(self.attachment_permit()?);
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::FileAck(
                FileAckCommand::Prepare(uuid, permit, r),
            )))
        })
        .await
    }
    pub async fn accept_epoch_file_ack(
        &self,
        observed: EpochObservation<EpochDurableFileAck, v2::FileAcknowledgements>,
    ) -> CryptoResult<()> {
        self.request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::FileAck(
                FileAckCommand::Accept(Box::new(observed), r),
            )))
        })
        .await
    }
}
