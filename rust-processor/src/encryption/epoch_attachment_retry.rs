//! Exact postpromotion file retry. Does not create/replace a Signal message.
//! Ordinary preparation APIs keep their strict current_journal loader.
use super::*;
use crate::encryption::{
    blob_control as boundary,
    native::{EpochBlobLease, EpochBlobSendFailure},
};

/// Native-only checked handoff. No constructor, Debug, serde or secret getter.
/// A snapshot is never standing authority: each operation reloads lifecycle,
/// exact pending outbox/cohort, operation CAS and chunks in its own transaction.
#[derive(Clone)]
pub struct EpochAttachmentRetryPlan {
    outgoing: EpochOutgoingPlan,
    failure: v2::BlobSendFailure,
    control: EpochBlobControlPlan,
    lease: Option<v2::BlobLease>,
    linked_seen: bool,
}
impl EpochAttachmentRetryPlan {
    pub fn outgoing(&self) -> &EpochOutgoingPlan {
        &self.outgoing
    }
    pub fn control(&self) -> &EpochBlobControlPlan {
        &self.control
    }
    pub fn lease(&self) -> Option<&v2::BlobLease> {
        self.lease.as_ref()
    }
    pub fn linked_seen(&self) -> bool {
        self.linked_seen
    }
}
pub(in crate::encryption::signal_state) enum RetryCommand {
    Plan(Box<EpochBlobSendFailure>, Reply<EpochAttachmentRetryPlan>),
    Accept(
        Box<EpochAttachmentRetryPlan>,
        Box<EpochBlobLease>,
        Reply<EpochAttachmentRetryPlan>,
    ),
    Renew(
        Box<EpochAttachmentRetryPlan>,
        Reply<EpochAttachmentRetryPlan>,
    ),
    Upload(Box<EpochAttachmentRetryPlan>, Reply<EpochBlobUpload>),
    Refresh(
        Box<EpochAttachmentRetryPlan>,
        Reply<EpochAttachmentRetryPlan>,
    ),
}

fn authorized_journal(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    outgoing: &EpochOutgoingPlan,
    failure: &v2::BlobSendFailure,
) -> CryptoResult<Journal> {
    recovery_indexes::require_ready(tx, own)?;
    if outgoing.store_scope() != store || !outgoing.needs_send() {
        return Err(CryptoError::Conflict);
    }
    boundary::validate_send_failure(outgoing.request(), failure)
        .map_err(|_| CryptoError::Invalid)?;
    let e = outgoing.request().envelope.as_ref().unwrap();
    let j =
        load_retained_journal(tx, own, &e.message_id)?.ok_or(CryptoError::MessageUnavailable)?;
    let saved = promotion::retained_plan(tx, own, store, &e.message_id, &j)?
        .ok_or(CryptoError::Conflict)?;
    if !saved.needs_send() || saved.request() != outgoing.request() {
        return Err(CryptoError::Conflict);
    }
    // A receipt/fence may settle an archived scope without files. A NEW file
    // transfer cannot: always recheck the present authenticated lifecycle.
    let current =
        current(tx, own, store, &e.conversation_id)?.ok_or(CryptoError::ConversationUnavailable)?;
    if e.crypto_epoch != Some(current.epoch)
        || e.sender.as_ref() != Some(&current.own)
        || e.recipient.as_ref() != Some(&current.peer)
    {
        return Err(CryptoError::Conflict);
    }
    let _ = local_plan(tx, own, store, &j)?;
    Ok(j)
}
fn current_retry(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    p: &EpochAttachmentRetryPlan,
) -> CryptoResult<Journal> {
    let j = authorized_journal(tx, own, store, &p.outgoing, &p.failure)?;
    control::check_plan(&p.control, &j, own, store)?;
    Ok(j)
}
fn snapshot(
    outgoing: EpochOutgoingPlan,
    failure: v2::BlobSendFailure,
    own: &DeviceRef,
    store: &StoreScope,
    j: &Journal,
) -> EpochAttachmentRetryPlan {
    EpochAttachmentRetryPlan {
        outgoing,
        failure,
        control: control::plan(j, own, store),
        lease: j.lease.clone(),
        linked_seen: j.linked_seen.unwrap(),
    }
}
impl Engine {
    fn attachment_retry_refresh(
        &mut self,
        p: EpochAttachmentRetryPlan,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let j = current_retry(tx, own, &store, &p)?;
            if j.operation.as_ref() != Some(p.control.request()) {
                return Err(CryptoError::Conflict);
            }
            Ok(snapshot(p.outgoing, p.failure, own, &store, &j))
        })
    }
    fn attachment_retry_plan(
        &mut self,
        observed: EpochBlobSendFailure,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            if observed.store_scope() != &store {
                return Err(CryptoError::Conflict);
            }
            let outgoing = observed.request();
            let failure = observed.response();
            let j = authorized_journal(tx, own, &store, outgoing, failure)?;
            Ok(snapshot(outgoing.clone(), failure.clone(), own, &store, &j))
        })
    }
    fn attachment_retry_accept(
        &mut self,
        p: EpochAttachmentRetryPlan,
        observed: EpochBlobLease,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let j = current_retry(tx, own, &store, &p)?;
            // The RPC response cannot substitute a different operation/cohort.
            if observed.request().request() != p.control.request() {
                return Err(CryptoError::Conflict);
            }
            let progress = control::merge_lease(tx, own, &store, j, observed)?;
            Ok(EpochAttachmentRetryPlan {
                control: progress.plan().clone(),
                lease: Some(progress.lease().clone()),
                linked_seen: progress.linked_seen(),
                ..p
            })
        })
    }
    fn attachment_retry_renew(
        &mut self,
        p: EpochAttachmentRetryPlan,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let j = current_retry(tx, own, &store, &p)?;
            let control = control::renew_operation(tx, own, &store, j, p.control.clone())?;
            let j = load_retained_journal(tx, own, &control.request().message_id)?
                .ok_or(CryptoError::Conflict)?;
            Ok(snapshot(p.outgoing, p.failure, own, &store, &j))
        })
    }
    fn attachment_retry_upload(
        &mut self,
        p: EpochAttachmentRetryPlan,
        permit: OwnedSemaphorePermit,
    ) -> CryptoResult<EpochBlobUpload> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let j = current_retry(tx, own, &store, &p)?;
            if j.operation.as_ref() != Some(p.control.request()) {
                return Err(CryptoError::Conflict);
            }
            control::upload_source(tx, own, &store, &j, permit)
        })
    }
    pub(super) fn handle_attachment_retry(
        &mut self,
        command: RetryCommand,
        permit: OwnedSemaphorePermit,
    ) {
        match command {
            RetryCommand::Refresh(plan, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_retry_refresh(*plan);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            RetryCommand::Plan(observed, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_retry_plan(*observed);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            RetryCommand::Accept(plan, observed, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_retry_accept(*plan, *observed);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            RetryCommand::Renew(plan, reply) => {
                if !reply.is_closed() {
                    let result = self.attachment_retry_renew(*plan);
                    drop(permit);
                    let _ = reply.send(result);
                }
            }
            RetryCommand::Upload(plan, reply) => {
                if !reply.is_closed() {
                    let _ = reply.send(self.attachment_retry_upload(*plan, permit));
                }
            }
        }
    }
}
impl CryptoCoordinator {
    /// Recheck a queued metadata command at dispatch, before its network RPC.
    pub(in crate::encryption) async fn refresh_epoch_attachment_retry(
        &self,
        plan: EpochAttachmentRetryPlan,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Retry(
                Box::new(RetryCommand::Refresh(Box::new(plan), reply)),
                permit,
            )))
        })
        .await
    }
    pub async fn epoch_attachment_retry(
        &self,
        observed: EpochBlobSendFailure,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Retry(
                Box::new(RetryCommand::Plan(Box::new(observed), reply)),
                permit,
            )))
        })
        .await
    }
    pub async fn accept_epoch_attachment_retry_lease(
        &self,
        plan: EpochAttachmentRetryPlan,
        observed: EpochBlobLease,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Retry(
                Box::new(RetryCommand::Accept(
                    Box::new(plan),
                    Box::new(observed),
                    reply,
                )),
                permit,
            )))
        })
        .await
    }
    pub async fn renew_epoch_attachment_retry(
        &self,
        plan: EpochAttachmentRetryPlan,
    ) -> CryptoResult<EpochAttachmentRetryPlan> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Retry(
                Box::new(RetryCommand::Renew(Box::new(plan), reply)),
                permit,
            )))
        })
        .await
    }
    pub async fn epoch_attachment_retry_upload(
        &self,
        plan: EpochAttachmentRetryPlan,
    ) -> CryptoResult<EpochBlobUpload> {
        let permit = self.attachment_permit()?;
        self.request(|reply| {
            Command::Epoch(EpochCommand::Attachment(AttachmentCommand::Retry(
                Box::new(RetryCommand::Upload(Box::new(plan), reply)),
                permit,
            )))
        })
        .await
    }
}
