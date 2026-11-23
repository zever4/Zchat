//! One native upload step, without polling, spawned tasks or Signal promotion.
//! The caller owns pacing and must stop/join the transport before account unlock.
use super::{
    blob_control,
    native::{BlobHttpError, EpochBlobControlOutcome, EpochBlobLease, EpochObservation, RpcError},
    signal_state::{
        CryptoCoordinator, CryptoError, EpochBlobControlPlan, EpochBlobProgress, EpochBlobUpload,
    },
};
use crate::proto::e2ee_v2 as v2;
use tokio::sync::watch;

/// Trusted native implementations only, preserving scope and wire validation.
/// No renderer URLs/bytes or secret descriptor enters this interface.
#[async_trait::async_trait]
pub trait EpochBlobUploadTransport: Send {
    async fn observe(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome, RpcError>;
    async fn reserve(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome, RpcError>;
    async fn put(&mut self, source: EpochBlobUpload) -> Result<u16, BlobHttpError>;
    /// Wait until LAST body owner releases the protected source/file permit.
    async fn join(&mut self);
    /// Permanent stop for this account transport, not window minimization.
    fn cancel(&mut self);
}

#[derive(Debug, thiserror::Error)]
pub enum BlobUploadError {
    #[error("blob upload stopped; the server result may be unknown")]
    Stopped,
    #[error("blob metadata RPC failed; retry the saved operation")]
    Rpc(#[from] RpcError),
    #[error("blob control rejected the saved operation: {0:?}")]
    Rejected(v2::BlobFailureReason),
    #[error("protected blob operation failed")]
    Crypto(#[from] CryptoError),
}

/// Local control progress only. Never a Signal promotion, Send or file receipt.
/// Even a successful PUT can be followed by a failed/unknown Observe.
pub enum BlobUploadStep {
    Observed {
        progress: Box<EpochBlobProgress>,
        http: Option<Result<u16, BlobHttpError>>,
    },
    /// Successor operation committed locally; no new reservation was sent yet.
    Renewed(Box<EpochBlobControlPlan>),
}

pub(super) fn failure(
    plan: &EpochBlobControlPlan,
    observed: &EpochObservation<EpochBlobControlPlan, v2::BlobFailure>,
    observe: bool,
) -> Result<v2::BlobFailureReason, BlobUploadError> {
    if observed.store_scope() != plan.store_scope()
        || observed.request().request() != plan.request()
        || blob_control::validate_failure(plan.request(), observed.response(), observe).is_err()
    {
        return Err(RpcError::Invalid.into());
    }
    v2::BlobFailureReason::try_from(observed.response().reason)
        .map_err(|_| RpcError::Invalid.into())
}
pub(super) fn lease(
    result: EpochBlobControlOutcome,
    plan: &EpochBlobControlPlan,
    observe: bool,
) -> Result<Box<EpochBlobLease>, BlobUploadError> {
    match result {
        EpochBlobControlOutcome::Lease(value) => Ok(value),
        EpochBlobControlOutcome::Rejected(value) => {
            Err(BlobUploadError::Rejected(failure(plan, &value, observe)?))
        }
    }
}

/// At most three metadata RPCs, one PUT and one local renewal; never a retry loop.
/// Unknown results preserve the original journal/ciphertext. Caller cancellation
/// can leave a local COMMIT in flight; the owner must still join its crypto worker.
pub async fn upload_epoch_blob<T: EpochBlobUploadTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    message: String,
    stop: &mut watch::Receiver<bool>,
) -> Result<BlobUploadStep, BlobUploadError> {
    let result = {
        tokio::select! {
            biased;
            _ = super::delivery_runner::cancelled(stop) => Err(BlobUploadError::Stopped),
            result = step(worker, transport, message) => result,
        }
    };
    if matches!(result, Err(BlobUploadError::Stopped)) {
        transport.cancel();
    }
    // Also join on every error path. No queued body may overlap a later step.
    transport.join().await;
    result
}

async fn step<T: EpochBlobUploadTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    message: String,
) -> Result<BlobUploadStep, BlobUploadError> {
    let plan = worker.epoch_attachment_control(message.clone()).await?;
    let observation = match transport.observe(&plan).await? {
        EpochBlobControlOutcome::Lease(value) => value,
        EpochBlobControlOutcome::Rejected(value) => {
            let reason = failure(&plan, &value, true)?;
            if reason != v2::BlobFailureReason::OperationNotObserved {
                return Err(BlobUploadError::Rejected(reason));
            }
            // Committed absence authorizes only replaying THIS saved operation.
            // It is neither an unsent proof nor permission to renew/reseal.
            lease(transport.reserve(&plan).await?, &plan, false)?
        }
    };
    let progress = worker.accept_epoch_blob_progress(*observation).await?;
    // Decide from durable merged state, never an older remote reply.
    if !progress.linked_seen() && blob_control::renewable(progress.lease()) {
        return Ok(BlobUploadStep::Renewed(Box::new(
            worker
                .renew_epoch_blob_control(progress.plan().clone())
                .await?,
        )));
    }
    if progress.linked_seen() || progress.lease().state != v2::BlobLeaseState::Reserved as i32 {
        return Ok(BlobUploadStep::Observed {
            progress: Box::new(progress),
            http: None,
        });
    }
    let source = worker.epoch_blob_upload(message).await?;
    if source.plan().store_scope() != plan.store_scope()
        || source.plan().request() != plan.request()
        || source.grant() != progress.lease()
    {
        return Err(CryptoError::Conflict.into());
    }
    let http = transport.put(source).await;
    // Before RPC/acceptance, release ALL source/chunk owners and their file permit.
    transport.join().await;
    if matches!(http, Err(BlobHttpError::Cancelled)) {
        // A stopped account must not reconcile using its old authentication.
        // Next login observes the SAME durable operation before another PUT.
        return Err(BlobUploadError::Stopped);
    }
    // Reconcile even after201,503,timeout or a lost reply; do not repeat PUT here.
    let observed = lease(transport.observe(&plan).await?, &plan, true)?;
    let progress = worker.accept_epoch_blob_progress(*observed).await?;
    Ok(BlobUploadStep::Observed {
        progress: Box::new(progress),
        http: Some(http),
    })
}
