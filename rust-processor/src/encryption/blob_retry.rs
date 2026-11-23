//! One retained-file step following an exact failed Send; never reencrypts.
use super::{
    blob_control,
    blob_upload::{BlobUploadError, EpochBlobUploadTransport, failure, lease},
    native::{BlobHttpError, EpochBlobControlOutcome, EpochBlobSendFailure},
    signal_state::{CryptoCoordinator, CryptoError, EpochAttachmentRetryPlan},
};
use crate::proto::e2ee_v2 as v2;
use tokio::sync::watch;

/// The account proxy rechecks the worker at actual serial RPC dispatch. This
/// separate entry cannot accidentally treat a retained plan as a preparation.
#[async_trait::async_trait]
pub(super) trait RetainedBlobTransport: EpochBlobUploadTransport {
    async fn observe_retry(
        &mut self,
        plan: &EpochAttachmentRetryPlan,
    ) -> Result<EpochBlobControlOutcome, BlobUploadError>;
    async fn reserve_retry(
        &mut self,
        plan: &EpochAttachmentRetryPlan,
    ) -> Result<EpochBlobControlOutcome, BlobUploadError>;
}
pub(super) struct RetainedBlobStep {
    pub plan: EpochAttachmentRetryPlan,
}

/// <=4metadata RPCs, one local renewal and one PUT. Every unknown outcome is
/// followed by a NEW Send before another step; caller owns hint freshness/pacing.
pub(super) async fn retry_epoch_blob<T: RetainedBlobTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    failed: EpochBlobSendFailure,
    stop: &mut watch::Receiver<bool>,
) -> Result<RetainedBlobStep, BlobUploadError> {
    let result = tokio::select! {
        biased;
        _=super::delivery_runner::cancelled(stop)=>Err(BlobUploadError::Stopped),
        result=step(worker,transport,failed)=>result,
    };
    if matches!(result, Err(BlobUploadError::Stopped)) {
        transport.cancel();
    }
    transport.join().await;
    result
}
async fn step<T: RetainedBlobTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    failed: EpochBlobSendFailure,
) -> Result<RetainedBlobStep, BlobUploadError> {
    let mut p = worker.epoch_attachment_retry(failed).await?;
    let observed = match transport.observe_retry(&p).await? {
        EpochBlobControlOutcome::Lease(v) => v,
        EpochBlobControlOutcome::Rejected(v) => {
            let reason = failure(p.control(), &v, true)?;
            if reason != v2::BlobFailureReason::OperationNotObserved {
                return Err(BlobUploadError::Rejected(reason));
            }
            // Replay THIS saved request, never invent an operation for absence.
            lease(transport.reserve_retry(&p).await?, p.control(), false)?
        }
    };
    p = worker
        .accept_epoch_attachment_retry_lease(p, *observed)
        .await?;
    if !p.linked_seen() && blob_control::renewable(p.lease().ok_or(CryptoError::Conflict)?) {
        p = worker.renew_epoch_attachment_retry(p).await?;
        let observed = lease(transport.reserve_retry(&p).await?, p.control(), false)?;
        p = worker
            .accept_epoch_attachment_retry_lease(p, *observed)
            .await?;
    }
    let grant = p.lease().ok_or(CryptoError::Conflict)?;
    if p.linked_seen() || grant.expired || grant.state != v2::BlobLeaseState::Reserved as i32 {
        return Ok(RetainedBlobStep { plan: p });
    }
    let source = worker.epoch_attachment_retry_upload(p.clone()).await?;
    if source.plan().store_scope() != p.control().store_scope()
        || source.plan().request() != p.control().request()
        || Some(source.grant()) != p.lease()
    {
        return Err(CryptoError::Conflict.into());
    }
    let http = transport.put(source).await;
    // No metadata/worker acceptance while a queued chunk still holds the permit.
    transport.join().await;
    if matches!(http, Err(BlobHttpError::Cancelled)) {
        return Err(BlobUploadError::Stopped);
    }
    let observed = lease(transport.observe_retry(&p).await?, p.control(), true)?;
    p = worker
        .accept_epoch_attachment_retry_lease(p, *observed)
        .await?;
    Ok(RetainedBlobStep { plan: p })
}
