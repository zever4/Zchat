//! One explicit download on the account's existing shared HTTP future. No RPC
//! or per-file spawned task; cancellation keeps IO/worker ownership until join.
use super::*;
use crate::encryption::{
    account_owner::blobs::{Body, Proxy},
    blob_upload::EpochBlobUploadTransport,
    native::{BlobReadError, RpcError},
    signal_state::AttachmentDownloadError,
};

pub(in crate::encryption::account_owner) async fn serve<B: Body>(
    worker: &CryptoCoordinator,
    proxy: &mut Proxy<B>,
    mut command: Command,
    stop: &mut watch::Receiver<bool>,
) -> Result<()> {
    if command.reply.is_closed() {
        return Ok(());
    }
    let Request::Download {
        conversation,
        message,
    } = &command.request
    else {
        return Err(AccountError::Invalid);
    };
    let operation = async {
        // The queue accepts an ID, never a public read request/proof. Resolve
        // and revalidate this account's protected incoming history at dispatch.
        let plan = worker
            .epoch_attachment_download_plan(message.clone())
            .await?;
        if conversation
            .as_deref()
            .is_some_and(|chat| chat != plan.conversation_id())
        {
            return Err(CryptoError::Conflict.into());
        }
        worker
            .download_epoch_attachment(proxy, plan)
            .await
            .map_err(|e| match e {
                AttachmentDownloadError::Crypto(e) => AccountError::Crypto(e),
                AttachmentDownloadError::Http(e) => AccountError::Attachment(e),
            })?;
        Ok(Response::AttachmentCached)
    };
    let result = tokio::select! {
        biased;
        _=super::super::super::delivery_runner::cancelled(stop)=>None,
        _=command.reply.closed()=>None,
        r=tokio::time::timeout(Duration::from_secs(60),operation)=>Some(r.unwrap_or(Err(AccountError::CommandTimeout))),
    };
    // Even a cancelled waiter can leave a queued/running Begin/Append/Finish.
    // Join LAST HTTP chunk, then a FIFO barrier on the SAME worker before the
    // command budget is released or another file job starts. Account stop may
    // drop this future; AccountOwner's independent cleanup repeats joins.
    proxy.join().await;
    worker.join_attachment_work().await?;
    let Some(result) = result else {
        return Ok(());
    };
    let fatal = matches!(
        &result,
        Err(AccountError::Attachment(BlobReadError::Rejected(401)))
    );
    let _ = command.reply.send(result);
    if fatal {
        return Err(RpcError::Status(tonic::Code::Unauthenticated).into());
    }
    Ok(())
}
