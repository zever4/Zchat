//! One account-owned HTTP lane, polled in the owner's existing task. Metadata
//! goes through the SAME serial RPC lane as text/control, never a cloned client.
use super::*;
use crate::encryption::{
    blob_download::EpochBlobDownloadTransport,
    blob_retry::{RetainedBlobTransport, retry_epoch_blob},
    blob_upload::{BlobUploadError, BlobUploadStep, EpochBlobUploadTransport, upload_epoch_blob},
    epoch_runner::AttachmentRetryNotice,
    native::{BlobHttpError, EpochBlobControlOutcome, NativeBlobHttp, RpcError},
    signal_state::{
        EpochAttachmentPromotion, EpochAttachmentRetryPlan, EpochBlobControlPlan, EpochBlobUpload,
    },
};
use std::collections::BTreeMap;
use tokio::sync::oneshot;

pub(in crate::encryption) const RPC_CAPACITY: usize = 1;
pub(in crate::encryption) const NOTICE_CAPACITY: usize = 64;
pub(in crate::encryption) enum Rpc {
    Control {
        reserve: bool,
        plan: EpochBlobControlPlan,
        reply: oneshot::Sender<std::result::Result<EpochBlobControlOutcome, RpcError>>,
    },
    Promote {
        plan: EpochBlobControlPlan,
        reply: oneshot::Sender<Result<()>>,
    },
    RetainedControl {
        reserve: bool,
        plan: Box<EpochAttachmentRetryPlan>,
        reply: oneshot::Sender<std::result::Result<EpochBlobControlOutcome, BlobUploadError>>,
    },
}

#[async_trait::async_trait]
pub(in crate::encryption) trait Body: EpochBlobDownloadTransport {
    async fn put(&mut self, source: EpochBlobUpload) -> std::result::Result<u16, BlobHttpError>;
    async fn join(&mut self);
    fn cancel(&mut self);
}
#[async_trait::async_trait]
impl Body for NativeBlobHttp {
    async fn put(&mut self, source: EpochBlobUpload) -> std::result::Result<u16, BlobHttpError> {
        NativeBlobHttp::put(self, source).await.map(|r| r.status())
    }
    async fn join(&mut self) {
        self.wait_idle().await;
    }
    fn cancel(&mut self) {
        self.cancellation().cancel();
    }
}

pub(super) struct Proxy<B> {
    body: B,
    rpc: mpsc::Sender<Rpc>,
    notices: mpsc::Receiver<AttachmentRetryNotice>,
    stopped: bool,
}
impl<B: Body> Proxy<B> {
    pub(super) fn new(
        body: B,
        rpc: mpsc::Sender<Rpc>,
        notices: mpsc::Receiver<AttachmentRetryNotice>,
    ) -> Self {
        Self {
            body,
            rpc,
            notices,
            stopped: false,
        }
    }
    async fn metadata(
        &mut self,
        plan: &EpochBlobControlPlan,
        reserve: bool,
    ) -> std::result::Result<EpochBlobControlOutcome, RpcError> {
        if self.stopped {
            return Err(RpcError::Status(tonic::Code::Cancelled));
        }
        let (reply, response) = oneshot::channel();
        self.rpc
            .try_send(Rpc::Control {
                reserve,
                plan: plan.clone(),
                reply,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    RpcError::Status(tonic::Code::ResourceExhausted)
                }
                mpsc::error::TrySendError::Closed(_) => RpcError::Transport,
            })?;
        response.await.map_err(|_| RpcError::Transport)?
    }
    async fn promote(&mut self, plan: EpochBlobControlPlan) -> Result<()> {
        if self.stopped {
            return Err(AccountError::Stopped);
        }
        let (reply, response) = oneshot::channel();
        self.rpc
            .try_send(Rpc::Promote { plan, reply })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => AccountError::Busy,
                mpsc::error::TrySendError::Closed(_) => AccountError::Stopped,
            })?;
        response.await.map_err(|_| AccountError::Stopped)?
    }
    async fn retained_metadata(
        &mut self,
        plan: &EpochAttachmentRetryPlan,
        reserve: bool,
    ) -> std::result::Result<EpochBlobControlOutcome, BlobUploadError> {
        if self.stopped {
            return Err(BlobUploadError::Stopped);
        }
        let (reply, response) = oneshot::channel();
        self.rpc
            .try_send(Rpc::RetainedControl {
                reserve,
                plan: Box::new(plan.clone()),
                reply,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => {
                    BlobUploadError::Rpc(RpcError::Status(tonic::Code::ResourceExhausted))
                }
                mpsc::error::TrySendError::Closed(_) => BlobUploadError::Stopped,
            })?;
        response.await.map_err(|_| BlobUploadError::Stopped)?
    }
}
#[async_trait::async_trait]
impl<B: Body> RetainedBlobTransport for Proxy<B> {
    async fn observe_retry(
        &mut self,
        p: &EpochAttachmentRetryPlan,
    ) -> std::result::Result<EpochBlobControlOutcome, BlobUploadError> {
        self.retained_metadata(p, false).await
    }
    async fn reserve_retry(
        &mut self,
        p: &EpochAttachmentRetryPlan,
    ) -> std::result::Result<EpochBlobControlOutcome, BlobUploadError> {
        self.retained_metadata(p, true).await
    }
}
#[async_trait::async_trait]
impl<B: Body> EpochBlobUploadTransport for Proxy<B> {
    async fn observe(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> std::result::Result<EpochBlobControlOutcome, RpcError> {
        self.metadata(plan, false).await
    }
    async fn reserve(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> std::result::Result<EpochBlobControlOutcome, RpcError> {
        self.metadata(plan, true).await
    }
    async fn put(&mut self, source: EpochBlobUpload) -> std::result::Result<u16, BlobHttpError> {
        if self.stopped {
            return Err(BlobHttpError::Cancelled);
        }
        self.body.put(source).await
    }
    async fn join(&mut self) {
        self.body.join().await;
    }
    fn cancel(&mut self) {
        self.stopped = true;
        self.body.cancel();
    }
}
#[async_trait::async_trait]
impl<B: Body> EpochBlobDownloadTransport for Proxy<B> {
    async fn get(
        &mut self,
        request: crate::encryption::native::BlobReadRequest,
        sink: &mut dyn crate::encryption::native::BlobCiphertextSink,
    ) -> std::result::Result<
        crate::encryption::native::BlobGetResponse,
        crate::encryption::native::BlobReadError,
    > {
        if self.stopped {
            return Err(BlobHttpError::Cancelled.into());
        }
        self.body.get(request, sink).await
    }
}

pub(super) async fn rpc<T: EpochDeliveryTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    request: Rpc,
    stop: &mut watch::Receiver<bool>,
    wake: &Notify,
) -> Result<()> {
    let (reserve, plan, mut reply) = match request {
        Rpc::RetainedControl {
            reserve,
            plan,
            mut reply,
        } => {
            let result = tokio::select! {
                biased;
                _=crate::encryption::delivery_runner::cancelled(stop)=>return Ok(()),
                _=reply.closed()=>return Ok(()),
                result=tokio::time::timeout(Duration::from_secs(10),async {
                    // Queue waiting is not authorization. Recheck at dispatch.
                    let p=worker.refresh_epoch_attachment_retry(*plan).await?;
                    Ok::<_,BlobUploadError>(if reserve {transport.reserve_blob(p.control()).await?}else{transport.observe_blob(p.control()).await?})
                })=>result.unwrap_or(Err(BlobUploadError::Rpc(RpcError::LocalDeadline))),
            };
            let fatal = match &result {
                Err(BlobUploadError::Rpc(e)) if commands::fatal_rpc(e) => Some(e.clone()),
                _ => None,
            };
            let _ = reply.send(result);
            return fatal.map_or(Ok(()), |e| Err(e.into()));
        }
        Rpc::Control {
            reserve,
            plan,
            reply,
        } => (reserve, plan, reply),
        Rpc::Promote { plan, mut reply } => {
            let result = tokio::select! {
                biased;
                _ = crate::encryption::delivery_runner::cancelled(stop) => return Ok(()),
                _ = reply.closed() => return Ok(()),
                result = tokio::time::timeout(Duration::from_secs(15), promote(worker, transport, plan)) => result.unwrap_or(Err(AccountError::CommandTimeout)),
            };
            let fatal = match &result {
                Err(AccountError::Rpc(e)) if commands::fatal_rpc(e) => Some(e.clone()),
                _ => None,
            };
            if result.is_ok() {
                wake.notify_one(); // Existing durable outbox runner owns Send.
            }
            let _ = reply.send(result);
            return fatal.map_or(Ok(()), |e| Err(e.into()));
        }
    };
    let result = tokio::select! {
        biased;
        _ = crate::encryption::delivery_runner::cancelled(stop) => return Ok(()),
        _ = reply.closed() => return Ok(()),
        result = tokio::time::timeout(Duration::from_secs(10), async {
            if reserve { transport.reserve_blob(&plan).await }
            else { transport.observe_blob(&plan).await }
        }) => result.unwrap_or(Err(RpcError::LocalDeadline)),
    };
    let fatal = result
        .as_ref()
        .err()
        .filter(|e| commands::fatal_rpc(e))
        .cloned();
    let _ = reply.send(result);
    if let Some(e) = fatal {
        return Err(AccountError::Rpc(e));
    }
    Ok(())
}

// Only a saved user preparation reaching Ready triggers initial bootstrap.
// This uses the SAME RPC client, bounded acquisition renewal and the existing
// current-scope worker gates. No initial-catalog creation or recovery consent.
async fn promote<T: EpochDeliveryTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    plan: EpochBlobControlPlan,
) -> Result<()> {
    if matches!(
        worker.promote_epoch_attachment_if(plan.clone()).await?,
        EpochAttachmentPromotion::Queued(_)
    ) {
        return Ok(());
    }
    let request = plan.request();
    let lookup = worker
        .prepare_conversation_lookup(request.conversation_id.clone())
        .await?;
    worker
        .accept_conversation_lookup(transport.lookup(&lookup).await?)
        .await?;
    let scope = worker
        .active_epoch_scope(request.conversation_id.clone())
        .await?
        .ok_or(CryptoError::ConversationUnavailable)?;
    if Some(scope.crypto_epoch()) != request.crypto_epoch
        || request.sender.as_ref() != Some(scope.own_device())
        || request.recipient.as_ref() != Some(scope.peer_device())
    {
        return Err(CryptoError::Conflict.into());
    }
    if scope.crypto_epoch() == 0 || scope.bootstrap_initiator() {
        match worker.prepare_epoch_acquisition(scope).await {
            Ok(mut acquisition) => {
                if !acquisition.applied() {
                    for attempt in 0..2 {
                        match transport.acquire(&acquisition).await? {
                            crate::encryption::native::EpochAcquisitionOutcome::Bundle(bundle) => {
                                worker.bootstrap_epoch(*bundle).await?;
                                break;
                            }
                            crate::encryption::native::EpochAcquisitionOutcome::Expired(proof) => {
                                acquisition = worker.renew_epoch_acquisition(*proof).await?;
                                if attempt == 1 {
                                    return Err(CryptoError::Expired.into());
                                }
                            }
                        }
                    }
                }
            }
            Err(CryptoError::SessionExists) => (), // Incoming ratchet won the race.
            Err(e) => return Err(e.into()),
        }
    }
    match worker.promote_epoch_attachment_if(plan).await? {
        EpochAttachmentPromotion::Queued(_) => Ok(()),
        EpochAttachmentPromotion::BootstrapRequired => Err(AccountError::SessionPending),
    }
}
pub(super) async fn receive(queue: &mut Option<mpsc::Receiver<Rpc>>) -> Option<Rpc> {
    match queue {
        Some(queue) => queue.recv().await,
        None => std::future::pending().await,
    }
}
struct Retry {
    due: Instant,
    failures: u32,
    // Scheduling watermark only, never a cryptographic/DB expiry timestamp.
    after: Option<Instant>,
}
fn admit_notice(
    notice: AttachmentRetryNotice,
    manifest: &[String],
    entries: &BTreeMap<String, Retry>,
    pending: &mut BTreeMap<String, AttachmentRetryNotice>,
) {
    let Some(e) = notice.failure.request().request().envelope.as_ref() else {
        return;
    };
    let id = &e.message_id;
    if manifest.binary_search(id).is_err()
        || entries
            .get(id)
            .and_then(|r| r.after)
            .is_some_and(|after| notice.send_started <= after)
    {
        return;
    }
    if pending
        .get(id)
        .is_none_or(|old| old.send_started < notice.send_started)
    {
        pending.insert(id.clone(), notice);
    }
}

pub(super) async fn run<B: Body>(
    worker: &CryptoCoordinator,
    proxy: &mut Option<Proxy<B>>,
    config: RunnerConfig,
    mut stop: watch::Receiver<bool>,
    status: &watch::Sender<AccountStatus>,
    wake: &Notify,
    mut downloads: mpsc::Receiver<commands::Command>,
) -> Result<()> {
    let Some(proxy) = proxy else {
        return std::future::pending().await;
    };
    let mut entries: BTreeMap<String, Retry> = BTreeMap::new();
    let mut pending = BTreeMap::new();
    let mut after = String::new();
    let mut download_turn = true;
    loop {
        if *stop.borrow() || stop.has_changed().is_err() {
            return Ok(());
        }
        // At most one explicit read before giving a due upload/retry a turn.
        // Requests share the account command budget; no history prefetch/task.
        if download_turn && let Ok(command) = downloads.try_recv() {
            commands::download(worker, proxy, command, &mut stop).await?;
            download_turn = false;
            continue;
        }
        let manifest = tokio::select! {
            biased;
            _=crate::encryption::delivery_runner::cancelled(&mut stop)=>return Ok(()),
            result=worker.epoch_attachment_manifest()=>result?,
        };
        let ids = tokio::select! {
            biased;
            _ = crate::encryption::delivery_runner::cancelled(&mut stop) => return Ok(()),
            result = worker.epoch_attachment_candidates() => result?,
        };
        let now = Instant::now();
        entries.retain(|id, _| manifest.binary_search(id).is_ok());
        pending.retain(|id, _| manifest.binary_search(id).is_ok());
        for id in &manifest {
            entries.entry(id.clone()).or_insert(Retry {
                due: now,
                failures: 0,
                after: None,
            });
        }
        while let Ok(notice) = proxy.notices.try_recv() {
            admit_notice(notice, &manifest, &entries, &mut pending);
        }
        pending.retain(|id, notice| {
            entries[id]
                .after
                .is_none_or(|after| notice.send_started > after)
        });
        // Manifest and candidate reads are separate snapshots; a newly prepared
        // ID may appear only in the second. Admit it on the next bounded pass.
        let mut ids: Vec<_> = ids
            .into_iter()
            .filter(|id| entries.contains_key(id))
            .collect();
        ids.extend(pending.keys().cloned());
        ids.sort();
        ids.dedup();
        let split = ids.partition_point(|id| id <= &after);
        let candidate = ids[split..]
            .iter()
            .chain(ids[..split].iter())
            .find(|id| entries[*id].due <= now)
            .cloned();
        let Some(id) = candidate else {
            let due = ids
                .iter()
                .map(|id| entries[id].due)
                .min()
                .unwrap_or(now + config.poll_interval)
                .min(now + config.poll_interval);
            tokio::select! {
                biased;
                _ = crate::encryption::delivery_runner::cancelled(&mut stop) => return Ok(()),
                _ = tokio::time::sleep_until(due) => {},
                Some(notice)=proxy.notices.recv()=>{admit_notice(notice,&manifest,&entries,&mut pending);},
                Some(command)=downloads.recv()=>{
                    commands::download(worker,proxy,command,&mut stop).await?;
                    download_turn=false;
                },
            }
            continue;
        };
        after = id.clone();
        download_turn = true;
        if let Some(notice) = pending.remove(&id) {
            let result = retry_epoch_blob(worker, proxy, notice.into_failure(), &mut stop).await;
            let (health, failed, slow) = match &result {
                Err(BlobUploadError::Stopped) => return Ok(()),
                Err(BlobUploadError::Rpc(e)) if commands::fatal_rpc(e) => {
                    return Err(e.clone().into());
                }
                Err(BlobUploadError::Rpc(RpcError::LocalDeadline)) => {
                    (BlobHealth::LocalDeadline, true, false)
                }
                Err(BlobUploadError::Rejected(_)) => (BlobHealth::Unavailable, true, false),
                Err(_) => (BlobHealth::Retry, true, false),
                Ok(step) => {
                    let grant = step.plan.lease().expect("worker accepted exact lease");
                    if step.plan.linked_seen() {
                        (BlobHealth::Linked, false, true)
                    } else if grant.state == crate::proto::e2ee_v2::BlobLeaseState::Ready as i32
                        && !grant.expired
                    {
                        wake.notify_one();
                        (BlobHealth::Ready, false, true)
                    } else {
                        (BlobHealth::Waiting, false, false)
                    }
                }
            };
            let r = entries.get_mut(&id).unwrap();
            r.after = Some(Instant::now());
            r.failures = if failed {
                r.failures.saturating_add(1)
            } else {
                0
            };
            let delay = if slow {
                config.retry_max.max(config.poll_interval)
            } else {
                config.delay(r.failures)
            };
            let millis = delay.as_millis() as u64;
            r.due = Instant::now()
                + Duration::from_millis(rand::random_range(millis.div_ceil(2)..=millis));
            status.send_modify(|s| {
                s.blob_steps = s.blob_steps.saturating_add(1);
                s.blob_retry_steps = s.blob_retry_steps.saturating_add(1);
                s.blob_health = Some(health);
            });
            tokio::task::yield_now().await;
            continue;
        }
        let result = upload_epoch_blob(worker, proxy, id.clone(), &mut stop).await;
        let ready = match &result {
            Ok(BlobUploadStep::Observed { progress, .. })
                if !progress.linked_seen()
                    && progress.lease().state
                        == crate::proto::e2ee_v2::BlobLeaseState::Ready as i32
                    && !progress.lease().expired =>
            {
                Some(progress.plan().clone())
            }
            _ => None,
        };
        let (health, mut failed, slow) = match &result {
            Err(BlobUploadError::Stopped) => return Ok(()),
            Err(BlobUploadError::Rpc(e)) if commands::fatal_rpc(e) => {
                return Err(AccountError::Rpc(e.clone()));
            }
            Err(BlobUploadError::Rpc(RpcError::LocalDeadline)) => {
                (BlobHealth::LocalDeadline, true, false)
            }
            Err(BlobUploadError::Rejected(_)) => (BlobHealth::Unavailable, true, false),
            Err(_) => (BlobHealth::Retry, true, false),
            Ok(BlobUploadStep::Renewed(_)) => (BlobHealth::Waiting, false, false),
            Ok(BlobUploadStep::Observed { progress, .. }) => {
                use crate::proto::e2ee_v2::BlobLeaseState;
                if progress.linked_seen() {
                    (BlobHealth::Linked, false, true)
                } else if progress.lease().state == BlobLeaseState::Ready as i32
                    && !progress.lease().expired
                {
                    (BlobHealth::Ready, false, true)
                } else {
                    (BlobHealth::Waiting, false, false)
                }
            }
        };
        if let Some(plan) = ready {
            let promotion = tokio::select! {
                biased;
                _ = crate::encryption::delivery_runner::cancelled(&mut stop) => return Ok(()),
                result = proxy.promote(plan) => result,
            };
            let promoted = match promotion {
                Ok(()) => BlobPromotionHealth::Queued,
                Err(AccountError::Stopped) => return Ok(()),
                Err(AccountError::Rpc(e)) if commands::fatal_rpc(&e) => return Err(e.into()),
                Err(AccountError::SessionPending) => BlobPromotionHealth::SessionPending,
                Err(AccountError::CommandTimeout) => {
                    failed = true;
                    BlobPromotionHealth::LocalDeadline
                }
                Err(_) => {
                    failed = true;
                    BlobPromotionHealth::Retry
                }
            };
            status.send_modify(|s| {
                s.blob_promotion_health = Some(promoted);
                if promoted == BlobPromotionHealth::Queued {
                    s.blob_promotions = s.blob_promotions.saturating_add(1);
                }
            });
        }
        let entry = entries.get_mut(&id).unwrap();
        entry.failures = if failed {
            entry.failures.saturating_add(1)
        } else {
            0
        };
        let delay = if slow {
            config.retry_max.max(config.poll_interval)
        } else {
            config.delay(entry.failures)
        };
        let millis = delay.as_millis() as u64;
        entry.due =
            Instant::now() + Duration::from_millis(rand::random_range(millis.div_ceil(2)..=millis));
        status.send_modify(|s| {
            s.blob_steps = s.blob_steps.saturating_add(1);
            s.blob_health = Some(health);
        });
        tokio::task::yield_now().await;
    }
}
