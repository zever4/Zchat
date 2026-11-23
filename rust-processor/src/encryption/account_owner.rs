//! Native account lifetime. No renderer configuration, key export or spawned
//! task per command/message. One lease owns one worker, one serial RPC lane and
//! one bounded HTTP transfer.
use super::{
    delivery_runner::{RunnerConfig, RunnerError},
    discovery::{DiscoveredAccount, DiscoveryError},
    epoch_runner::{EpochDeliveryRunner, EpochDeliveryTransport, EpochPoll, NativeEpochDelivery},
    prekey_maintenance::{
        MaintenanceConfig, MaintenanceError, MaintenanceStep, PreKeyMaintenance,
        PreKeyMaintenanceTransport,
    },
    provisioning::DeviceProvisioning,
    signal_state::{CryptoCoordinator, CryptoError},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{Notify, mpsc, watch},
    task::JoinHandle,
    time::Instant,
};

#[path = "account_commands.rs"]
mod commands;
#[cfg(feature = "compression")]
pub use commands::photos::{AttachmentAdmission, FileSource, PhotoAdmission, PhotoSource};
pub use commands::{
    AccountClient, AccountUpdates, ConversationView, DialogView, OutgoingView, ReceivedText,
    RecoveryView,
};
#[path = "account_blobs.rs"]
pub(in crate::encryption) mod blobs;
#[path = "account_file_acks.rs"]
mod file_acks;
#[path = "account_file_statuses.rs"]
mod file_statuses;
#[path = "account_network.rs"]
mod network;

pub const COMMAND_CAPACITY: usize = 8;
#[derive(Clone, Copy, Default)]
pub struct AccountConfig {
    pub delivery: RunnerConfig,
    pub maintenance: MaintenanceConfig,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccountPhase {
    RecoveryIndexes,
    HistoryIndex,
    KeyIds,
    /// Local scheduling initialized; not server permission or messaging readiness.
    Running,
    Stopped,
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryHealth {
    Healthy,
    Retry,
    LocalDeadline,
    RoundTimeout,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MaintenanceHealth {
    Healthy,
    CapacityExhausted,
    Pending,
    Retry,
    LocalDeadline,
    TimedOut,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobHealth {
    Waiting,
    Ready,
    Linked,
    Retry,
    LocalDeadline,
    Unavailable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobPromotionHealth {
    Queued,
    SessionPending,
    Retry,
    LocalDeadline,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileAckHealth {
    Waiting,
    Healthy,
    Retry,
    Busy,
    LocalDeadline,
    TimedOut,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileStatusHealth {
    Initializing,
    Waiting,
    Healthy,
    Retry,
    Busy,
    LocalDeadline,
    TimedOut,
}
/// Small volatile observations, never authority for messages, consent or keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccountStatus {
    /// Volatile observations, never per-file durable status or filesystem proof.
    pub file_status_steps: u64,
    pub file_status_reads: u64,
    pub file_status_health: Option<FileStatusHealth>,
    /// Volatile scheduler observations; durable per-file truth is in SQLite.
    pub file_ack_steps: u64,
    pub file_acks: u64,
    pub file_ack_health: Option<FileAckHealth>,
    pub phase: AccountPhase,
    pub delivery_rounds: u64,
    /// Volatile count of newly stored user messages in this runtime.
    pub incoming_message_revision: u64,
    /// Volatile progress only; never proof that a particular chat may send.
    pub bootstrap_completions: u64,
    pub maintenance_steps: u64,
    pub delivery_health: Option<DeliveryHealth>,
    pub maintenance_health: Option<MaintenanceHealth>,
    pub blob_steps: u64,
    pub blob_health: Option<BlobHealth>,
    /// Volatile local handoffs, not unique messages or server/file receipts.
    pub blob_promotions: u64,
    /// Volatile retained-file step completions, never delivery/file receipts.
    pub blob_retry_steps: u64,
    pub blob_promotion_health: Option<BlobPromotionHealth>,
}
#[derive(Debug, thiserror::Error)]
pub enum AccountError {
    #[error("invalid native account configuration or command")]
    Invalid,
    #[error("native account is still initializing")]
    Initializing,
    #[error("native account command queue is full")]
    Busy,
    #[error("native attachment transport is unavailable")]
    AttachmentUnavailable,
    #[error("native attachment read failed")]
    Attachment(#[from] super::native::BlobReadError),
    #[cfg(feature = "compression")]
    #[error("native private photo preparation failed")]
    Photo(#[from] super::photo_preparation::PhotoAttachmentError),
    #[cfg(feature = "compression")]
    #[error("unsupported or invalid voice recording format")]
    VoiceFormat,
    #[error("native foreground operation timed out; its durable result may be unknown")]
    CommandTimeout,
    #[error("waiting for the peer to initiate the agreed cryptographic session")]
    SessionPending,
    #[error("native foreground RPC failed")]
    Rpc(#[from] super::native::RpcError),
    #[error("native account has stopped")]
    Stopped,
    #[error("native account runtime failed")]
    Runtime,
    #[error("native account discovery is not ready")]
    Discovery(#[from] DiscoveryError),
    #[error("protected account operation failed")]
    Crypto(#[from] CryptoError),
    #[error("native account delivery stopped")]
    Delivery(#[from] RunnerError),
    #[error("native account key maintenance stopped")]
    Maintenance(#[from] MaintenanceError),
}
type Result<T> = std::result::Result<T, AccountError>;

/// Not Clone: request handles do not own this stop lease. Explicit shutdown waits
/// for store release; dropping the lease requests the same cleanup asynchronously.
pub struct AccountOwner {
    client: AccountClient,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<()>>>,
}
impl AccountOwner {
    /// Discovery/provisioning precede this handoff. The native caller selected
    /// the trusted tuple and account store; no paths/UUID claims come from IPC.
    pub async fn start(
        account: DiscoveredAccount,
        mut local: DeviceProvisioning,
        config: AccountConfig,
    ) -> Result<Self> {
        if !config.delivery.valid() || tokio::runtime::Handle::try_current().is_err() {
            return Err(AccountError::Invalid);
        }
        let maintenance = PreKeyMaintenance::new(config.maintenance)?;
        let (client, store, own, blob_endpoint) = tokio::task::spawn_blocking(move || {
            let (client, own, blob_endpoint) = account.into_ready_client(&mut local)?;
            Ok::<_, DiscoveryError>((client, local.into_store(), own, blob_endpoint))
        })
        .await
        .map_err(|_| AccountError::Runtime)??;
        let transport = NativeEpochDelivery::new(client).map_err(DiscoveryError::from)?;
        let body = transport
            .blob_transport(blob_endpoint)
            .map_err(|_| AccountError::Invalid)?;
        let runner = EpochDeliveryRunner::new(transport, config.delivery)?;
        let worker = CryptoCoordinator::start(store, own).await?;
        Ok(Self::spawn_with_blobs(
            worker,
            runner,
            maintenance,
            Some(body),
        ))
    }
    pub fn client(&self) -> AccountClient {
        self.client.clone()
    }
    pub async fn shutdown(mut self) -> Result<()> {
        let _ = self.stop.send(true);
        self.task
            .take()
            .ok_or(AccountError::Stopped)?
            .await
            .map_err(|_| AccountError::Runtime)?
    }
    #[cfg(test)]
    pub(in crate::encryption) fn spawn<T>(
        worker: CryptoCoordinator,
        runner: EpochDeliveryRunner<T>,
        maintenance: PreKeyMaintenance,
    ) -> Self
    where
        T: EpochDeliveryTransport + PreKeyMaintenanceTransport + 'static,
    {
        Self::spawn_with_blobs(
            worker,
            runner,
            maintenance,
            None::<crate::encryption::native::NativeBlobHttp>,
        )
    }
    pub(in crate::encryption) fn spawn_with_blobs<T, B>(
        worker: CryptoCoordinator,
        mut runner: EpochDeliveryRunner<T>,
        mut maintenance: PreKeyMaintenance,
        body: Option<B>,
    ) -> Self
    where
        T: EpochDeliveryTransport + PreKeyMaintenanceTransport + 'static,
        B: blobs::Body + 'static,
    {
        let (blob_tx, blob_rx) = mpsc::channel(blobs::RPC_CAPACITY);
        let (notice_tx, notice_rx) = mpsc::channel(blobs::NOTICE_CAPACITY);
        let mut blobs = body.map(|body| blobs::Proxy::new(body, blob_tx, notice_rx));
        let blob_rx = if blobs.is_some() { Some(blob_rx) } else { None };
        let notice_tx = if blobs.is_some() {
            Some(notice_tx)
        } else {
            None
        };
        let blob_config = runner.config();
        let (tx, rx) = mpsc::channel(COMMAND_CAPACITY);
        #[cfg(feature = "compression")]
        let (photo_tx, photo_rx) = mpsc::channel(1);
        let (download_tx, download_rx) = mpsc::channel(COMMAND_CAPACITY);
        let download_tx = blobs.as_ref().map(|_| download_tx);
        let (network_tx, network_rx) = mpsc::channel(COMMAND_CAPACITY);
        let (stop_tx, mut stop) = watch::channel(false);
        let (status_tx, status) = watch::channel(AccountStatus {
            file_status_steps: 0,
            file_status_reads: 0,
            file_status_health: None,
            file_ack_steps: 0,
            file_acks: 0,
            file_ack_health: None,
            phase: AccountPhase::RecoveryIndexes,
            delivery_rounds: 0,
            incoming_message_revision: 0,
            bootstrap_completions: 0,
            maintenance_steps: 0,
            delivery_health: None,
            maintenance_health: None,
            blob_steps: 0,
            blob_health: None,
            blob_promotions: 0,
            blob_retry_steps: 0,
            blob_promotion_health: None,
        });
        let wake = Arc::new(Notify::new());
        let client = AccountClient::new(tx, network_tx.clone(), download_tx, status, stop.clone());
        #[cfg(feature = "compression")]
        let client = client.with_photos(photo_tx);
        let guard = StatusGuard(status_tx);
        let task_stop = stop_tx.clone();
        let task = tokio::spawn(async move {
            let result = async {
                if !initialize(&worker, &mut stop, &guard.0).await? {
                    return Ok(());
                }
                let network_stop = stop.clone();
                let command_stop = stop.clone();
                let blob_stop = stop.clone();
                #[cfg(feature = "compression")]
                let photo_stop = stop.clone();
                let photo_lane = async {
                    #[cfg(feature = "compression")]
                    return commands::photos::serve(&worker, photo_rx, photo_stop, &wake).await;
                    #[cfg(not(feature = "compression"))]
                    std::future::pending::<Result<()>>().await
                };
                // Bounded futures in ONE task. Only network uses registry/RPC;
                // HTTP waits do not starve text/control/delivery/maintenance.
                tokio::select! {
                    biased;
                    _ = super::delivery_runner::cancelled(&mut stop) => Ok(()),
                    r = network::run(&worker, &mut runner, &mut maintenance,
                        network_stop, &guard.0, &wake, network::Queues { commands: network_rx, blobs: blob_rx, notices:notice_tx }) => r,
                    r = commands::serve(&worker, rx, network_tx, command_stop, &wake) => r,
                    r = photo_lane => r,
                    r = blobs::run(&worker, &mut blobs, blob_config, blob_stop, &guard.0, &wake, download_rx) => r,
                }
            }
            .await;
            // Stop all request handles BEFORE potentially slow HTTP/SQLite
            // cleanup, including when a role failed without explicit logout.
            let _ = task_stop.send(true);
            // The cleanup task outlives a dropped shutdown waiter. Keep store
            // ownership until LAST HTTP body/chunk releases the file permit.
            if let Some(body) = &mut blobs {
                use crate::encryption::blob_upload::EpochBlobUploadTransport;
                body.cancel();
                body.join().await;
            }
            drop(blobs);
            // All borrowing futures are gone before transport secrets and the
            // worker/store are closed. A canceled caller cannot skip this await.
            drop(runner);
            let closed = worker.shutdown().await.map_err(AccountError::from);
            let result = result.and(closed);
            guard.0.send_modify(|s| {
                s.phase = if result.is_ok() {
                    AccountPhase::Stopped
                } else {
                    AccountPhase::Failed
                };
            });
            result
        });
        Self {
            client,
            stop: stop_tx,
            task: Some(task),
        }
    }
}
impl Drop for AccountOwner {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
struct StatusGuard(watch::Sender<AccountStatus>);
impl Drop for StatusGuard {
    fn drop(&mut self) {
        self.0.send_modify(|s| {
            if !matches!(s.phase, AccountPhase::Stopped | AccountPhase::Failed) {
                s.phase = AccountPhase::Failed;
            }
        });
    }
}
async fn initialize(
    worker: &CryptoCoordinator,
    stop: &mut watch::Receiver<bool>,
    status: &watch::Sender<AccountStatus>,
) -> Result<bool> {
    for phase in [
        AccountPhase::RecoveryIndexes,
        AccountPhase::HistoryIndex,
        AccountPhase::KeyIds,
    ] {
        status.send_modify(|s| s.phase = phase);
        loop {
            let ready = tokio::select! {
                biased;
                _ = super::delivery_runner::cancelled(stop) => return Ok(false),
                r = async {
                    if phase == AccountPhase::RecoveryIndexes {
                        worker.seed_recovery_indexes().await.map(|p| p.ready)
                    } else if phase == AccountPhase::HistoryIndex {
                        worker.seed_history_index().await.map(|p|p.ready)
                    } else {
                        worker.seed_prekey_ids().await.map(|p| p.ready)
                    }
                } => r?,
            };
            if ready {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
    status.send_modify(|s| s.phase = AccountPhase::Running);
    Ok(true)
}
