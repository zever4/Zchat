//! Native-only maintenance, called by the future account network owner.
//! One bounded step/one RPC; no spawned timer, per-message task or implicit GC.
use super::{
    native::{NativeV1Client, RpcError},
    protected::StoreScope,
    signal_state::{
        CryptoCoordinator, CryptoError, PreKeyPublicationPlan, PreKeyRefillUpload,
        VerifiedPreKeyRefillUpload,
    },
};
use crate::proto::e2ee::{DeviceRef, FailureReason, PreKeyInventory};
use async_trait::async_trait;
use rand::Rng;
use std::time::Duration;
use tokio::{sync::watch, time::Instant};
use tonic::Code;

/// Public counts, but bound to the trusted origin/account/credential/generation.
/// An inventory is an observation, NEVER a publication receipt or authority to
/// rebase local revision, replace keys or discard private material.
#[derive(Clone)]
pub struct VerifiedPreKeyInventory {
    scope: StoreScope,
    value: PreKeyInventory,
}
impl VerifiedPreKeyInventory {
    pub fn value(&self) -> &PreKeyInventory {
        &self.value
    }
    pub(super) fn scope(&self) -> &StoreScope {
        &self.scope
    }
    pub(super) fn from_rpc(
        scope: StoreScope,
        own: &DeviceRef,
        v: PreKeyInventory,
    ) -> Result<Self, RpcError> {
        if v.own_device.as_ref() != Some(own)
            || v.max_retained_ec != 4096
            || v.max_retained_pq != 4096
            || v.max_retained_signed != 128
            || v.max_publication_batches != 8192
            || v.retained_ec > v.max_retained_ec
            || v.retained_pq > v.max_retained_pq
            || v.retained_signed > v.max_retained_signed
            || v.publication_batches > v.max_publication_batches
            || v.available_ec > v.retained_ec
            || v.available_pq > v.retained_pq
            || v.publication_revision != u64::from(v.publication_batches)
            || v.active_signed_id.is_some() != v.active_signed_created_at_unix_ms.is_some()
            || v.active_signed_created_at_unix_ms.is_some_and(|t| t < 0)
            || v.active_signed_id.is_some() && v.retained_signed == 0
            || v.active_last_resort_id.is_some() && v.available_pq >= v.retained_pq
            || v.public_material_ready
                != (v.active_signed_id.is_some() && v.active_last_resort_id.is_some())
            || v.publication_revision == 0
                && (v.retained_ec != 0 || v.retained_pq != 0 || v.retained_signed != 0)
        {
            return Err(RpcError::Invalid);
        }
        Ok(Self { scope, value: v })
    }
}

/// Refill each kind independently when below 8, towards 32 available keys.
/// Small MVP batch: never more than 32 fresh keys/kind and two RPC batches.
#[derive(Clone, Copy, Debug)]
pub struct RefillPolicy {
    pub low_water: u32,
    pub target: u32,
}
impl Default for RefillPolicy {
    fn default() -> Self {
        Self {
            low_water: 8,
            target: 32,
        }
    }
}
impl RefillPolicy {
    pub(super) fn valid(self) -> bool {
        self.low_water > 0 && self.low_water <= self.target && self.target <= 32
    }
    pub(super) fn amounts(self, v: &PreKeyInventory) -> Option<(u32, u32)> {
        let wanted = |available| {
            if available < self.low_water {
                self.target - available
            } else {
                0
            }
        };
        let ec = wanted(v.available_ec);
        let pq = wanted(v.available_pq);
        if ec + pq == 0 {
            return None;
        }
        let slots = v.max_publication_batches - v.publication_batches;
        if slots == 0 {
            return Some((0, 0));
        }
        // Worst-case uint32 IDs/varints: 32EC + 28PQ fit one 48KiB
        // batch; 32PQ need at most two. Do not strand a partial plan at quota.
        Some((
            ec.min(v.max_retained_ec - v.retained_ec),
            pq.min(v.max_retained_pq - v.retained_pq)
                .min(if slots == 1 { 28 } else { 32 }),
        ))
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum InventoryRefill {
    Healthy,
    CapacityExhausted,
    Publication(PreKeyPublicationPlan),
}

#[async_trait]
pub trait PreKeyMaintenanceTransport: Send {
    async fn inventory(&mut self) -> Result<VerifiedPreKeyInventory, RpcError>;
    async fn upload(
        &mut self,
        plan: &PreKeyRefillUpload,
    ) -> Result<VerifiedPreKeyRefillUpload, RpcError>;
}
#[async_trait]
impl PreKeyMaintenanceTransport for NativeV1Client {
    async fn inventory(&mut self) -> Result<VerifiedPreKeyInventory, RpcError> {
        self.prekey_inventory().await
    }
    async fn upload(
        &mut self,
        plan: &PreKeyRefillUpload,
    ) -> Result<VerifiedPreKeyRefillUpload, RpcError> {
        self.upload_refill(plan).await
    }
}

#[derive(Clone, Copy, Debug)]
pub struct MaintenanceConfig {
    pub policy: RefillPolicy,
    pub check_interval: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub step_timeout: Duration,
}
impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            policy: RefillPolicy::default(),
            check_interval: Duration::from_secs(300),
            retry_initial: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            step_timeout: Duration::from_secs(15),
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum MaintenanceError {
    #[error("invalid prekey maintenance configuration")]
    Configuration,
    #[error("prekey maintenance RPC failed")]
    Rpc(#[from] RpcError),
    #[error("prekey maintenance local state failed")]
    Crypto(#[from] CryptoError),
}
#[derive(Debug)]
pub enum MaintenanceStep {
    Waiting,
    Stopped,
    Halted,
    Healthy,
    CapacityExhausted,
    Planned(PreKeyPublicationPlan),
    Uploaded { complete: bool },
    Retry(MaintenanceError),
    TimedOut,
}

/// Single account owner calls tick only when due, on the SAME serial network
/// lane as delivery. It must initialize the bounded ID allocator first and drop
/// this scheduler on logout. Timers/backoff are volatile; durable plans are not.
/// Dropping/canceling an RPC may leave a remote/local COMMIT: next tick resumes
/// the exact protected plan BEFORE fetching inventory, including rotation.
pub struct PreKeyMaintenance {
    config: MaintenanceConfig,
    due: Instant,
    failures: u32,
    halted: bool,
}
impl PreKeyMaintenance {
    pub fn new(config: MaintenanceConfig) -> Result<Self, MaintenanceError> {
        let valid =
            |d: Duration| (Duration::from_millis(1)..=Duration::from_secs(3600)).contains(&d);
        if !config.policy.valid()
            || !valid(config.check_interval)
            || config.check_interval < Duration::from_secs(30)
            || !valid(config.retry_initial)
            || config.retry_initial < Duration::from_millis(100)
            || !valid(config.retry_max)
            || !valid(config.step_timeout)
            || config.retry_initial > config.retry_max
        {
            return Err(MaintenanceError::Configuration);
        }
        Ok(Self {
            config,
            due: Instant::now(),
            failures: 0,
            halted: false,
        })
    }
    pub fn next_due(&self) -> Instant {
        self.due
    }
    // Controlled monotonic scheduling probes; absent from production builds.
    #[cfg(test)]
    pub(in crate::encryption) fn due_for_test(&mut self) {
        self.due = Instant::now();
    }
    #[cfg(test)]
    pub(in crate::encryption) fn failures_for_test(&mut self, value: u32) {
        self.failures = value;
    }
    #[cfg(test)]
    pub(in crate::encryption) fn delay_for_test(&self, failed: bool, pending: bool) -> Duration {
        self.delay(failed, pending)
    }
    fn delay(&self, failed: bool, pending: bool) -> Duration {
        let base = if failed {
            self.config
                .retry_initial
                .saturating_mul(1u32 << self.failures.saturating_sub(1).min(20))
                .min(self.config.retry_max)
        } else if pending {
            self.config.retry_initial
        } else {
            self.config.check_interval
        };
        // 80..100%, bounded even at retry_max. Never zero/busy-loop. No clock dates.
        base.mul_f64(rand::rng().random_range(0.8..=1.0))
            .max(Duration::from_millis(1))
    }
    pub async fn tick<T: PreKeyMaintenanceTransport>(
        &mut self,
        worker: &CryptoCoordinator,
        transport: &mut T,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<MaintenanceStep, MaintenanceError> {
        if *stop.borrow() || stop.has_changed().is_err() {
            return Ok(MaintenanceStep::Stopped);
        }
        if self.halted {
            return Ok(MaintenanceStep::Halted);
        }
        if Instant::now() < self.due {
            return Ok(MaintenanceStep::Waiting);
        }
        let result = tokio::select! {
            biased;
            _ = stopped(stop) => return Ok(MaintenanceStep::Stopped),
            result = tokio::time::timeout(self.config.step_timeout, step(worker, transport, self.config.policy)) => result,
        };
        let outcome = match result {
            Err(_) => MaintenanceStep::TimedOut,
            Ok(Ok(value)) => value,
            Ok(Err(error)) if fatal(&error) => {
                self.halted = true;
                return Err(error);
            }
            Ok(Err(error)) => MaintenanceStep::Retry(error),
        };
        let failed = matches!(
            outcome,
            MaintenanceStep::Retry(_) | MaintenanceStep::TimedOut
        );
        self.failures = if failed {
            self.failures.saturating_add(1)
        } else {
            0
        };
        let pending = matches!(
            outcome,
            MaintenanceStep::Planned(_) | MaintenanceStep::Uploaded { complete: false }
        );
        self.due = Instant::now() + self.delay(failed, pending);
        Ok(outcome)
    }
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}
fn fatal(e: &MaintenanceError) -> bool {
    match e {
        MaintenanceError::Configuration => true,
        MaintenanceError::Rpc(RpcError::Invalid) => true,
        MaintenanceError::Rpc(RpcError::Failure {
            reason: FailureReason::GenerationChanged,
            ..
        }) => true,
        MaintenanceError::Rpc(RpcError::Status(code) | RpcError::Failure { code, .. }) => matches!(
            code,
            Code::Unauthenticated
                | Code::PermissionDenied
                | Code::Unimplemented
                | Code::AlreadyExists
                | Code::Aborted
        ),
        MaintenanceError::Crypto(CryptoError::Busy | CryptoError::Conflict) => false,
        MaintenanceError::Crypto(_) => true,
        MaintenanceError::Rpc(RpcError::Transport | RpcError::LocalDeadline) => false,
    }
}
async fn step<T: PreKeyMaintenanceTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    policy: RefillPolicy,
) -> Result<MaintenanceStep, MaintenanceError> {
    if let Some(mut p) = worker
        .current_prekey_publication()
        .await?
        .filter(|p| !p.complete)
    {
        if !p.materialized {
            p = worker.materialize_prekey_refill(p.operation_id).await?;
        }
        let upload = worker
            .next_prekey_refill_upload()
            .await?
            .ok_or(CryptoError::Conflict)?;
        worker
            .accept_prekey_refill_upload(transport.upload(&upload).await?)
            .await?;
        let complete = worker
            .current_prekey_publication()
            .await?
            .is_some_and(|head| head.operation_id == p.operation_id && head.complete);
        return Ok(MaintenanceStep::Uploaded { complete });
    }
    let inventory = transport.inventory().await?;
    Ok(
        match worker.refill_from_inventory(inventory, policy).await? {
            InventoryRefill::Healthy => MaintenanceStep::Healthy,
            InventoryRefill::CapacityExhausted => MaintenanceStep::CapacityExhausted,
            InventoryRefill::Publication(p) => MaintenanceStep::Planned(p),
        },
    )
}
