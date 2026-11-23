//! Production v2 orchestration: one serial native network lane, durable worker
//! truth and bounded rotating work. Never creates consent, plaintext or a reset.
use super::{
    delivery_runner::{
        self as policy, DeliveryIssue, Issue, Operation, RoundReport, RunnerConfig, RunnerError,
    },
    native::{
        EpochAcquisitionOutcome, EpochDelivery, EpochObservation, EpochPreparedSendOutcome,
        EpochRecoveryError, EpochStatuses, InboxWakeups, NativeEpochClient, NativeV1Client,
        RpcError,
    },
    signal_state::{
        ConversationCreation, ConversationLookup, CryptoCoordinator, CryptoError, EpochAckCursor,
        EpochAcquisitionPlan, EpochCatalogQuery, EpochConversationCursor, EpochDurableAck,
        EpochOutboxCursor, EpochOutgoingPlan, EpochRecoveryIntent, EpochRecoveryQuery,
        EpochRecoverySnapshot, EpochScope, EpochTransitionQuery, VerifiedConversationCreation,
        VerifiedConversationLookup,
    },
};
use crate::proto::{e2ee as common, e2ee_v2 as v2};
use prost::Message;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Notify, watch};
use tonic::Code;
#[path = "epoch_runner_transport.rs"]
mod transport;
pub use transport::*;
#[path = "epoch_runner_recovery.rs"]
mod recovery;
pub const MAX_ROUND_RPCS: usize = 64;
const CHAT_BUDGET: usize = 2;
/// Bounded observations only. These IDs/states contain no plaintext, credentials
/// or signatures; protected journals remain authoritative after report eviction.
pub struct RecoveryNotice {
    pub conversation_id: String,
    pub snapshot: EpochRecoverySnapshot,
}
/// Scheduling hint only. Host-monotonic Send START time lets the account reject
/// failures from before an unknown HTTP outcome. Never a DB lease/expiry clock.
pub struct AttachmentRetryNotice {
    pub(in crate::encryption) failure: crate::encryption::native::EpochBlobSendFailure,
    pub(in crate::encryption) send_started: tokio::time::Instant,
}
impl AttachmentRetryNotice {
    pub fn into_failure(self) -> crate::encryption::native::EpochBlobSendFailure {
        self.failure
    }
}
#[derive(Default)]
pub struct EpochRoundReport {
    pub delivery: RoundReport,
    pub rpc_attempts: usize,
    pub sealed: usize,
    pub activated: usize,
    pub bootstrapped: usize,
    pub recovery: Vec<RecoveryNotice>,
    /// Bounded exact observations for the native attachment lane, not receipts.
    /// A new round/restart must Send again; no durable unsent/renew grant here.
    pub attachment_retries: Vec<AttachmentRetryNotice>,
}
pub struct EpochDeliveryRunner<T> {
    transport: T,
    config: RunnerConfig,
    outbox_after: Option<EpochOutboxCursor>,
    ack_after: Option<EpochAckCursor>,
    chat_after: Option<EpochConversationCursor>,
    remote_after: String,
    next_lane: u8,
    last_report: Option<EpochRoundReport>,
    incoming_message_revision: u64,
}
pub(in crate::encryption) enum EpochPoll {
    Stopped,
    Complete,
}
impl<T: EpochDeliveryTransport> EpochDeliveryRunner<T> {
    pub fn new(transport: T, config: RunnerConfig) -> Result<Self, RunnerError> {
        if !config.valid() {
            return Err(RunnerError::Configuration);
        }
        Ok(Self {
            transport,
            config,
            outbox_after: None,
            ack_after: None,
            chat_after: None,
            remote_after: String::new(),
            next_lane: 0,
            last_report: None,
            incoming_message_revision: 0,
        })
    }
    pub fn last_report(&self) -> Option<&EpochRoundReport> {
        self.last_report.as_ref()
    }
    pub(super) fn incoming_message_revision(&self) -> u64 {
        self.incoming_message_revision
    }
    pub(in crate::encryption) fn take_attachment_retries(&mut self) -> Vec<AttachmentRetryNotice> {
        self.last_report
            .as_mut()
            .map(|r| std::mem::take(&mut r.attachment_retries))
            .unwrap_or_default()
    }
    pub(in crate::encryption) fn config(&self) -> RunnerConfig {
        self.config
    }
    pub(in crate::encryption) fn transport_mut(&mut self) -> &mut T {
        &mut self.transport
    }
    /// Shared by standalone polling and the serial account scheduler. Cursors
    /// stay on this runner across maintenance and foreground commands.
    pub(in crate::encryption) async fn poll_bounded(
        &mut self,
        w: &CryptoCoordinator,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<EpochPoll, RunnerError> {
        let timeout = self.config.round_timeout;
        let result = tokio::select! {
            biased;
            _ = policy::cancelled(stop) => return Ok(EpochPoll::Stopped),
            r = tokio::time::timeout(timeout, self.poll_once(w)) => r,
        };
        self.last_report = Some(match result {
            Ok(r) => r?,
            Err(_) => EpochRoundReport {
                delivery: RoundReport {
                    issues: vec![Issue {
                        operation: Operation::Round,
                        error: DeliveryIssue::Timeout,
                    }],
                    ..Default::default()
                },
                ..Default::default()
            },
        });
        Ok(EpochPoll::Complete)
    }
    fn reserve(r: &mut EpochRoundReport) -> bool {
        if r.rpc_attempts == MAX_ROUND_RPCS {
            return false;
        }
        r.rpc_attempts += 1;
        true
    }
    pub async fn poll_once(
        &mut self,
        w: &CryptoCoordinator,
    ) -> Result<EpochRoundReport, RunnerError> {
        let mut r = EpochRoundReport::default();
        for _ in 0..4 {
            if r.rpc_attempts == MAX_ROUND_RPCS {
                break;
            }
            let lane = self.next_lane;
            self.next_lane = (lane + 1) % 4;
            let result = match lane {
                0 => self.acks(w, &mut r).await,
                1 => self.outbox(w, &mut r).await,
                2 => self.recover(w, &mut r).await,
                _ => self.receive(w, &mut r).await,
            };
            match result {
                Err(RunnerError::Crypto(CryptoError::Busy)) => {
                    r.delivery.crypto(Operation::Round, CryptoError::Busy)?;
                }
                Err(e) => return Err(e),
                Ok(()) => {}
            }
        }
        Ok(r)
    }
    async fn ack(
        &mut self,
        w: &CryptoCoordinator,
        p: &EpochDurableAck,
        op: Operation,
        r: &mut EpochRoundReport,
    ) -> Result<bool, RunnerError> {
        if !Self::reserve(r) {
            return Ok(true);
        }
        match self.transport.acknowledge(p).await {
            Ok(o) => {
                if let Err(e) = w.accept_epoch_ack(o).await {
                    r.delivery.local_busy(op, e)?;
                    return Ok(true);
                }
                r.delivery.acknowledged += 1;
                Ok(false)
            }
            Err(e) => r.delivery.rpc(op, e),
        }
    }
    async fn acks(
        &mut self,
        w: &CryptoCoordinator,
        r: &mut EpochRoundReport,
    ) -> Result<(), RunnerError> {
        let page = match w.epoch_ack_page(self.ack_after.clone()).await {
            Ok(p) => p,
            Err(e) => return r.delivery.local_busy(Operation::RecoverAck, e),
        };
        for p in &page.acknowledgements {
            if r.rpc_attempts == MAX_ROUND_RPCS {
                return Ok(());
            }
            self.ack_after = Some(p.cursor());
            if self.ack(w, p, Operation::RecoverAck, r).await? {
                return Ok(());
            }
        }
        self.ack_after = page.next;
        Ok(())
    }
    async fn send_plan(
        &mut self,
        w: &CryptoCoordinator,
        p: &EpochOutgoingPlan,
        r: &mut EpochRoundReport,
    ) -> Result<bool, RunnerError> {
        if !Self::reserve(r) {
            return Ok(true);
        }
        let send_started = tokio::time::Instant::now();
        match self.transport.send(p).await {
            Ok(EpochPreparedSendOutcome::Accepted(o)) => {
                if let Err(e) = w.accept_epoch_send_receipt(*o).await {
                    r.delivery.local_busy(Operation::Send, e)?;
                    return Ok(true);
                }
                r.delivery.sent += 1;
                Ok(false)
            }
            Ok(EpochPreparedSendOutcome::Sealed(o)) => {
                // Fence alone cannot adopt consent. Fetch the exact recovery record
                // and let the worker require the previously persisted local intent.
                let env = p.request().envelope.as_ref().ok_or(RpcError::Invalid)?;
                let scope = w
                    .epoch_scope(
                        env.conversation_id.clone(),
                        env.crypto_epoch.ok_or(RpcError::Invalid)?,
                    )
                    .await?;
                let id = o
                    .response()
                    .recovery
                    .as_ref()
                    .and_then(|o| o.proposal.as_ref())
                    .and_then(|p| p.control.as_ref())
                    .ok_or(RpcError::Invalid)?
                    .proposal_id
                    .clone();
                let q = w.prepare_epoch_recovery_query(scope, id).await?;
                if !Self::reserve(r) {
                    return Ok(true);
                }
                let record = match self.transport.query(&q).await {
                    Ok(o) => o,
                    Err(e) => return r.delivery.rpc(Operation::Recovery, e),
                };
                match w.accept_epoch_recovery_query(record).await {
                    Ok(snapshot) => r.recovery.push(RecoveryNotice {
                        conversation_id: env.conversation_id.clone(),
                        snapshot,
                    }),
                    Err(e) => return r.delivery.crypto(Operation::Recovery, e),
                }
                if let Err(e) = w.accept_epoch_send_fence(*o).await {
                    return r.delivery.crypto(Operation::Send, e);
                }
                r.sealed += 1;
                Ok(false)
            }
            Ok(EpochPreparedSendOutcome::BlobUnavailable(o)) => {
                r.delivery
                    .rpc(Operation::Send, RpcError::Status(Code::FailedPrecondition))?;
                r.attachment_retries.push(AttachmentRetryNotice {
                    failure: *o,
                    send_started,
                });
                Ok(false)
            }
            Err(e) => r.delivery.rpc(Operation::Send, e),
        }
    }
    async fn outbox(
        &mut self,
        w: &CryptoCoordinator,
        r: &mut EpochRoundReport,
    ) -> Result<(), RunnerError> {
        let page = match w.epoch_outbox_page(self.outbox_after.clone()).await {
            Ok(p) => p,
            Err(e) => return r.delivery.local_busy(Operation::Send, e),
        };
        let mut refresh = Vec::new();
        for p in &page.messages {
            if r.rpc_attempts == MAX_ROUND_RPCS {
                return Ok(());
            }
            self.outbox_after = Some(p.cursor());
            if p.needs_send() {
                if self.send_plan(w, p, r).await? {
                    return Ok(());
                }
            } else if p.needs_status_poll() {
                refresh.push(p.clone());
            }
        }
        self.outbox_after = page.next;
        if !refresh.is_empty() && Self::reserve(r) {
            match self.transport.statuses(&refresh).await {
                Ok(o) => match w.accept_epoch_statuses(o).await {
                    Ok(p) => r.delivery.statuses += p.len(),
                    Err(e) => r.delivery.local_busy(Operation::Status, e)?,
                },
                Err(e) => {
                    r.delivery.rpc(Operation::Status, e)?;
                }
            }
        }
        Ok(())
    }
    async fn route(
        &mut self,
        w: &CryptoCoordinator,
        chat: &str,
        r: &mut EpochRoundReport,
    ) -> Result<Option<EpochScope>, RunnerError> {
        let q = w.prepare_conversation_lookup(chat.into()).await?;
        if !Self::reserve(r) {
            return Ok(None);
        }
        match self.transport.lookup(&q).await {
            Ok(o) => {
                if let Err(e) = w.accept_conversation_lookup(o).await {
                    r.delivery.local_busy(Operation::Lookup, e)?;
                    return Ok(None);
                }
            }
            Err(e) => {
                r.delivery.rpc(Operation::Lookup, e)?;
                return Ok(None);
            }
        }
        if let Some(s) = w.active_epoch_scope(chat.into()).await? {
            return Ok(Some(s));
        }
        let q = w.prepare_initial_epoch(chat.into()).await?;
        if !Self::reserve(r) {
            return Ok(None);
        }
        let o = match self.transport.initial(&q).await {
            Ok(o) => o,
            Err(e) => {
                r.delivery.rpc(Operation::Initialize, e)?;
                return Ok(None);
            }
        };
        match w.accept_initial_epoch(o).await {
            Ok(s) => Ok(Some(s)),
            Err(e) => {
                r.delivery.crypto(Operation::Initialize, e)?;
                Ok(None)
            }
        }
    }
    async fn receive(
        &mut self,
        w: &CryptoCoordinator,
        r: &mut EpochRoundReport,
    ) -> Result<(), RunnerError> {
        if !Self::reserve(r) {
            return Ok(());
        }
        let page = match self.transport.pending().await {
            Ok(p) => p,
            Err(e) => {
                r.delivery.rpc(Operation::Pending, e)?;
                return Ok(());
            }
        };
        if page.len() > 32
            || page
                .iter()
                .map(|d| d.response().encoded_len().saturating_add(6))
                .fold(0usize, usize::saturating_add)
                > super::contract::MAX_RESPONSE_BYTES
        {
            return Err(RpcError::Invalid.into());
        }
        let mut scopes = HashMap::new();
        for d in page {
            if r.rpc_attempts == MAX_ROUND_RPCS {
                return Ok(());
            }
            let env = d.response().envelope.as_ref().ok_or(RpcError::Invalid)?;
            let chat = env.conversation_id.clone();
            let epoch = env.crypto_epoch.ok_or(RpcError::Invalid)?;
            if !scopes.contains_key(&chat) {
                let s = self.route(w, &chat, r).await?;
                scopes.insert(chat.clone(), s);
            }
            if scopes.get(&chat).is_none_or(Option::is_none) {
                continue;
            }
            let s = match w.epoch_scope(chat, epoch).await {
                Ok(s) => s,
                Err(e) => {
                    if r.delivery.crypto(Operation::Receive, e)? {
                        return Ok(());
                    }
                    continue;
                }
            };
            let uuid = env.message_id.clone();
            let proof = match w.receive_epoch(s, d).await {
                Ok((_, proof)) => {
                    if proof.new_message() {
                        self.incoming_message_revision =
                            self.incoming_message_revision.saturating_add(1);
                    }
                    r.delivery.received += 1;
                    proof
                }
                Err(CryptoError::Quarantined) => w.epoch_ack(uuid).await?,
                Err(e) => {
                    if r.delivery.crypto(Operation::Receive, e)? {
                        return Ok(());
                    }
                    continue;
                }
            };
            if self.ack(w, &proof, Operation::Acknowledge, r).await? {
                return Ok(());
            }
        }
        Ok(())
    }
    pub async fn run(
        &mut self,
        w: &CryptoCoordinator,
        stop: watch::Receiver<bool>,
    ) -> Result<(), RunnerError> {
        self.run_core(w, stop, None).await
    }
    pub async fn run_with_wakeups(
        &mut self,
        w: &CryptoCoordinator,
        stop: watch::Receiver<bool>,
        mut wakeups: InboxWakeups,
    ) -> Result<(), RunnerError> {
        let hints = Arc::new(Notify::new());
        tokio::select! {result=self.run_core(w,stop.clone(),Some(hints.clone()))=>result,_=wakeups.run(hints,stop)=>Ok(())}
    }
    async fn run_core(
        &mut self,
        w: &CryptoCoordinator,
        mut stop: watch::Receiver<bool>,
        hints: Option<Arc<Notify>>,
    ) -> Result<(), RunnerError> {
        let initial = Duration::from_millis(rand::random_range(
            0..=self.config.poll_interval.as_millis() as u64,
        ));
        if policy::wait(initial, &mut stop, hints.as_deref()).await {
            return Ok(());
        }
        let mut failures = 0u32;
        loop {
            if matches!(self.poll_bounded(w, &mut stop).await?, EpochPoll::Stopped) {
                return Ok(());
            }
            let report = self.last_report.as_ref().expect("completed bounded poll");
            failures = if report.delivery.issues.is_empty() {
                0
            } else {
                failures.saturating_add(1)
            };
            let millis = self.config.delay(failures).as_millis() as u64;
            let delay = Duration::from_millis(rand::random_range(millis.div_ceil(2)..=millis));
            if policy::wait(
                delay,
                &mut stop,
                if failures == 0 {
                    hints.as_deref()
                } else {
                    None
                },
            )
            .await
            {
                return Ok(());
            }
        }
    }
}
