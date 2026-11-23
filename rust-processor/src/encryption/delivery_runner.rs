//! Poll-first native delivery. One serial network lane, bounded recovery pages,
//! no spawned tasks, outbound bootstrap/replacement, or automatic quarantine.
//! The account worker atomically prepares required session confirmations.
//! The owner must run only one runner per account and cancel it before logout.
use super::{
    native::{InboxWakeups, NativeV1Client, RpcError},
    signal_state::{
        ConversationLookup, CryptoCoordinator, CryptoError, DurableAck, OutgoingPlan, SessionScope,
        VerifiedAck, VerifiedConversationLookup, VerifiedMessageStatuses, VerifiedSendReceipt,
    },
};
use crate::proto::e2ee::{FailureReason, MessageState, PendingMessages};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{Notify, watch};
use tonic::Code;

/// Trusted native adapter; implementations must validate the same bindings and
/// limits as NativeV1Client. Neither this interface nor its proofs are renderer IPC.
#[async_trait::async_trait]
pub trait DeliveryTransport: Send {
    async fn send(&mut self, plan: &OutgoingPlan) -> Result<VerifiedSendReceipt, RpcError>;
    async fn statuses(
        &mut self,
        plans: &[OutgoingPlan],
    ) -> Result<VerifiedMessageStatuses, RpcError>;
    async fn pending(&mut self) -> Result<PendingMessages, RpcError>;
    async fn lookup(
        &mut self,
        plan: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError>;
    async fn acknowledge(&mut self, proof: &DurableAck) -> Result<VerifiedAck, RpcError>;
}
#[async_trait::async_trait]
impl DeliveryTransport for NativeV1Client {
    async fn send(&mut self, plan: &OutgoingPlan) -> Result<VerifiedSendReceipt, RpcError> {
        self.send_outbox(plan).await
    }
    async fn statuses(
        &mut self,
        plans: &[OutgoingPlan],
    ) -> Result<VerifiedMessageStatuses, RpcError> {
        self.refresh_outgoing(plans).await
    }
    async fn pending(&mut self) -> Result<PendingMessages, RpcError> {
        NativeV1Client::pending(self).await
    }
    async fn lookup(
        &mut self,
        plan: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError> {
        self.get_prepared_conversation(plan).await
    }
    async fn acknowledge(&mut self, proof: &DurableAck) -> Result<VerifiedAck, RpcError> {
        NativeV1Client::acknowledge(self, proof).await
    }
}
#[async_trait::async_trait]
impl<T: DeliveryTransport + ?Sized> DeliveryTransport for &mut T {
    async fn send(&mut self, plan: &OutgoingPlan) -> Result<VerifiedSendReceipt, RpcError> {
        T::send(self, plan).await
    }
    async fn statuses(
        &mut self,
        plans: &[OutgoingPlan],
    ) -> Result<VerifiedMessageStatuses, RpcError> {
        T::statuses(self, plans).await
    }
    async fn pending(&mut self) -> Result<PendingMessages, RpcError> {
        T::pending(self).await
    }
    async fn lookup(
        &mut self,
        plan: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError> {
        T::lookup(self, plan).await
    }
    async fn acknowledge(&mut self, proof: &DurableAck) -> Result<VerifiedAck, RpcError> {
        T::acknowledge(self, proof).await
    }
}

#[derive(Clone, Copy)]
pub struct RunnerConfig {
    pub poll_interval: Duration,
    pub retry_initial: Duration,
    pub retry_max: Duration,
    pub round_timeout: Duration,
}
impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(3),
            retry_initial: Duration::from_secs(1),
            retry_max: Duration::from_secs(30),
            round_timeout: Duration::from_secs(20),
        }
    }
}
impl RunnerConfig {
    pub(super) fn valid(&self) -> bool {
        [
            self.poll_interval,
            self.retry_initial,
            self.retry_max,
            self.round_timeout,
        ]
        .into_iter()
        .all(|d| d >= Duration::from_millis(1) && d <= Duration::from_secs(300))
            && self.retry_initial <= self.retry_max
    }
    pub(super) fn delay(&self, failures: u32) -> Duration {
        if failures == 0 {
            self.poll_interval
        } else {
            self.retry_initial
                .saturating_mul(1u32 << failures.saturating_sub(1).min(31))
                .min(self.retry_max)
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("invalid delivery runner configuration")]
    Configuration,
    #[error("delivery requires native authentication/configuration reconciliation")]
    Rpc(#[from] RpcError),
    #[error("delivery requires protected worker reconciliation")]
    Crypto(#[from] CryptoError),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Recovery,
    Transition,
    Bootstrap,
    Initialize,
    RecoverAck,
    Send,
    Status,
    Pending,
    Lookup,
    Receive,
    Acknowledge,
    Round,
}
#[derive(Debug, thiserror::Error)]
pub enum DeliveryIssue {
    #[error("RPC attempt failed")]
    Rpc(#[from] RpcError),
    #[error("message processing was deferred; no automatic quarantine")]
    Crypto(#[from] CryptoError),
    #[error("delivery round deadline exceeded; persisted operations remain recoverable")]
    Timeout,
}
#[derive(Debug)]
pub struct Issue {
    pub operation: Operation,
    pub error: DeliveryIssue,
}
/// Bounded, in-memory observation, never a source of delivery truth. No plaintext,
/// credentials, envelopes or unbounded event queue are retained by the runner.
#[derive(Default, Debug)]
pub struct RoundReport {
    pub acknowledged: usize,
    pub sent: usize,
    pub statuses: usize,
    pub received: usize,
    pub issues: Vec<Issue>,
}
impl RoundReport {
    pub(super) fn local_busy(
        &mut self,
        operation: Operation,
        error: CryptoError,
    ) -> Result<(), RunnerError> {
        if matches!(error, CryptoError::Busy) {
            self.crypto(operation, error)?;
            Ok(())
        } else {
            Err(error.into())
        }
    }
    /// True means this lane should yield after a transient transport failure.
    pub(super) fn rpc(
        &mut self,
        operation: Operation,
        error: RpcError,
    ) -> Result<bool, RunnerError> {
        let code = match &error {
            RpcError::Invalid => return Err(error.into()),
            RpcError::Failure {
                reason: FailureReason::GenerationChanged,
                ..
            } => return Err(error.into()),
            RpcError::Failure { code, .. } | RpcError::Status(code) => Some(*code),
            RpcError::Transport | RpcError::LocalDeadline => None,
        };
        if matches!(
            code,
            Some(Code::Unauthenticated | Code::PermissionDenied | Code::Unimplemented)
        ) {
            return Err(error.into());
        }
        let yield_lane = !matches!(
            code,
            Some(Code::NotFound | Code::AlreadyExists | Code::FailedPrecondition)
        );
        self.issues.push(Issue {
            operation,
            error: error.into(),
        });
        Ok(yield_lane)
    }
    pub(super) fn crypto(
        &mut self,
        operation: Operation,
        error: CryptoError,
    ) -> Result<bool, RunnerError> {
        if matches!(
            error,
            CryptoError::Storage(_) | CryptoError::Stopped | CryptoError::ArchivedGeneration
        ) {
            return Err(error.into());
        }
        let yield_lane = matches!(error, CryptoError::Busy);
        self.issues.push(Issue {
            operation,
            error: error.into(),
        });
        Ok(yield_lane)
    }
}

pub struct DeliveryRunner<T> {
    transport: T,
    config: RunnerConfig,
    outbox_after: Option<String>,
    ack_after: Option<String>,
    next_lane: u8,
    last_report: Option<RoundReport>,
}
impl<T: DeliveryTransport> DeliveryRunner<T> {
    pub fn new(transport: T, config: RunnerConfig) -> Result<Self, RunnerError> {
        if !config.valid() {
            return Err(RunnerError::Configuration);
        }
        Ok(Self {
            transport,
            config,
            outbox_after: None,
            ack_after: None,
            next_lane: 0,
            last_report: None,
        })
    }
    pub fn last_report(&self) -> Option<&RoundReport> {
        self.last_report.as_ref()
    }
    /// At most one page (32 pending rows) per local journal, one pending batch
    /// (32 / 64 KiB), and one serial RPC at a time. A failed item never deletes
    /// durable work. Cursors wrap without traversing completed local history.
    pub async fn poll_once(
        &mut self,
        worker: &CryptoCoordinator,
    ) -> Result<RoundReport, RunnerError> {
        let mut report = RoundReport::default();
        for _ in 0..3 {
            let lane = self.next_lane;
            // Advance before awaiting: a round timeout cannot starve later lanes.
            self.next_lane = (lane + 1) % 3;
            match lane {
                0 => self.recover_acks(worker, &mut report).await?,
                1 => self.send_outbox(worker, &mut report).await?,
                _ => self.receive(worker, &mut report).await?,
            }
        }
        Ok(report)
    }
    async fn ack(
        &mut self,
        worker: &CryptoCoordinator,
        proof: &DurableAck,
        operation: Operation,
        report: &mut RoundReport,
    ) -> Result<bool, RunnerError> {
        match self.transport.acknowledge(proof).await {
            Ok(verified) => {
                // An unknown local result is recoverable by exact ACK replay.
                if let Err(error) = worker.mark_acknowledged(verified).await {
                    report.local_busy(operation, error)?;
                    return Ok(true);
                }
                report.acknowledged += 1;
                Ok(false)
            }
            Err(error) => report.rpc(operation, error),
        }
    }
    async fn recover_acks(
        &mut self,
        worker: &CryptoCoordinator,
        report: &mut RoundReport,
    ) -> Result<(), RunnerError> {
        let page = match worker
            .pending_acknowledgements(self.ack_after.clone())
            .await
        {
            Ok(page) => page,
            Err(CryptoError::Busy) => {
                report.crypto(Operation::RecoverAck, CryptoError::Busy)?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        for proof in &page.acknowledgements {
            // Cancellation may happen after the remote commit. Revisit on wrap
            // or restart rather than blocking all later journal entries.
            self.ack_after = Some(proof.request().messages[0].message_id.clone());
            if self
                .ack(worker, proof, Operation::RecoverAck, report)
                .await?
            {
                return Ok(());
            }
        }
        self.ack_after = page.next;
        Ok(())
    }
    async fn send_outbox(
        &mut self,
        worker: &CryptoCoordinator,
        report: &mut RoundReport,
    ) -> Result<(), RunnerError> {
        let page = match worker.outbox_page(self.outbox_after.clone()).await {
            Ok(page) => page,
            Err(CryptoError::Busy) => {
                report.crypto(Operation::Send, CryptoError::Busy)?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let mut refresh = Vec::new();
        for plan in &page.messages {
            self.outbox_after = Some(
                plan.request()
                    .envelope
                    .as_ref()
                    .ok_or(CryptoError::Invalid)?
                    .message_id
                    .clone(),
            );
            if plan.progress().status().is_some_and(|s| {
                matches!(
                    MessageState::try_from(s.state),
                    Ok(MessageState::RecipientStored
                        | MessageState::RecipientRejected
                        | MessageState::RecipientReplaced)
                )
            }) {
                continue;
            }
            if plan.progress().receipt().is_some() {
                // Expired remains eligible for a later durable recipient ACK.
                refresh.push(plan.clone());
                continue;
            }
            match self.transport.send(plan).await {
                Ok(verified) => {
                    if let Err(error) = worker.accept_send_receipt(verified).await {
                        return report.local_busy(Operation::Send, error);
                    }
                    report.sent += 1;
                }
                Err(error) => {
                    if report.rpc(Operation::Send, error)? {
                        return Ok(());
                    }
                }
            }
        }
        self.outbox_after = page.next;
        if !refresh.is_empty() {
            match self.transport.statuses(&refresh).await {
                Ok(verified) => {
                    match worker.accept_message_statuses(verified).await {
                        Ok(progress) => report.statuses += progress.len(),
                        Err(error) => {
                            // Busy can occur after the remote status observation;
                            // fetch it again, never infer local progress.
                            if matches!(error, CryptoError::Busy) {
                                report.crypto(Operation::Status, error)?;
                            } else {
                                return Err(error.into());
                            }
                        }
                    }
                }
                Err(error) => {
                    report.rpc(Operation::Status, error)?;
                }
            }
        }
        Ok(())
    }
    async fn receive(
        &mut self,
        worker: &CryptoCoordinator,
        report: &mut RoundReport,
    ) -> Result<(), RunnerError> {
        let page = match self.transport.pending().await {
            Ok(page) => page,
            Err(error) => {
                report.rpc(Operation::Pending, error)?;
                return Ok(());
            }
        };
        // Native validates before returning, but enforce allocation bounds even
        // for another trusted adapter. Individual bindings are checked by worker.
        if page.messages.len() > super::contract::MAX_INBOX_MESSAGES
            || prost::Message::encoded_len(&page) > super::contract::MAX_RESPONSE_BYTES
        {
            return Err(RpcError::Invalid.into());
        }
        let mut scopes: HashMap<String, SessionScope> = HashMap::new();
        for delivery in page.messages {
            let id = delivery
                .envelope
                .as_ref()
                .ok_or(RpcError::Invalid)?
                .conversation_id
                .clone();
            if !scopes.contains_key(&id) {
                let plan = match worker.prepare_conversation_lookup(id.clone()).await {
                    Ok(plan) => plan,
                    Err(error) => return report.local_busy(Operation::Lookup, error),
                };
                match self.transport.lookup(&plan).await {
                    Ok(verified) => {
                        if let Err(error) = worker.accept_conversation_lookup(verified).await {
                            return report.local_busy(Operation::Lookup, error);
                        }
                        let scope = match worker.conversation_scope(id.clone()).await {
                            Ok(scope) => scope,
                            Err(error) => return report.local_busy(Operation::Lookup, error),
                        };
                        scopes.insert(id.clone(), scope);
                    }
                    Err(error) => {
                        if report.rpc(Operation::Lookup, error)? {
                            return Ok(());
                        }
                        continue;
                    }
                }
            }
            let scope = scopes.get(&id).ok_or(CryptoError::Invalid)?.clone();
            let proof = match worker
                .receive_for_delivery(scope.clone(), delivery.clone())
                .await
            {
                Ok((_payload, proof)) => {
                    report.received += 1;
                    proof
                }
                // Only a previously EXPLICIT quarantine can yield Rejected.
                Err(CryptoError::Quarantined) => match worker.prepare_ack(scope, delivery).await {
                    Ok(proof) => proof,
                    Err(error) => return report.local_busy(Operation::Receive, error),
                },
                Err(error) => {
                    if report.crypto(Operation::Receive, error)? {
                        return Ok(());
                    }
                    continue;
                }
            };
            if self
                .ack(worker, &proof, Operation::Acknowledge, report)
                .await?
            {
                return Ok(());
            }
        }
        Ok(())
    }
    /// Cancellation (true or dropped sender) drops in-flight RPC futures; it does
    /// not roll back already committed remote/local work. Journals recover that
    /// ambiguity. No child task survives this future. Channel reconnect belongs
    /// to tonic; this loop retries exact durable operations after backoff.
    pub async fn run(
        &mut self,
        worker: &CryptoCoordinator,
        stop: watch::Receiver<bool>,
    ) -> Result<(), RunnerError> {
        self.run_core(worker, stop, None).await
    }
    /// Both futures are scoped here: runner failure/logout/drop also drops the
    /// optional stream. No spawned account task or unbounded hint queue survives.
    pub async fn run_with_wakeups(
        &mut self,
        worker: &CryptoCoordinator,
        stop: watch::Receiver<bool>,
        mut wakeups: InboxWakeups,
    ) -> Result<(), RunnerError> {
        let hints = Arc::new(Notify::new());
        tokio::select! {
            result = self.run_core(worker, stop.clone(), Some(hints.clone())) => result,
            _ = wakeups.run(hints, stop) => Ok(()),
        }
    }
    async fn run_core(
        &mut self,
        worker: &CryptoCoordinator,
        mut stop: watch::Receiver<bool>,
        hints: Option<Arc<Notify>>,
    ) -> Result<(), RunnerError> {
        let initial = Duration::from_millis(rand::random_range(
            0..=self.config.poll_interval.as_millis() as u64,
        ));
        if wait(initial, &mut stop, hints.as_deref()).await {
            return Ok(());
        }
        let mut failures = 0u32;
        loop {
            let timeout = self.config.round_timeout;
            let result = tokio::select! {
                biased;
                _ = cancelled(&mut stop) => return Ok(()),
                result = tokio::time::timeout(timeout, self.poll_once(worker)) => result,
            };
            let report = match result {
                Ok(result) => result?,
                Err(_) => RoundReport {
                    issues: vec![Issue {
                        operation: Operation::Round,
                        error: DeliveryIssue::Timeout,
                    }],
                    ..Default::default()
                },
            };
            failures = if report.issues.is_empty() {
                0
            } else {
                failures.saturating_add(1)
            };
            self.last_report = Some(report);
            let millis = self.config.delay(failures).as_millis() as u64;
            let delay = Duration::from_millis(rand::random_range(millis.div_ceil(2)..=millis));
            // Hints cannot bypass failure backoff. On success, a250ms floor
            // limits duplicate/flooded hints; the periodic timer always remains.
            if wait(
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
pub(super) async fn cancelled(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() || stop.changed().await.is_err() {
            return;
        }
    }
}
pub(super) async fn wait(
    delay: Duration,
    stop: &mut watch::Receiver<bool>,
    hints: Option<&Notify>,
) -> bool {
    let earliest = tokio::time::Instant::now() + delay.min(Duration::from_millis(250));
    tokio::select! {
        biased;
        _ = cancelled(stop) => true,
        _ = tokio::time::sleep(delay) => false,
        _ = async {
            match hints { Some(hints) => hints.notified().await, None => std::future::pending().await }
            tokio::time::sleep_until(earliest).await;
        } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn duplicate_hints_coalesce_and_cannot_spin_the_poll_lane() {
        let hint = Notify::new();
        for _ in 0..10_000 {
            hint.notify_one();
        }
        let (_sender, mut stop) = watch::channel(false);
        let start = tokio::time::Instant::now();
        assert!(!wait(Duration::from_secs(10), &mut stop, Some(&hint)).await);
        assert!(start.elapsed() >= Duration::from_millis(200));
        assert!(
            tokio::time::timeout(
                Duration::from_millis(80),
                wait(Duration::from_secs(10), &mut stop, Some(&hint))
            )
            .await
            .is_err()
        );
    }
    #[tokio::test]
    async fn hints_do_not_override_failure_backoff_and_stop_remains_prompt() {
        let hints = Notify::new();
        hints.notify_one();
        let (sender, mut stop) = watch::channel(false);
        // Failure path passes no hint source even when a permit is pending.
        assert!(
            tokio::time::timeout(
                Duration::from_millis(80),
                wait(Duration::from_secs(30), &mut stop, None)
            )
            .await
            .is_err()
        );
        sender.send(true).unwrap();
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                wait(Duration::from_secs(30), &mut stop, Some(&hints))
            )
            .await
            .unwrap()
        );
    }
    #[test]
    fn retry_backoff_is_capped_and_success_resets_to_poll_interval() {
        let config = RunnerConfig::default();
        assert_eq!(config.delay(0), Duration::from_secs(3));
        for (attempt, seconds) in [(1, 1), (2, 2), (3, 4), (4, 8), (5, 16), (6, 30)] {
            assert_eq!(config.delay(attempt), Duration::from_secs(seconds));
        }
        assert_eq!(config.delay(u32::MAX), Duration::from_secs(30));
        assert_eq!(config.delay(0), Duration::from_secs(3));
    }
    #[test]
    fn configuration_does_not_allow_busy_spin_or_unbounded_delays() {
        assert!(RunnerConfig::default().valid());
        for duration in [Duration::ZERO, Duration::from_nanos(1), Duration::MAX] {
            assert!(
                !RunnerConfig {
                    poll_interval: duration,
                    ..Default::default()
                }
                .valid()
            );
        }
        assert!(
            !RunnerConfig {
                retry_initial: Duration::from_secs(31),
                ..Default::default()
            }
            .valid()
        );
    }
}
