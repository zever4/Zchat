//! Fault-injected transport + real protected storage/libsignal. Socket/TLS/PG
//! acceptance is separately exercised by the native Go fixture.
use super::incoming::tests::{payload, prepared};
use super::tests::{Fixture, fixture, index, initialize, records};
use super::*;
use crate::encryption::{
    delivery_runner::{RunnerConfig, RunnerError},
    epoch_contract as data,
    epoch_runner::*,
    native::{
        EpochAcquisitionOutcome, EpochDelivery, EpochFileStatuses, EpochObservation,
        EpochPreparedSendOutcome, EpochRecoveryError, EpochStatuses, RpcError,
    },
    recovery_contract as control,
    signal_state::tests::store,
};
use crate::proto::e2ee as common;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, watch};
use tonic::Code;
const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
#[derive(Default)]
struct Network {
    conversations: HashMap<(String, String), Conversation>,
    catalogs: HashMap<(String, String), v2::ConversationEpochs>,
    records: HashMap<String, v2::RecoveryRecord>,
    receipts: HashMap<String, v2::SendReceipt>,
    inbox: Vec<v2::Delivery>,
    acks: HashMap<String, i32>,
    sends: Vec<v2::SendMessageRequest>,
    submits: Vec<v2::RecoveryCommandRequest>,
    calls: Vec<&'static str>,
    bundle: Option<AcquiredBundle>,
    acquires: Vec<v2::AcquireBundleRequest>,
    expire_once: bool,
    lose_send: usize,
    lose_ack: usize,
    lose_accept: usize,
    send_error: Option<Code>,
    blob_unavailable: bool,
    send_gate: Option<Arc<Notify>>,
    pending_error: Option<Code>,
    hold_old: bool,
    oversized_refs: bool,
    gate: Option<Arc<Notify>>,
    started: Option<Arc<Notify>>,
    active: Arc<AtomicUsize>,
}
#[derive(Clone)]
struct Fake {
    own: DeviceRef,
    scope: StoreScope,
    network: Arc<Mutex<Network>>,
}
impl Fake {
    fn new(s: &EpochScope, n: &Arc<Mutex<Network>>) -> Self {
        Self {
            own: s.own.clone(),
            scope: s.scope.clone(),
            network: n.clone(),
        }
    }
    fn obs<Q, R>(&self, q: Q, r: R) -> EpochObservation<Q, R> {
        EpochObservation::test_observation(self.scope.clone(), q, r)
    }
}
async fn network(f: &Fixture, a: &EpochScope, b: &EpochScope) -> Arc<Mutex<Network>> {
    let mut n = Network {
        bundle: Some(f.next_bundle.clone()),
        ..Default::default()
    };
    for (w, s) in [(&f.alice, a), (&f.bob, b)] {
        let q = w.prepare_initial_epoch(CHAT.into()).await.unwrap();
        let c = q.pins().conversation().clone();
        let key = (s.own.user_id.clone(), CHAT.into());
        n.catalogs.insert(
            key.clone(),
            v2::ConversationEpochs {
                protocol_version: 2,
                conversation: Some(c.clone()),
                active_epoch: Some(0),
                epoch_revision: Some(0),
                epochs: vec![v2::EpochRecord {
                    epoch: Some(0),
                    revision: Some(0),
                    created_at_unix_ms: 1_700_000_000_000,
                    ..Default::default()
                }],
                next_after_epoch: None,
            },
        );
        n.conversations.insert(key, c);
    }
    Arc::new(Mutex::new(n))
}
fn receipt(e: &v2::Envelope) -> v2::SendReceipt {
    v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    }
}
#[async_trait::async_trait]
impl EpochDeliveryTransport for Fake {
    async fn file_statuses(
        &mut self,
        _: &[EpochOutgoingPlan],
    ) -> Result<EpochFileStatuses, RpcError> {
        Err(RpcError::Status(Code::Unavailable))
    }
    async fn create(
        &mut self,
        _: &ConversationCreation,
    ) -> Result<VerifiedConversationCreation, RpcError> {
        Err(RpcError::Transport)
    }
    async fn send(&mut self, p: &EpochOutgoingPlan) -> Result<EpochPreparedSendOutcome, RpcError> {
        let gate = {
            let mut n = self.network.lock().unwrap();
            n.send_gate.take().map(|g| (g, n.started.clone()))
        };
        if let Some((gate, started)) = gate {
            if let Some(started) = started {
                started.notify_one();
            }
            gate.notified().await;
        }
        let mut n = self.network.lock().unwrap();
        n.calls.push("send");
        n.sends.push(p.request().clone());
        if let Some(code) = n.send_error.take() {
            return Err(RpcError::Status(code));
        }
        let e = p.request().envelope.as_ref().unwrap();
        if let Some(r) = n.receipts.get(&e.message_id) {
            return Ok(EpochPreparedSendOutcome::Accepted(Box::new(
                self.obs(p.clone(), r.clone()),
            )));
        }
        if n.blob_unavailable && e.protocol_version == 3 {
            return Ok(EpochPreparedSendOutcome::BlobUnavailable(Box::new(
                self.obs(
                    p.clone(),
                    v2::BlobSendFailure {
                        reason: 1,
                        binding: Some(data::send_binding(e).unwrap()),
                    },
                ),
            )));
        }
        let page = n
            .catalogs
            .get(&(self.own.user_id.clone(), e.conversation_id.clone()))
            .unwrap();
        if page.active_epoch > e.crypto_epoch {
            let row = page
                .epochs
                .iter()
                .find(|r| r.epoch == e.crypto_epoch)
                .unwrap();
            let record = &n.records[&row.sealed_by_proposal_id];
            let o = record.outcome.as_ref().unwrap();
            let failure = v2::EpochSealedFailure {
                fence: Some(common::EpochSealedFence {
                    binding: Some(data::send_binding(e).unwrap()),
                    proposal_digest: control::control_digest(
                        o.proposal.as_ref().unwrap().control.as_ref().unwrap(),
                    )
                    .unwrap()
                    .to_vec(),
                    next_epoch: o.new_epoch,
                    epoch_revision: o.epoch_revision,
                }),
                recovery: Some(o.clone()),
            };
            return Ok(EpochPreparedSendOutcome::Sealed(Box::new(
                self.obs(p.clone(), failure),
            )));
        }
        let r = receipt(e);
        n.receipts.insert(e.message_id.clone(), r.clone());
        let sequence = n.receipts.len() as i64;
        n.inbox.push(v2::Delivery {
            sequence,
            envelope: Some(e.clone()),
            envelope_digest: r.envelope_digest.clone(),
            expires_at_unix_ms: r.expires_at_unix_ms,
        });
        if n.lose_send > 0 {
            n.lose_send -= 1;
            return Err(RpcError::Status(Code::Unavailable));
        }
        Ok(EpochPreparedSendOutcome::Accepted(Box::new(
            self.obs(p.clone(), r),
        )))
    }
    async fn statuses(&mut self, p: &[EpochOutgoingPlan]) -> Result<EpochStatuses, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("statuses");
        let messages = p
            .iter()
            .map(|p| {
                let e = p.request().envelope.as_ref().unwrap();
                let r = n.receipts[&e.message_id].clone();
                let state = match n.acks.get(&e.message_id) {
                    Some(1) => common::MessageState::RecipientStored,
                    Some(_) => common::MessageState::RecipientRejected,
                    None => common::MessageState::Pending,
                };
                v2::MessageStatus {
                    receipt: Some(r.clone()),
                    state: state as i32,
                    observed_at_unix_ms: r.stored_at_unix_ms
                        + if state == common::MessageState::Pending {
                            0
                        } else {
                            1
                        },
                }
            })
            .collect();
        Ok(self.obs(p.to_vec(), v2::MessageStatuses { messages }))
    }
    async fn pending(&mut self) -> Result<Vec<EpochDelivery>, RpcError> {
        let (gate, started, active, error) = {
            let mut n = self.network.lock().unwrap();
            n.calls.push("pending");
            (
                n.gate.clone(),
                n.started.clone(),
                n.active.clone(),
                n.pending_error,
            )
        };
        struct Guard(Arc<AtomicUsize>);
        impl Drop for Guard {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::SeqCst);
            }
        }
        active.fetch_add(1, Ordering::SeqCst);
        let _guard = Guard(active);
        if let Some(started) = started {
            started.notify_one();
        }
        if let Some(gate) = gate {
            gate.notified().await;
        }
        if let Some(code) = error {
            return Err(RpcError::Status(code));
        }
        let n = self.network.lock().unwrap();
        let request = v2::GetPendingMessagesRequest {
            context: Some(ClientContext {
                protocol_version: 2,
                device_generation_id: self.own.generation_id.clone(),
            }),
        };
        Ok(n.inbox
            .iter()
            .filter(|d| {
                d.envelope.as_ref().unwrap().recipient.as_ref() == Some(&self.own)
                    && (!n.hold_old || d.envelope.as_ref().unwrap().crypto_epoch != Some(0))
            })
            .take(32)
            .map(|d| self.obs(request.clone(), d.clone()))
            .collect())
    }
    async fn acknowledge(
        &mut self,
        p: &EpochDurableAck,
    ) -> Result<EpochObservation<EpochDurableAck, common::Empty>, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("ack");
        let a = &p.request().messages[0];
        n.acks.insert(a.message_id.clone(), a.disposition);
        n.inbox
            .retain(|d| d.envelope.as_ref().unwrap().message_id != a.message_id);
        if n.lose_ack > 0 {
            n.lose_ack -= 1;
            return Err(RpcError::Status(Code::Unavailable));
        }
        Ok(self.obs(p.clone(), common::Empty {}))
    }
    async fn lookup(
        &mut self,
        p: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("lookup");
        let c = n
            .conversations
            .get(&(
                self.own.user_id.clone(),
                p.request().conversation_id.clone(),
            ))
            .ok_or(RpcError::Status(Code::NotFound))?;
        Ok(VerifiedConversationLookup::from_rpc(p.clone(), c.clone()))
    }
    async fn initial(
        &mut self,
        p: &EpochCatalogQuery,
    ) -> Result<EpochObservation<EpochCatalogQuery, v2::ConversationEpochs>, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("initial");
        let mut c = n.catalogs[&(
            self.own.user_id.clone(),
            p.request().conversation_id.clone(),
        )]
            .clone();
        c.epochs.truncate(1);
        c.next_after_epoch = if c.active_epoch > Some(0) {
            Some(0)
        } else {
            None
        };
        Ok(self.obs(p.clone(), c))
    }
    async fn references(&mut self, after: &str) -> Result<RecoveryReferences, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("refs");
        let mut proposals = n
            .records
            .values()
            .filter(|r| r.outcome.as_ref().unwrap().state == common::RecoveryState::Pending as i32)
            .map(|r| {
                let c = r
                    .outcome
                    .as_ref()
                    .unwrap()
                    .proposal
                    .as_ref()
                    .unwrap()
                    .control
                    .as_ref()
                    .unwrap();
                v2::PendingRecoveryReference {
                    conversation_id: c.conversation_id.clone(),
                    proposal_id: c.proposal_id.clone(),
                    expected_epoch: c.expected_epoch,
                    expected_revision: c.expected_revision,
                    initiated_by_own_device: c.initiator.as_ref().and_then(|p| p.device.as_ref())
                        == Some(&self.own),
                    created_at_unix_ms: r.created_at_unix_ms,
                    expires_at_unix_ms: r.expires_at_unix_ms,
                }
            })
            .filter(|r| r.conversation_id.as_str() > after)
            .collect::<Vec<_>>();
        proposals.sort_by(|a, b| a.conversation_id.cmp(&b.conversation_id));
        proposals.truncate(32);
        if n.oversized_refs {
            proposals = vec![v2::PendingRecoveryReference::default(); 33];
        }
        Ok(self.obs(
            v2::GetPendingEpochRecoveriesRequest {
                context: Some(ClientContext {
                    protocol_version: 2,
                    device_generation_id: self.own.generation_id.clone(),
                }),
                after_conversation_id: after.into(),
                limit: 32,
            },
            v2::PendingEpochRecoveries {
                proposals,
                next_conversation_id: String::new(),
            },
        ))
    }
    async fn query(&mut self, p: &EpochRecoveryQuery) -> Result<RecoveryReply, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("query");
        let r = n
            .records
            .get(&p.request().proposal_id)
            .ok_or(RpcError::Status(Code::NotFound))?;
        Ok(self.obs(p.clone(), r.clone()))
    }
    async fn submit(&mut self, p: &EpochRecoveryIntent) -> Result<IntentReply, EpochRecoveryError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("submit");
        n.submits.push(p.request().clone());
        let c = p
            .request()
            .command
            .as_ref()
            .unwrap()
            .control
            .as_ref()
            .unwrap();
        if c.action == common::RecoveryAction::Propose as i32
            && n.records.values().any(|r| {
                let o = r.outcome.as_ref().unwrap();
                let existing = o.proposal.as_ref().unwrap().control.as_ref().unwrap();
                o.state == common::RecoveryState::Pending as i32
                    && existing.conversation_id == c.conversation_id
                    && existing.proposal_id != c.proposal_id
            })
        {
            return Err(RpcError::Status(Code::Aborted).into());
        }
        let r = n
            .records
            .entry(c.proposal_id.clone())
            .or_insert_with(|| v2::RecoveryRecord {
                outcome: Some(common::RecoveryOutcome {
                    protocol_version: 2,
                    proposal: Some(p.proposal().clone()),
                    state: common::RecoveryState::Pending as i32,
                    ..Default::default()
                }),
                created_at_unix_ms: 1_700_000_000_000,
                expires_at_unix_ms: 1_700_086_400_000,
                resolved_at_unix_ms: None,
            });
        if c.action == common::RecoveryAction::Accept as i32
            && r.outcome.as_ref().unwrap().state == common::RecoveryState::Pending as i32
        {
            let o = r.outcome.as_mut().unwrap();
            o.decision = p.request().command.clone();
            o.state = common::RecoveryState::Accepted as i32;
            o.new_epoch = c.expected_epoch + 1;
            o.epoch_revision = c.expected_revision + 1;
            r.resolved_at_unix_ms = Some(1_700_000_000_001);
            let r = r.clone();
            for page in n.catalogs.values_mut().filter(|page| {
                page.conversation.as_ref().unwrap().conversation_id == c.conversation_id
            }) {
                let old = page.epochs.last_mut().unwrap();
                old.sealed = true;
                old.sealed_by_proposal_id = c.proposal_id.clone();
                old.sealed_at_unix_ms = r.resolved_at_unix_ms;
                page.epochs.push(v2::EpochRecord {
                    epoch: Some(c.expected_epoch + 1),
                    revision: Some(c.expected_revision + 1),
                    created_at_unix_ms: r.resolved_at_unix_ms.unwrap(),
                    created_by_proposal_id: c.proposal_id.clone(),
                    bootstrap_device: c.initiator.as_ref().and_then(|p| p.device.clone()),
                    ..Default::default()
                });
                page.active_epoch = Some(c.expected_epoch + 1);
                page.epoch_revision = Some(c.expected_revision + 1);
            }
            if n.lose_accept > 0 {
                n.lose_accept -= 1;
                return Err(RpcError::Status(Code::Unavailable).into());
            }
        }
        Ok(self.obs(p.clone(), n.records[&c.proposal_id].clone()))
    }
    async fn catalog(&mut self, p: &EpochTransitionQuery) -> Result<TransitionReply, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("catalog");
        Ok(self.obs(
            p.clone(),
            n.catalogs[&(
                self.own.user_id.clone(),
                p.request().conversation_id.clone(),
            )]
                .clone(),
        ))
    }
    async fn acquire(
        &mut self,
        p: &EpochAcquisitionPlan,
    ) -> Result<EpochAcquisitionOutcome, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.calls.push("acquire");
        n.acquires.push(p.request().clone());
        if n.expire_once {
            n.expire_once = false;
            return Ok(EpochAcquisitionOutcome::Expired(Box::new(self.obs(
                p.clone(),
                v2::AcquisitionExpiredFailure {
                    request: Some(p.request().clone()),
                },
            ))));
        }
        let b = n.bundle.as_ref().unwrap();
        Ok(EpochAcquisitionOutcome::Bundle(Box::new(self.obs(
            p.clone(),
            v2::AcquiredBundle {
                protocol_version: 2,
                conversation_id: p.request().conversation_id.clone(),
                acquisition_id: p.request().acquisition_id.clone(),
                device: b.device.clone(),
                signed_pre_key: b.signed_pre_key.clone(),
                one_time_pre_key: b.one_time_pre_key.clone(),
                kyber_pre_key: b.kyber_pre_key.clone(),
                not_after_unix_ms: b.not_after_unix_ms,
                crypto_epoch: p.request().crypto_epoch,
            },
        ))))
    }
}
fn runner(f: Fake) -> EpochDeliveryRunner<Fake> {
    EpochDeliveryRunner::new(f, RunnerConfig::default()).unwrap()
}

#[tokio::test]
async fn lost_send_and_ack_reopen_repeat_exact_ciphertext_and_continue_signal() {
    exact_send_retry_after_reopen(None).await;
}

#[tokio::test]
async fn server_overload_preserves_outbox_and_retries_exact_ciphertext_after_reopen() {
    exact_send_retry_after_reopen(Some(Code::ResourceExhausted)).await;
}

async fn exact_send_retry_after_reopen(overload: Option<Code>) {
    let (f, a, b) = prepared().await;
    let n = network(&f, &a, &b).await;
    let p = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "driver sends committed outbox"))
        .await
        .unwrap();
    if let Some(code) = overload {
        n.lock().unwrap().send_error = Some(code);
    } else {
        n.lock().unwrap().lose_send = 1;
    }
    let deferred = runner(Fake::new(&a, &n)).poll_once(&f.alice).await.unwrap();
    assert!(!deferred.delivery.issues.is_empty());
    assert!(
        f.alice
            .epoch_outgoing_plan(p.request().envelope.as_ref().unwrap().message_id.clone())
            .await
            .unwrap()
            .needs_send()
    );
    f.alice.shutdown().await.unwrap();
    let wa = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    let mut ra = runner(Fake::new(&a, &n));
    let mut rb = runner(Fake::new(&b, &n));
    ra.poll_once(&wa).await.unwrap();
    n.lock().unwrap().lose_ack = 1;
    rb.poll_once(&f.bob).await.unwrap();
    assert_eq!(rb.incoming_message_revision(), 1);
    assert_eq!(
        f.bob
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    f.bob.shutdown().await.unwrap();
    let wb = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    for _ in 0..3 {
        rb.poll_once(&wb).await.unwrap();
        ra.poll_once(&wa).await.unwrap();
    }
    assert_eq!(
        rb.incoming_message_revision(),
        1,
        "ACK replay does not notify"
    );
    assert_eq!(
        ra.incoming_message_revision(),
        0,
        "session confirmation is silent"
    );
    let next = wa
        .encrypt_epoch(a.clone(), payload(&a, "Signal after driver confirmation"))
        .await
        .unwrap();
    assert_eq!(
        next.request().envelope.as_ref().unwrap().message_type,
        common::CiphertextType::Signal as i32
    );
    for _ in 0..2 {
        ra.poll_once(&wa).await.unwrap();
        rb.poll_once(&wb).await.unwrap();
    }
    assert_eq!(rb.incoming_message_revision(), 2);
    assert!(
        wb.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    {
        let n = n.lock().unwrap();
        assert_eq!(n.sends[0], n.sends[1]);
        assert_eq!(n.receipts.len(), 3);
        assert_eq!(n.acks.len(), 3);
        assert!(n.inbox.is_empty());
    }
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
}

#[tokio::test]
async fn manual_consent_lost_accept_expiry_and_mixed_epochs_resume_after_reopen() {
    let (f, a, b) = prepared().await;
    let n = network(&f, &a, &b).await;
    let late_text = payload(&a, "late old PreKey after new session");
    let late = f
        .alice
        .encrypt_epoch(a.clone(), late_text.clone())
        .await
        .unwrap();
    let mut ta = Fake::new(&a, &n);
    let EpochPreparedSendOutcome::Accepted(o) = ta.send(&late).await.unwrap() else {
        panic!()
    };
    f.alice.accept_epoch_send_receipt(*o).await.unwrap();
    let unsent = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "must remain unsent"))
        .await
        .unwrap();
    let proposal = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let id = proposal
        .proposal()
        .control
        .as_ref()
        .unwrap()
        .proposal_id
        .clone();
    {
        let mut net = n.lock().unwrap();
        net.send_error = Some(Code::Unavailable);
        net.hold_old = true;
    }
    let mut ra = runner(Fake::new(&a, &n));
    let mut rb = runner(Fake::new(&b, &n));
    ra.poll_once(&f.alice).await.unwrap();
    let report = rb.poll_once(&f.bob).await.unwrap();
    assert!(
        report
            .recovery
            .iter()
            .any(|n| n.snapshot.proposal_id() == id),
        "issues={:?}, diagnostic={:?} {}",
        report.delivery.issues,
        {
            let net = n.lock().unwrap();
            (net.submits.len(), net.calls.clone())
        },
        ""
    );
    assert!(
        !f.bob
            .epoch_scope(CHAT.into(), 0)
            .await
            .unwrap()
            .sending_frozen()
    );
    let before = records(f.root.path(), B);
    assert!(
        f.bob
            .resume_epoch_recovery(b.clone(), id.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert!(records(f.root.path(), B) == before);
    assert_eq!(n.lock().unwrap().submits.len(), 1); // No automatic Accept.
    let accept = f
        .bob
        .prepare_epoch_recovery_decision(b.clone(), id.clone(), EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    {
        let mut n = n.lock().unwrap();
        n.lose_accept = 1;
        n.expire_once = true;
    }
    rb.poll_once(&f.bob).await.unwrap();
    f.bob.shutdown().await.unwrap();
    let wb = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    // Recreate both loops: all consent, unknown outcomes and progress are durable.
    let mut rb = runner(Fake::new(&b, &n));
    ra.poll_once(&f.alice).await.unwrap();
    rb.poll_once(&wb).await.unwrap();
    ra.poll_once(&f.alice).await.unwrap();
    let a1 = f.alice.epoch_scope(CHAT.into(), 1).await.unwrap();
    let b1 = wb.epoch_scope(CHAT.into(), 1).await.unwrap();
    for _ in 0..4 {
        if f.alice
            .prepare_epoch_acquisition(a1.clone())
            .await
            .unwrap()
            .applied()
        {
            break;
        }
        let r = ra.poll_once(&f.alice).await.unwrap();
        assert!(
            r.delivery.issues.is_empty(),
            "bootstrap deferred: {:?}",
            r.delivery.issues
        );
    }
    assert!(
        f.alice
            .prepare_epoch_acquisition(a1.clone())
            .await
            .unwrap()
            .applied()
    );
    let failed = f
        .alice
        .epoch_outgoing_plan(
            unsent
                .request()
                .envelope
                .as_ref()
                .unwrap()
                .message_id
                .clone(),
        )
        .await
        .unwrap();
    assert!(
        failed.sealed_failure().is_some() && failed.receipt().is_none() && !failed.needs_send()
    );
    let text = payload(&a1, "real new epoch via loop");
    f.alice
        .encrypt_epoch(a1.clone(), text.clone())
        .await
        .unwrap();
    for _ in 0..3 {
        ra.poll_once(&f.alice).await.unwrap();
        rb.poll_once(&wb).await.unwrap();
    }
    assert_eq!(
        wb.epoch_incoming_message(text.message_id.clone())
            .await
            .unwrap(),
        text
    );
    assert!(
        wb.epoch_incoming_message(late_text.message_id.clone())
            .await
            .is_err()
    );
    n.lock().unwrap().hold_old = false;
    for _ in 0..2 {
        rb.poll_once(&wb).await.unwrap();
        ra.poll_once(&f.alice).await.unwrap();
    }
    assert_eq!(
        wb.epoch_incoming_message(late_text.message_id.clone())
            .await
            .unwrap(),
        late_text
    );
    assert!(
        wb.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert!(wb.epoch_scope(CHAT.into(), 0).await.unwrap().sealed() && !b1.sealed());
    {
        let n = n.lock().unwrap();
        assert_eq!(n.acquires.len(), 2);
        assert_ne!(n.acquires[0].acquisition_id, n.acquires[1].acquisition_id);
        assert!(
            n.submits
                .iter()
                .filter(
                    |p| p.command.as_ref().unwrap().control.as_ref().unwrap().action
                        == common::RecoveryAction::Accept as i32
                )
                .all(|p| p == accept.request())
        );
        assert!(n.inbox.is_empty());
    }
    f.alice.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_ciphertext_is_deferred_while_next_valid_message_is_stored() {
    let (f, a, b) = prepared().await;
    let n = network(&f, &a, &b).await;
    let p = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "valid after poison"))
        .await
        .unwrap();
    let mut bad = p.request().envelope.as_ref().unwrap().clone();
    bad.message_id = uuid::Uuid::new_v4().to_string();
    bad.ciphertext = vec![1];
    bad.message_type = common::CiphertextType::Signal as i32;
    let rr = receipt(&bad);
    n.lock().unwrap().inbox.push(v2::Delivery {
        sequence: 1,
        envelope: Some(bad.clone()),
        envelope_digest: rr.envelope_digest,
        expires_at_unix_ms: rr.expires_at_unix_ms,
    });
    Fake::new(&a, &n).send(&p).await.unwrap();
    let r = runner(Fake::new(&b, &n)).poll_once(&f.bob).await.unwrap();
    assert_eq!(r.delivery.received, 1);
    assert_eq!(r.delivery.acknowledged, 1);
    assert!(!r.delivery.issues.is_empty());
    assert!(f.bob.epoch_ack(bad.message_id.clone()).await.is_err());
    assert!(
        f.bob
            .epoch_quarantined_message(bad.message_id)
            .await
            .is_err()
    );
    assert_eq!(n.lock().unwrap().inbox.len(), 1);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_run_cancels_inflight_rpc_without_a_child_task_and_deadline_retries() {
    let (f, a, b) = prepared().await;
    let n = network(&f, &a, &b).await;
    let started = Arc::new(Notify::new());
    let gate = Arc::new(Notify::new());
    {
        let mut n = n.lock().unwrap();
        n.started = Some(started.clone());
        n.gate = Some(gate);
    }
    let config = RunnerConfig {
        poll_interval: Duration::from_millis(1),
        retry_initial: Duration::from_millis(1),
        retry_max: Duration::from_millis(2),
        round_timeout: Duration::from_secs(10),
    };
    let mut r = EpochDeliveryRunner::new(Fake::new(&a, &n), config).unwrap();
    let (tx, stop) = watch::channel(false);
    {
        let future = r.run(&f.alice, stop);
        tokio::pin!(future);
        tokio::select! {_=started.notified()=>{},result=&mut future=>panic!("unexpected completion {result:?}")}
        assert_eq!(n.lock().unwrap().active.load(Ordering::SeqCst), 1);
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), future)
            .await
            .unwrap()
            .unwrap();
    }
    assert_eq!(n.lock().unwrap().active.load(Ordering::SeqCst), 0);
    let baseline = n
        .lock()
        .unwrap()
        .calls
        .iter()
        .filter(|c| **c == "pending")
        .count();
    let mut r = EpochDeliveryRunner::new(
        Fake::new(&a, &n),
        RunnerConfig {
            round_timeout: Duration::from_millis(30),
            ..config
        },
    )
    .unwrap();
    let (tx, stop) = watch::channel(false);
    {
        let future = r.run(&f.alice, stop);
        tokio::pin!(future);
        let retried = async {
            loop {
                if n.lock()
                    .unwrap()
                    .calls
                    .iter()
                    .filter(|c| **c == "pending")
                    .count()
                    >= baseline + 2
                {
                    return;
                }
                started.notified().await;
            }
        };
        tokio::select! {result=tokio::time::timeout(Duration::from_secs(10),retried)=>result.unwrap(),result=&mut future=>panic!("unexpected completion {result:?}")}
        tx.send(true).unwrap();
        future.await.unwrap();
    }
    assert_eq!(n.lock().unwrap().active.load(Ordering::SeqCst), 0);
    assert!(
        r.last_report()
            .unwrap()
            .delivery
            .issues
            .iter()
            .any(|i| matches!(
                i.error,
                crate::encryption::delivery_runner::DeliveryIssue::Timeout
            ))
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_discovery_and_authentication_stop_the_loop_without_mutation() {
    let (f, a, b) = prepared().await;
    let n = network(&f, &a, &b).await;
    n.lock().unwrap().oversized_refs = true;
    f.alice.epoch_outbox_page(None).await.unwrap();
    let before = records(f.root.path(), A);
    assert!(matches!(
        runner(Fake::new(&a, &n)).poll_once(&f.alice).await,
        Err(RunnerError::Rpc(RpcError::Invalid))
    ));
    assert!(records(f.root.path(), A) == before);
    {
        let mut n = n.lock().unwrap();
        n.oversized_refs = false;
        n.pending_error = Some(Code::Unauthenticated);
    }
    assert!(matches!(
        runner(Fake::new(&a, &n)).poll_once(&f.alice).await,
        Err(RunnerError::Rpc(RpcError::Status(Code::Unauthenticated)))
    ));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn scheduling_pages_are_read_only_bound_and_missing_root_never_initializes() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let before = records(f.root.path(), A);
    let page = f.alice.epoch_conversation_page(None).await.unwrap();
    assert_eq!(page.scopes.len(), 1);
    assert!(
        f.bob
            .epoch_conversation_page(Some(a.conversation_cursor()))
            .await
            .is_err()
    );
    assert!(records(f.root.path(), A) == before);
    let proposal = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let before = records(f.root.path(), A);
    let id = proposal
        .proposal()
        .control
        .as_ref()
        .unwrap()
        .proposal_id
        .clone();
    assert_eq!(
        f.alice
            .resume_epoch_recovery(a.clone(), id)
            .await
            .unwrap()
            .unwrap()
            .request(),
        proposal.request()
    );
    assert!(records(f.root.path(), A) == before);
    f.alice.shutdown().await.unwrap();
    let path = a.scope.database_path(f.root.path());
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute(
        "DELETE FROM protected_records WHERE record_id=?1",
        [format!("epoch-v2:{CHAT}:head")],
    )
    .unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    assert!(w.active_epoch_scope(CHAT.into()).await.is_err());
    assert!(w.epoch_conversation_page(None).await.is_err());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let _ = b;
}

#[tokio::test]
async fn conversation_cursor_counts_source_rows_and_skips_no_initialized_chat() {
    let f = fixture().await;
    initialize(&f.alice).await;
    initialize(&f.bob).await;
    for i in 1..=34 {
        let mut c = f.a.clone();
        c.conversation_id = format!("{i:08x}-1111-4111-8111-111111111111");
        index(&f.alice, &c).await;
        assert!(
            f.alice
                .active_epoch_scope(c.conversation_id)
                .await
                .unwrap()
                .is_none()
        );
    }
    let before = records(f.root.path(), A);
    let page = f.alice.epoch_conversation_page(None).await.unwrap();
    assert!(page.scopes.is_empty() && page.next.is_some());
    assert!(
        f.bob
            .epoch_conversation_page(page.next.clone())
            .await
            .is_err()
    );
    let page = f.alice.epoch_conversation_page(page.next).await.unwrap();
    assert_eq!(page.scopes.len(), 1);
    assert!(page.next.is_none());
    assert_eq!(page.scopes[0].conversation_id(), CHAT);
    assert!(records(f.root.path(), A) == before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn resume_never_reseeds_missing_index_metadata_or_changes_saved_intent() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let id = p.proposal().control.as_ref().unwrap().proposal_id.clone();
    f.alice.shutdown().await.unwrap();
    let db = rusqlite::Connection::open(a.scope.database_path(f.root.path())).unwrap();
    assert_eq!(db.execute("DELETE FROM protected_records WHERE record_id IN ('recovery-index:state:v1','recovery-index:anchor:v1')",[]).unwrap(),2);
    let before = records(f.root.path(), A);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        w.resume_epoch_recovery(a, id).await,
        Err(CryptoError::RecoveryIndexesNotReady)
    ));
    assert!(records(f.root.path(), A) == before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn competing_local_proposal_cannot_hide_peer_offer_or_starve_explicit_acceptance() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let n = network(&f, &a, &b).await;
    let own = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let peer = f.bob.prepare_epoch_recovery(b.clone()).await.unwrap();
    Fake::new(&b, &n).submit(&peer).await.unwrap();
    let peer_id = peer
        .proposal()
        .control
        .as_ref()
        .unwrap()
        .proposal_id
        .clone();
    let mut r = runner(Fake::new(&a, &n));
    let seen = r.poll_once(&f.alice).await.unwrap();
    assert!(
        seen.recovery
            .iter()
            .any(|p| p.snapshot.proposal_id() == peer_id && p.snapshot.decision().is_none())
    );
    assert_eq!(
        f.alice
            .current_epoch_recoveries(a.clone())
            .await
            .unwrap()
            .len(),
        2
    );
    let accept = f
        .alice
        .prepare_epoch_recovery_decision(a.clone(), peer_id.clone(), EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    let progress = r.poll_once(&f.alice).await.unwrap();
    assert_eq!(progress.activated, 1);
    assert_eq!(progress.bootstrapped, 0);
    let new = f
        .alice
        .active_epoch_scope(CHAT.into())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(new.crypto_epoch(), 1);
    assert!(!new.bootstrap_initiator());
    let old = f
        .alice
        .epoch_recovery_snapshot(
            a,
            own.proposal().control.as_ref().unwrap().proposal_id.clone(),
        )
        .await
        .unwrap();
    assert_eq!(old.retired_by(), Some(peer_id.as_str()));
    assert!(
        n.lock()
            .unwrap()
            .submits
            .iter()
            .any(|p| p == accept.request())
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn rpc_budget_and_rotating_lanes_let_pending_run_after_full_ack_and_outbox_pages() {
    let (f, a, b) = prepared().await;
    let n = network(&f, &a, &b).await;
    let mut ta = Fake::new(&a, &n);
    let mut tb = Fake::new(&b, &n);
    let seed = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "seed real reverse session"))
        .await
        .unwrap();
    let EpochPreparedSendOutcome::Accepted(o) = ta.send(&seed).await.unwrap() else {
        panic!()
    };
    f.alice.accept_epoch_send_receipt(*o).await.unwrap();
    f.bob
        .receive_epoch(b.clone(), tb.pending().await.unwrap().remove(0))
        .await
        .unwrap();
    let confirm = f
        .bob
        .epoch_outbox_page(None)
        .await
        .unwrap()
        .messages
        .remove(0);
    let EpochPreparedSendOutcome::Accepted(o) = tb.send(&confirm).await.unwrap() else {
        panic!()
    };
    f.bob.accept_epoch_send_receipt(*o).await.unwrap();
    for i in 0..31 {
        let p = f
            .bob
            .encrypt_epoch(b.clone(), payload(&b, &format!("reverse {i}")))
            .await
            .unwrap();
        let EpochPreparedSendOutcome::Accepted(o) = tb.send(&p).await.unwrap() else {
            panic!()
        };
        f.bob.accept_epoch_send_receipt(*o).await.unwrap();
    }
    for d in ta.pending().await.unwrap() {
        f.alice.receive_epoch(a.clone(), d).await.unwrap();
    }
    assert_eq!(
        f.alice
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        32
    );
    for i in 0..32 {
        f.alice
            .encrypt_epoch(a.clone(), payload(&a, &format!("new outgoing {i}")))
            .await
            .unwrap();
    }
    n.lock().unwrap().calls.clear();
    let mut r = runner(Fake::new(&a, &n));
    let one = r.poll_once(&f.alice).await.unwrap();
    assert_eq!(one.rpc_attempts, MAX_ROUND_RPCS);
    assert_eq!(one.delivery.acknowledged, 32);
    assert!(!n.lock().unwrap().calls.contains(&"pending"));
    let two = r.poll_once(&f.alice).await.unwrap();
    assert!(two.rpc_attempts <= MAX_ROUND_RPCS);
    assert!(n.lock().unwrap().calls.contains(&"pending"));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn bound_blob_failure_yields_retry_notice_without_settlement_and_preserves_exact_outbox() {
    use crate::encryption::attachment_payload::AttachmentDraft;
    let (f, a, b) = prepared().await;
    let n = network(&f, &a, &b).await;
    n.lock().unwrap().blob_unavailable = true;
    let id = uuid::Uuid::new_v4().to_string();
    f.alice
        .prepare_epoch_attachment(
            CHAT.into(),
            id.clone(),
            AttachmentDraft {
                kind: v2::AttachmentKind::File,
                filename: Zeroizing::new("file.bin".into()),
                mime_type: Zeroizing::new("application/octet-stream".into()),
                caption: Zeroizing::new(String::new()),
                photo_metadata_stripped: None,
            },
            Zeroizing::new(vec![42; 64]),
        )
        .await
        .unwrap();
    let p = f.alice.epoch_attachment_control(id.clone()).await.unwrap();
    f.alice
        .accept_epoch_blob_lease(EpochObservation::test_observation(
            a.scope.clone(),
            p.clone(),
            v2::BlobLease {
                message_id: id.clone(),
                operation_id: p.request().operation_id.clone(),
                attempt_id: uuid::Uuid::new_v4().to_string(),
                revision: 1,
                reserved_at_unix_ms: 1000,
                expires_at_unix_ms: 3_601_000,
                observed_at_unix_ms: 1000,
                state: v2::BlobLeaseState::Ready as i32,
                expired: false,
            },
        ))
        .await
        .unwrap();
    f.alice.promote_epoch_attachment(id.clone()).await.unwrap();
    let mut runner = runner(Fake::new(&a, &n));
    let gate = Arc::new(Notify::new());
    let started = Arc::new(Notify::new());
    {
        let mut n = n.lock().unwrap();
        n.send_gate = Some(gate.clone());
        n.started = Some(started.clone());
    }
    let mut poll = Box::pin(runner.poll_once(&f.alice));
    tokio::select! {
        _=started.notified()=>{},
        result=&mut poll=>panic!("expected delayed Send: {}",result.is_ok()),
    }
    let cutoff = tokio::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(10)).await;
    gate.notify_one();
    let mut round = poll.await.unwrap();
    assert_eq!(round.attachment_retries.len(), 1);
    assert!(
        round.attachment_retries[0].send_started <= cutoff,
        "late reply must keep its earlier Send START timestamp"
    );
    assert_eq!(round.delivery.sent, 0);
    assert_eq!(round.sealed, 0);
    assert!(round.rpc_attempts <= MAX_ROUND_RPCS);
    assert!(n.lock().unwrap().calls.contains(&"pending"));
    let proof = round.attachment_retries.remove(0).into_failure();
    let retry = f.alice.epoch_attachment_retry(proof).await.unwrap();
    assert!(retry.outgoing().needs_send());
    n.lock().unwrap().blob_unavailable = false;
    let round = runner.poll_once(&f.alice).await.unwrap();
    assert_eq!(round.delivery.sent, 1);
    assert!(round.attachment_retries.is_empty());
    let exact = f
        .alice
        .epoch_attachment_outgoing(id)
        .await
        .unwrap()
        .unwrap();
    assert!(!exact.needs_send());
    assert_eq!(exact.request(), retry.outgoing().request());
    let sends = n.lock().unwrap().sends.clone();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0], sends[1]);
    assert!(f.alice.renew_epoch_attachment_retry(retry).await.is_err());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
