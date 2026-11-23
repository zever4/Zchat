//! Fault-injected network with REAL protected store and libsignal. This is not
//! evidence of an actual socket outage; native TLS/PG interop covers the adapter.
use super::*;
use crate::encryption::{
    contract::{MESSAGE_RETENTION_MS, envelope_digest},
    delivery_runner::{DeliveryRunner, DeliveryTransport, RunnerConfig, RunnerError},
    native::RpcError,
};
use crate::proto::e2ee::{
    AckDisposition, Acknowledgement, Delivery, MessageState, MessageStatus, MessageStatuses,
    PendingMessages, SendMessageRequest, SendReceipt,
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, watch};
use tonic::Code;

#[derive(Default)]
struct Network {
    pending_limit: Option<usize>,
    conversations: HashMap<String, Conversation>,
    requests: Vec<SendMessageRequest>,
    receipts: HashMap<String, SendReceipt>,
    inbox: Vec<Delivery>,
    acknowledgements: Vec<Acknowledgement>,
    events: Vec<&'static str>,
    lose_send: usize,
    lose_ack: usize,
    send_error: Option<Code>,
    pending_error: Option<Code>,
    pending_started: Option<Arc<Notify>>,
    pending_gate: Option<Arc<Notify>>,
    send_started: Option<Arc<Notify>>,
    send_gate: Option<Arc<Notify>>,
}
#[derive(Clone)]
struct Fake {
    own: String,
    network: Arc<Mutex<Network>>,
}
fn conversation(scope: &SessionScope) -> Conversation {
    Conversation {
        conversation_id: scope.conversation.clone(),
        mode: ConversationMode::SignalPq1 as i32,
        own_device: Some(scope.own.clone()),
        peer_device: Some(Device {
            reference: Some(scope.peer.clone()),
            registration_id: scope.peer_registration,
            identity_key: scope.peer_identity.serialize().to_vec(),
            public_material_ready: true,
            public_material_revision: 1,
        }),
        state: ConversationState::Active as i32,
        ..Default::default()
    }
}
fn transport(p: &Pair) -> Fake {
    Fake {
        own: A.into(),
        network: Arc::new(Mutex::new(Network {
            conversations: [
                (A.into(), conversation(&p.a)),
                (B.into(), conversation(&p.b)),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        })),
    }
}
fn make_receipt(plan: &OutgoingPlan) -> SendReceipt {
    let e = plan.request().envelope.as_ref().unwrap();
    SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
    }
}
#[async_trait::async_trait]
impl DeliveryTransport for Fake {
    async fn send(&mut self, plan: &OutgoingPlan) -> Result<VerifiedSendReceipt, RpcError> {
        let (started, gate) = {
            let mut n = self.network.lock().unwrap();
            n.events.push("send");
            n.requests.push(plan.request().clone());
            if let Some(code) = n.send_error {
                return Err(RpcError::Status(code));
            }
            (n.send_started.clone(), n.send_gate.clone())
        };
        if let Some(started) = started {
            started.notify_one();
        }
        if let Some(gate) = gate {
            gate.notified().await;
        }
        let receipt = make_receipt(plan);
        let mut n = self.network.lock().unwrap();
        if !n.receipts.contains_key(&receipt.message_id) {
            n.receipts
                .insert(receipt.message_id.clone(), receipt.clone());
            let sequence = n.receipts.len() as i64;
            n.inbox.push(Delivery {
                sequence,
                envelope: plan.request().envelope.clone(),
                expires_at_unix_ms: receipt.expires_at_unix_ms,
                envelope_digest: receipt.envelope_digest.clone(),
            });
        }
        if n.lose_send != 0 {
            n.lose_send -= 1;
            return Err(RpcError::Transport);
        }
        Ok(VerifiedSendReceipt::from_rpc(plan.clone(), receipt))
    }
    async fn statuses(
        &mut self,
        plans: &[OutgoingPlan],
    ) -> Result<VerifiedMessageStatuses, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.events.push("statuses");
        let response = MessageStatuses {
            messages: plans
                .iter()
                .map(|plan| {
                    let receipt = make_receipt(plan);
                    let state = n
                        .acknowledgements
                        .iter()
                        .find(|a| a.message_id == receipt.message_id)
                        .map(|a| {
                            if a.disposition == AckDisposition::Stored as i32 {
                                MessageState::RecipientStored
                            } else {
                                MessageState::RecipientRejected
                            }
                        })
                        .unwrap_or(MessageState::Pending);
                    MessageStatus {
                        observed_at_unix_ms: receipt.stored_at_unix_ms,
                        receipt: Some(receipt),
                        state: state as i32,
                    }
                })
                .collect(),
        };
        Ok(VerifiedMessageStatuses::from_rpc(plans.to_vec(), response))
    }
    async fn pending(&mut self) -> Result<PendingMessages, RpcError> {
        let (started, gate) = {
            let mut n = self.network.lock().unwrap();
            n.events.push("pending");
            if let Some(code) = n.pending_error {
                return Err(RpcError::Status(code));
            }
            (n.pending_started.clone(), n.pending_gate.clone())
        };
        if let Some(started) = started {
            started.notify_one();
        }
        if let Some(gate) = gate {
            gate.notified().await;
        }
        let n = self.network.lock().unwrap();
        let mut page = PendingMessages::default();
        for delivery in n
            .inbox
            .iter()
            .filter(|d| {
                d.envelope
                    .as_ref()
                    .unwrap()
                    .recipient
                    .as_ref()
                    .unwrap()
                    .user_id
                    == self.own
            })
            .take(n.pending_limit.unwrap_or(32).min(32))
        {
            page.messages.push(delivery.clone());
            if page.encoded_len() > crate::encryption::contract::MAX_RESPONSE_BYTES {
                page.messages.pop();
                break;
            }
        }
        Ok(page)
    }
    async fn lookup(
        &mut self,
        plan: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.events.push("lookup");
        Ok(VerifiedConversationLookup::from_rpc(
            plan.clone(),
            n.conversations[&self.own].clone(),
        ))
    }
    async fn acknowledge(&mut self, proof: &DurableAck) -> Result<VerifiedAck, RpcError> {
        let mut n = self.network.lock().unwrap();
        n.events.push("ack");
        n.acknowledgements.push(proof.request().messages[0].clone());
        n.inbox.retain(|d| {
            d.envelope.as_ref().unwrap().message_id != proof.request().messages[0].message_id
        });
        if n.lose_ack != 0 {
            n.lose_ack -= 1;
            return Err(RpcError::Transport);
        }
        Ok(VerifiedAck::from_rpc(proof.clone()))
    }
}
async fn encrypt(p: &Pair, count: usize) -> Vec<EncryptedPayload> {
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let mut result = Vec::new();
    for i in 0..count {
        let text = payload(&p.a, &format!("runner real Signal {i}"));
        p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
        result.push(text);
    }
    result
}
fn runner(api: Fake) -> DeliveryRunner<Fake> {
    DeliveryRunner::new(api, RunnerConfig::default()).unwrap()
}

#[tokio::test]
async fn lost_send_and_ack_recover_after_worker_restart_without_new_ciphertext() {
    let p = pair(true, false).await;
    let text = encrypt(&p, 1).await.pop().unwrap();
    let api = transport(&p);
    api.network.lock().unwrap().lose_send = 1;
    let mut sending = runner(api.clone());
    let report = sending.poll_once(&p.alice).await.unwrap();
    assert_eq!(report.sent, 0);
    assert_eq!(report.issues.len(), 1);
    assert_eq!(api.network.lock().unwrap().receipts.len(), 1);
    assert!(
        p.alice
            .outgoing_plan(text.message_id.clone())
            .await
            .unwrap()
            .progress()
            .receipt()
            .is_none()
    );
    p.alice.shutdown().await.unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let mut sending = runner(api.clone());
    assert_eq!(sending.poll_once(&alice).await.unwrap().sent, 1);
    {
        let n = api.network.lock().unwrap();
        assert_eq!(n.requests.len(), 2);
        assert_eq!(n.requests[0], n.requests[1]);
        assert_eq!(n.receipts.len(), 1);
    }
    let bob_api = Fake {
        own: B.into(),
        network: api.network.clone(),
    };
    api.network.lock().unwrap().lose_ack = 1;
    let mut receiving = runner(bob_api.clone());
    let report = receiving.poll_once(&p.bob).await.unwrap();
    assert_eq!((report.received, report.acknowledged), (1, 0));
    assert_eq!(report.issues.len(), 1);
    assert!(api.network.lock().unwrap().inbox.is_empty());
    assert_eq!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    let managed = p.bob.conversation_scope(CHAT.into()).await.unwrap();
    let wire = api.network.lock().unwrap().requests[0]
        .envelope
        .clone()
        .unwrap();
    assert_eq!(p.bob.decrypt(managed, wire).await.unwrap(), text);
    p.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    let mut receiving = runner(bob_api);
    assert_eq!(receiving.poll_once(&bob).await.unwrap().acknowledged, 1);
    assert!(
        bob.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    {
        let n = api.network.lock().unwrap();
        assert_eq!(n.acknowledgements.len(), 2);
        assert_eq!(n.acknowledgements[0], n.acknowledgements[1]);
        assert_eq!(n.events.iter().filter(|e| **e == "lookup").count(), 1);
    }
    assert_eq!(sending.poll_once(&alice).await.unwrap().statuses, 1);
    assert_eq!(
        alice
            .outgoing_plan(text.message_id)
            .await
            .unwrap()
            .progress()
            .status()
            .unwrap()
            .state,
        MessageState::RecipientStored as i32
    );
    let before = api
        .network
        .lock()
        .unwrap()
        .events
        .iter()
        .filter(|e| **e == "statuses")
        .count();
    sending.poll_once(&alice).await.unwrap();
    assert_eq!(
        api.network
            .lock()
            .unwrap()
            .events
            .iter()
            .filter(|e| **e == "statuses")
            .count(),
        before
    );
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn pages_are_bounded_and_pending_ack_skips_completed_history_on_restart() {
    let p = pair(true, false).await;
    encrypt(&p, 33).await;
    let api = transport(&p);
    let mut sending = runner(api.clone());
    assert_eq!(sending.poll_once(&p.alice).await.unwrap().sent, 32);
    assert_eq!(sending.poll_once(&p.alice).await.unwrap().sent, 1);
    let bob_api = Fake {
        own: B.into(),
        network: api.network.clone(),
    };
    let mut receiving = runner(bob_api.clone());
    let mut total = 0;
    loop {
        let remaining = api
            .network
            .lock()
            .unwrap()
            .inbox
            .iter()
            .filter(|d| {
                d.envelope
                    .as_ref()
                    .unwrap()
                    .recipient
                    .as_ref()
                    .unwrap()
                    .user_id
                    == B
            })
            .count();
        if remaining == 1 {
            break;
        }
        api.network.lock().unwrap().pending_limit = Some(remaining - 1);
        let report = receiving.poll_once(&p.bob).await.unwrap();
        assert!(report.issues.is_empty());
        assert!(report.received > 0 && report.received <= 32);
        total += report.received;
    }
    assert_eq!(total, 32);
    api.network.lock().unwrap().pending_limit = None;
    api.network.lock().unwrap().lose_ack = 1;
    assert_eq!(receiving.poll_once(&p.bob).await.unwrap().received, 1);
    // A fresh runner seeks the pending 33rd journal immediately, bypassing
    // 32 retained ACKs. No polling round walks completed history.
    let mut restarted = runner(bob_api);
    assert_eq!(restarted.poll_once(&p.bob).await.unwrap().acknowledged, 1);
    assert_eq!(restarted.poll_once(&p.bob).await.unwrap().acknowledged, 0);
    assert_eq!(
        api.network
            .lock()
            .unwrap()
            .requests
            .iter()
            .filter(|r| {
                r.envelope
                    .as_ref()
                    .unwrap()
                    .sender
                    .as_ref()
                    .unwrap()
                    .user_id
                    == A
            })
            .count(),
        33
    );
    assert_eq!(p.bob.outbox_page(None).await.unwrap().messages.len(), 32);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_incoming_is_not_auto_rejected_and_does_not_block_other_batch_items() {
    let p = pair(true, false).await;
    encrypt(&p, 2).await;
    let api = transport(&p);
    runner(api.clone()).poll_once(&p.alice).await.unwrap();
    {
        let mut n = api.network.lock().unwrap();
        let bad = &mut n.inbox[0];
        bad.envelope.as_mut().unwrap().ciphertext = vec![1, 2, 3];
        bad.envelope_digest = envelope_digest(bad.envelope.as_ref().unwrap())
            .unwrap()
            .to_vec();
    }
    let mut receiving = runner(Fake {
        own: B.into(),
        network: api.network.clone(),
    });
    let report = receiving.poll_once(&p.bob).await.unwrap();
    assert_eq!(
        (report.received, report.acknowledged, report.issues.len()),
        (1, 1, 1)
    );
    assert_eq!(api.network.lock().unwrap().inbox.len(), 1);
    assert!(
        api.network
            .lock()
            .unwrap()
            .acknowledgements
            .iter()
            .all(|a| a.disposition == AckDisposition::Stored as i32)
    );
    // No synthetic quarantine/Rejected journal was created for the bad message.
    assert!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn closed_peer_chat_receives_first_prekey_but_does_not_generate_outbound_messages() {
    let p = pair(true, false).await;
    encrypt(&p, 1).await;
    let api = transport(&p);
    runner(api.clone()).poll_once(&p.alice).await.unwrap();
    {
        let mut n = api.network.lock().unwrap();
        let value = n.conversations.get_mut(B).unwrap();
        value.state = ConversationState::Closed as i32;
        value.close_reason = crate::proto::e2ee::ConversationCloseReason::DeviceReplaced as i32;
        value.closed_generation_id = AG.into();
        value.closed_at_unix_ms = 1_700_000_000_000;
        value.peer_device.as_mut().unwrap().public_material_ready = false;
    }
    let report = runner(Fake {
        own: B.into(),
        network: api.network.clone(),
    })
    .poll_once(&p.bob)
    .await
    .unwrap();
    assert_eq!((report.received, report.acknowledged), (1, 1));
    assert!(p.bob.outbox_page(None).await.unwrap().messages.is_empty());
    let scope = p.bob.conversation_scope(CHAT.into()).await.unwrap();
    assert!(matches!(
        p.bob
            .encrypt(scope.clone(), payload(&scope, "closed"))
            .await,
        Err(CryptoError::ConversationClosed)
    ));
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn generic_auth_failure_stops_without_replacement_or_mutating_saved_outbox() {
    let p = pair(true, false).await;
    let text = encrypt(&p, 1).await.pop().unwrap();
    let before = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap()
        .request()
        .clone();
    let api = transport(&p);
    api.network.lock().unwrap().send_error = Some(Code::Unauthenticated);
    assert!(matches!(
        runner(api.clone()).poll_once(&p.alice).await,
        Err(RunnerError::Rpc(RpcError::Status(Code::Unauthenticated)))
    ));
    assert_eq!(
        p.alice
            .outgoing_plan(text.message_id)
            .await
            .unwrap()
            .request(),
        &before
    );
    assert_eq!(api.network.lock().unwrap().events, ["send"]);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn outage_yields_network_lane_and_later_round_retries_saved_work() {
    let p = pair(true, false).await;
    encrypt(&p, 2).await;
    let api = transport(&p);
    api.network.lock().unwrap().send_error = Some(Code::Unavailable);
    let mut sending = runner(api.clone());
    let report = sending.poll_once(&p.alice).await.unwrap();
    assert_eq!(report.issues.len(), 1);
    assert_eq!(api.network.lock().unwrap().events, ["send", "pending"]);
    api.network.lock().unwrap().send_error = None;
    for _ in 0..3 {
        sending.poll_once(&p.alice).await.unwrap();
    }
    assert_eq!(api.network.lock().unwrap().receipts.len(), 2);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_drops_inflight_network_future_and_preserves_durable_work() {
    let p = pair(true, false).await;
    let text = encrypt(&p, 1).await.pop().unwrap();
    let api = transport(&p);
    let started = Arc::new(Notify::new());
    {
        let mut n = api.network.lock().unwrap();
        n.send_started = Some(started.clone());
        n.send_gate = Some(Arc::new(Notify::new()));
    }
    let mut running = DeliveryRunner::new(
        api.clone(),
        RunnerConfig {
            poll_interval: Duration::from_millis(1),
            ..Default::default()
        },
    )
    .unwrap();
    let (stop, receiver) = watch::channel(false);
    let cancel = async {
        started.notified().await;
        stop.send(true).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(running.run(&p.alice, receiver), cancel)
    })
    .await
    .unwrap();
    result.unwrap();
    assert!(
        p.alice
            .outgoing_plan(text.message_id)
            .await
            .unwrap()
            .progress()
            .receipt()
            .is_none()
    );
    assert!(api.network.lock().unwrap().receipts.is_empty());
    // There is no spawned RPC task: opening the abandoned gate cannot send it.
    api.network
        .lock()
        .unwrap()
        .send_gate
        .take()
        .unwrap()
        .notify_one();
    running.poll_once(&p.alice).await.unwrap();
    for _ in 0..2 {
        running.poll_once(&p.alice).await.unwrap();
    }
    assert_eq!(api.network.lock().unwrap().receipts.len(), 1);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_or_pre_cancelled_owner_starts_no_network_work() {
    let p = pair(true, false).await;
    let api = transport(&p);
    let (sender, receiver) = watch::channel(false);
    drop(sender);
    runner(api.clone()).run(&p.alice, receiver).await.unwrap();
    let (_sender, receiver) = watch::channel(true);
    runner(api.clone()).run(&p.alice, receiver).await.unwrap();
    assert!(api.network.lock().unwrap().events.is_empty());
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn round_timeout_rotates_to_incoming_lane_and_cancels_stalled_send() {
    let p = pair(true, false).await;
    encrypt(&p, 1).await;
    let api = transport(&p);
    let incoming = Arc::new(Notify::new());
    {
        let mut n = api.network.lock().unwrap();
        n.send_gate = Some(Arc::new(Notify::new()));
        n.pending_started = Some(incoming.clone());
        n.pending_gate = Some(Arc::new(Notify::new()));
    }
    let mut running = DeliveryRunner::new(
        api.clone(),
        RunnerConfig {
            poll_interval: Duration::from_millis(1),
            retry_initial: Duration::from_millis(2),
            retry_max: Duration::from_millis(4),
            round_timeout: Duration::from_millis(20),
        },
    )
    .unwrap();
    let (stop, receiver) = watch::channel(false);
    let cancel = async {
        incoming.notified().await;
        stop.send(true).unwrap();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(running.run(&p.alice, receiver), cancel)
    })
    .await
    .unwrap();
    result.unwrap();
    assert!(matches!(
        running.last_report().unwrap().issues[0].error,
        crate::encryption::delivery_runner::DeliveryIssue::Timeout
    ));
    assert_eq!(api.network.lock().unwrap().events, ["send", "pending"]);
    assert!(api.network.lock().unwrap().receipts.is_empty());
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn previously_explicit_quarantine_recovers_rejected_ack_without_decrypt() {
    let p = pair(true, false).await;
    encrypt(&p, 1).await;
    let api = transport(&p);
    runner(api.clone()).poll_once(&p.alice).await.unwrap();
    let mut bob_api = Fake {
        own: B.into(),
        network: api.network.clone(),
    };
    let plan = p
        .bob
        .prepare_conversation_lookup(CHAT.into())
        .await
        .unwrap();
    p.bob
        .accept_conversation_lookup(bob_api.lookup(&plan).await.unwrap())
        .await
        .unwrap();
    let scope = p.bob.conversation_scope(CHAT.into()).await.unwrap();
    let mut wire = api.network.lock().unwrap().inbox[0].clone();
    wire.envelope.as_mut().unwrap().ciphertext = vec![1, 2, 3];
    wire.envelope_digest = envelope_digest(wire.envelope.as_ref().unwrap())
        .unwrap()
        .to_vec();
    api.network.lock().unwrap().inbox[0] = wire.clone();
    p.bob
        .quarantine_delivery(scope, wire, QuarantineReason::InvalidCiphertext)
        .await
        .unwrap();
    let report = runner(bob_api).poll_once(&p.bob).await.unwrap();
    assert_eq!((report.received, report.acknowledged), (0, 1));
    assert_eq!(
        api.network.lock().unwrap().acknowledgements[0].disposition,
        AckDisposition::Rejected as i32
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn automatic_loop_retries_unknown_send_outcome_and_stops_after_receipt_commit() {
    let p = pair(true, false).await;
    let text = encrypt(&p, 1).await.pop().unwrap();
    let api = transport(&p);
    let polled = Arc::new(Notify::new());
    {
        let mut n = api.network.lock().unwrap();
        n.lose_send = 1;
        n.pending_started = Some(polled.clone());
    }
    let mut running = DeliveryRunner::new(
        api.clone(),
        RunnerConfig {
            poll_interval: Duration::from_millis(2),
            retry_initial: Duration::from_millis(4),
            retry_max: Duration::from_millis(8),
            round_timeout: Duration::from_secs(1),
        },
    )
    .unwrap();
    let (stop, receiver) = watch::channel(false);
    let cancel_when_committed = async {
        loop {
            polled.notified().await;
            if p.alice
                .outgoing_plan(text.message_id.clone())
                .await
                .unwrap()
                .progress()
                .receipt()
                .is_some()
            {
                stop.send(true).unwrap();
                break;
            }
        }
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(running.run(&p.alice, receiver), cancel_when_committed)
    })
    .await
    .unwrap();
    result.unwrap();
    {
        let n = api.network.lock().unwrap();
        assert_eq!(n.requests.len(), 2);
        assert_eq!(n.requests[0], n.requests[1]);
        assert_eq!(n.receipts.len(), 1);
    }
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}
