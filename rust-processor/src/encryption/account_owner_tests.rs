//! Real protected workers; a gated transport proves lifecycle and scheduling.
use super::tests::{Fixture, index, initialize, records};
use super::*;
use crate::encryption::{
    account_owner::*, delivery_runner::RunnerConfig, epoch_runner::*, native::*,
    prekey_maintenance::*, signal_state::tests::store,
};
use crate::proto::e2ee as common;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Waker},
    time::Duration,
};
use tokio::sync::Notify;
use tonic::Code;
#[path = "account_archive_file_tests.rs"]
mod archive_file_tests;
#[path = "account_archive_outgoing_file_tests.rs"]
mod archive_outgoing_file_tests;
#[path = "account_blob_tests.rs"]
mod blob_tests;
#[path = "account_download_tests.rs"]
mod download_tests;
#[path = "account_file_ack_tests.rs"]
mod file_ack_tests;
#[path = "account_file_status_tests.rs"]
mod file_status_tests;
#[path = "account_outgoing_retirement_tests.rs"]
mod outgoing_retirement_tests;
const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";

// Bare install_material fixtures intentionally lack provisioning/publication
// journals. An account owner also runs maintenance, so use the real durable
// registration/publication flow, without bypassing production readiness checks.
async fn fixture() -> Fixture {
    fixture_at("https://chat.example.test").await
}
async fn fixture_at(origin: &str) -> Fixture {
    use crate::encryption::{provisioning::DeviceProvisioning, publication::publication_digest};
    let root = tempfile::tempdir().unwrap();
    let mut locals = Vec::new();
    let mut devices = Vec::new();
    let mut bundles = Vec::new();
    for (user, generation) in [
        (A, "33333333-3333-4333-8333-333333333333"),
        (B, "44444444-4444-4444-8444-444444444444"),
    ] {
        let mut local = DeviceProvisioning::new(crate::encryption::protected::test_open(
            root.path(),
            crate::encryption::protected::StoreScope::new(origin, user).unwrap(),
            true,
        ));
        let attempt = local.prepare_registration().unwrap();
        let mut device = common::Device {
            reference: Some(DeviceRef {
                user_id: user.into(),
                device_id: 1,
                generation_id: generation.into(),
            }),
            registration_id: attempt.request.registration_id,
            identity_key: attempt.request.identity_key.clone(),
            public_material_ready: false,
            public_material_revision: 0,
        };
        local
            .bind_registration(
                &attempt.request.registration_request_id,
                &common::OwnDeviceState {
                    user_id: user.into(),
                    active_device: Some(device.clone()),
                },
            )
            .unwrap();
        let mut signed = None;
        let mut ec = None;
        let mut pq = None;
        while let Some(upload) = local.next_upload().unwrap() {
            signed = signed.or(upload.signed_pre_key.clone());
            ec = ec.or(upload.one_time_pre_keys.first().cloned());
            pq = pq.or(upload
                .kyber_pre_keys
                .iter()
                .find(|k| !k.is_last_resort)
                .cloned());
            let receipt = common::UploadReceipt {
                batch_id: upload.batch_id.clone(),
                batch_digest: publication_digest(&upload).unwrap().to_vec(),
                publication_revision: upload.expected_revision + 1,
            };
            local.acknowledge_upload(&receipt).unwrap();
            device.public_material_revision = receipt.publication_revision;
        }
        device.public_material_ready = true;
        let lease = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            + Duration::from_secs(3600))
        .as_millis() as i64;
        bundles.push(common::AcquiredBundle {
            conversation_id: CHAT.into(),
            acquisition_id: uuid::Uuid::new_v4().to_string(),
            device: Some(device.clone()),
            signed_pre_key: signed,
            one_time_pre_key: ec,
            kyber_pre_key: pq,
            not_after_unix_ms: lease,
        });
        assert_eq!(
            local.ready_device().unwrap(),
            *device.reference.as_ref().unwrap()
        );
        devices.push(device);
        locals.push(local);
    }
    let alice = CryptoCoordinator::start(
        locals.remove(0).into_store(),
        devices[0].reference.clone().unwrap(),
    )
    .await
    .unwrap();
    let bob = CryptoCoordinator::start(
        locals.remove(0).into_store(),
        devices[1].reference.clone().unwrap(),
    )
    .await
    .unwrap();
    let a = Conversation {
        conversation_id: CHAT.into(),
        mode: 1,
        own_device: devices[0].reference.clone(),
        peer_device: Some(devices[1].clone()),
        state: ConversationState::Active as i32,
        ..Default::default()
    };
    let b = Conversation {
        own_device: devices[1].reference.clone(),
        peer_device: Some(devices[0].clone()),
        ..a.clone()
    };
    index(&alice, &a).await;
    index(&bob, &b).await;
    let reverse_bundle = bundles.remove(0);
    let bundle = bundles.remove(0);
    Fixture {
        root,
        alice,
        bob,
        a,
        next_bundle: bundle.clone(),
        bundle,
        reverse_bundle,
    }
}
async fn prepared() -> (Fixture, EpochScope, EpochScope) {
    prepare_fixture(fixture().await).await
}
async fn prepared_at(origin: &str) -> (Fixture, EpochScope, EpochScope) {
    prepare_fixture(fixture_at(origin).await).await
}
async fn prepare_fixture(f: Fixture) -> (Fixture, EpochScope, EpochScope) {
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let p = f.alice.prepare_epoch_acquisition(a.clone()).await.unwrap();
    let old = &f.bundle;
    let bundle = v2::AcquiredBundle {
        protocol_version: 2,
        conversation_id: CHAT.into(),
        acquisition_id: p.request().acquisition_id.clone(),
        device: old.device.clone(),
        signed_pre_key: old.signed_pre_key.clone(),
        one_time_pre_key: old.one_time_pre_key.clone(),
        kyber_pre_key: old.kyber_pre_key.clone(),
        not_after_unix_ms: old.not_after_unix_ms,
        crypto_epoch: Some(0),
    };
    f.alice
        .bootstrap_epoch(EpochObservation::test_observation(
            a.scope.clone(),
            p,
            bundle,
        ))
        .await
        .unwrap();
    (f, a, b)
}

#[tokio::test]
async fn established_text_commits_offline_while_the_only_rpc_is_stalled() {
    let (f, scope, _) = prepared().await;
    let p = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    tokio::time::timeout(Duration::from_secs(10), p.pending_started.notified())
        .await
        .unwrap();
    let message = uuid::Uuid::new_v4().to_string();
    let queued = tokio::time::timeout(
        Duration::from_secs(5),
        c.send_text(CHAT, &message, "offline native text"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(queued.message_id, message);
    assert!(!queued.stored_on_server);
    assert_eq!(
        c.send_text(CHAT, &message, "offline native text")
            .await
            .unwrap(),
        queued
    );
    assert!(matches!(
        c.send_text(CHAT, &message, "changed").await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    assert_eq!(p.active.load(Ordering::SeqCst), 1);
    assert!(
        !p.calls
            .lock()
            .unwrap()
            .iter()
            .any(|v| matches!(*v, "lookup" | "initial" | "acquire" | "send"))
    );
    owner.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), scope.own.clone())
        .await
        .unwrap();
    let plan = w.epoch_outgoing_plan(message).await.unwrap();
    assert!(plan.needs_send());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_queued_text_has_no_history_or_outbox_and_no_bootstrap() {
    let (f, scope, _) = prepared().await;
    let p = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    let message = uuid::Uuid::new_v4().to_string();
    let mut request = Box::pin(c.send_text(CHAT, &message, "abandoned before execution"));
    assert!(
        request
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(request);
    // A following local request is a FIFO barrier after the abandoned command.
    c.conversation(CHAT).await.unwrap();
    assert!(matches!(
        c.outgoing(&message).await,
        Err(AccountError::Crypto(CryptoError::MessageUnavailable))
    ));
    owner.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), scope.own.clone())
        .await
        .unwrap();
    assert!(w.epoch_outbox_page(None).await.unwrap().messages.is_empty());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn both_network_lanes_progress_during_foreground_queries_without_overlapping_rpcs() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let p = Arc::new(Probe::default());
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    let mut updates = c.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(
            async {
                for _ in 0..100 {
                    c.conversation(CHAT).await.unwrap();
                }
            },
            async {
                while c.status().maintenance_steps < 2 || c.status().delivery_rounds < 2 {
                    updates.changed().await.unwrap();
                }
            }
        );
    })
    .await
    .unwrap();
    owner.shutdown().await.unwrap();
    assert_eq!(p.active.load(Ordering::SeqCst), 0);
    {
        let calls = p.calls.lock().unwrap();
        assert!(calls.iter().filter(|&&c| c == "inventory").count() >= 2);
        assert!(calls.iter().filter(|&&c| c == "pending").count() >= 2);
    }
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn text_views_are_account_bound_and_do_not_render_service_confirmations() {
    use super::incoming::tests::{delivery as observed, payload, send};
    let (f, a, b) = prepared().await;
    let text = payload(&a, "native-only local plaintext view");
    let d = send(&f.alice, &a, text.clone(), 1).await;
    f.bob
        .receive_epoch(b.clone(), observed(&b, &d))
        .await
        .unwrap();
    let reply = f
        .bob
        .epoch_outbox_page(None)
        .await
        .unwrap()
        .messages
        .remove(0);
    let envelope = reply.request().envelope.clone().unwrap();
    let confirmation_id = envelope.message_id.clone();
    let d = v2::Delivery {
        sequence: 2,
        envelope_digest: crate::encryption::epoch_contract::envelope_digest(&envelope)
            .unwrap()
            .to_vec(),
        envelope: Some(envelope),
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
    };
    assert!(f.alice.receive_epoch(a, observed(&b, &d)).await.is_err()); // wrong native account observation
    let a = f
        .alice
        .active_epoch_scope(CHAT.into())
        .await
        .unwrap()
        .unwrap();
    f.alice
        .receive_epoch(a.clone(), observed(&a, &d))
        .await
        .unwrap();
    let oa = launch(f.alice, &Arc::new(Probe::default()));
    let ob = launch(f.bob, &Arc::new(Probe::default()));
    let ca = oa.client();
    let cb = ob.client();
    phase(&ca, AccountPhase::Running).await;
    phase(&cb, AccountPhase::Running).await;
    assert_eq!(
        cb.received_text(&text.message_id)
            .await
            .unwrap()
            .unwrap()
            .text,
        "native-only local plaintext view"
    );
    assert!(ca.received_text(&text.message_id).await.is_err());
    assert!(ca.received_text(&confirmation_id).await.unwrap().is_none());
    assert!(cb.received_text(&confirmation_id).await.is_err());
    let progress = ca.outgoing(&text.message_id).await.unwrap();
    assert!(!progress.stored_on_server && !progress.sealed_unsent);
    assert!(cb.outgoing(&text.message_id).await.is_err());
    oa.shutdown().await.unwrap();
    ob.shutdown().await.unwrap();
}
fn test_send_receipt(p: &EpochOutgoingPlan) -> v2::SendReceipt {
    let e = p.request().envelope.as_ref().unwrap();
    v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: crate::encryption::epoch_contract::envelope_digest(e)
            .unwrap()
            .to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    }
}
#[derive(Default)]
struct Probe {
    file_status: Option<Arc<file_status_tests::FileStatusProbe>>,
    file_ack_requests: Mutex<Vec<v2::AcknowledgeFilesRequest>>,
    file_ack_receipts: Mutex<std::collections::HashMap<String, v2::FileAcknowledgements>>,
    file_ack_errors: Mutex<std::collections::HashMap<String, RpcError>>,
    file_ack_delay: Duration,
    file_ack_lose_reply_once: AtomicBool,
    block_file_ack: AtomicBool,
    file_ack_started: Notify,
    file_ack_release: Notify,
    file_ack_released: AtomicBool,
    active: AtomicUsize,
    calls: Mutex<Vec<&'static str>>,
    pending_started: Notify,
    release: Notify,
    block_pending: bool,
    pending_released: AtomicBool,
    create_started: Notify,
    create_release: Notify,
    block_create: AtomicBool,
    create_reply: Mutex<Option<common::Conversation>>,
    create_error: Mutex<Option<RpcError>>,
    create_attempts: Mutex<Vec<common::CreateConversationRequest>>,
    fatal: bool,
    revoked: AtomicBool,
    blob_leases: Mutex<std::collections::HashMap<String, v2::BlobLease>>,
    blob_errors: Mutex<std::collections::HashMap<String, RpcError>>,
    blob_requests: Mutex<Vec<v2::BlobLeaseRequest>>,
    blob_ready: Mutex<std::collections::HashSet<String>>,
    blob_events: Mutex<Vec<(&'static str, String)>>,
    blob_lose_reserve_once: AtomicBool,
    blob_fail_observe_after_put_once: AtomicBool,
    hold_receipt_for_observe_fault: AtomicBool,
    block_observe: AtomicBool,
    observe_started: Notify,
    observe_release: Notify,
    observe_released: AtomicBool,
    lookup_reply: Mutex<Option<common::Conversation>>,
    initial_accept: AtomicBool,
    acquire_bundle: Mutex<Option<common::AcquiredBundle>>,
    acquire_requests: Mutex<Vec<v2::AcquireBundleRequest>>,
    acquire_started: Notify,
    acquire_release: Notify,
    block_acquire: AtomicBool,
    acquire_released: AtomicBool,
    send_requests: Mutex<Vec<v2::SendMessageRequest>>,
    send_body_owned: Mutex<Vec<bool>>,
    send_accept: AtomicBool,
    send_requires_ready: AtomicBool,
    block_send: AtomicBool,
    send_started: Notify,
    send_release: Notify,
    send_released: AtomicBool,
    lose_send_reply: AtomicBool,
    send_receipts: Mutex<std::collections::HashMap<String, v2::SendReceipt>>,
    body_owned: Mutex<Option<Arc<AtomicBool>>>,
}
struct Flight(Arc<Probe>);
impl Drop for Flight {
    fn drop(&mut self) {
        assert_eq!(self.0.active.fetch_sub(1, Ordering::SeqCst), 1);
    }
}
struct Fake(Arc<Probe>);
impl Fake {
    fn blob(
        &self,
        plan: &crate::encryption::signal_state::EpochBlobControlPlan,
        reserve: bool,
    ) -> Result<EpochBlobControlOutcome, RpcError> {
        let _flight = self.enter(if reserve {
            "reserve_blob"
        } else {
            "observe_blob"
        });
        let r = plan.request();
        self.0.blob_events.lock().unwrap().push((
            if reserve { "reserve" } else { "observe" },
            r.message_id.clone(),
        ));
        self.0.blob_requests.lock().unwrap().push(r.clone());
        if self.0.revoked.load(Ordering::SeqCst) {
            return Err(RpcError::Status(Code::Unauthenticated));
        }
        if let Some(error) = self
            .0
            .blob_errors
            .lock()
            .unwrap()
            .get(&r.message_id)
            .cloned()
        {
            return Err(error);
        }
        let mut leases = self.0.blob_leases.lock().unwrap();
        if reserve
            && let Some(old) = leases.get(&r.message_id)
            && old.operation_id != r.operation_id
        {
            if old.revision != r.expected_revision || old.attempt_id != r.expected_attempt_id {
                return Err(RpcError::Status(Code::Aborted));
            }
            if !old.expired && old.state != v2::BlobLeaseState::Reclaimed as i32 {
                return Err(RpcError::Status(Code::FailedPrecondition));
            }
            let at = old.observed_at_unix_ms + 1;
            let g = v2::BlobLease {
                message_id: r.message_id.clone(),
                operation_id: r.operation_id.clone(),
                attempt_id: uuid::Uuid::new_v4().to_string(),
                revision: r.expected_revision + 1,
                reserved_at_unix_ms: at,
                expires_at_unix_ms: at + crate::encryption::blob_control::STAGING_LEASE_MS,
                observed_at_unix_ms: at,
                state: v2::BlobLeaseState::Reserved as i32,
                expired: false,
            };
            leases.insert(r.message_id.clone(), g);
            self.0.blob_ready.lock().unwrap().remove(&r.message_id);
        }
        if reserve && !leases.contains_key(&r.message_id) {
            leases.insert(
                r.message_id.clone(),
                v2::BlobLease {
                    message_id: r.message_id.clone(),
                    operation_id: r.operation_id.clone(),
                    attempt_id: uuid::Uuid::new_v4().to_string(),
                    revision: r.expected_revision + 1,
                    reserved_at_unix_ms: 1000,
                    expires_at_unix_ms: 3_601_000,
                    observed_at_unix_ms: 1000,
                    state: v2::BlobLeaseState::Reserved as i32,
                    expired: false,
                },
            );
        }
        let Some(lease) = leases.get_mut(&r.message_id) else {
            return Ok(EpochBlobControlOutcome::Rejected(Box::new(
                EpochObservation::test_observation(
                    plan.store_scope().clone(),
                    plan.clone(),
                    v2::BlobFailure {
                        reason: v2::BlobFailureReason::OperationNotObserved as i32,
                        request: Some(r.clone()),
                    },
                ),
            )));
        };
        if reserve
            && r.expected_revision > 0
            && self.0.blob_lose_reserve_once.swap(false, Ordering::SeqCst)
        {
            return Err(RpcError::Transport);
        }
        if !reserve
            && self.0.blob_ready.lock().unwrap().contains(&r.message_id)
            && self
                .0
                .blob_fail_observe_after_put_once
                .swap(false, Ordering::SeqCst)
        {
            return Err(RpcError::Transport);
        }
        if self.0.blob_ready.lock().unwrap().contains(&r.message_id) {
            lease.state = v2::BlobLeaseState::Ready as i32;
            lease.observed_at_unix_ms += 1;
        }
        Ok(EpochBlobControlOutcome::Lease(Box::new(
            EpochObservation::test_observation(
                plan.store_scope().clone(),
                plan.clone(),
                lease.clone(),
            ),
        )))
    }
    fn enter(&self, call: &'static str) -> Flight {
        assert_eq!(
            self.0.active.fetch_add(1, Ordering::SeqCst),
            0,
            "account RPCs overlapped"
        );
        self.0.calls.lock().unwrap().push(call);
        Flight(self.0.clone())
    }
}
#[async_trait::async_trait]
impl EpochDeliveryTransport for Fake {
    async fn file_statuses(
        &mut self,
        plans: &[EpochOutgoingPlan],
    ) -> Result<EpochFileStatuses, RpcError> {
        let Some(probe) = &self.0.file_status else {
            return Err(RpcError::Status(Code::Unavailable));
        };
        let _flight = self.enter("file_status");
        probe.observe(plans).await
    }
    async fn acknowledge_file(
        &mut self,
        p: &crate::encryption::signal_state::EpochDurableFileAck,
    ) -> Result<
        EpochObservation<
            crate::encryption::signal_state::EpochDurableFileAck,
            v2::FileAcknowledgements,
        >,
        RpcError,
    > {
        let _flight = self.enter("file_ack");
        let r = p.request();
        {
            let mut all = self.0.file_ack_requests.lock().unwrap();
            assert!(all.len() < 256);
            all.push(r.clone());
        }
        if !self.0.file_ack_delay.is_zero() {
            tokio::time::sleep(self.0.file_ack_delay).await;
        }
        if let Some(e) = self
            .0
            .file_ack_errors
            .lock()
            .unwrap()
            .get(p.message_id())
            .cloned()
        {
            return Err(e);
        }
        let response = {
            let mut receipts = self.0.file_ack_receipts.lock().unwrap();
            let saved =
                receipts
                    .entry(p.message_id().into())
                    .or_insert_with(|| v2::FileAcknowledgements {
                        files: vec![v2::FileAcknowledgementReceipt {
                            acknowledgement: Some(r.files[0].clone()),
                            acknowledged_at_unix_ms: 1_700_000_000_123,
                        }],
                    });
            assert_eq!(saved.files[0].acknowledgement.as_ref(), Some(&r.files[0]));
            saved.clone()
        };
        self.0.file_ack_started.notify_one();
        if self.0.block_file_ack.load(Ordering::SeqCst)
            && !self.0.file_ack_released.load(Ordering::SeqCst)
        {
            self.0.file_ack_release.notified().await;
        }
        if self
            .0
            .file_ack_lose_reply_once
            .swap(false, Ordering::SeqCst)
        {
            return Err(RpcError::Transport);
        }
        Ok(EpochObservation::test_observation(
            p.store_scope().clone(),
            p.clone(),
            response,
        ))
    }
    async fn observe_blob(
        &mut self,
        plan: &crate::encryption::signal_state::EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome, RpcError> {
        if self.0.block_observe.load(Ordering::SeqCst) {
            self.0.observe_started.notify_one();
            while !self.0.observe_released.load(Ordering::SeqCst) {
                self.0.observe_release.notified().await;
            }
        }
        self.blob(plan, false)
    }
    async fn reserve_blob(
        &mut self,
        plan: &crate::encryption::signal_state::EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome, RpcError> {
        self.blob(plan, true)
    }
    async fn create(
        &mut self,
        p: &ConversationCreation,
    ) -> Result<VerifiedConversationCreation, RpcError> {
        let _f = self.enter("create");
        self.0
            .create_attempts
            .lock()
            .unwrap()
            .push(p.request().clone());
        self.0.create_started.notify_one();
        if self.0.block_create.swap(false, Ordering::SeqCst) {
            self.0.create_release.notified().await;
        }
        if let Some(e) = self.0.create_error.lock().unwrap().clone() {
            return Err(e);
        }
        let reply = self
            .0
            .create_reply
            .lock()
            .unwrap()
            .clone()
            .ok_or(RpcError::Transport)?;
        Ok(VerifiedConversationCreation::from_rpc(p.clone(), reply))
    }
    async fn send(&mut self, p: &EpochOutgoingPlan) -> Result<EpochPreparedSendOutcome, RpcError> {
        let _f = self.enter("send");
        let e = p.request().envelope.as_ref().unwrap();
        self.0
            .blob_events
            .lock()
            .unwrap()
            .push(("send", e.message_id.clone()));
        self.0.send_started.notify_one();
        while self.0.block_send.load(Ordering::SeqCst)
            && !self.0.send_released.load(Ordering::SeqCst)
        {
            self.0.send_release.notified().await;
        }
        // Another message may legitimately Send while an unrelated PUT waits.
        // Tests assert the ownership observation for the relevant message.
        self.0.send_body_owned.lock().unwrap().push(
            self.0
                .body_owned
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|owned| owned.load(Ordering::SeqCst)),
        );
        self.0
            .send_requests
            .lock()
            .unwrap()
            .push(p.request().clone());
        if !self.0.send_accept.load(Ordering::SeqCst) {
            return Err(RpcError::Transport);
        }
        let saved = self
            .0
            .send_receipts
            .lock()
            .unwrap()
            .get(&e.message_id)
            .cloned();
        // Fault fixture keeps Send unavailable until the intended post-PUT
        // Observe failure has happened, rather than racing it with a receipt.
        if saved.is_none()
            && self.0.hold_receipt_for_observe_fault.load(Ordering::SeqCst)
            && self
                .0
                .blob_fail_observe_after_put_once
                .load(Ordering::SeqCst)
            && self.0.blob_ready.lock().unwrap().contains(&e.message_id)
        {
            return Err(RpcError::Transport);
        }
        if saved.is_none()
            && e.protocol_version == 3
            && self.0.send_requires_ready.load(Ordering::SeqCst)
        {
            let lease = self
                .0
                .blob_leases
                .lock()
                .unwrap()
                .get(&e.message_id)
                .cloned();
            if !lease.as_ref().is_some_and(|g| {
                !g.expired
                    && (g.state == v2::BlobLeaseState::Ready as i32
                        || self.0.blob_ready.lock().unwrap().contains(&e.message_id))
            }) {
                let reason = if lease.is_some_and(|g| g.expired) {
                    2
                } else {
                    1
                };
                return Ok(EpochPreparedSendOutcome::BlobUnavailable(Box::new(
                    EpochObservation::test_observation(
                        p.store_scope().clone(),
                        p.clone(),
                        v2::BlobSendFailure {
                            reason,
                            binding: Some(
                                crate::encryption::epoch_contract::send_binding(e).unwrap(),
                            ),
                        },
                    ),
                )));
            }
        }
        let receipt = saved.unwrap_or_else(|| test_send_receipt(p));
        {
            let mut receipts = self.0.send_receipts.lock().unwrap();
            if let Some(old) = receipts.get(&e.message_id) {
                assert_eq!(old, &receipt);
            } else {
                receipts.insert(e.message_id.clone(), receipt.clone());
            }
        }
        if self.0.lose_send_reply.swap(false, Ordering::SeqCst) {
            return Err(RpcError::Transport);
        }
        Ok(EpochPreparedSendOutcome::Accepted(Box::new(
            EpochObservation::test_observation(p.store_scope().clone(), p.clone(), receipt),
        )))
    }
    async fn statuses(&mut self, _: &[EpochOutgoingPlan]) -> Result<EpochStatuses, RpcError> {
        let _f = self.enter("statuses");
        Err(RpcError::Transport)
    }
    async fn pending(&mut self) -> Result<Vec<EpochDelivery>, RpcError> {
        let _f = self.enter("pending");
        self.0.pending_started.notify_one();
        if self.0.block_pending && !self.0.pending_released.load(Ordering::SeqCst) {
            self.0.release.notified().await;
        }
        if self.0.fatal || self.0.revoked.load(Ordering::SeqCst) {
            return Err(RpcError::Status(Code::Unauthenticated));
        }
        Ok(vec![])
    }
    async fn acknowledge(
        &mut self,
        _: &EpochDurableAck,
    ) -> Result<EpochObservation<EpochDurableAck, common::Empty>, RpcError> {
        let _f = self.enter("ack");
        Err(RpcError::Transport)
    }
    async fn lookup(
        &mut self,
        p: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError> {
        let _f = self.enter("lookup");
        let reply = self
            .0
            .lookup_reply
            .lock()
            .unwrap()
            .clone()
            .ok_or(RpcError::Transport)?;
        Ok(VerifiedConversationLookup::from_rpc(p.clone(), reply))
    }
    async fn initial(
        &mut self,
        p: &EpochCatalogQuery,
    ) -> Result<EpochObservation<EpochCatalogQuery, v2::ConversationEpochs>, RpcError> {
        let _f = self.enter("initial");
        if self.0.initial_accept.load(Ordering::SeqCst) {
            return Ok(EpochObservation::test_observation(
                p.scope.clone(),
                p.clone(),
                v2::ConversationEpochs {
                    protocol_version: 2,
                    conversation: Some(p.pins.conversation().clone()),
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
            ));
        }
        Err(RpcError::Transport)
    }
    async fn references(&mut self, _: &str) -> Result<RecoveryReferences, RpcError> {
        let _f = self.enter("references");
        Err(RpcError::Transport)
    }
    async fn query(&mut self, _: &EpochRecoveryQuery) -> Result<RecoveryReply, RpcError> {
        let _f = self.enter("query");
        Err(RpcError::Transport)
    }
    async fn submit(&mut self, _: &EpochRecoveryIntent) -> Result<IntentReply, EpochRecoveryError> {
        let _f = self.enter("submit");
        Err(RpcError::Transport.into())
    }
    async fn catalog(&mut self, _: &EpochTransitionQuery) -> Result<TransitionReply, RpcError> {
        let _f = self.enter("catalog");
        Err(RpcError::Transport)
    }
    async fn acquire(
        &mut self,
        p: &EpochAcquisitionPlan,
    ) -> Result<EpochAcquisitionOutcome, RpcError> {
        let _f = self.enter("acquire");
        self.0
            .acquire_requests
            .lock()
            .unwrap()
            .push(p.request().clone());
        self.0.acquire_started.notify_one();
        while self.0.block_acquire.load(Ordering::SeqCst)
            && !self.0.acquire_released.load(Ordering::SeqCst)
        {
            self.0.acquire_release.notified().await;
        }
        let old = self
            .0
            .acquire_bundle
            .lock()
            .unwrap()
            .clone()
            .ok_or(RpcError::Transport)?;
        Ok(EpochAcquisitionOutcome::Bundle(Box::new(
            EpochObservation::test_observation(
                p.store_scope().clone(),
                p.clone(),
                v2::AcquiredBundle {
                    protocol_version: 2,
                    conversation_id: p.request().conversation_id.clone(),
                    acquisition_id: p.request().acquisition_id.clone(),
                    device: old.device,
                    signed_pre_key: old.signed_pre_key,
                    one_time_pre_key: old.one_time_pre_key,
                    kyber_pre_key: old.kyber_pre_key,
                    not_after_unix_ms: old.not_after_unix_ms,
                    crypto_epoch: p.request().crypto_epoch,
                },
            ),
        )))
    }
}
#[async_trait::async_trait]
impl PreKeyMaintenanceTransport for Fake {
    async fn inventory(&mut self) -> Result<VerifiedPreKeyInventory, RpcError> {
        let _f = self.enter("inventory");
        Err(RpcError::Transport)
    }
    async fn upload(
        &mut self,
        _: &PreKeyRefillUpload,
    ) -> Result<VerifiedPreKeyRefillUpload, RpcError> {
        let _f = self.enter("upload");
        Err(RpcError::Transport)
    }
}
fn launch(w: CryptoCoordinator, p: &Arc<Probe>) -> AccountOwner {
    let config = RunnerConfig {
        poll_interval: Duration::from_millis(10),
        retry_initial: Duration::from_millis(100),
        retry_max: Duration::from_millis(500),
        round_timeout: Duration::from_secs(30),
    };
    AccountOwner::spawn(
        w,
        EpochDeliveryRunner::new(Fake(p.clone()), config).unwrap(),
        PreKeyMaintenance::new(MaintenanceConfig {
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_millis(500),
            ..Default::default()
        })
        .unwrap(),
    )
}
async fn phase(client: &AccountClient, target: AccountPhase) {
    let mut updates = client.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client.status().phase == target {
                return;
            }
            assert!(
                updates.changed().await.is_some(),
                "account ended before expected phase"
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn foreground_survives_stalled_rpc_and_shutdown_releases_store_before_return() {
    let f = fixture().await;
    let scope = initialize(&f.alice).await;
    let p = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    tokio::time::timeout(Duration::from_secs(10), p.pending_started.notified())
        .await
        .unwrap();
    assert_eq!(p.active.load(Ordering::SeqCst), 1);
    let view = tokio::time::timeout(Duration::from_secs(5), c.conversation(CHAT))
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(view.crypto_epoch, 0);
    assert!(!view.sending_frozen);
    assert!(c.recoveries(CHAT).await.unwrap().is_empty());
    tokio::time::timeout(Duration::from_secs(5), owner.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p.active.load(Ordering::SeqCst), 0);
    assert!(matches!(
        c.conversation(CHAT).await,
        Err(AccountError::Stopped)
    ));
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), scope.own_device().clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_scope(CHAT.into(), 0).await.unwrap().crypto_epoch(),
        0
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_shutdown_caller_still_finishes_the_independent_cleanup_task() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &p);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    tokio::time::timeout(Duration::from_secs(10), p.pending_started.notified())
        .await
        .unwrap();
    let mut shutdown = Box::pin(owner.shutdown());
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(shutdown);
    phase(&client, AccountPhase::Stopped).await;
    assert_eq!(p.active.load(Ordering::SeqCst), 0);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own_device().clone())
        .await
        .unwrap();
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_admission_is_bounded_and_canceled_queued_intent_never_freezes() {
    let (f, _, _) = prepared().await;
    let p = Arc::new(Probe::default());
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    let mut held = Vec::new();
    for _ in 0..COMMAND_CAPACITY {
        let mut future = Box::pin(c.conversation(CHAT));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        held.push(future);
    }
    assert!(matches!(
        c.conversation(CHAT).await,
        Err(AccountError::Busy)
    ));
    drop(held);
    tokio::task::yield_now().await;
    let mut proposal = Box::pin(c.propose_recovery(CHAT));
    assert!(
        proposal
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(proposal);
    assert!(!c.conversation(CHAT).await.unwrap().unwrap().sending_frozen);
    assert!(c.recoveries(CHAT).await.unwrap().is_empty());
    assert!(matches!(
        c.conversation("../foreign-account").await,
        Err(AccountError::Invalid)
    ));
    owner.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn explicit_recovery_is_durable_while_offline_and_cancel_does_not_fake_remote_release() {
    let (f, _, _) = prepared().await;
    let p = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    tokio::time::timeout(Duration::from_secs(10), p.pending_started.notified())
        .await
        .unwrap();
    let id = c.propose_recovery(CHAT).await.unwrap();
    assert_eq!(c.propose_recovery(CHAT).await.unwrap(), id);
    let r = c.recoveries(CHAT).await.unwrap();
    assert_eq!(r.len(), 1);
    assert!(r[0].own_proposal && r[0].sending_frozen && r[0].state.is_none());
    assert!(
        c.decide_recovery(CHAT, &id, EpochRecoveryDecision::Accept)
            .await
            .is_err()
    );
    c.decide_recovery(CHAT, &id, EpochRecoveryDecision::Cancel)
        .await
        .unwrap();
    assert!(c.conversation(CHAT).await.unwrap().unwrap().sending_frozen);
    owner.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    let s = w.active_epoch_scope(CHAT.into()).await.unwrap().unwrap();
    let overview = w.epoch_recovery_overview(CHAT.into()).await.unwrap();
    assert_eq!(
        overview.proposals[0].decision(),
        Some(EpochRecoveryDecision::Cancel)
    );
    // The cancellation is durable, but server existence is still unknown.
    // Replay the original signed proposal before the saved Cancel.
    let saved = w.resume_epoch_recovery(s, id).await.unwrap().unwrap();
    assert_eq!(
        saved
            .request()
            .command
            .as_ref()
            .unwrap()
            .control
            .as_ref()
            .unwrap()
            .action,
        common::RecoveryAction::Propose as i32
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_accounts_and_dropped_owner_leave_stale_handles_closed() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let pa = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let pb = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let oa = launch(f.alice, &pa);
    let ob = launch(f.bob, &pb);
    let ca = oa.client();
    let cb = ob.client();
    phase(&ca, AccountPhase::Running).await;
    phase(&cb, AccountPhase::Running).await;
    drop(oa);
    phase(&ca, AccountPhase::Stopped).await;
    assert!(matches!(
        ca.conversation(CHAT).await,
        Err(AccountError::Stopped)
    ));
    assert_eq!(
        cb.conversation(CHAT).await.unwrap().unwrap().crypto_epoch,
        0
    );
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    let restarted = launch(w, &Arc::new(Probe::default()));
    let fresh = restarted.client();
    phase(&fresh, AccountPhase::Running).await;
    assert_eq!(
        fresh
            .conversation(CHAT)
            .await
            .unwrap()
            .unwrap()
            .crypto_epoch,
        0
    );
    assert!(
        fresh
            .received_text(&uuid::Uuid::new_v4().to_string())
            .await
            .is_err()
    );
    restarted.shutdown().await.unwrap();
    ob.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own_device().clone())
        .await
        .unwrap();
    w.shutdown().await.unwrap();
}

#[tokio::test]
async fn fatal_auth_stops_worker_without_replacing_identity() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let identity = records(f.root.path(), A)
        .into_iter()
        .filter(|r| r.0 == RecordKind::Identity as i64)
        .collect::<Vec<_>>();
    let p = Arc::new(Probe {
        fatal: true,
        ..Default::default()
    });
    let o = launch(f.alice, &p);
    let c = o.client();
    phase(&c, AccountPhase::Failed).await;
    assert!(matches!(
        o.shutdown().await,
        Err(AccountError::Delivery(
            crate::encryption::delivery_runner::RunnerError::Rpc(RpcError::Status(
                Code::Unauthenticated
            ))
        ))
    ));
    assert_eq!(p.active.load(Ordering::SeqCst), 0);
    assert_eq!(
        records(f.root.path(), A)
            .into_iter()
            .filter(|r| r.0 == RecordKind::Identity as i64)
            .collect::<Vec<_>>(),
        identity
    );
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own_device().clone())
        .await
        .unwrap();
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_before_first_poll_does_not_seed_and_corrupt_guard_is_not_repaired() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let before = records(f.root.path(), A);
    let p = Arc::new(Probe::default());
    let o = launch(f.alice, &p);
    let c = o.client();
    assert!(matches!(
        c.conversation(CHAT).await,
        Err(AccountError::Initializing)
    ));
    drop(o);
    phase(&c, AccountPhase::Stopped).await;
    assert_eq!(records(f.root.path(), A), before);
    assert!(p.calls.lock().unwrap().is_empty());
    let mut local = store(f.root.path(), A, false);
    local
        .transaction(|tx| {
            tx.put(
                &RecordKey::new(
                    &s.own_device().generation_id,
                    RecordKind::Control,
                    "recovery-index:anchor:v1",
                )?,
                &[2],
            )
        })
        .unwrap();
    let w = CryptoCoordinator::start(local, s.own_device().clone())
        .await
        .unwrap();
    let o = launch(w, &p);
    let c = o.client();
    phase(&c, AccountPhase::Failed).await;
    assert!(o.shutdown().await.is_err());
    assert!(p.calls.lock().unwrap().is_empty());
    let mut local = store(f.root.path(), A, false);
    assert_eq!(
        local
            .transaction(|tx| tx.get(&RecordKey::new(
                &s.own_device().generation_id,
                RecordKind::Control,
                "recovery-index:anchor:v1"
            )?))
            .unwrap()
            .unwrap()
            .as_slice(),
        &[2]
    );
    drop(local);
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn foreground_creation_is_single_flight_and_local_reads_survive_its_stalled_rpc() {
    let f = fixture().await;
    let p = Arc::new(Probe::default());
    p.block_create.store(true, Ordering::SeqCst);
    *p.create_reply.lock().unwrap() = Some(f.a.clone());
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    let mut pending = Box::pin(c.create_dialog(2));
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tokio::time::timeout(Duration::from_secs(10), p.create_started.notified())
        .await
        .unwrap();
    assert_eq!(p.active.load(Ordering::SeqCst), 1);
    assert!(c.conversation(CHAT).await.unwrap().is_none()); // no initial epoch bootstrap
    p.create_release.notify_one();
    let v = pending.await.unwrap();
    assert_eq!(v.conversation_id, CHAT);
    assert_eq!(v.peer_user_id, B);
    assert_eq!(v.state, common::ConversationState::Active);
    assert!(c.conversation(CHAT).await.unwrap().is_none());
    assert!(
        !p.calls
            .lock()
            .unwrap()
            .iter()
            .any(|v| matches!(*v, "initial" | "acquire" | "send"))
    );
    owner.shutdown().await.unwrap();
    assert_eq!(p.active.load(Ordering::SeqCst), 0);
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn both_command_queues_share_one_budget_and_cancelled_queued_create_never_commits() {
    let f = fixture().await;
    let p = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    tokio::time::timeout(Duration::from_secs(10), p.pending_started.notified())
        .await
        .unwrap();
    let mut held = Vec::new();
    for _ in 0..COMMAND_CAPACITY {
        let mut q = Box::pin(c.create_dialog(2));
        assert!(
            q.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        held.push(q);
    }
    assert!(matches!(
        c.conversation(CHAT).await,
        Err(AccountError::Busy)
    ));
    assert!(matches!(c.get_dialog(CHAT).await, Err(AccountError::Busy)));
    drop(held);
    p.pending_released.store(true, Ordering::SeqCst);
    p.release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match c.conversation(CHAT).await {
                Err(AccountError::Busy) => tokio::task::yield_now().await,
                Ok(None) => break,
                _ => panic!("unexpected foreground result"),
            }
        }
    })
    .await
    .unwrap();
    assert!(p.create_attempts.lock().unwrap().is_empty());
    owner.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn cancelled_inflight_create_keeps_exact_request_and_peer_denial_is_not_account_revocation() {
    let f = fixture().await;
    let p = Arc::new(Probe::default());
    p.block_create.store(true, Ordering::SeqCst);
    *p.create_reply.lock().unwrap() = Some(f.a.clone());
    let owner = launch(f.alice, &p);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    let mut q = Box::pin(c.create_dialog(2));
    assert!(
        q.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tokio::time::timeout(Duration::from_secs(10), p.create_started.notified())
        .await
        .unwrap();
    drop(q);
    let v = c.create_dialog(2).await.unwrap();
    assert_eq!(v.conversation_id, CHAT);
    {
        let attempts = p.create_attempts.lock().unwrap();
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0], attempts[1]);
    }
    *p.create_error.lock().unwrap() = Some(RpcError::Status(Code::PermissionDenied));
    assert!(matches!(
        c.create_dialog(3).await,
        Err(AccountError::Rpc(RpcError::Status(Code::PermissionDenied)))
    ));
    assert_eq!(c.status().phase, AccountPhase::Running);
    assert!(c.conversation(CHAT).await.unwrap().is_none());
    *p.create_error.lock().unwrap() = Some(RpcError::Status(Code::Unauthenticated));
    assert!(matches!(
        c.create_dialog(3).await,
        Err(AccountError::Rpc(RpcError::Status(Code::Unauthenticated)))
    ));
    phase(&c, AccountPhase::Failed).await;
    assert!(matches!(
        owner.shutdown().await,
        Err(AccountError::Rpc(RpcError::Status(Code::Unauthenticated)))
    ));
    assert_eq!(p.active.load(Ordering::SeqCst), 0);
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn checked_account_recovery_binds_epoch_and_reference_and_resolves_unknown_result_by_read() {
    let (f, _, _) = prepared().await;
    // This test compares ALL protected records around rejected recovery calls.
    // Finish unrelated file-index initialization before the background owner
    // can insert its control records between those snapshots. Waiting after
    // spawn would depend on whether the intentionally blocked RPC runs first.
    assert!(f.alice.seed_epoch_file_status_index().await.unwrap().ready);
    let p = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let owner = launch(f.alice, &p);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    let initial = client.recovery_overview(CHAT).await.unwrap();
    assert_eq!(initial.crypto_epoch, Some(0));
    let before = records(f.root.path(), A);
    assert!(matches!(
        client.propose_recovery_at(CHAT, 1, None).await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    assert_eq!(records(f.root.path(), A), before);
    let id = client.propose_recovery_at(CHAT, 0, None).await.unwrap();
    assert!(matches!(
        client.propose_recovery_at(CHAT, 0, None).await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    let saved = client.recovery_overview(CHAT).await.unwrap();
    assert_eq!(saved.proposals[0].proposal_id(), id);
    assert_eq!(
        client
            .propose_recovery_at(CHAT, 0, Some(&id))
            .await
            .unwrap(),
        id
    );
    let before = records(f.root.path(), A);
    assert!(matches!(
        client
            .decide_recovery_at(CHAT, 1, &id, EpochRecoveryDecision::Cancel)
            .await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    assert_eq!(records(f.root.path(), A), before);
    let cancel = client
        .decide_recovery_at(CHAT, 0, &id, EpochRecoveryDecision::Cancel)
        .await
        .unwrap();
    assert_eq!(
        client
            .decide_recovery_at(CHAT, 0, &id, EpochRecoveryDecision::Cancel)
            .await
            .unwrap(),
        cancel
    );
    assert!(client.recovery_overview(CHAT).await.unwrap().sending_frozen);
    owner.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
