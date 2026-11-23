//! Real Signal/outbox +protected journals, scripted authenticated metadata. The
//! actual Go/TLS/PG/native fixture remains a separate test boundary.
use super::*;
use crate::encryption::{epoch_contract as data, file_status, signal_state::EpochFileProgress};
use std::collections::HashMap;
use std::sync::atomic::AtomicI64;
use std::time::Instant;

pub(super) struct FileStatusProbe {
    requests: Mutex<Vec<v2::GetFileStatusesRequest>>,
    errors: Mutex<HashMap<String, RpcError>>,
    ack: AtomicBool,
    offset: AtomicI64,
    lose_once: AtomicBool,
    block: AtomicBool,
    released: AtomicBool,
    started: Notify,
    release: Notify,
}
impl FileStatusProbe {
    fn new() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            errors: Mutex::new(HashMap::new()),
            ack: AtomicBool::new(false),
            offset: AtomicI64::new(100),
            lose_once: AtomicBool::new(false),
            block: AtomicBool::new(false),
            released: AtomicBool::new(false),
            started: Notify::new(),
            release: Notify::new(),
        }
    }
    pub(super) async fn observe(
        &self,
        plans: &[EpochOutgoingPlan],
    ) -> Result<EpochFileStatuses, RpcError> {
        assert_eq!(plans.len(), 1);
        let plan = &plans[0];
        let e = plan.request().envelope.as_ref().unwrap();
        let reference = v2::FileStatusReference {
            binding: Some(data::send_binding(e).unwrap()),
            blob: e.blob.clone(),
        };
        let request = v2::GetFileStatusesRequest {
            context: plan.request().context.clone(),
            files: vec![reference.clone()],
        };
        {
            let mut requests = self.requests.lock().unwrap();
            assert!(requests.len() < 512);
            requests.push(request);
        }
        self.started.notify_one();
        while self.block.load(Ordering::SeqCst) && !self.released.load(Ordering::SeqCst) {
            self.release.notified().await;
        }
        if let Some(e) = self.errors.lock().unwrap().get(&e.message_id).cloned() {
            return Err(e);
        }
        let r = plan.receipt().unwrap();
        let at = r.stored_at_unix_ms + self.offset.load(Ordering::SeqCst);
        let response = v2::FileStatuses {
            files: vec![v2::FileStatus {
                format_version: 1,
                reference: Some(reference),
                receipt: Some(r.clone()),
                observed_at_unix_ms: at,
                expires_at_unix_ms: r.stored_at_unix_ms + file_status::FILE_RETENTION_MS,
                acknowledged_at_unix_ms: self.ack.load(Ordering::SeqCst).then_some(at - 1),
                recipient_replaced_at_unix_ms: None,
            }],
        };
        let client = NativeEpochClient::test_metadata_transport(
            plan.store_scope().clone(),
            e.sender.clone().unwrap(),
        );
        let observed = client.test_file_status_observation(plans, response)?;
        if self.lose_once.swap(false, Ordering::SeqCst) {
            return Err(RpcError::Status(Code::Unavailable));
        }
        Ok(observed)
    }
}
async fn accepted(f: &Fixture, a: &EpochScope) -> EpochOutgoingPlan {
    let (p, _) = super::super::attachment_tests::message(a);
    let p = f.alice.encrypt_epoch(a.clone(), p).await.unwrap();
    let r = test_send_receipt(&p);
    f.alice
        .accept_epoch_statuses(EpochObservation::test_observation(
            p.store_scope().clone(),
            vec![p.clone()],
            v2::MessageStatuses {
                messages: vec![v2::MessageStatus {
                    receipt: Some(r.clone()),
                    state: common::MessageState::RecipientStored as i32,
                    observed_at_unix_ms: r.stored_at_unix_ms + 1,
                }],
            },
        ))
        .await
        .unwrap();
    f.alice.epoch_outgoing_plan(id(&p)).await.unwrap()
}
fn id(p: &EpochOutgoingPlan) -> String {
    p.request().envelope.as_ref().unwrap().message_id.clone()
}
fn probe(status: &Arc<FileStatusProbe>) -> Arc<Probe> {
    Arc::new(Probe {
        file_status: Some(status.clone()),
        send_accept: AtomicBool::new(true),
        ..Default::default()
    })
}
fn launch_status(w: CryptoCoordinator, rpc: &Arc<Probe>, timeout: Duration) -> AccountOwner {
    AccountOwner::spawn(
        w,
        EpochDeliveryRunner::new(
            Fake(rpc.clone()),
            RunnerConfig {
                poll_interval: Duration::from_millis(20),
                retry_initial: Duration::from_millis(100),
                retry_max: Duration::from_millis(500),
                round_timeout: timeout,
            },
        )
        .unwrap(),
        PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap(),
    )
}
async fn progress(
    c: &AccountClient,
    id: &str,
    predicate: impl Fn(&EpochFileProgress) -> bool,
) -> EpochFileProgress {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert_eq!(c.status().phase, AccountPhase::Running);
            match c.attachment_file_progress(id).await {
                Ok(p) if predicate(&p) => return p,
                Ok(_) | Err(AccountError::Initializing) => (),
                Err(e) => panic!("file progress read failed: {e:?}"),
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|e| panic!("file status wait {id}: {e}; {:?}", c.status()))
}

#[tokio::test]
async fn file_status_waits_behind_pending_delivery_after_remote_file_ack() {
    let (f, a, _) = prepared().await;
    let p = accepted(&f, &a).await;
    // The intentionally blocked delivery round can start before the background
    // file status index is seeded. Prepare it before testing RPC ordering.
    assert!(f.alice.seed_epoch_file_status_index().await.unwrap().ready);
    let metadata = Arc::new(FileStatusProbe::new());
    let rpc = Arc::new(Probe {
        file_status: Some(metadata.clone()),
        block_pending: true,
        ..Default::default()
    });
    let owner = launch_status(f.alice, &rpc, Duration::from_secs(5));
    let client = owner.client();
    download_tests::entered(&rpc.pending_started).await;
    let before = metadata.requests.lock().unwrap().len();
    metadata.offset.store(200, Ordering::SeqCst);
    metadata.ack.store(true, Ordering::SeqCst);
    let ack_at = Instant::now();

    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(rpc.active.load(Ordering::SeqCst), 1);
    assert_eq!(metadata.requests.lock().unwrap().len(), before);
    assert!(
        client
            .attachment_file_progress(&id(&p))
            .await
            .unwrap()
            .acknowledged_at_unix_ms()
            .is_none()
    );
    let released_at = Instant::now();
    rpc.pending_released.store(true, Ordering::SeqCst);
    rpc.release.notify_one();

    let saved = progress(&client, &id(&p), |p| !p.needs_poll()).await;
    assert_eq!(
        saved.acknowledged_at_unix_ms(),
        Some(p.receipt().unwrap().stored_at_unix_ms + 199)
    );
    eprintln!(
        "File status after remote FILEACK: {:?}; after delivery release: {:?}",
        ack_at.elapsed(),
        released_at.elapsed()
    );
    owner.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_owner_after_descriptor_ack_retries_lost_read_and_stays_terminal_after_reopen()
{
    let (f, a, _) = prepared().await;
    let p = accepted(&f, &a).await;
    assert!(!p.needs_status_poll());
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let metadata = Arc::new(FileStatusProbe::new());
    metadata.ack.store(true, Ordering::SeqCst);
    metadata.lose_once.store(true, Ordering::SeqCst);
    let rpc = probe(&metadata);
    let owner = launch_status(f.alice, &rpc, Duration::from_secs(2));
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    let saved = progress(&c, &id(&p), |p| !p.needs_poll()).await;
    assert_eq!(saved.receipt(), p.receipt().unwrap());
    assert_eq!(
        saved.acknowledged_at_unix_ms(),
        Some(p.receipt().unwrap().stored_at_unix_ms + 99)
    );
    owner.shutdown().await.unwrap();
    assert!(rpc.send_requests.lock().unwrap().is_empty());
    {
        let requests = metadata.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0], requests[1]);
    }
    assert!(matches!(
        c.attachment_file_progress(&id(&p)).await,
        Err(AccountError::Stopped)
    ));
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_outgoing_plan(id(&p)).await.unwrap().request(),
        p.request()
    );
    assert!(
        w.pending_epoch_file_statuses(None)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    let again = Arc::new(FileStatusProbe::new());
    let rpc = probe(&again);
    let owner = launch_status(w, &rpc, Duration::from_secs(2));
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    progress(&c, &id(&p), |p| !p.needs_poll()).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while c.status().file_status_steps < 3 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    owner.shutdown().await.unwrap();
    assert!(again.requests.lock().unwrap().is_empty());
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_owner_keeps_expiry_then_observes_late_ack_without_new_send() {
    let (f, a, _) = prepared().await;
    let p = accepted(&f, &a).await;
    // Metadata must work even while a body/cache reader owns file memory.
    let held = f.alice.attachment_permit().unwrap();
    let metadata = Arc::new(FileStatusProbe::new());
    metadata
        .offset
        .store(file_status::FILE_RETENTION_MS + 50, Ordering::SeqCst);
    let rpc = probe(&metadata);
    let owner = launch_status(f.alice, &rpc, Duration::from_secs(2));
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    let expired = progress(&c, &id(&p), |p| p.deadline_observed()).await;
    assert!(expired.needs_poll());
    tokio::time::timeout(Duration::from_secs(5), async {
        while c.status().file_status_reads < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    metadata.ack.store(true, Ordering::SeqCst);
    let saved = progress(&c, &id(&p), |p| !p.needs_poll()).await;
    assert!(saved.deadline_observed());
    assert!(saved.acknowledged_at_unix_ms().unwrap() > saved.expires_at_unix_ms());
    assert_eq!(saved.receipt(), p.receipt().unwrap());
    drop(held);
    owner.shutdown().await.unwrap();
    assert!(rpc.send_requests.lock().unwrap().is_empty());
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_owner_blocked_rpc_keeps_local_reads_text_and_stop_leaves_pending() {
    let (f, a, _) = prepared().await;
    let p = accepted(&f, &a).await;
    let metadata = Arc::new(FileStatusProbe::new());
    metadata.block.store(true, Ordering::SeqCst);
    let rpc = probe(&metadata);
    let owner = launch_status(f.alice, &rpc, Duration::from_secs(30));
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    download_tests::entered(&metadata.started).await;
    assert_eq!(rpc.active.load(Ordering::SeqCst), 1);
    assert!(
        c.attachment_file_progress(&id(&p))
            .await
            .unwrap()
            .observed_at_unix_ms()
            .is_none()
    );
    tokio::time::timeout(
        Duration::from_secs(5),
        c.send_text(
            CHAT,
            &uuid::Uuid::new_v4().to_string(),
            "local while status RPC blocks",
        ),
    )
    .await
    .unwrap()
    .unwrap();
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    metadata.released.store(true, Ordering::SeqCst);
    metadata.release.notify_one();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    assert!(
        w.epoch_file_progress(id(&p))
            .await
            .unwrap()
            .observed_at_unix_ms()
            .is_none()
    );
    assert_eq!(
        w.epoch_outgoing_plan(id(&p)).await.unwrap().request(),
        p.request()
    );
    assert_eq!(
        w.pending_epoch_file_statuses(None)
            .await
            .unwrap()
            .files
            .len(),
        1
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_owner_auth_or_generation_failure_preserves_ciphertext_and_pending() {
    for error in [
        RpcError::Status(Code::Unauthenticated),
        RpcError::Status(Code::PermissionDenied),
        RpcError::Failure {
            code: Code::FailedPrecondition,
            reason: common::FailureReason::GenerationChanged,
        },
    ] {
        let (f, a, _) = prepared().await;
        let p = accepted(&f, &a).await;
        let metadata = Arc::new(FileStatusProbe::new());
        metadata.errors.lock().unwrap().insert(id(&p), error);
        let owner = launch_status(f.alice, &probe(&metadata), Duration::from_secs(2));
        let c = owner.client();
        phase(&c, AccountPhase::Failed).await;
        assert!(owner.shutdown().await.is_err());
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
            .await
            .unwrap();
        assert_eq!(
            w.epoch_outgoing_plan(id(&p)).await.unwrap().request(),
            p.request()
        );
        assert!(w.epoch_file_progress(id(&p)).await.unwrap().needs_poll());
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn file_status_owner_timeout_retries_exact_reference_without_changing_outbox() {
    let (f, a, _) = prepared().await;
    let p = accepted(&f, &a).await;
    let metadata = Arc::new(FileStatusProbe::new());
    metadata.block.store(true, Ordering::SeqCst);
    metadata.ack.store(true, Ordering::SeqCst);
    let rpc = probe(&metadata);
    let owner = launch_status(f.alice, &rpc, Duration::from_secs(1));
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while c.status().file_status_health != Some(FileStatusHealth::TimedOut) {
            assert_eq!(c.status().phase, AccountPhase::Running);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    assert!(
        c.attachment_file_progress(&id(&p))
            .await
            .unwrap()
            .observed_at_unix_ms()
            .is_none()
    );
    metadata.released.store(true, Ordering::SeqCst);
    metadata.release.notify_one();
    progress(&c, &id(&p), |p| !p.needs_poll()).await;
    owner.shutdown().await.unwrap();
    {
        let requests = metadata.requests.lock().unwrap();
        assert!(requests.len() >= 2);
        assert!(requests.iter().all(|r| r == &requests[0]));
    }
    assert!(rpc.send_requests.lock().unwrap().is_empty());
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_outgoing_plan(id(&p)).await.unwrap().request(),
        p.request()
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_owner_pages_and_per_file_errors_preserve_fairness_and_delivery() {
    let (f, a, _) = prepared().await;
    let mut plans = Vec::new();
    for _ in 0..33 {
        plans.push(accepted(&f, &a).await);
    }
    plans.sort_by_key(id);
    let metadata = Arc::new(FileStatusProbe::new());
    metadata.ack.store(true, Ordering::SeqCst);
    let errors: Vec<_> = [1, 3, 5, 7].into_iter().map(|i| id(&plans[i])).collect();
    for (id, error) in errors.iter().zip([
        RpcError::Status(Code::NotFound),
        RpcError::Status(Code::ResourceExhausted),
        RpcError::Status(Code::FailedPrecondition),
        RpcError::LocalDeadline,
    ]) {
        metadata.errors.lock().unwrap().insert(id.clone(), error);
    }
    let rpc = probe(&metadata);
    let owner = launch_status(f.alice, &rpc, Duration::from_secs(2));
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    for p in &plans {
        if !errors.contains(&id(p)) {
            progress(&c, &id(p), |p| !p.needs_poll()).await;
        }
    }
    for id in &errors {
        let p = c.attachment_file_progress(id).await.unwrap();
        assert!(p.needs_poll() && p.observed_at_unix_ms().is_none());
    }
    assert!(c.status().delivery_rounds > 0);
    assert!(c.status().maintenance_steps > 0);
    let text_id = uuid::Uuid::new_v4().to_string();
    c.send_text(CHAT, &text_id, "delivery despite file status errors")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while !c.outgoing(&text_id).await.unwrap().stored_on_server {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    metadata.errors.lock().unwrap().clear();
    for id in &errors {
        progress(&c, id, |p| !p.needs_poll()).await;
    }
    owner.shutdown().await.unwrap();
    {
        let requests = metadata.requests.lock().unwrap();
        for p in &plans {
            let exact: Vec<_> = requests
                .iter()
                .filter(|r| r.files[0].binding.as_ref().unwrap().message_id == id(p))
                .collect();
            assert!(!exact.is_empty());
            assert!(exact.iter().all(|r| *r == exact[0]));
        }
    }
    assert!(
        rpc.send_requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r.envelope.as_ref().unwrap().message_id == text_id)
    );
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_status_owner_failed_commit_halts_without_losing_pending_then_retries_on_restart() {
    let (f, a, _) = prepared().await;
    let p = accepted(&f, &a).await;
    let path = p.store_scope().database_path(f.root.path());
    let raw = rusqlite::Connection::open(path).unwrap();
    raw.execute_batch("CREATE TABLE owner_fp_parent(id INTEGER PRIMARY KEY); CREATE TABLE owner_fp_child(id INTEGER REFERENCES owner_fp_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER owner_fp_fault AFTER UPDATE ON protected_records WHEN NEW.record_id LIKE 'file-progress-anchor:%' BEGIN INSERT INTO owner_fp_child VALUES(1); END;").unwrap();
    let metadata = Arc::new(FileStatusProbe::new());
    metadata.ack.store(true, Ordering::SeqCst);
    let owner = launch_status(f.alice, &probe(&metadata), Duration::from_secs(2));
    let c = owner.client();
    phase(&c, AccountPhase::Failed).await;
    assert!(owner.shutdown().await.is_err());
    raw.execute_batch("DROP TRIGGER owner_fp_fault").unwrap();
    drop(raw);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    let before = w.epoch_file_progress(id(&p)).await.unwrap();
    assert!(before.needs_poll() && before.observed_at_unix_ms().is_none());
    assert_eq!(
        w.epoch_outgoing_plan(id(&p)).await.unwrap().request(),
        p.request()
    );
    let owner = launch_status(w, &probe(&metadata), Duration::from_secs(2));
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    progress(&c, &id(&p), |p| !p.needs_poll()).await;
    owner.shutdown().await.unwrap();
    assert_eq!(metadata.requests.lock().unwrap().len(), 2);
    f.bob.shutdown().await.unwrap();
}
