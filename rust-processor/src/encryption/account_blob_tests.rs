//! Real account task +protected stores; gated HTTP body and serial RPC probe.
use super::*;
use crate::encryption::{
    account_owner::blobs::Body,
    attachment_payload::AttachmentDraft,
    native::BlobHttpError,
    protected::StoreScope,
    signal_state::{EpochAttachmentPreparation, EpochBlobControlPlan, EpochBlobUpload},
};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

#[path = "account_blob_promotion_tests.rs"]
mod promotion;

#[path = "account_blob_retry_tests.rs"]
mod retained;

#[cfg(feature = "compression")]
#[path = "account_photo_tests.rs"]
mod photo_input_tests;

const SERVER: &str = "https://chat.example.test";
#[derive(Default)]
struct BodyProbe {
    started: Notify,
    release: Notify,
    block: AtomicBool,
    released: AtomicBool,
    join_started: Notify,
    join_release: Notify,
    hold_join: AtomicBool,
    join_released: AtomicBool,
    cancelled: AtomicBool,
    owns_source: Arc<AtomicBool>,
    active: AtomicUsize,
    ids: Mutex<Vec<String>>,
    digests: Mutex<Vec<[u8; 32]>>,
    ciphertexts: Mutex<Vec<Vec<u8>>>,
    lose_reply_once: AtomicBool,
}
struct BodyFlight(Arc<BodyProbe>);
impl Drop for BodyFlight {
    fn drop(&mut self) {
        assert_eq!(self.0.active.fetch_sub(1, Ordering::SeqCst), 1);
    }
}
struct Http {
    probe: Arc<BodyProbe>,
    rpc: Arc<Probe>,
    source: Option<EpochBlobUpload>,
}
#[async_trait::async_trait]
impl crate::encryption::blob_download::EpochBlobDownloadTransport for Http {
    async fn get(
        &mut self,
        _: crate::encryption::native::BlobReadRequest,
        _: &mut dyn crate::encryption::native::BlobCiphertextSink,
    ) -> Result<crate::encryption::native::BlobGetResponse, crate::encryption::native::BlobReadError>
    {
        panic!("PUT-only test body received unexpected GET")
    }
}
#[async_trait::async_trait]
impl Body for Http {
    async fn put(&mut self, source: EpochBlobUpload) -> Result<u16, BlobHttpError> {
        assert!(!self.probe.cancelled.load(Ordering::SeqCst));
        assert_eq!(self.probe.active.fetch_add(1, Ordering::SeqCst), 0);
        let _flight = BodyFlight(self.probe.clone());
        let id = source.plan().request().message_id.clone();
        self.rpc
            .blob_events
            .lock()
            .unwrap()
            .push(("put", id.clone()));
        self.probe.ids.lock().unwrap().push(id.clone());
        self.probe
            .digests
            .lock()
            .unwrap()
            .push(Sha256::digest(source.bytes()).into());
        self.probe
            .ciphertexts
            .lock()
            .unwrap()
            .push(source.bytes().to_vec());
        assert!(self.source.replace(source).is_none());
        self.probe.owns_source.store(true, Ordering::SeqCst);
        self.probe.started.notify_one();
        while self.probe.block.load(Ordering::SeqCst) && !self.probe.released.load(Ordering::SeqCst)
        {
            self.probe.release.notified().await;
        }
        self.rpc.blob_ready.lock().unwrap().insert(id);
        if self.probe.lose_reply_once.swap(false, Ordering::SeqCst) {
            return Err(BlobHttpError::Transport);
        }
        Ok(201)
    }
    async fn join(&mut self) {
        if self.source.is_some() {
            self.probe.join_started.notify_one();
            while self.probe.hold_join.load(Ordering::SeqCst)
                && !self.probe.join_released.load(Ordering::SeqCst)
            {
                self.probe.join_release.notified().await;
            }
        }
        drop(self.source.take());
        self.probe.owns_source.store(false, Ordering::SeqCst);
    }
    fn cancel(&mut self) {
        self.probe.cancelled.store(true, Ordering::SeqCst);
    }
}
pub(super) async fn stage(worker: &CryptoCoordinator, id: &str) -> EpochBlobControlPlan {
    loop {
        if worker.seed_recovery_indexes().await.unwrap().ready {
            break;
        }
    }
    let draft = AttachmentDraft {
        kind: v2::AttachmentKind::File,
        filename: Zeroizing::new("native-lifetime-private-file.bin".into()),
        mime_type: Zeroizing::new("application/octet-stream".into()),
        caption: Zeroizing::new(String::new()),
        photo_metadata_stripped: None,
    };
    assert!(matches!(
        worker
            .prepare_epoch_attachment(
                CHAT.into(),
                id.into(),
                draft,
                Zeroizing::new(b"private-account-upload-marker".repeat(1024))
            )
            .await
            .unwrap(),
        EpochAttachmentPreparation::Prepared(_)
    ));
    worker.epoch_attachment_control(id.into()).await.unwrap()
}
fn launch_upload(
    worker: CryptoCoordinator,
    rpc: &Arc<Probe>,
    body: &Arc<BodyProbe>,
) -> AccountOwner {
    *rpc.body_owned.lock().unwrap() = Some(body.owns_source.clone());
    AccountOwner::spawn_with_blobs(
        worker,
        EpochDeliveryRunner::new(
            Fake(rpc.clone()),
            RunnerConfig {
                poll_interval: Duration::from_millis(20),
                retry_initial: Duration::from_millis(50),
                retry_max: Duration::from_millis(200),
                round_timeout: Duration::from_secs(5),
            },
        )
        .unwrap(),
        PreKeyMaintenance::new(MaintenanceConfig {
            retry_initial: Duration::from_millis(100),
            retry_max: Duration::from_millis(500),
            ..Default::default()
        })
        .unwrap(),
        Some(Http {
            probe: body.clone(),
            rpc: rpc.clone(),
            source: None,
        }),
    )
}
async fn health(client: &AccountClient, expected: BlobHealth) {
    let mut updates = client.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let s = client.status();
            if s.blob_health == Some(expected) {
                break;
            }
            assert!(
                !matches!(s.phase, AccountPhase::Stopped | AccountPhase::Failed),
                "unexpected account status: {s:?}"
            );
            assert!(updates.changed().await.is_some());
        }
    })
    .await
    .unwrap();
}
async fn entered(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(10), notify.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn durable_preparation_is_automatically_uploaded_and_reopened_without_signal_promotion() {
    let f = fixture().await;
    let scope = initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    let plan = stage(&f.alice, &id).await;
    assert_eq!(
        f.alice.epoch_attachment_candidates().await.unwrap(),
        vec![id.clone()]
    );
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    health(&client, BlobHealth::Ready).await;
    owner.shutdown().await.unwrap();
    assert!(body.cancelled.load(Ordering::SeqCst) && !body.owns_source.load(Ordering::SeqCst));
    assert_eq!(
        body.ids.lock().unwrap().as_slice(),
        std::slice::from_ref(&id)
    );
    assert_eq!(
        body.digests.lock().unwrap()[0].as_slice(),
        plan.request().blob.as_ref().unwrap().sha256
    );
    assert!(
        rpc.blob_requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r == plan.request())
    );
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    let worker =
        CryptoCoordinator::start(store(f.root.path(), A, false), scope.own_device().clone())
            .await
            .unwrap();
    assert_eq!(
        worker.epoch_attachment_control(id).await.unwrap().request(),
        plan.request()
    );
    assert!(
        worker
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    worker.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn blocked_http_does_not_starve_local_foreground_serial_rpcs_or_background_delivery() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(BodyProbe {
        block: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.started).await;
    assert!(body.owns_source.load(Ordering::SeqCst));
    let before = client.status();
    assert!(client.conversation(CHAT).await.unwrap().is_some());
    // Fake Lookup fails, but reaching it proves HTTP does not own the RPC lane.
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), client.get_dialog(CHAT))
            .await
            .unwrap(),
        Err(AccountError::Rpc(RpcError::Transport))
    ));
    let mut updates = client.subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.status().delivery_rounds <= before.delivery_rounds
            || client.status().maintenance_steps <= before.maintenance_steps
        {
            assert!(updates.changed().await.is_some());
        }
    })
    .await
    .unwrap();
    assert_eq!(body.active.load(Ordering::SeqCst), 1);
    owner.shutdown().await.unwrap();
    assert!(body.cancelled.load(Ordering::SeqCst));
    assert_eq!(body.active.load(Ordering::SeqCst), 0);
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    assert!(rpc.calls.lock().unwrap().contains(&"lookup"));
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_shutdown_waiter_keeps_store_locked_until_last_body_owner_joins() {
    let f = fixture().await;
    let scope = initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    let plan = stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(BodyProbe {
        block: AtomicBool::new(true),
        hold_join: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.started).await;
    let mut shutdown = Box::pin(owner.shutdown());
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(shutdown);
    entered(&body.join_started).await;
    assert!(body.cancelled.load(Ordering::SeqCst) && body.owns_source.load(Ordering::SeqCst));
    assert_eq!(client.status().phase, AccountPhase::Running);
    let path = StoreScope::new(SERVER, A)
        .unwrap()
        .database_path(f.root.path());
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.parent().unwrap().join("owner.lock"))
        .unwrap();
    assert!(
        lock.try_lock().is_err(),
        "account unlocked before body join"
    );
    drop(lock);
    body.join_released.store(true, Ordering::SeqCst);
    body.join_release.notify_one();
    phase(&client, AccountPhase::Stopped).await;
    assert!(!body.owns_source.load(Ordering::SeqCst));
    assert!(matches!(
        client.conversation(CHAT).await,
        Err(AccountError::Stopped)
    ));
    let worker =
        CryptoCoordinator::start(store(f.root.path(), A, false), scope.own_device().clone())
            .await
            .unwrap();
    assert_eq!(
        worker.epoch_attachment_control(id).await.unwrap().request(),
        plan.request()
    );
    worker.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_failed_attachment_does_not_starve_another_and_retries_keep_saved_operations() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let bad = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    let good = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    let failed = stage(&f.alice, bad).await;
    let succeeded = stage(&f.alice, good).await;
    let rpc = Arc::new(Probe::default());
    rpc.blob_errors
        .lock()
        .unwrap()
        .insert(bad.into(), RpcError::Transport);
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.started).await;
    health(&client, BlobHealth::Ready).await;
    owner.shutdown().await.unwrap();
    assert_eq!(body.ids.lock().unwrap().as_slice(), &[good.to_string()]);
    {
        let requests = rpc.blob_requests.lock().unwrap();
        assert!(requests.iter().any(|r| r == failed.request()));
        assert!(
            requests
                .iter()
                .filter(|r| r.message_id == bad)
                .all(|r| r == failed.request())
        );
        assert!(
            requests
                .iter()
                .filter(|r| r.message_id == good)
                .all(|r| r == succeeded.request())
        );
        assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    }
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_one_account_owner_cancels_its_http_without_stopping_another_account() {
    let f = fixture().await;
    let scope = initialize(&f.alice).await;
    initialize(&f.bob).await;
    let id = uuid::Uuid::new_v4().to_string();
    let plan = stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(BodyProbe {
        block: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let stale = owner.client();
    let other = launch(f.bob, &Arc::new(Probe::default()));
    let live = other.client();
    phase(&live, AccountPhase::Running).await;
    entered(&body.started).await;
    drop(owner);
    phase(&stale, AccountPhase::Stopped).await;
    assert!(body.cancelled.load(Ordering::SeqCst) && !body.owns_source.load(Ordering::SeqCst));
    assert!(matches!(
        stale.conversation(CHAT).await,
        Err(AccountError::Stopped)
    ));
    assert!(live.conversation(CHAT).await.unwrap().is_some());
    let worker =
        CryptoCoordinator::start(store(f.root.path(), A, false), scope.own_device().clone())
            .await
            .unwrap();
    assert_eq!(
        worker.epoch_attachment_control(id).await.unwrap().request(),
        plan.request()
    );
    worker.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_observes_ready_file_without_reupload_or_new_operation() {
    let f = fixture().await;
    let scope = initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    let plan = stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    health(&owner.client(), BlobHealth::Ready).await;
    owner.shutdown().await.unwrap();
    let worker =
        CryptoCoordinator::start(store(f.root.path(), A, false), scope.own_device().clone())
            .await
            .unwrap();
    let second = Arc::new(BodyProbe::default());
    let owner = launch_upload(worker, &rpc, &second);
    health(&owner.client(), BlobHealth::Ready).await;
    owner.shutdown().await.unwrap();
    assert_eq!(body.ids.lock().unwrap().len(), 1);
    assert!(second.ids.lock().unwrap().is_empty());
    assert!(
        rpc.blob_requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r == plan.request())
    );
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn revoked_session_stops_active_http_and_releases_store_without_resetting_intent() {
    let f = fixture().await;
    let scope = initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    let plan = stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(BodyProbe {
        block: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.started).await;
    rpc.revoked.store(true, Ordering::SeqCst);
    phase(&client, AccountPhase::Failed).await;
    assert!(owner.shutdown().await.is_err());
    assert!(body.cancelled.load(Ordering::SeqCst) && !body.owns_source.load(Ordering::SeqCst));
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    assert_eq!(body.active.load(Ordering::SeqCst), 0);
    assert_eq!(rpc.blob_requests.lock().unwrap().len(), 2); // Observe +Reserve only.
    let worker =
        CryptoCoordinator::start(store(f.root.path(), A, false), scope.own_device().clone())
            .await
            .unwrap();
    assert_eq!(
        worker.epoch_attachment_control(id).await.unwrap().request(),
        plan.request()
    );
    worker.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
