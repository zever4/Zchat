//! Real provisioned account/Signal/SQLite. Scripted body gates prove queue and
//! cleanup ownership; the separate Native HTTP case exercises real loopback IO.
use super::*;
use crate::encryption::{
    account_owner::blobs::Body,
    attachment::{self, AttachmentBinding},
    attachment_payload::{AttachmentDraft, PAYLOAD_FORMAT},
    blob_download::EpochBlobDownloadTransport,
    epoch_contract as data,
    protected::{StoreScope, test_open},
    signal_state::{EpochAttachmentFile, EpochBlobUpload},
};
use std::collections::HashMap;
use std::sync::atomic::AtomicU16;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroize::Zeroizing;
const ORIGIN: &str = "https://chat.example.test";
const CONTENT: &[u8] = b"account-owned private attachment bytes";

fn message(s: &EpochScope, bytes: &[u8]) -> (v2::EncryptedPayload, Vec<u8>) {
    let mut p = super::super::incoming::tests::payload(s, "");
    let blob = uuid::Uuid::new_v4().to_string();
    let binding = AttachmentBinding::new(
        CHAT,
        s.crypto_epoch(),
        &p.message_id,
        &blob,
        s.own_device(),
        s.peer_device(),
    )
    .unwrap();
    let prepared = attachment::seal(&binding, Zeroizing::new(bytes.to_vec())).unwrap();
    p.format_version = PAYLOAD_FORMAT;
    p.content = Some(v2::encrypted_payload::Content::Attachment(
        AttachmentDraft {
            kind: v2::AttachmentKind::File,
            filename: Zeroizing::new("account-private.bin".into()),
            mime_type: Zeroizing::new("application/octet-stream".into()),
            caption: Zeroizing::new("private caption".into()),
            photo_metadata_stripped: None,
        }
        .into_message(blob, prepared.descriptor()),
    ));
    (p, prepared.blob().to_vec())
}
pub(super) async fn receive(f: &Fixture, a: &EpochScope, b: &EpochScope) -> (String, Vec<u8>) {
    receive_between(&f.alice, &f.bob, a, b, CONTENT, 1).await
}
pub(super) async fn receive_between(
    sender: &CryptoCoordinator,
    receiver: &CryptoCoordinator,
    a: &EpochScope,
    b: &EpochScope,
    bytes: &[u8],
    sequence: i64,
) -> (String, Vec<u8>) {
    let (p, blob) = message(a, bytes);
    let id = p.message_id.clone();
    let plan = sender.encrypt_epoch(a.clone(), p).await.unwrap();
    let envelope = plan.request().envelope.clone().unwrap();
    let delivery = v2::Delivery {
        sequence,
        envelope_digest: data::envelope_digest(&envelope).unwrap().to_vec(),
        envelope: Some(envelope),
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
    };
    receiver
        .receive_epoch(
            b.clone(),
            super::super::incoming::tests::delivery(b, &delivery),
        )
        .await
        .unwrap();
    (id, blob)
}
#[derive(Default)]
pub(super) struct ReadProbe {
    data: Mutex<HashMap<String, Vec<u8>>>,
    pub(super) seen: Mutex<Vec<BlobReadRequest>>,
    events: Mutex<Vec<&'static str>>,
    active: AtomicUsize,
    block: AtomicBool,
    started: Notify,
    release: Notify,
    released: AtomicBool,
    error: AtomicU16,
    owns: AtomicBool,
    cancelled: AtomicBool,
    hold_join: AtomicBool,
    join_started: Notify,
    join_release: Notify,
    join_released: AtomicBool,
    block_put: AtomicBool,
    put_started: Notify,
    put_release: Notify,
    put_released: AtomicBool,
}
impl ReadProbe {
    pub(super) fn file(&self, id: &str, bytes: Vec<u8>) {
        self.data.lock().unwrap().insert(id.into(), bytes);
    }
}
struct ReadFlight(Arc<ReadProbe>);
impl Drop for ReadFlight {
    fn drop(&mut self) {
        assert_eq!(self.0.active.fetch_sub(1, Ordering::SeqCst), 1);
    }
}
struct Http {
    state: Arc<ReadProbe>,
    rpc: Arc<Probe>,
    upload: Option<EpochBlobUpload>,
}
#[async_trait::async_trait]
impl EpochBlobDownloadTransport for Http {
    async fn get(
        &mut self,
        request: BlobReadRequest,
        sink: &mut dyn BlobCiphertextSink,
    ) -> Result<BlobGetResponse, BlobReadError> {
        assert!(!self.state.cancelled.load(Ordering::SeqCst));
        assert_eq!(self.state.active.fetch_add(1, Ordering::SeqCst), 0);
        let _flight = ReadFlight(self.state.clone());
        self.state.owns.store(true, Ordering::SeqCst);
        self.state.events.lock().unwrap().push("get");
        self.state.seen.lock().unwrap().push(request.clone());
        self.state.started.notify_one();
        while self.state.block.load(Ordering::SeqCst) && !self.state.released.load(Ordering::SeqCst)
        {
            self.state.release.notified().await;
        }
        let error = self.state.error.load(Ordering::SeqCst);
        if error != 0 {
            return Err(BlobReadError::Rejected(error));
        }
        let data = self
            .state
            .data
            .lock()
            .unwrap()
            .get(&request.binding().message_id)
            .unwrap()
            .clone();
        for chunk in data.chunks(17) {
            sink.write(BlobCiphertextChunk::test_chunk(chunk))
                .await
                .map_err(|_| BlobReadError::Sink)?;
        }
        Ok(BlobGetResponse::test_complete(request))
    }
}
#[async_trait::async_trait]
impl Body for Http {
    async fn put(&mut self, source: EpochBlobUpload) -> Result<u16, BlobHttpError> {
        assert_eq!(self.state.active.fetch_add(1, Ordering::SeqCst), 0);
        let _flight = ReadFlight(self.state.clone());
        self.state.events.lock().unwrap().push("put");
        let id = source.plan().request().message_id.clone();
        self.upload = Some(source);
        self.state.owns.store(true, Ordering::SeqCst);
        self.state.put_started.notify_one();
        while self.state.block_put.load(Ordering::SeqCst)
            && !self.state.put_released.load(Ordering::SeqCst)
        {
            self.state.put_release.notified().await;
        }
        self.rpc.blob_ready.lock().unwrap().insert(id);
        Ok(201)
    }
    async fn join(&mut self) {
        if self.state.owns.load(Ordering::SeqCst) {
            self.state.join_started.notify_one();
            while self.state.hold_join.load(Ordering::SeqCst)
                && !self.state.join_released.load(Ordering::SeqCst)
            {
                self.state.join_release.notified().await;
            }
        }
        drop(self.upload.take());
        self.state.owns.store(false, Ordering::SeqCst);
    }
    fn cancel(&mut self) {
        self.state.cancelled.store(true, Ordering::SeqCst);
    }
}
fn launch_body<B: Body + 'static>(
    worker: CryptoCoordinator,
    rpc: &Arc<Probe>,
    body: B,
) -> AccountOwner {
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
        Some(body),
    )
}
pub(super) fn launch_read(
    worker: CryptoCoordinator,
    rpc: &Arc<Probe>,
    state: &Arc<ReadProbe>,
) -> AccountOwner {
    launch_body(
        worker,
        rpc,
        Http {
            state: state.clone(),
            rpc: rpc.clone(),
            upload: None,
        },
    )
}
pub(super) async fn entered(n: &Notify) {
    tokio::time::timeout(Duration::from_secs(10), n.notified())
        .await
        .unwrap();
}
async fn bytes(c: &AccountClient, id: &str) -> EpochAttachmentFile {
    // Automatic proof verification shares the existing whole-file permit.
    // Busy is bounded admission, not missing/corrupt cache or a reason for GET.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match c.open_cached_attachment(id).await {
                Ok(file) => return file,
                Err(AccountError::Crypto(CryptoError::Busy)) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(e) => panic!("cached file read failed: {e:?}"),
            }
        }
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn explicit_download_reopens_verified_cache_without_second_get_or_implicit_descriptor_ack() {
    use crate::encryption::signal_state::{AttachmentCacheState, ViewPage, ViewRequest};

    async fn cache_state(client: &AccountClient, id: &str) -> Option<AttachmentCacheState> {
        let ViewPage::History(items, _) = client
            .read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .unwrap()
        else {
            panic!("expected local history");
        };
        items
            .into_iter()
            .find(|item| item.message_id == id)
            .unwrap()
            .cache_state
    }
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(ReadProbe::default());
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    assert!(matches!(
        client.open_cached_attachment(&id).await,
        Err(AccountError::Crypto(CryptoError::MessageUnavailable))
    ));
    assert_eq!(
        cache_state(&client, &id).await,
        Some(AttachmentCacheState::Missing)
    );
    assert!(matches!(
        client
            .download_attachment_in(&uuid::Uuid::new_v4().to_string(), &id)
            .await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    assert!(body.seen.lock().unwrap().is_empty());
    client.download_attachment_in(CHAT, &id).await.unwrap();
    // Settle automatic file-ACK work before attempting another file-buffer lease.
    super::file_ack_tests::saved(&client, &id).await;
    assert!(matches!(
        client
            .open_current_attachment(&uuid::Uuid::new_v4().to_string(), &id)
            .await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    assert_eq!(
        client
            .open_current_attachment(CHAT, &id)
            .await
            .unwrap()
            .as_bytes(),
        CONTENT
    );
    assert_eq!(
        cache_state(&client, &id).await,
        Some(AttachmentCacheState::Saved)
    );
    assert_eq!(bytes(&client, &id).await.as_bytes(), CONTENT);
    client.download_attachment(&id).await.unwrap();
    assert_eq!(body.seen.lock().unwrap().len(), 1);
    assert!(rpc.blob_requests.lock().unwrap().is_empty());
    owner.shutdown().await.unwrap();
    let worker = CryptoCoordinator::start(store(f.root.path(), B, false), b.own_device().clone())
        .await
        .unwrap();
    assert!(!worker.epoch_ack(id.clone()).await.unwrap().acknowledged());
    let rpc = Arc::new(Probe::default());
    let offline = launch(worker, &rpc);
    let client = offline.client();
    phase(&client, AccountPhase::Running).await;
    assert_eq!(bytes(&client, &id).await.as_bytes(), CONTENT);
    assert_eq!(
        cache_state(&client, &id).await,
        Some(AttachmentCacheState::Saved)
    );
    assert!(matches!(
        client.download_attachment(&id).await,
        Err(AccountError::AttachmentUnavailable)
    ));
    offline.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_plaintext_reply_is_discarded_if_caller_resumes_after_account_stop() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(ReadProbe::default());
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    client.download_attachment(&id).await.unwrap();
    // Ensure the delayed response below really contains plaintext, rather
    // than racing a transient file-memory Busy from background verification.
    super::file_ack_tests::saved(&client, &id).await;
    let mut delayed = Box::pin(client.open_cached_attachment(&id));
    assert!(
        delayed
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    // Same local command lane: this reply proves the earlier plaintext has
    // already been queued, without polling its waiting caller again.
    client.conversation(CHAT).await.unwrap();
    owner.shutdown().await.unwrap();
    assert!(matches!(delayed.await, Err(AccountError::Stopped)));
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn blocked_get_keeps_local_text_serial_rpc_and_delivery_maintenance_running() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(ReadProbe {
        block: AtomicBool::new(true),
        ..Default::default()
    });
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    let mut download = Box::pin(client.download_attachment(&id));
    tokio::select! {_=entered(&body.started)=>{},r=&mut download=>panic!("GET must block: {r:?}")};
    let before = client.status();
    let id2 = uuid::Uuid::new_v4().to_string();
    tokio::time::timeout(
        Duration::from_secs(5),
        client.send_text(CHAT, &id2, "text while file waits"),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(client.conversation(CHAT).await.unwrap().is_some());
    assert!(matches!(
        client.get_dialog(CHAT).await,
        Err(AccountError::Rpc(RpcError::Transport))
    ));
    let mut changes = client.subscribe();
    tokio::time::timeout(Duration::from_secs(5), async {
        while client.status().delivery_rounds <= before.delivery_rounds
            || client.status().maintenance_steps <= before.maintenance_steps
        {
            changes.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(body.active.load(Ordering::SeqCst), 1);
    assert!(rpc.active.load(Ordering::SeqCst) <= 1);
    body.released.store(true, Ordering::SeqCst);
    body.release.notify_one();
    download.await.unwrap();
    owner.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_queue_shares_command_budget_and_cancelled_queued_downloads_never_start() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let (cold, blob2) =
        receive_between(&f.alice, &f.bob, &a, &b, b"must remain undownloaded", 2).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(ReadProbe {
        block: AtomicBool::new(true),
        ..Default::default()
    });
    body.file(&id, blob);
    body.file(&cold, blob2);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    let mut first = Box::pin(client.download_attachment(&id));
    tokio::select! {_=entered(&body.started)=>{},_=&mut first=>panic!("expected blocked GET")};
    let mut queued = Vec::new();
    for _ in 1..COMMAND_CAPACITY {
        let mut future = Box::pin(client.download_attachment(&cold));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        queued.push(future);
    }
    assert!(matches!(
        client.download_attachment(&id).await,
        Err(AccountError::Busy)
    ));
    assert!(matches!(
        client.conversation(CHAT).await,
        Err(AccountError::Busy)
    ));
    drop(queued);
    body.released.store(true, Ordering::SeqCst);
    body.release.notify_one();
    first.await.unwrap();
    // A queued different/foreign UUID never reaches GET before protected lookup.
    assert!(matches!(
        client
            .download_attachment(&uuid::Uuid::new_v4().to_string())
            .await,
        Err(AccountError::Crypto(CryptoError::MessageUnavailable))
    ));
    assert_eq!(body.seen.lock().unwrap().len(), 1);
    assert!(matches!(
        client.open_cached_attachment(&cold).await,
        Err(AccountError::Crypto(CryptoError::MessageUnavailable))
    ));
    owner.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_download_and_shutdown_waiters_keep_store_owned_and_fence_old_client() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(ReadProbe {
        block: AtomicBool::new(true),
        hold_join: AtomicBool::new(true),
        ..Default::default()
    });
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    let mut request = Box::pin(client.download_attachment(&id));
    tokio::select! {_=entered(&body.started)=>{},_=&mut request=>panic!("GET must block")};
    drop(request);
    entered(&body.join_started).await;
    // The HTTP future is waiting on retained work; independent local lane works.
    assert!(client.conversation(CHAT).await.unwrap().is_some());
    let mut queued = Box::pin(client.download_attachment(&id));
    assert!(
        queued
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    let mut shutdown = Box::pin(owner.shutdown());
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(shutdown);
    entered(&body.join_started).await;
    assert!(body.cancelled.load(Ordering::SeqCst));
    assert!(matches!(queued.await, Err(AccountError::Stopped)));
    assert!(matches!(
        client.conversation(CHAT).await,
        Err(AccountError::Stopped)
    ));
    assert!(matches!(
        client.download_attachment(&id).await,
        Err(AccountError::Stopped)
    ));
    let path = StoreScope::new(ORIGIN, B)
        .unwrap()
        .database_path(f.root.path());
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.parent().unwrap().join("owner.lock"))
        .unwrap();
    assert!(lock.try_lock().is_err());
    drop(lock);
    body.join_released.store(true, Ordering::SeqCst);
    body.join_release.notify_one();
    phase(&client, AccountPhase::Stopped).await;
    let worker = CryptoCoordinator::start(store(f.root.path(), B, false), b.own_device().clone())
        .await
        .unwrap();
    assert!(matches!(
        worker.cached_epoch_attachment(id).await,
        Err(CryptoError::MessageUnavailable)
    ));
    worker.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn http_failures_do_not_reset_ratchets_and_explicit_retry_keeps_read_identity() {
    for error in [404, 429, 503] {
        let (f, a, b) = prepared().await;
        let (id, blob) = receive(&f, &a, &b).await;
        let before = records(f.root.path(), B)
            .into_iter()
            .filter(|r| r.0 == RecordKind::Session as i64)
            .collect::<Vec<_>>();
        let rpc = Arc::new(Probe::default());
        let body = Arc::new(ReadProbe {
            error: AtomicU16::new(error),
            ..Default::default()
        });
        body.file(&id, blob);
        let owner = launch_read(f.bob, &rpc, &body);
        let client = owner.client();
        phase(&client, AccountPhase::Running).await;
        assert!(
            matches!(client.download_attachment(&id).await,Err(AccountError::Attachment(BlobReadError::Rejected(n))) if n==error)
        );
        assert_eq!(client.status().phase, AccountPhase::Running);
        body.error.store(0, Ordering::SeqCst);
        client.download_attachment(&id).await.unwrap();
        assert_eq!(bytes(&client, &id).await.as_bytes(), CONTENT);
        {
            let seen = body.seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0].binding(), seen[1].binding());
            assert_eq!(seen[0].blob(), seen[1].blob());
        }
        owner.shutdown().await.unwrap();
        assert_eq!(
            records(f.root.path(), B)
                .into_iter()
                .filter(|r| r.0 == RecordKind::Session as i64)
                .collect::<Vec<_>>(),
            before
        );
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn http_401_stops_account_without_replacing_keys_or_deleting_received_history() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let before = records(f.root.path(), B)
        .into_iter()
        .filter(|r| {
            [RecordKind::Identity, RecordKind::History, RecordKind::Inbox]
                .iter()
                .any(|k| r.0 == *k as i64)
        })
        .collect::<Vec<_>>();
    let rpc = Arc::new(Probe::default());
    let body = Arc::new(ReadProbe {
        error: AtomicU16::new(401),
        ..Default::default()
    });
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    assert!(matches!(
        client.download_attachment(&id).await,
        Err(AccountError::Attachment(BlobReadError::Rejected(401)))
    ));
    phase(&client, AccountPhase::Failed).await;
    assert!(matches!(
        owner.shutdown().await,
        Err(AccountError::Rpc(RpcError::Status(Code::Unauthenticated)))
    ));
    assert!(body.cancelled.load(Ordering::SeqCst));
    assert_eq!(
        records(f.root.path(), B)
            .into_iter()
            .filter(
                |r| [RecordKind::Identity, RecordKind::History, RecordKind::Inbox]
                    .iter()
                    .any(|k| r.0 == *k as i64)
            )
            .collect::<Vec<_>>(),
        before
    );
    let worker = CryptoCoordinator::start(store(f.root.path(), B, false), b.own_device().clone())
        .await
        .unwrap();
    assert!(worker.epoch_incoming_message(id).await.is_ok());
    worker.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn uploads_and_explicit_downloads_share_one_body_lane_without_starvation() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let outgoing = uuid::Uuid::new_v4().to_string();
    super::blob_tests::stage(&f.bob, &outgoing).await;
    let rpc = Arc::new(Probe::default());
    *rpc.lookup_reply.lock().unwrap() =
        Some(super::super::incoming::tests::b_conversation(&f.a, &b));
    let body = Arc::new(ReadProbe {
        block_put: AtomicBool::new(true),
        ..Default::default()
    });
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    entered(&body.put_started).await;
    let mut request = Box::pin(client.download_attachment(&id));
    assert!(
        request
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(body.seen.lock().unwrap().is_empty());
    body.put_released.store(true, Ordering::SeqCst);
    body.put_release.notify_one();
    tokio::time::timeout(Duration::from_secs(10), request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*body.events.lock().unwrap(), vec!["put", "get"]);
    owner.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn different_accounts_are_isolated_and_foreign_uuid_never_reaches_http() {
    let (f, a, b) = prepared().await;
    let (id_b, blob_b) = receive(&f, &a, &b).await;
    let (id_a, blob_a) = receive_between(&f.bob, &f.alice, &b, &a, b"other account file", 1).await;
    let rpc_a = Arc::new(Probe::default());
    let rpc_b = Arc::new(Probe::default());
    let body_a = Arc::new(ReadProbe::default());
    body_a.file(&id_a, blob_a);
    let body_b = Arc::new(ReadProbe {
        block: AtomicBool::new(true),
        ..Default::default()
    });
    body_b.file(&id_b, blob_b);
    let owner_a = launch_read(f.alice, &rpc_a, &body_a);
    let ca = owner_a.client();
    phase(&ca, AccountPhase::Running).await;
    let owner_b = launch_read(f.bob, &rpc_b, &body_b);
    let cb = owner_b.client();
    phase(&cb, AccountPhase::Running).await;
    let mut pending = Box::pin(cb.download_attachment(&id_b));
    tokio::select! {_=entered(&body_b.started)=>{},_=&mut pending=>panic!("GET must block")};
    assert!(matches!(
        ca.download_attachment(&id_b).await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    assert!(body_a.seen.lock().unwrap().is_empty());
    ca.download_attachment(&id_a).await.unwrap();
    assert_eq!(bytes(&ca, &id_a).await.as_bytes(), b"other account file");
    drop(owner_b);
    assert!(matches!(pending.await, Err(AccountError::Stopped)));
    phase(&cb, AccountPhase::Stopped).await;
    assert!(!body_a.cancelled.load(Ordering::SeqCst));
    assert!(ca.conversation(CHAT).await.unwrap().is_some());
    owner_a.shutdown().await.unwrap();
}

async fn header(socket: &mut tokio::net::TcpStream) -> String {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut b = [0; 1024];
        let n = socket.read(&mut b).await.unwrap();
        assert!(n > 0 && bytes.len() + n < 8192);
        bytes.extend(&b[..n]);
    }
    String::from_utf8(bytes).unwrap()
}
fn response(size: usize) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nContent-Type: application/octet-stream\r\nCache-Control: private, no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Disposition: attachment; filename=\"encrypted.bin\"\r\nAccept-Ranges: none\r\nConnection: close\r\n\r\n"
    )
}

#[tokio::test]
async fn native_http_account_shutdown_closes_partial_socket_and_reopen_retries_exact_blob() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (f, a, b) = prepared_at(&origin).await;
    let (id, blob) = receive(&f, &a, &b).await;
    let (started, ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let h1 = header(&mut first).await;
        first
            .write_all(response(blob.len()).as_bytes())
            .await
            .unwrap();
        first.write_all(&blob[..17]).await.unwrap();
        let _ = started.send(());
        let mut b = [0; 1];
        let n = tokio::time::timeout(Duration::from_secs(5), first.read(&mut b))
            .await
            .unwrap();
        assert!(matches!(n, Ok(0) | Err(_)));
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        let h2 = header(&mut second).await;
        assert_eq!(h1, h2);
        second
            .write_all(response(blob.len()).as_bytes())
            .await
            .unwrap();
        second.write_all(&blob).await.unwrap();
    });
    let rpc = Arc::new(Probe::default());
    let owner = launch_body(
        f.bob,
        &rpc,
        NativeBlobHttp::test_reader(&origin, b.own_device().clone()),
    );
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    let mut request = Box::pin(client.download_attachment(&id));
    tokio::select! {_=ready=>{},_=&mut request=>panic!("expected incomplete body")};
    owner.shutdown().await.unwrap();
    assert!(matches!(request.await, Err(AccountError::Stopped)));
    let worker = CryptoCoordinator::start(
        test_open(f.root.path(), StoreScope::new(&origin, B).unwrap(), false),
        b.own_device().clone(),
    )
    .await
    .unwrap();
    assert!(matches!(
        worker.cached_epoch_attachment(id.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    let owner = launch_body(
        worker,
        &rpc,
        NativeBlobHttp::test_reader(&origin, b.own_device().clone()),
    );
    let newer = owner.client();
    phase(&newer, AccountPhase::Running).await;
    assert!(matches!(
        client.download_attachment(&id).await,
        Err(AccountError::Stopped)
    ));
    newer.download_attachment(&id).await.unwrap();
    assert_eq!(bytes(&newer, &id).await.as_bytes(), CONTENT);
    server.await.unwrap();
    owner.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}
