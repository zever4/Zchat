use super::super::tests::{acquired, material, public};
use super::*;
use crate::encryption::{
    native::EpochObservation,
    protected::{AccountKey, AccountKind, StoreScope, test_open},
    publication::publication_digest,
};
use crate::proto::{
    e2ee::{ConversationMode, Device, OwnDeviceState, TextMessage, UploadReceipt},
    e2ee_v2 as v2,
};
use std::{fs::OpenOptions, path::Path};
const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const G: &str = "33333333-3333-4333-8333-333333333333";
const BG: &str = "44444444-4444-4444-8444-444444444444";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
fn scope(user: &str) -> StoreScope {
    StoreScope::new("https://archive-owner.test", user).unwrap()
}
fn open(root: &Path, user: &str, create: bool) -> DeviceProvisioning {
    DeviceProvisioning::new(test_open(root, scope(user), create))
}
fn initialized(root: &Path, user: &str) -> DeviceProvisioning {
    let mut p = open(root, user, true);
    let attempt = p.prepare_registration().unwrap();
    let device = Device {
        reference: Some(DeviceRef {
            user_id: user.into(),
            device_id: 1,
            generation_id: if user == A { G } else { BG }.into(),
        }),
        registration_id: attempt.request.registration_id,
        identity_key: attempt.request.identity_key.clone(),
        public_material_ready: false,
        public_material_revision: 0,
    };
    p.bind_registration(
        &attempt.request.registration_request_id,
        &OwnDeviceState {
            user_id: user.into(),
            active_device: Some(device),
        },
    )
    .unwrap();
    while let Some(batch) = p.next_upload().unwrap() {
        p.acknowledge_upload(&UploadReceipt {
            batch_id: batch.batch_id.clone(),
            batch_digest: publication_digest(&batch).unwrap().to_vec(),
            publication_revision: batch.expected_revision + 1,
        })
        .unwrap();
    }
    p
}
fn locked(root: &Path) -> bool {
    let path = scope(A).database_path(root);
    let f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path.parent().unwrap().join("owner.lock"))
        .unwrap();
    match f.try_lock() {
        Ok(()) => {
            f.unlock().unwrap();
            false
        }
        Err(std::fs::TryLockError::WouldBlock) => true,
        Err(e) => panic!("lock failed: {e}"),
    }
}
async fn released(root: &Path) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while locked(root) {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
}
async fn with_history(root: &Path) -> DeviceProvisioning {
    let mut p = initialized(root, A);
    let own = p.ready_device().unwrap();
    let worker = CryptoCoordinator::start(p.into_store(), own.clone())
        .await
        .unwrap();
    let peer_material = material();
    let peer = public(
        DeviceRef {
            user_id: B.into(),
            device_id: 1,
            generation_id: BG.into(),
        },
        &peer_material,
    );
    let c = Conversation {
        conversation_id: CHAT.into(),
        mode: ConversationMode::SignalPq1 as i32,
        own_device: Some(own.clone()),
        peer_device: Some(peer.clone()),
        state: ConversationState::Active as i32,
        ..Default::default()
    };
    let q = worker
        .prepare_conversation_lookup(CHAT.into())
        .await
        .unwrap();
    worker
        .accept_conversation_lookup(VerifiedConversationLookup::from_rpc(q, c.clone()))
        .await
        .unwrap();
    let q = worker.prepare_initial_epoch(CHAT.into()).await.unwrap();
    let s = worker
        .accept_initial_epoch(EpochObservation::test_observation(
            scope(A),
            q,
            v2::ConversationEpochs {
                protocol_version: 2,
                conversation: Some(c),
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
        ))
        .await
        .unwrap();
    let old = acquired(peer, &peer_material, true, false);
    let plan = worker.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let bundle = v2::AcquiredBundle {
        protocol_version: 2,
        conversation_id: CHAT.into(),
        acquisition_id: plan.request().acquisition_id.clone(),
        device: old.device,
        signed_pre_key: old.signed_pre_key,
        one_time_pre_key: old.one_time_pre_key,
        kyber_pre_key: old.kyber_pre_key,
        not_after_unix_ms: old.not_after_unix_ms,
        crypto_epoch: Some(0),
    };
    worker
        .bootstrap_epoch(EpochObservation::test_observation(scope(A), plan, bundle))
        .await
        .unwrap();
    for i in 0..12 {
        worker
            .encrypt_epoch(
                s.clone(),
                v2::EncryptedPayload {
                    format_version: 2,
                    message_id: uuid::Uuid::new_v4().to_string(),
                    conversation_id: CHAT.into(),
                    sender: Some(s.own_device().clone()),
                    recipient: Some(s.peer_device().clone()),
                    created_at_unix_ms: 1_700_000_000_000 + i,
                    crypto_epoch: Some(0),
                    content: Some(v2::encrypted_payload::Content::Text(TextMessage {
                        text: format!("archive text {i}"),
                    })),
                },
            )
            .await
            .unwrap();
    }
    while !worker.seed_history_index().await.unwrap().ready {}
    worker.shutdown().await.unwrap();
    open(root, A, false)
}
fn next(page: ViewPage) -> ViewCursor {
    match page {
        ViewPage::History(v, c) => {
            assert_eq!(v.len(), 8);
            c.unwrap()
        }
        _ => panic!("wrong page"),
    }
}
#[tokio::test]
async fn readonly_owner_reads_actual_signal_history_and_cloned_clients_cannot_keep_store_alive() {
    let root = tempfile::tempdir().unwrap();
    let local = with_history(root.path()).await;
    drop(local);
    let path = scope(A).database_path(root.path());
    let before = std::fs::read(&path).unwrap();
    let owner = ArchiveOwner::start(open(root.path(), A, false))
        .await
        .unwrap()
        .unwrap();
    let client = owner.client();
    assert!(locked(root.path()));
    let cursor = next(
        client
            .read_views(ViewRequest::ArchiveHistory(
                G.into(),
                CHAT.into(),
                Some(0),
                None,
            ))
            .await
            .unwrap(),
    );
    match client
        .read_views(ViewRequest::ArchiveHistory(
            G.into(),
            CHAT.into(),
            Some(0),
            Some(cursor.clone()),
        ))
        .await
        .unwrap()
    {
        ViewPage::History(v, c) => {
            assert_eq!(v.len(), 4);
            assert!(c.is_none());
            assert!(v.iter().all(|v| v.text.starts_with("archive text ")));
        }
        _ => panic!("wrong page"),
    }
    for query in [
        ViewRequest::Dialogs(None),
        ViewRequest::History(CHAT.into(), None),
        ViewRequest::Statuses(CHAT.into(), vec![G.into()]),
    ] {
        assert!(matches!(
            client.read_views(query).await,
            Err(CryptoError::Invalid)
        ));
    }
    assert_eq!(std::fs::read(&path).unwrap(), before);
    owner.shutdown().await.unwrap();
    assert!(!locked(root.path()));
    assert!(!client.is_running());
    assert!(matches!(
        client.read_views(ViewRequest::Archives(None)).await,
        Err(CryptoError::Stopped)
    ));
    let again = ArchiveOwner::start(open(root.path(), A, false))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        again
            .client()
            .read_views(ViewRequest::ArchiveHistory(
                G.into(),
                CHAT.into(),
                Some(0),
                Some(cursor)
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    again.shutdown().await.unwrap();
    assert_eq!(std::fs::read(path).unwrap(), before);
}
#[test]
fn sqlite_query_only_rejects_record_and_account_writes_without_changing_store() {
    let root = tempfile::tempdir().unwrap();
    let local = initialized(root.path(), A);
    let (mut store, own) = local.into_archive_store().unwrap().unwrap();
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert!(
        store
            .transaction(|tx| tx.get(&RecordKey::new(
                &own.generation_id,
                RecordKind::Identity,
                "local"
            )?))
            .unwrap()
            .is_some()
    );
    assert!(
        store
            .transaction(|tx| tx.put(
                &RecordKey::new(G, RecordKind::Identity, "local")?,
                b"must fail"
            ))
            .is_err()
    );
    assert!(
        store
            .transaction(|tx| tx.put_account(
                &AccountKey::new(AccountKind::ActiveDevice, "initial")?,
                b"must fail"
            ))
            .is_err()
    );
    // Reopen cannot turn this downgraded connection back into a writer.
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    drop(store);
    assert!(!locked(root.path()));
}
#[tokio::test]
async fn empty_or_unbound_store_is_not_seeded_and_returns_no_reader() {
    for pending in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut p = open(root.path(), A, true);
        if pending {
            p.prepare_registration().unwrap();
        }
        drop(p);
        let path = scope(A).database_path(root.path());
        let before = std::fs::read(&path).unwrap();
        assert!(
            ArchiveOwner::start(open(root.path(), A, false))
                .await
                .unwrap()
                .is_none()
        );
        assert!(!locked(root.path()));
        assert_eq!(std::fs::read(path).unwrap(), before);
    }
}
#[tokio::test]
async fn cancelled_start_and_dropped_shutdown_future_release_even_with_old_reader_clones() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    let root = tempfile::tempdir().unwrap();
    let local = initialized(root.path(), A);
    let mut start = Box::pin(ArchiveOwner::start(local));
    let mut cx = Context::from_waker(Waker::noop());
    match start.as_mut().poll(&mut cx) {
        Poll::Ready(v) => drop(v.unwrap()),
        Poll::Pending => {}
    }
    drop(start);
    released(root.path()).await;
    let owner = ArchiveOwner::start(open(root.path(), A, false))
        .await
        .unwrap()
        .unwrap();
    let client = owner.client();
    drop(owner.shutdown());
    released(root.path()).await;
    assert!(!client.is_running());
}
#[tokio::test]
async fn archive_budget_includes_queued_requests_and_stop_wakes_without_a_free_slot() {
    let (sender, receiver) = mpsc::sync_channel(CAPACITY + 1);
    let control = Arc::new(Control {
        sender,
        budget: Arc::new(Semaphore::new(CAPACITY)),
        file_memory: Arc::new(Semaphore::new(1)),
        stopped: AtomicBool::new(false),
    });
    let client = ArchiveClient(control.clone());
    let mut calls = Vec::new();
    let mut requests = Vec::new();
    for _ in 0..CAPACITY {
        let c = client.clone();
        let mut call = Box::pin(async move { c.read_views(ViewRequest::Archives(None)).await });
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        assert!(matches!(
            call.as_mut().poll(&mut Context::from_waker(Waker::noop())),
            Poll::Pending
        ));
        calls.push(call);
    }
    assert!(matches!(
        client.read_views(ViewRequest::Archives(None)).await,
        Err(CryptoError::Busy)
    ));
    control.stop();
    for _ in 0..CAPACITY {
        requests.push(receiver.recv().unwrap().unwrap());
    }
    assert!(receiver.recv().unwrap().is_none());
    assert!(matches!(
        client.read_views(ViewRequest::Archives(None)).await,
        Err(CryptoError::Stopped)
    ));
    drop(requests);
    for call in calls {
        assert!(matches!(call.await, Err(CryptoError::Stopped)));
    }
}

#[tokio::test]
async fn cancelled_archive_file_waiter_keeps_memory_and_queue_until_request_drops() {
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    let (sender, receiver) = mpsc::sync_channel(CAPACITY + 1);
    let control = Arc::new(Control {
        sender,
        budget: Arc::new(Semaphore::new(CAPACITY)),
        file_memory: Arc::new(Semaphore::new(1)),
        stopped: AtomicBool::new(false),
    });
    let client = ArchiveClient(control.clone());
    let mut call = Box::pin(client.open_archived_attachment(G, CHAT, BG));
    let mut context = Context::from_waker(Waker::noop());
    assert!(matches!(call.as_mut().poll(&mut context), Poll::Pending));
    drop(call);
    assert_eq!(control.file_memory.available_permits(), 0);
    assert_eq!(control.budget.available_permits(), CAPACITY - 1);
    assert!(matches!(
        client.open_archived_attachment(G, CHAT, BG).await,
        Err(CryptoError::Busy)
    ));
    let queued = receiver.recv().unwrap().unwrap();
    assert_eq!(control.file_memory.available_permits(), 0);
    control.stop();
    assert!(receiver.recv().unwrap().is_none());
    assert!(matches!(
        client.open_archived_attachment(G, CHAT, BG).await,
        Err(CryptoError::Stopped)
    ));
    drop(queued);
    assert_eq!(control.file_memory.available_permits(), 1);
    assert_eq!(control.budget.available_permits(), CAPACITY);
}
