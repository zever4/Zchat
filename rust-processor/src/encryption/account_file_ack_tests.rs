//! Actual Signal/cache +account lifetime, scripted remote FILEACK outcomes.
use super::download_tests::{ReadProbe, launch_read, receive};
use super::*;
use std::time::Instant;

async fn seed(f: &Fixture, a: &EpochScope, b: &EpochScope, sequence: i64) -> String {
    use super::super::attachment_download::tests::{append, begin, finish};
    let (id, blob) = super::download_tests::receive_between(
        &f.alice,
        &f.bob,
        a,
        b,
        b"durable FILEACK cache",
        sequence,
    )
    .await;
    let plan = f
        .bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, &blob).await.unwrap();
    finish(&f.bob, run).await.unwrap();
    id
}
async fn health(c: &AccountClient, expected: FileAckHealth) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert_eq!(c.status().phase, AccountPhase::Running);
            if c.status().file_ack_health == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

pub(super) async fn saved(c: &AccountClient, id: &str) -> i64 {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            assert_eq!(c.status().phase, AccountPhase::Running);
            if let Some(at) = c.attachment_file_ack_status(id).await.unwrap() {
                return at;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|e| panic!("FILEACK wait for {id}: {e}; account={:?}", c.status()))
}

#[tokio::test]
async fn file_ack_waits_behind_pending_delivery_after_the_file_is_already_saved() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let rpc = Arc::new(Probe {
        block_pending: true,
        ..Default::default()
    });
    let body = Arc::new(ReadProbe::default());
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    super::download_tests::entered(&rpc.pending_started).await;
    client.download_attachment(&id).await.unwrap();
    let cached_at = Instant::now();

    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(rpc.active.load(Ordering::SeqCst), 1);
    assert!(rpc.file_ack_requests.lock().unwrap().is_empty());
    assert!(
        client
            .attachment_file_ack_status(&id)
            .await
            .unwrap()
            .is_none()
    );
    let released_at = Instant::now();
    rpc.pending_released.store(true, Ordering::SeqCst);
    rpc.release.notify_one();

    assert_eq!(saved(&client, &id).await, 1_700_000_000_123);
    eprintln!(
        "FILEACK after local cache: {:?}; after delivery release: {:?}",
        cached_at.elapsed(),
        released_at.elapsed()
    );
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.file_ack_requests.lock().unwrap().len(), 1);
    f.alice.shutdown().await.unwrap();
}
#[tokio::test]
async fn file_ack_owner_automatically_retries_lost_reply_without_duplicate_fact_or_descriptor_ack()
{
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let rpc = Arc::new(Probe {
        file_ack_lose_reply_once: AtomicBool::new(true),
        ..Default::default()
    });
    let body = Arc::new(ReadProbe::default());
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    c.download_attachment(&id).await.unwrap();
    assert_eq!(saved(&c, &id).await, 1_700_000_000_123);
    assert_eq!(rpc.file_ack_receipts.lock().unwrap().len(), 1);
    {
        let r = rpc.file_ack_requests.lock().unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], r[1]);
    }
    assert_eq!(body.seen.lock().unwrap().len(), 1);
    owner.shutdown().await.unwrap();
    assert!(matches!(
        c.attachment_file_ack_status(&id).await,
        Err(AccountError::Stopped)
    ));
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own_device().clone())
        .await
        .unwrap();
    assert!(!w.epoch_ack(id.clone()).await.unwrap().acknowledged());
    assert!(w.epoch_file_ack_status(id.clone()).await.unwrap().is_some());
    let next = Arc::new(Probe::default());
    let owner = launch(w, &next);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    assert_eq!(saved(&c, &id).await, 1_700_000_000_123);
    owner.shutdown().await.unwrap();
    assert!(next.file_ack_requests.lock().unwrap().is_empty());
    f.alice.shutdown().await.unwrap();
}
#[tokio::test]
async fn file_ack_blocked_remote_does_not_block_local_reads_or_text_and_stop_preserves_pending() {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let rpc = Arc::new(Probe {
        block_file_ack: AtomicBool::new(true),
        ..Default::default()
    });
    let body = Arc::new(ReadProbe::default());
    body.file(&id, blob);
    let owner = launch_read(f.bob, &rpc, &body);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    c.download_attachment(&id).await.unwrap();
    super::download_tests::entered(&rpc.file_ack_started).await;
    assert_eq!(rpc.active.load(Ordering::SeqCst), 1);
    assert!(c.attachment_file_ack_status(&id).await.unwrap().is_none());
    assert!(c.conversation(CHAT).await.unwrap().is_some());
    c.send_text(
        CHAT,
        &uuid::Uuid::new_v4().to_string(),
        "local text during ACK",
    )
    .await
    .unwrap();
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own_device().clone())
        .await
        .unwrap();
    assert!(w.epoch_file_ack_status(id.clone()).await.unwrap().is_none());
    let next = launch(w, &rpc);
    let c = next.client();
    phase(&c, AccountPhase::Running).await;
    rpc.file_ack_released.store(true, Ordering::SeqCst);
    rpc.file_ack_release.notify_one();
    saved(&c, &id).await;
    next.shutdown().await.unwrap();
    {
        let r = rpc.file_ack_requests.lock().unwrap();
        assert!(r.len() >= 2);
        assert!(r.iter().all(|x| x == &r[0]));
    }
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_pages_and_per_file_errors_do_not_starve_other_files_or_delivery() {
    let (f, a, b) = prepared().await;
    let mut ids = Vec::new();
    for sequence in 1..=33 {
        ids.push(seed(&f, &a, &b, sequence).await);
    }
    // Candidate pages are ordered by UUID, not by the order they were seeded.
    // Keep the 10s bound per next candidate instead of timing a whole 33-file
    // backlog by accidentally waiting for the last UUID first.
    ids.sort();
    let rpc = Arc::new(Probe {
        send_accept: AtomicBool::new(true),
        file_ack_delay: Duration::from_millis(350),
        ..Default::default()
    });
    let errors: Vec<_> = [1, 3, 5, 7].into_iter().map(|i| ids[i].clone()).collect();
    for (id, error) in errors.iter().zip([
        RpcError::Status(Code::NotFound),
        RpcError::Status(Code::ResourceExhausted),
        RpcError::Status(Code::FailedPrecondition),
        RpcError::LocalDeadline,
    ]) {
        rpc.file_ack_errors
            .lock()
            .unwrap()
            .insert(id.clone(), error);
    }
    // Seeded prekey messages also produce delivery work. Bound that lane's
    // quantum below the per-file10s assertion; launch() otherwise permits an
    // entire30s delivery round before another FILEACK turn.
    let owner = AccountOwner::spawn(
        f.bob,
        EpochDeliveryRunner::new(
            Fake(rpc.clone()),
            RunnerConfig {
                poll_interval: Duration::from_millis(10),
                retry_initial: Duration::from_millis(100),
                retry_max: Duration::from_millis(500),
                round_timeout: Duration::from_secs(2),
            },
        )
        .unwrap(),
        PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap(),
    );
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    for id in ids.iter().filter(|id| !errors.contains(id)) {
        saved(&c, id).await;
    }
    for id in &errors {
        assert!(c.attachment_file_ack_status(id).await.unwrap().is_none());
    }
    let text = uuid::Uuid::new_v4().to_string();
    c.send_text(CHAT, &text, "delivery survives file ACK errors")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let tried = rpc
                .file_ack_requests
                .lock()
                .unwrap()
                .iter()
                .map(|r| r.files[0].binding.as_ref().unwrap().message_id.clone())
                .collect::<Vec<_>>();
            if errors
                .iter()
                .all(|id| tried.iter().filter(|x| *x == id).count() >= 2)
                && rpc.send_receipts.lock().unwrap().contains_key(&text)
                && c.status().maintenance_steps > 0
                && c.status().delivery_rounds >= 2
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    rpc.file_ack_errors.lock().unwrap().clear();
    for id in &errors {
        saved(&c, id).await;
    }
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.file_ack_receipts.lock().unwrap().len(), 33);
    {
        let requests = rpc.file_ack_requests.lock().unwrap();
        for id in &ids {
            let mut matching = requests
                .iter()
                .filter(|r| r.files[0].binding.as_ref().unwrap().message_id == *id);
            let first = matching.next().unwrap();
            assert!(matching.all(|r| r == first));
        }
    }
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_waits_for_shared_file_memory_without_blocking_text_or_faking_receipt() {
    let (f, a, b) = prepared().await;
    let id = seed(&f, &a, &b, 1).await;
    let cache = f.bob.cached_epoch_attachment(id.clone()).await.unwrap();
    let held = f.bob.open_cached_epoch_attachment(cache).await.unwrap();
    let rpc = Arc::new(Probe::default());
    let owner = launch(f.bob, &rpc);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    health(&c, FileAckHealth::Busy).await;
    assert!(rpc.file_ack_requests.lock().unwrap().is_empty());
    assert!(c.attachment_file_ack_status(&id).await.unwrap().is_none());
    c.send_text(
        CHAT,
        &uuid::Uuid::new_v4().to_string(),
        "file memory is busy",
    )
    .await
    .unwrap();
    drop(held);
    saved(&c, &id).await;
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.file_ack_requests.lock().unwrap().len(), 1);
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_deadline_cancels_rpc_and_replays_original_after_unknown_commit() {
    let (f, a, b) = prepared().await;
    let id = seed(&f, &a, &b, 1).await;
    let rpc = Arc::new(Probe {
        block_file_ack: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = AccountOwner::spawn(
        f.bob,
        EpochDeliveryRunner::new(
            Fake(rpc.clone()),
            RunnerConfig {
                poll_interval: Duration::from_millis(20),
                retry_initial: Duration::from_millis(100),
                retry_max: Duration::from_millis(200),
                round_timeout: Duration::from_secs(1),
            },
        )
        .unwrap(),
        PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap(),
    );
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    health(&c, FileAckHealth::TimedOut).await;
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    assert!(c.attachment_file_ack_status(&id).await.unwrap().is_none());
    rpc.file_ack_released.store(true, Ordering::SeqCst);
    rpc.file_ack_release.notify_one();
    saved(&c, &id).await;
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.file_ack_receipts.lock().unwrap().len(), 1);
    {
        let requests = rpc.file_ack_requests.lock().unwrap();
        assert!(requests.len() >= 2);
        assert!(requests.iter().all(|r| r == &requests[0]));
    }
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_auth_or_generation_failure_stops_owner_and_preserves_pending_cache() {
    for error in [
        RpcError::Status(Code::Unauthenticated),
        RpcError::Status(Code::PermissionDenied),
        RpcError::Failure {
            code: Code::FailedPrecondition,
            reason: common::FailureReason::GenerationChanged,
        },
    ] {
        let (f, a, b) = prepared().await;
        let id = seed(&f, &a, &b, 1).await;
        let before = records(f.root.path(), B)
            .into_iter()
            .filter(|r| r.0 == RecordKind::Identity as i64)
            .collect::<Vec<_>>();
        let rpc = Arc::new(Probe::default());
        rpc.file_ack_errors
            .lock()
            .unwrap()
            .insert(id.clone(), error);
        let owner = launch(f.bob, &rpc);
        let c = owner.client();
        phase(&c, AccountPhase::Failed).await;
        assert!(owner.shutdown().await.is_err());
        assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
        assert!(rpc.file_ack_receipts.lock().unwrap().is_empty());
        let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own_device().clone())
            .await
            .unwrap();
        assert!(w.epoch_file_ack_status(id.clone()).await.unwrap().is_none());
        assert_eq!(
            w.pending_epoch_file_acks(None).await.unwrap().messages,
            vec![id.clone()]
        );
        let proof = w.epoch_file_ack(id).await.unwrap();
        assert!(proof.acknowledged_at_unix_ms().is_none());
        w.shutdown().await.unwrap();
        assert_eq!(
            records(f.root.path(), B)
                .into_iter()
                .filter(|r| r.0 == RecordKind::Identity as i64)
                .collect::<Vec<_>>(),
            before
        );
        f.alice.shutdown().await.unwrap();
    }
}
