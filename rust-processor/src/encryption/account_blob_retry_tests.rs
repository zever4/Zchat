//! Real account task/store/Signal outbox, controlled ambiguous server outcomes.
use super::promotion::stored;
use super::*;
use crate::encryption::signal_state::EpochAttachmentPromotion;

fn server() -> Arc<Probe> {
    Arc::new(Probe {
        send_accept: AtomicBool::new(true),
        send_requires_ready: AtomicBool::new(true),
        ..Default::default()
    })
}
async fn promote(
    w: &CryptoCoordinator,
    rpc: &Arc<Probe>,
    id: &str,
) -> (EpochOutgoingPlan, Vec<u8>) {
    let control = stage(w, id).await;
    let p = w
        .prepared_epoch_attachment(id.into())
        .await
        .unwrap()
        .unwrap();
    let bytes = w
        .prepared_epoch_attachment_blob(p)
        .await
        .unwrap()
        .as_bytes()
        .to_vec();
    rpc.blob_ready.lock().unwrap().insert(id.into());
    let EpochBlobControlOutcome::Lease(lease) = Fake(rpc.clone()).blob(&control, true).unwrap()
    else {
        panic!("Ready")
    };
    w.accept_epoch_blob_lease(*lease).await.unwrap();
    let EpochAttachmentPromotion::Queued(p) = w.promote_epoch_attachment_if(control).await.unwrap()
    else {
        panic!("outbox")
    };
    rpc.blob_requests.lock().unwrap().clear();
    rpc.blob_events.lock().unwrap().clear();
    (*p, bytes)
}
fn expire(rpc: &Probe, id: &str) {
    rpc.blob_ready.lock().unwrap().remove(id);
    let mut leases = rpc.blob_leases.lock().unwrap();
    let g = leases.get_mut(id).unwrap();
    g.expired = true;
    g.observed_at_unix_ms = g.expires_at_unix_ms;
}
async fn steps(client: &AccountClient, n: u64) {
    let mut updates = client.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        while client.status().blob_retry_steps < n {
            assert!(!matches!(
                client.status().phase,
                AccountPhase::Stopped | AccountPhase::Failed
            ));
            assert!(updates.changed().await.is_some());
        }
    })
    .await
    .unwrap();
}
fn exact(rpc: &Probe, body: &BodyProbe, p: &EpochOutgoingPlan, bytes: &[u8]) {
    let sends = rpc.send_requests.lock().unwrap();
    assert!(sends.len() >= 2);
    assert!(sends.iter().all(|s| s == p.request()));
    assert_eq!(rpc.send_receipts.lock().unwrap().len(), 1);
    assert!(rpc.acquire_requests.lock().unwrap().is_empty());
    assert_eq!(
        body.ciphertexts.lock().unwrap().as_slice(),
        &[bytes.to_vec()]
    );
    let id = &p.request().envelope.as_ref().unwrap().message_id;
    assert_eq!(
        body.ids.lock().unwrap().as_slice(),
        std::slice::from_ref(id)
    );
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn retained_expired_file_sends_first_then_renews_uploads_and_sends_identical_signal() {
    let (f, _, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let rpc = server();
    let (send, bytes) = promote(&f.alice, &rpc, &id).await;
    expire(&rpc, &id);
    rpc.block_send.store(true, Ordering::SeqCst);
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&rpc.send_started).await;
    assert!(client.conversation(CHAT).await.unwrap().is_some());
    assert!(rpc.blob_requests.lock().unwrap().is_empty());
    assert!(body.ids.lock().unwrap().is_empty());
    rpc.send_released.store(true, Ordering::SeqCst);
    rpc.send_release.notify_one();
    stored(&client, &id).await;
    steps(&client, 1).await;
    assert_eq!(client.status().blob_promotions, 0); // A retained retry cannot promote again.
    owner.shutdown().await.unwrap();
    exact(&rpc, &body, &send, &bytes);
    {
        let events = rpc.blob_events.lock().unwrap();
        assert_eq!(events[0], ("send", id.clone()));
        let reserve = events.iter().position(|e| e.0 == "reserve").unwrap();
        let put = events.iter().position(|e| e.0 == "put").unwrap();
        assert!(reserve < put);
        assert_eq!(events.iter().filter(|e| e.0 == "reserve").count(), 1);
    }
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_unknown_reserve_put_and_observe_results_recover_without_resealing_or_duplicate_upload()
 {
    for fault in ["reserve", "put", "observe"] {
        eprintln!("retained unknown-result fault={fault}");
        let (f, _, _) = prepared().await;
        let id = uuid::Uuid::new_v4().to_string();
        let rpc = server();
        let (send, bytes) = promote(&f.alice, &rpc, &id).await;
        expire(&rpc, &id);
        rpc.blob_lose_reserve_once
            .store(fault == "reserve", Ordering::SeqCst);
        rpc.blob_fail_observe_after_put_once
            .store(fault == "observe", Ordering::SeqCst);
        rpc.hold_receipt_for_observe_fault
            .store(fault == "observe", Ordering::SeqCst);
        let body = Arc::new(BodyProbe {
            lose_reply_once: AtomicBool::new(fault == "put"),
            ..Default::default()
        });
        let owner = launch_upload(f.alice, &rpc, &body);
        let client = owner.client();
        stored(&client, &id).await;
        steps(&client, 1).await;
        owner.shutdown().await.unwrap();
        exact(&rpc, &body, &send, &bytes);
        assert!(!rpc.blob_lose_reserve_once.load(Ordering::SeqCst));
        assert!(!body.lose_reply_once.load(Ordering::SeqCst));
        assert!(!rpc.blob_fail_observe_after_put_once.load(Ordering::SeqCst));
        {
            let requests = rpc.blob_requests.lock().unwrap();
            let mut successor = None;
            for r in requests.iter().filter(|r| r.expected_revision > 0) {
                if let Some(old) = successor {
                    assert_eq!(r, old);
                } else {
                    successor = Some(r);
                }
                assert_eq!(r.blob, send.request().envelope.as_ref().unwrap().blob);
            }
            assert!(successor.is_some());
        }
        assert_eq!(
            rpc.blob_events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.0 == "reserve")
                .count(),
            1
        );
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn retained_shutdown_joins_body_before_unlock_and_restart_observes_writing_without_overwrite()
{
    let (f, a, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let rpc = server();
    let (send, bytes) = promote(&f.alice, &rpc, &id).await;
    expire(&rpc, &id);
    let body = Arc::new(BodyProbe {
        block: AtomicBool::new(true),
        hold_join: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.started).await;
    {
        let mut all = rpc.blob_leases.lock().unwrap();
        let g = all.get_mut(&id).unwrap();
        g.state = v2::BlobLeaseState::Writing as i32;
        g.observed_at_unix_ms += 1;
    }
    let mut shutdown = Box::pin(owner.shutdown());
    assert!(
        shutdown
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(shutdown);
    entered(&body.join_started).await;
    let path = StoreScope::new(SERVER, A)
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
    assert!(!body.owns_source.load(Ordering::SeqCst));
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .unwrap()
            .request(),
        send.request()
    );
    let second = Arc::new(BodyProbe::default());
    let owner = launch_upload(w, &rpc, &second);
    let client = owner.client();
    steps(&client, 1).await;
    assert!(second.ids.lock().unwrap().is_empty()); // Writing is not a grant to overwrite.
    expire(&rpc, &id);
    stored(&client, &id).await;
    owner.shutdown().await.unwrap();
    assert_eq!(second.ciphertexts.lock().unwrap().as_slice(), &[bytes]);
    assert!(
        rpc.send_requests
            .lock()
            .unwrap()
            .iter()
            .all(|p| p == send.request())
    );
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_failed_file_does_not_starve_another_file_or_text() {
    let (f, _, _) = prepared().await;
    let rpc = server();
    let bad = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    let good = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
    promote(&f.alice, &rpc, bad).await;
    expire(&rpc, bad);
    let (good_send, bytes) = promote(&f.alice, &rpc, good).await;
    expire(&rpc, good);
    rpc.blob_errors
        .lock()
        .unwrap()
        .insert(bad.into(), RpcError::Transport);
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    stored(&client, good).await;
    let text = uuid::Uuid::new_v4().to_string();
    client
        .send_text(CHAT, &text, "text survives another file failure")
        .await
        .unwrap();
    stored(&client, &text).await;
    assert!(!client.outgoing(bad).await.unwrap().stored_on_server);
    owner.shutdown().await.unwrap();
    assert_eq!(body.ids.lock().unwrap().as_slice(), &[good.to_string()]);
    assert_eq!(body.ciphertexts.lock().unwrap().as_slice(), &[bytes]);
    assert!(
        rpc.send_requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.envelope.as_ref().unwrap().message_id == good)
            .all(|r| r == good_send.request())
    );
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_generic_send_failure_has_no_transfer_hint_and_never_checks_the_file() {
    let (f, _, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let rpc = server();
    let (send, _) = promote(&f.alice, &rpc, &id).await;
    expire(&rpc, &id);
    rpc.send_accept.store(false, Ordering::SeqCst);
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    let mut updates = client.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        while client.status().delivery_rounds == 0 {
            assert!(updates.changed().await.is_some());
        }
    })
    .await
    .unwrap();
    assert_eq!(client.status().blob_retry_steps, 0);
    owner.shutdown().await.unwrap();
    assert!(rpc.blob_requests.lock().unwrap().is_empty());
    assert!(body.ids.lock().unwrap().is_empty());
    assert!(
        rpc.send_requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r == send.request())
    );
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_receipt_during_last_body_join_blocks_followup_metadata_and_new_upload() {
    let (f, _, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let rpc = server();
    let (send, bytes) = promote(&f.alice, &rpc, &id).await;
    expire(&rpc, &id);
    let body = Arc::new(BodyProbe {
        hold_join: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.join_started).await;
    stored(&client, &id).await;
    assert!(body.owns_source.load(Ordering::SeqCst));
    let metadata = rpc.blob_requests.lock().unwrap().len();
    body.join_released.store(true, Ordering::SeqCst);
    body.join_release.notify_one();
    steps(&client, 1).await;
    owner.shutdown().await.unwrap();
    exact(&rpc, &body, &send, &bytes);
    assert_eq!(rpc.blob_requests.lock().unwrap().len(), metadata);
    assert_eq!(
        rpc.blob_events
            .lock()
            .unwrap()
            .iter()
            .filter(|e| e.0 == "reserve")
            .count(),
        1
    );
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_local_recovery_freeze_during_metadata_blocks_renewal_without_auto_consent() {
    let (f, _, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let rpc = server();
    promote(&f.alice, &rpc, &id).await;
    expire(&rpc, &id);
    rpc.block_observe.store(true, Ordering::SeqCst);
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&rpc.observe_started).await;
    let proposal = client.propose_recovery(CHAT).await.unwrap();
    assert!(uuid::Uuid::parse_str(&proposal).is_ok());
    assert!(
        client
            .conversation(CHAT)
            .await
            .unwrap()
            .unwrap()
            .sending_frozen
    );
    rpc.observe_released.store(true, Ordering::SeqCst);
    rpc.observe_release.notify_one();
    steps(&client, 1).await;
    assert!(
        client
            .conversation(CHAT)
            .await
            .unwrap()
            .unwrap()
            .sending_frozen
    );
    owner.shutdown().await.unwrap();
    assert!(body.ids.lock().unwrap().is_empty());
    assert!(
        !rpc.blob_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.0 == "reserve")
    );
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn retained_revoked_session_stops_http_and_keeps_exact_outbox_for_next_login() {
    let (f, a, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let rpc = server();
    let (send, _) = promote(&f.alice, &rpc, &id).await;
    expire(&rpc, &id);
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
    assert!(body.cancelled.load(Ordering::SeqCst));
    assert!(!body.owns_source.load(Ordering::SeqCst));
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_attachment_outgoing(id)
            .await
            .unwrap()
            .unwrap()
            .request(),
        send.request()
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
