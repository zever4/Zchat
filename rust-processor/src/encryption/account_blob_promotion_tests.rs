//! Real protected account lifecycle and Signal/file AEAD, with test-only wire observations.
use super::*;
use crate::encryption::{
    attachment::{self, AttachmentBinding, AttachmentDescriptor},
    signal_state::EpochAttachmentPromotion,
};

fn enable(rpc: &Probe, f: &Fixture) {
    *rpc.lookup_reply.lock().unwrap() = Some(f.a.clone());
    *rpc.acquire_bundle.lock().unwrap() = Some(f.bundle.clone());
    rpc.send_accept.store(true, Ordering::SeqCst);
}

pub(super) async fn stored(client: &AccountClient, id: &str) {
    let mut updates = client.subscribe();
    let mut last_query_succeeded = None;
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match client.outgoing(id).await {
                Ok(view) => {
                    last_query_succeeded = Some(true);
                    if view.stored_on_server {
                        return;
                    }
                }
                Err(_) => last_query_succeeded = Some(false),
            }
            assert!(!matches!(
                client.status().phase,
                AccountPhase::Stopped | AccountPhase::Failed
            ));
            assert!(updates.changed().await.is_some());
        }
    })
    .await;
    assert!(
        result.is_ok(),
        "server receipt wait exceeded10s: id={id}, last_query_succeeded={last_query_succeeded:?}, status={:?}",
        client.status()
    );
}

#[tokio::test]
async fn ready_bootstraps_promotes_and_sends_real_signal_file_with_exact_lost_reply_retry() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let id = uuid::Uuid::new_v4().to_string();
    let control = stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    enable(&rpc, &f);
    rpc.lose_send_reply.store(true, Ordering::SeqCst);
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    stored(&client, &id).await;
    // Volatile health may be polled after the durable receipt on a fast worker.
    let mut updates = client.subscribe();
    tokio::time::timeout(Duration::from_secs(10), async {
        while client.status().blob_promotions == 0 {
            assert!(updates.changed().await.is_some());
        }
    })
    .await
    .unwrap();
    assert_eq!(client.status().blob_promotions, 1);
    assert_eq!(
        client.status().blob_promotion_health,
        Some(BlobPromotionHealth::Queued)
    );
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    assert_eq!(rpc.acquire_requests.lock().unwrap().len(), 1);
    assert_eq!(rpc.send_receipts.lock().unwrap().len(), 1);
    let sends = rpc.send_requests.lock().unwrap().clone();
    assert_eq!(sends.len(), 2);
    assert_eq!(sends[0], sends[1]);
    assert_eq!(*rpc.send_body_owned.lock().unwrap(), vec![false, false]);
    let envelope = sends[0].envelope.as_ref().unwrap();
    assert_eq!(envelope.protocol_version, 3);
    assert_eq!(envelope.blob, control.request().blob);
    assert_eq!(
        body.ids.lock().unwrap().as_slice(),
        std::slice::from_ref(&id)
    );
    assert!(
        rpc.blob_requests
            .lock()
            .unwrap()
            .iter()
            .all(|r| r == control.request())
    );
    let ciphertext = body.ciphertexts.lock().unwrap()[0].clone();
    let delivery = v2::Delivery {
        sequence: 1,
        envelope: Some(envelope.clone()),
        envelope_digest: crate::encryption::epoch_contract::envelope_digest(envelope)
            .unwrap()
            .to_vec(),
        expires_at_unix_ms: rpc.send_receipts.lock().unwrap()[&id].expires_at_unix_ms,
    };
    let observation =
        crate::encryption::signal_state::epoch_state::incoming::tests::delivery(&b, &delivery);
    let (payload, _) = f.bob.receive_epoch(b.clone(), observation).await.unwrap();
    let Some(v2::encrypted_payload::Content::Attachment(part)) = payload.content else {
        panic!("not an attachment")
    };
    let descriptor = AttachmentDescriptor::decode_from_signal(&part.private_descriptor).unwrap();
    let binding =
        AttachmentBinding::new(CHAT, 0, &id, &part.blob_id, a.own_device(), b.own_device())
            .unwrap();
    assert_eq!(
        attachment::open(&binding, &descriptor, &ciphertext)
            .unwrap()
            .as_slice(),
        b"private-account-upload-marker".repeat(1024)
    );
    let worker = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    let retained = worker.epoch_attachment_outgoing(id).await.unwrap().unwrap();
    assert_eq!(retained.request(), &sends[0]);
    assert!(!retained.needs_send());
    worker.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_sends_promoted_outbox_before_any_file_check_even_without_local_chunks() {
    let (f, a, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let control = stage(&f.alice, &id).await;
    let wire = Arc::new(Probe::default());
    wire.blob_ready.lock().unwrap().insert(id.clone());
    let EpochBlobControlOutcome::Lease(observation) = Fake(wire).blob(&control, true).unwrap()
    else {
        panic!("lease")
    };
    f.alice.accept_epoch_blob_lease(*observation).await.unwrap();
    let EpochAttachmentPromotion::Queued(outgoing) =
        f.alice.promote_epoch_attachment_if(control).await.unwrap()
    else {
        panic!("outbox")
    };
    f.alice.shutdown().await.unwrap();
    let db = rusqlite::Connection::open(
        StoreScope::new(SERVER, A)
            .unwrap()
            .database_path(f.root.path()),
    )
    .unwrap();
    db.execute("DELETE FROM protected_attachment_chunks", [])
        .unwrap();
    drop(db);
    let worker = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    let rpc = Arc::new(Probe::default());
    rpc.send_accept.store(true, Ordering::SeqCst);
    rpc.blob_errors
        .lock()
        .unwrap()
        .insert(id.clone(), RpcError::Status(Code::Unauthenticated));
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(worker, &rpc, &body);
    stored(&owner.client(), &id).await;
    owner.shutdown().await.unwrap();
    assert_eq!(
        rpc.send_requests.lock().unwrap().as_slice(),
        std::slice::from_ref(outgoing.request())
    );
    assert!(rpc.blob_requests.lock().unwrap().is_empty());
    assert!(rpc.acquire_requests.lock().unwrap().is_empty());
    assert!(body.ids.lock().unwrap().is_empty());
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn last_http_body_join_precedes_initial_acquisition_and_signal_promotion() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    enable(&rpc, &f);
    let body = Arc::new(BodyProbe {
        hold_join: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.join_started).await;
    assert!(body.owns_source.load(Ordering::SeqCst));
    assert!(client.outgoing(&id).await.is_err());
    assert_eq!(client.status().blob_promotions, 0);
    assert!(rpc.acquire_requests.lock().unwrap().is_empty());
    assert!(rpc.send_requests.lock().unwrap().is_empty());
    body.join_released.store(true, Ordering::SeqCst);
    body.join_release.notify_one();
    stored(&client, &id).await;
    owner.shutdown().await.unwrap();
    assert!(!body.owns_source.load(Ordering::SeqCst));
    assert_eq!(*rpc.send_body_owned.lock().unwrap(), vec![false]);
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn stop_during_bootstrap_retains_ready_file_and_reuses_acquisition_after_restart() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    let control = stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    enable(&rpc, &f);
    rpc.block_acquire.store(true, Ordering::SeqCst);
    let first_body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &first_body);
    entered(&rpc.acquire_started).await;
    owner.shutdown().await.unwrap();
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    assert!(!first_body.owns_source.load(Ordering::SeqCst));
    assert!(rpc.send_requests.lock().unwrap().is_empty());
    let saved = rpc.acquire_requests.lock().unwrap()[0].clone();
    let worker = CryptoCoordinator::start(store(f.root.path(), A, false), a.own_device().clone())
        .await
        .unwrap();
    assert!(
        worker
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        worker
            .epoch_attachment_control(id.clone())
            .await
            .unwrap()
            .request(),
        control.request()
    );
    rpc.acquire_released.store(true, Ordering::SeqCst);
    rpc.acquire_release.notify_one();
    let second_body = Arc::new(BodyProbe::default());
    let owner = launch_upload(worker, &rpc, &second_body);
    stored(&owner.client(), &id).await;
    owner.shutdown().await.unwrap();
    assert_eq!(
        rpc.acquire_requests.lock().unwrap().as_slice(),
        &[saved.clone(), saved]
    );
    assert!(second_body.ids.lock().unwrap().is_empty());
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn bootstrap_local_deadline_keeps_the_same_ready_file_and_pending_acquisition() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let id = uuid::Uuid::new_v4().to_string();
    stage(&f.alice, &id).await;
    let rpc = Arc::new(Probe::default());
    enable(&rpc, &f);
    rpc.block_acquire.store(true, Ordering::SeqCst);
    let body = Arc::new(BodyProbe::default());
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&rpc.acquire_started).await;
    let first = rpc.acquire_requests.lock().unwrap()[0].clone();
    let mut updates = client.subscribe();
    // Exercise the real production 15s dispatch budget, with a bounded test wait.
    tokio::time::timeout(Duration::from_secs(20), async {
        while client.status().blob_promotion_health != Some(BlobPromotionHealth::LocalDeadline) {
            assert!(updates.changed().await.is_some());
        }
    })
    .await
    .unwrap();
    assert_eq!(client.status().blob_promotions, 0);
    assert!(client.outgoing(&id).await.is_err());
    assert!(client.conversation(CHAT).await.unwrap().is_some());
    rpc.acquire_released.store(true, Ordering::SeqCst);
    rpc.acquire_release.notify_one();
    stored(&client, &id).await;
    owner.shutdown().await.unwrap();
    assert!(
        rpc.acquire_requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request == &first)
    );
    assert_eq!(
        body.ids.lock().unwrap().as_slice(),
        std::slice::from_ref(&id)
    );
    assert_eq!(rpc.send_requests.lock().unwrap().len(), 1);
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn an_unrelated_pending_file_does_not_block_established_text_delivery() {
    let (f, _, _) = prepared().await;
    let file = uuid::Uuid::new_v4().to_string();
    stage(&f.alice, &file).await;
    let rpc = Arc::new(Probe::default());
    rpc.send_accept.store(true, Ordering::SeqCst);
    let body = Arc::new(BodyProbe {
        block: AtomicBool::new(true),
        ..Default::default()
    });
    let owner = launch_upload(f.alice, &rpc, &body);
    let client = owner.client();
    entered(&body.started).await;
    let text = uuid::Uuid::new_v4().to_string();
    client
        .send_text(CHAT, &text, "text while unrelated upload is waiting")
        .await
        .unwrap();
    stored(&client, &text).await;
    assert!(body.owns_source.load(Ordering::SeqCst));
    assert_eq!(*rpc.send_body_owned.lock().unwrap(), vec![true]);
    assert_eq!(
        rpc.send_requests.lock().unwrap()[0]
            .envelope
            .as_ref()
            .unwrap()
            .message_id,
        text
    );
    owner.shutdown().await.unwrap();
    assert!(!body.owns_source.load(Ordering::SeqCst));
    assert_eq!(rpc.active.load(Ordering::SeqCst), 0);
    f.bob.shutdown().await.unwrap();
}
