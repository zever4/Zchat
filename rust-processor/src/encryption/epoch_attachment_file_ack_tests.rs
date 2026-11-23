use super::*;
use crate::encryption::native::EpochObservation;

#[tokio::test]
async fn file_ack_native_client_refuses_foreign_account_generation_and_origin_before_rpc() {
    use crate::encryption::native::{NativeEpochClient, RpcError};
    let (f, a, b) = prepared().await;
    let id = save(
        &f.bob,
        &f.alice,
        &a,
        &b,
        b"actual protected proof, no synthetic constructor",
    )
    .await;
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    let before = records(f.root.path(), B);
    let mut other_generation = b.own.clone();
    other_generation.generation_id = uuid::Uuid::new_v4().to_string();
    for (scope, own) in [
        (proof.store_scope().clone(), a.own.clone()),
        (proof.store_scope().clone(), other_generation),
        (
            StoreScope::new("https://another.example.test", B).unwrap(),
            b.own.clone(),
        ),
    ] {
        let mut client = NativeEpochClient::test_metadata_transport(scope, own);
        assert!(matches!(
            client.acknowledge_files_prepared(&proof).await,
            Err(RpcError::Invalid)
        ));
    }
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(
        f.bob.pending_epoch_file_acks(None).await.unwrap().messages,
        vec![id]
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

fn response(p: &EpochDurableFileAck, at: i64) -> v2::FileAcknowledgements {
    v2::FileAcknowledgements {
        files: vec![v2::FileAcknowledgementReceipt {
            acknowledgement: Some(p.request().files[0].clone()),
            acknowledged_at_unix_ms: at,
        }],
    }
}
// Synthetic trusted server response, NOT a real authenticated FILEACK RPC.
fn observation(
    p: &EpochDurableFileAck,
    at: i64,
) -> EpochObservation<EpochDurableFileAck, v2::FileAcknowledgements> {
    EpochObservation::test_observation(p.store_scope().clone(), p.clone(), response(p, at))
}
async fn save(
    w: &CryptoCoordinator,
    a: &CryptoCoordinator,
    sa: &EpochScope,
    sb: &EpochScope,
    bytes: &[u8],
) -> String {
    let (p, file) = message(sa, bytes);
    let uuid = p.message_id.clone();
    receive(a, w, sa, sb, p).await;
    let plan = w
        .epoch_attachment_download_plan(uuid.clone())
        .await
        .unwrap();
    let mut run = begin(w, plan).await;
    append(w, &mut run, file.blob()).await.unwrap();
    finish(w, run).await.unwrap();
    uuid
}

#[tokio::test]
async fn file_ack_pending_survives_reopen_exact_proof_and_server_receipt_replay() {
    let (f, a, b) = prepared().await;
    let uuid = save(&f.bob, &f.alice, &a, &b, b"durable pending ACK").await;
    let before = records(f.root.path(), B);
    let proof = f.bob.epoch_file_ack(uuid.clone()).await.unwrap();
    assert_eq!(proof.message_id(), uuid);
    assert_eq!(proof.acknowledged_at_unix_ms(), None);
    assert_eq!(
        f.bob.pending_epoch_file_acks(None).await.unwrap().messages,
        vec![uuid.clone()]
    );
    assert_eq!(records(f.root.path(), B), before); // Read/proof creation never changes Signal/journal.
    f.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    let reloaded = bob.epoch_file_ack(uuid.clone()).await.unwrap();
    assert_eq!(reloaded.request(), proof.request()); // Lost RPC reply: no new identity/UUID/hash.
    const AT: i64 = 1_700_000_000_123;
    bob.accept_epoch_file_ack(observation(&proof, AT))
        .await
        .unwrap(); // Old snapshot, same durable run.
    let settled = records(f.root.path(), B);
    bob.accept_epoch_file_ack(observation(&reloaded, AT))
        .await
        .unwrap();
    assert_eq!(records(f.root.path(), B), settled); // Exact replay doesn't rewrite first time.
    // Reuse via the download path must not reset a settled ACK to pending.
    let plan = bob
        .epoch_attachment_download_plan(uuid.clone())
        .await
        .unwrap();
    let permit = Arc::new(bob.attachment_permit().unwrap());
    let start = bob
        .request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Begin(
                Box::new(plan),
                permit,
                r,
            )))
        })
        .await
        .unwrap();
    assert!(matches!(start, Start::Cached(_)));
    assert_eq!(records(f.root.path(), B), settled);
    assert!(matches!(
        bob.accept_epoch_file_ack(observation(&proof, AT + 1)).await,
        Err(CryptoError::Conflict)
    ));
    assert!(
        bob.pending_epoch_file_acks(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert_eq!(
        bob.epoch_file_ack(uuid.clone())
            .await
            .unwrap()
            .acknowledged_at_unix_ms(),
        Some(AT)
    );
    let cache = bob.cached_epoch_attachment(uuid.clone()).await.unwrap();
    assert_eq!(
        bob.open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        b"durable pending ACK"
    );
    bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own)
        .await
        .unwrap();
    assert_eq!(
        bob.epoch_file_ack(uuid)
            .await
            .unwrap()
            .acknowledged_at_unix_ms(),
        Some(AT)
    );
    assert!(
        bob.pending_epoch_file_acks(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_partial_and_metadata_locator_never_prove_saved_bytes() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a, b"verified bytes required");
    let uuid = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    assert!(matches!(
        f.bob.epoch_file_ack(uuid.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    let plan = f
        .bob
        .epoch_attachment_download_plan(uuid.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    assert!(
        f.bob
            .pending_epoch_file_acks(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert!(matches!(
        f.bob.epoch_file_ack(uuid.clone()).await,
        Err(CryptoError::Busy)
    )); // In-flight run owns buffer permit.
    let cache = finish(&f.bob, run).await.unwrap();
    drop(cache);
    assert!(f.bob.epoch_file_ack("not-a-uuid".into()).await.is_err());
    db(f.root.path(), B)
        .execute(
            "UPDATE protected_attachment_download_chunks SET payload=zeroblob(length(payload))",
            [],
        )
        .unwrap();
    let before = records(f.root.path(), B);
    // Both still expose metadata, so neither can be an ACK proof.
    assert!(f.bob.cached_epoch_attachment(uuid.clone()).await.is_ok());
    assert_eq!(
        f.bob.pending_epoch_file_acks(None).await.unwrap().messages,
        vec![uuid.clone()]
    );
    assert!(matches!(
        f.bob.epoch_file_ack(uuid).await,
        Err(CryptoError::Storage(StoreError::Protection))
    ));
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(chunk_count(f.root.path()), 1);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_rejects_changed_response_scope_and_account_without_mutation() {
    let (f, a, b) = prepared().await;
    let uuid = save(&f.bob, &f.alice, &a, &b, b"exact bound ACK only").await;
    let proof = f.bob.epoch_file_ack(uuid.clone()).await.unwrap();
    let before = records(f.root.path(), B);
    for case in 0..8 {
        let mut r = response(&proof, 1_700_000_000_123);
        match case {
            0 => r.files.clear(),
            1 => r.files[0].acknowledged_at_unix_ms = 0,
            2 => {
                r.files[0]
                    .acknowledgement
                    .as_mut()
                    .unwrap()
                    .binding
                    .as_mut()
                    .unwrap()
                    .envelope_digest[0] ^= 1
            }
            3 => {
                r.files[0]
                    .acknowledgement
                    .as_mut()
                    .unwrap()
                    .blob
                    .as_mut()
                    .unwrap()
                    .sha256[0] ^= 1
            }
            4 => {
                r.files[0]
                    .acknowledgement
                    .as_mut()
                    .unwrap()
                    .binding
                    .as_mut()
                    .unwrap()
                    .epoch += 1
            }
            5 => {
                r.files[0]
                    .acknowledgement
                    .as_mut()
                    .unwrap()
                    .binding
                    .as_mut()
                    .unwrap()
                    .recipient
                    .as_mut()
                    .unwrap()
                    .generation_id = uuid::Uuid::new_v4().to_string()
            }
            6 => r.files[0].acknowledgement = None,
            _ => r.files.push(r.files[0].clone()),
        }
        assert!(
            f.bob
                .accept_epoch_file_ack(EpochObservation::test_observation(
                    proof.store_scope().clone(),
                    proof.clone(),
                    r
                ))
                .await
                .is_err(),
            "case {case}"
        );
    }
    let foreign = StoreScope::new("https://another.example.test", B).unwrap();
    assert!(matches!(
        f.bob
            .accept_epoch_file_ack(EpochObservation::test_observation(
                foreign,
                proof.clone(),
                response(&proof, 1)
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.alice.accept_epoch_file_ack(observation(&proof, 1)).await,
        Err(CryptoError::Conflict)
    ));
    assert!(f.alice.epoch_file_ack(uuid.clone()).await.is_err());
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(
        f.bob.pending_epoch_file_acks(None).await.unwrap().messages,
        vec![uuid]
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_failed_accept_commit_retains_pending_and_exact_retry_keeps_cache() {
    let (f, a, b) = prepared().await;
    let uuid = save(&f.bob, &f.alice, &a, &b, b"ACK COMMIT rollback").await;
    let proof = f.bob.epoch_file_ack(uuid.clone()).await.unwrap();
    let before = records(f.root.path(), B);
    let connection = db(f.root.path(), B);
    connection.execute_batch("CREATE TABLE ack_parent(id INTEGER PRIMARY KEY); CREATE TABLE ack_child(id INTEGER REFERENCES ack_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER ack_fault AFTER UPDATE ON protected_records WHEN NEW.record_id LIKE 'attachment-download:%' BEGIN INSERT INTO ack_child VALUES(1); END;").unwrap();
    assert!(matches!(
        f.bob.accept_epoch_file_ack(observation(&proof, 123)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(
        f.bob.pending_epoch_file_acks(None).await.unwrap().messages,
        vec![uuid.clone()]
    );
    connection.execute_batch("DROP TRIGGER ack_fault").unwrap();
    f.bob
        .accept_epoch_file_ack(observation(&proof, 123))
        .await
        .unwrap();
    assert!(
        f.bob
            .pending_epoch_file_acks(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let cache = f.bob.cached_epoch_attachment(uuid).await.unwrap();
    assert_eq!(
        f.bob
            .open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        b"ACK COMMIT rollback"
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_pages_are_bounded_skip_completed_and_cursor_is_account_scoped() {
    let (f, a, b) = prepared().await;
    let mut ids = Vec::new();
    for i in 0..33 {
        ids.push(save(&f.bob, &f.alice, &a, &b, &[i]).await);
    }
    ids.sort();
    let first = f.bob.pending_epoch_file_acks(None).await.unwrap();
    assert_eq!(first.messages, ids[..32]);
    assert!(first.next.is_some());
    assert!(matches!(
        f.alice.pending_epoch_file_acks(first.next.clone()).await,
        Err(CryptoError::Conflict)
    ));
    let last = f.bob.pending_epoch_file_acks(first.next).await.unwrap();
    assert_eq!(last.messages, ids[32..]);
    assert!(last.next.is_none());
    for uuid in &ids[..32] {
        let proof = f.bob.epoch_file_ack(uuid.clone()).await.unwrap();
        f.bob
            .accept_epoch_file_ack(observation(&proof, 1))
            .await
            .unwrap();
    }
    let first = f.bob.pending_epoch_file_acks(None).await.unwrap();
    assert!(first.messages.is_empty());
    assert!(first.next.is_some()); // Scan budget, not pending count.
    let last = f.bob.pending_epoch_file_acks(first.next).await.unwrap();
    assert_eq!(last.messages, ids[32..]);
    assert!(last.next.is_none());
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn file_ack_unknown_journal_state_or_old_private_format_never_infers_pending() {
    for case in 0..3 {
        let (f, a, b) = prepared().await;
        let uuid = save(&f.bob, &f.alice, &a, &b, b"no silent ACK repair").await;
        f.bob.shutdown().await.unwrap();
        let mut db = store(f.root.path(), B, false);
        db.transaction(|tx| {
            let key = key(&b.own, &uuid).unwrap();
            let mut journal = Journal::decode(tx.get(&key)?.unwrap().as_slice()).unwrap();
            match case {
                0 => journal.file_acknowledged_at_unix_ms = None,
                1 => journal.file_acknowledged_at_unix_ms = Some(-1),
                _ => journal.format = 1,
            }
            tx.put(&key, &journal.encode_to_vec())
        })
        .unwrap();
        drop(db);
        let before = records(f.root.path(), B);
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        assert!(matches!(
            bob.epoch_file_ack(uuid.clone()).await,
            Err(CryptoError::Conflict)
        ));
        assert!(bob.pending_epoch_file_acks(None).await.is_err());
        assert!(bob.cached_epoch_attachment(uuid).await.is_err());
        assert_eq!(records(f.root.path(), B), before);
        assert_eq!(chunk_count(f.root.path()), 1);
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn file_ack_peer_closure_allowed_own_closure_blocks_even_prior_proof() {
    for own_closed in [false, true] {
        let (f, a, b) = prepared().await;
        let uuid = save(&f.bob, &f.alice, &a, &b, b"retained proof ownership").await;
        let proof = f.bob.epoch_file_ack(uuid.clone()).await.unwrap();
        let mut c = super::super::super::incoming::tests::b_conversation(&f.a, &b);
        c.state = ConversationState::Closed as i32;
        c.close_reason = ConversationCloseReason::DeviceReplaced as i32;
        c.closed_at_unix_ms = 1_700_000_000_000;
        c.closed_generation_id = if own_closed {
            b.own.generation_id.clone()
        } else {
            a.own.generation_id.clone()
        };
        index(&f.bob, &c).await;
        let before = records(f.root.path(), B);
        let fresh = f.bob.epoch_file_ack(uuid).await;
        let accept = f.bob.accept_epoch_file_ack(observation(&proof, 1)).await;
        if own_closed {
            assert!(matches!(fresh, Err(CryptoError::ArchivedGeneration)));
            assert!(matches!(accept, Err(CryptoError::ArchivedGeneration)));
            assert_eq!(records(f.root.path(), B), before);
        } else {
            fresh.unwrap();
            accept.unwrap();
        }
        f.bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn file_ack_abandoned_queued_proof_holds_shared_permit_until_worker_drains() {
    let (f, a, b) = prepared().await;
    let uuid = save(&f.bob, &f.alice, &a, &b, b"cancelled proof ownership").await;
    let (release, wait) = std::sync::mpsc::channel();
    let (entered, ready) = tokio::sync::oneshot::channel();
    assert!(
        f.bob
            .sender
            .try_send(Command::Epoch(EpochCommand::AttachmentDownload(
                DownloadCommand::Barrier(wait, entered)
            )))
            .is_ok()
    );
    ready.await.unwrap();
    let permit = Arc::new(f.bob.attachment_permit().unwrap());
    let (reply, abandoned) = tokio::sync::oneshot::channel();
    assert!(
        f.bob
            .sender
            .try_send(Command::Epoch(EpochCommand::AttachmentDownload(
                DownloadCommand::FileAck(file_ack::FileAckCommand::Prepare(
                    uuid.clone(),
                    permit,
                    reply
                ))
            )))
            .is_ok()
    );
    drop(abandoned);
    assert!(matches!(f.bob.attachment_permit(), Err(CryptoError::Busy)));
    release.send(()).unwrap();
    f.bob.join_attachment_work().await.unwrap();
    assert!(f.bob.attachment_permit().is_ok());
    assert_eq!(
        f.bob.pending_epoch_file_acks(None).await.unwrap().messages,
        vec![uuid]
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}
