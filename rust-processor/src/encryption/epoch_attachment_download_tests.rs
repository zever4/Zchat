use super::super::super::tests::store;
use super::super::incoming::tests::{accepted, delivery, prepared};
use super::super::tests::{index, records, rewrite_frozen};
use super::*;
use crate::encryption::attachment_payload::{AttachmentDraft, PAYLOAD_FORMAT};
use crate::encryption::native::NativeBlobHttp;
use crate::proto::e2ee::{ConversationCloseReason, ConversationState};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
const B: &str = "22222222-2222-4222-8222-222222222222";
#[path = "epoch_attachment_removal_tests.rs"]
mod removal_tests;

fn message(s: &EpochScope, bytes: &[u8]) -> (v2::EncryptedPayload, attachment::PreparedAttachment) {
    let mut p = super::super::incoming::tests::payload(s, "");
    let blob = uuid::Uuid::new_v4().to_string();
    let binding = AttachmentBinding::new(
        &p.conversation_id,
        s.epoch,
        &p.message_id,
        &blob,
        &s.own,
        &s.peer,
    )
    .unwrap();
    let file = attachment::seal(&binding, Zeroizing::new(bytes.to_vec())).unwrap();
    let draft = AttachmentDraft {
        kind: v2::AttachmentKind::File,
        filename: Zeroizing::new("private-cache-name.bin".into()),
        mime_type: Zeroizing::new("application/octet-stream".into()),
        caption: Zeroizing::new("private-cache-caption".into()),
        photo_metadata_stripped: None,
    };
    p.format_version = PAYLOAD_FORMAT;
    p.content = Some(v2::encrypted_payload::Content::Attachment(
        draft.into_message(blob, file.descriptor()),
    ));
    (p, file)
}
async fn receive(
    w: &CryptoCoordinator,
    receiver: &CryptoCoordinator,
    a: &EpochScope,
    b: &EpochScope,
    p: v2::EncryptedPayload,
) -> EpochDurableAck {
    let plan = w.encrypt_epoch(a.clone(), p).await.unwrap();
    let e = plan.request().envelope.clone().unwrap();
    let d = v2::Delivery {
        sequence: 1,
        envelope_digest: data::envelope_digest(&e).unwrap().to_vec(),
        envelope: Some(e),
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
    };
    receiver
        .receive_epoch(b.clone(), delivery(b, &d))
        .await
        .unwrap()
        .1
}
pub(in crate::encryption::signal_state::epoch_state) async fn begin(
    w: &CryptoCoordinator,
    plan: EpochAttachmentDownloadPlan,
) -> Run {
    let permit = Arc::new(w.attachment_permit().unwrap());
    match w
        .request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Begin(
                Box::new(plan),
                permit,
                r,
            )))
        })
        .await
        .unwrap()
    {
        Start::Download(r) => r,
        Start::Cached(_) => panic!("expected incomplete staging"),
    }
}
pub(in crate::encryption::signal_state::epoch_state) async fn append(
    w: &CryptoCoordinator,
    run: &mut Run,
    bytes: &[u8],
) -> CryptoResult<()> {
    let (offset, _) = w
        .request(|r| {
            Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Append(
                Box::new(run.clone()),
                BlobCiphertextChunk::test_chunk(bytes),
                r,
            )))
        })
        .await?;
    run.offset = offset;
    Ok(())
}
pub(in crate::encryption::signal_state::epoch_state) async fn finish(
    w: &CryptoCoordinator,
    run: Run,
) -> CryptoResult<EpochAttachmentCache> {
    let response = BlobGetResponse::test_complete(run.plan.read_request());
    w.request(|r| {
        Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Finish(
            Box::new(run),
            Box::new(response),
            BlobCiphertextChunk::test_chunk(b"last"),
            r,
        )))
    })
    .await
}
fn db(root: &std::path::Path, user: &str) -> rusqlite::Connection {
    rusqlite::Connection::open(
        StoreScope::new("https://chat.example.test", user)
            .unwrap()
            .database_path(root),
    )
    .unwrap()
}
fn chunk_count(root: &std::path::Path) -> i64 {
    db(root, B)
        .query_row(
            "SELECT count(*) FROM protected_attachment_download_chunks",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[tokio::test]
async fn saved_incoming_only_and_key_free_plan_survives_descriptor_ack_and_reopen() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a, b"protected cache plaintext marker");
    let uuid = p.message_id.clone();
    assert!(matches!(
        f.bob.epoch_attachment_download_plan(uuid.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    let ack = receive(&f.alice, &f.bob, &a, &b, p.clone()).await;
    assert!(matches!(
        f.alice.epoch_attachment_download_plan(uuid.clone()).await,
        Err(CryptoError::Conflict)
    ));
    let plan = f
        .bob
        .epoch_attachment_download_plan(uuid.clone())
        .await
        .unwrap();
    assert_eq!(
        plan.read_request().blob().sha256,
        file.descriptor().blob_digest()
    );
    f.bob.accept_epoch_ack(accepted(&ack)).await.unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    let cache = finish(&f.bob, run).await.unwrap();
    assert!(f.bob.epoch_ack(uuid.clone()).await.unwrap().acknowledged());
    let body = f.bob.open_cached_epoch_attachment(cache).await.unwrap();
    assert_eq!(body.as_bytes(), b"protected cache plaintext marker");
    assert!(matches!(f.bob.attachment_permit(), Err(CryptoError::Busy)));
    drop(body);
    f.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    let plan = bob.epoch_attachment_download_plan(uuid).await.unwrap();
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
    let Start::Cached(cache) = start else {
        panic!("cache must survive reopen")
    };
    assert_eq!(
        bob.open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        b"protected cache plaintext marker"
    );
    let sqlite = std::fs::read(
        StoreScope::new("https://chat.example.test", B)
            .unwrap()
            .database_path(f.root.path()),
    )
    .unwrap();
    for secret in [
        b"protected cache plaintext marker".as_slice(),
        b"private-cache-name.bin",
        b"private-cache-caption",
        part(&p).unwrap().private_descriptor.as_slice(),
    ] {
        assert!(!sqlite.windows(secret.len()).any(|w| w == secret));
    }
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn fragmented_staging_restarts_with_new_run_and_rejects_stale_cas() {
    let (f, a, b) = prepared().await;
    let original = vec![42; 600 * 1024];
    let (p, file) = message(&a, &original);
    let uuid = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f
        .bob
        .epoch_attachment_download_plan(uuid.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan.clone()).await;
    append(&f.bob, &mut run, &file.blob()[..17]).await.unwrap();
    assert!(matches!(
        finish(&f.bob, run.clone()).await,
        Err(CryptoError::Invalid)
    ));
    assert!(matches!(f.bob.attachment_permit(), Err(CryptoError::Busy)));
    drop(run);
    f.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    let mut run = begin(&bob, plan).await;
    assert_eq!(chunk_count(f.root.path()), 0);
    let stale = run.clone();
    for bytes in file.blob().chunks(100_003) {
        append(&bob, &mut run, bytes).await.unwrap();
    }
    assert_eq!(chunk_count(f.root.path()), 3);
    assert!(matches!(
        finish(&bob, stale).await,
        Err(CryptoError::Conflict)
    ));
    let cache = finish(&bob, run).await.unwrap();
    assert_eq!(
        bob.open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        original
    );
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn incoming_plan_rejects_foreign_account_origin_and_generation_before_staging() {
    let (f, a, b) = prepared().await;
    let (p, _) = message(&a, b"private");
    let uuid = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f.bob.epoch_attachment_download_plan(uuid).await.unwrap();
    for kind in [0, 1, 2] {
        let mut foreign = plan.clone();
        match kind {
            0 => foreign.own = a.own.clone(),
            1 => foreign.store = StoreScope::new("https://other.example.test", B).unwrap(),
            _ => foreign.own.generation_id = uuid::Uuid::new_v4().to_string(),
        }
        let permit = Arc::new(f.bob.attachment_permit().unwrap());
        let result = f
            .bob
            .request(|r| {
                Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Begin(
                    Box::new(foreign),
                    permit,
                    r,
                )))
            })
            .await;
        assert!(matches!(result, Err(CryptoError::Conflict)));
    }
    assert_eq!(
        db(f.root.path(), B)
            .query_row::<i64, _, _>(
                "SELECT count(*) FROM protected_attachment_downloads",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        0
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn ciphertext_tampering_never_publishes_cache_or_changes_descriptor_ack() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a, b"private cache integrity");
    let uuid = p.message_id.clone();
    let ack = receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f
        .bob
        .epoch_attachment_download_plan(uuid.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    db(f.root.path(), B)
        .execute(
            "UPDATE protected_attachment_download_chunks SET payload=zeroblob(length(payload))",
            [],
        )
        .unwrap();
    let before = records(f.root.path(), B);
    assert!(matches!(
        finish(&f.bob, run).await,
        Err(CryptoError::Storage(StoreError::Protection))
    ));
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(
        f.bob.epoch_ack(uuid).await.unwrap().request(),
        ack.request()
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn valid_hash_with_wrong_aead_binding_cannot_mint_verified_cache() {
    let (f, a, b) = prepared().await;
    let (mut p, prepared_file) = message(&a, b"wrong binding");
    // Authenticated Signal data can still contain a malformed file. The blob
    // and descriptor match SHA/size, but were sealed under another message AAD.
    p.message_id = uuid::Uuid::new_v4().to_string();
    let uuid = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f.bob.epoch_attachment_download_plan(uuid).await.unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, prepared_file.blob())
        .await
        .unwrap();
    let before = records(f.root.path(), B);
    assert!(matches!(
        finish(&f.bob, run).await,
        Err(CryptoError::Invalid)
    ));
    assert_eq!(records(f.root.path(), B), before);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn peer_closure_and_send_freeze_do_not_gate_saved_files_but_own_closure_does() {
    for own_closed in [false, true] {
        let (f, a, b) = prepared().await;
        let (p, file) = message(&a, b"retained attachment");
        let uuid = p.message_id.clone();
        receive(&f.alice, &f.bob, &a, &b, p).await;
        let plan = f.bob.epoch_attachment_download_plan(uuid).await.unwrap();
        let mut c = super::super::incoming::tests::b_conversation(&f.a, &b);
        f.bob.shutdown().await.unwrap();
        rewrite_frozen(f.root.path(), &c, true);
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        let mut run = begin(&bob, plan).await;
        c.state = ConversationState::Closed as i32;
        c.close_reason = ConversationCloseReason::DeviceReplaced as i32;
        c.closed_at_unix_ms = 1_700_000_000_000;
        c.closed_generation_id = if own_closed {
            b.own.generation_id.clone()
        } else {
            a.own.generation_id.clone()
        };
        index(&bob, &c).await;
        let result = append(&bob, &mut run, file.blob()).await;
        if own_closed {
            assert!(matches!(result, Err(CryptoError::ArchivedGeneration)));
        } else {
            result.unwrap();
            let cache = finish(&bob, run).await.unwrap();
            assert_eq!(
                bob.open_cached_epoch_attachment(cache)
                    .await
                    .unwrap()
                    .as_bytes(),
                b"retained attachment"
            );
        }
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn archived_incoming_cache_reads_own_closed_history_without_network_or_ack_authority() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a, b"saved before own closure");
    let id = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f
        .bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    finish(&f.bob, run).await.unwrap();
    let mut closed = super::super::incoming::tests::b_conversation(&f.a, &b);
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_at_unix_ms = 1_700_000_000_000;
    closed.closed_generation_id = b.own.generation_id.clone();
    index(&f.bob, &closed).await;
    let before = records(f.root.path(), B);
    assert!(matches!(
        f.bob.epoch_attachment_download_plan(id.clone()).await,
        Err(CryptoError::ArchivedGeneration)
    ));
    assert!(f.bob.epoch_file_ack(id.clone()).await.is_err());
    assert_eq!(
        f.bob
            .open_archived_epoch_attachment(
                b.own.generation_id.clone(),
                closed.conversation_id.clone(),
                id
            )
            .await
            .unwrap()
            .as_bytes(),
        b"saved before own closure"
    );
    assert_eq!(records(f.root.path(), B), before);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn native_get_streams_real_signal_attachment_into_verified_sqlite_and_reuses_it_offline() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (f, a, b) = super::super::incoming::tests::prepared_at(&origin).await;
    let plaintext = vec![42; 600 * 1024];
    let (p, file) = message(&a, &plaintext);
    let uuid = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let body = file.blob().to_vec();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            let mut buf = [0; 1024];
            let n = socket.read(&mut buf).await.unwrap();
            assert!(n > 0 && headers.len() + n < 8192);
            headers.extend(&buf[..n]);
        }
        let headers = String::from_utf8(headers).unwrap().to_ascii_lowercase();
        assert!(headers.starts_with("get /api/e2ee/attachments/"));
        assert!(headers.contains("authorization: bearer synthetic-jwt"));
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nCache-Control: private, no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Disposition: attachment; filename=\"encrypted.bin\"\r\nAccept-Ranges: none\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
        for chunk in body.chunks(100_003) {
            socket.write_all(chunk).await.unwrap();
        }
        socket.shutdown().await.unwrap();
    });
    let mut http = NativeBlobHttp::test_reader(&origin, b.own.clone());
    let plan = f.bob.epoch_attachment_download_plan(uuid).await.unwrap();
    let cache = f
        .bob
        .download_epoch_attachment(&mut http, plan.clone())
        .await
        .unwrap();
    server.await.unwrap();
    http.close().await;
    assert_eq!(
        f.bob
            .open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        plaintext
    );
    // A stopped transport is intentionally unusable: this proves the cache
    // branch rechecks locally and makes no second GET or network assertion.
    let cache = f
        .bob
        .download_epoch_attachment(&mut http, plan)
        .await
        .unwrap();
    assert_eq!(
        f.bob
            .open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        plaintext
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_final_waiter_keeps_http_slot_until_queued_cache_work_is_discarded() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (f, a, b) = super::super::incoming::tests::prepared_at(&origin).await;
    let (p, file) = message(&a, b"LAST queued cache owner");
    let id = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let body = file.blob().to_vec();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut tail = Vec::new();
        while !tail.ends_with(b"\r\n\r\n") {
            let mut one = [0];
            assert_eq!(socket.read(&mut one).await.unwrap(), 1);
            tail.extend(one);
            assert!(tail.len() < 8192);
        }
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/octet-stream\r\nCache-Control: private, no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Disposition: attachment; filename=\"encrypted.bin\"\r\nAccept-Ranges: none\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
        socket.write_all(&body).await.unwrap();
    });
    let mut http = NativeBlobHttp::test_reader(&origin, b.own.clone());
    let plan = f.bob.epoch_attachment_download_plan(id).await.unwrap();
    let run = begin(&f.bob, plan.clone()).await;
    let mut sink = Sink {
        worker: &f.bob,
        run,
        last: None,
        error: None,
    };
    let response = http.get(plan.read_request(), &mut sink).await.unwrap();
    server.await.unwrap();
    // A deterministic worker barrier avoids relying on slow disk/timing to
    // prove that an abandoned queued command still owns LAST chunk/admission.
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
    let (reply, abandoned) = tokio::sync::oneshot::channel();
    assert!(
        f.bob
            .sender
            .try_send(Command::Epoch(EpochCommand::AttachmentDownload(
                DownloadCommand::Finish(
                    Box::new(sink.run.clone()),
                    Box::new(response),
                    sink.last.take().unwrap(),
                    reply
                )
            )))
            .is_ok()
    );
    drop(abandoned);
    drop(sink);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(30), http.close())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), http.close())
        .await
        .unwrap();
    // Queue fence ensures all command resources have dropped before admission.
    f.bob
        .epoch_attachment_download_plan(plan.message_id().into())
        .await
        .unwrap();
    let run = begin(&f.bob, plan).await;
    assert_eq!(run.offset, 0); // Cancelled-before-dispatch Finish did not COMMIT.
    drop(run);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_saved_history_or_envelope_never_grants_download_authority() {
    for kind in [RecordKind::History, RecordKind::Inbox] {
        let (f, a, b) = prepared().await;
        let (p, _) = message(&a, b"saved Signal cohort");
        let uuid = p.message_id.clone();
        receive(&f.alice, &f.bob, &a, &b, p).await;
        db(f.root.path(), B)
            .execute(
                "UPDATE protected_records SET payload=zeroblob(length(payload)) WHERE kind=?1",
                [kind as u8],
            )
            .unwrap();
        let before = records(f.root.path(), B);
        assert!(f.bob.epoch_attachment_download_plan(uuid).await.is_err());
        assert_eq!(records(f.root.path(), B), before);
        assert_eq!(chunk_count(f.root.path()), 0);
        f.bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn verified_cache_tamper_is_reported_without_eviction_or_automatic_redownload() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a, b"immutable verified cache");
    let uuid = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f.bob.epoch_attachment_download_plan(uuid).await.unwrap();
    let mut run = begin(&f.bob, plan.clone()).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    let cache = finish(&f.bob, run).await.unwrap();
    db(f.root.path(), B)
        .execute(
            "UPDATE protected_attachment_download_chunks SET payload=zeroblob(length(payload))",
            [],
        )
        .unwrap();
    let before = records(f.root.path(), B);
    assert!(matches!(
        f.bob.open_cached_epoch_attachment(cache).await,
        Err(CryptoError::Storage(StoreError::Protection))
    ));
    let permit = Arc::new(f.bob.attachment_permit().unwrap());
    assert!(matches!(
        f.bob
            .request(
                |r| Command::Epoch(EpochCommand::AttachmentDownload(DownloadCommand::Begin(
                    Box::new(plan),
                    permit,
                    r
                )))
            )
            .await,
        Err(CryptoError::Storage(StoreError::Protection))
    ));
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(chunk_count(f.root.path()), 1);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_final_commit_cannot_publish_cache_and_exact_retry_does_not_touch_signal() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a, b"durable verified cache");
    let uuid = p.message_id.clone();
    receive(&f.alice, &f.bob, &a, &b, p).await;
    let plan = f.bob.epoch_attachment_download_plan(uuid).await.unwrap();
    let mut run = begin(&f.bob, plan).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    let before = records(f.root.path(), B);
    let connection = db(f.root.path(), B);
    connection.execute_batch("CREATE TABLE cache_parent(id INTEGER PRIMARY KEY); CREATE TABLE cache_child(id INTEGER REFERENCES cache_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER cache_fault AFTER UPDATE ON protected_records WHEN NEW.record_id LIKE 'attachment-download:%' BEGIN INSERT INTO cache_child VALUES(1); END;").unwrap();
    assert!(matches!(
        finish(&f.bob, run.clone()).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    connection
        .execute_batch("DROP TRIGGER cache_fault")
        .unwrap();
    let cache = finish(&f.bob, run).await.unwrap();
    assert_eq!(
        f.bob
            .open_cached_epoch_attachment(cache)
            .await
            .unwrap()
            .as_bytes(),
        b"durable verified cache"
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[test]
#[ignore = "explicit process-exit fixture; run by cache_commit_process_exit test"]
fn download_cache_crash_child() {
    let root = std::path::PathBuf::from(std::env::var_os("ZCHAT_CACHE_CRASH_ROOT").unwrap());
    let uuid = std::env::var("ZCHAT_CACHE_CRASH_MESSAGE").unwrap();
    let after = std::env::var("ZCHAT_CACHE_CRASH_AFTER").unwrap() == "1";
    let own = DeviceRef {
        user_id: B.into(),
        device_id: 1,
        generation_id: "44444444-4444-4444-8444-444444444444".into(),
    };
    let mut engine = Engine {
        store: store(&root, B, false),
        own: own.clone(),
        runtime: tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    };
    let scope = engine.store.scope().clone();
    let plan = engine
        .store
        .transaction_with(|tx| snapshot(tx, &own, &scope, &uuid).map(|(p, _)| p))
        .unwrap();
    let permit = Arc::new(
        Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .unwrap(),
    );
    let Start::Download(mut run) = engine.begin_download(plan, permit).unwrap() else {
        panic!("unexpected existing cache")
    };
    let bytes = std::fs::read(root.join("download-crash.ciphertext")).unwrap();
    for chunk in bytes.chunks(256 * 1024) {
        run.offset = engine.append_download(&run, chunk).unwrap();
    }
    engine
        .store
        .test_exit_on_record(key(&own, &uuid).unwrap(), after);
    let response = BlobGetResponse::test_complete(run.plan.read_request());
    let _ = engine.finish_download(run, response);
    panic!("cache COMMIT hook did not exit");
}

#[tokio::test]
async fn cache_commit_process_exit_before_and_after_recovers_unverified_or_verified_without_lost_ack()
 {
    for after in [false, true] {
        let (f, a, b) = prepared().await;
        let (p, file) = message(&a, b"process exit cache proof");
        let uuid = p.message_id.clone();
        let ack = receive(&f.alice, &f.bob, &a, &b, p).await;
        f.bob.shutdown().await.unwrap();
        std::fs::write(f.root.path().join("download-crash.ciphertext"), file.blob()).unwrap();
        let output=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored","--exact","encryption::signal_state::epoch_state::attachment_download::tests::download_cache_crash_child","--nocapture"])
            .env("ZCHAT_CACHE_CRASH_ROOT",f.root.path()).env("ZCHAT_CACHE_CRASH_MESSAGE",&uuid)
            .env("ZCHAT_CACHE_CRASH_AFTER",if after {"1"}else{"0"}).output().unwrap();
        assert_eq!(
            output.status.code(),
            Some(if after { 74 } else { 73 }),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let mut saved = store(f.root.path(), B, false);
        let j = saved
            .transaction(|tx| tx.get(&key(&b.own, &uuid).unwrap()))
            .unwrap()
            .unwrap();
        let j = Journal::decode(j.as_slice()).unwrap();
        assert_eq!(j.verified, Some(after));
        assert_eq!(j.file_acknowledged_at_unix_ms, after.then_some(0));
        assert_eq!(j.offset, Some(file.blob().len() as u64));
        let bob = CryptoCoordinator::start(saved, b.own.clone())
            .await
            .unwrap();
        assert_eq!(
            bob.epoch_ack(uuid.clone()).await.unwrap().request(),
            ack.request()
        );
        let due = bob.pending_epoch_file_acks(None).await.unwrap();
        assert_eq!(
            due.messages,
            if after { vec![uuid.clone()] } else { vec![] }
        );
        if !after {
            assert!(bob.epoch_file_ack(uuid.clone()).await.is_err());
        }
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
        let cache = match start {
            Start::Cached(c) => {
                assert!(after);
                c
            }
            Start::Download(mut run) => {
                assert!(!after);
                assert_eq!(chunk_count(f.root.path()), 0);
                append(&bob, &mut run, file.blob()).await.unwrap();
                finish(&bob, run).await.unwrap()
            }
        };
        assert_eq!(
            bob.open_cached_epoch_attachment(cache)
                .await
                .unwrap()
                .as_bytes(),
            b"process exit cache proof"
        );
        assert_eq!(
            bob.epoch_file_ack(uuid)
                .await
                .unwrap()
                .acknowledged_at_unix_ms(),
            None
        );
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[path = "epoch_attachment_file_ack_tests.rs"]
mod file_ack_tests;
