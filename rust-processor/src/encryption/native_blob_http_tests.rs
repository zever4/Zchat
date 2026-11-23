use super::*;
use crate::proto::{e2ee as common, e2ee_v2 as v2};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn request(bytes: &[u8]) -> v2::BlobLeaseRequest {
    v2::BlobLeaseRequest {
        context: Some(common::ClientContext {
            protocol_version: 2,
            device_generation_id: "33333333-3333-4333-8333-333333333333".into(),
        }),
        message_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".into(),
        conversation_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb".into(),
        crypto_epoch: Some(0),
        sender: Some(common::DeviceRef {
            user_id: "11111111-1111-4111-8111-111111111111".into(),
            generation_id: "33333333-3333-4333-8333-333333333333".into(),
            device_id: 1,
        }),
        recipient: Some(common::DeviceRef {
            user_id: "22222222-2222-4222-8222-222222222222".into(),
            generation_id: "44444444-4444-4444-8444-444444444444".into(),
            device_id: 1,
        }),
        blob: Some(v2::BlobRef {
            format_version: 1,
            blob_id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc".into(),
            ciphertext_bytes: bytes.len() as u64,
            sha256: Sha256::digest(bytes).to_vec(),
        }),
        operation_id: "dddddddd-dddd-4ddd-8ddd-dddddddddddd".into(),
        expected_revision: 0,
        expected_attempt_id: String::new(),
    }
}
fn grant(r: &v2::BlobLeaseRequest) -> v2::BlobLease {
    v2::BlobLease {
        message_id: r.message_id.clone(),
        operation_id: r.operation_id.clone(),
        attempt_id: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee".into(),
        revision: 1,
        reserved_at_unix_ms: 1000,
        expires_at_unix_ms: 3_601_000,
        observed_at_unix_ms: 1000,
        state: v2::BlobLeaseState::Reserved as i32,
        expired: false,
    }
}
fn upload(origin: &str, n: usize) -> EpochBlobUpload {
    let bytes = vec![42; n];
    let r = request(&bytes);
    let g = grant(&r);
    let scope = StoreScope::new(origin, &r.sender.as_ref().unwrap().user_id).unwrap();
    EpochBlobUpload::test_upload(EpochBlobControlPlan::test_plan(scope, r), g, bytes)
}
fn epoch_client(origin: &str) -> super::super::NativeEpochClient {
    let own = request(&[42; 64]).sender.unwrap();
    let scope = StoreScope::new(origin, &own.user_id).unwrap();
    let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    super::super::NativeEpochClient {
        client: v2::messenger_client::MessengerClient::new(channel),
        own,
        scope,
        jwt: Zeroizing::new("synthetic-jwt".into()),
        credential: Zeroizing::new([37; 32]),
    }
}
fn client(origin: &str) -> NativeBlobHttp {
    epoch_client(origin)
        .blob_transport(BlobHttpEndpoint::new(origin, None).unwrap())
        .unwrap()
}
#[tokio::test]
async fn upload_adapter_rejects_account_generation_credentials_and_origin_mixups_before_rpc() {
    use crate::encryption::blob_upload::EpochBlobUploadTransport;
    let origin = "http://127.0.0.1:1";
    for mismatch in 0..5 {
        let mut rpc = epoch_client(origin);
        let mut http = client(origin);
        match mismatch {
            0 => {
                http.scope =
                    StoreScope::new("https://other.example.test", &http.own.user_id).unwrap()
            }
            1 => http.own.user_id = "22222222-2222-4222-8222-222222222222".into(),
            2 => http.own.generation_id = "44444444-4444-4444-8444-444444444444".into(),
            3 => *http.jwt = "different-synthetic-token".into(),
            _ => http.credential[0] ^= 1,
        }
        assert!(matches!(
            rpc.upload_transport(&mut http),
            Err(BlobHttpError::Invalid)
        ));
    }
    let mut rpc = epoch_client(origin);
    let mut http = client(origin);
    let plan = upload(origin, 64).plan().clone();
    let mut transport = rpc.upload_transport(&mut http).unwrap();
    transport.cancel();
    assert!(matches!(
        transport.observe(&plan).await,
        Err(RpcError::Status(Code::Cancelled))
    ));
    assert!(matches!(
        transport.reserve(&plan).await,
        Err(RpcError::Status(Code::Cancelled))
    ));
    transport.join().await;
}
async fn listener() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", l.local_addr().unwrap());
    (l, origin)
}
async fn headers(s: &mut TcpStream) -> (String, Vec<u8>, usize) {
    let mut data = Vec::new();
    let split = loop {
        let mut part = [0; 4096];
        let n = s.read(&mut part).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&part[..n]);
        assert!(data.len() < 16384);
        if let Some(p) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let text = String::from_utf8(data[..split].to_vec()).unwrap();
    let len = text
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    (text, data[split..].to_vec(), len)
}
async fn capture(s: &mut TcpStream) -> (String, Vec<u8>) {
    let (h, mut bytes, len) = headers(s).await;
    let initial = bytes.len();
    bytes.resize(len, 0);
    s.read_exact(&mut bytes[initial..]).await.unwrap();
    (h, bytes)
}
#[test]
fn upload_header_matches_shared_go_vector_without_float_counter_loss() {
    let vector: serde_json::Value = serde_json::from_str(include_str!(
        "../../../proto/fixtures/attachment-upload-http-v1.json"
    ))
    .unwrap();
    let json: serde_json::Value =
        serde_json::from_str(vector["canonical_metadata_json"].as_str().unwrap()).unwrap();
    let mut r = request(&[42; 37]);
    r.crypto_epoch = Some(json["crypto_epoch"].as_str().unwrap().parse().unwrap());
    r.blob.as_mut().unwrap().sha256 = json["sha256"]
        .as_str()
        .unwrap()
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
        .collect();
    let g = grant(&r);
    assert_eq!(
        metadata(&r, &g).unwrap(),
        vector["metadata_header"].as_str().unwrap()
    );
    let mut bad = r;
    bad.expected_revision = 1;
    assert!(metadata(&bad, &g).is_err());
}
#[tokio::test]
async fn put_sends_exact_owned_ciphertext_length_and_sensitive_headers() {
    let (l, origin) = listener().await;
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let (h, b) = capture(&mut s).await;
        assert!(h.starts_with("PUT /api/e2ee/blobs/eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee HTTP/1.1"));
        let h = h.to_ascii_lowercase();
        assert!(h.contains("authorization: bearer synthetic-jwt\r\n"));
        assert!(h.contains("x-zchat-device-credential: "));
        assert!(
            !h.contains("transfer-encoding:") && !h.contains("cookie:") && !h.contains("origin:")
        );
        assert_eq!(b, vec![42; CHUNK + 37]);
        s.write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            .await
            .unwrap();
    });
    let mut c = client(&origin);
    assert_eq!(
        c.put(upload(&origin, CHUNK + 37)).await.unwrap().status(),
        201
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), c.close())
        .await
        .unwrap();
    assert_eq!(c.slots.available_permits(), 1);
    server.await.unwrap();
}
#[tokio::test]
async fn redirects_and_oversized_decoded_responses_are_rejected_without_retry() {
    let mut chunked = format!(
        "HTTP/1.1 201 Created\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n",
        MAX_REPLY + 1
    )
    .into_bytes();
    chunked.extend(vec![42; MAX_REPLY + 1]);
    chunked.extend(b"\r\n0\r\n\r\n");
    let redirect = b"HTTP/1.1 307 Temporary Redirect\r\nLocation: http://127.0.0.1:1/leak\r\nContent-Length: 0\r\n\r\n".to_vec();
    let oversized = format!(
        "HTTP/1.1 201 Created\r\nContent-Length: {}\r\n\r\n",
        MAX_REPLY + 1
    )
    .into_bytes();
    for (reply, expected) in [
        (redirect, BlobHttpError::Redirect),
        (oversized, BlobHttpError::ResponseTooLarge),
        (chunked, BlobHttpError::ResponseTooLarge),
    ] {
        let (l, origin) = listener().await;
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let _ = capture(&mut s).await;
            s.write_all(&reply).await.unwrap();
        });
        let mut c = client(&origin);
        assert_eq!(c.put(upload(&origin, 64)).await.err(), Some(expected));
        c.close().await;
        server.await.unwrap();
    }
}
#[tokio::test]
async fn missing_response_is_unknown_and_remote_failure_is_not_a_receipt() {
    for lost in [true, false] {
        let (l, origin) = listener().await;
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let _ = capture(&mut s).await;
            if !lost {
                s.write_all(b"HTTP/1.1 507 Insufficient Storage\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
            }
        });
        let mut c = client(&origin);
        let result = c.put(upload(&origin, 64)).await;
        if lost {
            assert!(matches!(result, Err(BlobHttpError::Transport)));
        } else {
            assert_eq!(result.unwrap().status(), 507);
        }
        c.close().await;
        server.await.unwrap();
    }
}
#[tokio::test]
async fn account_cancel_and_dropped_waiter_join_body_before_releasing_admission() {
    for (drop_waiter, after_body) in [(false, false), (true, false), (false, true), (true, true)] {
        let (l, origin) = listener().await;
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            if after_body {
                let _ = capture(&mut s).await;
            } else {
                let _ = headers(&mut s).await;
            }
            let _ = entered.send(());
            let _ = wait.await;
        });
        let mut c = client(&origin);
        let cancel = c.cancellation();
        let mut pending = Box::pin(c.put(upload(
            &origin,
            if after_body { 64 } else { 16 * 1024 * 1024 },
        )));
        tokio::select! {_=ready=>{},_= &mut pending=>panic!("server must stall")};
        if !drop_waiter {
            cancel.cancel();
            assert!(matches!(
                pending.as_mut().await,
                Err(BlobHttpError::Cancelled)
            ));
        }
        drop(pending);
        tokio::time::timeout(std::time::Duration::from_secs(2), c.close())
            .await
            .unwrap();
        assert_eq!(c.slots.available_permits(), 1);
        assert!(matches!(
            c.put(upload(&origin, 64)).await,
            Err(BlobHttpError::Cancelled)
        ));
        let _ = release.send(());
        server.await.unwrap();
    }
}
#[tokio::test]
async fn last_queued_chunk_retains_source_slot_and_close_is_retryable() {
    let (_, origin) = listener().await;
    let mut c = client(&origin);
    let slot = c.slots.clone().try_acquire_owned().unwrap();
    let (done, wait) = watch::channel(false);
    c.body_done = wait;
    let owner = Arc::new(BodyOwner {
        source: Some(upload(&origin, CHUNK + 1)),
        slot: Some(slot),
        done,
    });
    let chunk = Bytes::from_owner(ChunkOwner(owner.clone())).slice(..37);
    drop(owner);
    assert_eq!(c.slots.available_permits(), 0);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), c.close())
            .await
            .is_err()
    );
    assert_eq!(c.slots.available_permits(), 0);
    drop(chunk);
    c.close().await;
    assert_eq!(c.slots.available_permits(), 1);
}
#[tokio::test]
async fn foreign_scope_and_changed_body_cannot_release_credentials_or_open_connection() {
    let (_, origin) = listener().await;
    let mut c = client(&origin);
    assert!(matches!(
        c.put(upload("https://foreign.example.test", 64)).await,
        Err(BlobHttpError::Invalid)
    ));
    let mut source = upload(&origin, 64);
    // Forge only in this test: normal callers cannot construct the opaque source.
    let r = source.plan().request().clone();
    let g = source.grant().clone();
    let plan = source.plan().clone();
    drop(source);
    source =
        EpochBlobUpload::test_upload(plan, g, vec![43; r.blob.unwrap().ciphertext_bytes as usize]);
    assert!(matches!(c.put(source).await, Err(BlobHttpError::Invalid)));
    c.close().await;
}

#[test]
#[ignore = "requires the isolated Go ciphertext HTTP/TLS/PostgreSQL fixture"]
fn go_blob_http_worker() {
    use crate::encryption::{
        attachment,
        blob_upload::EpochBlobUploadTransport,
        native::{EpochBlobControlOutcome, NativeEndpoint, NativeV1Client},
    };
    use std::io::Read;
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(16384)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input.lines().collect();
    assert_eq!(fields.len(), 6);
    let ca = std::fs::read(fields[2]).unwrap();
    let mut r = v2::BlobLeaseRequest::decode(std::fs::read(fields[3]).unwrap().as_slice()).unwrap();
    r.message_id = Uuid::new_v4().to_string();
    r.operation_id = Uuid::new_v4().to_string();
    let blob_id = Uuid::new_v4().to_string();
    let binding = attachment::AttachmentBinding::new(
        &r.conversation_id,
        r.crypto_epoch.unwrap(),
        &r.message_id,
        &blob_id,
        r.sender.as_ref().unwrap(),
        r.recipient.as_ref().unwrap(),
    )
    .unwrap();
    let plain = Zeroizing::new(b"private-native-http-file-marker".repeat(4096));
    let sealed = attachment::seal(&binding, Zeroizing::new(plain.to_vec())).unwrap();
    r.blob = Some(v2::BlobRef {
        format_version: 1,
        blob_id,
        ciphertext_bytes: sealed.blob().len() as u64,
        sha256: sealed.descriptor().blob_digest().to_vec(),
    });
    assert_eq!(fields[5].len(), 64);
    let mut credential = Zeroizing::new([0u8; 32]);
    for (i, b) in credential.iter_mut().enumerate() {
        *b = u8::from_str_radix(&fields[5][i * 2..i * 2 + 2], 16).unwrap();
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let own = r.sender.clone().unwrap();
            let scope = StoreScope::new(fields[1], &own.user_id).unwrap();
            let mut registry = NativeV1Client::connect(
                NativeEndpoint::new(fields[0], Some(&ca)).unwrap(),
                &own.user_id,
                Zeroizing::new(fields[4].into()),
                credential,
            )
            .await
            .unwrap();
            registry.bind_generation(&own).unwrap();
            registry.bind_store_scope(scope.clone());
            let mut rpc = registry.epoch_transport().unwrap();
            let plan = EpochBlobControlPlan::test_plan(scope, r);
            // Exercise the trusted tuple's HTTP origin/custom CA construction.
            // Discovery/ready handoff move this same opaque endpoint unchanged.
            let endpoint =
                crate::encryption::discovery::TrustedServer::new(fields[1], fields[0], Some(&ca))
                    .unwrap()
                    .blob_endpoint_for_test();
            let mut http = rpc.blob_transport(endpoint).unwrap();
            let reserved = rpc
                .upload_transport(&mut http)
                .unwrap()
                .reserve(&plan)
                .await
                .unwrap();
            let EpochBlobControlOutcome::Lease(granted) = reserved else {
                panic!("reservation missing")
            };
            // Reject the unique untrusted test CA BEFORE sending HTTP credentials/body.
            let mut untrusted = rpc
                .blob_transport(BlobHttpEndpoint::new(fields[1], None).unwrap())
                .unwrap();
            let rejected = EpochBlobUpload::test_upload(
                plan.clone(),
                granted.response().clone(),
                sealed.blob().to_vec(),
            );
            assert!(matches!(
                untrusted.put(rejected).await,
                Err(BlobHttpError::Transport)
            ));
            untrusted.close().await;
            let upload = EpochBlobUpload::test_upload(
                plan.clone(),
                granted.response().clone(),
                sealed.blob().to_vec(),
            );
            // Go intentionally replaces the response AFTER filesystem+Ready COMMIT.
            // The same operation resolves through gRPC, without another PUT/reseal.
            let reconciled = {
                let mut transport = rpc.upload_transport(&mut http).unwrap();
                assert_eq!(transport.put(upload).await.unwrap(), 503);
                transport.join().await;
                transport.observe(&plan).await.unwrap()
            };
            http.close().await;
            let EpochBlobControlOutcome::Lease(ready) = reconciled else {
                panic!("unknown committed upload")
            };
            assert_eq!(ready.response().state, v2::BlobLeaseState::Ready as i32);
            assert_eq!(ready.response().attempt_id, granted.response().attempt_id);
            assert_eq!(
                ready.response().expires_at_unix_ms,
                granted.response().expires_at_unix_ms
            );
            assert!(
                attachment::open(&binding, sealed.descriptor(), sealed.blob())
                    .unwrap()
                    .as_slice()
                    == plain.as_slice()
            );
            let caps = rpc.capabilities().await.unwrap();
            assert!(!caps.messaging_available && !caps.recovery_available);
        });
    println!("zchat-native-blob-http-complete:encrypted-body");
}
