use super::*;
use crate::encryption::native::NativeEpochClient;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn read_request(origin: &str, bytes: &[u8]) -> BlobReadRequest {
    let own = common::DeviceRef {
        user_id: "11111111-1111-4111-8111-111111111111".into(),
        device_id: 1,
        generation_id: "33333333-3333-4333-8333-333333333333".into(),
    };
    BlobReadRequest::new(
        StoreScope::new(origin, &own.user_id).unwrap(),
        common::EpochSendBinding {
            protocol_version: 3,
            conversation_id: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb".into(),
            message_id: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".into(),
            epoch: 0,
            sender: Some(common::DeviceRef {
                user_id: "22222222-2222-4222-8222-222222222222".into(),
                device_id: 1,
                generation_id: "44444444-4444-4444-8444-444444444444".into(),
            }),
            recipient: Some(own),
            envelope_digest: vec![43; 32],
        },
        v2::BlobRef {
            format_version: 1,
            blob_id: "cccccccc-cccc-4ccc-8ccc-cccccccccccc".into(),
            ciphertext_bytes: bytes.len() as u64,
            sha256: Sha256::digest(bytes).to_vec(),
        },
    )
    .unwrap()
}
fn client(origin: &str) -> NativeBlobHttp {
    let r = read_request(origin, &[42; 64]);
    NativeEpochClient {
        client: v2::messenger_client::MessengerClient::new(
            tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
        ),
        own: r.binding.recipient.clone().unwrap(),
        scope: r.scope,
        jwt: Zeroizing::new("synthetic-jwt".into()),
        credential: Zeroizing::new([37; 32]),
    }
    .blob_transport(BlobHttpEndpoint::new(origin, None).unwrap())
    .unwrap()
}
async fn listener() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", l.local_addr().unwrap());
    (l, origin)
}
async fn headers(s: &mut TcpStream) -> String {
    let mut data = Vec::new();
    loop {
        let mut b = [0; 1024];
        let n = s.read(&mut b).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&b[..n]);
        assert!(data.len() < 8192);
        if data.ends_with(b"\r\n\r\n") {
            return String::from_utf8(data).unwrap();
        }
    }
}
fn success_headers(n: usize) -> String {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {n}\r\nContent-Type: application/octet-stream\r\nCache-Control: private, no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Disposition: attachment; filename=\"encrypted.bin\"\r\nAccept-Ranges: none\r\nConnection: close\r\n\r\n"
    )
}
#[derive(Default)]
struct Sink {
    bytes: Vec<u8>,
    max: usize,
    writes: usize,
}
#[async_trait::async_trait]
impl BlobCiphertextSink for Sink {
    async fn write(&mut self, c: BlobCiphertextChunk) -> std::result::Result<(), BlobSinkError> {
        self.max = self.max.max(c.as_bytes().len());
        self.writes += 1;
        self.bytes.extend_from_slice(c.as_bytes());
        Ok(())
    }
}

#[test]
fn download_header_matches_go_vector_with_large_counter() {
    let v: serde_json::Value = serde_json::from_str(include_str!(
        "../../../proto/fixtures/attachment-download-http-v1.json"
    ))
    .unwrap();
    let j: serde_json::Value =
        serde_json::from_str(v["canonical_metadata_json"].as_str().unwrap()).unwrap();
    let device = |u: &str, g: &str| common::DeviceRef {
        user_id: j[u].as_str().unwrap().into(),
        generation_id: j[g].as_str().unwrap().into(),
        device_id: 1,
    };
    let unhex = |s: &str| -> Vec<u8> {
        s.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|b| u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap())
            .collect()
    };
    let binding = common::EpochSendBinding {
        protocol_version: 3,
        conversation_id: j["conversation_id"].as_str().unwrap().into(),
        message_id: j["message_id"].as_str().unwrap().into(),
        epoch: j["crypto_epoch"].as_str().unwrap().parse().unwrap(),
        sender: Some(device("sender_user_id", "sender_generation_id")),
        recipient: Some(device("recipient_user_id", "recipient_generation_id")),
        envelope_digest: unhex(j["envelope_digest"].as_str().unwrap()),
    };
    let scope = StoreScope::new(
        "https://example.test",
        &binding.recipient.as_ref().unwrap().user_id,
    )
    .unwrap();
    let r = BlobReadRequest::new(
        scope,
        binding,
        v2::BlobRef {
            format_version: 1,
            blob_id: j["blob_id"].as_str().unwrap().into(),
            ciphertext_bytes: j["ciphertext_bytes"].as_str().unwrap().parse().unwrap(),
            sha256: unhex(j["sha256"].as_str().unwrap()),
        },
    )
    .unwrap();
    assert_eq!(
        r.metadata().unwrap(),
        v["metadata_header"].as_str().unwrap()
    );
    assert_eq!(r.binding().epoch, (1 << 53) + 17);
    let mut invalid = r.binding().clone();
    invalid.protocol_version = 2;
    assert!(BlobReadRequest::new(r.store_scope().clone(), invalid, r.blob().clone()).is_err());
}

#[tokio::test]
async fn get_streams_exact_ciphertext_and_head_never_fills_sink() {
    let (l, origin) = listener().await;
    let data = vec![42; CHUNK * 2 + 17];
    let request = read_request(&origin, &data);
    let expected = request.metadata().unwrap();
    let server = tokio::spawn(async move {
        for head in [false, true] {
            let (mut socket, _) = l.accept().await.unwrap();
            let h = headers(&mut socket).await;
            assert!(h.starts_with(if head {
                "HEAD /api/e2ee/attachments/"
            } else {
                "GET /api/e2ee/attachments/"
            }));
            assert!(
                h.lines()
                    .any(|line| line.split_once(':').is_some_and(|(name, value)| name
                        .eq_ignore_ascii_case("x-zchat-blob-read-metadata")
                        && value.trim() == expected))
            );
            assert!(h.contains("synthetic-jwt"));
            assert!(!h.to_ascii_lowercase().contains("content-type:"));
            assert!(!h.contains('?'));
            socket
                .write_all(success_headers(data.len()).as_bytes())
                .await
                .unwrap();
            if !head {
                socket.write_all(&data).await.unwrap()
            }
        }
    });
    let mut c = client(&origin);
    let mut sink = Sink::default();
    let result = c.get(request.clone(), &mut sink).await.unwrap();
    assert_eq!(result.request().binding(), request.binding());
    assert_eq!(sink.bytes, vec![42; CHUNK * 2 + 17]);
    assert!(sink.max <= CHUNK);
    assert!(sink.writes > 1);
    c.wait_idle().await;
    let head = c.head(request.clone()).await.unwrap();
    assert_eq!(head.request().blob(), request.blob());
    c.close().await;
    server.await.unwrap();
}

#[tokio::test]
async fn foreign_scope_or_recipient_never_opens_a_socket() {
    let (l, origin) = listener().await;
    let base = read_request(&origin, &[42; 64]);
    let mut c = client(&origin);
    for kind in 0..3 {
        let mut r = base.clone();
        match kind {
            0 => r.scope = StoreScope::new("https://other.example.test", &c.own.user_id).unwrap(),
            1 => {
                r.binding.recipient.as_mut().unwrap().generation_id =
                    "55555555-5555-4555-8555-555555555555".into()
            }
            _ => {
                r.binding.recipient.as_mut().unwrap().user_id =
                    "66666666-6666-4666-8666-666666666666".into()
            }
        };
        assert!(matches!(
            c.get(r, &mut Sink::default()).await,
            Err(BlobReadError::Invalid)
        ));
    }
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), l.accept())
            .await
            .is_err()
    );
    assert_eq!(c.slots.available_permits(), 1);
    c.close().await;
}

#[tokio::test]
async fn malformed_headers_and_redirects_never_reach_the_sink() {
    for kind in [
        "length",
        "mime",
        "encoding",
        "range",
        "duplicate",
        "chunked",
        "nosniff",
        "redirect",
        "notfound",
        "unauthorized",
        "busy",
        "partial",
    ] {
        let (l, origin) = listener().await;
        let mut h = success_headers(64);
        match kind{
            "length"=>h=h.replace("Content-Length: 64","Content-Length: 65"),"mime"=>h=h.replace("application/octet-stream","image/jpeg"),"encoding"=>h=h.replace("Connection: close","Content-Encoding: gzip\r\nConnection: close"),
            "range"=>h=h.replace("Connection: close","Content-Range: bytes 0-63/64\r\nConnection: close"),"duplicate"=>h=h.replace("Cache-Control: private, no-store","Cache-Control: private, no-store\r\nCache-Control: private, no-store"),
            "chunked"=>h=h.replace("Content-Length: 64","Transfer-Encoding: chunked"),"nosniff"=>h=h.replace("X-Content-Type-Options: nosniff\r\n",""),"redirect"=>h="HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/steal\r\nContent-Length: 0\r\n\r\n".into(),
            "notfound"=>h="HTTP/1.1 404 Not Found\r\nContent-Length: 999999999\r\n\r\n".into(),"unauthorized"=>h="HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n".into(),"busy"=>h="HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\n\r\n".into(),_=>h=h.replace("200 OK","206 Partial Content")
        }
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let _ = headers(&mut s).await;
            let _ = s.write_all(h.as_bytes()).await;
        });
        let mut c = client(&origin);
        let mut sink = Sink::default();
        let e = c.get(read_request(&origin, &[42; 64]), &mut sink).await;
        match kind {
            "redirect" => assert!(matches!(
                e,
                Err(BlobReadError::Http(BlobHttpError::Redirect))
            )),
            "notfound" => assert!(matches!(e, Err(BlobReadError::Rejected(404)))),
            "unauthorized" => assert!(matches!(e, Err(BlobReadError::Rejected(401)))),
            "busy" => assert!(matches!(e, Err(BlobReadError::Rejected(429)))),
            "partial" => assert!(matches!(e, Err(BlobReadError::Rejected(206)))),
            _ => assert!(e.is_err()),
        }
        assert_eq!(sink.writes, 0, "{kind}");
        c.close().await;
        server.await.unwrap();
    }
}

#[tokio::test]
async fn hash_corruption_and_truncation_cannot_return_a_completed_read() {
    for truncated in [false, true] {
        let (l, origin) = listener().await;
        let server = tokio::spawn(async move {
            let (mut s, _) = l.accept().await.unwrap();
            let _ = headers(&mut s).await;
            s.write_all(success_headers(64).as_bytes()).await.unwrap();
            s.write_all(&vec![43; if truncated { 63 } else { 64 }])
                .await
                .unwrap();
        });
        let mut c = client(&origin);
        let e = c
            .get(read_request(&origin, &[42; 64]), &mut Sink::default())
            .await;
        if truncated {
            assert!(e.is_err())
        } else {
            assert!(matches!(e, Err(BlobReadError::Integrity)))
        }
        c.close().await;
        server.await.unwrap();
    }
}

struct RetainSink {
    chunk: Option<BlobCiphertextChunk>,
    entered: tokio::sync::oneshot::Sender<()>,
}

struct FailingSink;
#[async_trait::async_trait]
impl BlobCiphertextSink for FailingSink {
    async fn write(&mut self, _: BlobCiphertextChunk) -> std::result::Result<(), BlobSinkError> {
        Err(BlobSinkError)
    }
}
#[tokio::test]
async fn sink_error_releases_io_but_does_not_stop_the_account_transport() {
    let (l, origin) = listener().await;
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let _ = headers(&mut s).await;
        s.write_all(success_headers(64).as_bytes()).await.unwrap();
        s.write_all(&[42; 64]).await.unwrap();
    });
    let mut c = client(&origin);
    assert!(matches!(
        c.get(read_request(&origin, &[42; 64]), &mut FailingSink)
            .await,
        Err(BlobReadError::Sink)
    ));
    c.wait_idle().await;
    assert_eq!(c.slots.available_permits(), 1);
    assert!(!c.stop.is_cancelled());
    c.close().await;
    server.await.unwrap();
}
#[async_trait::async_trait]
impl BlobCiphertextSink for RetainSink {
    async fn write(
        &mut self,
        chunk: BlobCiphertextChunk,
    ) -> std::result::Result<(), BlobSinkError> {
        self.chunk = Some(chunk); // keep LAST ownership even if this async waiter drops
        let (replacement, _) = tokio::sync::oneshot::channel();
        let entered = std::mem::replace(&mut self.entered, replacement);
        let _ = entered.send(());
        std::future::pending().await
    }
}
#[tokio::test]
async fn cancelled_sink_waiter_keeps_shared_slot_until_last_chunk_drops() {
    let (l, origin) = listener().await;
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let _ = headers(&mut s).await;
        s.write_all(success_headers(64).as_bytes()).await.unwrap();
        s.write_all(&[42; 64]).await.unwrap();
        let mut b = [0; 1];
        let _ = s.read(&mut b).await;
    });
    let mut c = client(&origin);
    let request = read_request(&origin, &[42; 64]);
    let (entered, wait) = tokio::sync::oneshot::channel();
    let mut sink = RetainSink {
        chunk: None,
        entered,
    };
    {
        let read = c.get(request.clone(), &mut sink);
        tokio::pin!(read);
        tokio::select! {_=wait=>{},_=&mut read=>panic!("sink did not block")}
    }
    assert_eq!(c.slots.available_permits(), 0);
    assert!(matches!(
        c.head(request).await,
        Err(BlobReadError::Http(BlobHttpError::Busy))
    ));
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), c.close())
            .await
            .is_err()
    );
    drop(sink.chunk.take());
    c.close().await;
    assert_eq!(c.slots.available_permits(), 1);
    server.await.unwrap();
}

#[tokio::test]
async fn account_cancellation_interrupts_a_stalled_response() {
    let (l, origin) = listener().await;
    let (sent, wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let _ = headers(&mut s).await;
        let _ = sent.send(());
        let mut b = [0; 1];
        assert_eq!(s.read(&mut b).await.unwrap(), 0);
    });
    let mut c = client(&origin);
    let cancel = c.cancellation();
    let req = read_request(&origin, &[42; 64]);
    let mut sink = Sink::default();
    {
        let read = c.get(req, &mut sink);
        tokio::pin!(read);
        tokio::select! {_=wait=>cancel.cancel(),_=&mut read=>panic!("request did not reach server")};
        assert!(matches!(
            read.await,
            Err(BlobReadError::Http(BlobHttpError::Cancelled))
        ))
    }
    c.close().await;
    assert_eq!(c.slots.available_permits(), 1);
    server.await.unwrap();
}

#[test]
#[ignore = "requires isolated Go HTTP/TLS/PostgreSQL accepted-file fixture"]
fn go_blob_download_worker() {
    use prost::Message;
    use std::io::Read;
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(16384)
        .read_to_string(&mut input)
        .unwrap();
    let j: serde_json::Value = serde_json::from_str(&input).unwrap();
    let value = |name: &str| j[name].as_str().unwrap();
    let binding = common::EpochSendBinding::decode(
        URL_SAFE_NO_PAD.decode(value("binding")).unwrap().as_slice(),
    )
    .unwrap();
    let blob =
        v2::BlobRef::decode(URL_SAFE_NO_PAD.decode(value("blob")).unwrap().as_slice()).unwrap();
    let own = binding.recipient.clone().unwrap();
    let scope = StoreScope::new(value("origin"), &own.user_id).unwrap();
    let request = BlobReadRequest::new(scope.clone(), binding, blob).unwrap();
    assert_eq!(value("credential").len(), 64);
    let mut credential = Zeroizing::new([0u8; 32]);
    for (i, b) in value("credential")
        .as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .enumerate()
    {
        credential[i] = u8::from_str_radix(std::str::from_utf8(b).unwrap(), 16).unwrap();
    }
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        // This fixture isolates the HTTP transport, not RPC discovery/local keys.
        let rpc = NativeEpochClient {
            client: v2::messenger_client::MessengerClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            own,
            scope,
            jwt: Zeroizing::new(value("jwt").into()),
            credential,
        };
        let mut untrusted = rpc
            .blob_transport(BlobHttpEndpoint::new(value("origin"), None).unwrap())
            .unwrap();
        assert!(matches!(
            untrusted.head(request.clone()).await,
            Err(BlobReadError::Http(BlobHttpError::Transport))
        ));
        untrusted.close().await;
        let mut http = rpc
            .blob_transport(
                BlobHttpEndpoint::new(value("origin"), Some(value("ca").as_bytes())).unwrap(),
            )
            .unwrap();
        let observed = http.head(request.clone()).await.unwrap();
        assert_eq!(observed.request().binding(), request.binding());
        let mut sink = Sink::default();
        let observed = http.get(request.clone(), &mut sink).await.unwrap();
        assert_eq!(observed.request().blob(), request.blob());
        assert_eq!(sink.bytes.len() as u64, request.blob().ciphertext_bytes);
        assert_eq!(
            Sha256::digest(&sink.bytes).as_slice(),
            request.blob().sha256.as_slice()
        );
        http.close().await;
        println!("zchat-native-blob-read-complete:sha-and-scope");
    });
}
