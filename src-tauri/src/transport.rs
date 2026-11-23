//! Ordinary-chat transport. The renderer supplies a path, never a destination host.
use futures_util::{SinkExt, StreamExt};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tauri::{
    Url,
    http::{Method, Response},
    ipc::Channel,
};
use tokio::sync::{Semaphore, mpsc};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

const MAX_BODY: usize = 54 * 1024 * 1024;
const MAX_CONTROL_RESPONSE: usize = 8 * 1024 * 1024;
const CONTROL_BODY_BUDGET: usize = 16 * 1024 * 1024;
const TRANSFER_BODY_BUDGET: usize = 64 * 1024 * 1024;
const MAX_FRAME: usize = 1024 * 1024;
const MAX_PENDING_EVENTS: u64 = 16;

pub struct Transport {
    server: Mutex<(Url, u64)>,
    client: reqwest::Client,
    http: Mutex<HashMap<String, CancellationToken>>,
    sockets: Mutex<HashMap<String, SocketHandle>>,
    permits: Semaphore,
    transfer_permits: Semaphore,
    pending_control: Semaphore,
    pending_transfers: Semaphore,
    control_body_budget: Semaphore,
    transfer_body_budget: Semaphore,
    serial: AtomicU64,
}

struct SocketHandle {
    cancel: CancellationToken,
    tx: mpsc::Sender<Message>,
    delivered: Arc<AtomicU64>,
    acknowledged: Arc<AtomicU64>,
}

#[derive(Clone, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SocketEvent {
    Open,
    Text { data: String, sequence: u64 },
    Binary { data: Vec<u8>, sequence: u64 },
    Close { code: u16 },
    Error,
}

impl Transport {
    pub fn new(server: &str) -> Result<Self, String> {
        // Both HTTP and WSS use rustls with the same explicit crypto provider.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|_| "Не удалось создать сетевой клиент")?;
        Ok(Self {
            server: Mutex::new((Url::parse(server).map_err(|_| "Некорректный сервер")?, 1)),
            client,
            http: Mutex::new(HashMap::new()),
            sockets: Mutex::new(HashMap::new()),
            permits: Semaphore::new(4),
            transfer_permits: Semaphore::new(2),
            pending_control: Semaphore::new(32),
            pending_transfers: Semaphore::new(32),
            control_body_budget: Semaphore::new(CONTROL_BODY_BUDGET),
            transfer_body_budget: Semaphore::new(TRANSFER_BODY_BUDGET),
            serial: AtomicU64::new(1),
        })
    }

    pub fn snapshot(&self) -> (String, u64) {
        let server = self.server.lock().expect("transport server lock poisoned");
        (
            server.0.as_str().trim_end_matches('/').to_string(),
            server.1,
        )
    }

    pub fn cancel_all(&self, server: Option<&str>) {
        let mut current = self.server.lock().expect("transport server lock poisoned");
        if let Some(server) = server {
            current.0 = Url::parse(server).expect("validated origin");
        }
        current.1 += 1;
        for request in self.http.lock().expect("HTTP lock poisoned").values() {
            request.cancel();
        }
        for socket in self.sockets.lock().expect("socket lock poisoned").values() {
            socket.cancel.cancel();
        }
    }

    pub fn cancel_request(&self, id: &str) {
        if let Some(request) = self.http.lock().expect("HTTP lock poisoned").get(id) {
            request.cancel();
        }
    }

    fn target(&self, epoch: u64, path: &str, socket: bool) -> Result<Url, &'static str> {
        validate_path(path, socket)?;
        let server = self.server.lock().expect("transport server lock poisoned");
        if server.1 != epoch {
            return Err("Страница устарела; перезагрузите приложение");
        }
        let mut url = server.0.join(path).map_err(|_| "Некорректный путь")?;
        if socket {
            let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
            url.set_scheme(scheme)
                .map_err(|_| "Некорректный протокол")?;
        }
        Ok(url)
    }

    pub async fn health(&self) -> Result<(), String> {
        let (server, _) = self.snapshot();
        let result = tokio::time::timeout(Duration::from_secs(4), async {
            let response = self.client.get(format!("{server}/health")).send().await?;
            if response.status() != reqwest::StatusCode::OK
                || response.content_length().unwrap_or(0) > 16
            {
                return Ok(false);
            }
            let mut response = response;
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if body.len() + chunk.len() > 16 {
                    return Ok(false);
                }
                body.extend_from_slice(&chunk);
            }
            Ok::<_, reqwest::Error>(body == b"ok")
        })
        .await;
        match result {
            Ok(Ok(true)) => Ok(()),
            _ => Err("Сервер недоступен или не прошёл проверку готовности".into()),
        }
    }

    pub async fn request(&self, request: tauri::http::Request<Vec<u8>>) -> Response<Vec<u8>> {
        let path = request.uri().path().trim_start_matches('/');
        let Some((revision, path)) = path.split_once('/') else {
            return error(400);
        };
        let Ok(epoch) = revision.parse::<u64>() else {
            return error(400);
        };
        let path = format!(
            "/{}{}",
            path,
            request
                .uri()
                .query()
                .map(|q| format!("?{q}"))
                .unwrap_or_default()
        );
        let Ok(url) = self.target(epoch, &path, false) else {
            return error(409);
        };
        if !matches!(
            *request.method(),
            Method::GET
                | Method::HEAD
                | Method::POST
                | Method::PUT
                | Method::PATCH
                | Method::DELETE
        ) {
            return error(405);
        }
        if request.body().len() > MAX_BODY {
            return error(413);
        }
        let transfer = path.starts_with("/uploads/")
            || request.body().len() > 1024 * 1024
            || request
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.starts_with("multipart/form-data"));
        // Queue normal startup bursts and reserve control capacity independently
        // of slow attachments. Bound queued jobs and their retained bodies.
        let (admission, permits, budget, response_limit) = if transfer {
            (
                &self.pending_transfers,
                &self.transfer_permits,
                &self.transfer_body_budget,
                MAX_BODY,
            )
        } else {
            (
                &self.pending_control,
                &self.permits,
                &self.control_body_budget,
                MAX_CONTROL_RESPONSE,
            )
        };
        let Ok(_queued) = admission.try_acquire() else {
            return error(429);
        };
        let Ok(_body_budget) = budget.try_acquire_many(request.body().len() as u32 + 1) else {
            return error(429);
        };
        let id = request
            .headers()
            .get("x-zchat-request-id")
            .and_then(|v| v.to_str().ok())
            .filter(|s| s.len() <= 64 && s.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-'))
            .map(str::to_owned)
            .unwrap_or_else(|| format!("resource-{}", self.serial.fetch_add(1, Ordering::Relaxed)));
        let cancel = CancellationToken::new();
        {
            let mut active = self.http.lock().expect("HTTP lock poisoned");
            if active.contains_key(&id) {
                return error(409);
            }
            active.insert(id.clone(), cancel.clone());
        }
        let mut upstream = self.client.request(request.method().clone(), url);
        // Never forward cookies, Origin, Host or forwarding headers.
        for header in ["authorization", "content-type", "range"] {
            if let Some(value) = request.headers().get(header) {
                upstream = upstream.header(header, value);
            }
        }
        let upstream = upstream.body(request.into_parts().1);
        if self.snapshot().1 != epoch {
            cancel.cancel();
        }
        let response = tokio::select! {
            _ = cancel.cancelled() => error(499),
            result = tokio::time::timeout(Duration::from_secs(60), async {
                let _active = permits.acquire().await.map_err(|_| ())?;
                self.read_response(upstream, response_limit).await
            }) => result.ok().and_then(Result::ok).unwrap_or_else(|| error(502)),
        };
        self.http.lock().expect("HTTP lock poisoned").remove(&id);
        if self.snapshot().1 != epoch {
            return error(409);
        }
        response
    }

    async fn read_response(
        &self,
        request: reqwest::RequestBuilder,
        limit: usize,
    ) -> Result<Response<Vec<u8>>, ()> {
        let mut upstream = request.send().await.map_err(|_| ())?;
        if upstream.status().is_redirection()
            || upstream.content_length().is_some_and(|n| n > limit as u64)
        {
            return Err(());
        }
        let mut result = Response::builder().status(upstream.status());
        for header in [
            "content-type",
            "content-disposition",
            "content-range",
            "accept-ranges",
            "x-zchat-operation-committed",
        ] {
            if let Some(value) = upstream.headers().get(header) {
                result = result.header(header, value);
            }
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = upstream.chunk().await.map_err(|_| ())? {
            if bytes.len() + chunk.len() > limit {
                return Err(());
            }
            bytes.extend_from_slice(&chunk);
        }
        result
            .header("cache-control", "no-store")
            .header("x-content-type-options", "nosniff")
            .body(bytes)
            .map_err(|_| ())
    }

    pub fn connect(
        self: &Arc<Self>,
        epoch: u64,
        id: String,
        path: String,
        channel: Channel<SocketEvent>,
    ) -> Result<(), String> {
        if id.len() != 36 || !id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-') {
            return Err("Некорректный ID соединения".into());
        }
        let url = self.target(epoch, &path, true)?;
        let cancel = CancellationToken::new();
        let (tx, rx) = mpsc::channel(8);
        let delivered = Arc::new(AtomicU64::new(0));
        let acknowledged = Arc::new(AtomicU64::new(0));
        {
            let mut sockets = self.sockets.lock().expect("socket lock poisoned");
            if sockets.len() >= 4 || sockets.contains_key(&id) {
                return Err("Лимит соединений".into());
            }
            sockets.insert(
                id.clone(),
                SocketHandle {
                    cancel: cancel.clone(),
                    tx,
                    delivered: delivered.clone(),
                    acknowledged: acknowledged.clone(),
                },
            );
        }
        let transport = self.clone();
        if self.snapshot().1 != epoch {
            cancel.cancel();
        }
        tauri::async_runtime::spawn(async move {
            transport
                .socket_loop(url, rx, cancel, channel, delivered, acknowledged)
                .await;
            transport
                .sockets
                .lock()
                .expect("socket lock poisoned")
                .remove(&id);
        });
        Ok(())
    }

    pub fn send(&self, id: &str, data: Vec<u8>, binary: bool) -> Result<(), String> {
        if data.len() > 64 * 1024 {
            return Err("Сообщение слишком большое".into());
        }
        let sockets = self.sockets.lock().expect("socket lock poisoned");
        let socket = sockets.get(id).ok_or("Соединение закрыто")?;
        let message = if binary {
            Message::Binary(data.into())
        } else {
            Message::Text(
                String::from_utf8(data)
                    .map_err(|_| "Неверная кодировка")?
                    .into(),
            )
        };
        socket.tx.try_send(message).map_err(|_| {
            socket.cancel.cancel();
            "Буфер соединения заполнен".into()
        })
    }

    pub fn close(&self, id: &str) {
        if let Some(socket) = self.sockets.lock().expect("socket lock poisoned").get(id) {
            socket.cancel.cancel();
        }
    }

    pub fn acknowledge(&self, id: &str, sequence: u64) {
        if let Some(socket) = self.sockets.lock().expect("socket lock poisoned").get(id)
            && sequence <= socket.delivered.load(Ordering::Relaxed)
        {
            socket.acknowledged.fetch_max(sequence, Ordering::Relaxed);
        }
    }

    async fn socket_loop(
        &self,
        url: Url,
        mut outgoing: mpsc::Receiver<Message>,
        cancel: CancellationToken,
        channel: Channel<SocketEvent>,
        delivered: Arc<AtomicU64>,
        acknowledged: Arc<AtomicU64>,
    ) {
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_FRAME))
            .max_frame_size(Some(MAX_FRAME));
        let connection = tokio::select! {
            _ = cancel.cancelled() => { let _ = channel.send(SocketEvent::Close { code: 1000 }); return; },
            result = tokio::time::timeout(Duration::from_secs(8), connect_async_with_config(url.as_str(), Some(config), false)) => result,
        };
        let Ok(Ok((mut socket, _))) = connection else {
            let _ = channel.send(SocketEvent::Error);
            let _ = channel.send(SocketEvent::Close { code: 1006 });
            return;
        };
        if channel.send(SocketEvent::Open).is_err() {
            return;
        }
        let mut timer = tokio::time::interval(Duration::from_secs(5));
        let mut last_ack = 0;
        let mut stalled = 0;
        let mut code = 1000;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = timer.tick() => {
                    let ack = acknowledged.load(Ordering::Relaxed);
                    if delivered.load(Ordering::Relaxed) > ack && ack == last_ack { stalled += 1; } else { stalled = 0; }
                    last_ack = ack;
                    if stalled >= 3 { code = 1008; break; }
                },
                message = outgoing.recv() => {
                    let Some(message) = message else { break; };
                    if !matches!(tokio::time::timeout(Duration::from_secs(5), socket.send(message)).await, Ok(Ok(()))) { code = 1006; break; }
                },
                message = socket.next() => {
                    let event = match message {
                        Some(Ok(Message::Text(data))) => Some((Some(data.to_string()), None)),
                        Some(Ok(Message::Binary(data))) => Some((None, Some(data.to_vec()))),
                        Some(Ok(Message::Close(frame))) => { code = frame.map(|f| u16::from(f.code)).unwrap_or(1000); break; },
                        Some(Ok(_)) => None,
                        _ => { code = 1006; break; },
                    };
                    if let Some((text, binary)) = event {
                        let sequence = delivered.load(Ordering::Relaxed) + 1;
                        if sequence - acknowledged.load(Ordering::Relaxed) > MAX_PENDING_EVENTS { code = 1008; break; }
                        delivered.store(sequence, Ordering::Relaxed);
                        let event = if let Some(data) = text { SocketEvent::Text { data, sequence } } else { SocketEvent::Binary { data: binary.unwrap_or_default(), sequence } };
                        if channel.send(event).is_err() { break; }
                    }
                }
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(1), socket.close(None)).await;
        let _ = channel.send(SocketEvent::Close { code });
    }
}

fn validate_path(path: &str, socket: bool) -> Result<(), &'static str> {
    let base = path.split('?').next().unwrap_or("");
    let lower = base.to_ascii_lowercase();
    if path.len() > 16384
        || !base.starts_with('/')
        || base.starts_with("//")
        || base.contains('\\')
        || path.contains('#')
        || base.split('/').any(|part| matches!(part, "." | ".."))
        || ["%2e", "%2f", "%5c", "%00"]
            .iter()
            .any(|p| lower.contains(p))
    {
        return Err("Недопустимый путь");
    }
    let allowed = if socket {
        base == "/ws" || matches!(base, "/rtc/rtc" | "/rtc/rtc/v1")
    } else {
        base.starts_with("/api/")
            || base.starts_with("/uploads/")
            || base == "/health"
            || matches!(base, "/rtc/rtc/validate" | "/rtc/rtc/v1/validate")
    };
    if !allowed {
        return Err("Путь недоступен desktop-клиенту");
    }
    Ok(())
}

pub fn error(status: u16) -> Response<Vec<u8>> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("cache-control", "no-store")
        .body(b"Desktop request failed".to_vec())
        .expect("static response")
}

pub fn cors(response: &mut Response<Vec<u8>>, origin: &str) {
    let headers = response.headers_mut();
    headers.insert(
        "access-control-allow-origin",
        origin.parse().expect("validated app origin"),
    );
    headers.insert(
        "access-control-allow-methods",
        "GET, HEAD, POST, PUT, PATCH, DELETE, OPTIONS"
            .parse()
            .unwrap(),
    );
    headers.insert(
        "access-control-allow-headers",
        "authorization, content-type, range, x-zchat-request-id"
            .parse()
            .unwrap(),
    );
    headers.insert("access-control-expose-headers", "content-type, content-disposition, content-range, accept-ranges, x-zchat-operation-committed".parse().unwrap());
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    async fn server(reply: &'static [u8]) -> (String, tokio::task::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; 8192];
            let count = stream.read(&mut bytes).await.unwrap();
            stream.write_all(reply).await.unwrap();
            String::from_utf8_lossy(&bytes[..count]).to_lowercase()
        });
        (origin, task)
    }

    #[test]
    fn paths_cannot_override_origin_or_escape_allowed_routes() {
        for path in [
            "https://evil.test/api/a",
            "//evil.test/api/a",
            "/api/../health",
            "/api/%2e%2e/a",
            "/api/a%2fb",
            "/api/a\\b",
            "/api/a#fragment",
            "/metrics",
            "/login.html",
        ] {
            assert!(validate_path(path, false).is_err(), "{path}");
        }
        assert!(validate_path("/api/users?q=%D0%B0", false).is_ok());
        assert!(validate_path("/uploads/media/a.ogg", false).is_ok());
        assert!(validate_path("/api/profile", true).is_err());
        assert!(validate_path("/ws?ticket=test", true).is_ok());
    }

    #[tokio::test]
    async fn old_page_is_rejected_after_server_switch_even_when_switching_back() {
        let transport = Transport::new("http://127.0.0.1:9").unwrap();
        let (_, old) = transport.snapshot();
        transport.cancel_all(Some("http://127.0.0.1:10"));
        transport.cancel_all(Some("http://127.0.0.1:9"));
        let response = transport
            .request(
                tauri::http::Request::builder()
                    .uri(format!("/{old}/api/profile"))
                    .body(Vec::new())
                    .unwrap(),
            )
            .await;
        assert_eq!(response.status(), 409);
    }

    #[tokio::test]
    async fn forwards_bearer_and_commit_status_without_cookies_or_origin() {
        let (origin, peer) = server(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 2\r\nX-Zchat-Operation-Committed: true\r\nSet-Cookie: forbidden=yes\r\n\r\nok").await;
        let transport = Transport::new(&origin).unwrap();
        let response = transport
            .request(
                tauri::http::Request::builder()
                    .uri("/1/api/profile/logout")
                    .method("POST")
                    .header("authorization", "Bearer synthetic-only")
                    .header("cookie", "private=not-forwarded")
                    .header("origin", "http://tauri.localhost")
                    .header("x-forwarded-host", "evil.test")
                    .body(Vec::new())
                    .unwrap(),
            )
            .await;
        assert_eq!(response.status(), 503);
        assert_eq!(response.headers()["x-zchat-operation-committed"], "true");
        assert!(response.headers().get("set-cookie").is_none());
        let request = peer.await.unwrap();
        assert!(request.contains("authorization: bearer synthetic-only"));
        assert!(
            !request.contains("cookie:")
                && !request.contains("origin:")
                && !request.contains("x-forwarded-host:")
        );
    }

    #[tokio::test]
    async fn redirect_and_oversized_responses_are_not_followed_or_buffered() {
        for reply in [b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/api/a\r\nContent-Length: 0\r\n\r\n".as_slice(), b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n".as_slice()] {
            let (origin, task) = server(reply).await;
            let transport = Transport::new(&origin).unwrap();
            let response = transport.request(tauri::http::Request::builder().uri("/1/api/profile").body(Vec::new()).unwrap()).await;
            assert_eq!(response.status(), 502);
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn health_requires_http_ok_body_instead_of_a_listening_port() {
        for (reply, ready) in [
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok".as_slice(),
                true,
            ),
            (
                b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nhtml".as_slice(),
                false,
            ),
        ] {
            let (origin, task) = server(reply).await;
            assert_eq!(
                Transport::new(&origin).unwrap().health().await.is_ok(),
                ready
            );
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancellation_releases_a_request_waiting_for_response_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let transport = Arc::new(
            Transport::new(&format!("http://{}", listener.local_addr().unwrap())).unwrap(),
        );
        let client = transport.clone();
        let request = tokio::spawn(async move {
            client
                .request(
                    tauri::http::Request::builder()
                        .uri("/1/api/profile")
                        .header("x-zchat-request-id", "cancel-test")
                        .body(Vec::new())
                        .unwrap(),
                )
                .await
        });
        let (_peer, _) = listener.accept().await.unwrap();
        transport.cancel_request("cancel-test");
        let response = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), 499);
        assert!(transport.http.lock().unwrap().is_empty());
        assert_eq!(transport.permits.available_permits(), 4);
        assert_eq!(transport.pending_control.available_permits(), 32);
        assert_eq!(
            transport.control_body_budget.available_permits(),
            CONTROL_BODY_BUDGET
        );
    }

    #[tokio::test]
    async fn startup_bursts_queue_and_slow_transfers_cannot_block_control_requests() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let mut handlers = Vec::new();
            for _ in 0..12 {
                let (mut socket, _) = listener.accept().await.unwrap();
                handlers.push(tokio::spawn(async move {
                    let mut bytes = [0; 2048];
                    let received = socket.read(&mut bytes).await.unwrap();
                    assert!(received > 0);
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                        )
                        .await
                        .unwrap();
                }));
            }
            for handler in handlers {
                handler.await.unwrap();
            }
        });
        let transport = Arc::new(Transport::new(&origin).unwrap());
        let occupied_transfers = transport.transfer_permits.acquire_many(2).await.unwrap();
        let mut requests = tokio::task::JoinSet::new();
        for _ in 0..12 {
            let client = transport.clone();
            requests.spawn(async move {
                client
                    .request(
                        tauri::http::Request::builder()
                            .uri("/1/api/profile")
                            .body(Vec::new())
                            .unwrap(),
                    )
                    .await
                    .status()
            });
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(status) = requests.join_next().await {
                assert_eq!(status.unwrap(), 200);
            }
        })
        .await
        .unwrap();
        drop(occupied_transfers);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn queued_request_can_be_cancelled_before_opening_a_connection() {
        let transport = Arc::new(Transport::new("http://127.0.0.1:9").unwrap());
        let occupied = transport.permits.acquire_many(4).await.unwrap();
        let client = transport.clone();
        let request = tokio::spawn(async move {
            client
                .request(
                    tauri::http::Request::builder()
                        .uri("/1/api/profile")
                        .header("x-zchat-request-id", "queued-test")
                        .body(Vec::new())
                        .unwrap(),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !transport.http.lock().unwrap().contains_key("queued-test") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        transport.cancel_request("queued-test");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), request)
                .await
                .unwrap()
                .unwrap()
                .status(),
            499
        );
        assert!(transport.http.lock().unwrap().is_empty());
        assert_eq!(transport.pending_control.available_permits(), 32);
        drop(occupied);
    }

    #[tokio::test]
    async fn a_renderer_not_acknowledging_messages_cannot_grow_the_event_queue() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!("ws://{}/ws", listener.local_addr().unwrap())).unwrap();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            for _ in 0..MAX_PENDING_EVENTS + 2 {
                if socket.send(Message::Text("test".into())).await.is_err() {
                    break;
                }
            }
            while let Some(Ok(message)) = socket.next().await {
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        let received = events.clone();
        let channel = Channel::new(move |body| {
            if let tauri::ipc::InvokeResponseBody::Json(json) = body {
                received
                    .lock()
                    .unwrap()
                    .push(serde_json::from_str::<serde_json::Value>(&json).unwrap());
            }
            Ok(())
        });
        // Keep the writer alive while exercising the real TCP reader.
        let (writer, rx) = mpsc::channel::<Message>(8);
        let transport = Transport::new("http://127.0.0.1:9").unwrap();
        let delivered = Arc::new(AtomicU64::new(0));
        tokio::time::timeout(
            Duration::from_secs(3),
            transport.socket_loop(
                url,
                rx,
                CancellationToken::new(),
                channel,
                delivered.clone(),
                Arc::new(AtomicU64::new(0)),
            ),
        )
        .await
        .unwrap();
        drop(writer);
        peer.await.unwrap();
        assert_eq!(delivered.load(Ordering::Relaxed), MAX_PENDING_EVENTS);
        assert_eq!(events.lock().unwrap().last().unwrap()["code"], 1008);
    }
}
