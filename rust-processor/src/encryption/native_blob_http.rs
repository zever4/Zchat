//! Account-scoped ciphertext HTTP. No keys/renderer URLs, retry or local writes.
//! Upload callers must Observe the exact operation; reads never create an ACK.
use super::*;
use crate::encryption::{
    blob_control,
    signal_state::{EpochBlobControlPlan, EpochBlobUpload},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures_util::stream;
use http_body_util::{BodyExt, StreamBody};
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::sync::CancellationToken;

const CHUNK: usize = 256 * 1024;
const MAX_REPLY: usize = 4096;
const PUT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(35);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BlobHttpError {
    #[error("invalid native blob endpoint, scope or transfer snapshot")]
    Invalid,
    #[error("another blob body still owns the transfer budget")]
    Busy,
    #[error("blob transfer cancelled; its server result may be unknown")]
    Cancelled,
    #[error("blob transfer timed out; its server result may be unknown")]
    Timeout,
    #[error("blob transport failed; its server result may be unknown")]
    Transport,
    #[error("blob transport refused a redirect")]
    Redirect,
    #[error("blob transport response exceeds its byte limit")]
    ResponseTooLarge,
}
type HttpResult<T> = std::result::Result<T, BlobHttpError>;

/// Trusted native config only. CA/origin are not accepted from renderer DTOs.
pub struct BlobHttpEndpoint {
    origin: String,
    host: String,
    port: u16,
    tls: Option<Arc<rustls::ClientConfig>>,
}
impl BlobHttpEndpoint {
    pub fn new(origin: &str, extra_ca: Option<&[u8]>) -> HttpResult<Self> {
        super::NativeEndpoint::new(origin, extra_ca).map_err(|_| BlobHttpError::Invalid)?;
        let url = url::Url::parse(origin).map_err(|_| BlobHttpError::Invalid)?;
        let origin = url.origin().ascii_serialization();
        let host = url
            .host_str()
            .ok_or(BlobHttpError::Invalid)?
            .trim_matches(['[', ']'])
            .to_owned();
        let port = url.port_or_known_default().ok_or(BlobHttpError::Invalid)?;
        let tls = if url.scheme() == "https" {
            use rustls::pki_types::{CertificateDer, pem::PemObject};
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let verifier = if let Some(ca) = extra_ca {
                let roots = CertificateDer::pem_slice_iter(ca)
                    .collect::<std::result::Result<Vec<_>, _>>()
                    .map_err(|_| BlobHttpError::Invalid)?;
                if roots.is_empty() {
                    return Err(BlobHttpError::Invalid);
                }
                rustls_platform_verifier::Verifier::new_with_extra_roots(roots, provider.clone())
            } else {
                rustls_platform_verifier::Verifier::new(provider.clone())
            }
            .map_err(|_| BlobHttpError::Invalid)?;
            // Standard platform hostname/chain verifier, NOT an accept-all hook.
            // No process-global provider/key-log changes.
            let mut config = rustls::ClientConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|_| BlobHttpError::Invalid)?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth();
            config.alpn_protocols = vec![b"http/1.1".to_vec()];
            config.enable_early_data = false;
            Some(Arc::new(config))
        } else {
            None
        };
        Ok(Self {
            origin,
            host,
            port,
            tls,
        })
    }
}
trait BlobIo: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> BlobIo for T {}

/// Transport observation only. Even201 requires an authoritative gRPC Observe;
/// no renderer export/Debug/Deserialize or conversion to a persistence receipt.
pub struct BlobPutResponse {
    status: u16,
    plan: EpochBlobControlPlan,
}
impl BlobPutResponse {
    pub fn status(&self) -> u16 {
        self.status
    }
    pub fn plan(&self) -> &EpochBlobControlPlan {
        &self.plan
    }
}

#[derive(Clone)]
pub struct BlobTransferCancellation {
    token: CancellationToken,
}
impl BlobTransferCancellation {
    /// Account logout/switch/shutdown. Hiding the window must not call this.
    pub fn cancel(&self) {
        self.token.cancel();
    }
}

/// Not Clone. The account owner cancels and awaits close BEFORE releasing its
/// store/starting another owner. Dropped PUT futures cancel their own body but
/// preserve every durable operation/key/blob. No per-file task is spawned here.
pub struct NativeBlobHttp {
    endpoint: BlobHttpEndpoint,
    own: DeviceRef,
    scope: StoreScope,
    jwt: Zeroizing<String>,
    credential: Zeroizing<[u8; 32]>,
    stop: CancellationToken,
    slots: Arc<Semaphore>,
    body_done: watch::Receiver<bool>,
}
impl super::NativeEpochClient {
    pub fn blob_transport(&self, endpoint: BlobHttpEndpoint) -> HttpResult<NativeBlobHttp> {
        if StoreScope::new(&endpoint.origin, &self.own.user_id)
            .map_err(|_| BlobHttpError::Invalid)?
            != self.scope
            || self.jwt.len() + 7 > 4096
        {
            return Err(BlobHttpError::Invalid);
        }
        let (_, body_done) = watch::channel(true);
        Ok(NativeBlobHttp {
            endpoint,
            own: self.own.clone(),
            scope: self.scope.clone(),
            jwt: Zeroizing::new(self.jwt.to_string()),
            credential: Zeroizing::new(*self.credential),
            stop: CancellationToken::new(),
            slots: Arc::new(Semaphore::new(1)),
            body_done,
        })
    }
}

// The LAST body/chunk reference owns BOTH source and admission. Hyper may retain
// a queued Bytes after the send future drops. from_owner +slice keeps this Arc
// alive; there is no early permit release or second50MB ciphertext allocation.
struct BodyOwner {
    source: Option<EpochBlobUpload>,
    slot: Option<OwnedSemaphorePermit>,
    done: watch::Sender<bool>,
}
impl Drop for BodyOwner {
    fn drop(&mut self) {
        drop(self.source.take());
        drop(self.slot.take());
        let _ = self.done.send(true);
    }
}
struct ChunkOwner(Arc<BodyOwner>);
impl AsRef<[u8]> for ChunkOwner {
    fn as_ref(&self) -> &[u8] {
        self.0.source.as_ref().unwrap().bytes()
    }
}
fn upload_body(
    source: Arc<BodyOwner>,
    stop: CancellationToken,
) -> impl hyper::body::Body<Data = Bytes, Error = std::io::Error> + Send {
    let body = stream::try_unfold(
        (source, stop, 0usize),
        |(source, stop, offset)| async move {
            if stop.is_cancelled() {
                return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
            }
            let len = source.source.as_ref().unwrap().bytes().len();
            if offset == len {
                return Ok(None);
            }
            let end = (offset + CHUNK).min(len);
            let chunk = Bytes::from_owner(ChunkOwner(source.clone())).slice(offset..end);
            Ok(Some((hyper::body::Frame::data(chunk), (source, stop, end))))
        },
    );
    StreamBody::new(body)
}

pub(in crate::encryption) fn metadata(
    r: &crate::proto::e2ee_v2::BlobLeaseRequest,
    g: &crate::proto::e2ee_v2::BlobLease,
) -> HttpResult<String> {
    blob_control::validate_lease(r, g).map_err(|_| BlobHttpError::Invalid)?;
    let (s, t, b) = (
        r.sender.as_ref().unwrap(),
        r.recipient.as_ref().unwrap(),
        r.blob.as_ref().unwrap(),
    );
    let sha: String = b.sha256.iter().map(|v| format!("{v:02x}")).collect();
    // All strings are validated ASCII UUIDs/hex/decimal integers, so there is no
    // arbitrary text to escape. Order/encoding are fixed by the shared v1 vector.
    let json = format!(
        r#"{{"v":1,"message_id":"{}","conversation_id":"{}","crypto_epoch":"{}","sender_user_id":"{}","sender_generation_id":"{}","recipient_user_id":"{}","recipient_generation_id":"{}","blob_id":"{}","ciphertext_bytes":"{}","sha256":"{}","operation_id":"{}","expected_revision":"{}","expected_attempt_id":"{}","revision":"{}","attempt_id":"{}"}}"#,
        r.message_id,
        r.conversation_id,
        r.crypto_epoch.unwrap(),
        s.user_id,
        s.generation_id,
        t.user_id,
        t.generation_id,
        b.blob_id,
        b.ciphertext_bytes,
        sha,
        r.operation_id,
        r.expected_revision,
        r.expected_attempt_id,
        g.revision,
        g.attempt_id
    );
    let encoded = URL_SAFE_NO_PAD.encode(json);
    if encoded.len() > 4096 {
        return Err(BlobHttpError::Invalid);
    }
    Ok(encoded)
}
impl NativeBlobHttp {
    pub fn cancellation(&self) -> BlobTransferCancellation {
        BlobTransferCancellation {
            token: self.stop.clone(),
        }
    }
    /// Cancel and JOIN the last source/queued chunk release. A cancelled waiter
    /// does not clear ownership: call close again; do not unlock the account early.
    pub async fn close(&mut self) {
        self.stop.cancel();
        self.wait_idle().await;
    }
    /// Release barrier without stopping a reusable account transport.
    pub(in crate::encryption) async fn wait_idle(&mut self) {
        while !*self.body_done.borrow() {
            if self.body_done.changed().await.is_err() {
                break;
            }
        }
    }
    async fn connect_io(&self) -> HttpResult<Box<dyn BlobIo>> {
        let connect = async {
            let tcp =
                tokio::net::TcpStream::connect((self.endpoint.host.as_str(), self.endpoint.port))
                    .await
                    .map_err(|_| BlobHttpError::Transport)?;
            if let Some(config) = &self.endpoint.tls {
                let name = rustls::pki_types::ServerName::try_from(self.endpoint.host.clone())
                    .map_err(|_| BlobHttpError::Invalid)?;
                let tls = tokio_rustls::TlsConnector::from(config.clone())
                    .connect(name, tcp)
                    .await
                    .map_err(|_| BlobHttpError::Transport)?;
                Ok::<Box<dyn BlobIo>, BlobHttpError>(Box::new(tls))
            } else {
                Ok(Box::new(tcp) as Box<dyn BlobIo>)
            }
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), connect)
            .await
            .map_err(|_| BlobHttpError::Timeout)?
    }
    pub async fn put(&mut self, source: EpochBlobUpload) -> HttpResult<BlobPutResponse> {
        use hyper::header::{AUTHORIZATION, HeaderValue};
        if self.stop.is_cancelled() {
            return Err(BlobHttpError::Cancelled);
        }
        let r = source.plan().request();
        let g = source.grant();
        if source.plan().store_scope() != &self.scope
            || r.sender.as_ref() != Some(&self.own)
            || r.context.as_ref().map(|v| v.device_generation_id.as_str())
                != Some(self.own.generation_id.as_str())
            || g.expired
            || g.state != crate::proto::e2ee_v2::BlobLeaseState::Reserved as i32
        {
            return Err(BlobHttpError::Invalid);
        }
        let header = metadata(r, g)?;
        let b = r.blob.as_ref().unwrap();
        if b.ciphertext_bytes != source.bytes().len() as u64
            || <[u8; 32]>::from(Sha256::digest(source.bytes())).as_slice() != b.sha256.as_slice()
        {
            return Err(BlobHttpError::Invalid);
        }
        let path = format!("/api/e2ee/blobs/{}", g.attempt_id);
        let len = b.ciphertext_bytes;
        let plan = source.plan().clone();
        let mut auth =
            HeaderValue::from_str(&Zeroizing::new(format!("Bearer {}", self.jwt.as_str())))
                .map_err(|_| BlobHttpError::Invalid)?;
        auth.set_sensitive(true);
        let mut credential = HeaderValue::from_str(&Zeroizing::new(
            URL_SAFE_NO_PAD.encode(self.credential.as_slice()),
        ))
        .map_err(|_| BlobHttpError::Invalid)?;
        credential.set_sensitive(true);
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| BlobHttpError::Busy)?;
        let (done, wait) = watch::channel(false);
        self.body_done = wait;
        let owner = Arc::new(BodyOwner {
            source: Some(source),
            slot: Some(slot),
            done,
        });
        let stop = self.stop.child_token();
        let _cancel_on_drop = stop.clone().drop_guard();
        let request = hyper::Request::put(path)
            .header(
                hyper::header::HOST,
                self.endpoint
                    .origin
                    .split("://")
                    .nth(1)
                    .ok_or(BlobHttpError::Invalid)?,
            )
            .header(AUTHORIZATION, auth)
            .header("X-Zchat-Device-Credential", credential)
            .header("X-Zchat-Blob-Metadata", header)
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", len)
            .header("Accept-Encoding", "identity")
            .header("Connection", "close")
            .body(upload_body(owner, stop.clone()))
            .map_err(|_| BlobHttpError::Invalid)?;
        let exchange = async {
            let io = self.connect_io().await?;
            let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
                .max_headers(32)
                .max_buf_size(8192)
                .handshake(hyper_util::rt::TokioIo::new(io))
                .await
                .map_err(|_| BlobHttpError::Transport)?;
            let response = async {
                let response = sender
                    .send_request(request)
                    .await
                    .map_err(|_| BlobHttpError::Transport)?;
                if response.status().is_redirection() {
                    return Err(BlobHttpError::Redirect);
                }
                if let Some(n) = response.headers().get(hyper::header::CONTENT_LENGTH) {
                    let n = n
                        .to_str()
                        .ok()
                        .and_then(|s| s.parse::<u64>().ok())
                        .ok_or(BlobHttpError::Transport)?;
                    if n > MAX_REPLY as u64 {
                        return Err(BlobHttpError::ResponseTooLarge);
                    }
                }
                if response
                    .headers()
                    .get(hyper::header::CONTENT_ENCODING)
                    .is_some_and(|v| v != "identity")
                {
                    return Err(BlobHttpError::Transport);
                }
                let status = response.status().as_u16();
                let mut body = response.into_body();
                let mut bytes = 0;
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(|_| BlobHttpError::Transport)?;
                    if let Ok(chunk) = frame.into_data() {
                        bytes += chunk.len();
                        if bytes > MAX_REPLY {
                            return Err(BlobHttpError::ResponseTooLarge);
                        }
                    }
                }
                Ok(BlobPutResponse { status, plan })
            };
            // Drive the standard HTTP connection IN THIS future. Cancellation
            // drops the socket and all queued chunk owners, not just a waiter.
            let drive = async {
                connection.await.map_err(|_| BlobHttpError::Transport)?;
                std::future::pending::<HttpResult<BlobPutResponse>>().await
            };
            tokio::select! {biased;result=response=>result,result=drive=>result}
        };
        tokio::select! {biased;_=stop.cancelled()=>Err(BlobHttpError::Cancelled),result=tokio::time::timeout(PUT_TIMEOUT,exchange)=>result.map_err(|_|BlobHttpError::Timeout)?}
    }
}

/// Exclusive borrow of the SAME account's RPC and HTTP transports. No cloned
/// channel, background task, endpoint selection or authentication recovery.
pub struct NativeBlobUploadTransport<'a> {
    rpc: &'a mut super::NativeEpochClient,
    http: &'a mut NativeBlobHttp,
}
impl super::NativeEpochClient {
    pub fn upload_transport<'a>(
        &'a mut self,
        http: &'a mut NativeBlobHttp,
    ) -> HttpResult<NativeBlobUploadTransport<'a>> {
        if self.scope != http.scope
            || self.own != http.own
            || self.jwt.as_str() != http.jwt.as_str()
            || *self.credential != *http.credential
        {
            return Err(BlobHttpError::Invalid);
        }
        Ok(NativeBlobUploadTransport { rpc: self, http })
    }
}
#[async_trait::async_trait]
impl crate::encryption::blob_upload::EpochBlobUploadTransport for NativeBlobUploadTransport<'_> {
    async fn observe(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> Result<super::EpochBlobControlOutcome> {
        if self.http.stop.is_cancelled() {
            return Err(RpcError::Status(Code::Cancelled));
        }
        self.rpc.observe_blob(plan).await
    }
    async fn reserve(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> Result<super::EpochBlobControlOutcome> {
        if self.http.stop.is_cancelled() {
            return Err(RpcError::Status(Code::Cancelled));
        }
        self.rpc.reserve_blob(plan).await
    }
    async fn put(&mut self, source: EpochBlobUpload) -> HttpResult<u16> {
        self.http.put(source).await.map(|result| result.status())
    }
    async fn join(&mut self) {
        self.http.wait_idle().await;
    }
    fn cancel(&mut self) {
        self.http.stop.cancel();
    }
}

impl Drop for NativeBlobHttp {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

#[cfg(test)]
#[path = "native_blob_http_tests.rs"]
mod tests;

#[path = "native_blob_download.rs"]
mod download;
pub use download::{
    BlobCiphertextChunk, BlobCiphertextSink, BlobGetResponse, BlobHeadResponse, BlobReadError,
    BlobReadRequest, BlobSinkError,
};
