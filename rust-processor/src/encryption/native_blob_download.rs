//! Scoped GET/HEAD, not a protected history/cache/AEAD/file-ACK implementation.
//! Request fields are public identity only; the server authorizes each attempt.
use super::*;
use crate::encryption::{epoch_contract, recovery_contract};
use crate::proto::{e2ee as common, e2ee_v2 as v2};
use http_body_util::Empty;
use hyper::header::{self, HeaderValue};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum BlobReadError {
    #[error("invalid native blob read request or account scope")]
    Invalid,
    #[error("blob download transport failed")]
    Http(#[from] BlobHttpError),
    #[error("blob read rejected with HTTP status {0}; not a Send outcome")]
    Rejected(u16),
    #[error("invalid blob download response headers")]
    Response,
    #[error("downloaded ciphertext size or SHA mismatch")]
    Integrity,
    #[error("ciphertext sink failed")]
    Sink,
}
type ReadResult<T> = std::result::Result<T, BlobReadError>;

/// Public read identity, NOT proof of authenticated local history or permission
/// to mutate/ACK it. Future cache code must derive this from saved Signal data.
/// No URLs/attempt IDs/file keys/private description; no renderer Deserialize.
#[derive(Clone)]
pub struct BlobReadRequest {
    scope: StoreScope,
    binding: common::EpochSendBinding,
    blob: v2::BlobRef,
}
impl BlobReadRequest {
    pub fn new(
        scope: StoreScope,
        binding: common::EpochSendBinding,
        blob: v2::BlobRef,
    ) -> ReadResult<Self> {
        recovery_contract::validate_send_binding(&binding).map_err(|_| BlobReadError::Invalid)?;
        epoch_contract::validate_blob_ref(&blob).map_err(|_| BlobReadError::Invalid)?;
        if binding.protocol_version != 3 {
            return Err(BlobReadError::Invalid);
        }
        Ok(Self {
            scope,
            binding,
            blob,
        })
    }
    pub fn store_scope(&self) -> &StoreScope {
        &self.scope
    }
    pub fn binding(&self) -> &common::EpochSendBinding {
        &self.binding
    }
    pub fn blob(&self) -> &v2::BlobRef {
        &self.blob
    }
    fn metadata(&self) -> ReadResult<String> {
        let b = &self.binding;
        let s = b.sender.as_ref().ok_or(BlobReadError::Invalid)?;
        let r = b.recipient.as_ref().ok_or(BlobReadError::Invalid)?;
        let digest: String = b
            .envelope_digest
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect();
        let sha: String = self
            .blob
            .sha256
            .iter()
            .map(|v| format!("{v:02x}"))
            .collect();
        // Validated ASCII UUID/hex/integer fields need no arbitrary-text escape.
        let json = format!(
            r#"{{"v":1,"message_id":"{}","conversation_id":"{}","crypto_epoch":"{}","sender_user_id":"{}","sender_generation_id":"{}","recipient_user_id":"{}","recipient_generation_id":"{}","envelope_digest":"{}","blob_id":"{}","ciphertext_bytes":"{}","sha256":"{}"}}"#,
            b.message_id,
            b.conversation_id,
            b.epoch,
            s.user_id,
            s.generation_id,
            r.user_id,
            r.generation_id,
            digest,
            self.blob.blob_id,
            self.blob.ciphertext_bytes,
            sha
        );
        let encoded = URL_SAFE_NO_PAD.encode(json);
        if encoded.len() > 4096 {
            return Err(BlobReadError::Invalid);
        }
        Ok(encoded)
    }
}

/// HEAD is metadata only. Neither response is a durable cache, AEAD or file ACK.
pub struct BlobHeadResponse {
    request: BlobReadRequest,
}
impl BlobHeadResponse {
    pub fn request(&self) -> &BlobReadRequest {
        &self.request
    }
}
pub struct BlobGetResponse {
    request: BlobReadRequest,
}
impl BlobGetResponse {
    pub fn request(&self) -> &BlobReadRequest {
        &self.request
    }
    #[cfg(test)]
    pub(in crate::encryption) fn test_complete(request: BlobReadRequest) -> Self {
        Self { request }
    }
}

struct ReadOwner {
    slot: Option<OwnedSemaphorePermit>,
    done: watch::Sender<bool>,
}
impl Drop for ReadOwner {
    fn drop(&mut self) {
        drop(self.slot.take());
        let _ = self.done.send(true);
    }
}

/// Opaque ciphertext and LAST-work ownership. A sink/SQLite worker must retain
/// this value until its work finishes; never copy-and-spawn unjoined writes.
/// No Clone/Serialize/Debug or file key. Ciphertext is unverified until GET ends.
pub struct BlobCiphertextChunk {
    bytes: Bytes,
    _owner: Arc<ReadOwner>,
}
impl BlobCiphertextChunk {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
    #[cfg(test)]
    pub(in crate::encryption) fn test_chunk(bytes: &[u8]) -> Self {
        let (done, _) = watch::channel(false);
        Self {
            bytes: Bytes::copy_from_slice(bytes),
            _owner: Arc::new(ReadOwner {
                slot: Some(
                    Arc::new(tokio::sync::Semaphore::new(1))
                        .try_acquire_owned()
                        .unwrap(),
                ),
                done,
            }),
        }
    }
}
#[derive(Debug, thiserror::Error)]
#[error("ciphertext sink failed")]
pub struct BlobSinkError;

#[async_trait::async_trait]
pub trait BlobCiphertextSink: Send {
    async fn write(&mut self, chunk: BlobCiphertextChunk)
    -> std::result::Result<(), BlobSinkError>;
}

enum ReadResponse {
    Head(BlobHeadResponse),
    Get(BlobGetResponse),
}
struct NoBodySink;
#[async_trait::async_trait]
impl BlobCiphertextSink for NoBodySink {
    async fn write(&mut self, _: BlobCiphertextChunk) -> std::result::Result<(), BlobSinkError> {
        Err(BlobSinkError)
    }
}

fn exact(headers: &hyper::HeaderMap, name: &str, value: &str) -> bool {
    headers.get_all(name).iter().count() == 1
        && headers
            .get(name)
            .is_some_and(|v| v.as_bytes() == value.as_bytes())
}
fn response_headers(headers: &hyper::HeaderMap, expected: u64) -> ReadResult<()> {
    if !exact(headers, "content-length", &expected.to_string())
        || !exact(headers, "content-type", "application/octet-stream")
        || !exact(headers, "cache-control", "private, no-store")
        || !exact(headers, "x-content-type-options", "nosniff")
        || !exact(
            headers,
            "content-disposition",
            "attachment; filename=\"encrypted.bin\"",
        )
        || !exact(headers, "accept-ranges", "none")
        || headers.contains_key(header::TRANSFER_ENCODING)
        || headers.contains_key(header::CONTENT_ENCODING)
        || headers.contains_key(header::CONTENT_RANGE)
    {
        return Err(BlobReadError::Response);
    }
    Ok(())
}

impl NativeBlobHttp {
    #[cfg(test)]
    pub(in crate::encryption) fn test_reader(origin: &str, own: common::DeviceRef) -> Self {
        crate::encryption::native::NativeEpochClient {
            client: v2::messenger_client::MessengerClient::new(
                tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            scope: StoreScope::new(origin, &own.user_id).unwrap(),
            own,
            jwt: Zeroizing::new("synthetic-jwt".into()),
            credential: Zeroizing::new([37; 32]),
        }
        .blob_transport(BlobHttpEndpoint::new(origin, None).unwrap())
        .unwrap()
    }
    pub async fn head(&mut self, request: BlobReadRequest) -> ReadResult<BlobHeadResponse> {
        match self.read_blob(request, &mut NoBodySink, true).await? {
            ReadResponse::Head(r) => Ok(r),
            ReadResponse::Get(_) => Err(BlobReadError::Response),
        }
    }
    pub async fn get<S: BlobCiphertextSink + ?Sized>(
        &mut self,
        request: BlobReadRequest,
        sink: &mut S,
    ) -> ReadResult<BlobGetResponse> {
        match self.read_blob(request, sink, false).await? {
            ReadResponse::Get(r) => Ok(r),
            ReadResponse::Head(_) => Err(BlobReadError::Response),
        }
    }
    async fn read_blob<S: BlobCiphertextSink + ?Sized>(
        &mut self,
        request: BlobReadRequest,
        sink: &mut S,
        head: bool,
    ) -> ReadResult<ReadResponse> {
        if self.stop.is_cancelled() {
            return Err(BlobHttpError::Cancelled.into());
        }
        if request.scope != self.scope || request.binding.recipient.as_ref() != Some(&self.own) {
            return Err(BlobReadError::Invalid);
        }
        let metadata = request.metadata()?;
        let mut auth =
            HeaderValue::from_str(&Zeroizing::new(format!("Bearer {}", self.jwt.as_str())))
                .map_err(|_| BlobReadError::Invalid)?;
        auth.set_sensitive(true);
        let mut credential = HeaderValue::from_str(&Zeroizing::new(
            URL_SAFE_NO_PAD.encode(self.credential.as_slice()),
        ))
        .map_err(|_| BlobReadError::Invalid)?;
        credential.set_sensitive(true);
        let message = hyper::Request::builder()
            .method(if head { "HEAD" } else { "GET" })
            .uri(format!(
                "/api/e2ee/attachments/{}",
                request.binding.message_id
            ))
            .header(
                header::HOST,
                self.endpoint
                    .origin
                    .split("://")
                    .nth(1)
                    .ok_or(BlobReadError::Invalid)?,
            )
            .header(header::AUTHORIZATION, auth)
            .header("X-Zchat-Device-Credential", credential)
            .header("X-Zchat-Blob-Read-Metadata", metadata)
            .header(header::ACCEPT_ENCODING, "identity")
            .header(header::CONNECTION, "close")
            .body(Empty::<Bytes>::new())
            .map_err(|_| BlobReadError::Invalid)?;
        let slot = self
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| BlobHttpError::Busy)?;
        let (done, wait) = watch::channel(false);
        self.body_done = wait;
        let owner = Arc::new(ReadOwner {
            slot: Some(slot),
            done,
        });
        let stop = self.stop.child_token();
        let _cancel_on_drop = stop.clone().drop_guard();
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
                    .send_request(message)
                    .await
                    .map_err(|_| BlobHttpError::Transport)?;
                if response.status().is_redirection() {
                    return Err(BlobHttpError::Redirect.into());
                }
                if response.status() != hyper::StatusCode::OK {
                    return Err(BlobReadError::Rejected(response.status().as_u16()));
                }
                response_headers(response.headers(), request.blob.ciphertext_bytes)?;
                let mut body = response.into_body();
                let mut received = 0u64;
                let mut digest = Sha256::new();
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(|_| BlobHttpError::Transport)?;
                    let bytes = frame.into_data().map_err(|_| BlobReadError::Response)?;
                    if bytes.is_empty() {
                        continue;
                    }
                    if head || bytes.len() as u64 > request.blob.ciphertext_bytes - received {
                        return Err(BlobReadError::Integrity);
                    }
                    for part in (0..bytes.len()).step_by(CHUNK) {
                        if stop.is_cancelled() {
                            return Err(BlobHttpError::Cancelled.into());
                        }
                        let end = (part + CHUNK).min(bytes.len());
                        let bytes = bytes.slice(part..end);
                        received += bytes.len() as u64;
                        digest.update(&bytes);
                        sink.write(BlobCiphertextChunk {
                            bytes,
                            _owner: owner.clone(),
                        })
                        .await
                        .map_err(|_| BlobReadError::Sink)?;
                    }
                }
                if head {
                    return Ok(ReadResponse::Head(BlobHeadResponse { request }));
                }
                if received != request.blob.ciphertext_bytes
                    || digest.finalize().as_slice() != request.blob.sha256.as_slice()
                {
                    return Err(BlobReadError::Integrity);
                }
                Ok(ReadResponse::Get(BlobGetResponse { request }))
            };
            let drive = async {
                connection.await.map_err(|_| BlobHttpError::Transport)?;
                std::future::pending::<ReadResult<ReadResponse>>().await
            };
            tokio::select! {biased;result=response=>result,result=drive=>result}
        };
        tokio::select! {biased;_=stop.cancelled()=>Err(BlobHttpError::Cancelled.into()),result=tokio::time::timeout(PUT_TIMEOUT,exchange)=>result.map_err(|_|BlobHttpError::Timeout)?}
    }
}

#[cfg(test)]
#[path = "native_blob_download_tests.rs"]
mod tests;
