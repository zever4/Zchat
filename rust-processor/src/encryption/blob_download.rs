//! Trusted native ciphertext reader. The caller owns account cancellation and
//! joins HTTP chunks and the serial crypto worker before releasing its store.
use super::native::{
    BlobCiphertextSink, BlobGetResponse, BlobReadError, BlobReadRequest, NativeBlobHttp,
};

#[async_trait::async_trait]
pub trait EpochBlobDownloadTransport: Send {
    async fn get(
        &mut self,
        request: BlobReadRequest,
        sink: &mut dyn BlobCiphertextSink,
    ) -> Result<BlobGetResponse, BlobReadError>;
}
#[async_trait::async_trait]
impl EpochBlobDownloadTransport for NativeBlobHttp {
    async fn get(
        &mut self,
        request: BlobReadRequest,
        sink: &mut dyn BlobCiphertextSink,
    ) -> Result<BlobGetResponse, BlobReadError> {
        NativeBlobHttp::get(self, request, sink).await
    }
}
