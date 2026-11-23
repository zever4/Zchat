use thiserror::Error;

#[derive(Error, Debug)]
pub enum CompressionError {
    #[error("Failed to detect image format: {0}")]
    DetectionFailed(String),

    #[error("Invalid quality value: {0}")]
    InvalidQuality(u8),

    #[error("Invalid maximum image dimension: {0}")]
    InvalidMaxDimension(u32),

    #[error("Image input exceeds 20 MiB")]
    InputTooLarge,

    #[error("Image exceeds dimension or decoded allocation limits")]
    ImageTooLarge,

    #[error("Failed to decode image: {0}")]
    DecodeFailed(String),

    #[error("Failed to encode image: {0}")]
    EncodeFailed(String),

    #[error("Unsupported image format")]
    UnsupportedFormat,

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
}

#[cfg(feature = "e2ee")]
impl From<CompressionError> for tonic::Status {
    fn from(err: CompressionError) -> Self {
        match err {
            CompressionError::DetectionFailed(_) | CompressionError::UnsupportedFormat => {
                tonic::Status::invalid_argument("Invalid or unsupported image format")
            }

            CompressionError::DecodeFailed(_) => {
                tonic::Status::invalid_argument("Failed to decode image")
            }

            CompressionError::InputTooLarge | CompressionError::ImageTooLarge => {
                tonic::Status::resource_exhausted("Image exceeds processing limits")
            }

            CompressionError::InvalidQuality(_) | CompressionError::InvalidMaxDimension(_) => {
                tonic::Status::invalid_argument("Invalid quality value")
            }

            CompressionError::EncodeFailed(e) => {
                tonic::Status::internal(format!("Compression failed: {}", e))
            }

            CompressionError::IoError(e) => tonic::Status::internal(format!("I/O error: {}", e)),
        }
    }
}
