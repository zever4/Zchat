// pub use grpc::GrpcClient;
pub use store::Stores;

pub mod account_owner;
pub mod attachment;
pub mod attachment_payload;
pub mod blob_control;
pub mod blob_download;
mod blob_retry;
pub mod blob_upload;
pub mod contract;
pub mod crypto;
pub mod delivery_runner;
pub mod discovery;
pub mod epoch_contract;
pub mod epoch_rpc;
pub mod epoch_runner;
pub mod file_ack;
pub mod file_status;
pub mod grpc;
pub mod native;
#[cfg(feature = "compression")]
pub mod photo_preparation;
pub mod prekey_maintenance;
pub mod protected;
pub mod provisioning;
pub mod publication;
pub mod recovery_contract;
pub mod signal_state;
pub mod store;
pub mod voice_format;

#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("legacy E2EE transport is retired; use the authenticated epoch transport")]
    LegacyTransportRetired,
    #[error("signal protocol error: {0}")]
    Signal(#[from] libsignal_protocol::SignalProtocolError),
    #[error("libsignal curve error: {0}")]
    SignalCurve(String),
    #[error("invalid device id: {0}")]
    InvalidDeviceId(String),
    #[error("local user information is missing or empty")]
    MissingUserInfo,
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("grpc error: {0}")]
    Grpc(#[from] tonic::transport::Error),
    #[error("grpc status: {0}")]
    GrpcStatus(#[from] tonic::Status),
    #[error("bundle missing field: {0}")]
    MissingBundleField(String),
    #[error("missing last resort kyber prekey in bundle")]
    MissingLastResortKyberPreKey,
    #[error("time error: {0}")]
    Time(#[from] std::time::SystemTimeError),
}

pub type Result<T> = std::result::Result<T, EncryptionError>;
