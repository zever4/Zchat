//! Method-specific authenticated Send observation. No mutation or retry authority.
use super::*;

pub(super) fn failure(
    status: &tonic::Status,
    request: &v2::SendMessageRequest,
) -> Option<v2::BlobSendFailure> {
    if status.code() != Code::FailedPrecondition {
        return None;
    }
    let bytes = detail(status, "type.googleapis.com/zchat.e2ee.v2.BlobSendFailure")?;
    let f = v2::BlobSendFailure::decode(bytes.as_slice()).ok()?;
    crate::encryption::blob_control::validate_send_failure(request, &f).ok()?;
    Some(f)
}

#[cfg(test)]
#[path = "native_epoch_blob_send_tests.rs"]
mod tests;
