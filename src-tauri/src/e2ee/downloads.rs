//! Explicit file retrieval by the current account. Old installations use the
//! separate read-only archive API and cannot dispatch this command.
use super::{AttachmentTarget, DesktopE2ee, Reason};
use tauri::ipc::Response;

pub(crate) async fn execute(
    state: &DesktopE2ee,
    context: u64,
    request_id: String,
    target: AttachmentTarget,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    target.validate()?;
    #[cfg(feature = "experimental-e2ee")]
    {
        let (client, session_cancel) = state.manager.dialog_client(context)?;
        let guard = state.admission.begin(context, request_id)?;
        let download = async {
            client
                .download_attachment_in(&target.conversation_id, &target.message_id)
                .await
                .map_err(download_error)
        };
        let result = tokio::select! {
            biased;
            _ = guard.cancel.cancelled() => return Err(Reason::Stale),
            _ = session_cancel.cancelled() => return Err(Reason::Stale),
            r = tokio::time::timeout(std::time::Duration::from_secs(70), download) => r.map_err(|_| Reason::NativeWait)?,
        };
        check_page()?;
        state.manager.snapshot(context)?;
        result?;
        state.manager.ensure_dialog_ready(context)?;
        guard.deliver(
            context,
            &Reply {
                context,
                attachment: target,
            },
        )
    }
    #[cfg(not(feature = "experimental-e2ee"))]
    {
        let _ = request_id;
        state.manager.ensure_dialog_ready(context)?;
        Err(Reason::NotReady)
    }
}

#[cfg(feature = "experimental-e2ee")]
#[derive(serde::Serialize)]
struct Reply {
    context: u64,
    attachment: AttachmentTarget,
}

#[cfg(feature = "experimental-e2ee")]
fn download_error(e: rust_processor::encryption::account_owner::AccountError) -> Reason {
    use rust_processor::encryption::{
        account_owner::AccountError,
        native::{BlobHttpError, BlobReadError},
        signal_state::CryptoError,
    };
    match e {
        AccountError::AttachmentUnavailable => Reason::NotReady,
        AccountError::Crypto(CryptoError::AttachmentRemoved) => Reason::AttachmentRemoved,
        AccountError::Crypto(CryptoError::MessageUnavailable) => Reason::NotFound,
        AccountError::Attachment(BlobReadError::Rejected(401)) => Reason::Authentication,
        AccountError::Attachment(BlobReadError::Rejected(404)) => Reason::AttachmentUnavailable,
        AccountError::Attachment(
            BlobReadError::Rejected(429) | BlobReadError::Http(BlobHttpError::Busy),
        ) => Reason::Busy,
        AccountError::Attachment(
            BlobReadError::Rejected(408) | BlobReadError::Http(BlobHttpError::Timeout),
        ) => Reason::NativeWait,
        AccountError::Attachment(BlobReadError::Integrity | BlobReadError::Response) => {
            Reason::AttachmentIntegrity
        }
        AccountError::Attachment(_) => Reason::Remote,
        other => super::factory::account_error(other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn download_requires_current_page_context_and_active_owner() {
        let state = DesktopE2ee::new(Default::default(), Default::default());
        let chat = "11111111-1111-4111-8111-111111111111";
        let message = "22222222-2222-4222-8222-222222222222";
        let request = "33333333-3333-4333-8333-333333333333";
        for (context, page, id, reason) in [
            (1, Err(Reason::Stale), message, Reason::Stale),
            (1, Ok(()), "invalid", Reason::Invalid),
            (0, Ok(()), message, Reason::Stale),
            (1, Ok(()), message, Reason::NotReady),
        ] {
            let target = AttachmentTarget {
                conversation_id: chat.into(),
                message_id: id.into(),
            };
            let result = execute(&state, context, request.into(), target, || page).await;
            assert_eq!(result.err(), Some(reason));
        }
    }

    #[cfg(feature = "experimental-e2ee")]
    #[test]
    fn remote_unavailability_and_local_removal_have_distinct_reasons() {
        use rust_processor::encryption::{
            account_owner::AccountError, native::BlobReadError, signal_state::CryptoError,
        };
        assert_eq!(
            download_error(AccountError::Attachment(BlobReadError::Rejected(404))),
            Reason::AttachmentUnavailable
        );
        assert_eq!(
            download_error(AccountError::Crypto(CryptoError::AttachmentRemoved)),
            Reason::AttachmentRemoved
        );
    }
}
