//! Narrow public metadata only. Requests select a peer/chat, never account scope,
//! keys, server bindings or protobuf. Create/Get do not initialize a Signal epoch.
use super::{DesktopE2ee, Reason};
use serde::{Deserialize, Serialize};
use tauri::ipc::Response;

#[derive(Deserialize)]
#[serde(
    tag = "action",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DialogRequest {
    Create {
        peer_user_id: String,
    },
    CreateAfterClosed {
        peer_user_id: String,
        closed_conversation_id: String,
        confirmed: bool,
    },
    Get {
        conversation_id: String,
    },
}
impl DialogRequest {
    pub fn validate(&self) -> Result<(), Reason> {
        match self {
            Self::Create { peer_user_id } => peer_id(peer_user_id).map(|_| ()),
            Self::CreateAfterClosed {
                peer_user_id,
                closed_conversation_id,
                confirmed: true,
            } if super::admission::valid_id(closed_conversation_id) => {
                peer_id(peer_user_id).map(|_| ())
            }
            Self::Get { conversation_id } if super::admission::valid_id(conversation_id) => Ok(()),
            _ => Err(Reason::Invalid),
        }
    }
}
fn peer_id(value: &str) -> Result<i64, Reason> {
    if value.is_empty() || value.len() > 19 || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Reason::Invalid);
    }
    let id = value.parse::<i64>().map_err(|_| Reason::Invalid)?;
    if id <= 0 || id.to_string() != value {
        return Err(Reason::Invalid);
    }
    Ok(id)
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Dialog {
    pub conversation_id: String,
    pub peer_user_id: String,
    pub state: String,
    pub close_reason: Option<String>,
    // Decimal wire strings avoid silently rounding i64 in JavaScript.
    pub closed_at_unix_ms: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DialogReply {
    context: u64,
    conversation: Dialog,
}
/// Shared by actual Tauri and the native integration harness. Page checks bracket
/// the await and serialization/admission happens only after the current account
/// is rechecked. An abandoned caller does not fabricate a server rollback/ACK.
pub(crate) async fn execute(
    state: &DesktopE2ee,
    context: u64,
    request_id: String,
    request: DialogRequest,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    request.validate()?;
    state.manager.ensure_dialog_ready(context)?;
    let guard = state.admission.begin(context, request_id)?;
    let conversation = tokio::select! {
        biased;
        _ = guard.cancel.cancelled() => return Err(Reason::Stale),
        r = tokio::time::timeout(std::time::Duration::from_secs(30),state.manager.dialog(context,request)) => r.map_err(|_|Reason::NativeWait)??,
    };
    check_page()?;
    state.manager.ensure_dialog_ready(context)?;
    guard.deliver(
        context,
        &DialogReply {
            context,
            conversation,
        },
    )
}

#[cfg(feature = "experimental-e2ee")]
pub(super) async fn request(
    client: &rust_processor::encryption::account_owner::AccountClient,
    request: DialogRequest,
) -> Result<Dialog, Reason> {
    use rust_processor::proto::e2ee::{ConversationCloseReason, ConversationState};
    let view = match request {
        DialogRequest::Create { peer_user_id } => {
            client.create_dialog(peer_id(&peer_user_id)?).await
        }
        DialogRequest::Get { conversation_id } => client.get_dialog(&conversation_id).await,
        DialogRequest::CreateAfterClosed {
            peer_user_id,
            closed_conversation_id,
            confirmed: true,
        } => {
            client
                .create_dialog_after_closed(peer_id(&peer_user_id)?, &closed_conversation_id)
                .await
        }
        DialogRequest::CreateAfterClosed {
            confirmed: false, ..
        } => return Err(Reason::Invalid),
    }
    .map_err(super::factory::account_error)?;
    let state = match view.state {
        ConversationState::Active => "active",
        ConversationState::Closed => "closed",
        _ => return Err(Reason::Runtime),
    };
    let close_reason = match view.close_reason {
        None => None,
        Some(ConversationCloseReason::DeviceReplaced) => Some("device_replaced".into()),
        _ => return Err(Reason::Runtime),
    };
    Ok(Dialog {
        conversation_id: view.conversation_id,
        peer_user_id: view.peer_user_id,
        state: state.into(),
        close_reason,
        closed_at_unix_ms: view.closed_at_unix_ms.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn successor_requires_explicit_consent_canonical_peer_chat_and_no_journal_claims() {
        let valid = serde_json::json!({"action":"create_after_closed", "peerUserId":"42", "closedConversationId":"11111111-1111-4111-8111-111111111111", "confirmed":true});
        assert!(
            serde_json::from_value::<DialogRequest>(valid.clone())
                .unwrap()
                .validate()
                .is_ok()
        );
        for (field, value) in [
            ("confirmed", serde_json::json!(false)),
            ("closedConversationId", serde_json::json!("../store")),
            ("peerUserId", serde_json::json!("042")),
        ] {
            let mut bad = valid.clone();
            bad[field] = value;
            assert_eq!(
                serde_json::from_value::<DialogRequest>(bad)
                    .unwrap()
                    .validate(),
                Err(Reason::Invalid)
            );
        }
        for field in ["closedRequestId", "generation", "scope", "request", "path"] {
            let mut bad = valid.clone();
            bad[field] = "forged".into();
            assert!(serde_json::from_value::<DialogRequest>(bad).is_err());
        }
        let mut missing = valid;
        missing.as_object_mut().unwrap().remove("confirmed");
        assert!(serde_json::from_value::<DialogRequest>(missing).is_err());
    }
    #[test]
    fn inputs_are_canonical_and_reject_scope_target_keys_and_unsafe_ids() {
        for peer in [
            "",
            "0",
            "01",
            "-1",
            "+1",
            "1.0",
            "9223372036854775808",
            "../path",
        ] {
            assert!(
                DialogRequest::Create {
                    peer_user_id: peer.into()
                }
                .validate()
                .is_err()
            );
        }
        for peer in ["1", "9007199254740993", "9223372036854775807"] {
            assert!(
                DialogRequest::Create {
                    peer_user_id: peer.into()
                }
                .validate()
                .is_ok()
            );
        }
        assert!(
            DialogRequest::Get {
                conversation_id: "11111111-1111-4111-8111-111111111111".into()
            }
            .validate()
            .is_ok()
        );
        for body in [
            r#"{"action":"create","peerUserId":1}"#,
            r#"{"action":"create","peerUserId":"1","scope":"forged"}"#,
            r#"{"action":"get","conversationId":"11111111-1111-4111-8111-111111111111","generation":"forged"}"#,
            r#"{"action":"send","text":"unsupported"}"#,
        ] {
            assert!(serde_json::from_str::<DialogRequest>(body).is_err());
        }
    }
}
