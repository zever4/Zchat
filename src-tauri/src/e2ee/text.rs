//! Explicit user text only. Native code owns bindings, timestamp, bootstrap
//! and the atomic protected outbox COMMIT. No plaintext/ciphertext in replies.
use super::{DesktopE2ee, Reason};
use serde::{Deserialize, Serialize};
use tauri::ipc::Response;
use zeroize::Zeroizing;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TextRequest {
    pub conversation_id: String,
    pub message_id: String,
    #[serde(deserialize_with = "plaintext")]
    pub text: Zeroizing<String>,
}
fn plaintext<'de, D: serde::Deserializer<'de>>(decoder: D) -> Result<Zeroizing<String>, D::Error> {
    String::deserialize(decoder).map(Zeroizing::new)
}
impl TextRequest {
    pub fn validate(&self) -> Result<(), Reason> {
        if !super::admission::valid_id(&self.conversation_id)
            || !super::admission::valid_id(&self.message_id)
            || self.text.is_empty()
            || self.text.len() > 2000
        {
            return Err(Reason::Invalid);
        }
        Ok(())
    }
}
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TextQueued {
    pub conversation_id: String,
    pub message_id: String,
    pub crypto_epoch: String,
    pub created_at_unix_ms: String,
    pub stored_on_server: bool,
    pub sealed_unsent: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Reply {
    context: u64,
    queued: TextQueued,
}
pub(crate) async fn execute(
    state: &DesktopE2ee,
    context: u64,
    request_id: String,
    input: TextRequest,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    input.validate()?;
    state.manager.ensure_dialog_ready(context)?;
    let guard = state.admission.begin(context, request_id)?;
    let queued = tokio::select! {
        biased;
        _ = guard.cancel.cancelled() => return Err(Reason::Stale),
        r = tokio::time::timeout(std::time::Duration::from_secs(30), state.manager.send_text(context,input)) => r.map_err(|_|Reason::NativeWait)??,
    };
    check_page()?;
    state.manager.ensure_dialog_ready(context)?;
    guard.deliver(context, &Reply { context, queued })
}
#[cfg(feature = "experimental-e2ee")]
pub(super) async fn request(
    client: &rust_processor::encryption::account_owner::AccountClient,
    input: TextRequest,
) -> Result<TextQueued, Reason> {
    let view = client
        .send_text(&input.conversation_id, &input.message_id, &input.text)
        .await
        .map_err(super::factory::account_error)?;
    Ok(TextQueued {
        conversation_id: view.conversation_id,
        message_id: view.message_id,
        crypto_epoch: view.crypto_epoch.to_string(),
        created_at_unix_ms: view.created_at_unix_ms.to_string(),
        stored_on_server: view.stored_on_server,
        sealed_unsent: view.sealed_unsent,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn text_inputs_use_utf8_bytes_and_never_accept_scope_or_timestamp() {
        let id = "11111111-1111-4111-8111-111111111111";
        let request = |text: String| TextRequest {
            conversation_id: id.into(),
            message_id: id.into(),
            text: Zeroizing::new(text),
        };
        assert!(request("я".repeat(1000)).validate().is_ok());
        for text in [String::new(), "я".repeat(1001), "🦀".repeat(501)] {
            assert_eq!(request(text).validate(), Err(Reason::Invalid));
        }
        for extra in [
            "cryptoEpoch",
            "createdAtUnixMs",
            "scope",
            "generation",
            "sender",
            "keys",
        ] {
            let body = serde_json::json!({"conversationId": id, "messageId": id, "text": "x", (extra): "forged"});
            assert!(serde_json::from_value::<TextRequest>(body).is_err());
        }
    }
}
