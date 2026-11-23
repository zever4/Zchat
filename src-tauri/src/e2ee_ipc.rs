//! Session lifetime, explicit provisioning/replacement and bounded conversation metadata.
use crate::{
    DesktopState, MAIN_WINDOW,
    e2ee::{Reason, Snapshot},
    is_app_url,
};
use std::time::Duration;
use tauri::{State, WebviewWindow, ipc::Response};

fn page(window: &WebviewWindow, state: &DesktopState, epoch: u64) -> Result<(), Reason> {
    let url = window.url().map_err(|_| Reason::Invalid)?;
    validate_page(window.label(), &url, state.transport.snapshot().1, epoch)
}
fn validate_page(label: &str, url: &tauri::Url, current: u64, epoch: u64) -> Result<(), Reason> {
    if label != MAIN_WINDOW || !is_app_url(url) || url.path() != "/chat.html" {
        return Err(Reason::Invalid);
    }
    if current != epoch {
        return Err(Reason::Stale);
    }
    Ok(())
}
#[tauri::command]
pub async fn e2ee_prepare_attachment(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    request: tauri::ipc::Request<'_>,
) -> Result<Response, Reason> {
    let tauri::ipc::InvokeBody::Raw(bytes) = request.body() else {
        return Err(Reason::Invalid);
    };
    let (frame, source) = crate::e2ee::attachment_input::decode(bytes)?;
    let epoch = frame.epoch;
    crate::e2ee::attachment_input::execute(&state.e2ee, frame, source, || {
        page(&window, &state, epoch)
    })
    .await
}
#[tauri::command]
pub async fn e2ee_attachment_input(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    query: crate::e2ee::AttachmentTarget,
) -> Result<Response, Reason> {
    crate::e2ee::attachment_input::lookup(&state.e2ee, context, request_id, query, || {
        page(&window, &state, epoch)
    })
    .await
}
#[tauri::command]
pub async fn e2ee_download_attachment(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    target: crate::e2ee::AttachmentTarget,
) -> Result<Response, Reason> {
    crate::e2ee::downloads::execute(&state.e2ee, context, request_id, target, || {
        page(&window, &state, epoch)
    })
    .await
}
#[tauri::command]
pub async fn e2ee_local_attachment(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    target: crate::e2ee::local_files::Target,
    operation: crate::e2ee::local_files::Operation,
) -> Result<Response, Reason> {
    crate::e2ee::local_files::execute(
        &window,
        &state.e2ee,
        context,
        request_id,
        target,
        operation,
        || page(&window, &state, epoch),
    )
    .await
}
#[tauri::command]
pub async fn e2ee_recovery(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    operation: crate::e2ee::recovery::RecoveryRequest,
) -> Result<Response, Reason> {
    crate::e2ee::recovery::execute(&state.e2ee, context, request_id, operation, || {
        page(&window, &state, epoch)
    })
    .await
}
#[tauri::command]
pub async fn e2ee_read(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    query: crate::e2ee::pages::PageRequest,
) -> Result<Response, Reason> {
    crate::e2ee::pages::execute(&state.e2ee, context, request_id, query, || {
        page(&window, &state, epoch)
    })
    .await
}
#[tauri::command]
pub async fn e2ee_send_text(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    message: crate::e2ee::text::TextRequest,
) -> Result<Response, Reason> {
    crate::e2ee::text::execute(&state.e2ee, context, request_id, message, || {
        page(&window, &state, epoch)
    })
    .await
}
#[tauri::command]
pub async fn e2ee_conversation(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    operation: crate::e2ee::dialogs::DialogRequest,
) -> Result<Response, Reason> {
    crate::e2ee::dialogs::execute(&state.e2ee, context, request_id, operation, || {
        page(&window, &state, epoch)
    })
    .await
}
#[tauri::command]
pub async fn e2ee_session(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    token: Option<String>,
) -> Result<Response, Reason> {
    transition(
        window,
        state,
        epoch,
        context,
        request_id,
        Action::Bind(token),
    )
    .await
}
#[tauri::command]
pub async fn e2ee_setup(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
) -> Result<Response, Reason> {
    transition(window, state, epoch, context, request_id, Action::Setup).await
}
#[tauri::command]
pub async fn e2ee_retry(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
) -> Result<Response, Reason> {
    transition(window, state, epoch, context, request_id, Action::Retry).await
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplacementConsent {
    #[serde(deserialize_with = "replacement_password")]
    password: zeroize::Zeroizing<String>,
    confirmed: bool,
}
fn replacement_password<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<zeroize::Zeroizing<String>, D::Error> {
    <String as serde::Deserialize>::deserialize(decoder).map(zeroize::Zeroizing::new)
}
#[tauri::command]
pub async fn e2ee_replace(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    consent: ReplacementConsent,
) -> Result<Response, Reason> {
    // Password already has a drop guard, including partial deserialization errors.
    transition(
        window,
        state,
        epoch,
        context,
        request_id,
        Action::Replace(consent.password, consent.confirmed),
    )
    .await
}
enum Action {
    Bind(Option<String>),
    Setup,
    Retry,
    Replace(zeroize::Zeroizing<String>, bool),
}
async fn transition(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    action: Action,
) -> Result<Response, Reason> {
    page(&window, &state, epoch)?;
    if let Action::Bind(Some(token)) = &action
        && (token.is_empty() || token.len() > 8192)
    {
        return Err(Reason::Invalid);
    }
    // Retire uses the immutable page authentication lease, available even while
    // the bind/setup/retry reply has not reached JS. Other actions use current CAS.
    if !matches!(action, Action::Bind(None)) {
        state.e2ee.manager.snapshot(context)?;
    }
    let guard = state.e2ee.admission.begin(context, request_id)?;
    let selected = match action {
        Action::Bind(Some(token)) => {
            state
                .e2ee
                .manager
                .bind(context, state.transport.snapshot().0, Some(token))?
        }
        Action::Bind(None) => state.e2ee.manager.retire(context)?,
        Action::Setup => state.e2ee.manager.setup(context)?,
        Action::Retry => state.e2ee.manager.retry(context)?,
        Action::Replace(password, confirmed) => {
            state.e2ee.manager.replace(context, password, confirmed)?
        }
    };
    let snapshot = tokio::select! {
        biased;
        _=guard.cancel.cancelled()=>return Err(Reason::Stale),
        r=tokio::time::timeout(Duration::from_secs(30),state.e2ee.manager.wait_released(selected))=>r.map_err(|_|Reason::NativeWait)??,
    };
    page(&window, &state, epoch)?;
    state.e2ee.manager.snapshot(selected)?;
    guard.deliver(snapshot.context, &snapshot)
}

#[tauri::command]
pub fn e2ee_status(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
) -> Result<Response, Reason> {
    page(&window, &state, epoch)?;
    state.e2ee.manager.snapshot_for_binding(context)?;
    let guard = state.e2ee.admission.begin(context, request_id)?;
    let snapshot: Snapshot = state.e2ee.manager.snapshot_for_binding(context)?;
    guard.deliver(snapshot.context, &snapshot)
}
#[tauri::command]
pub fn e2ee_ack(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
    reply_lease: String,
) -> Result<(), Reason> {
    page(&window, &state, epoch)?;
    state
        .e2ee
        .admission
        .acknowledge(context, &request_id, &reply_lease)?;
    Ok(())
}
#[tauri::command]
pub fn e2ee_cancel(
    window: WebviewWindow,
    state: State<'_, DesktopState>,
    epoch: u64,
    context: u64,
    request_id: String,
) -> Result<(), Reason> {
    page(&window, &state, epoch)?;
    state.e2ee.admission.cancel(context, &request_id)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replacement_consent_rejects_renderer_generation_or_scope_fields() {
        assert!(
            serde_json::from_str::<ReplacementConsent>(
                r#"{"password":"synthetic","confirmed":true}"#
            )
            .is_ok()
        );
        for extra in ["expectedGeneration", "scope", "path"] {
            let mut body = serde_json::json!({"password":"synthetic","confirmed":true});
            body[extra] = "untrusted".into();
            assert!(serde_json::from_value::<ReplacementConsent>(body).is_err());
        }
    }
    #[test]
    fn native_commands_require_current_main_chat_not_settings_login_or_remote_pages() {
        let chat = crate::app_asset_url("chat.html").unwrap();
        assert_eq!(validate_page(MAIN_WINDOW, &chat, 3, 3), Ok(()));
        assert_eq!(validate_page(MAIN_WINDOW, &chat, 4, 3), Err(Reason::Stale));
        assert_eq!(validate_page("settings", &chat, 3, 3), Err(Reason::Invalid));
        for url in [
            crate::app_asset_url("login.html").unwrap(),
            crate::app_asset_url("settings.html").unwrap(),
            tauri::Url::parse("https://example.test/chat.html").unwrap(),
        ] {
            assert_eq!(validate_page(MAIN_WINDOW, &url, 3, 3), Err(Reason::Invalid));
        }
    }
}
