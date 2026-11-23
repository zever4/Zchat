//! Reset only microphone consent for the bundled origin, without touching data.
use tauri::{Url, WebviewWindow};

fn allowed_caller(label: &str, url: &Url) -> bool {
    label == crate::MAIN_WINDOW && crate::is_app_url(url) && url.path() == "/chat.html"
}

#[tauri::command]
pub async fn reset_microphone_permission(window: WebviewWindow) -> Result<(), String> {
    let url = window.url().map_err(|_| "Не удалось проверить окно")?;
    if !allowed_caller(window.label(), &url) {
        return Err("Сброс доступен только в главном окне чата".into());
    }
    reset(&window).await
}

#[cfg(windows)]
async fn reset(window: &WebviewWindow) -> Result<(), String> {
    use std::{cell::RefCell, rc::Rc, time::Duration};
    use webview2_com::{
        Microsoft::Web::WebView2::Win32::{
            COREWEBVIEW2_PERMISSION_KIND_MICROPHONE, COREWEBVIEW2_PERMISSION_STATE_DEFAULT,
            ICoreWebView2_13, ICoreWebView2Profile4,
        },
        SetPermissionStateCompletedHandler,
    };
    use windows_core::{HSTRING, Interface, PCWSTR};

    let (sender, receiver) = tokio::sync::oneshot::channel();
    // COM objects and the completion handler stay on the WebView's UI thread.
    window.with_webview(move |webview| {
        let sender = Rc::new(RefCell::new(Some(sender)));
        let completed_sender = sender.clone();
        let handler = SetPermissionStateCompletedHandler::create(Box::new(move |result| {
            if let Some(sender) = completed_sender.borrow_mut().take() {
                let _ = sender.send(result.map_err(|_| "WebView2 не смог сбросить доступ к микрофону".to_string()));
            }
            Ok(())
        }));
        // The renderer cannot select another origin, permission kind, or state.
        let origin = HSTRING::from(crate::app_asset_url("").expect("bundled origin").origin().ascii_serialization());
        let result = unsafe {
            (|| -> windows_core::Result<()> {
                let core: ICoreWebView2_13 = webview.controller().CoreWebView2()?.cast()?;
                let profile: ICoreWebView2Profile4 = core.Profile()?.cast()?;
                profile.SetPermissionState(COREWEBVIEW2_PERMISSION_KIND_MICROPHONE, PCWSTR(origin.as_ptr()), COREWEBVIEW2_PERMISSION_STATE_DEFAULT, &handler)
            })()
        };
        if result.is_err()
            && let Some(sender) = sender.borrow_mut().take() {
                let _ = sender.send(Err("Сброс разрешения недоступен. Обновите Microsoft Edge WebView2 Runtime и повторите попытку".into()));
        }
    }).map_err(|_| "Не удалось обратиться к WebView2".to_string())?;

    tokio::time::timeout(Duration::from_secs(10), receiver)
        .await
        .map_err(|_| "WebView2 не ответил. Повторите сброс разрешения".to_string())?
        .map_err(|_| "Окно закрылось до завершения сброса разрешения".to_string())?
}

#[cfg(not(windows))]
async fn reset(_: &WebviewWindow) -> Result<(), String> {
    Err("Сброс разрешения сейчас поддерживается только в Windows".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_bundled_main_chat_can_reset_microphone() {
        assert!(allowed_caller(
            "main",
            &crate::app_asset_url("chat.html").unwrap()
        ));
        for label in ["settings", "other"] {
            assert!(!allowed_caller(
                label,
                &crate::app_asset_url("chat.html").unwrap()
            ));
        }
        for url in [
            crate::app_asset_url("login.html").unwrap(),
            crate::app_asset_url("settings.html").unwrap(),
            Url::parse("https://example.com/chat.html").unwrap(),
            Url::parse("http://tauri.localhost.evil/chat.html").unwrap(),
        ] {
            assert!(!allowed_caller("main", &url));
        }
    }
}
