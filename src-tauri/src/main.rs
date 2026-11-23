#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tauri::{
    AppHandle, Manager, State, Url, WebviewUrl,
    webview::{NewWindowResponse, PermissionKind, PermissionResponse, WebviewWindowBuilder},
};

mod e2ee;
mod e2ee_ipc;
mod image_processing;
mod media_permissions;
mod transport;
use transport::{SocketEvent, Transport};

const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:18080";
const CONFIG_FILE_NAME: &str = "desktop-config.json";
const MAIN_WINDOW: &str = "main";
const SETTINGS_WINDOW: &str = "settings";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DesktopConfig {
    server_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LoadError {
    description: String,
    url: String,
    at: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ConfigResponse {
    server_url: String,
    default_server_url: String,
    app_version: String,
    platform: String,
    last_load_error: Option<LoadError>,
    epoch: u64,
    e2ee_compiled: bool,
    e2ee_context: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CommandResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    server_url: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WindowAction {
    State,
    Minimize,
    ToggleMaximize,
    Drag,
    Close,
}

#[derive(Serialize)]
struct WindowState {
    maximized: bool,
    minimized: bool,
}

// A renderer may control only its own main window, never name another window.
#[tauri::command]
async fn window_control(
    window: tauri::WebviewWindow,
    action: WindowAction,
) -> Result<Option<WindowState>, String> {
    let url = window.url().map_err(|error| error.to_string())?;
    if window.label() != MAIN_WINDOW
        || !is_app_url(&url)
        || !matches!(url.path(), "/login.html" | "/chat.html" | "/offline.html")
    {
        return Err("Управление доступно только в главном окне".into());
    }
    let result = match action {
        WindowAction::State => Ok(()),
        WindowAction::Minimize => window.minimize(),
        WindowAction::ToggleMaximize => {
            if window.is_maximized().map_err(|error| error.to_string())? {
                window.unmaximize()
            } else {
                window.maximize()
            }
        }
        WindowAction::Drag => window.start_dragging(),
        WindowAction::Close => {
            window.close().map_err(|error| error.to_string())?;
            return Ok(None);
        }
    };
    result.map_err(|error| error.to_string())?;
    Ok(Some(WindowState {
        maximized: window.is_maximized().map_err(|error| error.to_string())?,
        minimized: window.is_minimized().map_err(|error| error.to_string())?,
    }))
}

#[derive(Clone)]
struct DesktopState {
    e2ee: Arc<e2ee::DesktopE2ee>,
    server_update: Arc<tokio::sync::Mutex<()>>,
    exit_started: Arc<AtomicBool>,
    exit_ready: Arc<AtomicBool>,
    transport: Arc<Transport>,
    config: Arc<Mutex<DesktopConfig>>,
    default_server_url: String,
    last_load_error: Arc<Mutex<Option<LoadError>>>,
}

impl DesktopState {
    fn new(
        default_server_url: String,
        config: DesktopConfig,
        e2ee: e2ee::DesktopE2ee,
    ) -> Result<Self, String> {
        Ok(Self {
            e2ee: Arc::new(e2ee),
            server_update: Arc::new(tokio::sync::Mutex::new(())),
            exit_started: Arc::new(AtomicBool::new(false)),
            exit_ready: Arc::new(AtomicBool::new(false)),
            transport: Arc::new(Transport::new(&config.server_url)?),
            config: Arc::new(Mutex::new(config)),
            default_server_url,
            last_load_error: Arc::new(Mutex::new(None)),
        })
    }

    fn set_server_url(&self, server_url: String) -> Result<(), String> {
        let mut config = self
            .config
            .lock()
            .map_err(|_| "Не удалось получить доступ к настройкам".to_string())?;
        self.transport.cancel_all(Some(&server_url));
        config.server_url = server_url;
        Ok(())
    }

    fn set_load_error(&self, description: String, url: String) {
        if let Ok(mut error) = self.last_load_error.lock() {
            error.replace(LoadError {
                description,
                url,
                at: unix_timestamp(),
            });
        }
    }

    fn clear_load_error(&self) {
        if let Ok(mut error) = self.last_load_error.lock() {
            error.take();
        }
    }

    fn load_error(&self) -> Option<LoadError> {
        self.last_load_error
            .lock()
            .ok()
            .and_then(|error| error.clone())
    }
}

#[tauri::command]
fn get_config(app: AppHandle, state: State<'_, DesktopState>) -> ConfigResponse {
    let (server_url, epoch) = state.transport.snapshot();
    ConfigResponse {
        server_url,
        default_server_url: state.default_server_url.clone(),
        app_version: app.package_info().version.to_string(),
        platform: std::env::consts::OS.to_string(),
        last_load_error: state.load_error(),
        epoch,
        e2ee_compiled: e2ee::compiled(),
        e2ee_context: state.e2ee.manager.context(),
    }
}

#[tauri::command]
async fn set_server_url(
    app: AppHandle,
    state: State<'_, DesktopState>,
    server_url: String,
) -> Result<CommandResponse, String> {
    let _transition = state
        .server_update
        .try_lock()
        .map_err(|_| "Настройки уже изменяются")?;
    let normalized = normalize_server_url(&server_url)?;
    save_config(
        &app,
        &DesktopConfig {
            server_url: normalized.clone(),
        },
    )?;
    let retired = state.e2ee.invalidate();
    state.set_server_url(normalized.clone())?;
    state
        .e2ee
        .manager
        .wait_released(retired)
        .await
        .map_err(|_| "Не удалось завершить прежнюю E2EE-сессию")?;

    if let Some(settings) = app.get_webview_window(SETTINGS_WINDOW) {
        settings.close().map_err(|error| error.to_string())?;
    }

    load_main_app(&app, state.inner().clone())?;
    Ok(CommandResponse {
        ok: true,
        server_url: Some(normalized),
    })
}

#[tauri::command]
async fn reset_server_url(
    app: AppHandle,
    state: State<'_, DesktopState>,
) -> Result<CommandResponse, String> {
    let _transition = state
        .server_update
        .try_lock()
        .map_err(|_| "Настройки уже изменяются")?;
    let default_server_url = state.default_server_url.clone();
    save_config(
        &app,
        &DesktopConfig {
            server_url: default_server_url.clone(),
        },
    )?;
    let retired = state.e2ee.invalidate();
    state.set_server_url(default_server_url.clone())?;
    state
        .e2ee
        .manager
        .wait_released(retired)
        .await
        .map_err(|_| "Не удалось завершить прежнюю E2EE-сессию")?;
    load_main_app(&app, state.inner().clone())?;
    Ok(CommandResponse {
        ok: true,
        server_url: Some(default_server_url),
    })
}

#[tauri::command]
async fn retry_load(
    app: AppHandle,
    state: State<'_, DesktopState>,
) -> Result<CommandResponse, String> {
    health_check(state.inner()).await?;
    load_main_app(&app, state.inner().clone())?;
    Ok(CommandResponse {
        ok: true,
        server_url: None,
    })
}

#[tauri::command]
async fn open_settings_window(app: AppHandle) -> Result<CommandResponse, String> {
    open_settings(&app)?;
    Ok(CommandResponse {
        ok: true,
        server_url: None,
    })
}

#[tauri::command]
fn close_settings_window(app: AppHandle) -> Result<CommandResponse, String> {
    if let Some(settings) = app.get_webview_window(SETTINGS_WINDOW) {
        settings.close().map_err(|error| error.to_string())?;
    }
    Ok(CommandResponse {
        ok: true,
        server_url: None,
    })
}

#[tauri::command]
fn open_external(target_url: String) -> Result<CommandResponse, String> {
    open_external_url(&target_url)?;
    Ok(CommandResponse {
        ok: true,
        server_url: None,
    })
}

async fn health_check(state: &DesktopState) -> Result<(), String> {
    let (server, epoch) = state.transport.snapshot();
    let result = state.transport.health().await;
    if state.transport.snapshot().1 != epoch {
        return Err("Настройки подключения изменились".into());
    }
    match &result {
        Ok(()) => state.clear_load_error(),
        Err(error) => state.set_load_error(error.clone(), server),
    }
    result
}

#[tauri::command]
async fn check_server(state: State<'_, DesktopState>) -> Result<(), String> {
    health_check(state.inner()).await
}

#[tauri::command]
fn cancel_request(state: State<'_, DesktopState>, id: String) {
    state.transport.cancel_request(&id);
}

#[tauri::command]
fn socket_connect(
    state: State<'_, DesktopState>,
    epoch: u64,
    id: String,
    path: String,
    channel: tauri::ipc::Channel<SocketEvent>,
) -> Result<(), String> {
    state.transport.connect(epoch, id, path, channel)
}

#[tauri::command]
fn socket_send(
    state: State<'_, DesktopState>,
    id: String,
    data: Vec<u8>,
    binary: bool,
) -> Result<(), String> {
    state.transport.send(&id, data, binary)
}

#[tauri::command]
fn socket_close(state: State<'_, DesktopState>, id: String) {
    state.transport.close(&id);
}

#[tauri::command]
fn socket_ack(state: State<'_, DesktopState>, id: String, sequence: u64) {
    state.transport.acknowledge(&id, sequence);
}

#[tauri::command]
async fn prepare_image(
    window: tauri::WebviewWindow,
    processor: State<'_, image_processing::ImageProcessor>,
    request: tauri::ipc::Request<'_>,
) -> Result<tauri::ipc::Response, String> {
    let url = window
        .url()
        .map_err(|_| "Не удалось проверить окно".to_string())?;
    if window.label() != MAIN_WINDOW || !is_app_url(&url) || url.path() != "/chat.html" {
        return Err("Обработка доступна только в окне чата".into());
    }
    let options = image_processing::options(request.headers())?;
    let tauri::ipc::InvokeBody::Raw(data) = request.body() else {
        return Err("Ожидались данные изображения".into());
    };
    processor
        .prepare(data, options)
        .await
        .map(tauri::ipc::Response::new)
}

fn main() {
    tauri::Builder::default()
        .on_window_event(|window, event| {
            if window.label() == MAIN_WINDOW && matches!(event, tauri::WindowEvent::Destroyed) {
                if let Some(state) = window.try_state::<DesktopState>() {
                    state.e2ee.invalidate();
                    state.transport.cancel_all(None);
                }
                // Closing the main window also closes a still-open settings view.
                window.app_handle().exit(0);
            }
        })
        .register_asynchronous_uri_scheme_protocol("zchat-api", |context, request, responder| {
            let app = context.app_handle();
            let origin = request
                .headers()
                .get("origin")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            let valid_origin =
                origin.is_empty() || Url::parse(&origin).is_ok_and(|u| is_app_url(&u));
            if context.webview_label() != MAIN_WINDOW || !valid_origin {
                responder.respond(transport::error(403));
                return;
            }
            let origin = if origin.is_empty() {
                app_asset_url("")
                    .expect("app origin")
                    .origin()
                    .ascii_serialization()
            } else {
                origin
            };
            if request.method() == tauri::http::Method::OPTIONS {
                let mut response = transport::error(204);
                *response.body_mut() = Vec::new();
                transport::cors(&mut response, &origin);
                responder.respond(response);
                return;
            }
            let transport = app.state::<DesktopState>().transport.clone();
            tauri::async_runtime::spawn(async move {
                let mut response = transport.request(request).await;
                transport::cors(&mut response, &origin);
                responder.respond(response);
            });
        })
        .invoke_handler(tauri::generate_handler![
            get_config,
            e2ee_ipc::e2ee_session,
            e2ee_ipc::e2ee_conversation,
            e2ee_ipc::e2ee_send_text,
            e2ee_ipc::e2ee_prepare_attachment,
            e2ee_ipc::e2ee_attachment_input,
            e2ee_ipc::e2ee_download_attachment,
            e2ee_ipc::e2ee_local_attachment,
            e2ee_ipc::e2ee_read,
            e2ee_ipc::e2ee_recovery,
            e2ee_ipc::e2ee_setup,
            e2ee_ipc::e2ee_retry,
            e2ee_ipc::e2ee_replace,
            e2ee_ipc::e2ee_status,
            e2ee_ipc::e2ee_ack,
            e2ee_ipc::e2ee_cancel,
            set_server_url,
            reset_server_url,
            retry_load,
            open_settings_window,
            close_settings_window,
            open_external,
            check_server,
            cancel_request,
            socket_connect,
            socket_send,
            socket_close,
            socket_ack,
            prepare_image,
            media_permissions::reset_microphone_permission,
            window_control
        ])
        .setup(|app| {
            let app_handle = app.handle().clone();
            let default_server_url = default_server_url();
            let config = load_config(&app_handle, &default_server_url);
            let root = custom_data_directory()?.unwrap_or(app_handle.path().app_data_dir()?);
            let native_config = config_path(&app_handle)?.with_file_name("e2ee-servers.json");
            let state = DesktopState::new(
                default_server_url,
                config,
                e2ee::DesktopE2ee::new(native_config, root),
            )?;

            app.manage(state.clone());
            app.manage(image_processing::ImageProcessor::default());
            create_main_window(&app_handle, state)?;
            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building Zchat Desktop")
        .run(|app, event| {
            if let tauri::RunEvent::ExitRequested { api, .. } = event {
                let Some(state) = app.try_state::<DesktopState>() else {
                    return;
                };
                if state.exit_ready.load(Ordering::SeqCst) {
                    return;
                }
                api.prevent_exit();
                if !state.exit_started.swap(true, Ordering::SeqCst) {
                    let state = state.inner().clone();
                    let app = app.clone();
                    state.transport.cancel_all(None);
                    tauri::async_runtime::spawn(async move {
                        state.e2ee.shutdown().await;
                        state.exit_ready.store(true, Ordering::SeqCst);
                        app.exit(0);
                    });
                }
            }
        });
}

fn create_main_window(app: &AppHandle, state: DesktopState) -> Result<(), String> {
    if let Some(window) = app.get_webview_window(MAIN_WINDOW) {
        window.show().map_err(|error| error.to_string())?;
        window.set_focus().map_err(|error| error.to_string())?;
        return Ok(());
    }

    let navigation_state = state.clone();
    let page_state = state.clone();
    let builder = WebviewWindowBuilder::new(app, MAIN_WINDOW, WebviewUrl::App("login.html".into()));
    let builder = if let Some(path) = custom_data_directory()? {
        builder.data_directory(path.join("webview"))
    } else {
        builder
    };
    builder
        .title("Zchat")
        .decorations(false)
        .shadow(true)
        .theme(Some(tauri::Theme::Dark))
        .background_color(tauri::window::Color(10, 10, 12, 255))
        .inner_size(1440.0, 920.0)
        .min_inner_size(1100.0, 720.0)
        .resizable(true)
        .on_navigation(move |url| is_allowed_navigation(url, &navigation_state))
        .on_new_window(|_, _| NewWindowResponse::Deny)
        .on_permission_request(|webview, kind| {
            media_permission_response(webview.url().ok().as_ref(), kind)
        })
        .on_page_load(move |_, payload| {
            if payload.event() == tauri::webview::PageLoadEvent::Started {
                page_state.e2ee.invalidate();
                page_state.transport.cancel_all(None);
            }
        })
        .build()
        .map_err(|error| error.to_string())?;

    Ok(())
}

fn open_settings(app: &AppHandle) -> Result<(), String> {
    if let Some(settings) = app.get_webview_window(SETTINGS_WINDOW) {
        settings.show().map_err(|error| error.to_string())?;
        settings.set_focus().map_err(|error| error.to_string())?;
        return Ok(());
    }

    let builder = WebviewWindowBuilder::new(
        app,
        SETTINGS_WINDOW,
        WebviewUrl::App("settings.html".into()),
    );
    let builder = if let Some(path) = custom_data_directory()? {
        builder.data_directory(path.join("webview"))
    } else {
        builder
    };
    builder
        .title("Настройки сервера")
        .theme(Some(tauri::Theme::Dark))
        .background_color(tauri::window::Color(10, 10, 12, 255))
        .inner_size(560.0, 520.0)
        .min_inner_size(520.0, 480.0)
        .resizable(true)
        .on_navigation(|url| is_app_url(url) && url.path() == "/settings.html")
        .on_new_window(|_, _| NewWindowResponse::Deny)
        .on_permission_request(|_, kind| media_permission_response(None, kind))
        .build()
        .map_err(|error| error.to_string())?;

    Ok(())
}

fn load_main_app(app: &AppHandle, state: DesktopState) -> Result<(), String> {
    if app.get_webview_window(MAIN_WINDOW).is_none() {
        return create_main_window(app, state);
    }
    let window = app
        .get_webview_window(MAIN_WINDOW)
        .ok_or("Главное окно не найдено")?;
    window
        .navigate(app_asset_url("login.html")?)
        .map_err(|error| error.to_string())?;
    window.show().map_err(|error| error.to_string())?;
    window.set_focus().map_err(|error| error.to_string())?;
    Ok(())
}

fn normalize_server_url(raw_value: &str) -> Result<String, String> {
    let raw = raw_value.trim();
    if raw.is_empty() {
        return Err("Укажите адрес сервера".to_string());
    }

    let with_protocol = if raw.contains("://") {
        raw.to_string()
    } else {
        format!("http://{raw}")
    };

    let mut parsed = Url::parse(&with_protocol)
        .map_err(|_| "Не удалось распознать адрес сервера".to_string())?;
    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err("Поддерживаются только http:// и https:// адреса".to_string());
    }

    if parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err("Укажите только адрес сервера с портом, без логина, пути и параметров".into());
    }
    parsed.set_path("");

    Ok(parsed.to_string().trim_end_matches('/').to_string())
}

fn default_server_url() -> String {
    let raw = std::env::var("ZCHAT_SERVER_URL").unwrap_or_else(|_| DEFAULT_SERVER_URL.to_string());
    normalize_server_url(&raw).unwrap_or_else(|_| DEFAULT_SERVER_URL.to_string())
}

fn config_path(app: &AppHandle) -> Result<PathBuf, String> {
    if let Some(path) = custom_data_directory()? {
        return Ok(path.join(CONFIG_FILE_NAME));
    }
    Ok(app
        .path()
        .app_config_dir()
        .map_err(|error| error.to_string())?
        .join(CONFIG_FILE_NAME))
}

fn validate_data_directory(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute() || path.parent().is_none() {
        return Err("Укажите полный путь к отдельной папке данных Zchat".into());
    }
    Ok(path.to_path_buf())
}

fn custom_data_directory() -> Result<Option<PathBuf>, String> {
    let path = std::env::var_os("ZCHAT_DESKTOP_DATA_DIR");
    #[cfg(debug_assertions)]
    let path = path.or_else(|| std::env::var_os("ZCHAT_DESKTOP_TEST_PROFILE"));
    path.map(|value| validate_data_directory(Path::new(&value)))
        .transpose()
}

fn load_config(app: &AppHandle, default_server_url: &str) -> DesktopConfig {
    let fallback = DesktopConfig {
        server_url: default_server_url.to_string(),
    };

    let Ok(path) = config_path(app) else {
        return fallback;
    };
    let Ok(raw) = fs::read_to_string(path) else {
        return fallback;
    };
    let Ok(config) = serde_json::from_str::<DesktopConfig>(&raw) else {
        return fallback;
    };
    let Ok(server_url) = normalize_server_url(&config.server_url) else {
        return fallback;
    };

    DesktopConfig { server_url }
}

fn save_config(app: &AppHandle, config: &DesktopConfig) -> Result<(), String> {
    let path = config_path(app)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let raw = serde_json::to_string_pretty(config).map_err(|error| error.to_string())?;
    fs::write(path, raw).map_err(|error| error.to_string())
}

fn is_allowed_navigation(url: &Url, _state: &DesktopState) -> bool {
    is_app_url(url) && matches!(url.path(), "/login.html" | "/chat.html" | "/offline.html")
}

fn is_app_url(url: &Url) -> bool {
    let Ok(base) = app_asset_url("") else {
        return false;
    };
    url.scheme() == base.scheme()
        && url.host_str() == base.host_str()
        && url.port_or_known_default() == base.port_or_known_default()
        && url.username().is_empty()
        && url.password().is_none()
}

fn is_external_browser_url(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
}

fn media_permission_response(url: Option<&Url>, kind: PermissionKind) -> PermissionResponse {
    if matches!(kind, PermissionKind::Microphone | PermissionKind::Camera)
        && !url.is_some_and(|url| is_app_url(url) && url.path() == "/chat.html")
    {
        return PermissionResponse::Deny;
    }
    // Keep the WebView/OS consent prompt. Bundling a page is not permission to
    // automatically capture a microphone or camera, including after a denial.
    PermissionResponse::Default
}

fn open_external_url(raw_url: &str) -> Result<(), String> {
    let url = Url::parse(raw_url).map_err(|_| "Не удалось открыть внешний адрес".to_string())?;
    if !is_external_browser_url(&url) || !url.username().is_empty() || url.password().is_some() {
        return Err("Можно открывать только http:// и https:// адреса".to_string());
    }
    open::that(url.as_str()).map_err(|error| error.to_string())
}

fn app_asset_url(path: &str) -> Result<Url, String> {
    let clean_path = path.trim_start_matches('/');

    #[cfg(windows)]
    let base = "http://tauri.localhost";
    #[cfg(not(windows))]
    let base = "tauri://localhost";

    Url::parse(&format!("{base}/{clean_path}")).map_err(|error| error.to_string())
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_profile_requires_an_absolute_dedicated_directory() {
        assert!(validate_data_directory(Path::new("relative-profile")).is_err());
        assert!(validate_data_directory(Path::new("")).is_err());
        let profile = std::env::temp_dir().join("zchat-profile-validation");
        assert_eq!(validate_data_directory(&profile).unwrap(), profile);
        let root = profile.ancestors().last().unwrap();
        assert!(validate_data_directory(root).is_err());
    }

    #[test]
    fn media_capture_requires_the_chat_page_and_preserves_user_consent() {
        for kind in [PermissionKind::Microphone, PermissionKind::Camera] {
            assert!(matches!(
                media_permission_response(app_asset_url("chat.html").ok().as_ref(), kind),
                PermissionResponse::Default
            ));
            for url in [
                app_asset_url("login.html").ok(),
                app_asset_url("settings.html").ok(),
                Url::parse("https://example.test/chat.html").ok(),
                None,
            ] {
                assert!(matches!(
                    media_permission_response(url.as_ref(), kind),
                    PermissionResponse::Deny
                ));
            }
        }
    }

    #[test]
    fn server_settings_accept_only_http_origins() {
        assert_eq!(
            normalize_server_url(" localhost:18080 ").unwrap(),
            "http://localhost:18080"
        );
        assert_eq!(
            normalize_server_url("https://example.test/").unwrap(),
            "https://example.test"
        );
        for url in [
            "file:///tmp",
            "https://user:pass@example.test",
            "http://example.test/path",
            "http://example.test?x=1",
            "http://example.test#x",
        ] {
            assert!(normalize_server_url(url).is_err(), "{url}");
        }
    }

    #[test]
    fn remote_and_lookalike_origins_are_not_bundled_pages() {
        assert!(is_app_url(&app_asset_url("chat.html").unwrap()));
        for url in [
            "http://tauri.localhost.evil.test/chat.html",
            "https://example.test/chat.html",
            "data:text/html,hello",
            "blob:http://tauri.localhost/id",
        ] {
            assert!(!is_app_url(&Url::parse(url).unwrap()), "{url}");
        }
    }
}
