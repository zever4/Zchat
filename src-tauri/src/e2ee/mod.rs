//! Desktop session boundary. Renderer inputs never select storage, account UUIDs,
//! RPC endpoints, trust anchors, credentials or cryptographic scopes.
use serde::Serialize;
use std::path::PathBuf;

mod admission;
pub(crate) mod attachment_input;
pub(crate) mod dialogs;
#[cfg(not(feature = "experimental-e2ee"))]
mod disabled;
pub(crate) mod downloads;
#[cfg(feature = "experimental-e2ee")]
mod factory;
pub(crate) mod local_files;
pub(crate) mod pages;
pub(crate) mod recovery;
#[cfg(feature = "experimental-e2ee")]
mod replacement;
#[cfg(feature = "experimental-e2ee")]
mod session;
pub(crate) mod text;
#[cfg(not(feature = "experimental-e2ee"))]
use disabled::SessionManager;
#[cfg(feature = "experimental-e2ee")]
use session::SessionManager;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    Invalid,
    Stale,
    Busy,
    Closed,
    NotReady,
    #[cfg(feature = "experimental-e2ee")]
    NotConfigured,
    #[cfg(feature = "experimental-e2ee")]
    Configuration,
    #[cfg(feature = "experimental-e2ee")]
    UnsupportedPlatform,
    #[cfg(feature = "experimental-e2ee")]
    Storage,
    #[cfg(feature = "experimental-e2ee")]
    Authentication,
    #[cfg(feature = "experimental-e2ee")]
    GenerationChanged,
    #[cfg(feature = "experimental-e2ee")]
    PasswordInvalid,
    #[cfg(feature = "experimental-e2ee")]
    Forbidden,
    #[cfg(feature = "experimental-e2ee")]
    NotFound,
    #[cfg(feature = "experimental-e2ee")]
    Conflict,
    #[cfg(feature = "experimental-e2ee")]
    ConversationClosed,
    #[cfg(feature = "experimental-e2ee")]
    SendingFrozen,
    #[cfg(feature = "experimental-e2ee")]
    RecoveryRequired,
    #[cfg(feature = "experimental-e2ee")]
    SessionPending,
    #[cfg(feature = "experimental-e2ee")]
    AcquisitionExpired,
    #[cfg(feature = "experimental-e2ee")]
    LocalDeadline,
    #[cfg(feature = "experimental-e2ee")]
    RemoteCancelled,
    #[cfg(feature = "experimental-e2ee")]
    RemoteDeadline,
    #[cfg(feature = "experimental-e2ee")]
    Remote,
    NativeWait,
    PhotoSourceTooLarge,
    #[cfg(feature = "experimental-e2ee")]
    PhotoDimensions,
    #[cfg(feature = "experimental-e2ee")]
    PhotoOutputTooLarge,
    PhotoOriginalConsent,
    #[cfg(feature = "experimental-e2ee")]
    PhotoAnimationOriginal,
    #[cfg(feature = "experimental-e2ee")]
    PhotoMultipageOriginal,
    #[cfg(feature = "experimental-e2ee")]
    PhotoUnsupported,
    #[cfg(feature = "experimental-e2ee")]
    PhotoDecode,
    #[cfg(feature = "experimental-e2ee")]
    PhotoEncode,
    PhotoChoiceInvalid,
    FileSourceTooLarge,
    VoiceSourceTooLarge,
    #[cfg(feature = "experimental-e2ee")]
    VoiceFormat,
    #[cfg(feature = "experimental-e2ee")]
    AttachmentUnavailable,
    #[cfg(feature = "experimental-e2ee")]
    AttachmentRemoved,
    #[cfg(feature = "experimental-e2ee")]
    AttachmentIntegrity,
    #[cfg(feature = "experimental-e2ee")]
    AttachmentPreview,
    #[cfg(all(feature = "experimental-e2ee", any(windows, test)))]
    AttachmentDestination,
    #[cfg(all(feature = "experimental-e2ee", any(windows, test)))]
    AttachmentExists,
    Runtime,
}

#[derive(serde::Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct AttachmentTarget {
    pub(super) conversation_id: String,
    pub(super) message_id: String,
}
impl AttachmentTarget {
    fn validate(&self) -> Result<(), Reason> {
        if admission::valid_id(&self.conversation_id) && admission::valid_id(&self.message_id) {
            Ok(())
        } else {
            Err(Reason::Invalid)
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    #[cfg(not(feature = "experimental-e2ee"))]
    Disabled,
    #[cfg(feature = "experimental-e2ee")]
    Idle,
    #[cfg(feature = "experimental-e2ee")]
    Stopping,
    #[cfg(feature = "experimental-e2ee")]
    Starting,
    #[cfg(feature = "experimental-e2ee")]
    SetupRequired,
    #[cfg(feature = "experimental-e2ee")]
    ReplacementRequired,
    #[cfg(feature = "experimental-e2ee")]
    ReplacementPending,
    #[cfg(feature = "experimental-e2ee")]
    DeviceUnavailable,
    #[cfg(feature = "experimental-e2ee")]
    Active,
    #[cfg(feature = "experimental-e2ee")]
    Failed,
    Closed,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplacementView {
    pub expected_generation: String,
    pub continuing: bool,
    pub retargeting: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub context: u64,
    pub phase: Phase,
    pub error: Option<Reason>,
    pub replacement: Option<ReplacementView>,
    // An observation of the local runtime, never messaging permission/readiness.
    pub runtime_phase: Option<String>,
    pub incoming_message_revision: Option<String>,
    pub delivery_health: Option<String>,
    pub maintenance_health: Option<String>,
}
impl Snapshot {
    fn new(context: u64, phase: Phase, error: Option<Reason>) -> Self {
        Self {
            context,
            phase,
            error,
            replacement: None,
            runtime_phase: None,
            incoming_message_revision: None,
            delivery_health: None,
            maintenance_health: None,
        }
    }
}

pub struct DesktopE2ee {
    pub manager: SessionManager,
    pub admission: admission::Admission,
    pub(super) cursors: pages::CursorStore,
}
impl DesktopE2ee {
    pub fn new(config_file: PathBuf, data_root: PathBuf) -> Self {
        #[cfg(feature = "experimental-e2ee")]
        let manager = {
            let factory = factory::NativeFactory::load(config_file, data_root);
            let (manager, task) = SessionManager::new(std::sync::Arc::new(factory));
            tauri::async_runtime::spawn(task);
            manager
        };
        #[cfg(not(feature = "experimental-e2ee"))]
        let manager = {
            let _ = (config_file, data_root);
            SessionManager::new()
        };
        Self {
            manager,
            admission: admission::Admission::default(),
            cursors: Default::default(),
        }
    }
    pub fn invalidate(&self) -> u64 {
        let context = self.manager.clear();
        self.admission.invalidate();
        self.cursors.invalidate(context);
        context
    }
    pub async fn shutdown(&self) {
        self.admission.invalidate();
        self.manager.shutdown().await;
    }
}

pub fn compiled() -> bool {
    cfg!(feature = "experimental-e2ee")
}

#[cfg(all(test, feature = "experimental-e2ee", windows))]
mod integration_tests;
