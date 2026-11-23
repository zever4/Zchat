//! Opt-in native configuration; no endpoint guessing, renderer CA/path or JWT claims.
use super::{
    Phase, Reason,
    replacement::{ReplacementInput, ReplacementTarget, offer},
    session::{Binding, Factory, Intent, Opened},
};
use rust_processor::encryption::{
    account_owner::{AccountConfig, AccountError, AccountOwner},
    discovery::{DiscoveryError, TrustedServer},
    native::RpcError,
    protected::{ProtectedStore, StoreError},
    provisioning::{DeviceProvisioning, Reconciliation},
    signal_state::{ArchiveOwner, CryptoError},
};
use rust_processor::proto::e2ee::FailureReason;
use serde::Deserialize;
use std::{
    fs::File,
    future::Future,
    io::Read,
    path::{Path, PathBuf},
    pin::Pin,
};
use tokio_util::sync::CancellationToken;

const MAX_CONFIG: usize = 128 * 1024;
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Entry {
    http_origin: String,
    grpc_origin: String,
    extra_ca_pem: Option<String>,
}
impl Entry {
    fn trusted(&self) -> Result<TrustedServer, Reason> {
        if self.http_origin.len() > 2048
            || self.grpc_origin.len() > 2048
            || self.extra_ca_pem.as_ref().is_some_and(|p| p.len() > 16384)
        {
            return Err(Reason::Configuration);
        }
        TrustedServer::new(
            &self.http_origin,
            &self.grpc_origin,
            self.extra_ca_pem.as_deref().map(str::as_bytes),
        )
        .map_err(|_| Reason::Configuration)
    }
    fn origin(&self) -> Result<String, Reason> {
        Ok(tauri::Url::parse(&self.http_origin)
            .map_err(|_| Reason::Configuration)?
            .origin()
            .ascii_serialization())
    }
}
pub(super) struct NativeFactory {
    entries: Result<Vec<Entry>, Reason>,
    root: PathBuf,
}
impl NativeFactory {
    pub fn load(path: PathBuf, root: PathBuf) -> Self {
        Self {
            entries: read_config(&path),
            root,
        }
    }
}
fn read_config(path: &Path) -> Result<Vec<Entry>, Reason> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Err(Reason::NotConfigured),
        Err(_) => return Err(Reason::Configuration),
    };
    let mut bytes = Vec::new();
    file.take(MAX_CONFIG as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Reason::Configuration)?;
    if bytes.len() > MAX_CONFIG {
        return Err(Reason::Configuration);
    }
    let entries: Vec<Entry> = serde_json::from_slice(&bytes).map_err(|_| Reason::Configuration)?;
    if entries.is_empty() || entries.len() > 8 {
        return Err(Reason::Configuration);
    }
    let mut origins = std::collections::HashSet::new();
    for entry in &entries {
        entry.trusted()?;
        if !origins.insert(entry.origin()?) {
            return Err(Reason::Configuration);
        }
    }
    Ok(entries)
}
impl Factory for NativeFactory {
    fn open(
        &self,
        binding: Binding,
        replacement: Option<ReplacementInput>,
        cancel: CancellationToken,
    ) -> Pin<Box<dyn Future<Output = Result<Opened, Reason>> + Send>> {
        let entry = self.entries.as_ref().map_err(|e| *e).and_then(|entries| {
            entries
                .iter()
                .find(|e| e.origin().is_ok_and(|o| o == binding.origin))
                .cloned()
                .ok_or(Reason::NotConfigured)
        });
        let root = self.root.clone();
        Box::pin(async move {
            let trusted = entry?.trusted()?;
            let intent = binding.intent;
            if (intent == Intent::Replace) != replacement.is_some() {
                return Err(Reason::Invalid);
            }
            if !cfg!(windows) {
                return Err(Reason::UnsupportedPlatform);
            }
            let account = tokio::select! {
                biased;
                _=cancel.cancelled()=>return Err(Reason::Stale),
                r=trusted.discover(&binding.origin,binding.jwt)=>r.map_err(discovery_error)?,
            };
            let handle = tokio::runtime::Handle::current();
            let stop = cancel.clone();
            // Join even after cancellation. Opening/DPAPI/reconciliation/key recovery
            // are blocking, and the store must be released BEFORE the next account.
            let prepared = tokio::task::spawn_blocking(move || {
                handle.block_on(async move {
                    if stop.is_cancelled() {
                        return Err(Reason::Stale);
                    }
                    let mut account = account;
                    let scope = account.scope();
                    let exists = scope
                        .database_path(&root)
                        .try_exists()
                        .map_err(|_| Reason::Storage)?;
                    let store = if exists {
                        ProtectedStore::open(&root, scope).map_err(store_error)?
                    } else {
                        if account.state().active_device.is_some() {
                            let active = account
                                .state()
                                .active_device
                                .as_ref()
                                .and_then(|d| d.reference.as_ref())
                                .ok_or(Reason::Runtime)?;
                            let target = ReplacementTarget {
                                expected: active.generation_id.clone(),
                                pending: None,
                                continuing: false,
                            };
                            if replacement.as_ref().is_none_or(|r| r.target != target) {
                                if replacement.is_some() {
                                    return Err(Reason::GenerationChanged);
                                }
                                return Ok(Prepared::Blocked(
                                    Phase::ReplacementRequired,
                                    Some(target),
                                ));
                            }
                        }
                        if intent != Intent::Setup && intent != Intent::Replace {
                            return Ok(Prepared::Blocked(Phase::SetupRequired, None));
                        }
                        if intent == Intent::Replace && account.state().active_device.is_none() {
                            return Err(Reason::GenerationChanged);
                        }
                        // Native authenticated scope, fresh explicit user intent.
                        // create refuses existing/partial files; never fallback/reset.
                        ProtectedStore::create(&root, scope).map_err(store_error)?
                    };
                    let mut local = DeviceProvisioning::new(store);
                    let observed = account.reconcile(&mut local).map_err(discovery_error)?;
                    if let Some(replacement) = replacement {
                        let selected = offer(&observed, &mut local, account.state())?;
                        if selected.as_ref() != Some(&replacement.target) {
                            return Err(Reason::GenerationChanged);
                        }
                        let resumed = tokio::select! {
                            biased;
                            _=stop.cancelled()=>return Err(Reason::Stale),
                            r=async {
                                if !replacement.target.continuing {
                                    account.begin_replacement(
                                        &mut local,
                                        &replacement.target.expected,
                                        replacement.target.pending.as_deref(),
                                    ).await.map_err(discovery_error)?;
                                }
                                account.resume_replacement(&mut local, replacement.password)
                                    .await.map_err(discovery_error)
                            }=>r?,
                        };
                        if resumed != Reconciliation::Ready {
                            return Err(Reason::Runtime);
                        }
                        return Ok(Prepared::Ready(Box::new((account, local))));
                    }
                    if let Some(phase) = blocked(&observed, intent) {
                        let target = offer(&observed, &mut local, account.state())?;
                        return Ok(Prepared::Archive(phase, target, Box::new(local)));
                    }
                    // An explicit setup may start a new registration. Resume/retry
                    // only recover an already committed registration/publication.
                    let resumed = tokio::select! {
                        biased;
                        _=stop.cancelled()=>return Err(Reason::Stale),
                        r=account.resume_initial(&mut local)=>r.map_err(discovery_error)?,
                    };
                    if let Some(phase) = blocked(&resumed, intent) {
                        let target = offer(&resumed, &mut local, account.state())?;
                        return Ok(Prepared::Archive(phase, target, Box::new(local)));
                    }
                    if resumed != Reconciliation::Ready {
                        return Err(Reason::Runtime);
                    }
                    Ok(Prepared::Ready(Box::new((account, local))))
                })
            })
            .await
            .map_err(|_| Reason::Runtime)??;
            if cancel.is_cancelled() {
                return Err(Reason::Stale);
            }
            match prepared {
                Prepared::Blocked(phase, target) => Ok(Opened::Blocked(phase, target)),
                Prepared::Archive(phase, target, local) => {
                    // The discovered transport has already been dropped. The
                    // local reader has no network/delivery or mutation client.
                    match ArchiveOwner::start(*local).await.map_err(crypto_error)? {
                        Some(owner) => Ok(Opened::ReadOnly(phase, target, Box::new(owner))),
                        None => Ok(Opened::Blocked(phase, target)),
                    }
                }
                Prepared::Ready(ready) => {
                    let (account, local) = *ready;
                    let runtime = AccountOwner::start(account, local, AccountConfig::default())
                        .await
                        .map_err(account_error)?;
                    // The lifecycle owner receives this value even if the native
                    // initialization await completed just after a session change.
                    Ok(Opened::Runtime(Box::new(runtime)))
                }
            }
        })
    }
}
enum Prepared {
    Ready(
        Box<(
            rust_processor::encryption::discovery::DiscoveredAccount,
            DeviceProvisioning,
        )>,
    ),
    Blocked(Phase, Option<ReplacementTarget>),
    Archive(Phase, Option<ReplacementTarget>, Box<DeviceProvisioning>),
}
fn blocked(observed: &Reconciliation, intent: Intent) -> Option<Phase> {
    match observed {
        Reconciliation::Unregistered if intent != Intent::Setup => Some(Phase::SetupRequired),
        Reconciliation::ReplacementRequired { .. } => Some(Phase::ReplacementRequired),
        Reconciliation::ReplacementPending { .. } => Some(Phase::ReplacementPending),
        Reconciliation::DeviceUnavailable { .. } => Some(Phase::DeviceUnavailable),
        _ => None,
    }
}
fn store_error(e: StoreError) -> Reason {
    match e {
        StoreError::UnsupportedPlatform => Reason::UnsupportedPlatform,
        StoreError::Busy => Reason::Busy,
        _ => Reason::Storage,
    }
}
pub(super) fn crypto_error(e: CryptoError) -> Reason {
    match e {
        CryptoError::Storage(e) => store_error(e),
        CryptoError::Busy => Reason::Busy,
        CryptoError::HistoryIndexNotReady => Reason::NotReady,
        CryptoError::Conflict => Reason::Conflict,
        CryptoError::ConversationClosed | CryptoError::ArchivedGeneration => {
            Reason::ConversationClosed
        }
        CryptoError::ConversationUnavailable | CryptoError::MessageUnavailable => Reason::NotFound,
        CryptoError::AttachmentRemoved => Reason::AttachmentRemoved,
        CryptoError::EpochSendingFrozen => Reason::SendingFrozen,
        CryptoError::EpochRecoveryRequired => Reason::RecoveryRequired,
        CryptoError::Expired => Reason::AcquisitionExpired,
        _ => Reason::Storage,
    }
}
fn discovery_error(e: DiscoveryError) -> Reason {
    match e {
        DiscoveryError::Rpc(e) => rpc_error(e),
        DiscoveryError::Crypto(e) => crypto_error(e),
        DiscoveryError::ExpectedGenerationChanged => Reason::GenerationChanged,
        _ => Reason::Runtime,
    }
}
pub(super) fn account_error(e: AccountError) -> Reason {
    match e {
        AccountError::Discovery(e) => discovery_error(e),
        AccountError::Rpc(e) => rpc_error(e),
        AccountError::Invalid => Reason::Invalid,
        AccountError::Busy => Reason::Busy,
        AccountError::Initializing | AccountError::Stopped => Reason::NotReady,
        AccountError::CommandTimeout => Reason::NativeWait,
        AccountError::SessionPending => Reason::SessionPending,
        AccountError::Crypto(e) => crypto_error(e),
        AccountError::Delivery(rust_processor::encryption::delivery_runner::RunnerError::Rpc(
            e,
        ))
        | AccountError::Maintenance(
            rust_processor::encryption::prekey_maintenance::MaintenanceError::Rpc(e),
        ) => rpc_error(e),
        AccountError::Delivery(
            rust_processor::encryption::delivery_runner::RunnerError::Crypto(e),
        )
        | AccountError::Maintenance(
            rust_processor::encryption::prekey_maintenance::MaintenanceError::Crypto(e),
        ) => crypto_error(e),
        _ => Reason::Runtime,
    }
}
fn rpc_error(e: RpcError) -> Reason {
    match e {
        RpcError::LocalDeadline => Reason::LocalDeadline,
        RpcError::Failure {
            reason: FailureReason::PasswordInvalid,
            ..
        } => Reason::PasswordInvalid,
        RpcError::Failure {
            reason: FailureReason::GenerationChanged,
            ..
        } => Reason::GenerationChanged,
        RpcError::Status(code) | RpcError::Failure { code, .. } => match code as i32 {
            // Fixed google.rpc.Code wire values, independent of diagnostic text.
            1 => Reason::RemoteCancelled,
            4 => Reason::RemoteDeadline,
            5 => Reason::NotFound,
            7 => Reason::Forbidden,
            16 => Reason::Authentication,
            _ => Reason::Remote,
        },
        _ => Reason::Remote,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn config_has_explicit_origin_ca_bindings_and_no_renderer_fields() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("servers.json");
        assert!(matches!(read_config(&path), Err(Reason::NotConfigured)));
        for body in [
            r#"[]"#,
            r#"[{"httpOrigin":"https://server.test","grpcOrigin":"http://127.0.0.1:9090"}]"#,
            r#"[{"httpOrigin":"http://lan.test","grpcOrigin":"http://127.0.0.1:9090"}]"#,
            r#"[{"httpOrigin":"https://server.test","grpcOrigin":"https://rpc.test","path":"untrusted"}]"#,
            r#"[{"httpOrigin":"http://localhost:8080","grpcOrigin":"http://localhost:9090"},{"httpOrigin":"http://localhost:8080/","grpcOrigin":"http://localhost:9091"}]"#,
        ] {
            std::fs::write(&path, body).unwrap();
            assert!(
                matches!(read_config(&path), Err(Reason::Configuration)),
                "{body}"
            );
        }
        std::fs::write(
            &path,
            r#"[{"httpOrigin":"http://localhost:8080","grpcOrigin":"http://127.0.0.1:9090"}]"#,
        )
        .unwrap();
        assert_eq!(read_config(&path).unwrap().len(), 1);
        std::fs::write(&path, vec![b' '; MAX_CONFIG + 1]).unwrap();
        assert!(matches!(read_config(&path), Err(Reason::Configuration)));
    }
    #[test]
    fn login_never_turns_unregistered_or_replacement_into_ready() {
        assert_eq!(
            blocked(&Reconciliation::Unregistered, Intent::Resume),
            Some(Phase::SetupRequired)
        );
        assert_eq!(blocked(&Reconciliation::Unregistered, Intent::Setup), None);
        assert_eq!(blocked(&Reconciliation::Ready, Intent::Resume), None);
        assert_eq!(
            blocked(&Reconciliation::RegistrationPending, Intent::Resume),
            None
        );
        assert_eq!(
            blocked(&Reconciliation::PublicationPending, Intent::Resume),
            None
        );
    }
    #[test]
    fn local_rpc_timer_remote_codes_and_authentication_remain_distinct() {
        assert_eq!(rpc_error(RpcError::LocalDeadline), Reason::LocalDeadline);
        assert_eq!(
            rpc_error(RpcError::Status(1.into())),
            Reason::RemoteCancelled
        );
        assert_eq!(
            rpc_error(RpcError::Status(4.into())),
            Reason::RemoteDeadline
        );
        assert_eq!(
            rpc_error(RpcError::Status(16.into())),
            Reason::Authentication
        );
        assert_eq!(
            rpc_error(RpcError::Failure {
                code: 16.into(),
                reason: FailureReason::PasswordInvalid
            }),
            Reason::PasswordInvalid
        );
        assert_eq!(
            rpc_error(RpcError::Failure {
                code: 9.into(),
                reason: FailureReason::GenerationChanged
            }),
            Reason::GenerationChanged
        );
    }
    #[tokio::test]
    async fn unconfigured_selected_origin_releases_no_jwt_and_opens_no_store() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("servers.json");
        std::fs::write(
            &config,
            r#"[{"httpOrigin":"http://127.0.0.1:8080","grpcOrigin":"http://127.0.0.1:9"}]"#,
        )
        .unwrap();
        let root = dir.path().join("absent-root");
        let factory = NativeFactory::load(config, root.clone());
        let outcome = factory
            .open(
                Binding {
                    intent: Intent::Resume,
                    origin: "http://127.0.0.1:8081".into(),
                    jwt: zeroize::Zeroizing::new("private.synthetic.token".into()),
                },
                None,
                CancellationToken::new(),
            )
            .await;
        assert!(matches!(outcome, Err(Reason::NotConfigured)));
        assert!(!root.exists());
    }
}
