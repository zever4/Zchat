//! Latest desired session wins; exactly one lifecycle task owns initialization
//! and cleanup. An abandoned invoke cannot detach cleanup or publish stale keys.
use super::{
    Phase, Reason, Snapshot,
    replacement::{ReplacementInput, ReplacementTarget},
};
use rust_processor::encryption::account_owner::{
    AccountClient, AccountOwner, AccountPhase, AccountUpdates,
};
use rust_processor::encryption::signal_state::{ArchiveClient, ArchiveOwner};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;
use zeroize::Zeroizing;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Intent {
    Resume,
    Setup,
    Replace,
}
#[derive(Clone)]
pub(super) struct Binding {
    pub intent: Intent,
    pub origin: String,
    pub jwt: Zeroizing<String>,
}
pub(super) trait Runtime: Send {
    fn client(&self) -> Option<AccountClient>;
    fn archive_client(&self) -> Option<ArchiveClient> {
        None
    }
    fn shutdown(self: Box<Self>) -> BoxFuture<Result<(), Reason>>;
}
impl Runtime for ArchiveOwner {
    fn client(&self) -> Option<AccountClient> {
        None
    }
    fn archive_client(&self) -> Option<ArchiveClient> {
        Some(self.client())
    }
    fn shutdown(self: Box<Self>) -> BoxFuture<Result<(), Reason>> {
        Box::pin(async move {
            (*self)
                .shutdown()
                .await
                .map_err(super::factory::crypto_error)
        })
    }
}
impl Runtime for AccountOwner {
    fn client(&self) -> Option<AccountClient> {
        Some(self.client())
    }
    fn shutdown(self: Box<Self>) -> BoxFuture<Result<(), Reason>> {
        Box::pin(async move {
            (*self)
                .shutdown()
                .await
                .map_err(super::factory::account_error)
        })
    }
}
pub(super) enum Opened {
    Runtime(Box<dyn Runtime>),
    Blocked(Phase, Option<ReplacementTarget>),
    ReadOnly(Phase, Option<ReplacementTarget>, Box<dyn Runtime>),
}
pub(super) trait Factory: Send + Sync + 'static {
    fn open(
        &self,
        binding: Binding,
        replacement: Option<ReplacementInput>,
        cancel: CancellationToken,
    ) -> BoxFuture<Result<Opened, Reason>>;
}
struct State {
    epoch: u64,
    binding_context: Option<u64>,
    desired: Option<Binding>,
    cancel: CancellationToken,
    snapshot: Snapshot,
    client: Option<AccountClient>,
    archive: Option<ArchiveClient>,
    closing: bool,
    replacement: Option<ReplacementInput>,
    target: Option<ReplacementTarget>,
}
struct Shared {
    state: Mutex<State>,
    wake: Notify,
    changed: watch::Sender<u64>,
    local_file_gate: Arc<tokio::sync::Semaphore>,
}
#[derive(Clone)]
pub struct SessionManager(Arc<Shared>);
impl SessionManager {
    pub(super) fn new(
        factory: Arc<dyn Factory>,
    ) -> (Self, impl Future<Output = ()> + Send + 'static) {
        let (changed, _) = watch::channel(1);
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                epoch: 1,
                binding_context: None,
                desired: None,
                cancel: CancellationToken::new(),
                snapshot: Snapshot::new(1, Phase::Idle, None),
                client: None,
                archive: None,
                closing: false,
                replacement: None,
                target: None,
            }),
            wake: Notify::new(),
            changed,
            local_file_gate: Arc::new(tokio::sync::Semaphore::new(1)),
        });
        let guard = TaskGuard(shared.clone());
        (Self(shared.clone()), run(shared, factory, guard))
    }
    pub fn context(&self) -> u64 {
        self.0.state.lock().expect("E2EE session lock").epoch
    }
    pub fn snapshot(&self, context: u64) -> Result<Snapshot, Reason> {
        let s = self.0.state.lock().expect("E2EE session lock");
        if s.epoch != context {
            return Err(Reason::Stale);
        }
        let mut snapshot = s.snapshot.clone();
        if let Some(c) = &s.client {
            let r = c.status();
            snapshot.runtime_phase = Some(format!("{:?}", r.phase));
            snapshot.incoming_message_revision = Some(r.incoming_message_revision.to_string());
            snapshot.delivery_health = r.delivery_health.map(|v| format!("{v:?}"));
            snapshot.maintenance_health = r.maintenance_health.map(|v| format!("{v:?}"));
        } else if s.archive.as_ref().is_some_and(ArchiveClient::is_running) {
            snapshot.runtime_phase = Some("ArchiveReadOnly".into());
        }
        Ok(snapshot)
    }
    /// Read-only recovery after an invoke lost its operation-context reply. The
    /// immutable authentication lease cannot adopt another account's new context.
    pub fn snapshot_for_binding(&self, bound_context: u64) -> Result<Snapshot, Reason> {
        let context = {
            let s = self.0.state.lock().expect("E2EE session lock");
            if s.binding_context != Some(bound_context) {
                return Err(Reason::Stale);
            }
            s.epoch
        };
        self.snapshot(context)
    }
    pub(super) fn dialog_client(
        &self,
        context: u64,
    ) -> Result<(AccountClient, CancellationToken), Reason> {
        let s = self.0.state.lock().expect("E2EE session lock");
        if s.epoch != context {
            return Err(Reason::Stale);
        }
        if s.closing || s.snapshot.phase != Phase::Active {
            return Err(Reason::NotReady);
        }
        let client = s.client.clone().ok_or(Reason::NotReady)?;
        if client.status().phase != AccountPhase::Running {
            return Err(Reason::NotReady);
        }
        Ok((client, s.cancel.clone()))
    }
    pub(super) fn ensure_dialog_ready(&self, context: u64) -> Result<(), Reason> {
        self.dialog_client(context).map(|_| ())
    }
    pub(super) async fn dialog(
        &self,
        context: u64,
        request: super::dialogs::DialogRequest,
    ) -> Result<super::dialogs::Dialog, Reason> {
        let (client, cancel) = self.dialog_client(context)?;
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Reason::Stale),
            r = super::dialogs::request(&client,request) => r,
        };
        self.snapshot(context)?; // A stale account must not expose any old result.
        if result.is_ok() {
            self.ensure_dialog_ready(context)?;
        }
        result
    }
    pub(super) async fn recovery(
        &self,
        context: u64,
        operation: super::recovery::RecoveryRequest,
    ) -> Result<super::recovery::RecoveryReply, Reason> {
        let (client, cancel) = self.dialog_client(context)?;
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Reason::Stale),
            r = super::recovery::request(&client, operation) => r,
        };
        self.snapshot(context)?;
        if result.is_ok() {
            self.ensure_dialog_ready(context)?;
        }
        result
    }
    pub(super) async fn send_text(
        &self,
        context: u64,
        input: super::text::TextRequest,
    ) -> Result<super::text::TextQueued, Reason> {
        let (client, cancel) = self.dialog_client(context)?;
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Reason::Stale),
            r = super::text::request(&client,input) => r,
        };
        self.snapshot(context)?;
        if result.is_ok() {
            self.ensure_dialog_ready(context)?;
        }
        result
    }
    pub(super) async fn read_views(
        &self,
        context: u64,
        query: rust_processor::encryption::signal_state::ViewRequest,
    ) -> Result<rust_processor::encryption::signal_state::ViewPage, Reason> {
        let archive = query.is_archive();
        let (client, cancel) = self.view_client(context, archive)?;
        let result = tokio::select! {
            biased;
            _=cancel.cancelled()=>return Err(Reason::Stale),
            r=async {
                match client {
                    ViewClient::Current(c) => c.read_views(query).await.map_err(super::factory::account_error),
                    ViewClient::Archive(c) => c.read_views(query).await.map_err(super::factory::crypto_error),
                }
            }=>r,
        };
        self.snapshot(context)?;
        if result.is_ok() {
            self.ensure_views_ready(context, archive)?;
        }
        result
    }
    fn view_client(
        &self,
        context: u64,
        archive: bool,
    ) -> Result<(ViewClient, CancellationToken), Reason> {
        let s = self.0.state.lock().expect("E2EE session lock");
        if s.epoch != context {
            return Err(Reason::Stale);
        }
        if s.closing {
            return Err(Reason::NotReady);
        }
        if s.snapshot.phase == Phase::Active {
            let c = s
                .client
                .clone()
                .filter(|c| c.status().phase == AccountPhase::Running)
                .ok_or(Reason::NotReady)?;
            return Ok((ViewClient::Current(c), s.cancel.clone()));
        }
        if archive
            && matches!(
                s.snapshot.phase,
                Phase::DeviceUnavailable | Phase::ReplacementPending | Phase::ReplacementRequired
            )
        {
            let c = s
                .archive
                .clone()
                .filter(ArchiveClient::is_running)
                .ok_or(Reason::NotReady)?;
            return Ok((ViewClient::Archive(c), s.cancel.clone()));
        }
        Err(Reason::NotReady)
    }
    pub(super) fn ensure_views_ready(&self, context: u64, archive: bool) -> Result<(), Reason> {
        self.view_client(context, archive).map(|_| ())
    }
    pub(super) async fn open_local_attachment(
        &self,
        context: u64,
        generation: Option<&str>,
        conversation: &str,
        message: &str,
    ) -> Result<rust_processor::encryption::signal_state::EpochAttachmentFile, Reason> {
        let (client, cancel) = self.view_client(context, generation.is_some())?;
        let open = async {
            match (client, generation) {
                (ViewClient::Current(c), None) => {
                    c.open_current_attachment(conversation, message).await
                }
                (ViewClient::Current(c), Some(g)) => {
                    c.open_archived_attachment(g, conversation, message).await
                }
                (ViewClient::Archive(c), Some(g)) => c
                    .open_archived_attachment(g, conversation, message)
                    .await
                    .map_err(Into::into),
                (ViewClient::Archive(_), None) => return Err(Reason::NotReady),
            }
            .map_err(super::factory::account_error)
        };
        let file = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Reason::Stale),
            result = open => result?,
        };
        self.ensure_views_ready(context, generation.is_some())?;
        Ok(file)
    }
    pub(super) fn view_cancellation(
        &self,
        context: u64,
        archive: bool,
    ) -> Result<CancellationToken, Reason> {
        self.view_client(context, archive).map(|(_, cancel)| cancel)
    }
    pub(super) fn local_file_permit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Reason> {
        self.0
            .local_file_gate
            .clone()
            .try_acquire_owned()
            .map_err(|_| Reason::Busy)
    }
    pub fn bind(&self, expected: u64, origin: String, jwt: Option<String>) -> Result<u64, Reason> {
        let jwt = jwt.map(Zeroizing::new);
        if jwt.as_ref().is_some_and(|j| {
            j.is_empty()
                || j.len() > 8192
                || !j
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'))
        }) {
            return Err(Reason::Invalid);
        }
        let desired = jwt.map(|jwt| Binding {
            origin,
            jwt,
            intent: Intent::Resume,
        });
        let mut s = self.0.state.lock().expect("E2EE session lock");
        if s.closing {
            return Err(Reason::Closed);
        }
        if s.epoch != expected {
            return Err(Reason::Stale);
        }
        if let (Some(a), Some(b)) = (&s.desired, &desired)
            && a.origin == b.origin
            && a.jwt.as_str() == b.jwt.as_str()
        {
            return Ok(s.epoch);
        }
        let bound = desired.is_some();
        self.change(&mut s, desired)?;
        s.binding_context = bound.then_some(expected);
        Ok(s.epoch)
    }
    /// The page knows this immutable parent context BEFORE its bind reply. Setup
    /// and retry advance operation contexts without changing this retirement lease.
    /// A late old page cannot retire a newer authentication binding.
    pub fn retire(&self, bound_context: u64) -> Result<u64, Reason> {
        let mut s = self.0.state.lock().expect("E2EE session lock");
        if s.closing {
            return Err(Reason::Closed);
        }
        if s.binding_context != Some(bound_context)
            && !(s.desired.is_none() && s.epoch == bound_context)
        {
            return Err(Reason::Stale);
        }
        self.change(&mut s, None)?;
        Ok(s.epoch)
    }
    /// Explicit first setup only. Native discovery must have observed no usable
    /// local installation; re-discovery in the factory still precedes any create.
    pub fn setup(&self, expected: u64) -> Result<u64, Reason> {
        self.action(expected, Intent::Setup)
    }
    /// Reconcile/replay only committed state. A failed setup before committing
    /// any initial plan requires another explicit setup action, never new keys.
    pub fn retry(&self, expected: u64) -> Result<u64, Reason> {
        self.action(expected, Intent::Resume)
    }
    /// A single-use password belongs to this operation only, never to the
    /// clonable login binding, status or retry. The target comes from discovery.
    pub fn replace(
        &self,
        expected: u64,
        password: Zeroizing<String>,
        confirmed: bool,
    ) -> Result<u64, Reason> {
        if !confirmed || password.is_empty() || password.len() > 72 {
            return Err(Reason::Invalid);
        }
        let mut s = self.0.state.lock().expect("E2EE session lock");
        if s.closing {
            return Err(Reason::Closed);
        }
        if s.epoch != expected {
            return Err(Reason::Stale);
        }
        let target = s.target.clone().ok_or(Reason::Invalid)?;
        let mut binding = s.desired.clone().ok_or(Reason::Invalid)?;
        binding.intent = Intent::Replace;
        self.change(&mut s, Some(binding))?;
        s.replacement = Some(ReplacementInput { target, password });
        Ok(s.epoch)
    }
    fn action(&self, expected: u64, intent: Intent) -> Result<u64, Reason> {
        let mut s = self.0.state.lock().expect("E2EE session lock");
        if s.closing {
            return Err(Reason::Closed);
        }
        if s.epoch != expected {
            return Err(Reason::Stale);
        }
        let allowed = match intent {
            Intent::Setup => s.snapshot.phase == Phase::SetupRequired,
            Intent::Resume => matches!(
                s.snapshot.phase,
                Phase::Failed
                    | Phase::ReplacementRequired
                    | Phase::ReplacementPending
                    | Phase::DeviceUnavailable
            ),
            Intent::Replace => false,
        };
        if !allowed {
            return Err(Reason::Invalid);
        }
        let mut binding = s.desired.clone().ok_or(Reason::Invalid)?;
        binding.intent = intent;
        self.change(&mut s, Some(binding))?;
        Ok(s.epoch)
    }
    fn change(&self, s: &mut State, desired: Option<Binding>) -> Result<(), Reason> {
        s.epoch = s
            .epoch
            .checked_add(1)
            .filter(|n| *n <= 9_007_199_254_740_991)
            .ok_or(Reason::Runtime)?;
        s.cancel.cancel();
        s.cancel = CancellationToken::new();
        if desired.is_none() {
            s.binding_context = None;
        }
        s.replacement = None;
        s.target = None;
        s.desired = desired;
        s.client = None;
        s.archive = None;
        s.snapshot = Snapshot::new(s.epoch, Phase::Stopping, None);
        self.0.changed.send_replace(s.epoch);
        self.0.wake.notify_one();
        Ok(())
    }
    pub fn clear(&self) -> u64 {
        let mut s = self.0.state.lock().expect("E2EE session lock");
        if !s.closing && self.change(&mut s, None).is_err() {
            s.closing = true;
            s.cancel.cancel();
            self.0.wake.notify_one();
        }
        s.epoch
    }
    pub async fn wait_released(&self, context: u64) -> Result<Snapshot, Reason> {
        let mut changed = self.0.changed.subscribe();
        loop {
            let snapshot = self.snapshot(context)?;
            if snapshot.phase != Phase::Stopping {
                return Ok(snapshot);
            }
            changed.changed().await.map_err(|_| Reason::Closed)?;
        }
    }
    pub async fn shutdown(&self) {
        {
            let mut s = self.0.state.lock().expect("E2EE session lock");
            if !s.closing {
                let _ = self.change(&mut s, None);
                s.closing = true;
                s.cancel.cancel();
                self.0.wake.notify_one();
            }
        }
        let mut changed = self.0.changed.subscribe();
        loop {
            let phase = self
                .0
                .state
                .lock()
                .expect("E2EE session lock")
                .snapshot
                .phase;
            if matches!(phase, Phase::Closed | Phase::Failed) {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
}
fn publish(
    shared: &Shared,
    epoch: u64,
    phase: Phase,
    error: Option<Reason>,
    client: Option<AccountClient>,
) -> bool {
    let mut s = shared.state.lock().expect("E2EE session lock");
    if s.epoch != epoch {
        return false;
    }
    s.target = None;
    s.snapshot = Snapshot::new(epoch, phase, error);
    s.client = client;
    s.archive = None;
    shared.changed.send_replace(epoch);
    true
}
async fn run(shared: Arc<Shared>, factory: Arc<dyn Factory>, _guard: TaskGuard) {
    let mut applied = 1;
    let mut owner: Option<Box<dyn Runtime>> = None;
    let mut poisoned = false;
    let mut updates: Option<AccountUpdates> = None;
    loop {
        let next = {
            let mut s = shared.state.lock().expect("E2EE session lock");
            (s.epoch != applied || s.closing).then(|| {
                (
                    s.epoch,
                    s.desired.clone(),
                    s.cancel.clone(),
                    s.closing,
                    s.replacement.take(),
                )
            })
        };
        let Some((epoch, desired, cancel, closing, replacement)) = next else {
            tokio::select! {
                biased;
                _=shared.wake.notified()=>{},
                observation=async {
                    match &mut updates {
                        Some(updates)=>updates.changed().await,
                        None=>std::future::pending().await,
                    }
                }=>{
                    if observation.is_none_or(|s| matches!(s.phase,AccountPhase::Failed|AccountPhase::Stopped)) {
                        updates=None;
                        if let Some(runtime)=owner.take() {
                            let error=runtime.shutdown().await.err().unwrap_or(Reason::Runtime);
                            poisoned|=unsafe_cleanup(error);
                            publish(&shared,applied,Phase::Failed,Some(error),None);
                        }
                    }
                }
            }
            continue;
        };
        updates = None;
        if let Some(old) = owner.take()
            && let Err(error) = old.shutdown().await
        {
            poisoned |= unsafe_cleanup(error);
        }
        // A stop while initialization was joining must not publish its old result.
        let current = {
            let s = shared.state.lock().expect("E2EE session lock");
            s.epoch == epoch
        };
        if !current {
            continue;
        }
        applied = epoch;
        if closing {
            publish(
                &shared,
                epoch,
                Phase::Closed,
                poisoned.then_some(Reason::Storage),
                None,
            );
            return;
        }
        if poisoned {
            publish(&shared, epoch, Phase::Failed, Some(Reason::Storage), None);
            continue;
        }
        let Some(binding) = desired else {
            publish(&shared, epoch, Phase::Idle, None, None);
            continue;
        };
        publish(&shared, epoch, Phase::Starting, None, None);
        match factory.open(binding, replacement, cancel.clone()).await {
            Ok(Opened::Runtime(runtime)) => {
                if cancel.is_cancelled()
                    || !publish(&shared, epoch, Phase::Active, None, runtime.client())
                {
                    if let Err(error) = runtime.shutdown().await {
                        poisoned |= unsafe_cleanup(error);
                    }
                } else {
                    updates = runtime.client().map(|c| c.subscribe());
                    owner = Some(runtime);
                }
            }
            Ok(Opened::Blocked(phase, target)) => {
                let mut s = shared.state.lock().expect("E2EE session lock");
                if s.epoch == epoch {
                    s.snapshot = Snapshot::new(epoch, phase, None);
                    s.snapshot.replacement = target.as_ref().map(ReplacementTarget::view);
                    s.target = target;
                    shared.changed.send_replace(epoch);
                }
            }
            Ok(Opened::ReadOnly(phase, target, runtime)) => {
                let accepted = {
                    let mut s = shared.state.lock().expect("E2EE session lock");
                    if s.epoch != epoch || cancel.is_cancelled() {
                        false
                    } else {
                        s.snapshot = Snapshot::new(epoch, phase, None);
                        s.snapshot.replacement = target.as_ref().map(ReplacementTarget::view);
                        s.target = target;
                        s.client = None;
                        s.archive = runtime.archive_client();
                        shared.changed.send_replace(epoch);
                        true
                    }
                };
                if accepted {
                    owner = Some(runtime)
                } else if let Err(error) = runtime.shutdown().await {
                    poisoned |= unsafe_cleanup(error);
                }
            }
            Err(error) => {
                publish(&shared, epoch, Phase::Failed, Some(error), None);
            }
        }
    }
}
fn unsafe_cleanup(error: Reason) -> bool {
    matches!(error, Reason::Storage | Reason::Runtime)
}
struct TaskGuard(Arc<Shared>);
impl Drop for TaskGuard {
    fn drop(&mut self) {
        let mut s = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        if s.snapshot.phase != Phase::Closed {
            s.cancel.cancel();
            s.desired = None;
            s.replacement = None;
            s.target = None;
            s.binding_context = None;
            s.client = None;
            s.archive = None;
            s.closing = true;
            s.snapshot = Snapshot::new(s.epoch, Phase::Failed, Some(Reason::Runtime));
            self.0.changed.send_replace(s.epoch);
        }
    }
}

enum ViewClient {
    Current(AccountClient),
    Archive(ArchiveClient),
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
