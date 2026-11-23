use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Probe {
    events: Mutex<Vec<String>>,
    intents: Mutex<Vec<Intent>>,
    open_gate: Notify,
    close_gate: Notify,
    entered: Notify,
    hold_open: AtomicBool,
    hold_close: AtomicBool,
    close_error: AtomicBool,
    live: AtomicUsize,
    max_live: AtomicUsize,
}
struct FakeFactory(Arc<Probe>);
struct FakeRuntime(Arc<Probe>, String);
impl Factory for FakeFactory {
    fn open(
        &self,
        b: Binding,
        replacement: Option<ReplacementInput>,
        cancel: CancellationToken,
    ) -> BoxFuture<Result<Opened, Reason>> {
        let p = self.0.clone();
        Box::pin(async move {
            let name = b.jwt.to_string();
            assert_eq!(replacement.is_some(), b.intent == Intent::Replace);
            p.intents.lock().unwrap().push(b.intent);
            p.events.lock().unwrap().push(format!("open:{name}"));
            p.entered.notify_one();
            if p.hold_open.swap(false, Ordering::SeqCst) {
                p.open_gate.notified().await;
            }
            if name == "needs-setup" && b.intent == Intent::Resume {
                return Ok(Opened::Blocked(Phase::SetupRequired, None));
            }
            if name == "needs-replacement" && b.intent != Intent::Replace {
                return Ok(Opened::Blocked(
                    Phase::ReplacementRequired,
                    Some(ReplacementTarget {
                        expected: "native-target".into(),
                        pending: None,
                        continuing: false,
                    }),
                ));
            }
            if let Some(r) = replacement {
                assert_eq!(r.target.expected, "native-target");
                assert_eq!(r.password.as_str(), "synthetic-password");
            }
            if name == "unauthorized" {
                return Err(Reason::Authentication);
            }
            // Simulates a blocking initializer returning an owner just after stop.
            if cancel.is_cancelled() {
                p.events
                    .lock()
                    .unwrap()
                    .push("cancelled-initializer-returned".into());
            }
            let live = p.live.fetch_add(1, Ordering::SeqCst) + 1;
            p.max_live.fetch_max(live, Ordering::SeqCst);
            if name == "readonly" {
                return Ok(Opened::ReadOnly(
                    Phase::DeviceUnavailable,
                    None,
                    Box::new(FakeRuntime(p, name)),
                ));
            }
            Ok(Opened::Runtime(Box::new(FakeRuntime(p, name))))
        })
    }
}

#[tokio::test]
async fn readonly_runtime_never_grants_send_scope_and_is_joined_before_next_account() {
    let (m, p, t) = fixture();
    let a = select(&m, "readonly");
    phase(&m, a, Phase::DeviceUnavailable).await;
    assert!(matches!(m.dialog_client(a), Err(Reason::NotReady)));
    assert_eq!(m.ensure_views_ready(a, false), Err(Reason::NotReady));
    assert_eq!(m.setup(a), Err(Reason::Invalid));
    p.hold_close.store(true, Ordering::SeqCst);
    let b = select(&m, "bob");
    event(&p, "stopping:readonly").await;
    assert_eq!(m.ensure_views_ready(a, true), Err(Reason::Stale));
    assert!(!p.events.lock().unwrap().iter().any(|e| e == "open:bob"));
    p.close_gate.notify_one();
    phase(&m, b, Phase::Active).await;
    assert_eq!(p.max_live.load(Ordering::SeqCst), 1);
    m.shutdown().await;
    t.await.unwrap();
    assert_eq!(p.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn cancelled_readonly_initialization_is_closed_and_cannot_publish_to_a_new_context() {
    let (m, p, t) = fixture();
    p.hold_open.store(true, Ordering::SeqCst);
    let a = select(&m, "readonly");
    event(&p, "open:readonly").await;
    let b = select(&m, "bob");
    p.open_gate.notify_one();
    phase(&m, b, Phase::Active).await;
    assert_eq!(m.snapshot(a), Err(Reason::Stale));
    event(&p, "closed:readonly").await;
    assert_eq!(p.max_live.load(Ordering::SeqCst), 1);
    m.shutdown().await;
    t.await.unwrap();
}
impl Runtime for FakeRuntime {
    fn client(&self) -> Option<AccountClient> {
        None
    }
    fn shutdown(self: Box<Self>) -> BoxFuture<Result<(), Reason>> {
        Box::pin(async move {
            self.0
                .events
                .lock()
                .unwrap()
                .push(format!("stopping:{}", self.1));
            self.0.entered.notify_one();
            if self.0.hold_close.swap(false, Ordering::SeqCst) {
                self.0.close_gate.notified().await;
            }
            self.0.live.fetch_sub(1, Ordering::SeqCst);
            self.0
                .events
                .lock()
                .unwrap()
                .push(format!("closed:{}", self.1));
            if self.0.close_error.swap(false, Ordering::SeqCst) {
                Err(Reason::Storage)
            } else {
                Ok(())
            }
        })
    }
}
fn fixture() -> (SessionManager, Arc<Probe>, tokio::task::JoinHandle<()>) {
    let p = Arc::new(Probe::default());
    let (m, task) = SessionManager::new(Arc::new(FakeFactory(p.clone())));
    let task = tokio::spawn(task);
    (m, p, task)
}
fn select(m: &SessionManager, jwt: &str) -> u64 {
    m.bind(
        m.context(),
        "http://127.0.0.1:8080".into(),
        Some(jwt.into()),
    )
    .unwrap()
}
async fn phase(m: &SessionManager, epoch: u64, expected: Phase) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        let mut updates = m.0.changed.subscribe();
        loop {
            if m.snapshot(epoch).unwrap().phase == expected {
                return;
            }
            updates.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
async fn event(p: &Probe, expected: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let notified = p.entered.notified();
            if p.events.lock().unwrap().iter().any(|s| s == expected) {
                return;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn account_change_waits_for_old_store_and_invalidates_stale_contexts() {
    let (m, p, t) = fixture();
    let a = select(&m, "alice");
    phase(&m, a, Phase::Active).await;
    assert_eq!(select(&m, "alice"), a);
    p.hold_close.store(true, Ordering::SeqCst);
    let b = select(&m, "bob");
    assert_eq!(m.snapshot(a), Err(Reason::Stale));
    event(&p, "stopping:alice").await;
    assert_eq!(m.snapshot(b).unwrap().phase, Phase::Stopping);
    assert!(!p.events.lock().unwrap().iter().any(|s| s == "open:bob"));
    p.close_gate.notify_one();
    phase(&m, b, Phase::Active).await;
    assert_eq!(p.max_live.load(Ordering::SeqCst), 1);
    let events = p.events.lock().unwrap().clone();
    assert!(
        events.iter().position(|s| s == "closed:alice")
            < events.iter().position(|s| s == "open:bob")
    );
    m.shutdown().await;
    t.await.unwrap();
    assert_eq!(p.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn blocked_initialization_is_joined_and_latest_session_wins_without_spawning_more() {
    let (m, p, t) = fixture();
    p.hold_open.store(true, Ordering::SeqCst);
    let a = select(&m, "alice");
    event(&p, "open:alice").await;
    let waiter = {
        let m = m.clone();
        tokio::spawn(async move { m.wait_released(a).await })
    };
    for n in 0..100 {
        select(&m, &format!("bob{n}"));
    }
    let b = m.context();
    assert_eq!(m.snapshot(b).unwrap().phase, Phase::Stopping);
    assert_eq!(p.events.lock().unwrap().len(), 1);
    waiter.abort();
    p.open_gate.notify_one();
    phase(&m, b, Phase::Active).await;
    let events = p.events.lock().unwrap().clone();
    assert_eq!(events.iter().filter(|s| s.starts_with("open:")).count(), 2);
    assert!(events.iter().any(|s| s == "cancelled-initializer-returned"));
    assert_eq!(p.max_live.load(Ordering::SeqCst), 1);
    m.shutdown().await;
    t.await.unwrap();
}
#[tokio::test]
async fn abandoned_shutdown_waiter_does_not_skip_cleanup_and_new_login_is_closed() {
    let (m, p, t) = fixture();
    let a = select(&m, "alice");
    phase(&m, a, Phase::Active).await;
    p.hold_close.store(true, Ordering::SeqCst);
    let waiter = {
        let m = m.clone();
        tokio::spawn(async move { m.shutdown().await })
    };
    event(&p, "stopping:alice").await;
    assert!(matches!(
        m.bind(
            m.context(),
            "http://127.0.0.1:8080".into(),
            Some("bob".into())
        ),
        Err(Reason::Closed)
    ));
    waiter.abort();
    assert_eq!(p.live.load(Ordering::SeqCst), 1);
    p.close_gate.notify_one();
    t.await.unwrap();
    assert_eq!(m.snapshot(m.context()).unwrap().phase, Phase::Closed);
    assert_eq!(p.live.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn failed_cleanup_blocks_any_further_store_open() {
    let (m, p, t) = fixture();
    let a = select(&m, "alice");
    phase(&m, a, Phase::Active).await;
    p.close_error.store(true, Ordering::SeqCst);
    let b = select(&m, "bob");
    phase(&m, b, Phase::Failed).await;
    assert_eq!(m.snapshot(b).unwrap().error, Some(Reason::Storage));
    let c = select(&m, "charlie");
    phase(&m, c, Phase::Failed).await;
    assert_eq!(
        p.events
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.starts_with("open:"))
            .count(),
        1
    );
    m.shutdown().await;
    t.await.unwrap();
}
#[tokio::test]
async fn auth_failure_is_redacted_and_not_an_identity_replacement() {
    let (m, p, t) = fixture();
    let a = select(&m, "unauthorized");
    phase(&m, a, Phase::Failed).await;
    let v = m.snapshot(a).unwrap();
    assert_eq!(v.error, Some(Reason::Authentication));
    let json = serde_json::to_string(&v).unwrap();
    assert!(!json.contains("unauthorized") && !json.contains("127.0.0.1"));
    assert_eq!(p.live.load(Ordering::SeqCst), 0);
    let b = select(&m, "alice");
    phase(&m, b, Phase::Active).await;
    m.shutdown().await;
    t.await.unwrap();
}
#[tokio::test]
async fn invalid_or_stale_bind_does_not_replace_the_current_session() {
    let (m, p, t) = fixture();
    let old = m.context();
    let a = select(&m, "alice");
    phase(&m, a, Phase::Active).await;
    assert!(matches!(
        m.bind(old, "native-origin".into(), Some("bob".into())),
        Err(Reason::Stale)
    ));
    for jwt in ["", "has spaces", "secret\nheader"] {
        assert!(matches!(
            m.bind(a, "native-origin".into(), Some(jwt.into())),
            Err(Reason::Invalid)
        ));
    }
    assert_eq!(m.context(), a);
    assert_eq!(p.live.load(Ordering::SeqCst), 1);
    let idle = m.clear();
    m.wait_released(idle).await.unwrap();
    assert_eq!(p.live.load(Ordering::SeqCst), 0);
    m.shutdown().await;
    t.await.unwrap();
}
#[tokio::test]
async fn never_polled_task_drop_closes_the_context() {
    let p = Arc::new(Probe::default());
    let (m, task) = SessionManager::new(Arc::new(FakeFactory(p.clone())));
    let a = select(&m, "alice");
    drop(task);
    assert_eq!(m.snapshot(a).unwrap().phase, Phase::Failed);
    assert!(matches!(
        m.bind(a, "native-origin".into(), Some("bob".into())),
        Err(Reason::Closed)
    ));
    assert!(p.events.lock().unwrap().is_empty());
    m.shutdown().await;
}

#[tokio::test]
async fn initial_setup_is_explicit_and_retry_does_not_keep_setup_permission() {
    let (m, p, t) = fixture();
    assert!(matches!(m.setup(m.context()), Err(Reason::Invalid)));
    let a = select(&m, "needs-setup");
    phase(&m, a, Phase::SetupRequired).await;
    assert_eq!(*p.intents.lock().unwrap(), vec![Intent::Resume]);
    let parent = a - 1;
    let setup = m.setup(a).unwrap();
    phase(&m, setup, Phase::Active).await;
    assert_eq!(m.snapshot_for_binding(parent).unwrap().context, setup);
    assert!(matches!(m.snapshot(a), Err(Reason::Stale)));
    assert!(matches!(m.setup(setup), Err(Reason::Invalid)));
    assert!(matches!(m.retry(setup), Err(Reason::Invalid)));
    assert_eq!(
        *p.intents.lock().unwrap(),
        vec![Intent::Resume, Intent::Setup]
    );
    let failed = select(&m, "unauthorized");
    phase(&m, failed, Phase::Failed).await;
    let retried = m.retry(failed).unwrap();
    phase(&m, retried, Phase::Failed).await;
    assert_eq!(p.intents.lock().unwrap().last(), Some(&Intent::Resume));
    m.shutdown().await;
    t.await.unwrap();
}
#[tokio::test]
async fn immutable_binding_lease_retires_initialization_before_its_reply_but_cannot_close_a_new_login()
 {
    let (m, p, t) = fixture();
    let parent = m.context();
    p.hold_open.store(true, Ordering::SeqCst);
    let pending = select(&m, "alice");
    event(&p, "open:alice").await;
    let stopped = m.retire(parent).unwrap();
    assert!(matches!(m.snapshot(pending), Err(Reason::Stale)));
    p.open_gate.notify_one();
    phase(&m, stopped, Phase::Idle).await;
    let b = select(&m, "bob");
    phase(&m, b, Phase::Active).await;
    assert!(matches!(m.retire(parent), Err(Reason::Stale)));
    assert!(matches!(m.snapshot_for_binding(parent), Err(Reason::Stale)));
    assert_eq!(p.live.load(Ordering::SeqCst), 1);
    m.shutdown().await;
    t.await.unwrap();
}
#[tokio::test]
async fn setup_and_retry_context_changes_do_not_invalidate_the_page_retirement_lease() {
    let (m, p, t) = fixture();
    let parent = m.context();
    let a = select(&m, "needs-setup");
    phase(&m, a, Phase::SetupRequired).await;
    p.hold_open.store(true, Ordering::SeqCst);
    let setup = m.setup(a).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let notified = p.entered.notified();
            if p.intents.lock().unwrap().len() >= 2 {
                break;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
    let idle = m.retire(parent).unwrap();
    assert!(matches!(m.snapshot(setup), Err(Reason::Stale)));
    p.open_gate.notify_one();
    phase(&m, idle, Phase::Idle).await;
    assert_eq!(p.live.load(Ordering::SeqCst), 0);
    let parent = m.context();
    let retired = m.retire(parent).unwrap();
    assert!(matches!(
        m.bind(parent, "native-origin".into(), Some("late-bind".into())),
        Err(Reason::Stale)
    ));
    phase(&m, retired, Phase::Idle).await;
    m.shutdown().await;
    t.await.unwrap();
}

#[tokio::test]
async fn replacement_is_one_shot_native_targeted_and_requires_current_confirmation() {
    let (m, p, t) = fixture();
    let a = select(&m, "needs-replacement");
    phase(&m, a, Phase::ReplacementRequired).await;
    assert_eq!(
        m.snapshot(a)
            .unwrap()
            .replacement
            .unwrap()
            .expected_generation,
        "native-target"
    );
    let pwd = || Zeroizing::new("synthetic-password".into());
    assert_eq!(m.replace(a, pwd(), false), Err(Reason::Invalid));
    assert_eq!(m.replace(a - 1, pwd(), true), Err(Reason::Stale));
    assert_eq!(
        m.replace(a, Zeroizing::new("é".repeat(37)), true),
        Err(Reason::Invalid)
    );
    let next = m.replace(a, pwd(), true).unwrap();
    assert_eq!(m.replace(next, pwd(), true), Err(Reason::Invalid));
    phase(&m, next, Phase::Active).await;
    assert_eq!(m.replace(next, pwd(), true), Err(Reason::Invalid));
    assert_eq!(
        *p.intents.lock().unwrap(),
        vec![Intent::Resume, Intent::Replace]
    );
    assert!(m.0.state.lock().unwrap().replacement.is_none());
    let snapshot = serde_json::to_string(&m.snapshot(next).unwrap()).unwrap();
    assert!(!snapshot.contains("synthetic-password"));
    m.shutdown().await;
    t.await.unwrap();
}
#[tokio::test]
async fn retirement_discards_a_queued_password_without_opening_the_replacement() {
    let (m, p, t) = fixture();
    let a = select(&m, "needs-replacement");
    phase(&m, a, Phase::ReplacementRequired).await;
    // No await between enqueue and retirement: the one lifecycle task cannot take it.
    let queued = m
        .replace(a, Zeroizing::new("synthetic-password".into()), true)
        .unwrap();
    assert!(m.0.state.lock().unwrap().replacement.is_some());
    let idle = m.retire(a - 1).unwrap();
    phase(&m, idle, Phase::Idle).await;
    assert_eq!(m.snapshot(queued), Err(Reason::Stale));
    assert!(m.0.state.lock().unwrap().replacement.is_none());
    assert_eq!(*p.intents.lock().unwrap(), vec![Intent::Resume]);
    m.shutdown().await;
    t.await.unwrap();
}

#[tokio::test]
async fn display_phase_without_live_account_client_cannot_authorize_dialog_commands() {
    let (m, _, t) = fixture();
    let old = m.context();
    let a = select(&m, "alice");
    phase(&m, a, Phase::Active).await;
    // FakeRuntime advertises Active but intentionally has no real AccountClient.
    assert_eq!(m.ensure_dialog_ready(a), Err(Reason::NotReady));
    assert_eq!(m.ensure_dialog_ready(old), Err(Reason::Stale));
    let setup = select(&m, "needs-setup");
    phase(&m, setup, Phase::SetupRequired).await;
    assert_eq!(m.ensure_dialog_ready(setup), Err(Reason::NotReady));
    m.shutdown().await;
    t.await.unwrap();
}
