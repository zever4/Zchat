//! Opt-in, headless Windows acceptance. No WebView/login UI acceptance claim.
use super::{Phase, Reason, factory::NativeFactory, session::SessionManager};
use rust_processor::encryption::{
    discovery::TrustedServer,
    protected::{ProtectedStore, StoreScope},
    provisioning::DeviceProvisioning,
};
use std::{io::Read, path::Path, sync::Arc, time::Duration};
use zeroize::Zeroizing;

const HTTP: &str = "https://desktop-session.invalid";
#[test]
#[ignore = "requires the actual Go TLS/PostgreSQL desktop-session harness on Windows"]
fn go_desktop_session_worker() {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin().read_to_string(&mut input).unwrap();
    let fields: Vec<_> = input
        .lines()
        .map(|s| Zeroizing::new(s.to_owned()))
        .collect();
    assert_eq!(fields.len(), 4);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(run(&fields));
    println!("zchat-desktop-session-complete:v1");
}
async fn observed(manager: &SessionManager, context: u64, phase: Phase, network: bool) {
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let s = manager.snapshot(context).unwrap();
            if s.phase == phase
                && (!network
                    || s.runtime_phase.as_deref() == Some("Running")
                        && s.maintenance_health.is_some())
            {
                return;
            }
            assert_ne!(s.phase, Phase::Failed, "native lifecycle failure: {s:?}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "expected {phase:?}; last native observation: {:?}",
            manager.snapshot(context)
        )
    });
}
async fn identity(endpoint: &str, ca: &[u8], jwt: &str) -> (StoreScope, String) {
    let account = TrustedServer::new(HTTP, endpoint, Some(ca))
        .unwrap()
        .discover(HTTP, Zeroizing::new(jwt.into()))
        .await
        .unwrap();
    let generation = account
        .state()
        .active_device
        .as_ref()
        .unwrap()
        .reference
        .as_ref()
        .unwrap()
        .generation_id
        .clone();
    (account.scope(), generation)
}
fn saved(root: &Path, scope: StoreScope, generation: &str) {
    let mut local = DeviceProvisioning::new(ProtectedStore::open(root, scope).unwrap());
    assert_eq!(local.ready_device().unwrap().generation_id, generation);
}
async fn run(fields: &[Zeroizing<String>]) {
    let endpoint = fields[0].as_str();
    let ca = std::fs::read(fields[1].as_str()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("e2ee-servers.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!([{"httpOrigin":HTTP,"grpcOrigin":endpoint,"extraCaPem":String::from_utf8(ca.clone()).unwrap()}])).unwrap()).unwrap();
    let factory = NativeFactory::load(config.clone(), root.path().into());
    let (manager, task) = SessionManager::new(Arc::new(factory));
    let task = tokio::spawn(task);
    let invalid = manager
        .bind(
            manager.context(),
            HTTP.into(),
            Some("invalid.invalid.invalid".into()),
        )
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let s = manager.snapshot(invalid).unwrap();
            if s.phase == Phase::Failed {
                assert_eq!(s.error, Some(Reason::Authentication));
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !root.path().join("e2ee-v1").exists(),
        "unauthenticated JWT selected a store"
    );
    let alice_parent = manager.context();
    let a = manager
        .bind(alice_parent, HTTP.into(), Some(fields[2].to_string()))
        .unwrap();
    observed(&manager, a, Phase::SetupRequired, false).await;
    assert!(
        !root.path().join("e2ee-v1").exists(),
        "ordinary login created keys or database"
    );
    let setup = manager.setup(a).unwrap();
    // Go drops the FIRST Register response after COMMIT. The same committed
    // registration is resumed by retry; ordinary login itself still cannot create.
    observed(&manager, setup, Phase::Failed, false).await;
    assert_eq!(manager.snapshot(setup).unwrap().error, Some(Reason::Remote));
    let a = manager.retry(setup).unwrap();
    observed(&manager, a, Phase::Active, true).await;
    let (alice, alice_generation) = identity(endpoint, &ca, &fields[2]).await;
    assert!(matches!(manager.setup(a), Err(Reason::Invalid)));

    let other = tempfile::tempdir().unwrap();
    let (foreign, foreign_task) =
        SessionManager::new(Arc::new(NativeFactory::load(config, other.path().into())));
    let foreign_task = tokio::spawn(foreign_task);
    let f = foreign
        .bind(foreign.context(), HTTP.into(), Some(fields[2].to_string()))
        .unwrap();
    observed(&foreign, f, Phase::ReplacementRequired, false).await;
    assert!(matches!(foreign.setup(f), Err(Reason::Invalid)));
    assert!(
        !other.path().join("e2ee-v1").exists(),
        "setup adopted or replaced a foreign installation"
    );
    foreign.shutdown().await;
    foreign_task.await.unwrap();

    let b = manager
        .bind(a, HTTP.into(), Some(fields[3].to_string()))
        .unwrap();
    assert!(matches!(manager.snapshot(a), Err(Reason::Stale)));
    observed(&manager, b, Phase::SetupRequired, false).await;
    saved(root.path(), alice.clone(), &alice_generation);
    let b = manager.setup(b).unwrap();
    observed(&manager, b, Phase::Active, true).await;
    let (bob, bob_generation) = identity(endpoint, &ca, &fields[3]).await;
    assert_ne!(
        alice.database_path(root.path()),
        bob.database_path(root.path())
    );
    assert!(matches!(manager.retire(alice_parent), Err(Reason::Stale)));
    let alice_parent = b;
    let a = manager
        .bind(b, HTTP.into(), Some(fields[2].to_string()))
        .unwrap();
    observed(&manager, a, Phase::Active, true).await;
    saved(root.path(), bob.clone(), &bob_generation);
    let idle = manager.retire(alice_parent).unwrap();
    manager.wait_released(idle).await.unwrap();
    observed(&manager, idle, Phase::Idle, false).await;
    saved(root.path(), alice.clone(), &alice_generation);
    saved(root.path(), bob.clone(), &bob_generation);
    let a = manager
        .bind(idle, HTTP.into(), Some(fields[2].to_string()))
        .unwrap();
    // Go rejects inventory #4. A safe explicit retry opens the same generation;
    // the old page lease remains usable without accepting stale operation replies.
    observed(&manager, a, Phase::Failed, false).await;
    assert_eq!(
        manager.snapshot(a).unwrap().error,
        Some(Reason::Authentication)
    );
    saved(root.path(), alice.clone(), &alice_generation);
    let retried = manager.retry(a).unwrap();
    observed(&manager, retried, Phase::Active, true).await;
    let b = manager
        .bind(retried, HTTP.into(), Some(fields[3].to_string()))
        .unwrap();
    observed(&manager, b, Phase::Active, true).await;
    assert!(matches!(manager.retire(idle), Err(Reason::Stale)));
    manager.shutdown().await;
    task.await.unwrap();
    saved(root.path(), alice, &alice_generation);
    saved(root.path(), bob, &bob_generation);
    assert_eq!(
        manager.snapshot(manager.context()).unwrap().phase,
        Phase::Closed
    );
}

#[path = "replacement_tests.rs"]
mod replacement_tests;

#[path = "dialog_tests.rs"]
mod dialog_tests;
#[path = "recovery_integration_tests.rs"]
mod recovery_integration_tests;
#[path = "text_tests.rs"]
mod text_tests;
