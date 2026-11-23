//! Real Go/TLS/PG + Windows DPAPI, including a real v2 protected history record.
use super::*;
use rust_processor::{
    encryption::{
        protected::{RecordKey, RecordKind},
        provisioning::Reconciliation,
        signal_state::CryptoCoordinator,
    },
    proto::{e2ee as common, e2ee_v2 as v2},
};

#[test]
#[ignore = "requires the Go replacement PostgreSQL/TLS harness and Windows DPAPI"]
fn go_desktop_replacement_worker() {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(24576)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input
        .lines()
        .map(|s| Zeroizing::new(s.to_owned()))
        .collect();
    assert_eq!(fields.len(), 6);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(replacement(&fields));
    println!("zchat-desktop-replacement-complete:v1");
}
async fn stopped(m: &SessionManager) {
    let idle = m.clear();
    observed(m, idle, Phase::Idle, false).await;
}
async fn failed(m: &SessionManager, context: u64, reason: Reason) {
    eprintln!("replacement fixture expects failure: {reason:?}");
    observed(m, context, Phase::Failed, false).await;
    assert_eq!(m.snapshot(context).unwrap().error, Some(reason));
}
fn history(root: &Path, scope: StoreScope, key: &RecordKey) -> Zeroizing<Vec<u8>> {
    ProtectedStore::open(root, scope)
        .unwrap()
        .transaction(|tx| tx.get(key))
        .unwrap()
        .unwrap()
}
async fn seed_history(
    root: &Path,
    endpoint: &str,
    ca: &[u8],
    fields: &[Zeroizing<String>],
) -> (StoreScope, String, RecordKey, String) {
    let mut aa = TrustedServer::new(HTTP, endpoint, Some(ca))
        .unwrap()
        .discover(HTTP, Zeroizing::new(fields[2].to_string()))
        .await
        .unwrap();
    let mut bb = TrustedServer::new(HTTP, endpoint, Some(ca))
        .unwrap()
        .discover(HTTP, Zeroizing::new(fields[3].to_string()))
        .await
        .unwrap();
    let alice = aa.scope();
    let mut la = DeviceProvisioning::new(ProtectedStore::open(root, alice.clone()).unwrap());
    let mut lb = DeviceProvisioning::new(ProtectedStore::create(root, bb.scope()).unwrap());
    assert_eq!(
        aa.resume_initial(&mut la).await.unwrap(),
        Reconciliation::Ready
    );
    assert_eq!(
        bb.resume_initial(&mut lb).await.unwrap(),
        Reconciliation::Ready
    );
    let own = la.ready_device().unwrap();
    let peer = lb.ready_device().unwrap();
    let wa = CryptoCoordinator::start(la.into_store(), own.clone())
        .await
        .unwrap();
    let wb = CryptoCoordinator::start(lb.into_store(), peer)
        .await
        .unwrap();
    let chat = aa
        .device_client()
        .unwrap()
        .create_conversation(&wa, fields[5].parse().unwrap())
        .await
        .unwrap()
        .conversation_id;
    bb.device_client()
        .unwrap()
        .get_conversation(&wb, &chat)
        .await
        .unwrap();
    let mut a = aa.device_client().unwrap().epoch_transport().unwrap();
    let mut b = bb.device_client().unwrap().epoch_transport().unwrap();
    let q = wa.prepare_initial_epoch(chat.clone()).await.unwrap();
    let scope = wa
        .accept_initial_epoch(a.initial_epoch_catalog(&q).await.unwrap())
        .await
        .unwrap();
    let q = wb.prepare_initial_epoch(chat.clone()).await.unwrap();
    wb.accept_initial_epoch(b.initial_epoch_catalog(&q).await.unwrap())
        .await
        .unwrap();
    let p = wa.prepare_epoch_acquisition(scope.clone()).await.unwrap();
    let rust_processor::encryption::native::EpochAcquisitionOutcome::Bundle(bundle) =
        a.acquire_prepared(&p).await.unwrap()
    else {
        panic!("unexpected expired acquisition")
    };
    wa.bootstrap_epoch(*bundle).await.unwrap();
    let id = "183134b1-040d-42db-a1ec-8403a076746a";
    let key = RecordKey::new(
        &own.generation_id,
        RecordKind::History,
        &format!("v2:{}:{:016x}:{id}", chat, scope.crypto_epoch()),
    )
    .unwrap();
    wa.encrypt_epoch(
        scope.clone(),
        v2::EncryptedPayload {
            format_version: 2,
            message_id: id.into(),
            conversation_id: chat.clone(),
            sender: Some(scope.own_device().clone()),
            recipient: Some(scope.peer_device().clone()),
            created_at_unix_ms: 1_700_000_000_002,
            content: Some(v2::encrypted_payload::Content::Text(common::TextMessage {
                text: format!("archive retention proof{}", "\u{1}".repeat(1500)),
            })),
            crypto_epoch: Some(scope.crypto_epoch()),
        },
    )
    .await
    .unwrap();
    while !wa.seed_history_index().await.unwrap().ready {}
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
    (alice, own.generation_id, key, chat)
}
async fn replacement(f: &[Zeroizing<String>]) {
    let endpoint = f[0].as_str();
    let ca = std::fs::read(f[1].as_str()).unwrap();
    let root = tempfile::tempdir().unwrap();
    let other = tempfile::tempdir().unwrap();
    let third = tempfile::tempdir().unwrap();
    let config = root.path().join("servers.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!([{"httpOrigin":HTTP,"grpcOrigin":endpoint,"extraCaPem":String::from_utf8(ca.clone()).unwrap()}])).unwrap()).unwrap();
    let new = |path: &Path| {
        SessionManager::new(Arc::new(NativeFactory::load(config.clone(), path.into())))
    };
    let (a, at) = new(root.path());
    let at = tokio::spawn(at);
    let login = a
        .bind(a.context(), HTTP.into(), Some(f[2].to_string()))
        .unwrap();
    observed(&a, login, Phase::SetupRequired, false).await;
    let setup = a.setup(login).unwrap();
    observed(&a, setup, Phase::Active, true).await;
    stopped(&a).await;
    eprintln!("replacement fixture seed real v2 history");
    let (scope, old, key, chat) = seed_history(root.path(), endpoint, &ca, f).await;
    let before = history(root.path(), scope.clone(), &key);
    assert!(
        before
            .windows(b"archive retention proof".len())
            .any(|w| w == b"archive retention proof")
    );
    eprintln!("replacement fixture history committed; foreign installation B");
    let (b, bt) = new(other.path());
    let bt = tokio::spawn(bt);
    let x = b
        .bind(b.context(), HTTP.into(), Some(f[2].to_string()))
        .unwrap();
    observed(&b, x, Phase::ReplacementRequired, false).await;
    assert_eq!(
        b.snapshot(x)
            .unwrap()
            .replacement
            .unwrap()
            .expected_generation,
        old
    );
    assert!(!scope.database_path(other.path()).exists());
    let x = b
        .replace(x, Zeroizing::new("wrong-password".into()), true)
        .unwrap();
    failed(&b, x, Reason::PasswordInvalid).await;
    assert_eq!(identity(endpoint, &ca, &f[2]).await.1, old);
    let x = b.retry(x).unwrap();
    observed(&b, x, Phase::ReplacementPending, false).await;
    assert!(b.snapshot(x).unwrap().replacement.unwrap().continuing);
    let x = b
        .replace(x, Zeroizing::new(f[4].to_string()), true)
        .unwrap();
    failed(&b, x, Reason::Remote).await;
    eprintln!("replacement fixture lost Replace committed");
    let (_, second) = identity(endpoint, &ca, &f[2]).await;
    assert_ne!(old, second);
    // Read-only retry detects our already committed candidate, but never replaces it.
    let x = b.retry(x).unwrap();
    observed(&b, x, Phase::ReplacementPending, false).await;
    let view = b.snapshot(x).unwrap().replacement.unwrap();
    assert!(view.continuing);
    assert_eq!(view.expected_generation, old);
    let x = b
        .replace(x, Zeroizing::new(f[4].to_string()), true)
        .unwrap();
    observed(&b, x, Phase::Active, true).await;
    assert_eq!(identity(endpoint, &ca, &f[2]).await.1, second);
    stopped(&b).await;
    // A displays B, but another installation C wins before A confirms.
    let x = a
        .bind(a.context(), HTTP.into(), Some(f[2].to_string()))
        .unwrap();
    observed(&a, x, Phase::DeviceUnavailable, false).await;
    exercise_archive(&a, x, &old, &chat).await;
    assert_eq!(
        a.snapshot(x)
            .unwrap()
            .replacement
            .unwrap()
            .expected_generation,
        second
    );
    let x = a
        .replace(x, Zeroizing::new("wrong-password".into()), true)
        .unwrap();
    failed(&a, x, Reason::PasswordInvalid).await;
    let x = a.retry(x).unwrap();
    observed(&a, x, Phase::ReplacementPending, false).await;
    assert!(a.snapshot(x).unwrap().replacement.unwrap().continuing);
    eprintln!("replacement fixture concurrent installation C");
    let (c, ct) = new(third.path());
    let ct = tokio::spawn(ct);
    let z = c
        .bind(c.context(), HTTP.into(), Some(f[2].to_string()))
        .unwrap();
    observed(&c, z, Phase::ReplacementRequired, false).await;
    let z = c
        .replace(z, Zeroizing::new(f[4].to_string()), true)
        .unwrap();
    observed(&c, z, Phase::Active, true).await;
    stopped(&c).await;
    let (_, third_gen) = identity(endpoint, &ca, &f[2]).await;
    assert_ne!(second, third_gen);
    let x = a
        .replace(x, Zeroizing::new(f[4].to_string()), true)
        .unwrap();
    failed(&a, x, Reason::GenerationChanged).await;
    assert_eq!(identity(endpoint, &ca, &f[2]).await.1, third_gen);
    assert_eq!(
        history(root.path(), scope.clone(), &key).as_slice(),
        before.as_slice()
    );
    let x = a.retry(x).unwrap();
    observed(&a, x, Phase::ReplacementPending, false).await;
    let view = a.snapshot(x).unwrap().replacement.unwrap();
    assert_eq!(view.expected_generation, third_gen);
    assert!(!view.continuing && view.retargeting);
    let x = a
        .replace(x, Zeroizing::new(f[4].to_string()), true)
        .unwrap();
    observed(&a, x, Phase::Active, true).await;
    eprintln!("replacement fixture final explicit replacement complete");
    let (_, final_gen) = identity(endpoint, &ca, &f[2]).await;
    assert_ne!(final_gen, third_gen);
    stopped(&a).await;
    assert_eq!(
        history(root.path(), scope.clone(), &key).as_slice(),
        before.as_slice()
    );
    let mut store = ProtectedStore::open(root.path(), scope.clone()).unwrap();
    let archived = RecordKey::new(&old, RecordKind::Control, "archived-device").unwrap();
    assert!(store.transaction(|tx| tx.get(&archived)).unwrap().is_some());
    drop(store);
    saved(root.path(), scope.clone(), &final_gen);
    saved(other.path(), scope.clone(), &second);
    saved(third.path(), scope, &third_gen);
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
    at.await.unwrap();
    bt.await.unwrap();
    ct.await.unwrap();
}

async fn archive_reply(
    state: &super::super::DesktopE2ee,
    context: u64,
    query: super::super::pages::PageRequest,
) -> Result<(String, serde_json::Value), Reason> {
    use tauri::ipc::{InvokeResponseBody, IpcResponse};
    let request = uuid::Uuid::new_v4().to_string();
    let history = matches!(
        &query,
        super::super::pages::PageRequest::ArchiveHistory { .. }
    );
    let response =
        super::super::pages::execute(state, context, request.clone(), query, || Ok(())).await?;
    let InvokeResponseBody::Json(raw) = response.body().unwrap() else {
        panic!("expected JSON")
    };
    assert!(raw.len() <= if history { 16384 } else { 4096 });
    if history {
        assert!(raw.len() > 4096);
    }
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(value["context"], context);
    Ok((request, value))
}
async fn exercise_archive(m: &SessionManager, context: u64, generation: &str, chat: &str) {
    use super::super::{
        DesktopE2ee,
        pages::{self, PageRequest},
    };
    let state = DesktopE2ee {
        manager: m.clone(),
        admission: Default::default(),
        cursors: Default::default(),
    };
    assert_eq!(
        m.snapshot(context).unwrap().runtime_phase.as_deref(),
        Some("ArchiveReadOnly")
    );
    assert!(matches!(m.dialog_client(context), Err(Reason::NotReady)));
    let query = || PageRequest::ArchiveHistory {
        generation_id: generation.into(),
        conversation_id: chat.into(),
        crypto_epoch: Some("0".into()),
        cursor: None,
    };
    for q in [
        PageRequest::Archives { cursor: None },
        PageRequest::ArchiveDialogs {
            generation_id: generation.into(),
            cursor: None,
        },
        PageRequest::Segments {
            generation_id: generation.into(),
            conversation_id: chat.into(),
            cursor: None,
        },
        query(),
    ] {
        let (request, reply) = archive_reply(&state, context, q).await.unwrap();
        let page = &reply["page"];
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        if page["kind"] == "archive_history" {
            assert_eq!(page["generationId"], generation);
            assert_eq!(page["conversationId"], chat);
            assert_eq!(page["cryptoEpoch"], "0");
            assert!(
                page["items"][0]["text"]
                    .as_str()
                    .unwrap()
                    .starts_with("archive retention proof")
            );
            let keys: std::collections::BTreeSet<_> = page["items"][0]
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect();
            assert_eq!(
                keys,
                std::collections::BTreeSet::from([
                    "messageId",
                    "conversationId",
                    "cryptoEpoch",
                    "createdAtUnixMs",
                    "senderUserId",
                    "text",
                    "outgoing",
                    "delivery"
                ])
            );
        }
        state
            .admission
            .acknowledge(context, &request, reply["replyLease"].as_str().unwrap())
            .unwrap();
    }
    assert!(matches!(
        pages::execute(
            &state,
            context,
            uuid::Uuid::new_v4().to_string(),
            PageRequest::History {
                conversation_id: chat.into(),
                cursor: None
            },
            || Ok(())
        )
        .await,
        Err(Reason::NotReady)
    ));
    let checks = std::sync::atomic::AtomicUsize::new(0);
    assert!(matches!(
        pages::execute(
            &state,
            context,
            uuid::Uuid::new_v4().to_string(),
            query(),
            || {
                if checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err(Reason::Stale)
                }
            }
        )
        .await,
        Err(Reason::Stale)
    ));
    let mut leases = Vec::new();
    for _ in 0..8 {
        leases.push(archive_reply(&state, context, query()).await.unwrap());
    }
    assert!(matches!(
        archive_reply(&state, context, query()).await,
        Err(Reason::Busy)
    ));
    for (request, reply) in leases {
        state
            .admission
            .acknowledge(context, &request, reply["replyLease"].as_str().unwrap())
            .unwrap();
    }
    let stale = context.saturating_sub(1);
    assert!(matches!(
        archive_reply(&state, stale, query()).await,
        Err(Reason::Stale)
    ));
}
