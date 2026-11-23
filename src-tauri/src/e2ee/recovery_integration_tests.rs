//! Actual recovery IPC, protected Windows DPAPI stores and Go/TLS/PostgreSQL.
use super::super::{
    DesktopE2ee,
    recovery::{self, RecoveryRequest},
    text::{self, TextRequest},
};
use super::dialog_tests::{command as dialog, create, get, setup, stop};
use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tauri::ipc::{InvokeResponseBody, IpcResponse};
const TEXT: &str = "desktop-recovery-sensitive-v1";
const FIRST: &str = "00000000-0000-4000-8000-000000000001";
const SECOND: &str = "00000000-0000-4000-8000-000000000002";
fn id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1000);
    format!(
        "00000000-0000-4000-8000-{:012x}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn body(response: tauri::ipc::Response) -> serde_json::Value {
    let InvokeResponseBody::Json(raw) = response.body().unwrap() else {
        panic!("expected JSON");
    };
    assert!(raw.len() <= 4096 && !raw.contains(TEXT));
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let keys: std::collections::BTreeSet<_> = value
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        std::collections::BTreeSet::from(["context", "recovery", "replyLease"])
    );
    value
}
fn read(chat: &str) -> serde_json::Value {
    serde_json::json!({"action":"get","conversationId":chat})
}
fn propose(chat: &str, own: Option<&str>) -> serde_json::Value {
    serde_json::json!({"action":"propose","conversationId":chat,"expectedEpoch":"0","expectedOwnProposalId":own,"confirmed":true})
}
fn decide(chat: &str, pid: &str, decision: &str) -> serde_json::Value {
    serde_json::json!({"action":"decide","conversationId":chat,"expectedEpoch":"0","proposalId":pid,"decision":decision,"confirmed":true})
}
async fn command(
    state: &DesktopE2ee,
    context: u64,
    operation: serde_json::Value,
) -> Result<serde_json::Value, Reason> {
    let rid = id();
    let operation = serde_json::from_value::<RecoveryRequest>(operation).unwrap();
    let value = body(recovery::execute(state, context, rid.clone(), operation, || Ok(())).await?);
    state
        .admission
        .acknowledge(context, &rid, value["replyLease"].as_str().unwrap())
        .unwrap();
    Ok(value["recovery"].clone())
}
async fn wait_proposal(
    state: &DesktopE2ee,
    context: u64,
    chat: &str,
    pid: &str,
    expected: &str,
) -> serde_json::Value {
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let view = command(state, context, read(chat)).await.unwrap();
            if view["proposals"]
                .as_array()
                .unwrap()
                .iter()
                .any(|p| p["proposalId"] == pid && p["state"] == expected)
            {
                return view;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("recovery state not observed")
}
async fn send(state: &DesktopE2ee, context: u64, chat: &str, uuid: &str) -> String {
    let rid = id();
    let input: TextRequest = serde_json::from_value(
        serde_json::json!({"conversationId":chat,"messageId":uuid,"text":TEXT}),
    )
    .unwrap();
    let response = text::execute(state, context, rid.clone(), input, || Ok(()))
        .await
        .unwrap();
    let InvokeResponseBody::Json(raw) = response.body().unwrap() else {
        panic!("JSON");
    };
    assert!(!raw.contains(TEXT));
    let value: serde_json::Value = serde_json::from_str(&raw).unwrap();
    state
        .admission
        .acknowledge(context, &rid, value["replyLease"].as_str().unwrap())
        .unwrap();
    value["queued"]["cryptoEpoch"].as_str().unwrap().into()
}
async fn stored(state: &DesktopE2ee, context: u64, uuid: &str) {
    let client = state.manager.dialog_client(context).unwrap().0;
    tokio::time::timeout(Duration::from_secs(35), async {
        loop {
            let view = client.outgoing(uuid).await.unwrap();
            if view.status == Some(rust_processor::proto::e2ee::MessageState::RecipientStored) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("recipient did not store text");
}
#[test]
#[ignore = "requires Go desktop-recovery TLS/PostgreSQL harness and Windows DPAPI"]
fn go_desktop_recovery_worker() {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(32768)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input
        .lines()
        .map(|s| Zeroizing::new(s.to_owned()))
        .collect();
    assert_eq!(fields.len(), 5);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run(&fields));
    println!("zchat-desktop-recovery-complete:v1");
}
async fn run(f: &[Zeroizing<String>]) {
    let root = tempfile::tempdir().unwrap();
    let config = root.path().join("servers.json");
    let ca = std::fs::read(f[1].as_str()).unwrap();
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!([{"httpOrigin":HTTP,"grpcOrigin":f[0].as_str(),"extraCaPem":String::from_utf8(ca).unwrap()}])).unwrap()).unwrap();
    let make = || {
        let (manager, task) = SessionManager::new(Arc::new(NativeFactory::load(
            config.clone(),
            root.path().into(),
        )));
        (
            DesktopE2ee {
                manager,
                admission: Default::default(),
                cursors: Default::default(),
            },
            tokio::spawn(task),
        )
    };
    let (a, at) = make();
    let (b, bt) = make();
    let ac = setup(&a, &f[2]).await;
    let bc = setup(&b, &f[3]).await;
    let chat = dialog(&a, ac, create(&f[4])).await.unwrap()["conversationId"]
        .as_str()
        .unwrap()
        .to_owned();
    dialog(&b, bc, get(&chat)).await.unwrap();
    let empty = command(&a, ac, read(&chat)).await.unwrap();
    assert!(empty["cryptoEpoch"].is_null());
    assert_eq!(empty["proposals"], serde_json::json!([]));
    assert_eq!(
        command(&a, ac, propose(&chat, None)).await.unwrap_err(),
        Reason::NotFound
    );
    let mut unconfirmed = propose(&chat, None);
    unconfirmed["confirmed"] = false.into();
    assert_eq!(
        command(&a, ac, unconfirmed).await.unwrap_err(),
        Reason::Invalid
    );
    assert_eq!(send(&a, ac, &chat, FIRST).await, "0");
    stored(&a, ac, FIRST).await;
    let before = command(&a, ac, read(&chat)).await.unwrap();
    assert_eq!(before["cryptoEpoch"], "0");
    assert_eq!(before["sendingFrozen"], false);
    let mut wrong = propose(&chat, None);
    wrong["expectedEpoch"] = "1".into();
    assert_eq!(command(&a, ac, wrong).await.unwrap_err(), Reason::Conflict);
    // Lose the local result after COMMIT, as with navigation; exact old CAS must
    // conflict rather than create another proposal. Read-only status resolves it.
    let checks = AtomicUsize::new(0);
    let result = recovery::execute(
        &a,
        ac,
        id(),
        serde_json::from_value(propose(&chat, None)).unwrap(),
        || {
            if checks.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(Reason::Stale)
            }
        },
    )
    .await;
    assert!(matches!(result, Err(Reason::Stale)));
    assert_eq!(
        command(&a, ac, propose(&chat, None)).await.unwrap_err(),
        Reason::Conflict
    );
    let own = command(&a, ac, read(&chat)).await.unwrap();
    let first = own["proposals"][0]["proposalId"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        command(&a, ac, propose(&chat, Some(&first))).await.unwrap()["operationId"],
        first
    );
    let pending = wait_proposal(&b, bc, &chat, &first, "pending").await;
    assert_eq!(pending["sendingFrozen"], false);
    assert!(
        pending["proposals"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["decision"].is_null())
    );
    assert_eq!(
        command(&a, ac, read(&chat)).await.unwrap()["cryptoEpoch"],
        "0"
    );
    let mut wrong = decide(&chat, &first, "decline");
    wrong["expectedEpoch"] = "1".into();
    assert_eq!(command(&b, bc, wrong).await.unwrap_err(), Reason::Conflict);
    // Native decline, own cancel, then a separately confirmed accepted recovery.
    command(&b, bc, decide(&chat, &first, "decline"))
        .await
        .unwrap();
    wait_proposal(&a, ac, &chat, &first, "declined").await;
    assert_eq!(
        command(&a, ac, propose(&chat, None)).await.unwrap_err(),
        Reason::Conflict
    );
    let second = command(&a, ac, propose(&chat, Some(&first))).await.unwrap()["operationId"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_proposal(&b, bc, &chat, &second, "pending").await;
    command(&a, ac, decide(&chat, &second, "cancel"))
        .await
        .unwrap();
    wait_proposal(&a, ac, &chat, &second, "cancelled").await;
    let third = command(&a, ac, propose(&chat, Some(&second)))
        .await
        .unwrap()["operationId"]
        .as_str()
        .unwrap()
        .to_owned();
    wait_proposal(&b, bc, &chat, &third, "pending").await;
    command(&b, bc, decide(&chat, &third, "accept"))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(40), async {
        loop {
            let av = command(&a, ac, read(&chat)).await.unwrap();
            let bv = command(&b, bc, read(&chat)).await.unwrap();
            if av["cryptoEpoch"] == "1" && bv["cryptoEpoch"] == "1" {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("signed recovery did not activate epoch1");
    assert_eq!(
        command(&a, ac, propose(&chat, Some(&third)))
            .await
            .unwrap_err(),
        Reason::Conflict
    );
    assert_eq!(
        command(&b, bc, decide(&chat, &third, "accept"))
            .await
            .unwrap_err(),
        Reason::Conflict
    );
    assert_eq!(send(&a, ac, &chat, SECOND).await, "1");
    stored(&a, ac, SECOND).await;
    let mut held = Vec::new();
    for _ in 0..8 {
        let rid = id();
        let v = body(
            recovery::execute(
                &a,
                ac,
                rid.clone(),
                serde_json::from_value(read(&chat)).unwrap(),
                || Ok(()),
            )
            .await
            .unwrap(),
        );
        held.push((rid, v));
    }
    assert_eq!(
        command(&a, ac, read(&chat)).await.unwrap_err(),
        Reason::Busy
    );
    for (rid, v) in held {
        a.admission
            .acknowledge(ac, &rid, v["replyLease"].as_str().unwrap())
            .unwrap();
    }
    stop(&a).await;
    assert_eq!(
        command(&a, ac, read(&chat)).await.unwrap_err(),
        Reason::Stale
    );
    let reopened = a
        .manager
        .bind(a.manager.context(), HTTP.into(), Some(f[2].to_string()))
        .unwrap();
    observed(&a.manager, reopened, Phase::Active, true).await;
    assert_eq!(
        command(&a, reopened, read(&chat)).await.unwrap()["cryptoEpoch"],
        "1"
    );
    let client = a.manager.dialog_client(reopened).unwrap().0;
    let page = client
        .read_views(rust_processor::encryption::signal_state::ViewRequest::History(chat, None))
        .await
        .unwrap();
    let rust_processor::encryption::signal_state::ViewPage::History(rows, _) = page else {
        panic!("history");
    };
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r.text.as_str() == TEXT));
    assert!(rows.iter().any(|r| r.crypto_epoch == 0) && rows.iter().any(|r| r.crypto_epoch == 1));
    a.manager.shutdown().await;
    b.manager.shutdown().await;
    at.await.unwrap();
    bt.await.unwrap();
}
