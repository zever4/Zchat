//! Shared actual IPC implementation, actual Windows DPAPI, Go TLS/PG fixture.
use super::super::{
    DesktopE2ee,
    text::{self, TextRequest},
};
use super::dialog_tests::{command, create, get, setup, stop};
use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tauri::ipc::{InvokeResponseBody, IpcResponse};

const FIRST: &str = "00000000-0000-4000-8000-000000000001";
const SECOND: &str = "00000000-0000-4000-8000-000000000002";
const TEXT: &str = "desktop-text-sensitive-v1";
#[path = "page_tests.rs"]
mod page_tests;
#[test]
#[ignore = "requires Go desktop-text TLS/PostgreSQL harness and Windows DPAPI"]
fn go_desktop_text_worker() {
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
        .block_on(run_texts(&fields));
    println!("zchat-desktop-text-complete:v1");
}
fn id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(100);
    format!(
        "00000000-0000-4000-8000-{:012x}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn input(chat: &str, message: &str, value: &str) -> TextRequest {
    serde_json::from_value(
        serde_json::json!({"conversationId":chat,"messageId":message,"text":value}),
    )
    .unwrap()
}
fn body(response: tauri::ipc::Response) -> serde_json::Value {
    let InvokeResponseBody::Json(raw) = response.body().unwrap() else {
        panic!("text acceptance must be JSON")
    };
    assert!(raw.len() <= 4096 && !raw.contains(TEXT));
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let mut keys: Vec<_> = v.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(keys, ["context", "queued", "replyLease"]);
    let mut keys: Vec<_> = v["queued"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        [
            "conversationId",
            "createdAtUnixMs",
            "cryptoEpoch",
            "messageId",
            "sealedUnsent",
            "storedOnServer"
        ]
    );
    v
}
async fn send(
    state: &DesktopE2ee,
    context: u64,
    chat: &str,
    message: &str,
    value: &str,
) -> Result<serde_json::Value, Reason> {
    let rid = id();
    let v = body(
        text::execute(
            state,
            context,
            rid.clone(),
            input(chat, message, value),
            || Ok(()),
        )
        .await?,
    );
    state
        .admission
        .acknowledge(context, &rid, v["replyLease"].as_str().unwrap())
        .unwrap();
    Ok(v["queued"].clone())
}
async fn received(
    client: &rust_processor::encryption::account_owner::AccountClient,
    message: &str,
    expected: &str,
) {
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            if let Ok(Some(v)) = client.received_text(message).await {
                assert_eq!(v.text, expected);
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("native protected receive did not finish");
}
async fn run_texts(f: &[Zeroizing<String>]) {
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
                cursors: Default::default(),
                manager,
                admission: Default::default(),
            },
            tokio::spawn(task),
        )
    };
    // Two different accounts on ONE PC/app data root, separate encrypted stores.
    let (a, at) = make();
    let (b, bt) = make();
    let ac = setup(&a, &f[2]).await;
    let bc = setup(&b, &f[3]).await;
    let chat = command(&a, ac, create(&f[4])).await.unwrap()["conversationId"]
        .as_str()
        .unwrap()
        .to_owned();
    command(&b, bc, get(&chat)).await.unwrap();
    let ca = a.manager.dialog_client(ac).unwrap().0;
    let cb = b.manager.dialog_client(bc).unwrap().0;
    assert!(ca.conversation(&chat).await.unwrap().is_none());
    assert!(cb.conversation(&chat).await.unwrap().is_none());
    page_tests::empty(&a, ac, &chat).await;
    // The fixture loses the first acquired bundle AFTER its DB commit.
    assert_eq!(
        send(&a, ac, &chat, FIRST, TEXT).await.unwrap_err(),
        Reason::Remote
    );
    assert!(matches!(
        ca.outgoing(FIRST).await,
        Err(
            rust_processor::encryption::account_owner::AccountError::Crypto(
                rust_processor::encryption::signal_state::CryptoError::MessageUnavailable
            )
        )
    ));
    // The retry bootstraps and commits locally, but navigation discards its reply.
    let checks = AtomicUsize::new(0);
    assert!(matches!(
        text::execute(&a, ac, id(), input(&chat, FIRST, TEXT), || {
            if checks.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(Reason::Stale)
            }
        })
        .await,
        Err(Reason::Stale)
    ));
    let first = send(&a, ac, &chat, FIRST, TEXT).await.unwrap();
    assert_eq!(first["messageId"], FIRST);
    assert_eq!(first["cryptoEpoch"], "0");
    assert_eq!(first["sealedUnsent"], false);
    assert_eq!(
        send(&a, ac, &chat, FIRST, "changed").await.unwrap_err(),
        Reason::Conflict
    );
    received(&cb, FIRST, TEXT).await;
    assert!(ca.received_text(FIRST).await.is_err());
    let reply = "🦀".repeat(500); // Exactly2000UTF8 bytes, no plaintext in IPC reply.
    let second = send(&b, bc, &chat, SECOND, &reply).await.unwrap();
    assert_eq!(second["messageId"], SECOND);
    received(&ca, SECOND, &reply).await;
    assert!(cb.received_text(SECOND).await.is_err());
    let page_cursor = page_tests::exercise(&a, ac, &b, bc, &chat).await;
    let mut held = Vec::new();
    for _ in 0..8 {
        let rid = id();
        let v = body(
            text::execute(&a, ac, rid.clone(), input(&chat, FIRST, TEXT), || Ok(()))
                .await
                .unwrap(),
        );
        held.push((rid, v));
    }
    assert!(matches!(
        text::execute(&a, ac, id(), input(&chat, FIRST, TEXT), || Ok(())).await,
        Err(Reason::Busy)
    ));
    for (rid, v) in held {
        a.admission
            .acknowledge(ac, &rid, v["replyLease"].as_str().unwrap())
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let first = ca.outgoing(FIRST).await.unwrap();
            let second = cb.outgoing(SECOND).await.unwrap();
            let stored = Some(rust_processor::proto::e2ee::MessageState::RecipientStored);
            if first.stored_on_server
                && second.stored_on_server
                && first.status == stored
                && second.status == stored
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // User receipt/status is not proof that a service confirmation's ACK has
    // finished. Complete another healthy round before shutting down; the Go
    // fixture separately asserts the exact persisted receipt/ACK/inbox facts.
    let ar = ca.status().delivery_rounds;
    let br = cb.status().delivery_rounds;
    tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let a = ca.status();
            let b = cb.status();
            let healthy = Some(rust_processor::encryption::account_owner::DeliveryHealth::Healthy);
            if a.delivery_rounds > ar
                && b.delivery_rounds > br
                && a.delivery_health == healthy
                && b.delivery_health == healthy
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop(&a).await;
    assert_eq!(
        send(&a, ac, &chat, FIRST, TEXT).await.unwrap_err(),
        Reason::Stale
    );
    let reopened = a
        .manager
        .bind(a.manager.context(), HTTP.into(), Some(f[2].to_string()))
        .unwrap();
    observed(&a.manager, reopened, Phase::Active, true).await;
    page_tests::reopened(&a, reopened, &chat, page_cursor).await;
    let retry = send(&a, reopened, &chat, FIRST, TEXT).await.unwrap();
    assert_eq!(retry["createdAtUnixMs"], first["createdAtUnixMs"]);
    assert_eq!(retry["cryptoEpoch"], first["cryptoEpoch"]);
    assert_eq!(retry["storedOnServer"], true);
    a.shutdown().await;
    b.shutdown().await;
    at.await.unwrap();
    bt.await.unwrap();
}
