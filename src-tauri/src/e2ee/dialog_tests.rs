//! Actual native command boundary + owned registry RPCs. No WebView acceptance.
use super::super::{
    DesktopE2ee,
    dialogs::{self, DialogRequest},
};
use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use tauri::ipc::{InvokeResponseBody, IpcResponse};

#[test]
#[ignore = "requires Go desktop-conversation TLS/PostgreSQL harness and Windows DPAPI"]
fn go_desktop_dialog_worker() {
    dialog_worker(false);
}
#[test]
#[ignore = "requires Go desktop-successor TLS/PostgreSQL harness and Windows DPAPI"]
fn go_desktop_successor_worker() {
    dialog_worker(true);
}
fn dialog_worker(successor: bool) {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(32768)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input
        .lines()
        .map(|s| Zeroizing::new(s.to_owned()))
        .collect();
    assert_eq!(fields.len(), 8);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(run_dialogs(&fields, successor));
    println!("zchat-desktop-dialog-complete:v1");
}
fn id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    format!(
        "00000000-0000-4000-8000-{:012x}",
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}
fn body(response: tauri::ipc::Response) -> serde_json::Value {
    let InvokeResponseBody::Json(value) = response.body().unwrap() else {
        panic!("metadata response must be JSON")
    };
    assert!(value.len() <= 4096);
    let v: serde_json::Value = serde_json::from_str(&value).unwrap();
    let mut keys: Vec<_> = v.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(keys, vec!["context", "conversation", "replyLease"]);
    let mut keys: Vec<_> = v["conversation"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            "closeReason",
            "closedAtUnixMs",
            "conversationId",
            "peerUserId",
            "state"
        ]
    );
    v
}
pub(super) async fn command(
    state: &DesktopE2ee,
    context: u64,
    request: DialogRequest,
) -> Result<serde_json::Value, Reason> {
    // Exercise the same strict serde input and command implementation as Tauri.
    let json = match &request {
        DialogRequest::Create { peer_user_id } => {
            serde_json::json!({"action":"create","peerUserId":peer_user_id})
        }
        DialogRequest::Get { conversation_id } => {
            serde_json::json!({"action":"get","conversationId":conversation_id})
        }
        DialogRequest::CreateAfterClosed {
            peer_user_id,
            closed_conversation_id,
            confirmed,
        } => {
            serde_json::json!({"action":"create_after_closed", "peerUserId":peer_user_id, "closedConversationId":closed_conversation_id, "confirmed":confirmed})
        }
    };
    let request = serde_json::from_value(json).unwrap();
    let rid = id();
    let v = body(dialogs::execute(state, context, rid.clone(), request, || Ok(())).await?);
    assert_eq!(v["context"], context);
    state
        .admission
        .acknowledge(context, &rid, v["replyLease"].as_str().unwrap())
        .unwrap();
    Ok(v["conversation"].clone())
}
pub(super) fn create(peer: &str) -> DialogRequest {
    DialogRequest::Create {
        peer_user_id: peer.into(),
    }
}
pub(super) fn get(chat: &str) -> DialogRequest {
    DialogRequest::Get {
        conversation_id: chat.into(),
    }
}
pub(super) async fn stop(state: &DesktopE2ee) {
    let context = state.invalidate();
    observed(&state.manager, context, Phase::Idle, false).await;
}
pub(super) async fn setup(state: &DesktopE2ee, token: &str) -> u64 {
    let c = state
        .manager
        .bind(state.manager.context(), HTTP.into(), Some(token.into()))
        .unwrap();
    observed(&state.manager, c, Phase::SetupRequired, false).await;
    let c = state.manager.setup(c).unwrap();
    observed(&state.manager, c, Phase::Active, true).await;
    c
}
async fn run_dialogs(f: &[Zeroizing<String>], successor: bool) {
    let ca = std::fs::read(f[1].as_str()).unwrap();
    let aroot = tempfile::tempdir().unwrap();
    let broot = tempfile::tempdir().unwrap();
    let croot = tempfile::tempdir().unwrap();
    let new_broot = tempfile::tempdir().unwrap();
    let config = aroot.path().join("servers.json");
    std::fs::write(&config,serde_json::to_vec(&serde_json::json!([{"httpOrigin":HTTP,"grpcOrigin":f[0].as_str(),"extraCaPem":String::from_utf8(ca.clone()).unwrap()}])).unwrap()).unwrap();
    let make = |root: &Path| {
        let (manager, task) =
            SessionManager::new(Arc::new(NativeFactory::load(config.clone(), root.into())));
        (
            DesktopE2ee {
                cursors: Default::default(),
                manager,
                admission: Default::default(),
            },
            tokio::spawn(task),
        )
    };
    let (a, at) = make(aroot.path());
    let (b, bt) = make(broot.path());
    let (c, ct) = make(croot.path());
    assert_eq!(
        command(&a, a.manager.context(), create(&f[6]))
            .await
            .unwrap_err(),
        Reason::NotReady
    );
    let ac = setup(&a, &f[2]).await;
    // First Create commits only the local request; peer keys are not registered.
    assert_eq!(
        command(&a, ac, create(&f[6])).await.unwrap_err(),
        Reason::Remote
    );
    assert_eq!(a.manager.snapshot(ac).unwrap().phase, Phase::Active);
    let bc = setup(&b, &f[3]).await;
    // Go fixture disabled new DMs after the first rejection; retry cannot bypass.
    assert_eq!(
        command(&a, ac, create(&f[6])).await.unwrap_err(),
        Reason::Forbidden
    );
    assert_eq!(a.manager.snapshot(ac).unwrap().phase, Phase::Active);
    // Go now allows DMs, but drops the first successful Create reply AFTER COMMIT.
    assert_eq!(
        command(&a, ac, create(&f[6])).await.unwrap_err(),
        Reason::Remote
    );
    let av = command(&a, ac, create(&f[6])).await.unwrap();
    let chat = av["conversationId"].as_str().unwrap();
    assert_eq!(av["state"], "active");
    let bv = command(&b, bc, get(chat)).await.unwrap();
    assert_ne!(av["peerUserId"], bv["peerUserId"]);
    assert_eq!(
        command(&a, ac, get("11111111-1111-4111-8111-111111111111"))
            .await
            .unwrap_err(),
        Reason::NotFound
    );
    let cc = setup(&c, &f[4]).await;
    assert_eq!(
        command(&a, ac, create(&f[7])).await.unwrap_err(),
        Reason::Forbidden
    ); // nonfriend
    assert_eq!(
        command(&c, cc, get(chat)).await.unwrap_err(),
        Reason::NotFound
    ); // foreign account
    assert_eq!(c.manager.snapshot(cc).unwrap().phase, Phase::Active);
    // Page check after a real committed Get rejects the response without a lease.
    let checks = AtomicUsize::new(0);
    assert!(matches!(
        dialogs::execute(&a, ac, id(), get(chat), || {
            if checks.fetch_add(1, Ordering::SeqCst) == 0 {
                Ok(())
            } else {
                Err(Reason::Stale)
            }
        })
        .await,
        Err(Reason::Stale)
    ));
    let mut held = Vec::new();
    for _ in 0..8 {
        let rid = id();
        let value = body(
            dialogs::execute(&a, ac, rid.clone(), get(chat), || Ok(()))
                .await
                .unwrap(),
        );
        held.push((rid, value));
    }
    assert!(matches!(
        dialogs::execute(&a, ac, id(), get(chat), || Ok(())).await,
        Err(Reason::Busy)
    ));
    for (rid, v) in held {
        a.admission
            .acknowledge(ac, &rid, v["replyLease"].as_str().unwrap())
            .unwrap();
    }
    // Release/reopen the same scope. Create must reuse the saved peer-slot UUID.
    stop(&a).await;
    assert_eq!(command(&a, ac, get(chat)).await.unwrap_err(), Reason::Stale);
    let ac = a
        .manager
        .bind(a.manager.context(), HTTP.into(), Some(f[2].to_string()))
        .unwrap();
    observed(&a.manager, ac, Phase::Active, true).await;
    assert_eq!(command(&a, ac, create(&f[6])).await.unwrap(), av);
    stop(&b).await;
    let (new_b, nbt) = make(new_broot.path());
    let nb = new_b
        .manager
        .bind(new_b.manager.context(), HTTP.into(), Some(f[3].to_string()))
        .unwrap();
    observed(&new_b.manager, nb, Phase::ReplacementRequired, false).await;
    let nb = new_b
        .manager
        .replace(nb, Zeroizing::new(f[5].to_string()), true)
        .unwrap();
    observed(&new_b.manager, nb, Phase::Active, true).await;
    let closed = command(&a, ac, get(chat)).await.unwrap();
    assert_eq!(closed["state"], "closed");
    assert_eq!(closed["closeReason"], "device_replaced");
    assert_ne!(closed["closedAtUnixMs"], "0");
    assert_eq!(command(&a, ac, create(&f[6])).await.unwrap(), closed); // no implicit new dialogue
    if successor {
        let next = |peer: &str, chat: &str| DialogRequest::CreateAfterClosed {
            peer_user_id: peer.into(),
            closed_conversation_id: chat.into(),
            confirmed: true,
        };
        assert_eq!(
            command(
                &a,
                ac,
                DialogRequest::CreateAfterClosed {
                    peer_user_id: f[6].to_string(),
                    closed_conversation_id: chat.into(),
                    confirmed: false,
                }
            )
            .await
            .unwrap_err(),
            Reason::Invalid
        );
        for request in [
            next(&f[6], "11111111-1111-4111-8111-111111111111"),
            next(&f[7], chat),
        ] {
            assert_eq!(
                command(&a, ac, request).await.unwrap_err(),
                Reason::Conflict
            );
        }
        assert_eq!(
            command(&c, cc, next(&f[6], chat)).await.unwrap_err(),
            Reason::Conflict
        );
        // The fixture changes CURRENT privacy/friendship/publication before
        // each request. None of these failures may invent a different UUID.
        for expected in [
            Reason::Forbidden,
            Reason::Forbidden,
            Reason::Remote,
            Reason::Remote,
        ] {
            assert_eq!(
                command(&a, ac, next(&f[6], chat)).await.unwrap_err(),
                expected
            );
            assert_eq!(a.manager.snapshot(ac).unwrap().phase, Phase::Active);
        }
        // The last failure was AFTER server COMMIT. Drop/reopen the real owner
        // and DPAPI store before retrying the SAME selected closed chat.
        stop(&a).await;
        assert_eq!(
            command(&a, ac, next(&f[6], chat)).await.unwrap_err(),
            Reason::Stale
        );
        let ac = a
            .manager
            .bind(a.manager.context(), HTTP.into(), Some(f[2].to_string()))
            .unwrap();
        observed(&a.manager, ac, Phase::Active, true).await;
        let active = command(&a, ac, next(&f[6], chat)).await.unwrap();
        assert_eq!(active["state"], "active");
        assert_ne!(active["conversationId"], chat);
        let fresh = active["conversationId"].as_str().unwrap();
        assert_eq!(command(&a, ac, get(chat)).await.unwrap(), closed);
        assert_eq!(
            command(&new_b, nb, get(chat)).await.unwrap_err(),
            Reason::NotFound
        );
        assert_eq!(
            command(&new_b, nb, get(fresh)).await.unwrap()["state"],
            "active"
        );
        let (left, right) = tokio::join!(
            command(&a, ac, next(&f[6], chat)),
            command(&a, ac, next(&f[6], chat))
        );
        assert_eq!(left.unwrap(), active);
        assert_eq!(right.unwrap(), active);
        let checks = AtomicUsize::new(0);
        assert!(matches!(
            dialogs::execute(&a, ac, id(), next(&f[6], chat), || {
                if checks.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(())
                } else {
                    Err(Reason::Stale)
                }
            })
            .await,
            Err(Reason::Stale)
        ));
        assert_eq!(command(&a, ac, create(&f[6])).await.unwrap(), active);
        assert!(
            a.manager
                .dialog_client(ac)
                .unwrap()
                .0
                .conversation(fresh)
                .await
                .unwrap()
                .is_none()
        );
    }
    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
    new_b.shutdown().await;
    at.await.unwrap();
    bt.await.unwrap();
    ct.await.unwrap();
    nbt.await.unwrap();
}
