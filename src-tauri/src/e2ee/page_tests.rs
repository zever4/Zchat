//! Actual shared read IPC boundary; invoked by the Go text acceptance worker.
use super::super::super::pages::{self, PageRequest};
use super::*;
async fn page(
    state: &DesktopE2ee,
    context: u64,
    query: PageRequest,
) -> Result<serde_json::Value, Reason> {
    let history = matches!(query, PageRequest::History { .. });
    let rid = id();
    let response = pages::execute(state, context, rid.clone(), query, || Ok(())).await?;
    let InvokeResponseBody::Json(raw) = response.body().unwrap() else {
        panic!("page must be JSON")
    };
    assert!(raw.len() <= if history { 16384 } else { 4096 });
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    let mut keys: Vec<_> = v.as_object().unwrap().keys().map(String::as_str).collect();
    keys.sort();
    assert_eq!(keys, ["context", "page", "replyLease"]);
    assert_eq!(v["context"], context);
    state
        .admission
        .acknowledge(context, &rid, v["replyLease"].as_str().unwrap())
        .unwrap();
    Ok(v["page"].clone())
}
fn history(chat: &str, cursor: Option<String>) -> PageRequest {
    PageRequest::History {
        conversation_id: chat.into(),
        cursor,
    }
}
pub(super) async fn empty(state: &DesktopE2ee, context: u64, chat: &str) {
    let d = page(state, context, PageRequest::Dialogs { cursor: None })
        .await
        .unwrap();
    assert_eq!(d["items"].as_array().unwrap().len(), 1);
    assert_eq!(d["items"][0]["conversationId"], chat);
    assert!(
        page(state, context, history(chat, None)).await.unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        state
            .manager
            .dialog_client(context)
            .unwrap()
            .0
            .conversation(chat)
            .await
            .unwrap()
            .is_none()
    );
}
pub(super) async fn exercise(
    a: &DesktopE2ee,
    ac: u64,
    b: &DesktopE2ee,
    bc: u64,
    chat: &str,
) -> String {
    let mut last = String::new();
    for i in 3..=12 {
        let message = format!("00000000-0000-4000-8000-{i:012x}");
        let text = if i >= 11 {
            "\u{0001}".repeat(2000)
        } else {
            format!("history-{i}")
        };
        send(a, ac, chat, &message, &text).await.unwrap();
        last = message;
    }
    let cb = b.manager.dialog_client(bc).unwrap().0;
    received(&cb, &last, &"\u{0001}".repeat(2000)).await;
    let first = page(a, ac, history(chat, None)).await.unwrap();
    let token = first["nextCursor"].as_str().unwrap().to_owned();
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["items"][0]["text"].as_str().unwrap().len(), 2000);
    let encoded = serde_json::to_vec(&first).unwrap();
    assert!(encoded.len() > 4096 && encoded.len() < 16384);
    assert_eq!(
        page(a, ac, history(chat, None)).await.unwrap()["nextCursor"],
        token
    ); // Same position, same opaque token.
    assert_eq!(
        page(b, bc, history(chat, Some(token.clone())))
            .await
            .unwrap_err(),
        Reason::Stale
    );
    assert_eq!(
        page(
            a,
            ac,
            PageRequest::Dialogs {
                cursor: Some(token.clone())
            }
        )
        .await
        .unwrap_err(),
        Reason::Invalid
    );
    let mut ids = std::collections::HashSet::new();
    let mut cursor = None;
    for _ in 0..16 {
        let v = page(a, ac, history(chat, cursor)).await.unwrap();
        for row in v["items"].as_array().unwrap() {
            let mut keys: Vec<_> = row
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
                    "delivery",
                    "messageId",
                    "outgoing",
                    "senderUserId",
                    "text"
                ]
            );
            assert!(ids.insert(row["messageId"].as_str().unwrap().to_owned()));
            assert_eq!(row["conversationId"], chat);
        }
        cursor = v["nextCursor"].as_str().map(str::to_owned);
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert_eq!(ids.len(), 12); // 12 user texts; confirmation hidden.
    let statuses = tokio::time::timeout(Duration::from_secs(25), async {
        loop {
            let v = page(
                a,
                ac,
                PageRequest::Statuses {
                    conversation_id: chat.into(),
                    message_ids: vec![FIRST.into()],
                },
            )
            .await
            .unwrap();
            if v["items"][0]["delivery"]["state"] == "recipient_stored" {
                break v;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        statuses["items"][0]["delivery"]["state"],
        "recipient_stored"
    );
    assert_eq!(
        page(
            b,
            bc,
            PageRequest::Statuses {
                conversation_id: chat.into(),
                message_ids: vec![FIRST.into()]
            }
        )
        .await
        .unwrap_err(),
        Reason::Conflict
    );
    let checks = AtomicUsize::new(0);
    assert!(matches!(
        pages::execute(a, ac, id(), history(chat, None), || {
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
        let response = pages::execute(a, ac, rid.clone(), history(chat, None), || Ok(()))
            .await
            .unwrap();
        let InvokeResponseBody::Json(raw) = response.body().unwrap() else {
            panic!("JSON")
        };
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        held.push((rid, v));
    }
    assert!(matches!(
        pages::execute(
            a,
            ac,
            id(),
            PageRequest::Dialogs { cursor: None },
            || Ok(())
        )
        .await,
        Err(Reason::Busy)
    ));
    for (rid, v) in held {
        a.admission
            .acknowledge(ac, &rid, v["replyLease"].as_str().unwrap())
            .unwrap();
    }
    token
}
pub(super) async fn reopened(a: &DesktopE2ee, context: u64, chat: &str, token: String) {
    assert_eq!(
        page(a, context, history(chat, Some(token)))
            .await
            .unwrap_err(),
        Reason::Stale
    );
    assert!(
        !page(a, context, history(chat, None)).await.unwrap()["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
