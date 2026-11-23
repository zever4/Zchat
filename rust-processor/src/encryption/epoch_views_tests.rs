use super::super::super::tests::store;
use super::super::incoming::tests::{delivery, payload, prepared, send};
use super::super::tests::{fixture, index, records};
use super::*;
use std::collections::HashSet;
const A: &str = "11111111-1111-4111-8111-111111111111";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
async fn seed(w: &CryptoCoordinator) {
    while !w.seed_history_index().await.unwrap().ready {}
}
fn dialogs(page: ViewPage) -> (Vec<ViewDialog>, Option<ViewCursor>) {
    match page {
        ViewPage::Dialogs(v, c) => (v, c),
        _ => panic!("wrong page"),
    }
}
fn history(page: ViewPage) -> (Vec<HistoryItem>, Option<ViewCursor>) {
    match page {
        ViewPage::History(v, c) => (v, c),
        _ => panic!("wrong page"),
    }
}
#[tokio::test]
async fn empty_dialogs_page_without_bootstrap_and_cursor_cannot_cross_query_account_or_reopen() {
    let f = fixture().await;
    for i in 1..11 {
        let mut c = f.a.clone();
        c.conversation_id = format!("00000000-0000-4000-8000-{i:012x}");
        index(&f.alice, &c).await;
    }
    seed(&f.alice).await;
    seed(&f.bob).await;
    let before = records(f.root.path(), A);
    let (first, cursor) = dialogs(
        f.alice
            .read_views(ViewRequest::Dialogs(None))
            .await
            .unwrap(),
    );
    assert_eq!(first.len(), 8);
    let cursor = cursor.unwrap();
    assert!(
        f.alice
            .active_epoch_scope(CHAT.into())
            .await
            .unwrap()
            .is_none()
    );
    let (last, end) = dialogs(
        f.alice
            .read_views(ViewRequest::Dialogs(Some(cursor.clone())))
            .await
            .unwrap(),
    );
    assert_eq!(last.len(), 3);
    assert!(end.is_none());
    assert!(matches!(
        f.bob
            .read_views(ViewRequest::Dialogs(Some(cursor.clone())))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.alice
            .read_views(ViewRequest::History(CHAT.into(), Some(cursor.clone())))
            .await,
        Err(CryptoError::Conflict)
    ));
    let (empty, end) = history(
        f.alice
            .read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .unwrap(),
    );
    assert!(empty.is_empty() && end.is_none());
    assert!(records(f.root.path(), A) == before);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        w.read_views(ViewRequest::Dialogs(Some(cursor))).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(
        dialogs(w.read_views(ViewRequest::Dialogs(None)).await.unwrap())
            .0
            .len(),
        8
    );
    assert!(records(f.root.path(), A) == before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn chronological_byte_bounded_history_never_skips_excluded_text_or_mutates_state() {
    let (f, a, _) = prepared().await;
    let mut expected = Vec::new();
    for i in 0..14 {
        let text = if i < 2 {
            "\u{0001}".repeat(2000)
        } else {
            format!("text-{i}")
        };
        let mut p = payload(&a, &text);
        p.message_id = format!("00000000-0000-4000-8000-{:012x}", i + 1);
        p.created_at_unix_ms += if i < 2 { 100 + i } else { i % 5 };
        expected.push((p.created_at_unix_ms, p.message_id.clone(), text));
        f.alice.encrypt_epoch(a.clone(), p).await.unwrap();
    }
    seed(&f.alice).await;
    expected.sort_by(|a, b| b.cmp(a));
    let before = records(f.root.path(), A);
    let mut cursor = None;
    let mut actual = Vec::new();
    let mut page_sizes = Vec::new();
    for _ in 0..16 {
        let (items, next) = history(
            f.alice
                .read_views(ViewRequest::History(CHAT.into(), cursor))
                .await
                .unwrap(),
        );
        assert!(
            items.len() <= 8
                && items.iter().map(|i| wire_cost(&i.text)).sum::<usize>() <= HISTORY_WIRE_BUDGET
        );
        page_sizes.push(items.len());
        for v in items {
            assert!(v.delivery.is_some());
            actual.push((v.created_at_unix_ms, v.message_id, v.text.to_string()));
        }
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert_eq!(page_sizes[0], 1);
    // A second long text may share its page with short texts when they fit;
    // the two escaped long texts themselves cannot fit in the same page.
    assert!(page_sizes[1] >= 1);
    assert_eq!(actual, expected);
    assert!(records(f.root.path(), A) == before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn incoming_and_outgoing_user_text_share_history_but_service_and_foreign_statuses_are_hidden()
{
    let (f, a, b) = prepared().await;
    let p = payload(&a, "read durable incoming only");
    let d = send(&f.alice, &a, p.clone(), 1).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    seed(&f.alice).await;
    seed(&f.bob).await;
    let (items, _) = history(
        f.bob
            .read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .unwrap(),
    );
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].text.as_str(), "read durable incoming only");
    assert!(items[0].delivery.is_none());
    let service = f
        .bob
        .epoch_outbox_page(None)
        .await
        .unwrap()
        .messages
        .remove(0)
        .request()
        .envelope
        .as_ref()
        .unwrap()
        .message_id
        .clone();
    assert!(matches!(
        f.bob
            .read_views(ViewRequest::Statuses(CHAT.into(), vec![service]))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.bob
            .read_views(ViewRequest::Statuses(
                CHAT.into(),
                vec![p.message_id.clone()]
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    let ViewPage::Statuses(status) = f
        .alice
        .read_views(ViewRequest::Statuses(CHAT.into(), vec![p.message_id]))
        .await
        .unwrap()
    else {
        panic!("wrong page")
    };
    assert_eq!(status.len(), 1);
    assert!(!status[0].delivery.stored_on_server);
    let ViewPage::Dialogs(v, _) = f.bob.read_views(ViewRequest::Dialogs(None)).await.unwrap()
    else {
        panic!("wrong page")
    };
    assert_eq!(v.len(), 1);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn explicit_successor_retains_closed_chat_history_ratchet_and_exact_outbox_after_reopen() {
    let (f, a, _) = prepared().await;
    let text = payload(&a, "retained after peer replacement");
    let message = text.message_id.clone();
    f.alice.encrypt_epoch(a.clone(), text).await.unwrap();
    seed(&f.alice).await;
    let outgoing = f
        .alice
        .epoch_outgoing_plan(message.clone())
        .await
        .unwrap()
        .request()
        .clone();
    let old = f.alice.prepare_conversation_creation(42).await.unwrap();
    f.alice
        .accept_conversation_creation(VerifiedConversationCreation::from_rpc(old, f.a.clone()))
        .await
        .unwrap();
    let mut closed = f.a.clone();
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = crate::proto::e2ee::ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_at_unix_ms = 1_700_000_000_001;
    closed.closed_generation_id = closed
        .peer_device
        .as_ref()
        .unwrap()
        .reference
        .as_ref()
        .unwrap()
        .generation_id
        .clone();
    closed.peer_device.as_mut().unwrap().public_material_ready = false;
    index(&f.alice, &closed).await;
    let before = records(f.root.path(), A);
    let next = f
        .alice
        .prepare_conversation_after_closed(42, CHAT.into())
        .await
        .unwrap();
    let mut active = f.a.clone();
    active.conversation_id = uuid::Uuid::new_v4().to_string();
    f.alice
        .accept_conversation_creation(VerifiedConversationCreation::from_rpc(
            next.clone(),
            active.clone(),
        ))
        .await
        .unwrap();
    // Every existing encrypted row, except the explicitly advanced peer pointer,
    // is preserved byte for byte (including history, ratchet and outbox cohorts).
    let after = records(f.root.path(), A);
    assert!(
        before
            .iter()
            .filter(|(_, id, _)| id != "create-peer:42")
            .all(|row| after.contains(row))
    );
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        w.prepare_conversation_after_closed(42, CHAT.into())
            .await
            .unwrap()
            .request(),
        next.request()
    );
    let (items, end) = history(
        w.read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .unwrap(),
    );
    assert!(end.is_none());
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].message_id, message);
    assert_eq!(items[0].text.as_str(), "retained after peer replacement");
    assert_eq!(
        w.epoch_outgoing_plan(message).await.unwrap().request(),
        &outgoing
    );
    assert!(
        w.active_epoch_scope(active.conversation_id)
            .await
            .unwrap()
            .is_none()
    );
    let (dialogs, _) = dialogs(w.read_views(ViewRequest::Dialogs(None)).await.unwrap());
    assert_eq!(dialogs.len(), 2);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn old_v2_index_seed_is_bounded_atomic_and_preserves_ciphertext_keys_and_history() {
    let (f, a, _) = prepared().await;
    for _ in 0..33 {
        f.alice
            .encrypt_epoch(a.clone(), payload(&a, "old retained text"))
            .await
            .unwrap();
    }
    f.alice.shutdown().await.unwrap();
    let db = super::super::outgoing::tests::db(f.root.path());
    db.execute(
        "DELETE FROM protected_records WHERE kind=?1 AND record_id GLOB 'ui-h2*'",
        [RecordKind::Control as u8],
    )
    .unwrap();
    let before = records(f.root.path(), A);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        w.read_views(ViewRequest::History(CHAT.into(), None)).await,
        Err(CryptoError::HistoryIndexNotReady)
    ));
    db.execute_batch("CREATE TABLE ui_parent(id INTEGER PRIMARY KEY);CREATE TABLE ui_child(id INTEGER REFERENCES ui_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER ui_fault AFTER INSERT ON protected_records WHEN NEW.record_id='ui-h2-state' BEGIN INSERT INTO ui_child VALUES(1);END;").unwrap();
    assert!(matches!(
        w.seed_history_index().await,
        Err(CryptoError::Storage(_))
    ));
    assert!(records(f.root.path(), A) == before);
    db.execute_batch("DROP TRIGGER ui_fault").unwrap();
    let first = w.seed_history_index().await.unwrap();
    assert_eq!(first.processed, 32);
    assert!(!first.ready);
    let last = w.seed_history_index().await.unwrap();
    assert_eq!(last.processed, 1);
    assert!(last.ready);
    let after = records(f.root.path(), A);
    for row in before {
        assert!(after.contains(&row));
    }
    let mut cursor = None;
    let mut ids = HashSet::new();
    loop {
        let (items, next) = history(
            w.read_views(ViewRequest::History(CHAT.into(), cursor))
                .await
                .unwrap(),
        );
        for i in items {
            assert!(ids.insert(i.message_id));
        }
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(ids.len(), 33);
    assert!(records(f.root.path(), A) == after);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn incomplete_index_and_cohort_fail_closed_without_repair() {
    let (f, a, _) = prepared().await;
    let p = payload(&a, "missing link must not repair");
    f.alice.encrypt_epoch(a.clone(), p.clone()).await.unwrap();
    seed(&f.alice).await;
    let db = super::super::outgoing::tests::db(f.root.path());
    db.execute(
        "DELETE FROM protected_records WHERE record_id=?1",
        [format!("ui-h2ref:{}", p.message_id)],
    )
    .unwrap();
    let before = records(f.root.path(), A);
    assert!(
        f.alice
            .read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .is_err()
    );
    assert!(
        f.alice
            .read_views(ViewRequest::Statuses(CHAT.into(), vec![p.message_id]))
            .await
            .is_err()
    );
    assert!(records(f.root.path(), A) == before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
