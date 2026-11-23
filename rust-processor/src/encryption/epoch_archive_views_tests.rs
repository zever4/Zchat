use super::super::super::incoming::tests::{delivery, payload, prepared, send};
use super::super::super::tests::{fixture, index, records};
use super::*;
use crate::encryption::signal_state::tests::{material, store};
use crate::proto::e2ee::{ConversationCloseReason, Device, OwnDeviceState};
const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const G: &str = "33333333-3333-4333-8333-333333333333";
const H: &str = "55555555-5555-4555-8555-555555555555";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
#[derive(Message)]
struct Marker {
    #[prost(message, optional, tag = "1")]
    previous: Option<OwnDeviceState>,
    #[prost(message, optional, tag = "2")]
    replaced_by: Option<DeviceRef>,
}
fn marker(device: Device, successor: DeviceRef) -> Vec<u8> {
    Marker {
        previous: Some(OwnDeviceState {
            user_id: device.reference.as_ref().unwrap().user_id.clone(),
            active_device: Some(device),
        }),
        replaced_by: Some(successor),
    }
    .encode_to_vec()
}
fn history(v: ViewPage) -> (Vec<HistoryItem>, Option<ViewCursor>) {
    match v {
        ViewPage::History(v, c) => (v, c),
        _ => panic!("wrong page"),
    }
}
fn archives(v: ViewPage) -> (Vec<ViewArchive>, Option<ViewCursor>) {
    match v {
        ViewPage::Archives(v, c) => (v, c),
        _ => panic!("wrong page"),
    }
}
async fn seed(w: &CryptoCoordinator) {
    while !w.seed_history_index().await.unwrap().ready {}
}

#[tokio::test]
async fn empty_inventory_reports_unready_without_seeding_or_creating_epoch() {
    let f = fixture().await;
    let before = records(f.root.path(), A);
    let (v, c) = archives(
        f.alice
            .read_views(ViewRequest::Archives(None))
            .await
            .unwrap(),
    );
    assert!(c.is_none());
    assert_eq!(v.len(), 1);
    assert_eq!(v[0].generation_id, G);
    assert!(v[0].replaced_by_generation_id.is_none() && !v[0].history_ready);
    match f
        .alice
        .read_views(ViewRequest::Segments(G.into(), CHAT.into(), None))
        .await
        .unwrap()
    {
        ViewPage::Segments(v, c) => assert!(v.is_empty() && c.is_none()),
        _ => panic!("wrong page"),
    }
    assert!(matches!(
        f.alice
            .read_views(ViewRequest::ArchiveHistory(
                G.into(),
                CHAT.into(),
                None,
                None
            ))
            .await,
        Err(CryptoError::HistoryIndexNotReady)
    ));
    assert!(
        f.alice
            .active_epoch_scope(CHAT.into())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn bounded_segment_history_cursor_is_bound_to_query_generation_chat_epoch_account_and_lifetime()
 {
    let (f, a, _) = prepared().await;
    let mut expected = std::collections::HashSet::new();
    for i in 0..13 {
        let text = if i >= 11 {
            "\u{1}".repeat(2000)
        } else {
            format!("archive-{i}")
        };
        let p = payload(&a, &text);
        expected.insert(p.message_id.clone());
        f.alice.encrypt_epoch(a.clone(), p).await.unwrap();
    }
    seed(&f.alice).await;
    let before = records(f.root.path(), A);
    let request = |epoch, c| ViewRequest::ArchiveHistory(G.into(), CHAT.into(), epoch, c);
    let (first, cursor) = history(f.alice.read_views(request(Some(0), None)).await.unwrap());
    assert!(first.len() <= 8);
    let cursor = cursor.unwrap();
    for query in [
        request(None, Some(cursor.clone())),
        request(Some(1), Some(cursor.clone())),
        ViewRequest::ArchiveHistory(H.into(), CHAT.into(), Some(0), Some(cursor.clone())),
        ViewRequest::ArchiveHistory(G.into(), H.into(), Some(0), Some(cursor.clone())),
        ViewRequest::History(CHAT.into(), Some(cursor.clone())),
        ViewRequest::ArchiveDialogs(G.into(), Some(cursor.clone())),
        ViewRequest::Segments(G.into(), CHAT.into(), Some(cursor.clone())),
        ViewRequest::Archives(Some(cursor.clone())),
    ] {
        assert!(matches!(
            f.alice.read_views(query).await,
            Err(CryptoError::Conflict)
        ));
    }
    assert!(matches!(
        f.bob
            .read_views(request(Some(0), Some(cursor.clone())))
            .await,
        Err(CryptoError::Conflict)
    ));
    let mut found = std::collections::HashSet::new();
    let mut c = None;
    for _ in 0..20 {
        let (items, next) = history(f.alice.read_views(request(Some(0), c)).await.unwrap());
        assert!(
            items.len() <= 8
                && items.iter().map(|v| wire_cost(&v.text)).sum::<usize>() <= HISTORY_WIRE_BUDGET
        );
        for item in items {
            assert_eq!(item.crypto_epoch, 0);
            assert!(found.insert(item.message_id));
        }
        c = next;
        if c.is_none() {
            break;
        }
    }
    assert!(c.is_none());
    assert_eq!(found, expected);
    assert_eq!(records(f.root.path(), A), before);
    assert!(matches!(
        f.alice.read_views(request(Some(u64::MAX), None)).await,
        Err(CryptoError::Invalid)
    ));
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    assert!(matches!(
        w.read_views(request(Some(0), Some(cursor))).await,
        Err(CryptoError::Conflict)
    ));
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn replaced_own_generation_preserves_incoming_outgoing_and_confirmation_history_but_cannot_send()
 {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "old outgoing"), 1).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let d = send(&f.bob, &b, payload(&b, "old incoming"), 2).await;
    f.alice
        .receive_epoch(a.clone(), delivery(&a, &d))
        .await
        .unwrap();
    // Bob's first incoming message was PreKey: its mandatory automatic reply
    // must still be validated after OWN closure, while remaining hidden in UI.
    seed(&f.bob).await;
    let bob_closed = Conversation {
        own_device: Some(b.own.clone()),
        peer_device: f.reverse_bundle.device.clone(),
        state: ConversationState::Closed as i32,
        close_reason: ConversationCloseReason::DeviceReplaced as i32,
        closed_at_unix_ms: 1_700_000_000_001,
        closed_generation_id: b.own.generation_id.clone(),
        ..f.a.clone()
    };
    index(&f.bob, &bob_closed).await;
    let bob_before = records(f.root.path(), B);
    let (bob_items, _) = history(
        f.bob
            .read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .unwrap(),
    );
    assert_eq!(bob_items.len(), 2);
    assert_eq!(records(f.root.path(), B), bob_before);
    seed(&f.alice).await;
    let mut closed = f.a.clone();
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_at_unix_ms = 1_700_000_000_001;
    closed.closed_generation_id = G.into();
    index(&f.alice, &closed).await;
    assert!(matches!(
        f.alice.active_epoch_scope(CHAT.into()).await,
        Err(CryptoError::ArchivedGeneration)
    ));
    // Even the old owner may DISPLAY an own-closed conversation, but never use
    // its saved outbox plan as mutation/delivery authority after own closure.
    let (v, _) = history(
        f.alice
            .read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .unwrap(),
    );
    assert_eq!(v.len(), 2);
    assert!(
        f.alice
            .epoch_outgoing_plan(
                v.iter()
                    .find(|v| v.delivery.is_some())
                    .unwrap()
                    .message_id
                    .clone()
            )
            .await
            .is_err()
    );
    f.alice.shutdown().await.unwrap();
    let successor = DeviceRef {
        generation_id: H.into(),
        ..a.own.clone()
    };
    let mut s = store(f.root.path(), A, false);
    s.transaction_with::<_, CryptoError>(|tx| {
        tx.put(
            &RecordKey::new(
                G,
                RecordKind::Control,
                crate::encryption::provisioning::ARCHIVED_DEVICE_RECORD,
            )?,
            &marker(f.reverse_bundle.device.clone().unwrap(), successor.clone()),
        )?;
        install_material(tx, &successor, material())?;
        Ok(())
    })
    .unwrap();
    drop(s);
    assert!(matches!(
        CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone()).await,
        Err(CryptoError::ArchivedGeneration)
    ));
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), successor)
        .await
        .unwrap();
    seed(&w).await;
    let before = records(f.root.path(), A);
    let (v, c) = archives(w.read_views(ViewRequest::Archives(None)).await.unwrap());
    assert_eq!(v.len(), 2);
    assert!(c.is_none());
    assert!(v.iter().any(|v| v.generation_id == G
        && v.replaced_by_generation_id.as_deref() == Some(H)
        && v.history_ready));
    match w
        .read_views(ViewRequest::ArchiveDialogs(G.into(), None))
        .await
        .unwrap()
    {
        ViewPage::Dialogs(v, c) => {
            assert_eq!(v.len(), 1);
            assert!(c.is_none());
            assert_eq!(v[0].state, ConversationState::Closed);
        }
        _ => panic!("wrong page"),
    }
    let (v, c) = history(
        w.read_views(ViewRequest::ArchiveHistory(
            G.into(),
            CHAT.into(),
            None,
            None,
        ))
        .await
        .unwrap(),
    );
    assert!(c.is_none());
    assert_eq!(v.len(), 2);
    assert!(v.iter().any(|v| v.text.as_str() == "old incoming"));
    assert!(v.iter().any(|v| v.text.as_str() == "old outgoing"));
    assert!(matches!(
        w.active_epoch_scope(CHAT.into()).await,
        Err(CryptoError::ConversationUnavailable)
    ));
    assert!(matches!(
        w.read_views(ViewRequest::History(CHAT.into(), None)).await,
        Err(CryptoError::ConversationUnavailable)
    ));
    assert!(
        w.encrypt_epoch(a.clone(), payload(&a, "must not send"))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn inventory_keyset_is_bounded_and_unmarked_or_foreign_archives_fail_closed_without_repair() {
    let f = fixture().await;
    f.alice.shutdown().await.unwrap();
    let own = f.a.own_device.clone().unwrap();
    let public = f.reverse_bundle.device.clone().unwrap();
    let mut s = store(f.root.path(), A, false);
    s.transaction_with::<_, CryptoError>(|tx| {
        let identity = tx
            .get(&RecordKey::new(G, RecordKind::Identity, "local")?)?
            .unwrap();
        for i in 0..18 {
            let generation = format!("00000000-0000-4000-8000-{i:012x}");
            let mut device = public.clone();
            device.reference.as_mut().unwrap().generation_id = generation.clone();
            tx.put(
                &RecordKey::new(&generation, RecordKind::Identity, "local")?,
                &identity,
            )?;
            tx.put(
                &RecordKey::new(
                    &generation,
                    RecordKind::Control,
                    crate::encryption::provisioning::ARCHIVED_DEVICE_RECORD,
                )?,
                &marker(device, own.clone()),
            )?;
        }
        Ok(())
    })
    .unwrap();
    drop(s);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own.clone())
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    let mut found = Vec::new();
    let mut c = None;
    let mut pages = 0;
    loop {
        let (v, next) = archives(w.read_views(ViewRequest::Archives(c)).await.unwrap());
        assert!(v.len() <= 8);
        pages += 1;
        found.extend(v.into_iter().map(|v| v.generation_id));
        c = next;
        if c.is_none() {
            break;
        }
        assert!(pages < 5);
    }
    assert_eq!(pages, 3);
    assert_eq!(found.len(), 19);
    let mut sorted = found.clone();
    sorted.sort();
    assert_eq!(found, sorted);
    assert!(
        w.read_views(ViewRequest::ArchiveDialogs(B.into(), None))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    let mut s = store(f.root.path(), A, false);
    s.transaction_with::<_, CryptoError>(|tx| {
        let mut foreign = public;
        foreign.reference.as_mut().unwrap().user_id = B.into();
        tx.put(
            &RecordKey::new(
                H,
                RecordKind::Control,
                crate::encryption::provisioning::ARCHIVED_DEVICE_RECORD,
            )?,
            &marker(foreign, own.clone()),
        )?;
        Ok(())
    })
    .unwrap();
    drop(s);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own)
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    assert!(
        w.read_views(ViewRequest::ArchiveDialogs(H.into(), None))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn generation_seek_uses_primary_key_and_read_does_not_repair_corrupt_history_index() {
    let (f, a, _) = prepared().await;
    f.alice
        .encrypt_epoch(a.clone(), payload(&a, "do not repair"))
        .await
        .unwrap();
    seed(&f.alice).await;
    f.alice.shutdown().await.unwrap();
    let mut s = store(f.root.path(), A, false);
    s.transaction(|tx| {
        tx.put(
            &RecordKey::new(G, RecordKind::Control, "ui-h2-state")?,
            &[0],
        )
    })
    .unwrap();
    drop(s);
    let path = StoreScope::new("https://chat.example.test", A)
        .unwrap()
        .database_path(f.root.path());
    let db = rusqlite::Connection::open(path).unwrap();
    let plan:Vec<String>=db.prepare("EXPLAIN QUERY PLAN SELECT generation FROM protected_records WHERE generation>?1 ORDER BY generation LIMIT 1").unwrap()
        .query_map([Vec::<u8>::new()],|r|r.get(3)).unwrap().collect::<Result<_,_>>().unwrap();
    assert!(
        plan.iter()
            .any(|v| v.contains("SEARCH protected_records USING PRIMARY KEY (generation>?)"))
    );
    assert!(
        !plan
            .iter()
            .any(|v| v.contains("SCAN") || v.contains("TEMP B-TREE"))
    );
    drop(db);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    assert!(w.read_views(ViewRequest::Archives(None)).await.is_err());
    assert!(
        w.read_views(ViewRequest::ArchiveHistory(
            G.into(),
            CHAT.into(),
            None,
            None
        ))
        .await
        .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
