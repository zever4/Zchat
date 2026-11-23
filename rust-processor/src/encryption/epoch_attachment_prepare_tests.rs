use super::super::super::tests::store;
use super::super::incoming::tests::{delivery, payload, prepared, send};
use super::super::tests::{fixture, index, initialize, records, rewrite_frozen, wrapped};
use super::*;
use crate::proto::e2ee::ConversationCloseReason;
use crate::proto::e2ee_v2::AttachmentKind;
use rusqlite::Connection;
use std::path::Path;
use uuid::Uuid;

#[path = "epoch_attachment_control_tests.rs"]
mod controls;

#[path = "epoch_attachment_promotion_tests.rs"]
mod promotion;

#[path = "epoch_attachment_retry_tests.rs"]
mod retry_tests;

#[path = "epoch_attachment_archive_tests.rs"]
mod archive_tests;

#[path = "epoch_attachment_retirement_tests.rs"]
mod retirement_tests;

#[cfg(feature = "compression")]
#[path = "epoch_photo_input_tests.rs"]
mod photo_input_tests;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const AG: &str = "33333333-3333-4333-8333-333333333333";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
const SERVER: &str = "https://chat.example.test";
const CRASH_ID: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";

fn draft() -> AttachmentDraft {
    AttachmentDraft {
        kind: AttachmentKind::File,
        filename: Zeroizing::new("private-native-filename.bin".into()),
        mime_type: Zeroizing::new("application/octet-stream".into()),
        caption: Zeroizing::new("private native caption".into()),
        photo_metadata_stripped: None,
    }
}
fn source() -> Zeroizing<Vec<u8>> {
    Zeroizing::new(b"private-native-file-content-marker".repeat(18000))
}

#[tokio::test]
async fn pending_attachments_restore_after_restart_and_move_to_history_on_promotion() {
    let (f, own, peer) = prepared().await;
    let mut other_chat = f.a.clone();
    other_chat.conversation_id = Uuid::new_v4().to_string();
    index(&f.alice, &other_chat).await;
    let ids: Vec<_> = (1..=10)
        .map(|i| format!("00000000-0000-4000-8000-{i:012}"))
        .collect();
    let long_caption = "\u{0001}".repeat(2000);
    for (i, id) in ids.iter().enumerate() {
        let mut draft = draft();
        if i < 2 {
            draft.caption = Zeroizing::new(long_caption.clone());
        }
        f.alice
            .prepare_epoch_attachment(CHAT.into(), id.clone(), draft, Zeroizing::new(vec![7; 40]))
            .await
            .unwrap();
    }
    let read = |cursor| ViewRequest::PendingAttachments(CHAT.into(), cursor);
    let ViewPage::PendingAttachments(items, Some(cursor)) =
        f.alice.read_views(read(None)).await.unwrap()
    else {
        panic!("expected pending page");
    };
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].message_id, ids[0]);
    assert!(matches!(
        f.alice
            .read_views(ViewRequest::PendingAttachments(
                other_chat.conversation_id.clone(),
                Some(cursor.clone())
            ))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.bob.read_views(read(Some(cursor.clone()))).await,
        Err(CryptoError::Conflict)
    ));

    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), own.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        w.read_views(read(Some(cursor))).await,
        Err(CryptoError::Conflict)
    ));
    let ViewPage::PendingAttachments(empty, None) = w
        .read_views(ViewRequest::PendingAttachments(
            other_chat.conversation_id,
            None,
        ))
        .await
        .unwrap()
    else {
        panic!("expected an empty page");
    };
    assert!(empty.is_empty());
    let before = snapshot(f.root.path(), A);
    let mut found = Vec::new();
    let mut cursor = None;
    loop {
        let ViewPage::PendingAttachments(items, next) = w.read_views(read(cursor)).await.unwrap()
        else {
            panic!()
        };
        for item in items {
            assert_eq!(item.attachment.plaintext_len, 40);
            if found.len() < 2 {
                assert_eq!(item.attachment.caption.as_str(), long_caption);
            }
            found.push(item.message_id);
        }
        if next.is_none() {
            break;
        }
        assert!(found.len() < ids.len());
        cursor = next;
    }
    assert_eq!(found, ids);
    assert_eq!(snapshot(f.root.path(), A), before);

    let control = w.epoch_attachment_control(ids[0].clone()).await.unwrap();
    w.accept_epoch_blob_lease(controls::observed(
        &control,
        controls::grant(&control, v2::BlobLeaseState::Ready),
    ))
    .await
    .unwrap();
    let EpochAttachmentPromotion::Queued(sent) =
        w.promote_epoch_attachment(ids[0].clone()).await.unwrap()
    else {
        panic!("expected a promoted attachment");
    };
    let ViewPage::PendingAttachments(items, _) = w.read_views(read(None)).await.unwrap() else {
        panic!()
    };
    assert_eq!(items[0].message_id, ids[1]);
    let env = sent.request().envelope.as_ref().unwrap();
    let d = v2::Delivery {
        sequence: 1,
        envelope: Some(env.clone()),
        envelope_digest: data::envelope_digest(env).unwrap().to_vec(),
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
    };
    f.bob
        .receive_epoch(peer.clone(), delivery(&peer, &d))
        .await
        .unwrap();
    for (worker, outgoing) in [(&w, true), (&f.bob, false)] {
        while !worker.seed_history_index().await.unwrap().ready {}
        let ViewPage::History(items, None) = worker
            .read_views(ViewRequest::History(CHAT.into(), None))
            .await
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].message_id, ids[0]);
        assert_eq!(items[0].delivery.is_some(), outgoing);
        assert_eq!(
            items[0].attachment.as_ref().unwrap().caption.as_str(),
            long_caption
        );
    }
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
fn plan(result: EpochAttachmentPreparation) -> EpochAttachmentPlan {
    match result {
        EpochAttachmentPreparation::Prepared(p) => *p,
        _ => panic!("expected durable preparation"),
    }
}
async fn prepare(w: &CryptoCoordinator, id: &str) -> CryptoResult<EpochAttachmentPlan> {
    Ok(plan(
        w.prepare_epoch_attachment(CHAT.into(), id.into(), draft(), source())
            .await?,
    ))
}
async fn seed(w: &CryptoCoordinator) {
    for _ in 0..10 {
        if w.seed_recovery_indexes().await.unwrap().ready {
            return;
        }
    }
    panic!("fixture recovery index initialization did not finish");
}
fn raw(root: &Path, user: &str) -> Connection {
    Connection::open(StoreScope::new(SERVER, user).unwrap().database_path(root)).unwrap()
}
fn cohort(root: &Path, user: &str) -> (i64, i64, i64) {
    raw(root, user).query_row("SELECT (SELECT count(*) FROM protected_attachment_blobs),(SELECT count(*) FROM protected_attachment_chunks),(SELECT coalesce(sum(length(payload)),0) FROM protected_attachment_chunks)", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
}
fn snapshot(root: &Path, user: &str) -> [u8; 32] {
    // Include raw ciphertext chunks and public manifest identity in immutability
    // checks, not only the protected journal/ratchet rows.
    let mut out = Vec::new();
    for row in records(root, user) {
        out.extend(row.0.to_be_bytes());
        out.extend(row.1.as_bytes());
        out.extend(row.2);
    }
    let db = raw(root, user);
    let mut q = db.prepare("SELECT record_key,blob_id,ciphertext_size,chunk_count FROM protected_attachment_blobs ORDER BY record_key").unwrap();
    for r in q
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })
        .unwrap()
    {
        let (key, blob, size, count) = r.unwrap();
        out.extend(key.as_bytes());
        out.extend(blob.as_bytes());
        out.extend(size.to_be_bytes());
        out.extend(count.to_be_bytes());
    }
    let mut q = db.prepare("SELECT record_key,chunk_index,payload FROM protected_attachment_chunks ORDER BY record_key,chunk_index").unwrap();
    for r in q
        .query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, Vec<u8>>(2)?,
            ))
        })
        .unwrap()
    {
        let (key, index, bytes) = r.unwrap();
        out.extend(key.as_bytes());
        out.extend(index.to_be_bytes());
        out.extend(bytes);
    }
    Sha256::digest(out).into()
}
fn verify_secret(root: &Path, own: &DeviceRef, id: &str, blob: &[u8]) {
    let mut db = store(root, &own.user_id, false);
    let j = db
        .transaction_with::<_, CryptoError>(|tx| load_journal(tx, own, id))
        .unwrap()
        .unwrap();
    let p = j.payload.as_ref().unwrap();
    let a = part(p).unwrap();
    let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor).unwrap();
    let binding = attachment::AttachmentBinding::new(
        CHAT,
        p.crypto_epoch.unwrap(),
        id,
        &a.blob_id,
        own,
        p.recipient.as_ref().unwrap(),
    )
    .unwrap();
    assert_eq!(
        attachment::open(&binding, &descriptor, blob)
            .unwrap()
            .as_slice(),
        source().as_slice()
    );
    let disk = std::fs::read(
        StoreScope::new(SERVER, &own.user_id)
            .unwrap()
            .database_path(root),
    )
    .unwrap();
    for private in [
        b"private-native-filename.bin".as_slice(),
        b"private native caption",
        b"private-native-file-content-marker",
        &a.private_descriptor[..32],
    ] {
        assert!(!disk.windows(private.len()).any(|bytes| bytes == private));
    }
}

#[tokio::test]
async fn prepare_commit_reopen_exact_retry_does_not_touch_signal_or_reseal() {
    let f = fixture().await;
    let a = initialize(&f.alice).await; // Deliberately no Signal bootstrap.
    seed(&f.alice).await;
    let before = records(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    let id = Uuid::new_v4().to_string();
    let p = prepare(&f.alice, &id).await.unwrap();
    let public = (p.blob_id().to_owned(), *p.blob_digest(), p.blob_len());
    assert_eq!(p.message_id(), id);
    assert_eq!(p.conversation_id(), CHAT);
    assert_eq!(p.crypto_epoch(), 0);
    let bytes = f.alice.prepared_epoch_attachment_blob(p).await.unwrap();
    let exact = bytes.as_bytes().to_vec();
    assert_eq!(<[u8; 32]>::from(Sha256::digest(&exact)), public.1);
    drop(bytes);
    assert_eq!(cohort(f.root.path(), A), (1, 3, public.2 as i64));
    let after = records(f.root.path(), A);
    assert_eq!(after.len(), before.len() + 1); // Only private preparation journal.
    for row in before {
        assert!(after.contains(&row));
    }
    assert_eq!(wrapped(f.root.path(), A), master);
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    f.alice.shutdown().await.unwrap();
    verify_secret(f.root.path(), &a.own, &id, &exact);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    let p = prepare(&w, &id).await.unwrap();
    assert_eq!(
        (p.blob_id().to_owned(), *p.blob_digest(), p.blob_len()),
        public
    );
    assert_eq!(
        w.prepared_epoch_attachment_blob(p)
            .await
            .unwrap()
            .as_bytes(),
        exact
    );
    assert_eq!(snapshot(f.root.path(), A), saved);
    let p = w
        .prepared_epoch_attachment(id.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(p.blob_id(), public.0);
    assert!(matches!(
        w.prepare_epoch_attachment(
            CHAT.into(),
            id.clone(),
            draft(),
            Zeroizing::new(vec![1; 32])
        )
        .await,
        Err(CryptoError::Conflict)
    ));
    let mut changed = draft();
    changed.caption.push('!');
    assert!(matches!(
        w.prepare_epoch_attachment(CHAT.into(), id, changed, source())
            .await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn no_catalog_no_preparation_and_global_message_uuid_conflicts_are_atomic() {
    let f = fixture().await;
    seed(&f.alice).await;
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice
            .prepare_epoch_attachment(CHAT.into(), Uuid::new_v4().to_string(), draft(), source())
            .await
            .unwrap(),
        EpochAttachmentPreparation::CatalogRequired
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let (f, a, b) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice
            .prepare_epoch_text(CHAT.into(), id.clone(), Zeroizing::new("collision".into()))
            .await,
        Err(CryptoError::Conflict)
    ));
    let mut p = payload(&a, "collision");
    p.message_id = id;
    assert!(matches!(
        f.alice.encrypt_epoch(a.clone(), p).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    let mut p = payload(&a, "incoming collision");
    prepare(&f.bob, &p.message_id).await.unwrap();
    let before = snapshot(f.root.path(), B);
    let d = send(&f.alice, &a, p.clone(), 1).await;
    assert!(matches!(
        f.bob.receive_epoch(b.clone(), delivery(&b, &d)).await,
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        f.bob
            .quarantine_epoch(
                b.clone(),
                delivery(&b, &d),
                EpochQuarantineReason::InvalidCiphertext
            )
            .await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), B), before);
    p.message_id = Uuid::new_v4().to_string();
    let d = send(&f.alice, &a, p.clone(), 2).await;
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        prepare(&f.alice, &p.message_id).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    f.bob
        .quarantine_epoch(
            b.clone(),
            delivery(&b, &d),
            EpochQuarantineReason::InvalidCiphertext,
        )
        .await
        .unwrap();
    let before = snapshot(f.root.path(), B);
    assert!(matches!(
        prepare(&f.bob, &p.message_id).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), B), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_commit_returns_no_plan_and_rolls_back_journal_manifest_and_all_chunks() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let id = Uuid::new_v4().to_string();
    let before = snapshot(f.root.path(), A);
    let db = raw(f.root.path(), A);
    db.execute_batch("CREATE TABLE prep_parent(id INTEGER PRIMARY KEY);CREATE TABLE prep_child(id INTEGER REFERENCES prep_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER prep_fault AFTER INSERT ON protected_attachment_chunks BEGIN INSERT INTO prep_child VALUES(1);END;").unwrap();
    assert!(matches!(
        prepare(&f.alice, &id).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
    assert!(
        f.alice
            .prepared_epoch_attachment(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    db.execute_batch("DROP TRIGGER prep_fault;").unwrap();
    drop(db);
    prepare(&f.alice, &id).await.unwrap();
    assert_eq!(cohort(f.root.path(), A).0, 1);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_frozen_closed_and_cross_account_proofs_cannot_authorize_upload() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    initialize(&f.bob).await;
    let id = Uuid::new_v4().to_string();
    let p = prepare(&f.alice, &id).await.unwrap();
    assert!(matches!(
        f.bob.prepared_epoch_attachment_blob(p).await,
        Err(CryptoError::Conflict)
    ));
    assert!(
        f.bob
            .prepared_epoch_attachment(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    // Same account at a different authenticated server cannot reuse the proof.
    let other = crate::encryption::protected::test_open(
        f.root.path(),
        StoreScope::new("https://other.example.test", A).unwrap(),
        true,
    );
    let other = CryptoCoordinator::start(other, a.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        other
            .prepared_epoch_attachment_blob(prepare(&f.alice, &id).await.unwrap())
            .await,
        Err(CryptoError::Conflict)
    ));
    other.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    let before = snapshot(f.root.path(), A);
    let old = prepare(&w, &id).await.unwrap(); // Local replay, no network authority.
    assert!(matches!(
        w.prepared_epoch_attachment_blob(old).await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    assert!(matches!(
        prepare(&w, &Uuid::new_v4().to_string()).await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    let mut closed = f.a.clone();
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_at_unix_ms = 1_700_000_000_000;
    closed.closed_generation_id = closed
        .peer_device
        .as_ref()
        .unwrap()
        .reference
        .as_ref()
        .unwrap()
        .generation_id
        .clone();
    index(&w, &closed).await;
    let before = snapshot(f.root.path(), A);
    let old = w
        .prepared_epoch_attachment(id.clone())
        .await
        .unwrap()
        .unwrap();
    let error = w
        .prepared_epoch_attachment_blob(old)
        .await
        .err()
        .expect("closed upload must fail");
    assert!(
        matches!(error, CryptoError::ConversationClosed),
        "{error:?}"
    );
    assert!(matches!(
        prepare(&w, &Uuid::new_v4().to_string()).await,
        Err(CryptoError::ConversationClosed)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    let old = w.prepared_epoch_attachment(id).await.unwrap().unwrap();
    let mut replacement = old.own.clone();
    replacement.generation_id = Uuid::new_v4().to_string();
    w.shutdown().await.unwrap();
    // Low-level owner fixture only: no product replacement/reset is performed.
    let replacement = CryptoCoordinator::start(store(f.root.path(), A, false), replacement)
        .await
        .unwrap();
    assert!(matches!(
        replacement.prepared_epoch_attachment_blob(old).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    replacement.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_missing_or_spliced_chunks_fail_closed_without_reseal_or_repair() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    let p = f
        .alice
        .prepared_epoch_attachment(id.clone())
        .await
        .unwrap()
        .unwrap();
    let db = raw(f.root.path(), A);
    let original: Vec<u8> = db
        .query_row(
            "SELECT payload FROM protected_attachment_chunks WHERE chunk_index=1",
            [],
            |r| r.get(0),
        )
        .unwrap();
    db.execute("UPDATE protected_attachment_chunks SET payload=zeroblob(length(payload)) WHERE chunk_index=1",[]).unwrap();
    let corrupt = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice
            .epoch_attachment_input_state(CHAT.into(), id.clone())
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert!(matches!(
        prepare(&f.alice, &id).await,
        Err(CryptoError::Storage(_))
    ));
    assert!(f.alice.prepared_epoch_attachment_blob(p).await.is_err());
    assert!(f.alice.prepared_epoch_attachment(id.clone()).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), corrupt);
    db.execute(
        "UPDATE protected_attachment_chunks SET payload=?1 WHERE chunk_index=1",
        [&original],
    )
    .unwrap();
    db.execute(
        "DELETE FROM protected_attachment_chunks WHERE chunk_index=1",
        [],
    )
    .unwrap();
    let missing = snapshot(f.root.path(), A);
    assert!(f.alice.prepared_epoch_attachment(id.clone()).await.is_err());
    assert!(prepare(&f.alice, &id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), missing);
    db.execute("INSERT INTO protected_attachment_chunks SELECT record_key,1,?1 FROM protected_attachment_blobs",[&original]).unwrap();
    assert!(
        f.alice
            .prepared_epoch_attachment(id.clone())
            .await
            .unwrap()
            .is_some()
    );
    db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
    assert!(
        db.execute("DELETE FROM protected_attachment_blobs", [])
            .is_err()
    );
    assert!(
        db.execute(
            "DELETE FROM protected_records WHERE record_id=?1",
            [format!("attachment-prepare:{id}")]
        )
        .is_err()
    );
    db.execute(
        "UPDATE protected_records SET attachment_ref=NULL WHERE record_id=?1",
        [format!("attachment-prepare:{id}")],
    )
    .unwrap();
    let missing = snapshot(f.root.path(), A);
    assert!(prepare(&f.alice, &id).await.is_err());
    assert_eq!(snapshot(f.root.path(), A), missing);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn attachment_input_state_never_labels_orphaned_ciphertext_as_vacant() {
    let (f, _, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    prepare(&f.alice, &id).await.unwrap();
    // Deliberately bypass SQLite's FK to model partial corruption while open;
    // ordinary COMMIT/crash paths cannot publish this orphan.
    let db = raw(f.root.path(), A);
    db.execute_batch("PRAGMA foreign_keys=OFF;").unwrap();
    db.execute(
        "DELETE FROM protected_records WHERE record_id=?1",
        [format!("attachment-prepare:{id}")],
    )
    .unwrap();
    let before = snapshot(f.root.path(), A);
    assert!(matches!(
        f.alice
            .epoch_attachment_input_state(CHAT.into(), id.clone())
            .await,
        Err(CryptoError::Storage(_))
    ));
    #[cfg(feature = "compression")]
    assert!(matches!(
        f.alice.new_epoch_photo_input(CHAT.into(), id).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(snapshot(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceled_commands_and_retained_ciphertext_buffers_keep_admission_bounded() {
    let f = fixture().await;
    initialize(&f.alice).await;
    let before = snapshot(f.root.path(), A);
    let (reply, abandoned) = tokio::sync::oneshot::channel();
    drop(abandoned);
    let input = Input {
        expected: None,
        chat: CHAT.into(),
        message: Uuid::new_v4().to_string(),
        draft: draft(),
        plaintext: source(),
        _permit: f.alice.attachment_permit().unwrap(),
    };
    f.alice
        .sender
        .try_send(Command::Epoch(EpochCommand::Attachment(
            AttachmentCommand::Prepare(Box::new(input), reply),
        )))
        .unwrap_or_else(|_| panic!("fixture queue unavailable"));
    // A non-file command is a FIFO barrier; no sleeps or race-dependent polling.
    f.alice.epoch_scope(CHAT.into(), 0).await.unwrap();
    assert_eq!(snapshot(f.root.path(), A), before);
    let id = Uuid::new_v4().to_string();
    let bytes = f
        .alice
        .prepared_epoch_attachment_blob(prepare(&f.alice, &id).await.unwrap())
        .await
        .unwrap();
    assert!(matches!(
        f.alice.prepared_epoch_attachment(id.clone()).await,
        Err(CryptoError::Busy)
    ));
    assert!(matches!(
        prepare(&f.alice, &Uuid::new_v4().to_string()).await,
        Err(CryptoError::Busy)
    ));
    drop(bytes);
    // Metadata-only replies release admission before waking the caller. No
    // caller retries/sleeps are needed between consecutive operations.
    for _ in 0..32 {
        let p = f
            .alice
            .prepared_epoch_attachment(id.clone())
            .await
            .unwrap()
            .unwrap();
        drop(f.alice.prepared_epoch_attachment_blob(p).await.unwrap());
    }
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn attachment_prepare_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_ATTACHMENT_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_ATTACHMENT_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = Path::new(&root);
            let own = DeviceRef {
                user_id: A.into(),
                generation_id: AG.into(),
                device_id: 1,
            };
            let mut db = store(root, A, false);
            db.test_exit_on_record(key(&own, CRASH_ID).unwrap(), after);
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let _ = prepare(&w, CRASH_ID).await;
            panic!("attachment crash hook did not fire");
        });
}

#[tokio::test]
async fn process_exit_before_and_after_commit_recovers_all_or_neither_and_lost_reply() {
    for after in [false, true] {
        let f = fixture().await;
        let a = initialize(&f.alice).await;
        f.alice.shutdown().await.unwrap();
        let before = snapshot(f.root.path(), A);
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","encryption::signal_state::epoch_state::attachment_prepare::tests::attachment_prepare_crash_child","--nocapture"])
            .env("ZCHAT_ATTACHMENT_CRASH_PATH",f.root.path()).env("ZCHAT_ATTACHMENT_CRASH_AFTER",if after { "1" } else { "0" }).status().unwrap();
        assert_eq!(child.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
            .await
            .unwrap();
        let committed = snapshot(f.root.path(), A);
        if !after {
            assert_eq!(committed, before);
            assert_eq!(cohort(f.root.path(), A), (0, 0, 0));
            assert!(
                w.prepared_epoch_attachment(CRASH_ID.into())
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert_eq!(cohort(f.root.path(), A).0, 1);
        }
        let p = prepare(&w, CRASH_ID).await.unwrap();
        if after {
            assert_eq!(snapshot(f.root.path(), A), committed);
        }
        let bytes = w.prepared_epoch_attachment_blob(p).await.unwrap();
        let exact = bytes.as_bytes().to_vec();
        drop(bytes);
        w.shutdown().await.unwrap();
        verify_secret(f.root.path(), &a.own, CRASH_ID, &exact);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
            .await
            .unwrap();
        assert_eq!(
            w.prepared_epoch_attachment_blob(prepare(&w, CRASH_ID).await.unwrap())
                .await
                .unwrap()
                .as_bytes(),
            exact
        );
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
