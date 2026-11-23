use super::super::tests::{acquired, material, public, store};
use super::*;
use crate::proto::e2ee::ConversationCloseReason;
use rusqlite::Connection;
use std::path::Path;
use uuid::Uuid;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const AG: &str = "33333333-3333-4333-8333-333333333333";
const BG: &str = "44444444-4444-4444-8444-444444444444";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
const SERVER: &str = "https://chat.example.test";
pub(super) struct Fixture {
    pub(super) root: tempfile::TempDir,
    pub(super) alice: CryptoCoordinator,
    pub(super) bob: CryptoCoordinator,
    pub(super) a: Conversation,
    pub(super) bundle: AcquiredBundle,
    pub(super) next_bundle: AcquiredBundle,
    pub(super) reverse_bundle: AcquiredBundle,
}
pub(super) async fn index(w: &CryptoCoordinator, c: &Conversation) {
    let q = w
        .prepare_conversation_lookup(c.conversation_id.clone())
        .await
        .unwrap();
    w.accept_conversation_lookup(VerifiedConversationLookup::from_rpc(q, c.clone()))
        .await
        .unwrap();
}
pub(super) async fn fixture() -> Fixture {
    fixture_at(SERVER).await
}
pub(super) async fn fixture_at(origin: &str) -> Fixture {
    let root = tempfile::tempdir().unwrap();
    let am = material();
    let bm = material();
    let own = |user: &str, generation: &str| DeviceRef {
        user_id: user.into(),
        generation_id: generation.into(),
        device_id: 1,
    };
    let ap = public(own(A, AG), &am);
    let bp = public(own(B, BG), &bm);
    let bundle = acquired(bp.clone(), &bm, true, false);
    let next_bundle = acquired(bp.clone(), &bm, false, true);
    let reverse_bundle = acquired(ap.clone(), &am, true, false);
    let a = Conversation {
        conversation_id: CHAT.into(),
        mode: 1,
        own_device: ap.reference.clone(),
        peer_device: Some(bp.clone()),
        state: ConversationState::Active as i32,
        ..Default::default()
    };
    let b = Conversation {
        conversation_id: CHAT.into(),
        mode: 1,
        own_device: bp.reference.clone(),
        peer_device: Some(ap),
        state: ConversationState::Active as i32,
        ..Default::default()
    };
    let open = |user| {
        crate::encryption::protected::test_open(
            root.path(),
            StoreScope::new(origin, user).unwrap(),
            true,
        )
    };
    let alice = CryptoCoordinator::start(open(A), a.own_device.clone().unwrap())
        .await
        .unwrap();
    let bob = CryptoCoordinator::start(open(B), b.own_device.clone().unwrap())
        .await
        .unwrap();
    alice.install_prepared(am).await.unwrap();
    bob.install_prepared(bm).await.unwrap();
    index(&alice, &a).await;
    index(&bob, &b).await;
    Fixture {
        root,
        alice,
        bob,
        a,
        bundle,
        next_bundle,
        reverse_bundle,
    }
}
fn response(q: &EpochCatalogQuery) -> v2::ConversationEpochs {
    v2::ConversationEpochs {
        protocol_version: 2,
        conversation: Some(q.pins.conversation().clone()),
        active_epoch: Some(0),
        epoch_revision: Some(0),
        epochs: vec![v2::EpochRecord {
            epoch: Some(0),
            revision: Some(0),
            created_at_unix_ms: 1_700_000_000_000,
            ..Default::default()
        }],
        next_after_epoch: None,
    }
}
fn observation(
    q: &EpochCatalogQuery,
) -> EpochObservation<EpochCatalogQuery, v2::ConversationEpochs> {
    EpochObservation::test_observation(q.scope.clone(), q.clone(), response(q))
}
pub(super) async fn initialize(w: &CryptoCoordinator) -> EpochScope {
    let q = w.prepare_initial_epoch(CHAT.into()).await.unwrap();
    w.accept_initial_epoch(observation(&q)).await.unwrap()
}
fn path(root: &Path, user: &str) -> std::path::PathBuf {
    StoreScope::new(SERVER, user).unwrap().database_path(root)
}
pub(super) fn records(root: &Path, user: &str) -> Vec<(i64, String, Vec<u8>)> {
    let db = Connection::open(path(root, user)).unwrap();
    let mut s = db
        .prepare("SELECT kind,record_id,payload FROM protected_records ORDER BY kind,record_id")
        .unwrap();
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
pub(super) fn wrapped(root: &Path, user: &str) -> Vec<u8> {
    Connection::open(path(root, user))
        .unwrap()
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap()
}
fn protected_count(root: &Path, user: &str) -> i64 {
    Connection::open(path(root, user))
        .unwrap()
        .query_row(
            "SELECT count(*)FROM protected_records WHERE record_id LIKE 'epoch-v2:%'",
            [],
            |r| r.get(0),
        )
        .unwrap()
}

#[tokio::test]
async fn initial_epoch_commit_reopen_and_exact_refresh_preserve_private_material() {
    let f = fixture().await;
    let before = records(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    assert!(matches!(
        f.alice.epoch_scope(CHAT.into(), 0).await,
        Err(CryptoError::ConversationUnavailable)
    ));
    let q = f.alice.prepare_initial_epoch(CHAT.into()).await.unwrap();
    assert_eq!(protected_count(f.root.path(), A), 0);
    assert_eq!(records(f.root.path(), A), before);
    let scope = f.alice.accept_initial_epoch(observation(&q)).await.unwrap();
    assert_eq!(scope.crypto_epoch(), 0);
    assert_eq!(scope.conversation_id(), CHAT);
    assert!(!scope.receive_only() && !scope.sending_frozen());
    assert_eq!(protected_count(f.root.path(), A), 5);
    let after = records(f.root.path(), A);
    for row in before {
        assert!(after.contains(&row));
    }
    assert_eq!(wrapped(f.root.path(), A), master);
    assert!(matches!(
        f.alice.accept_initial_epoch(observation(&q)).await,
        Err(CryptoError::Conflict)
    )); // Lost local response: point-read committed state, never reinitialize.
    let q = f.alice.prepare_initial_epoch(CHAT.into()).await.unwrap();
    let again = f.alice.accept_initial_epoch(observation(&q)).await.unwrap();
    assert_eq!(scope.session_record_id(), again.session_record_id());
    assert_eq!(records(f.root.path(), A), after);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    let restored = w.epoch_scope(CHAT.into(), 0).await.unwrap();
    assert_eq!(restored.session_record_id(), scope.session_record_id());
    assert_eq!(
        restored.acquisition_record_id(),
        scope.acquisition_record_id()
    );
    assert!(restored.store_scope() == scope.store_scope());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn initial_query_and_observation_are_account_server_and_generation_bound() {
    let f = fixture().await;
    let q = f.alice.prepare_initial_epoch(CHAT.into()).await.unwrap();
    assert!(matches!(
        f.bob.accept_initial_epoch(observation(&q)).await,
        Err(CryptoError::Conflict)
    ));
    let foreign = StoreScope::new("https://other.example.test", A).unwrap();
    let fake = EpochObservation::test_observation(foreign, q.clone(), response(&q));
    assert!(matches!(
        f.alice.accept_initial_epoch(fake).await,
        Err(CryptoError::Conflict)
    ));
    let mut wrong = q.clone();
    wrong.own.generation_id = Uuid::new_v4().to_string();
    assert!(matches!(
        f.alice.accept_initial_epoch(observation(&wrong)).await,
        Err(CryptoError::Conflict)
    ));
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    assert_ne!(a.session_record_id(), b.session_record_id());
    assert!(a.store_scope() != b.store_scope());
    assert_eq!(protected_count(f.root.path(), A), 5);
    assert_eq!(protected_count(f.root.path(), B), 5);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn higher_head_and_changed_initial_origin_never_activate_or_reset_local_epoch() {
    let f = fixture().await;
    let q = f.alice.prepare_initial_epoch(CHAT.into()).await.unwrap();
    let mut higher = response(&q);
    higher.active_epoch = Some(1);
    higher.epoch_revision = Some(1);
    higher.epochs[0].sealed = true;
    higher.epochs[0].sealed_at_unix_ms = Some(1_700_000_000_100);
    higher.epochs[0].sealed_by_proposal_id = Uuid::new_v4().to_string();
    higher.next_after_epoch = Some(0);
    let obs = EpochObservation::test_observation(q.scope.clone(), q.clone(), higher.clone());
    assert!(matches!(
        f.alice.accept_initial_epoch(obs).await,
        Err(CryptoError::EpochRecoveryRequired)
    ));
    assert_eq!(protected_count(f.root.path(), A), 0);
    initialize(&f.alice).await;
    let before = records(f.root.path(), A);
    let q = f.alice.prepare_initial_epoch(CHAT.into()).await.unwrap();
    let obs = EpochObservation::test_observation(q.scope.clone(), q.clone(), higher);
    assert!(matches!(
        f.alice.accept_initial_epoch(obs).await,
        Err(CryptoError::EpochRecoveryRequired)
    ));
    let mut changed = response(&q);
    changed.epochs[0].created_at_unix_ms += 1;
    let obs = EpochObservation::test_observation(q.scope.clone(), q.clone(), changed);
    assert!(matches!(
        f.alice.accept_initial_epoch(obs).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert!(matches!(
        f.alice.epoch_scope(CHAT.into(), 1).await,
        Err(CryptoError::EpochRecoveryRequired)
    ));
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_commit_failure_releases_no_scope_and_rolls_back_all_initial_records() {
    let f = fixture().await;
    let before = records(f.root.path(), A);
    let q = f.alice.prepare_initial_epoch(CHAT.into()).await.unwrap();
    let db = Connection::open(path(f.root.path(), A)).unwrap();
    db.execute_batch(&format!("CREATE TABLE epoch_fault_parent(id INTEGER PRIMARY KEY);CREATE TABLE epoch_fault_child(id INTEGER REFERENCES epoch_fault_parent(id)DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER epoch_fault AFTER INSERT ON protected_records WHEN NEW.record_id='epoch-v2:{CHAT}:head' BEGIN INSERT INTO epoch_fault_child VALUES(1);END;")).unwrap();
    assert!(matches!(
        f.alice.accept_initial_epoch(observation(&q)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(protected_count(f.root.path(), A), 0);
    assert_eq!(records(f.root.path(), A), before);
    db.execute_batch(
        "DROP TRIGGER epoch_fault;DROP TABLE epoch_fault_child;DROP TABLE epoch_fault_parent;",
    )
    .unwrap();
    f.alice.accept_initial_epoch(observation(&q)).await.unwrap();
    assert_eq!(protected_count(f.root.path(), A), 5);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_or_rolled_back_domain_records_fail_closed_and_are_not_recreated() {
    for suffix in [
        "head",
        "anchor",
        "row:0000000000000000",
        "all-root-retain-slot",
    ] {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        f.alice.shutdown().await.unwrap();
        let mut db = store(f.root.path(), A, false);
        if suffix == "all-root-retain-slot" {
            db.transaction(|tx| {
                tx.put(
                    &RecordKey::new(AG, RecordKind::Session, s.session_record_id())?,
                    b"retained v2 slot evidence",
                )
            })
            .unwrap();
        }
        drop(db);
        let sql = Connection::open(path(f.root.path(), A)).unwrap();
        sql.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
        if suffix == "all-root-retain-slot" {
            sql.execute(
                "DELETE FROM protected_records WHERE record_id LIKE ?1",
                [format!("epoch-v2:{CHAT}:%")],
            )
            .unwrap();
        } else {
            sql.execute(
                "DELETE FROM protected_records WHERE record_id=?1",
                [format!("epoch-v2:{CHAT}:{suffix}")],
            )
            .unwrap();
        }
        drop(sql);
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(
            store(f.root.path(), A, false),
            f.a.own_device.clone().unwrap(),
        )
        .await
        .unwrap();
        assert!(w.prepare_initial_epoch(CHAT.into()).await.is_err());
        assert!(w.epoch_scope(CHAT.into(), 0).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
    let f = fixture().await;
    initialize(&f.alice).await;
    f.alice.shutdown().await.unwrap();
    let mut db = store(f.root.path(), A, false);
    // Authenticated logical corruption, not just a broken AEAD tag: an old row
    // with a mismatched anchored digest must also be rejected.
    db.transaction(|tx| {
        let k = row_key(f.a.own_device.as_ref().unwrap(), CHAT).unwrap();
        let mut row = EpochRow::decode(tx.get(&k)?.unwrap().as_slice()).unwrap();
        row.epoch.as_mut().unwrap().created_at_unix_ms += 1;
        tx.put(&k, &row.encode_to_vec())
    })
    .unwrap();
    drop(db);
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    assert!(w.epoch_scope(CHAT.into(), 0).await.is_err());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn prototype_state_cannot_be_adopted_and_v2_catalogue_blocks_old_data_entrypoints() {
    let f = fixture().await;
    let legacy = SessionScope::from_conversation(&f.a).unwrap();
    f.alice.prepare_acquisition(legacy).await.unwrap();
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice.prepare_initial_epoch(CHAT.into()).await,
        Err(CryptoError::DataProtocolConflict)
    ));
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let f = fixture().await;
    initialize(&f.alice).await;
    let legacy = SessionScope::from_conversation(&f.a).unwrap();
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice.prepare_acquisition(legacy.clone()).await,
        Err(CryptoError::DataProtocolConflict)
    ));
    assert!(matches!(
        f.alice.bootstrap(legacy, f.bundle).await,
        Err(CryptoError::DataProtocolConflict)
    ));
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

pub(super) fn rewrite_frozen(root: &Path, c: &Conversation, frozen: bool) {
    let mut db = store(root, &c.own_device.as_ref().unwrap().user_id, false);
    db.transaction(|tx| {
        let own = c.own_device.as_ref().unwrap();
        let k = key(own, &c.conversation_id, "head").unwrap();
        let mut value = Root::decode(tx.get(&k)?.unwrap().as_slice()).unwrap();
        value.send_frozen = Some(frozen);
        value.local_revision += 1;
        let bytes = value.encode_to_vec();
        let anchor = Anchor {
            format: FORMAT,
            local_revision: value.local_revision,
            root_digest: digest(b"zchat.e2ee.epoch-root.v2\0", &bytes).to_vec(),
        };
        tx.put(&k, &bytes)?;
        tx.put(
            &key(own, &c.conversation_id, "anchor").unwrap(),
            &anchor.encode_to_vec(),
        )
    })
    .unwrap();
}

#[tokio::test]
async fn authenticated_root_replay_and_missing_required_flags_fail_closed() {
    let f = fixture().await;
    initialize(&f.alice).await;
    f.alice.shutdown().await.unwrap();
    let id = format!("epoch-v2:{CHAT}:head");
    let old: Vec<u8> = Connection::open(path(f.root.path(), A))
        .unwrap()
        .query_row(
            "SELECT payload FROM protected_records WHERE record_id=?1",
            [&id],
            |r| r.get(0),
        )
        .unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    // Replay a valid old AEAD frame at its original key; the newer anchor must
    // reject it. This does not claim protection against rollback of ALL state.
    Connection::open(path(f.root.path(), A))
        .unwrap()
        .execute(
            "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
            rusqlite::params![old, id],
        )
        .unwrap();
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        w.epoch_scope(CHAT.into(), 0).await,
        Err(CryptoError::Conflict)
    ));
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let f = fixture().await;
    initialize(&f.alice).await;
    f.alice.shutdown().await.unwrap();
    let mut db = store(f.root.path(), A, false);
    db.transaction(|tx| {
        let own = f.a.own_device.as_ref().unwrap();
        let k = key(own, CHAT, "head").unwrap();
        let mut value = Root::decode(tx.get(&k)?.unwrap().as_slice()).unwrap();
        value.send_frozen = None;
        let bytes = value.encode_to_vec();
        let anchor = Anchor {
            format: FORMAT,
            local_revision: value.local_revision,
            root_digest: digest(b"zchat.e2ee.epoch-root.v2\0", &bytes).to_vec(),
        };
        tx.put(&k, &bytes)?;
        tx.put(&key(own, CHAT, "anchor").unwrap(), &anchor.encode_to_vec())
    })
    .unwrap();
    drop(db);
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        w.epoch_scope(CHAT.into(), 0).await,
        Err(CryptoError::Conflict)
    ));
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn cas_and_metadata_refresh_cannot_clear_a_protected_freeze_or_reopen_a_closed_chat() {
    let f = fixture().await;
    let old = initialize(&f.alice).await;
    let q = f.alice.prepare_initial_epoch(CHAT.into()).await.unwrap();
    f.alice.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    let w = CryptoCoordinator::start(
        store(f.root.path(), A, false),
        f.a.own_device.clone().unwrap(),
    )
    .await
    .unwrap();
    assert!(matches!(
        w.accept_initial_epoch(observation(&q)).await,
        Err(CryptoError::Conflict)
    ));
    let q = w.prepare_initial_epoch(CHAT.into()).await.unwrap();
    assert!(
        w.accept_initial_epoch(observation(&q))
            .await
            .unwrap()
            .sending_frozen()
    );
    let pending = w.prepare_initial_epoch(CHAT.into()).await.unwrap();
    let mut closed = f.a.clone();
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_generation_id = BG.into();
    closed.closed_at_unix_ms = 1_700_000_000_200;
    index(&w, &closed).await;
    let s = w.accept_initial_epoch(observation(&pending)).await.unwrap();
    assert!(s.receive_only() && s.sending_frozen());
    assert_eq!(s.session_record_id(), old.session_record_id());
    index(&w, &f.a).await;
    assert!(w.epoch_scope(CHAT.into(), 0).await.unwrap().receive_only());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn v2_session_and_acquisition_keys_bind_every_epoch_including_zero_without_v1_aliases() {
    let own = DeviceRef {
        user_id: A.into(),
        generation_id: AG.into(),
        device_id: 1,
    };
    let peer = DeviceRef {
        user_id: B.into(),
        generation_id: BG.into(),
        device_id: 1,
    };
    let session = |e| slot_id(b"zchat.e2ee.signal-session.v2\0", CHAT, e, &own, &peer).unwrap();
    let acquisition =
        |e| slot_id(b"zchat.e2ee.acquisition-journal.v2\0", CHAT, e, &own, &peer).unwrap();
    assert_ne!(session(0), session(1));
    assert_ne!(session(0), acquisition(0));
    // Independently framed SHA256 vectors (including a value beyond JS integer
    // precision); no v1 or epoch0 branch is allowed in this storage namespace.
    for (epoch, expected) in [
        (
            0,
            "e86c1428b5ddffdfa8c77f04723f8ff1cb1bdd01e682db117805f4ca4355c03f",
        ),
        (
            1,
            "3c0f3273feabace63acefab604d57d8411f6c8dada2b5d8b8b0ef99b590c8eeb",
        ),
        (
            (1 << 53) + 17,
            "8e8102e2de3c69dc9b64125bf2b8e47ac5de3bd6b7cd4a480d40888e196fa52e",
        ),
    ] {
        assert!(session(epoch).ends_with(expected));
    }
    assert!(
        acquisition(0)
            .ends_with("eea98d207e06700041656fd8864c758e1d1a2609b762ff22504d31348c29be93")
    );
    assert_ne!(session(1), session((1 << 53) + 17));
    assert!(session(0).len() <= 128);
    assert!(
        slot_id(
            b"zchat.e2ee.signal-session.v2\0",
            CHAT,
            u64::MAX,
            &own,
            &peer
        )
        .is_err()
    );
    let mut changed = peer.clone();
    changed.generation_id = Uuid::new_v4().to_string();
    assert_ne!(
        session(0),
        slot_id(b"zchat.e2ee.signal-session.v2\0", CHAT, 0, &own, &changed).unwrap()
    );
    let other = Uuid::new_v4().to_string();
    assert_ne!(
        session(0),
        slot_id(b"zchat.e2ee.signal-session.v2\0", &other, 0, &own, &peer).unwrap()
    );
    assert!(
        RecordKey::new(AG, RecordKind::Control, &acquisition(0)).unwrap()
            != acquisition_key_for(&own, CHAT, &peer).unwrap()
    );
}

#[test]
fn epoch_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_EPOCH_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_EPOCH_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let mut db = store(Path::new(&root), A, false);
            let own = DeviceRef {
                user_id: A.into(),
                generation_id: AG.into(),
                device_id: 1,
            };
            db.test_exit_on_record(key(&own, CHAT, "head").unwrap(), after);
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let q = w.prepare_initial_epoch(CHAT.into()).await.unwrap();
            let _ = w.accept_initial_epoch(observation(&q)).await;
            panic!("process exit hook did not fire");
        });
}
#[tokio::test]
async fn real_process_exit_before_and_after_sqlite_commit_recovers_only_committed_epoch() {
    for after in [false, true] {
        let f = fixture().await;
        f.alice.shutdown().await.unwrap();
        let before = records(f.root.path(), A);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::epoch_state::tests::epoch_crash_child",
                "--nocapture",
            ])
            .env("ZCHAT_EPOCH_CRASH_PATH", f.root.path())
            .env("ZCHAT_EPOCH_CRASH_AFTER", if after { "1" } else { "0" })
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(
            store(f.root.path(), A, false),
            f.a.own_device.clone().unwrap(),
        )
        .await
        .unwrap();
        if after {
            assert_eq!(
                w.epoch_scope(CHAT.into(), 0).await.unwrap().crypto_epoch(),
                0
            );
            assert_eq!(protected_count(f.root.path(), A), 5);
        } else {
            assert!(matches!(
                w.epoch_scope(CHAT.into(), 0).await,
                Err(CryptoError::ConversationUnavailable)
            ));
            assert_eq!(records(f.root.path(), A), before);
            initialize(&w).await;
        }
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
