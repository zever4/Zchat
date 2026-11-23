//! Durable allocator boundaries, using the actual encrypted SQLite owner.
use super::*;
use PreKeyIdNamespace::{Ec, Pq, Signed};
use std::path::Path;

fn low_material() -> DeviceMaterial {
    let mut m = material();
    m.signed = SignedPreKeyRecord::new(
        2.into(),
        m.signed.timestamp().unwrap(),
        &m.signed.key_pair().unwrap(),
        &m.signed.signature().unwrap(),
    );
    m.ec = (1..=2)
        .map(|id| PreKeyRecord::new(id.into(), &KeyPair::generate(&mut rand::rng())))
        .collect();
    m.pq =
        m.pq.into_iter()
            .zip([2, 0])
            .map(|((key, lr), id)| {
                (
                    KyberPreKeyRecord::new(
                        id.into(),
                        key.timestamp().unwrap(),
                        &key.key_pair().unwrap(),
                        &key.signature().unwrap(),
                    ),
                    lr,
                )
            })
            .collect();
    m
}
fn engine(root: &Path, create: bool) -> Engine {
    Engine {
        store: store(root, A, create),
        own: device(A, AG),
        runtime: tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    }
}
fn installed(root: &Path) -> Engine {
    let mut e = engine(root, true);
    e.install(low_material()).unwrap();
    e
}
fn seed(e: &mut Engine) {
    for _ in 0..100 {
        let page = e.seed_prekey_ids().unwrap();
        assert!(page.scanned <= 32);
        if page.ready {
            return;
        }
    }
    panic!("unbounded seeding");
}
fn reserve(e: &mut Engine, ns: PreKeyIdNamespace, count: u32) -> PreKeyIdReservation {
    e.reserve_prekey_ids(&Uuid::new_v4().to_string(), ns, count)
        .unwrap()
}

#[test]
fn reservations_replay_after_restart_and_never_release_or_cross_namespaces() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path());
    let op = Uuid::new_v4().to_string();
    assert!(matches!(
        e.reserve_prekey_ids(&op, Ec, 2),
        Err(CryptoError::PreKeyIdsNotReady)
    ));
    seed(&mut e);
    let reserved = e.reserve_prekey_ids(&op, Ec, 2).unwrap();
    assert_eq!((reserved.first, reserved.count), (3, 2));
    // Simulate a caller losing the successful local reply then restarting.
    drop(e);
    let mut e = engine(root.path(), false);
    assert_eq!(e.reserve_prekey_ids(&op, Ec, 2).unwrap(), reserved);
    assert!(matches!(
        e.reserve_prekey_ids(&op, Ec, 1),
        Err(CryptoError::Conflict)
    ));
    assert!(matches!(
        e.reserve_prekey_ids(&op, Pq, 2),
        Err(CryptoError::Conflict)
    ));
    assert_eq!(reserve(&mut e, Ec, 1).first, 5);
    assert_eq!(reserve(&mut e, Pq, 1).first, 3); // initial LR0 shares PQ namespace
    assert_eq!(reserve(&mut e, Signed, 1).first, 3);
    assert!(e.seed_prekey_ids().unwrap().ready); // no reseed/reset
    assert_eq!(reserve(&mut e, Ec, 1).first, 6);
    for count in [0, 101, u32::MAX] {
        assert!(matches!(
            e.reserve_prekey_ids(&Uuid::new_v4().to_string(), Ec, count),
            Err(CryptoError::Invalid)
        ));
    }
    assert!(e.reserve_prekey_ids("not-a-uuid", Ec, 1).is_err());
}

#[test]
fn seed_is_paged_restartable_numeric_and_includes_used_and_tombstoned_ids() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path());
    e.store
        .transaction(|tx| {
            for id in 3..=45u32 {
                let record = PreKeyRecord::new(id.into(), &KeyPair::generate(&mut rand::rng()))
                    .serialize()
                    .unwrap();
                let bytes = if id == 45 {
                    prekey_state::tombstone(RecordKind::EcPreKey, id, &record)
                } else {
                    [&[1, 1, 0], record.as_slice()].concat()
                };
                tx.put(
                    &RecordKey::new(AG, RecordKind::EcPreKey, &id.to_string())?,
                    &bytes,
                )?;
            }
            Ok(())
        })
        .unwrap();
    let page = e.seed_prekey_ids().unwrap();
    assert_eq!(page.scanned, 32);
    assert!(!page.ready);
    drop(e);
    let mut e = engine(root.path(), false);
    assert_eq!(e.seed_prekey_ids().unwrap().scanned, 13);
    seed(&mut e);
    assert_eq!(reserve(&mut e, Ec, 1).first, 46); // lexical max would be 9
    assert_eq!(reserve(&mut e, Pq, 1).first, 3);
    // Redaction/consumption doesn't reset the already advanced counter.
    e.redact_consumed_prekeys(None).unwrap();
    assert_eq!(reserve(&mut e, Ec, 1).first, 47);
}

#[test]
fn new_installs_advance_seed_cursor_but_cannot_fill_burned_ids_or_gaps() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path());
    e.seed_prekey_ids().unwrap(); // EC already scanned; PQ/signed still pending
    let fresh = PreKeyRecord::new(1000.into(), &KeyPair::generate(&mut rand::rng()))
        .serialize()
        .unwrap();
    e.store
        .transaction_with::<_, CryptoError>(|tx| {
            prekey_state::save(tx, AG, RecordKind::EcPreKey, 1000, &fresh, None)
        })
        .unwrap();
    seed(&mut e);
    assert_eq!(reserve(&mut e, Ec, 2).first, 1001);
    for id in [0, 999, 1001] {
        let record = PreKeyRecord::new(id.into(), &KeyPair::generate(&mut rand::rng()))
            .serialize()
            .unwrap();
        assert!(matches!(
            e.store.transaction_with(|tx| prekey_state::save(
                tx,
                AG,
                RecordKind::EcPreKey,
                id,
                &record,
                None
            )),
            Err(CryptoError::Conflict)
        ));
    }
    // Existing exact material is still replayable, despite the higher counter.
    e.store
        .transaction_with::<_, CryptoError>(|tx| {
            prekey_state::save(tx, AG, RecordKind::EcPreKey, 1000, &fresh, None)
        })
        .unwrap();
    assert_eq!(reserve(&mut e, Ec, 1).first, 1003);
}

#[test]
fn uint32_exhaustion_is_atomic_and_exact_replay_survives_exhaustion() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    e.install(material()).unwrap(); // LR = MAX, signed = MAX-2
    seed(&mut e);
    assert!(matches!(
        e.reserve_prekey_ids(&Uuid::new_v4().to_string(), Pq, 1),
        Err(CryptoError::PreKeyIdsExhausted)
    ));
    assert!(matches!(
        e.reserve_prekey_ids(&Uuid::new_v4().to_string(), Signed, 3),
        Err(CryptoError::PreKeyIdsExhausted)
    ));
    let last = reserve(&mut e, Signed, 2);
    assert_eq!(last.first, u32::MAX - 1);
    drop(e);
    let mut e = engine(root.path(), false);
    assert_eq!(
        e.reserve_prekey_ids(&last.operation_id, Signed, 2).unwrap(),
        last
    );
    assert!(matches!(
        e.reserve_prekey_ids(&Uuid::new_v4().to_string(), Signed, 1),
        Err(CryptoError::PreKeyIdsExhausted)
    ));
    assert_eq!(reserve(&mut e, Ec, 1).first, EC + 1);
}

#[test]
fn failed_commit_preserves_counter_and_journal_as_one_unit() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path());
    seed(&mut e);
    drop(e);
    let op = Uuid::new_v4().to_string();
    commit_fault(root.path(), A, RecordKind::Control as u8, true);
    let mut e = engine(root.path(), false);
    assert!(matches!(
        e.reserve_prekey_ids(&op, Ec, 3),
        Err(CryptoError::Storage(_))
    ));
    drop(e);
    commit_fault(root.path(), A, RecordKind::Control as u8, false);
    let mut e = engine(root.path(), false);
    assert_eq!(e.reserve_prekey_ids(&op, Ec, 3).unwrap().first, 3);
    assert_eq!(reserve(&mut e, Ec, 1).first, 6);
}

#[test]
fn failed_initial_seed_commit_leaves_neither_anchor_nor_counter() {
    let root = tempfile::tempdir().unwrap();
    drop(installed(root.path()));
    commit_fault(root.path(), A, RecordKind::Control as u8, true);
    let mut e = engine(root.path(), false);
    assert!(matches!(e.seed_prekey_ids(), Err(CryptoError::Storage(_))));
    e.store
        .transaction(|tx| {
            for id in ["prekey-ids:state:v1", "prekey-ids:anchor:v1"] {
                assert!(
                    tx.get(&RecordKey::new(AG, RecordKind::Control, id)?)?
                        .is_none()
                );
            }
            Ok(())
        })
        .unwrap();
    drop(e);
    commit_fault(root.path(), A, RecordKind::Control as u8, false);
    let mut e = engine(root.path(), false);
    assert_eq!(e.seed_prekey_ids().unwrap().scanned, 2);
    seed(&mut e);
    assert_eq!(reserve(&mut e, Ec, 1).first, 3);
}

#[test]
fn malformed_record_rolls_back_seed_page_without_resetting_or_skipping_it() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path());
    let bad = RecordKey::new(AG, RecordKind::EcPreKey, "02").unwrap();
    e.store.transaction(|tx| tx.put(&bad, &[2, 1, 0])).unwrap();
    assert!(e.seed_prekey_ids().is_err());
    assert!(e.seed_prekey_ids().is_err());
    let op = Uuid::new_v4().to_string();
    assert!(matches!(
        e.reserve_prekey_ids(&op, Ec, 1),
        Err(CryptoError::PreKeyIdsNotReady)
    ));
    e.store
        .transaction(|tx| {
            assert!(
                tx.get(&RecordKey::new(
                    AG,
                    RecordKind::Control,
                    "prekey-ids:state:v1"
                )?)?
                .is_none()
            );
            assert!(
                tx.get(&RecordKey::new(
                    AG,
                    RecordKind::Control,
                    "prekey-ids:anchor:v1"
                )?)?
                .is_none()
            );
            Ok(())
        })
        .unwrap();
}

#[test]
fn damaged_counter_and_partial_deletion_fail_closed_without_reseeding() {
    for damage in 0..4 {
        let root = tempfile::tempdir().unwrap();
        let mut e = installed(root.path());
        seed(&mut e);
        reserve(&mut e, Ec, 3);
        if damage == 0 {
            e.store
                .transaction(|tx| {
                    tx.put(
                        &RecordKey::new(AG, RecordKind::Control, "prekey-ids:state:v1")?,
                        &[255],
                    )
                })
                .unwrap();
        } else {
            drop(e);
            let path = StoreScope::new(SERVER, A)
                .unwrap()
                .database_path(root.path());
            let db = rusqlite::Connection::open(path).unwrap();
            let ids: &[&str] = match damage {
                1 => &["prekey-ids:state:v1"],
                2 => &["prekey-ids:anchor:v1"],
                _ => &["prekey-ids:state:v1", "prekey-ids:anchor:v1"],
            };
            for id in ids {
                db.execute(
                    "DELETE FROM protected_records WHERE kind=13 AND record_id=?1",
                    [id],
                )
                .unwrap();
            }
            drop(db);
            e = engine(root.path(), false);
        }
        assert!(e.seed_prekey_ids().is_err());
        assert!(
            e.reserve_prekey_ids(&Uuid::new_v4().to_string(), Ec, 1)
                .is_err()
        );
    }
}

#[tokio::test]
async fn real_signal_consumption_does_not_release_prekey_ids_after_seed_and_reopen() {
    let p = pair(true, false).await;
    for worker in [&p.alice, &p.bob] {
        while !worker.seed_prekey_ids().await.unwrap().ready {}
    }
    let before = p
        .bob
        .reserve_prekey_ids(Uuid::new_v4().to_string(), Ec, 1)
        .await
        .unwrap();
    assert_eq!(before.first, EC + 1);
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(
        &p.a,
        "allocator state survives actual Signal prekey consumption",
    );
    let wire = p.alice.encrypt(p.a, text.clone()).await.unwrap();
    assert_eq!(p.bob.decrypt(p.b.clone(), wire).await.unwrap(), text);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string())
            .unwrap()
            .len(),
        35
    );
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::PqPreKey, &PQ.to_string())
            .unwrap()
            .len(),
        35
    );
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), p.b.own)
        .await
        .unwrap();
    assert_eq!(
        bob.reserve_prekey_ids(before.operation_id.clone(), Ec, 1)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        bob.reserve_prekey_ids(Uuid::new_v4().to_string(), Ec, 1)
            .await
            .unwrap()
            .first,
        EC + 2
    );
    bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_owner_requests_share_one_reservation_and_generations_are_isolated() {
    let root = tempfile::tempdir().unwrap();
    // Create/drop the synchronous runtime outside this Tokio runtime.
    let path = root.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut e = installed(&path);
        seed(&mut e);
    })
    .await
    .unwrap();
    let worker = CryptoCoordinator::start(store(root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let op = Uuid::new_v4().to_string();
    let (a, b) = tokio::join!(
        worker.reserve_prekey_ids(op.clone(), Ec, 2),
        worker.reserve_prekey_ids(op.clone(), Ec, 2)
    );
    assert_eq!(a.unwrap(), b.unwrap());
    worker.shutdown().await.unwrap();
    let other = CryptoCoordinator::start(store(root.path(), A, false), device(A, BG))
        .await
        .unwrap();
    assert!(other.seed_prekey_ids().await.is_err()); // no silent empty generation
    other.install_prepared(low_material()).await.unwrap();
    while !other.seed_prekey_ids().await.unwrap().ready {}
    assert_eq!(
        other
            .reserve_prekey_ids(op.clone(), Ec, 2)
            .await
            .unwrap()
            .first,
        3
    );
    other.shutdown().await.unwrap();
    let worker = CryptoCoordinator::start(store(root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert_eq!(
        worker
            .reserve_prekey_ids(Uuid::new_v4().to_string(), Ec, 1)
            .await
            .unwrap()
            .first,
        5
    );
    worker.shutdown().await.unwrap();
    for (origin, account) in [(SERVER, B), ("https://other.example.test", A)] {
        let fresh = test_open(root.path(), StoreScope::new(origin, account).unwrap(), true);
        let other = CryptoCoordinator::start(fresh, device(account, AG))
            .await
            .unwrap();
        other.install_prepared(low_material()).await.unwrap();
        while !other.seed_prekey_ids().await.unwrap().ready {}
        assert_eq!(
            other
                .reserve_prekey_ids(op.clone(), Ec, 2)
                .await
                .unwrap()
                .first,
            3
        );
        other.shutdown().await.unwrap();
    }
}

#[test]
fn process_exit_preserves_none_or_the_whole_reservation_before_and_after_commit() {
    for (after, exit) in [(false, 73), (true, 74)] {
        let root = tempfile::tempdir().unwrap();
        let mut e = installed(root.path());
        seed(&mut e);
        drop(e);
        let op = Uuid::new_v4().to_string();
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::tests::key_ids::allocator_exit_child",
                "--nocapture",
            ])
            .env("ZCHAT_KEY_IDS_CHILD_ROOT", root.path())
            .env("ZCHAT_KEY_IDS_CHILD_OPERATION", &op)
            .env("ZCHAT_KEY_IDS_CHILD_AFTER", if after { "1" } else { "0" })
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(exit));
        let mut e = engine(root.path(), false);
        let next = reserve(&mut e, Ec, 1);
        assert_eq!(next.first, if after { 6 } else { 3 });
        let retry = e.reserve_prekey_ids(&op, Ec, 3).unwrap();
        assert_eq!(retry.first, if after { 3 } else { 4 });
    }
}

#[test]
fn process_exit_keeps_seed_progress_and_anchor_atomic() {
    for (after, exit) in [(false, 73), (true, 74)] {
        let root = tempfile::tempdir().unwrap();
        drop(installed(root.path()));
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::tests::key_ids::allocator_exit_child",
                "--nocapture",
            ])
            .env("ZCHAT_KEY_IDS_CHILD_ROOT", root.path())
            .env("ZCHAT_KEY_IDS_CHILD_SEED", "1")
            .env("ZCHAT_KEY_IDS_CHILD_AFTER", if after { "1" } else { "0" })
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(exit));
        let mut e = engine(root.path(), false);
        e.store
            .transaction(|tx| {
                for id in ["prekey-ids:state:v1", "prekey-ids:anchor:v1"] {
                    assert_eq!(
                        tx.get(&RecordKey::new(AG, RecordKind::Control, id)?)?
                            .is_some(),
                        after
                    );
                }
                Ok(())
            })
            .unwrap();
        seed(&mut e);
        assert_eq!(reserve(&mut e, Ec, 1).first, 3);
    }
}

#[test]
fn allocator_exit_child() {
    let Some(root) = std::env::var_os("ZCHAT_KEY_IDS_CHILD_ROOT") else {
        return;
    };
    let after = std::env::var("ZCHAT_KEY_IDS_CHILD_AFTER").unwrap() == "1";
    let mut e = engine(Path::new(&root), false);
    if std::env::var_os("ZCHAT_KEY_IDS_CHILD_SEED").is_some() {
        e.store.test_exit_on_record(
            RecordKey::new(AG, RecordKind::Control, "prekey-ids:state:v1").unwrap(),
            after,
        );
        let _ = e.seed_prekey_ids();
        panic!("seed exit hook was not reached");
    }
    let op = std::env::var("ZCHAT_KEY_IDS_CHILD_OPERATION").unwrap();
    e.store.test_exit_on_record(
        RecordKey::new(
            AG,
            RecordKind::Control,
            &format!("prekey-ids:reservation:{op}"),
        )
        .unwrap(),
        after,
    );
    let _ = e.reserve_prekey_ids(&op, Ec, 3);
    panic!("process exit hook was not reached");
}
