use super::super::super::tests::store;
use super::super::tests::{fixture, index, initialize, records, rewrite_frozen, wrapped};
use super::*;
use crate::proto::e2ee::ConversationCloseReason;
use rusqlite::Connection;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";

// Fixture adapter only. Production v2 never converts a v1 acquired DTO.
pub(super) fn bundle(p: &EpochAcquisitionPlan, b: &AcquiredBundle) -> v2::AcquiredBundle {
    v2::AcquiredBundle {
        protocol_version: 2,
        conversation_id: p.request.conversation_id.clone(),
        acquisition_id: p.request.acquisition_id.clone(),
        device: b.device.clone(),
        signed_pre_key: b.signed_pre_key.clone(),
        one_time_pre_key: b.one_time_pre_key.clone(),
        kyber_pre_key: b.kyber_pre_key.clone(),
        not_after_unix_ms: b.not_after_unix_ms,
        crypto_epoch: p.request.crypto_epoch,
    }
}
pub(super) fn observed(
    p: &EpochAcquisitionPlan,
    b: &v2::AcquiredBundle,
) -> EpochObservation<EpochAcquisitionPlan, v2::AcquiredBundle> {
    EpochObservation::test_observation(p.scope.scope.clone(), p.clone(), b.clone())
}
fn path(root: &std::path::Path) -> std::path::PathBuf {
    StoreScope::new("https://chat.example.test", A)
        .unwrap()
        .database_path(root)
}
#[tokio::test]
async fn durable_nonce_bootstrap_reopen_and_exact_replay_preserve_keys_and_ratchet() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let before = records(f.root.path(), A);
    let master = wrapped(f.root.path(), A);
    let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    assert!(!p.applied());
    let prepared = records(f.root.path(), A);
    assert_eq!(prepared.len(), before.len() + 2);
    assert_eq!(
        f.alice
            .prepare_epoch_acquisition(s.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    assert_eq!(records(f.root.path(), A), prepared);
    let b = bundle(&p, &f.bundle);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert_eq!(
        w.prepare_epoch_acquisition(s.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    w.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    let committed = records(f.root.path(), A);
    assert!(
        committed
            .iter()
            .any(|r| r.0 == RecordKind::Session as i64 && r.1 == s.session_id)
    );
    for r in before {
        assert!(committed.contains(&r));
    }
    assert_eq!(wrapped(f.root.path(), A), master);
    assert!(
        w.prepare_epoch_acquisition(s.clone())
            .await
            .unwrap()
            .applied()
    );
    w.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    assert_eq!(records(f.root.path(), A), committed);
    let mut changed = b.clone();
    changed.not_after_unix_ms -= 1;
    assert!(matches!(
        w.bootstrap_epoch(observed(&p, &changed)).await,
        Err(CryptoError::Conflict)
    ));
    w.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert!(w.prepare_epoch_acquisition(s).await.unwrap().applied());
    w.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    assert_eq!(records(f.root.path(), A), committed);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn wrong_request_pin_signature_epoch_and_owner_never_change_state() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let b = bundle(&p, &f.bundle);
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.bob.prepare_epoch_acquisition(s).await,
        Err(CryptoError::Conflict)
    ));
    assert!(f.bob.bootstrap_epoch(observed(&p, &b)).await.is_err());
    let mut cases = vec![];
    let mut bad = b.clone();
    bad.acquisition_id = uuid::Uuid::new_v4().to_string();
    cases.push(bad);
    let mut bad = b.clone();
    bad.crypto_epoch = None;
    cases.push(bad);
    let mut bad = b.clone();
    bad.crypto_epoch = Some(1);
    cases.push(bad);
    let mut bad = b.clone();
    bad.signed_pre_key.as_mut().unwrap().signature[0] ^= 1;
    cases.push(bad);
    let mut bad = b.clone();
    bad.device.as_mut().unwrap().identity_key[2] ^= 1;
    cases.push(bad);
    for bad in cases {
        assert!(f.alice.bootstrap_epoch(observed(&p, &bad)).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
    }
    let mut bad = p.clone();
    bad.request.acquisition_id = uuid::Uuid::new_v4().to_string();
    assert!(matches!(
        f.alice
            .bootstrap_epoch(observed(&bad, &bundle(&bad, &f.bundle)))
            .await,
        Err(CryptoError::Conflict)
    ));
    let foreign = StoreScope::new("https://other.example.test", A).unwrap();
    assert!(
        f.alice
            .bootstrap_epoch(EpochObservation::test_observation(
                foreign,
                p.clone(),
                b.clone()
            ))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn expiry_and_clock_checks_run_before_fresh_ratchet_mutation() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s).await.unwrap();
    let b = bundle(&p, &f.bundle);
    let before = records(f.root.path(), A);
    let mut expired = b.clone();
    expired.not_after_unix_ms = 1;
    assert!(matches!(
        f.alice.bootstrap_epoch(observed(&p, &expired)).await,
        Err(CryptoError::Expired)
    ));
    let mut future = b.clone();
    future.not_after_unix_ms = i64::MAX;
    assert!(matches!(
        f.alice.bootstrap_epoch(observed(&p, &future)).await,
        Err(CryptoError::AcquisitionClockSkew)
    ));
    assert_eq!(records(f.root.path(), A), before);
    f.alice.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    // Applied replay is independent of today's clock; don't mutate its deadline.
    let mut db = store_after_shutdown(f.alice, f.root.path(), A).await;
    db.transaction_with::<_, CryptoError>(|tx| {
        let mut j = read(tx, &p.scope, &p.pins)?.unwrap();
        j.applied.as_mut().unwrap().not_after_unix_ms = 1;
        save(tx, &p.scope, &j)
    })
    .unwrap();
    drop(db);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), p.scope.own.clone())
        .await
        .unwrap();
    w.bootstrap_epoch(observed(&p, &expired)).await.unwrap();
    assert!(
        w.prepare_epoch_acquisition(p.scope.clone())
            .await
            .unwrap()
            .applied()
    );
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
async fn store_after_shutdown(
    w: CryptoCoordinator,
    root: &std::path::Path,
    user: &str,
) -> ProtectedStore {
    w.shutdown().await.unwrap();
    store(root, user, false)
}
#[tokio::test]
async fn deferred_commit_failure_rolls_back_ratchet_pin_and_applied_snapshot() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s).await.unwrap();
    let b = bundle(&p, &f.bundle);
    let before = records(f.root.path(), A);
    let db = Connection::open(path(f.root.path())).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON;CREATE TABLE fault_parent(id INTEGER PRIMARY KEY);CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER bootstrap_fault AFTER INSERT ON protected_records WHEN NEW.kind=7 BEGIN INSERT INTO fault_child VALUES(999);END;").unwrap();
    assert!(matches!(
        f.alice.bootstrap_epoch(observed(&p, &b)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert!(
        !f.alice
            .prepare_epoch_acquisition(p.scope.clone())
            .await
            .unwrap()
            .applied()
    );
    db.execute_batch("DROP TRIGGER bootstrap_fault").unwrap();
    drop(db);
    f.alice.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn cached_scope_and_inflight_plan_cannot_bypass_later_freeze_or_closure() {
    for frozen in [true, false] {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        let b = bundle(&p, &f.bundle);
        f.alice.shutdown().await.unwrap();
        if frozen {
            rewrite_frozen(f.root.path(), &f.a, true);
        }
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        if !frozen {
            let mut closed = f.a.clone();
            closed.state = ConversationState::Closed as i32;
            closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            closed.closed_generation_id = s.peer.generation_id.clone();
            closed.closed_at_unix_ms = 1_700_000_000_200;
            index(&w, &closed).await;
        }
        let before = records(f.root.path(), A);
        let result = w.bootstrap_epoch(observed(&p, &b)).await;
        assert!(if frozen {
            matches!(result, Err(CryptoError::EpochSendingFrozen))
        } else {
            matches!(result, Err(CryptoError::ConversationClosed))
        });
        assert!(w.prepare_epoch_acquisition(s).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn missing_partial_or_applied_ratchet_never_silently_rebootstrap() {
    for case in 0..5 {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        let b = bundle(&p, &f.bundle);
        if case == 2 {
            f.alice.bootstrap_epoch(observed(&p, &b)).await.unwrap();
        }
        let mut db = store_after_shutdown(f.alice, f.root.path(), A).await;
        if case == 3 {
            db.transaction_with::<_, CryptoError>(|tx| {
                let mut j = read(tx, &s, &p.pins)?.unwrap();
                j.has_message_state = Some(true);
                save(tx, &s, &j)
            })
            .unwrap();
        }
        if case == 4 {
            db.transaction_with::<_, CryptoError>(|tx| {
                let mut j = read(tx, &s, &p.pins)?.unwrap();
                j.has_message_state = None;
                save(tx, &s, &j)
            })
            .unwrap();
        }
        drop(db);
        let raw = Connection::open(path(f.root.path())).unwrap();
        if case < 2 {
            raw.execute(
                "DELETE FROM protected_records WHERE record_id=?1",
                [if case == 0 {
                    s.acquisition_id.clone()
                } else {
                    format!("epoch-v2:{}:acquire:0000000000000000", s.conversation)
                }],
            )
            .unwrap();
        }
        if case == 2 {
            raw.execute(
                "DELETE FROM protected_records WHERE kind=7 AND record_id=?1",
                [&s.session_id],
            )
            .unwrap();
        }
        drop(raw);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        let before = records(f.root.path(), A);
        assert!(w.prepare_epoch_acquisition(s).await.is_err());
        assert!(w.bootstrap_epoch(observed(&p, &b)).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn existing_inbound_session_or_residual_message_blocks_fresh_bootstrap() {
    for existing_session in [false, true] {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        let b = bundle(&p, &f.bundle);
        if existing_session {
            f.alice.bootstrap_epoch(observed(&p, &b)).await.unwrap();
        }
        let mut db = store_after_shutdown(f.alice, f.root.path(), A).await;
        db.transaction_with::<_, CryptoError>(|tx| {
            if existing_session {
                // A real session exists, but this outbound acquisition was not
                // its source (model an inbound bootstrap winning the RPC race).
                let mut j = read(tx, &s, &p.pins)?.unwrap();
                j.applied = None;
                save(tx, &s, &j)?;
            } else {
                tx.put(
                    &RecordKey::new(
                        &s.own.generation_id,
                        RecordKind::History,
                        &format!("v2:{}:{:016x}:retained", s.conversation, s.epoch),
                    )?,
                    b"retained fixture",
                )?;
            }
            Ok(())
        })
        .unwrap();
        drop(db);
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        let result = w.bootstrap_epoch(observed(&p, &b)).await;
        assert!(if existing_session {
            matches!(result, Err(CryptoError::SessionExists))
        } else {
            matches!(result, Err(CryptoError::Conflict))
        });
        assert!(w.prepare_epoch_acquisition(s).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn applied_replay_after_freeze_is_readonly_but_cannot_repair_empty_session() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let b = bundle(&p, &f.bundle);
    f.alice.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    f.alice.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    let before = records(f.root.path(), A);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert!(
        w.prepare_epoch_acquisition(s.clone())
            .await
            .unwrap()
            .applied()
    );
    w.bootstrap_epoch(observed(&p, &b)).await.unwrap();
    assert_eq!(records(f.root.path(), A), before);
    let mut db = store_after_shutdown(w, f.root.path(), A).await;
    db.transaction(|tx| {
        tx.put(
            &RecordKey::new(&s.own.generation_id, RecordKind::Session, &s.session_id)?,
            &SessionRecord::new_fresh().serialize().unwrap(),
        )
    })
    .unwrap();
    drop(db);
    let before = records(f.root.path(), A);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert!(w.prepare_epoch_acquisition(s).await.is_err());
    assert!(w.bootstrap_epoch(observed(&p, &b)).await.is_err());
    assert_eq!(records(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn epoch_view_real_signal_roundtrip_consumes_generation_prekeys_without_v1_session() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    // EC may be absent; both one-time and last-resort PQ must use the same
    // generation-wide callbacks while session bytes live ONLY in v2 slots.
    for ec in [false, true] {
        for last_resort in [false, true] {
            let f = rt.block_on(fixture());
            let a = rt.block_on(initialize(&f.alice));
            let b = rt.block_on(initialize(&f.bob));
            let mut acquired = f.bundle.clone();
            if !ec {
                acquired.one_time_pre_key = None;
            }
            if last_resort {
                let mut bm = store_after_bob_for_fixture(&rt, f.bob, f.root.path());
                acquired.kyber_pre_key = Some(
                    bm.transaction(|tx| {
                        let bytes = tx
                            .get(&RecordKey::new(
                                &b.own.generation_id,
                                RecordKind::PqPreKey,
                                "4294967295",
                            )?)?
                            .unwrap();
                        let key = KyberPreKeyRecord::deserialize(&bytes[3..]).unwrap();
                        Ok(crate::proto::e2ee::KyberPreKey {
                            key_id: u32::MAX,
                            public_key: key.public_key().unwrap().serialize().to_vec(),
                            signature: key.signature().unwrap().to_vec(),
                            is_last_resort: true,
                        })
                    })
                    .unwrap(),
                );
                drop(bm);
            } else {
                rt.block_on(f.bob.shutdown()).unwrap();
            }
            let plan = rt
                .block_on(f.alice.prepare_epoch_acquisition(a.clone()))
                .unwrap();
            rt.block_on(
                f.alice
                    .bootstrap_epoch(observed(&plan, &bundle(&plan, &acquired))),
            )
            .unwrap();
            rt.block_on(f.alice.shutdown()).unwrap();
            let mut alice = store(f.root.path(), A, false);
            let mut bob = store(f.root.path(), B, false);
            let encrypted = alice
                .transaction_with::<_, CryptoError>(|tx| {
                    let view = View::epoch(tx, &a)?;
                    Ok(rt.block_on(message_encrypt(
                        b"v2 callback roundtrip",
                        &address(&a.peer),
                        &address(&a.own),
                        &mut view.clone(),
                        &mut view.clone(),
                        SystemTime::now(),
                        &mut rand::rng(),
                    ))?)
                })
                .unwrap();
            let decrypted = bob
                .transaction_with::<_, CryptoError>(|tx| {
                    let view = View::epoch(tx, &b)?;
                    Ok(Zeroizing::new(rt.block_on(message_decrypt(
                        &encrypted,
                        &address(&b.peer),
                        &address(&b.own),
                        &mut view.clone(),
                        &mut view.clone(),
                        &mut view.clone(),
                        &view.clone(),
                        &mut view.clone(),
                        &mut rand::rng(),
                    ))?))
                })
                .unwrap();
            assert_eq!(decrypted.as_slice(), b"v2 callback roundtrip");
            bob.transaction_with::<_, CryptoError>(|tx| {
                if ec {
                    let id = acquired.one_time_pre_key.as_ref().unwrap().key_id;
                    let bytes = tx
                        .get(&RecordKey::new(
                            &b.own.generation_id,
                            RecordKind::EcPreKey,
                            &id.to_string(),
                        )?)?
                        .unwrap();
                    assert!(matches!(
                        prekey_state::decode(RecordKind::EcPreKey, id, &bytes)?,
                        prekey_state::Saved::Consumed(..)
                    ));
                }
                let id = acquired.kyber_pre_key.as_ref().unwrap().key_id;
                let bytes = tx
                    .get(&RecordKey::new(
                        &b.own.generation_id,
                        RecordKind::PqPreKey,
                        &id.to_string(),
                    )?)?
                    .unwrap();
                assert_eq!(
                    matches!(
                        prekey_state::decode(RecordKind::PqPreKey, id, &bytes)?,
                        prekey_state::Saved::Consumed(..)
                    ),
                    !last_resort
                );
                let old = SessionScope::build(&f.a, Some(a.scope.clone()))?;
                assert!(tx.get(&old.peer_key(RecordKind::Session, true)?)?.is_none());
                Ok(())
            })
            .unwrap();
            assert!(
                records(f.root.path(), A)
                    .iter()
                    .any(|r| r.0 == 7 && r.1 == a.session_id)
            );
            // This crypto-only fixture deliberately bypasses delivery. It proves
            // store callbacks, not an atomic v2 outbox/inbox/recipient ACK.
        }
    }
}
fn store_after_bob_for_fixture(
    rt: &tokio::runtime::Runtime,
    w: CryptoCoordinator,
    root: &std::path::Path,
) -> ProtectedStore {
    rt.block_on(w.shutdown()).unwrap();
    store(root, B, false)
}

#[test]
fn bootstrap_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_BOOTSTRAP_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_BOOTSTRAP_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let pbytes = std::fs::read(root.join("public-bootstrap.pb")).unwrap();
            let b = v2::AcquiredBundle::decode(pbytes.as_slice()).unwrap();
            let own = DeviceRef {
                user_id: A.into(),
                device_id: 1,
                generation_id: "33333333-3333-4333-8333-333333333333".into(),
            };
            let mut db = store(root, A, false);
            db.test_exit_on_record(
                RecordKey::new(
                    &own.generation_id,
                    RecordKind::Session,
                    &slot_id(
                        b"zchat.e2ee.signal-session.v2\0",
                        &b.conversation_id,
                        0,
                        &own,
                        b.device.as_ref().unwrap().reference.as_ref().unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap(),
                after,
            );
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let s = w.epoch_scope(b.conversation_id.clone(), 0).await.unwrap();
            let p = w.prepare_epoch_acquisition(s).await.unwrap();
            let _ = w.bootstrap_epoch(observed(&p, &b)).await;
            panic!("bootstrap process hook did not fire");
        });
}
#[tokio::test]
async fn real_process_exit_before_and_after_commit_preserves_exact_request_and_ratchet() {
    for after in [false, true] {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        let b = bundle(&p, &f.bundle);
        std::fs::write(f.root.path().join("public-bootstrap.pb"), b.encode_to_vec()).unwrap();
        f.alice.shutdown().await.unwrap();
        let before = records(f.root.path(), A);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::epoch_state::bootstrap::tests::bootstrap_crash_child",
                "--nocapture",
            ])
            .env("ZCHAT_BOOTSTRAP_CRASH_PATH", f.root.path())
            .env("ZCHAT_BOOTSTRAP_CRASH_AFTER", if after { "1" } else { "0" })
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        let recovered = w.prepare_epoch_acquisition(s).await.unwrap();
        assert_eq!(recovered.request(), p.request());
        assert_eq!(recovered.applied(), after);
        if !after {
            assert_eq!(records(f.root.path(), A), before);
        }
        w.bootstrap_epoch(observed(&p, &b)).await.unwrap();
        let committed = records(f.root.path(), A);
        w.bootstrap_epoch(observed(&p, &b)).await.unwrap();
        assert_eq!(records(f.root.path(), A), committed);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
