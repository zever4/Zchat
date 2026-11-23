use super::super::super::super::tests::store;
use super::super::super::incoming::tests::{delivery, payload, prepared, send};
use super::super::super::tests::{fixture, index, initialize, records, rewrite_frozen, wrapped};
use super::super::tests::{bundle, observed};
use super::*;
use crate::proto::e2ee::ConversationCloseReason;
use rusqlite::Connection;
const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
fn expired(p: &EpochAcquisitionPlan) -> EpochExpiredAcquisition {
    EpochObservation::test_observation(
        p.scope.scope.clone(),
        p.clone(),
        v2::AcquisitionExpiredFailure {
            request: Some(p.request.clone()),
        },
    )
}
fn raw(root: &std::path::Path, user: &str) -> Connection {
    Connection::open(
        StoreScope::new("https://chat.example.test", user)
            .unwrap()
            .database_path(root),
    )
    .unwrap()
}

#[tokio::test]
async fn authoritative_unused_expiry_changes_only_nonce_once_and_reopens_with_exact_successor() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let old = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let keys: Vec<_> = records(f.root.path(), A)
        .into_iter()
        .filter(|(k, _, _)| *k <= RecordKind::Publication as i64)
        .collect();
    let master = wrapped(f.root.path(), A);
    let next = f
        .alice
        .renew_epoch_acquisition(expired(&old))
        .await
        .unwrap();
    assert_ne!(next.request.acquisition_id, old.request.acquisition_id);
    let mut expected = old.request.clone();
    expected.acquisition_id = next.request.acquisition_id.clone();
    assert_eq!(next.request, expected);
    assert!(!next.applied());
    assert_eq!(wrapped(f.root.path(), A), master);
    assert_eq!(
        records(f.root.path(), A)
            .into_iter()
            .filter(|(k, _, _)| *k <= RecordKind::Publication as i64)
            .collect::<Vec<_>>(),
        keys
    );
    let committed = records(f.root.path(), A);
    assert_eq!(
        f.alice
            .renew_epoch_acquisition(expired(&old))
            .await
            .unwrap()
            .request,
        next.request
    );
    assert_eq!(records(f.root.path(), A), committed);
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert_eq!(
        w.prepare_epoch_acquisition(s.clone())
            .await
            .unwrap()
            .request,
        next.request
    );
    assert_eq!(
        w.renew_epoch_acquisition(expired(&old))
            .await
            .unwrap()
            .request,
        next.request
    );
    assert_eq!(records(f.root.path(), A), committed);
    assert!(
        w.bootstrap_epoch(observed(&old, &bundle(&old, &f.bundle)))
            .await
            .is_err()
    );
    w.bootstrap_epoch(observed(&next, &bundle(&next, &f.bundle)))
        .await
        .unwrap();
    assert!(w.prepare_epoch_acquisition(s).await.unwrap().applied());
    assert!(matches!(
        w.renew_epoch_acquisition(expired(&old)).await,
        Err(CryptoError::SessionExists)
    ));
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn concurrent_expiry_retries_share_successor_and_old_proof_cannot_skip_a_later_renewal() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let (one, two) = tokio::join!(
        f.alice.renew_epoch_acquisition(expired(&p)),
        f.alice.renew_epoch_acquisition(expired(&p))
    );
    let one = one.unwrap();
    assert_eq!(one.request, two.unwrap().request);
    let next = f
        .alice
        .renew_epoch_acquisition(expired(&one))
        .await
        .unwrap();
    assert_ne!(one.request.acquisition_id, next.request.acquisition_id);
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice.renew_epoch_acquisition(expired(&p)).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(
        f.alice
            .renew_epoch_acquisition(expired(&one))
            .await
            .unwrap()
            .request,
        next.request
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_expiry_is_not_renewal_and_every_remote_proof_binding_is_rechecked() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let before = records(f.root.path(), A);
    let mut old = bundle(&p, &f.bundle);
    old.not_after_unix_ms = 1_700_000_000_000;
    assert!(matches!(
        f.alice.bootstrap_epoch(observed(&p, &old)).await,
        Err(CryptoError::Expired)
    ));
    assert_eq!(
        f.alice
            .prepare_epoch_acquisition(s.clone())
            .await
            .unwrap()
            .request,
        p.request
    );
    for fault in 0..7 {
        let mut expected = p.clone();
        let mut response = v2::AcquisitionExpiredFailure {
            request: Some(p.request.clone()),
        };
        let mut scope = p.scope.scope.clone();
        match fault {
            0 => response.request = None,
            1 => {
                response.request.as_mut().unwrap().acquisition_id = uuid::Uuid::new_v4().to_string()
            }
            2 => response.request.as_mut().unwrap().crypto_epoch = Some(1),
            3 => {
                response
                    .request
                    .as_mut()
                    .unwrap()
                    .context
                    .as_mut()
                    .unwrap()
                    .protocol_version = 1
            }
            4 => scope = StoreScope::new("https://other.example.test", A).unwrap(),
            5 => expected.scope.peer_registration += 1,
            _ => expected.scope.own.user_id = B.into(),
        }
        assert!(
            f.alice
                .renew_epoch_acquisition(EpochObservation::test_observation(
                    scope, expected, response
                ))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
    }
    assert!(f.bob.renew_epoch_acquisition(expired(&p)).await.is_err());
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn stale_scope_cannot_renew_after_send_freeze_or_peer_closure() {
    for frozen in [true, false] {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        f.alice.shutdown().await.unwrap();
        if frozen {
            rewrite_frozen(f.root.path(), &f.a, true);
        }
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        if !frozen {
            let mut c = f.a.clone();
            c.state = ConversationState::Closed as i32;
            c.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            c.closed_generation_id = s.peer.generation_id.clone();
            c.closed_at_unix_ms = 1_700_000_000_001;
            index(&w, &c).await;
        }
        let before = records(f.root.path(), A);
        assert!(w.renew_epoch_acquisition(expired(&p)).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn applied_or_missing_established_ratchet_never_allows_a_new_nonce() {
    for fault in 0..3 {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        f.alice
            .bootstrap_epoch(observed(&p, &bundle(&p, &f.bundle)))
            .await
            .unwrap();
        if fault == 2 {
            f.alice
                .encrypt_epoch(s.clone(), payload(&s, "retained crypto traffic"))
                .await
                .unwrap();
        }
        f.alice.shutdown().await.unwrap();
        if fault != 0 {
            raw(f.root.path(), A)
                .execute(
                    "DELETE FROM protected_records WHERE kind=?1 AND record_id=?2",
                    rusqlite::params![RecordKind::Session as i64, s.session_id],
                )
                .unwrap();
        }
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        assert!(w.renew_epoch_acquisition(expired(&p)).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn inbound_session_or_quarantine_wins_over_inflight_expiry_proof() {
    for quarantined in [true, false] {
        let (f, a, b) = prepared().await;
        let p = f.bob.prepare_epoch_acquisition(b.clone()).await.unwrap();
        let mut d = send(&f.alice, &a, payload(&a, "inbound wins expiry race"), 1).await;
        if quarantined {
            let e = d.envelope.as_mut().unwrap();
            e.ciphertext = vec![1];
            d.envelope_digest = crate::encryption::epoch_contract::envelope_digest(e)
                .unwrap()
                .to_vec();
            f.bob
                .quarantine_epoch(
                    b.clone(),
                    delivery(&b, &d),
                    crate::encryption::signal_state::EpochQuarantineReason::InvalidCiphertext,
                )
                .await
                .unwrap();
        } else {
            f.bob
                .receive_epoch(b.clone(), delivery(&b, &d))
                .await
                .unwrap();
        }
        let before = records(f.root.path(), B);
        assert!(f.bob.renew_epoch_acquisition(expired(&p)).await.is_err());
        assert_eq!(records(f.root.path(), B), before);
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn failed_renewal_commit_rolls_back_journal_anchor_nonce_and_index_initialization() {
    let f = fixture().await;
    let s = initialize(&f.alice).await;
    let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let before = records(f.root.path(), A);
    let db = raw(f.root.path(), A);
    db.execute_batch("CREATE TABLE renewal_parent(id INTEGER PRIMARY KEY);CREATE TABLE renewal_child(id INTEGER REFERENCES renewal_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER renewal_fault AFTER UPDATE ON protected_records WHEN NEW.kind=9 BEGIN INSERT INTO renewal_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.alice.renew_epoch_acquisition(expired(&p)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert_eq!(
        f.alice.prepare_epoch_acquisition(s).await.unwrap().request,
        p.request
    );
    db.execute_batch("DROP TRIGGER renewal_fault").unwrap();
    assert_ne!(
        f.alice
            .renew_epoch_acquisition(expired(&p))
            .await
            .unwrap()
            .request
            .acquisition_id,
        p.request.acquisition_id
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_journal_anchor_or_partial_authenticated_replay_never_recreates_acquisition() {
    for fault in 0..4 {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        let old = records(f.root.path(), A)
            .into_iter()
            .find(|(k, id, _)| *k == RecordKind::Acquisition as i64 && id == &s.acquisition_id)
            .unwrap()
            .2;
        f.alice.renew_epoch_acquisition(expired(&p)).await.unwrap();
        f.alice.shutdown().await.unwrap();
        if fault < 3 {
            let db = raw(f.root.path(), A);
            if fault == 0 {
                db.execute(
                    "DELETE FROM protected_records WHERE kind=?1 AND record_id=?2",
                    rusqlite::params![RecordKind::Acquisition as i64, s.acquisition_id],
                )
                .unwrap();
            } else if fault == 1 {
                db.execute(
                    "DELETE FROM protected_records WHERE record_id=?1",
                    [format!(
                        "epoch-v2:{}:acquire:{:016x}",
                        s.conversation, s.epoch
                    )],
                )
                .unwrap();
            } else {
                db.execute(
                    "UPDATE protected_records SET payload=?1 WHERE kind=?2 AND record_id=?3",
                    rusqlite::params![old, RecordKind::Acquisition as i64, s.acquisition_id],
                )
                .unwrap();
            }
        } else {
            let mut db = store(f.root.path(), A, false);
            db.transaction_with::<_, CryptoError>(|tx| {
                let mut j = read(tx, &s, &p.pins)?.unwrap();
                j.replaces_request.as_mut().unwrap().crypto_epoch = Some(1);
                save(tx, &s, &j)
            })
            .unwrap();
        }
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        assert!(w.renew_epoch_acquisition(expired(&p)).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[test]
fn renewal_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_EPOCH_RENEW_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_EPOCH_RENEW_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let request = v2::AcquireBundleRequest::decode(
                std::fs::read(root.join("public-expired-request.pb"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let own = DeviceRef {
                user_id: A.into(),
                generation_id: request
                    .context
                    .as_ref()
                    .unwrap()
                    .device_generation_id
                    .clone(),
                device_id: 1,
            };
            let mut db = store(root, A, false);
            let s = db
                .transaction_with::<_, CryptoError>(|tx| {
                    let c =
                        conversation_lifecycle::load(tx, &own, &request.conversation_id)?.unwrap();
                    let (identity, reg) = identity(tx, &own)?;
                    let state = load(tx, &own, &c, identity, reg)?.unwrap();
                    scope(&state, &c, StoreScope::new("https://chat.example.test", A)?)
                })
                .unwrap();
            db.test_exit_on_record_change(journal_key(&s).unwrap(), after)
                .unwrap();
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            let mut p = w.prepare_epoch_acquisition(s.clone()).await.unwrap();
            p.request = request;
            let _ = w.renew_epoch_acquisition(expired(&p)).await;
            panic!("renewal crash hook did not fire");
        });
}

#[tokio::test]
async fn process_exit_before_after_renewal_commit_recovers_old_or_one_exact_new_nonce() {
    for after in [false, true] {
        let f = fixture().await;
        let s = initialize(&f.alice).await;
        let p = f.alice.prepare_epoch_acquisition(s.clone()).await.unwrap();
        std::fs::write(
            f.root.path().join("public-expired-request.pb"),
            p.request.encode_to_vec(),
        )
        .unwrap();
        f.alice.shutdown().await.unwrap();
        let before = records(f.root.path(), A);
        let status=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","encryption::signal_state::epoch_state::bootstrap::renewal::tests::renewal_crash_child","--nocapture"]).env("ZCHAT_EPOCH_RENEW_CRASH_PATH",f.root.path()).env("ZCHAT_EPOCH_RENEW_CRASH_AFTER",if after{"1"}else{"0"}).status().unwrap();
        assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
            .await
            .unwrap();
        let current = w.prepare_epoch_acquisition(s).await.unwrap();
        assert_eq!(current.request == p.request, !after);
        if !after {
            assert_eq!(records(f.root.path(), A), before);
        }
        let next = w.renew_epoch_acquisition(expired(&p)).await.unwrap();
        if after {
            assert_eq!(current.request, next.request);
        }
        let committed = records(f.root.path(), A);
        assert_eq!(
            w.renew_epoch_acquisition(expired(&p))
                .await
                .unwrap()
                .request,
            next.request
        );
        assert_eq!(records(f.root.path(), A), committed);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}
