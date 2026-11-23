use super::*;
use crate::encryption::{
    provisioning::{DeviceProvisioning, Reconciliation},
    publication::publication_digest,
};
use crate::proto::e2ee::{OwnDeviceState, UploadPreKeysRequest, UploadReceipt};
use std::path::Path;

#[path = "prekey_rotation_tests.rs"]
mod rotation;

#[path = "prekey_maintenance_tests.rs"]
mod maintenance;

#[path = "recovery_index_tests.rs"]
mod recovery;

fn ack(req: &UploadPreKeysRequest) -> UploadReceipt {
    UploadReceipt {
        batch_id: req.batch_id.clone(),
        batch_digest: publication_digest(req).unwrap().to_vec(),
        publication_revision: req.expected_revision + 1,
    }
}
fn installed(root: &Path, user: &str) -> Engine {
    let mut p = DeviceProvisioning::new(store(root, user, true));
    let attempt = p.prepare_registration().unwrap();
    let remote = OwnDeviceState {
        user_id: user.into(),
        active_device: Some(Device {
            reference: Some(device(user, if user == A { AG } else { BG })),
            registration_id: attempt.request.registration_id,
            identity_key: attempt.request.identity_key.clone(),
            public_material_ready: false,
            public_material_revision: 0,
        }),
    };
    p.bind_registration(&attempt.request.registration_request_id, &remote)
        .unwrap();
    while let Some(request) = p.next_upload().unwrap() {
        p.acknowledge_upload(&ack(&request)).unwrap();
    }
    let own = p.ready_device().unwrap();
    let mut e = Engine {
        store: p.into_store(),
        own,
        runtime: tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    };
    while !e.seed_prekey_ids().unwrap().ready {}
    e
}
fn reopen(root: &Path, own: &DeviceRef) -> Engine {
    Engine {
        store: store(root, &own.user_id, false),
        own: own.clone(),
        runtime: tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    }
}
fn finish(e: &mut Engine) -> Vec<UploadPreKeysRequest> {
    let mut requests = vec![];
    while let Some(upload) = e.next_prekey_refill_upload().unwrap() {
        let request = upload.request().clone();
        e.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(upload, ack(&request)))
            .unwrap();
        requests.push(request);
    }
    requests
}
fn private(e: &mut Engine, requests: &[UploadPreKeysRequest]) -> Vec<Vec<u8>> {
    let own = e.own.clone();
    e.store
        .transaction(|tx| {
            let mut bytes = vec![];
            for r in requests {
                for (kind, id) in r
                    .one_time_pre_keys
                    .iter()
                    .map(|k| (RecordKind::EcPreKey, k.key_id))
                    .chain(
                        r.kyber_pre_keys
                            .iter()
                            .map(|k| (RecordKind::PqPreKey, k.key_id)),
                    )
                {
                    bytes.push(
                        tx.get(&RecordKey::new(&own.generation_id, kind, &id.to_string())?)?
                            .unwrap()
                            .to_vec(),
                    );
                }
            }
            Ok(bytes)
        })
        .unwrap()
}

#[test]
fn refill_reserves_before_material_and_reopens_with_exact_public_private_and_receipts() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let plan = e.prepare_prekey_refill(3, 40, None).unwrap();
    assert!(!plan.materialized && !plan.complete);
    assert!(e.next_prekey_refill_upload().is_err());
    assert_eq!(e.prepare_prekey_refill(3, 40, None).unwrap(), plan);
    assert!(matches!(
        e.prepare_prekey_refill(4, 40, None),
        Err(CryptoError::Conflict)
    ));
    let own = e.own.clone();
    drop(e);
    let mut e = reopen(root.path(), &own);
    assert_eq!(e.prepare_prekey_refill(3, 40, None).unwrap(), plan);
    let ready = e.materialize_prekey_refill(&plan.operation_id).unwrap();
    assert!(ready.materialized && !ready.complete);
    let first = e.next_prekey_refill_upload().unwrap().unwrap();
    let first_request = first.request().clone();
    assert_eq!(first_request.one_time_pre_keys[0].key_id, 33);
    assert_eq!(first_request.kyber_pre_keys[0].key_id, 34); // production LR33
    let bytes = private(&mut e, std::slice::from_ref(&first_request));
    let proof = VerifiedPreKeyRefillUpload::from_rpc(first, ack(&first_request));
    drop(e);
    let mut e = reopen(root.path(), &own);
    assert_eq!(
        e.materialize_prekey_refill(&plan.operation_id).unwrap(),
        ready
    );
    assert_eq!(
        e.next_prekey_refill_upload().unwrap().unwrap().request(),
        &first_request
    );
    assert_eq!(private(&mut e, std::slice::from_ref(&first_request)), bytes);
    e.accept_prekey_refill_upload(proof).unwrap();
    assert_ne!(
        e.next_prekey_refill_upload()
            .unwrap()
            .unwrap()
            .request()
            .batch_id,
        first_request.batch_id
    );
    finish(&mut e);
    assert!(e.prepare_prekey_refill(3, 40, None).unwrap().complete);
    drop(e);
    let mut p = DeviceProvisioning::new(store(root.path(), A, false));
    assert!(p.next_upload().unwrap().is_none());
    assert_eq!(p.ready_device().unwrap(), own);
    assert_eq!(
        p.prepare_registration().unwrap().bound_device.as_ref(),
        Some(&own)
    );
    let probe = p.publication_probe().unwrap();
    p.acknowledge_upload(&ack(&probe)).unwrap();
}

#[test]
fn next_refill_uses_explicit_previous_cas_and_old_receipt_cannot_rewind_revision() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let old = e.prepare_prekey_refill(2, 0, None).unwrap();
    for invalid in ["", "invalid-operation"] {
        assert!(e.prepare_prekey_refill(2, 0, Some(invalid)).is_err());
    }
    assert!(
        e.prepare_prekey_refill(2, 0, Some(&old.operation_id))
            .is_err()
    );
    e.materialize_prekey_refill(&old.operation_id).unwrap();
    let old_upload = e.next_prekey_refill_upload().unwrap().unwrap();
    let old_receipt = ack(old_upload.request());
    finish(&mut e);
    let next = e
        .prepare_prekey_refill(0, 2, Some(&old.operation_id))
        .unwrap();
    assert_ne!(old.operation_id, next.operation_id);
    assert_eq!(
        e.prepare_prekey_refill(0, 2, Some(&old.operation_id))
            .unwrap(),
        next
    );
    assert!(
        e.prepare_prekey_refill(1, 2, Some(&old.operation_id))
            .is_err()
    );
    assert!(
        e.prepare_prekey_refill(0, 2, Some(&Uuid::new_v4().to_string()))
            .is_err()
    );
    e.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(
        old_upload,
        old_receipt,
    ))
    .unwrap();
    e.materialize_prekey_refill(&next.operation_id).unwrap();
    let requests = finish(&mut e);
    assert_eq!(requests[0].expected_revision, 3);
    assert!(
        e.prepare_prekey_refill(0, 2, Some(&old.operation_id))
            .unwrap()
            .complete
    );
    for (ec, pq) in [(0, 0), (101, 1), (1, 101), (u32::MAX, u32::MAX)] {
        assert!(e.prepare_prekey_refill(ec, pq, None).is_err());
    }
}

#[test]
fn lost_remote_receipt_is_replayed_on_login_without_new_keys_or_initial_revision_reset() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let plan = e.prepare_prekey_refill(2, 1, None).unwrap();
    e.materialize_prekey_refill(&plan.operation_id).unwrap();
    let request = e
        .next_prekey_refill_upload()
        .unwrap()
        .unwrap()
        .request()
        .clone();
    let original = private(&mut e, std::slice::from_ref(&request));
    let own = e.own.clone();
    drop(e);
    let mut p = DeviceProvisioning::new(store(root.path(), A, false));
    let attempt = p.prepare_registration().unwrap();
    let mut remote = OwnDeviceState {
        user_id: A.into(),
        active_device: Some(Device {
            reference: Some(own.clone()),
            registration_id: attempt.request.registration_id,
            identity_key: attempt.request.identity_key.clone(),
            public_material_ready: true,
            public_material_revision: request.expected_revision + 1,
        }),
    };
    assert_eq!(
        p.reconcile(&remote).unwrap(),
        Reconciliation::PublicationPending
    );
    assert!(p.ready_device().is_err());
    assert_eq!(p.next_upload().unwrap().unwrap(), request);
    p.acknowledge_upload(&ack(&request)).unwrap();
    assert_eq!(p.reconcile(&remote).unwrap(), Reconciliation::Ready);
    assert_eq!(p.ready_device().unwrap(), own);
    remote
        .active_device
        .as_mut()
        .unwrap()
        .public_material_revision = request.expected_revision;
    assert!(p.reconcile(&remote).is_err());
    remote
        .active_device
        .as_mut()
        .unwrap()
        .public_material_revision = request.expected_revision + 2;
    assert!(p.reconcile(&remote).is_err());
    drop(p);
    let mut e = reopen(root.path(), &own);
    assert_eq!(private(&mut e, &[request]), original);
}

#[test]
fn reserved_only_refill_resumes_materialization_during_native_initial_recovery() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let plan = e.prepare_prekey_refill(1, 1, None).unwrap();
    let own = e.own.clone();
    drop(e);
    let mut p = DeviceProvisioning::new(store(root.path(), A, false));
    let request = p.next_upload().unwrap().unwrap();
    p.acknowledge_upload(&ack(&request)).unwrap();
    assert_eq!(p.ready_device().unwrap(), own);
    drop(p);
    let mut e = reopen(root.path(), &own);
    let restored = e.prepare_prekey_refill(1, 1, None).unwrap();
    assert_eq!(restored.operation_id, plan.operation_id);
    assert!(restored.complete);
}

#[test]
fn failed_commits_keep_reservation_material_and_receipt_boundaries_atomic() {
    let root = tempfile::tempdir().unwrap();
    let e = installed(root.path(), A);
    let own = e.own.clone();
    drop(e);
    commit_fault(root.path(), A, RecordKind::Publication as u8, true);
    let mut e = reopen(root.path(), &own);
    assert!(matches!(
        e.prepare_prekey_refill(2, 2, None),
        Err(CryptoError::Storage(_))
    ));
    drop(e);
    commit_fault(root.path(), A, RecordKind::Publication as u8, false);
    let mut e = reopen(root.path(), &own);
    let plan = e.prepare_prekey_refill(2, 2, None).unwrap();
    drop(e);
    commit_fault(root.path(), A, RecordKind::PqPreKey as u8, true);
    let mut e = reopen(root.path(), &own);
    assert!(matches!(
        e.materialize_prekey_refill(&plan.operation_id),
        Err(CryptoError::Storage(_))
    ));
    assert!(!e.prepare_prekey_refill(2, 2, None).unwrap().materialized);
    e.store
        .transaction(|tx| {
            for (kind, id) in [(RecordKind::EcPreKey, 33), (RecordKind::PqPreKey, 34)] {
                assert!(
                    tx.get(&RecordKey::new(AG, kind, &id.to_string())?)?
                        .is_none()
                );
            }
            Ok(())
        })
        .unwrap();
    drop(e);
    commit_fault(root.path(), A, RecordKind::PqPreKey as u8, false);
    let mut e = reopen(root.path(), &own);
    e.materialize_prekey_refill(&plan.operation_id).unwrap();
    let upload = e.next_prekey_refill_upload().unwrap().unwrap();
    let request = upload.request().clone();
    let original = private(&mut e, std::slice::from_ref(&request));
    drop(e);
    commit_fault(root.path(), A, RecordKind::Control as u8, true);
    let mut e = reopen(root.path(), &own);
    assert!(matches!(
        e.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(upload, ack(&request))),
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(
        e.next_prekey_refill_upload().unwrap().unwrap().request(),
        &request
    );
    assert_eq!(private(&mut e, std::slice::from_ref(&request)), original);
    drop(e);
    commit_fault(root.path(), A, RecordKind::Control as u8, false);
    let mut e = reopen(root.path(), &own);
    finish(&mut e);
}

#[test]
fn mismatched_proofs_foreign_accounts_and_lost_private_records_fail_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let mut other = installed(root.path(), B);
    let plan = e.prepare_prekey_refill(2, 2, None).unwrap();
    e.materialize_prekey_refill(&plan.operation_id).unwrap();
    let upload = e.next_prekey_refill_upload().unwrap().unwrap();
    let mut bad = ack(upload.request());
    bad.batch_digest[0] ^= 1;
    assert!(
        e.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(upload.clone(), bad))
            .is_err()
    );
    assert!(
        other
            .accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(
                upload.clone(),
                ack(upload.request())
            ))
            .is_err()
    );
    let own = e.own.clone();
    drop(e);
    let db = rusqlite::Connection::open(
        StoreScope::new(SERVER, A)
            .unwrap()
            .database_path(root.path()),
    )
    .unwrap();
    db.execute(
        "DELETE FROM protected_records WHERE kind=3 AND record_id='33'",
        [],
    )
    .unwrap();
    drop(db);
    let mut e = reopen(root.path(), &own);
    assert!(e.next_prekey_refill_upload().is_err());
    assert!(e.materialize_prekey_refill(&plan.operation_id).is_err());
}

#[test]
fn missing_reservation_is_not_recreated_during_read_only_reconciliation() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let plan = e.prepare_prekey_refill(1, 1, None).unwrap();
    e.materialize_prekey_refill(&plan.operation_id).unwrap();
    drop(e);
    let path = StoreScope::new(SERVER, A)
        .unwrap()
        .database_path(root.path());
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute(
        "DELETE FROM protected_records WHERE kind=13 AND record_id LIKE 'prekey-ids:reservation:%'",
        [],
    )
    .unwrap();
    drop(db);
    let mut p = DeviceProvisioning::new(store(root.path(), A, false));
    let attempt = p.prepare_registration().unwrap();
    let remote = OwnDeviceState {
        user_id: A.into(),
        active_device: Some(Device {
            reference: attempt.bound_device,
            registration_id: attempt.request.registration_id,
            identity_key: attempt.request.identity_key,
            public_material_ready: true,
            public_material_revision: 2,
        }),
    };
    let before = std::fs::read(&path).unwrap();
    assert!(p.reconcile(&remote).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert!(p.next_upload().is_err());
}

#[test]
fn maximum_refill_is_bounded_and_never_rotates_signed_lr_or_copies_private_bytes_to_plan() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let original = e
        .store
        .transaction(|tx| {
            Ok([
                tx.get(&RecordKey::new(AG, RecordKind::SignedPreKey, "1")?)?
                    .unwrap()
                    .to_vec(),
                tx.get(&RecordKey::new(AG, RecordKind::PqPreKey, "33")?)?
                    .unwrap()
                    .to_vec(),
            ])
        })
        .unwrap();
    let plan = e.prepare_prekey_refill(100, 100, None).unwrap();
    e.materialize_prekey_refill(&plan.operation_id).unwrap();
    let requests = finish(&mut e);
    assert!(requests.len() > 1 && requests.len() <= 8);
    assert_eq!(
        requests
            .iter()
            .map(|r| r.one_time_pre_keys.len())
            .sum::<usize>(),
        100
    );
    assert_eq!(
        requests
            .iter()
            .map(|r| r.kyber_pre_keys.len())
            .sum::<usize>(),
        100
    );
    assert!(
        requests
            .iter()
            .all(|r| r.signed_pre_key.is_none()
                && r.kyber_pre_keys.iter().all(|k| !k.is_last_resort))
    );
    let private = private(&mut e, &requests);
    e.store
        .transaction(|tx| {
            assert_eq!(
                tx.get(&RecordKey::new(AG, RecordKind::SignedPreKey, "1")?)?
                    .unwrap()
                    .as_slice(),
                original[0]
            );
            assert_eq!(
                tx.get(&RecordKey::new(AG, RecordKind::PqPreKey, "33")?)?
                    .unwrap()
                    .as_slice(),
                original[1]
            );
            let journal = tx
                .get(&RecordKey::new(
                    AG,
                    RecordKind::Publication,
                    &format!("refill:{}", plan.operation_id),
                )?)?
                .unwrap();
            for key in &private {
                assert!(!journal.windows(key.len() - 3).any(|w| w == &key[3..]));
            }
            Ok(())
        })
        .unwrap();
}

#[test]
fn refilled_keys_decrypt_real_signal_and_late_publication_ack_never_restores_consumed_secrets() {
    let root = tempfile::tempdir().unwrap();
    let mut alice = installed(root.path(), A);
    let mut bob = installed(root.path(), B);
    let plan = bob.prepare_prekey_refill(1, 1, None).unwrap();
    bob.materialize_prekey_refill(&plan.operation_id).unwrap();
    let upload = bob.next_prekey_refill_upload().unwrap().unwrap();
    let own_public = |e: &mut Engine| {
        let own = e.own.clone();
        e.store
            .transaction(|tx| {
                let bytes = tx
                    .get(&RecordKey::new(
                        &own.generation_id,
                        RecordKind::Identity,
                        "local",
                    )?)?
                    .unwrap();
                Ok(Device {
                    reference: Some(own),
                    registration_id: u32::from_be_bytes(bytes[1..5].try_into().unwrap()),
                    identity_key: IdentityKeyPair::try_from(&bytes[5..])
                        .unwrap()
                        .public_key()
                        .serialize()
                        .to_vec(),
                    public_material_ready: true,
                    public_material_revision: 3,
                })
            })
            .unwrap()
    };
    let ap = own_public(&mut alice);
    let bp = own_public(&mut bob);
    let a = scope(alice.own.clone(), bp.clone());
    let b = scope(bob.own.clone(), ap);
    let signed = bob
        .store
        .transaction(|tx| {
            Ok(tx
                .get(&RecordKey::new(BG, RecordKind::SignedPreKey, "1")?)?
                .unwrap()
                .to_vec())
        })
        .unwrap();
    let signed = SignedPreKeyRecord::deserialize(&signed[3..]).unwrap();
    let lease = alice.prepare_acquisition(&a).unwrap();
    let acquired = AcquiredBundle {
        acquisition_id: lease.request.acquisition_id,
        conversation_id: CHAT.into(),
        device: Some(bp),
        signed_pre_key: Some(SignedPreKey {
            key_id: 1,
            public_key: signed.public_key().unwrap().serialize().to_vec(),
            signature: signed.signature().unwrap(),
            created_at_unix_ms: signed.timestamp().unwrap().epoch_millis() as i64,
        }),
        one_time_pre_key: Some(upload.request().one_time_pre_keys[0].clone()),
        kyber_pre_key: Some(upload.request().kyber_pre_keys[0].clone()),
        not_after_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 3_600_000,
    };
    alice.bootstrap(&a, &acquired).unwrap();
    let text = payload(
        &a,
        "new refill keys remain consumable before the local publication receipt",
    );
    let envelope = alice.encrypt(&a, &text).unwrap();
    assert_eq!(bob.decrypt(&b, &envelope).unwrap(), text);
    bob.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(
        upload.clone(),
        ack(upload.request()),
    ))
    .unwrap();
    bob.materialize_prekey_refill(&plan.operation_id).unwrap();
    bob.store
        .transaction(|tx| {
            for (kind, id) in [(RecordKind::EcPreKey, 33), (RecordKind::PqPreKey, 34)] {
                assert_eq!(
                    tx.get(&RecordKey::new(BG, kind, &id.to_string())?)?
                        .unwrap()
                        .len(),
                    35
                );
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(bob.decrypt(&b, &envelope).unwrap(), text);
}

#[test]
fn pq_exhaustion_rolls_back_the_whole_parent_and_ec_reservation() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let bytes = e
        .store
        .transaction(|tx| {
            Ok(tx
                .get(&RecordKey::new(AG, RecordKind::PqPreKey, "33")?)?
                .unwrap()
                .to_vec())
        })
        .unwrap();
    let lr = KyberPreKeyRecord::deserialize(&bytes[3..]).unwrap();
    let last = KyberPreKeyRecord::new(
        u32::MAX.into(),
        lr.timestamp().unwrap(),
        &lr.key_pair().unwrap(),
        &lr.signature().unwrap(),
    )
    .serialize()
    .unwrap();
    e.store
        .transaction_with::<_, CryptoError>(|tx| {
            prekey_state::save(tx, AG, RecordKind::PqPreKey, u32::MAX, &last, Some(true))
        })
        .unwrap();
    assert!(matches!(
        e.prepare_prekey_refill(2, 1, None),
        Err(CryptoError::PreKeyIdsExhausted)
    ));
    assert!(e.next_prekey_refill_upload().unwrap().is_none());
    assert_eq!(
        e.reserve_prekey_ids(&Uuid::new_v4().to_string(), PreKeyIdNamespace::Ec, 1)
            .unwrap()
            .first,
        33
    );
}

#[test]
fn process_exit_before_and_after_each_refill_commit_recovers_only_complete_steps() {
    for phase in ["prepare", "materialize", "receipt"] {
        for after in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut e = installed(root.path(), A);
            let own = e.own.clone();
            let operation = if phase == "prepare" {
                String::new()
            } else {
                e.prepare_prekey_refill(2, 2, None).unwrap().operation_id
            };
            let saved_request = if phase == "receipt" {
                e.materialize_prekey_refill(&operation).unwrap();
                Some(
                    e.next_prekey_refill_upload()
                        .unwrap()
                        .unwrap()
                        .request()
                        .clone(),
                )
            } else {
                None
            };
            let original = saved_request
                .as_ref()
                .map(|r| private(&mut e, std::slice::from_ref(r)));
            drop(e);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "encryption::signal_state::tests::refill::refill_exit_child",
                    "--nocapture",
                ])
                .env("ZCHAT_REFILL_ROOT", root.path())
                .env("ZCHAT_REFILL_PHASE", phase)
                .env("ZCHAT_REFILL_OPERATION", &operation)
                .env("ZCHAT_REFILL_AFTER", if after { "1" } else { "0" })
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
            let mut e = reopen(root.path(), &own);
            let restored = e.prepare_prekey_refill(2, 2, None).unwrap();
            if phase != "prepare" {
                assert_eq!(restored.operation_id, operation);
            }
            assert_eq!(
                restored.materialized,
                phase == "receipt" || (phase == "materialize" && after)
            );
            assert_eq!(restored.complete, phase == "receipt" && after);
            e.materialize_prekey_refill(&restored.operation_id).unwrap();
            if let Some(request) = saved_request {
                assert_eq!(private(&mut e, &[request]), original.unwrap());
            }
            finish(&mut e);
        }
    }
}

#[test]
fn refill_exit_child() {
    let Some(root) = std::env::var_os("ZCHAT_REFILL_ROOT") else {
        return;
    };
    let phase = std::env::var("ZCHAT_REFILL_PHASE").unwrap();
    let operation = std::env::var("ZCHAT_REFILL_OPERATION").unwrap();
    let after = std::env::var("ZCHAT_REFILL_AFTER").unwrap() == "1";
    let mut e = reopen(Path::new(&root), &device(A, AG));
    let target = match phase.as_str() {
        "prepare" => RecordKey::new(AG, RecordKind::Control, "prekey-refill:head:v1").unwrap(),
        "materialize" => RecordKey::new(AG, RecordKind::EcPreKey, "33").unwrap(),
        _ => RecordKey::new(AG, RecordKind::Control, "published:ec:33").unwrap(),
    };
    e.store.test_exit_on_record(target, after);
    match phase.as_str() {
        "prepare" => {
            let _ = e.prepare_prekey_refill(2, 2, None);
        }
        "materialize" => {
            let _ = e.materialize_prekey_refill(&operation);
        }
        _ => {
            finish(&mut e);
        }
    }
    panic!("refill exit hook not reached");
}
