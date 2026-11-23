//! Rotation uses the existing refill test fixture and the actual protected owner.
use super::*;
use crate::encryption::{
    protected::{AccountKey, AccountKind},
    publication::validate_upload,
};

fn record(e: &mut Engine, kind: RecordKind, id: &str) -> Vec<u8> {
    let key = RecordKey::new(&e.own.generation_id, kind, id).unwrap();
    e.store
        .transaction(|tx| Ok(tx.get(&key)?.unwrap().to_vec()))
        .unwrap()
}
fn pointers(e: &mut Engine) -> (u32, u32) {
    let signed = record(e, RecordKind::Control, "active-signed");
    let lr = record(e, RecordKind::Control, "active-last-resort");
    (
        u32::from_be_bytes(signed.try_into().unwrap()),
        u32::from_be_bytes(lr.try_into().unwrap()),
    )
}
fn public(e: &mut Engine) -> Device {
    e.store
        .transaction(|tx| {
            let bytes = tx
                .get_account(&AccountKey::new(AccountKind::ActiveDevice, "initial")?)?
                .unwrap();
            Ok(OwnDeviceState::decode(bytes.as_slice())
                .unwrap()
                .active_device
                .unwrap())
        })
        .unwrap()
}
fn snapshot(e: &mut Engine, request: &UploadPreKeysRequest) -> Vec<Vec<u8>> {
    let mut result = private(e, std::slice::from_ref(request));
    if let Some(k) = &request.signed_pre_key {
        result.push(record(e, RecordKind::SignedPreKey, &k.key_id.to_string()));
    }
    result
}
fn rotate(
    e: &mut Engine,
    previous: Option<&str>,
    signed: bool,
    lr: bool,
) -> (PreKeyRotationPlan, PreKeyRefillUpload) {
    let p = e.prepare_prekey_rotation(signed, lr, previous).unwrap();
    e.materialize_prekey_refill(&p.operation_id).unwrap();
    let upload = e.next_prekey_refill_upload().unwrap().unwrap();
    (p, upload)
}

#[test]
fn rotation_replays_original_private_public_and_activates_only_after_receipt() {
    for (signed, lr) in [(true, true), (true, false), (false, true)] {
        let root = tempfile::tempdir().unwrap();
        let mut e = installed(root.path(), A);
        assert!(e.current_prekey_publication().unwrap().is_none());
        assert!(e.prepare_prekey_rotation(false, false, None).is_err());
        let old_signed = record(&mut e, RecordKind::SignedPreKey, "1");
        let old_lr = record(&mut e, RecordKind::PqPreKey, "33");
        let old_identity = record(&mut e, RecordKind::Identity, "local");
        let old_credential = record(&mut e, RecordKind::DeviceCredential, "local");
        let (plan, upload) = rotate(&mut e, None, signed, lr);
        let request = upload.request().clone();
        let saved = snapshot(&mut e, &request);
        assert_eq!(pointers(&mut e), (1, 33));
        assert_eq!(plan.rotate_signed, signed);
        assert_eq!(plan.rotate_last_resort, lr);
        assert_eq!(
            e.current_prekey_publication()
                .unwrap()
                .unwrap()
                .operation_id,
            plan.operation_id
        );
        assert!(e.prepare_prekey_rotation(!signed, !lr, None).is_err());
        assert!(
            e.prepare_prekey_refill(1, 1, Some(&plan.operation_id))
                .is_err()
        );
        let own = e.own.clone();
        drop(e);
        let mut e = reopen(root.path(), &own);
        assert_eq!(
            e.prepare_prekey_rotation(signed, lr, None)
                .unwrap()
                .operation_id,
            plan.operation_id
        );
        e.materialize_prekey_refill(&plan.operation_id).unwrap();
        assert_eq!(
            e.next_prekey_refill_upload().unwrap().unwrap().request(),
            &request
        );
        assert_eq!(snapshot(&mut e, &request), saved);
        assert_eq!(pointers(&mut e), (1, 33));
        e.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(upload, ack(&request)))
            .unwrap();
        assert_eq!(
            pointers(&mut e),
            (if signed { 2 } else { 1 }, if lr { 34 } else { 33 })
        );
        assert!(e.current_prekey_publication().unwrap().unwrap().complete);
        assert_eq!(record(&mut e, RecordKind::SignedPreKey, "1"), old_signed);
        assert_eq!(record(&mut e, RecordKind::PqPreKey, "33"), old_lr);
        assert_eq!(record(&mut e, RecordKind::Identity, "local"), old_identity);
        assert_eq!(
            record(&mut e, RecordKind::DeviceCredential, "local"),
            old_credential
        );
        let journal = record(
            &mut e,
            RecordKind::Publication,
            &format!("refill:{}", plan.operation_id),
        );
        for bytes in saved {
            assert!(!journal.windows(bytes.len() - 3).any(|w| w == &bytes[3..]));
        }
    }
}

#[test]
fn refill_rotation_share_one_cas_slot_and_old_proof_does_not_reactivate_retained_keys() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let refill = e.prepare_prekey_refill(1, 1, None).unwrap();
    assert!(e.prepare_prekey_rotation(true, true, None).is_err());
    assert!(
        e.prepare_prekey_rotation(true, true, Some(&refill.operation_id))
            .is_err()
    );
    e.materialize_prekey_refill(&refill.operation_id).unwrap();
    finish(&mut e);
    let old_v1 = record(
        &mut e,
        RecordKind::Publication,
        &format!("refill:{}", refill.operation_id),
    );
    let (first, first_upload) = rotate(&mut e, Some(&refill.operation_id), true, true);
    assert_eq!(
        first_upload
            .request()
            .signed_pre_key
            .as_ref()
            .unwrap()
            .key_id,
        2
    );
    assert_eq!(first_upload.request().kyber_pre_keys[0].key_id, 35);
    let receipt = ack(first_upload.request());
    finish(&mut e);
    let (next, _) = rotate(&mut e, Some(&first.operation_id), true, true);
    finish(&mut e);
    assert_eq!(pointers(&mut e), (3, 36));
    e.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(first_upload, receipt))
        .unwrap();
    assert_eq!(pointers(&mut e), (3, 36));
    assert_eq!(
        e.prepare_prekey_rotation(true, true, Some(&first.operation_id))
            .unwrap()
            .operation_id,
        next.operation_id
    );
    assert!(
        e.prepare_prekey_rotation(false, true, Some(&first.operation_id))
            .is_err()
    );
    let last = e
        .prepare_prekey_refill(1, 1, Some(&next.operation_id))
        .unwrap();
    e.materialize_prekey_refill(&last.operation_id).unwrap();
    let request = e.next_prekey_refill_upload().unwrap().unwrap();
    assert_eq!(request.request().kyber_pre_keys[0].key_id, 37);
    finish(&mut e);
    assert_eq!(pointers(&mut e), (3, 36));
    assert_eq!(
        record(
            &mut e,
            RecordKind::Publication,
            &format!("refill:{}", refill.operation_id)
        ),
        old_v1
    );
}

#[test]
fn delayed_old_bundle_and_committed_outbox_survive_rotation_and_new_bundle_uses_new_keys() {
    for (old_bundle, encrypt_before) in [(true, true), (true, false), (false, false)] {
        let root = tempfile::tempdir().unwrap();
        let mut alice = installed(root.path(), A);
        let mut bob = installed(root.path(), B);
        let old_signed = record(&mut bob, RecordKind::SignedPreKey, "1");
        let old_lr = record(&mut bob, RecordKind::PqPreKey, "33");
        if !old_bundle {
            let (_, u) = rotate(&mut bob, None, true, true);
            assert!(validate_upload(u.request()).is_ok());
            finish(&mut bob);
        }
        let ap = public(&mut alice);
        let bp = public(&mut bob);
        let a = scope(alice.own.clone(), bp.clone());
        let b = scope(bob.own.clone(), ap);
        let (signed_id, lr_id) = pointers(&mut bob);
        let signed = record(&mut bob, RecordKind::SignedPreKey, &signed_id.to_string());
        let lr = record(&mut bob, RecordKind::PqPreKey, &lr_id.to_string());
        let signed = SignedPreKeyRecord::deserialize(&signed[3..]).unwrap();
        let lr = KyberPreKeyRecord::deserialize(&lr[3..]).unwrap();
        let lease = alice.prepare_acquisition(&a).unwrap();
        let bundle = AcquiredBundle {
            acquisition_id: lease.request.acquisition_id,
            conversation_id: CHAT.into(),
            device: Some(bp),
            signed_pre_key: Some(SignedPreKey {
                key_id: signed_id,
                public_key: signed.public_key().unwrap().serialize().to_vec(),
                signature: signed.signature().unwrap(),
                created_at_unix_ms: signed.timestamp().unwrap().epoch_millis() as i64,
            }),
            one_time_pre_key: None,
            kyber_pre_key: Some(KyberPreKey {
                key_id: lr_id,
                public_key: lr.public_key().unwrap().serialize().to_vec(),
                signature: lr.signature().unwrap(),
                is_last_resort: true,
            }),
            not_after_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
                + 3_600_000,
        };
        let text = payload(
            &a,
            "old signed/LR material survives until delayed messages arrive",
        );
        let earlier = if encrypt_before {
            alice.bootstrap(&a, &bundle).unwrap();
            Some(alice.encrypt(&a, &text).unwrap())
        } else {
            None
        };
        if old_bundle {
            rotate(&mut bob, None, true, true);
            finish(&mut bob);
        }
        if !encrypt_before {
            alice.bootstrap(&a, &bundle).unwrap();
        }
        let envelope = alice.encrypt(&a, &text).unwrap();
        if let Some(earlier) = earlier {
            assert_eq!(envelope, earlier);
        }
        let own = bob.own.clone();
        drop(bob);
        let mut bob = reopen(root.path(), &own);
        assert_eq!(bob.decrypt(&b, &envelope).unwrap(), text);
        assert_eq!(bob.decrypt(&b, &envelope).unwrap(), text);
        let reply = payload(&b, "existing ratchet continues after rotation");
        let wire = bob.encrypt(&b, &reply).unwrap();
        assert_eq!(alice.decrypt(&a, &wire).unwrap(), reply);
        assert_eq!(record(&mut bob, RecordKind::SignedPreKey, "1"), old_signed);
        assert_eq!(record(&mut bob, RecordKind::PqPreKey, "33"), old_lr);
        assert_eq!(pointers(&mut bob), (2, 34));
    }
}

#[test]
fn rotation_login_recovers_reserved_material_and_unknown_remote_commit_without_revision_guessing() {
    for materialized in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut e = installed(root.path(), A);
        let p = e.prepare_prekey_rotation(true, true, None).unwrap();
        let request = if materialized {
            e.materialize_prekey_refill(&p.operation_id).unwrap();
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
        let mut remote = OwnDeviceState {
            user_id: A.into(),
            active_device: Some(public(&mut e)),
        };
        if materialized {
            remote
                .active_device
                .as_mut()
                .unwrap()
                .public_material_revision += 1;
        }
        let own = e.own.clone();
        drop(e);
        let mut local = DeviceProvisioning::new(store(root.path(), A, false));
        assert_eq!(
            local.reconcile(&remote).unwrap(),
            Reconciliation::PublicationPending
        );
        let recovered = local.next_upload().unwrap().unwrap();
        if let Some(request) = request {
            assert_eq!(recovered, request);
        }
        local.acknowledge_upload(&ack(&recovered)).unwrap();
        assert_eq!(local.ready_device().unwrap(), own);
        remote
            .active_device
            .as_mut()
            .unwrap()
            .public_material_revision = ack(&recovered).publication_revision;
        assert_eq!(local.reconcile(&remote).unwrap(), Reconciliation::Ready);
        drop(local);
        let mut e = reopen(root.path(), &own);
        assert_eq!(pointers(&mut e), (2, 34));
    }
}

#[test]
fn rotation_commit_failures_preserve_reservations_material_and_activation_atomically() {
    let root = tempfile::tempdir().unwrap();
    let e = installed(root.path(), A);
    let own = e.own.clone();
    drop(e);
    commit_fault(root.path(), A, RecordKind::Publication as u8, true);
    let mut e = reopen(root.path(), &own);
    assert!(matches!(
        e.prepare_prekey_rotation(true, true, None),
        Err(CryptoError::Storage(_))
    ));
    drop(e);
    commit_fault(root.path(), A, RecordKind::Publication as u8, false);
    let mut e = reopen(root.path(), &own);
    let p = e.prepare_prekey_rotation(true, true, None).unwrap();
    drop(e);
    commit_fault(root.path(), A, RecordKind::SignedPreKey as u8, true);
    let mut e = reopen(root.path(), &own);
    assert!(matches!(
        e.materialize_prekey_refill(&p.operation_id),
        Err(CryptoError::Storage(_))
    ));
    e.store
        .transaction(|tx| {
            for (kind, id) in [(RecordKind::SignedPreKey, 2), (RecordKind::PqPreKey, 34)] {
                assert!(
                    tx.get(&RecordKey::new(AG, kind, &id.to_string())?)?
                        .is_none()
                );
            }
            Ok(())
        })
        .unwrap();
    drop(e);
    commit_fault(root.path(), A, RecordKind::SignedPreKey as u8, false);
    let mut e = reopen(root.path(), &own);
    e.materialize_prekey_refill(&p.operation_id).unwrap();
    let u = e.next_prekey_refill_upload().unwrap().unwrap();
    let r = u.request().clone();
    let bytes = snapshot(&mut e, &r);
    drop(e);
    commit_fault(root.path(), A, RecordKind::Control as u8, true);
    let mut e = reopen(root.path(), &own);
    assert!(matches!(
        e.accept_prekey_refill_upload(VerifiedPreKeyRefillUpload::from_rpc(u, ack(&r))),
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(pointers(&mut e), (1, 33));
    assert_eq!(snapshot(&mut e, &r), bytes);
    drop(e);
    commit_fault(root.path(), A, RecordKind::Control as u8, false);
    let mut e = reopen(root.path(), &own);
    finish(&mut e);
    assert_eq!(pointers(&mut e), (2, 34));
}

#[test]
fn damaged_rotation_version_material_or_lr_classification_is_never_repaired() {
    for damage in 0..4 {
        let root = tempfile::tempdir().unwrap();
        let mut e = installed(root.path(), A);
        let (p, _) = rotate(&mut e, None, true, true);
        e.store
            .transaction(|tx| {
                let key = if damage == 0 {
                    RecordKey::new(
                        AG,
                        RecordKind::Publication,
                        &format!("refill:{}", p.operation_id),
                    )?
                } else {
                    RecordKey::new(AG, RecordKind::PqPreKey, "34")?
                };
                let mut bytes = tx.get(&key)?.unwrap();
                match damage {
                    0 => bytes.extend_from_slice(&[8, 1]),
                    1 => bytes[2] = 0,
                    2 => *bytes = prekey_state::tombstone(RecordKind::PqPreKey, 34, &bytes[3..]),
                    _ => bytes[3] ^= 255,
                }
                tx.put(&key, &bytes)
            })
            .unwrap();
        assert!(e.next_prekey_refill_upload().is_err());
        assert!(e.materialize_prekey_refill(&p.operation_id).is_err());
        assert!(e.current_prekey_publication().is_err());
        assert_eq!(pointers(&mut e), (1, 33));
    }
}

#[test]
fn exhausted_signed_namespace_rolls_back_last_resort_reservation_too() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let bytes = record(&mut e, RecordKind::SignedPreKey, "1");
    let key = SignedPreKeyRecord::deserialize(&bytes[3..]).unwrap();
    let max = SignedPreKeyRecord::new(
        u32::MAX.into(),
        key.timestamp().unwrap(),
        &key.key_pair().unwrap(),
        &key.signature().unwrap(),
    )
    .serialize()
    .unwrap();
    e.store
        .transaction_with::<_, CryptoError>(|tx| {
            prekey_state::save(
                tx,
                AG,
                RecordKind::SignedPreKey,
                u32::MAX,
                &max,
                Some(false),
            )
        })
        .unwrap();
    assert!(matches!(
        e.prepare_prekey_rotation(true, true, None),
        Err(CryptoError::PreKeyIdsExhausted)
    ));
    assert!(e.current_prekey_publication().unwrap().is_none());
    assert_eq!(
        e.reserve_prekey_ids(&Uuid::new_v4().to_string(), PreKeyIdNamespace::Pq, 1)
            .unwrap()
            .first,
        34
    );
}

#[tokio::test]
async fn concurrent_refill_and_rotation_requests_cannot_create_independent_plans() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        drop(installed(&path, A));
    })
    .await
    .unwrap();
    let worker = CryptoCoordinator::start(store(root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let (refill, rotation) = tokio::join!(
        worker.prepare_prekey_refill(1, 1),
        worker.prepare_prekey_rotation(true, true)
    );
    let plan = match (refill, rotation) {
        (Ok(p), Err(CryptoError::Conflict)) | (Err(CryptoError::Conflict), Ok(p)) => p,
        _ => panic!("only one publication intent may occupy the shared worker slot"),
    };
    assert_eq!(
        worker
            .current_prekey_publication()
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        plan.operation_id
    );
    worker.shutdown().await.unwrap();
}

#[test]
fn process_exit_before_and_after_each_rotation_commit_retains_old_or_complete_activation() {
    for phase in ["prepare", "materialize", "receipt"] {
        for after in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut e = installed(root.path(), A);
            let own = e.own.clone();
            let id = if phase == "prepare" {
                String::new()
            } else {
                e.prepare_prekey_rotation(true, true, None)
                    .unwrap()
                    .operation_id
            };
            let saved = if phase == "receipt" {
                e.materialize_prekey_refill(&id).unwrap();
                let r = e
                    .next_prekey_refill_upload()
                    .unwrap()
                    .unwrap()
                    .request()
                    .clone();
                let bytes = snapshot(&mut e, &r);
                Some((r, bytes))
            } else {
                None
            };
            drop(e);
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "encryption::signal_state::tests::refill::rotation::rotation_exit_child",
                    "--nocapture",
                ])
                .env("ZCHAT_ROTATION_ROOT", root.path())
                .env("ZCHAT_ROTATION_PHASE", phase)
                .env("ZCHAT_ROTATION_OPERATION", &id)
                .env("ZCHAT_ROTATION_AFTER", if after { "1" } else { "0" })
                .status()
                .unwrap();
            assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
            let mut e = reopen(root.path(), &own);
            let p = e.prepare_prekey_rotation(true, true, None).unwrap();
            if phase != "prepare" {
                assert_eq!(p.operation_id, id);
            }
            assert_eq!(
                p.materialized,
                phase == "receipt" || (phase == "materialize" && after)
            );
            assert_eq!(p.complete, phase == "receipt" && after);
            assert_eq!(
                pointers(&mut e),
                if phase == "receipt" && after {
                    (2, 34)
                } else {
                    (1, 33)
                }
            );
            e.materialize_prekey_refill(&p.operation_id).unwrap();
            if let Some((r, bytes)) = saved {
                assert_eq!(snapshot(&mut e, &r), bytes);
            }
            finish(&mut e);
            assert_eq!(pointers(&mut e), (2, 34));
        }
    }
}
#[test]
fn rotation_exit_child() {
    let Some(root) = std::env::var_os("ZCHAT_ROTATION_ROOT") else {
        return;
    };
    let phase = std::env::var("ZCHAT_ROTATION_PHASE").unwrap();
    let id = std::env::var("ZCHAT_ROTATION_OPERATION").unwrap();
    let after = std::env::var("ZCHAT_ROTATION_AFTER").unwrap() == "1";
    let mut e = reopen(Path::new(&root), &device(A, AG));
    let key = match phase.as_str() {
        "prepare" => RecordKey::new(AG, RecordKind::Control, "prekey-refill:head:v1").unwrap(),
        "materialize" => RecordKey::new(AG, RecordKind::SignedPreKey, "2").unwrap(),
        _ => RecordKey::new(AG, RecordKind::Control, "published:signed:2").unwrap(),
    };
    e.store.test_exit_on_record(key, after);
    match phase.as_str() {
        "prepare" => {
            let _ = e.prepare_prekey_rotation(true, true, None);
        }
        "materialize" => {
            let _ = e.materialize_prekey_refill(&id);
        }
        _ => {
            finish(&mut e);
        }
    }
    panic!("rotation exit hook not reached");
}
