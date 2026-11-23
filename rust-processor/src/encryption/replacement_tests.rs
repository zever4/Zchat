use super::*;
use crate::encryption::protected::{StoreError, StoreScope, test_open};
use crate::proto::e2ee::Device;
use std::path::Path;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const G: &str = "33333333-3333-4333-8333-333333333333";
const H: &str = "44444444-4444-4444-8444-444444444444";
const J: &str = "55555555-5555-4555-8555-555555555555";
const SERVER: &str = "https://replacement.example.test";
fn scope(user: &str) -> StoreScope {
    StoreScope::new(SERVER, user).unwrap()
}
fn open(root: &Path, user: &str, create: bool) -> DeviceProvisioning {
    DeviceProvisioning::new(test_open(root, scope(user), create))
}
fn receipt(request: &RegisterDeviceRequest, generation: &str) -> OwnDeviceState {
    OwnDeviceState {
        user_id: A.into(),
        active_device: Some(Device {
            reference: Some(DeviceRef {
                user_id: A.into(),
                device_id: 1,
                generation_id: generation.into(),
            }),
            registration_id: request.registration_id,
            identity_key: request.identity_key.clone(),
            public_material_ready: false,
            public_material_revision: 0,
        }),
    }
}
fn publish(p: &mut DeviceProvisioning) {
    while let Some(batch) = p.next_upload().unwrap() {
        let receipt = UploadReceipt {
            batch_id: batch.batch_id.clone(),
            batch_digest: super::super::super::publication::publication_digest(&batch)
                .unwrap()
                .to_vec(),
            publication_revision: batch.expected_revision + 1,
        };
        p.acknowledge_upload(&receipt).unwrap();
    }
}
fn initialized(root: &Path) -> DeviceProvisioning {
    let mut p = open(root, A, true);
    let attempt = p.prepare_registration().unwrap();
    p.bind_registration(
        &attempt.request.registration_request_id,
        &receipt(&attempt.request, G),
    )
    .unwrap();
    publish(&mut p);
    p
}
fn db(root: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(scope(A).database_path(root)).unwrap()
}
fn wrapped(root: &Path) -> Vec<u8> {
    db(root)
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap()
}
fn rows(root: &Path, generation: &str) -> Vec<(i64, String, Vec<u8>)> {
    let id = Uuid::parse_str(generation).unwrap();
    db(root).prepare("SELECT kind,record_id,payload FROM protected_records WHERE generation=?1 AND record_id!='archived-device' ORDER BY kind,record_id").unwrap()
        .query_map([id.as_bytes().as_slice()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap().collect::<Result<_, _>>().unwrap()
}
fn fault(root: &Path, account: bool, install: bool) {
    let db = db(root);
    if install {
        let target = if account {
            "protected_account_records"
        } else {
            "protected_records"
        };
        db.execute_batch(&format!("CREATE TABLE fault_parent(id INTEGER PRIMARY KEY); CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
            CREATE TRIGGER replacement_fault AFTER INSERT ON {target} BEGIN INSERT INTO fault_child VALUES(1); END;")).unwrap();
    } else {
        db.execute_batch(
            "DROP TRIGGER replacement_fault; DROP TABLE fault_child; DROP TABLE fault_parent;",
        )
        .unwrap();
    }
}

#[test]
fn replacement_material_cas_and_credential_survive_reopen_and_account_isolation() {
    let root = tempfile::tempdir().unwrap();
    let mut a = open(root.path(), A, true);
    let mut b = open(root.path(), B, true);
    let first = a.prepare_replacement(G, None).unwrap();
    let other = b.prepare_replacement(G, None).unwrap();
    assert_ne!(first.credential(), other.credential());
    assert_ne!(first.request().identity_key, other.request().identity_key);
    assert!(a.prepare_registration().is_err());
    let mut remote = receipt(first.request(), G);
    remote.active_device.as_mut().unwrap().identity_key[1] ^= 1;
    assert!(matches!(
        a.reconcile(&remote).unwrap(),
        Reconciliation::ReplacementPending { .. }
    ));
    let key_before = wrapped(root.path());
    let raw = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert!(!raw.windows(32).any(|bytes| bytes == first.credential()));
    drop((a, b));
    let mut a = open(root.path(), A, false);
    let again = a.prepare_replacement(G, None).unwrap();
    assert_eq!(first.request(), again.request());
    assert_eq!(first.credential(), again.credential());
    assert_eq!(again.expected_generation_id(), G);
    assert_eq!(wrapped(root.path()), key_before);
}

#[test]
fn retargeting_requires_pending_request_cas_and_archives_its_private_material() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let first = p.prepare_replacement(G, None).unwrap();
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert!(p.prepare_replacement(H, None).is_err());
    assert!(p.prepare_replacement(H, Some(J)).is_err());
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    let next = p
        .prepare_replacement(H, Some(&first.request().registration_request_id))
        .unwrap();
    assert_ne!(next.request(), first.request());
    assert_ne!(next.credential(), first.credential());
    let archived = p
        .store
        .transaction_with(|tx| {
            load_pending_at(
                tx,
                &AccountKey::new(
                    AccountKind::Registration,
                    &format!("archived:{}", first.request().registration_request_id),
                )?,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(archived.credential.as_slice(), first.credential());
    assert_eq!(archived.request.as_ref().unwrap(), first.request());
    assert_eq!(p.replacement_attempt().unwrap().request(), next.request());
}

#[test]
fn binding_archives_old_generation_without_changing_any_existing_ciphertext_or_master_key() {
    let root = tempfile::tempdir().unwrap();
    let mut p = initialized(root.path());
    p.store
        .transaction(|tx| {
            tx.put(
                &RecordKey::new(G, RecordKind::History, "old-history")?,
                b"readable old history",
            )?;
            tx.put(
                &RecordKey::new(G, RecordKind::Outbox, "old-outbox")?,
                b"uncertain old ciphertext",
            )
        })
        .unwrap();
    let before = rows(root.path(), G);
    let key_before = wrapped(root.path());
    let old = p.prepare_registration().unwrap();
    let new = p.prepare_replacement(G, None).unwrap();
    assert_ne!(old.credential(), new.credential());
    let state = receipt(new.request(), H);
    p.bind_replacement(&new.request().registration_request_id, &state)
        .unwrap();
    assert_eq!(rows(root.path(), G), before);
    assert_eq!(wrapped(root.path()), key_before);
    assert_eq!(
        p.store
            .transaction(|tx| tx.get(&RecordKey::new(G, RecordKind::History, "old-history")?))
            .unwrap()
            .unwrap()
            .as_slice(),
        b"readable old history"
    );
    let archive = p
        .store
        .transaction(|tx| {
            tx.get(&RecordKey::new(
                G,
                RecordKind::Control,
                ARCHIVED_DEVICE_RECORD,
            )?)
        })
        .unwrap()
        .unwrap();
    let archive = ArchivedDevice::decode(archive.as_slice()).unwrap();
    assert_eq!(archive.replaced_by.unwrap().generation_id, H);
    assert_eq!(
        archive
            .previous
            .unwrap()
            .active_device
            .unwrap()
            .reference
            .unwrap()
            .generation_id,
        G
    );
    assert!(p.ready_device().is_err());
    publish(&mut p);
    assert_eq!(p.ready_device().unwrap().generation_id, H);
    let again = p.replacement_attempt().unwrap();
    assert_eq!(new.request(), again.request());
    assert_eq!(new.credential(), again.credential());
    let before_replay = std::fs::read(scope(A).database_path(root.path())).unwrap();
    p.bind_replacement(&again.request().registration_request_id, &state)
        .unwrap();
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before_replay
    );
    assert!(
        p.bind_registration(&again.request().registration_request_id, &state)
            .is_err()
    );
    assert!(
        p.store
            .transaction_with(load_pending)
            .unwrap()
            .unwrap()
            .identity
            .is_empty()
    );
}

#[test]
fn replacement_reply_must_match_request_account_keys_cas_and_fresh_unpublished_generation() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let attempt = p.prepare_replacement(G, None).unwrap();
    let correct = receipt(attempt.request(), H);
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    for mutation in 0..6 {
        let mut bad = correct.clone();
        let d = bad.active_device.as_mut().unwrap();
        match mutation {
            0 => {
                bad.user_id = B.into();
                d.reference.as_mut().unwrap().user_id = B.into();
            }
            1 => d.identity_key[1] ^= 1,
            2 => d.registration_id = if d.registration_id == 1 { 2 } else { 1 },
            3 => d.reference.as_mut().unwrap().generation_id = G.into(),
            4 => {
                d.public_material_ready = true;
                d.public_material_revision = 1;
            }
            _ => d.public_material_revision = 1,
        }
        assert!(
            p.bind_replacement(&attempt.request().registration_request_id, &bad)
                .is_err()
        );
    }
    assert!(p.bind_replacement(J, &correct).is_err());
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    p.bind_replacement(&attempt.request().registration_request_id, &correct)
        .unwrap();
    let mut changed = correct;
    changed
        .active_device
        .as_mut()
        .unwrap()
        .reference
        .as_mut()
        .unwrap()
        .generation_id = J.into();
    assert!(
        p.bind_replacement(&attempt.request().registration_request_id, &changed)
            .is_err()
    );
}

#[test]
fn replacement_commit_failure_rolls_back_binding_archive_and_private_installation() {
    let root = tempfile::tempdir().unwrap();
    let mut p = initialized(root.path());
    let attempt = p.prepare_replacement(G, None).unwrap();
    let old = rows(root.path(), G);
    let state = receipt(attempt.request(), H);
    fault(root.path(), false, true);
    assert!(matches!(
        p.bind_replacement(&attempt.request().registration_request_id, &state),
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    assert_eq!(p.ready_device().unwrap().generation_id, G);
    assert_eq!(rows(root.path(), G), old);
    assert!(rows(root.path(), H).is_empty());
    assert_eq!(
        p.replacement_attempt().unwrap().credential(),
        attempt.credential()
    );
    fault(root.path(), false, false);
    p.bind_replacement(&attempt.request().registration_request_id, &state)
        .unwrap();
}

#[test]
fn failed_preparation_commit_does_not_return_or_persist_an_attempt() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    fault(root.path(), true, true);
    assert!(p.prepare_replacement(G, None).is_err());
    assert!(p.store.transaction_with(load).unwrap().is_none());
    fault(root.path(), true, false);
    assert!(p.prepare_replacement(G, None).is_ok());
}

#[test]
fn replacing_an_unknown_initial_registration_preserves_its_unbound_private_journal() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let initial = p.prepare_registration().unwrap();
    let replacement = p.prepare_replacement(G, None).unwrap();
    p.bind_replacement(
        &replacement.request().registration_request_id,
        &receipt(replacement.request(), H),
    )
    .unwrap();
    let archived = p
        .store
        .transaction_with(|tx| {
            load_pending_at(
                tx,
                &AccountKey::new(
                    AccountKind::Registration,
                    &format!("archived:{}", initial.request.registration_request_id),
                )?,
            )
        })
        .unwrap()
        .unwrap();
    assert_eq!(archived.request.as_ref().unwrap(), &initial.request);
    assert_eq!(archived.credential.as_slice(), initial.credential());
    assert!(!archived.identity.is_empty());
    assert_eq!(
        p.prepare_registration()
            .unwrap()
            .bound_device
            .unwrap()
            .generation_id,
        H
    );
}

#[tokio::test]
async fn repeated_replacement_retains_both_archives_and_only_new_generation_can_start_worker() {
    let root = tempfile::tempdir().unwrap();
    let mut p = initialized(root.path());
    for (expected, next) in [(G, H), (H, J)] {
        let before = rows(root.path(), expected);
        let attempt = p.prepare_replacement(expected, None).unwrap();
        p.bind_replacement(
            &attempt.request().registration_request_id,
            &receipt(attempt.request(), next),
        )
        .unwrap();
        assert_eq!(rows(root.path(), expected), before);
        publish(&mut p);
    }
    let active = p.ready_device().unwrap();
    drop(p);
    for generation in [G, H] {
        let old = DeviceRef {
            user_id: A.into(),
            device_id: 1,
            generation_id: generation.into(),
        };
        assert!(matches!(
            super::super::super::signal_state::CryptoCoordinator::start(
                test_open(root.path(), scope(A), false),
                old
            )
            .await,
            Err(CryptoError::ArchivedGeneration)
        ));
    }
    let worker = super::super::super::signal_state::CryptoCoordinator::start(
        test_open(root.path(), scope(A), false),
        active,
    )
    .await
    .unwrap();
    worker.shutdown().await.unwrap();
}

#[test]
fn process_exit_before_and_after_replacement_bind_commit_keeps_old_or_complete_new_state() {
    for after in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut p = initialized(root.path());
        let attempt = p.prepare_replacement(G, None).unwrap();
        let state = receipt(attempt.request(), H);
        drop(p);
        std::fs::write(
            root.path().join("replacement-receipt.bin"),
            state.encode_to_vec(),
        )
        .unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::provisioning::replacement::tests::replacement_crash_worker",
                "--nocapture",
            ])
            .env("ZCHAT_REPLACEMENT_CHILD_ROOT", root.path())
            .env(
                "ZCHAT_REPLACEMENT_CHILD_AFTER",
                if after { "yes" } else { "no" },
            )
            .output()
            .unwrap();
        assert_eq!(
            child.status.code(),
            Some(if after { 74 } else { 73 }),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        let mut p = open(root.path(), A, false);
        assert_eq!(
            p.store
                .transaction_with(load_pending)
                .unwrap()
                .unwrap()
                .bound
                .as_ref()
                .unwrap()
                .generation_id,
            if after { H } else { G }
        );
        assert_eq!(
            p.replacement_attempt().unwrap().credential(),
            attempt.credential()
        );
        p.bind_replacement(&attempt.request().registration_request_id, &state)
            .unwrap();
        publish(&mut p);
        assert_eq!(p.ready_device().unwrap().generation_id, H);
    }
}

#[test]
fn replacement_crash_worker() {
    let Some(root) = std::env::var_os("ZCHAT_REPLACEMENT_CHILD_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let receipt = OwnDeviceState::decode(
        std::fs::read(root.join("replacement-receipt.bin"))
            .unwrap()
            .as_slice(),
    )
    .unwrap();
    let mut p = open(&root, A, false);
    let attempt = p.replacement_attempt().unwrap();
    p.store.test_exit_on_record(
        RecordKey::new(H, RecordKind::Identity, "local").unwrap(),
        std::env::var("ZCHAT_REPLACEMENT_CHILD_AFTER").unwrap() == "yes",
    );
    p.bind_replacement(&attempt.request().registration_request_id, &receipt)
        .unwrap();
    panic!("replacement fault hook did not terminate child");
}

#[test]
fn archived_public_locator_matches_actual_replacement_and_rejects_current_or_foreign_account() {
    let root = tempfile::tempdir().unwrap();
    let mut p = initialized(root.path());
    let old = p.ready_device().unwrap();
    let next = p.prepare_replacement(G, None).unwrap();
    p.bind_replacement(
        &next.request().registration_request_id,
        &receipt(next.request(), H),
    )
    .unwrap();
    let before = rows(root.path(), G);
    let successor = p
        .store
        .transaction_with::<_, CryptoError>(|tx| read_archived_device(tx, &old))
        .unwrap();
    assert_eq!(successor.generation_id, H);
    assert!(
        p.store
            .transaction_with::<_, CryptoError>(|tx| read_archived_device(tx, &successor))
            .is_err()
    );
    let mut foreign = old.clone();
    foreign.user_id = B.into();
    assert!(
        p.store
            .transaction_with::<_, CryptoError>(|tx| read_archived_device(tx, &foreign))
            .is_err()
    );
    assert_eq!(rows(root.path(), G), before);
    // Tamper with an authenticated but inconsistent snapshot: it must not turn
    // a different identity into a readable local archive locator.
    p.store
        .transaction_with::<_, CryptoError>(|tx| {
            let key = RecordKey::new(G, RecordKind::Control, ARCHIVED_DEVICE_RECORD)?;
            let mut v = ArchivedDevice::decode(tx.get(&key)?.unwrap().as_slice()).unwrap();
            v.previous
                .as_mut()
                .unwrap()
                .active_device
                .as_mut()
                .unwrap()
                .identity_key[1] ^= 1;
            tx.put(&key, &v.encode_to_vec())?;
            Ok(())
        })
        .unwrap();
    assert!(
        p.store
            .transaction_with::<_, CryptoError>(|tx| read_archived_device(tx, &old))
            .is_err()
    );
}
