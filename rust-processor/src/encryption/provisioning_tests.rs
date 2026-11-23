use super::super::protected::{StoreError, StoreScope, test_open};
use super::super::publication::publication_digest;
use super::*;
use crate::proto::e2ee::Device;
use std::path::Path;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const G: &str = "33333333-3333-4333-8333-333333333333";
const SERVER: &str = "https://chat.example.test";
fn scope(user: &str) -> StoreScope {
    StoreScope::new(SERVER, user).unwrap()
}
fn open(root: &Path, user: &str, create: bool) -> DeviceProvisioning {
    DeviceProvisioning::new(test_open(root, scope(user), create))
}
fn receipt(attempt: &RegistrationAttempt, user: &str) -> OwnDeviceState {
    OwnDeviceState {
        user_id: user.into(),
        active_device: Some(Device {
            reference: Some(DeviceRef {
                user_id: user.into(),
                device_id: 1,
                generation_id: G.into(),
            }),
            registration_id: attempt.request.registration_id,
            identity_key: attempt.request.identity_key.clone(),
            public_material_ready: false,
            public_material_revision: 0,
        }),
    }
}
fn ack(req: &UploadPreKeysRequest) -> UploadReceipt {
    UploadReceipt {
        batch_id: req.batch_id.clone(),
        batch_digest: publication_digest(req).unwrap().to_vec(),
        publication_revision: req.expected_revision + 1,
    }
}

#[test]
fn reconciliation_observes_pending_and_lost_register_without_binding_or_regenerating() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let none = OwnDeviceState {
        user_id: A.into(),
        active_device: None,
    };
    assert_eq!(p.reconcile(&none).unwrap(), Reconciliation::Unregistered);
    assert_eq!(
        db(root.path())
            .query_row("SELECT count(*) FROM protected_account_records", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    let attempt = p.prepare_registration().unwrap();
    assert_eq!(
        p.reconcile(&none).unwrap(),
        Reconciliation::RegistrationPending
    );
    let registered = receipt(&attempt, A);
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert_eq!(
        p.reconcile(&registered).unwrap(),
        Reconciliation::RegistrationPending
    );
    // Observation alone cannot bind generation or install private state.
    assert!(p.prepare_registration().unwrap().bound_device.is_none());
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    drop(p);
    let mut p = open(root.path(), A, false);
    let again = p.prepare_registration().unwrap();
    assert_eq!(again.request, attempt.request);
    assert_eq!(again.credential(), attempt.credential());
    p.bind_registration(&again.request.registration_request_id, &registered)
        .unwrap();
    assert_eq!(
        p.reconcile(&registered).unwrap(),
        Reconciliation::PublicationPending
    );
}

#[test]
fn reconciliation_requires_exact_publication_receipt_and_detects_rollback() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let a = p.prepare_registration().unwrap();
    let mut remote = receipt(&a, A);
    p.bind_registration(&a.request.registration_request_id, &remote)
        .unwrap();
    while let Some(req) = p.next_upload().unwrap() {
        let confirmation = ack(&req);
        remote
            .active_device
            .as_mut()
            .unwrap()
            .public_material_revision = confirmation.publication_revision;
        remote.active_device.as_mut().unwrap().public_material_ready = true;
        // Go committed, but local acknowledgement is missing: replay the saved batch.
        assert_eq!(
            p.reconcile(&remote).unwrap(),
            Reconciliation::PublicationPending
        );
        assert_eq!(p.next_upload().unwrap().unwrap(), req);
        p.acknowledge_upload(&confirmation).unwrap();
    }
    assert_eq!(p.reconcile(&remote).unwrap(), Reconciliation::Ready);
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    for kind in 0..4 {
        let mut bad = remote.clone();
        let d = bad.active_device.as_mut().unwrap();
        match kind {
            0 => d.public_material_revision -= 1,
            1 => d.public_material_revision += 1,
            2 => d.identity_key[1] ^= 1,
            _ => d.registration_id = if d.registration_id == 1 { 2 } else { 1 },
        }
        assert!(p.reconcile(&bad).is_err(), "mutation {kind}");
    }
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    drop(p);
    let mut p = open(root.path(), A, false);
    assert_eq!(p.reconcile(&remote).unwrap(), Reconciliation::Ready);
    assert_eq!(
        p.prepare_registration().unwrap().credential(),
        a.credential()
    );
}

#[test]
fn remote_replacement_or_missing_generation_preserves_local_keys_and_archive() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let a = p.prepare_registration().unwrap();
    let remote = receipt(&a, A);
    p.bind_registration(&a.request.registration_request_id, &remote)
        .unwrap();
    let own = remote
        .active_device
        .as_ref()
        .unwrap()
        .reference
        .as_ref()
        .unwrap()
        .clone();
    let history = RecordKey::new(G, RecordKind::History, "archive").unwrap();
    p.store
        .transaction(|tx| tx.put(&history, b"keep local history"))
        .unwrap();
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    let mut replaced = remote.clone();
    let active = replaced
        .active_device
        .as_mut()
        .unwrap()
        .reference
        .as_mut()
        .unwrap();
    active.generation_id = Uuid::new_v4().to_string();
    let active = active.clone();
    assert_eq!(
        p.reconcile(&replaced).unwrap(),
        Reconciliation::DeviceUnavailable {
            local_device: own.clone(),
            active_device: Some(active)
        }
    );
    replaced.active_device = None;
    assert_eq!(
        p.reconcile(&replaced).unwrap(),
        Reconciliation::DeviceUnavailable {
            local_device: own,
            active_device: None
        }
    );
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    assert_eq!(
        p.store
            .transaction(|tx| tx.get(&history))
            .unwrap()
            .unwrap()
            .as_slice(),
        b"keep local history"
    );
    assert_eq!(p.prepare_registration().unwrap().request, a.request);
}

#[test]
fn new_installation_conflicts_without_generating_keys_or_adopting_remote_identity() {
    let source = tempfile::tempdir().unwrap();
    let mut source = open(source.path(), A, true);
    let remote = receipt(&source.prepare_registration().unwrap(), A);
    let active_device = remote
        .active_device
        .as_ref()
        .unwrap()
        .reference
        .as_ref()
        .unwrap()
        .clone();
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert_eq!(
        p.reconcile(&remote).unwrap(),
        Reconciliation::ReplacementRequired {
            active_device: active_device.clone()
        }
    );
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    let pending = p.prepare_registration().unwrap();
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert_eq!(
        p.reconcile(&remote).unwrap(),
        Reconciliation::ReplacementRequired { active_device }
    );
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
    assert_eq!(p.prepare_registration().unwrap().request, pending.request);
    let mut foreign = remote;
    foreign.user_id = B.into();
    foreign
        .active_device
        .as_mut()
        .unwrap()
        .reference
        .as_mut()
        .unwrap()
        .user_id = B.into();
    assert!(p.reconcile(&foreign).is_err());
}

#[test]
fn inconsistent_pending_and_generation_records_are_not_repaired_by_reconciliation() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let a = p.prepare_registration().unwrap();
    p.store
        .transaction(|tx| {
            tx.put(
                &RecordKey::new(G, RecordKind::Control, "partial")?,
                b"partial",
            )
        })
        .unwrap();
    let before = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert!(p.reconcile(&receipt(&a, A)).is_err());
    assert_eq!(
        std::fs::read(scope(A).database_path(root.path())).unwrap(),
        before
    );
}
fn db(root: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(scope(A).database_path(root)).unwrap()
}
fn fault(root: &Path, table: &str, install: bool) {
    let db = db(root);
    if install {
        db.execute_batch(&format!("CREATE TABLE fault_parent(id INTEGER PRIMARY KEY);
        CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER provision_fault AFTER INSERT ON {table} BEGIN INSERT INTO fault_child VALUES(1); END;")).unwrap();
    } else {
        db.execute_batch(
            "DROP TRIGGER provision_fault; DROP TABLE fault_child; DROP TABLE fault_parent;",
        )
        .unwrap();
    }
}

#[test]
fn registration_reopen_preserves_request_identity_and_credential_with_account_isolation() {
    let root = tempfile::tempdir().unwrap();
    let mut a = open(root.path(), A, true);
    let mut b = open(root.path(), B, true);
    let first = a.prepare_registration().unwrap();
    let other = b.prepare_registration().unwrap();
    assert_ne!(first.request.identity_key, other.request.identity_key);
    assert_ne!(first.credential(), other.credential());
    let again = a.prepare_registration().unwrap();
    assert_eq!(first.request, again.request);
    assert_eq!(first.credential(), again.credential());
    assert!(first.bound_device.is_none());
    let saved = a
        .store
        .transaction_with(|tx| {
            let p = load_pending(tx)?.unwrap();
            Ok::<_, CryptoError>(Zeroizing::new(p.encode_to_vec()))
        })
        .unwrap();
    drop((a, b));
    let mut a = open(root.path(), A, false);
    let resumed = a.prepare_registration().unwrap();
    assert_eq!(resumed.request, first.request);
    assert_eq!(resumed.credential(), first.credential());
    let saved2 = a
        .store
        .transaction_with(|tx| {
            let p = load_pending(tx)?.unwrap();
            Ok::<_, CryptoError>(Zeroizing::new(p.encode_to_vec()))
        })
        .unwrap();
    assert_eq!(saved.as_slice(), saved2.as_slice());
    let raw = std::fs::read(scope(A).database_path(root.path())).unwrap();
    assert!(!raw.windows(32).any(|v| v == first.credential()));
    let pending = Pending::decode(saved.as_slice()).unwrap();
    assert!(
        !raw.windows(pending.identity.len())
            .any(|v| v == pending.identity)
    );
    assert_eq!(format!("{pending:?}"), "Pending([redacted])");
}

#[test]
fn failed_prepare_commit_returns_no_attempt_and_leaves_no_partial_state() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    fault(root.path(), "protected_account_records", true);
    assert!(matches!(
        p.prepare_registration(),
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    assert_eq!(
        db(root.path())
            .query_row("SELECT count(*) FROM protected_account_records", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
        0
    );
    fault(root.path(), "protected_account_records", false);
    assert!(p.prepare_registration().is_ok());
}

#[test]
fn bind_validates_account_request_identity_generation_and_never_replaces() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let prepared = p.prepare_registration().unwrap();
    let correct = receipt(&prepared, A);
    for case in 0..9 {
        let mut bad = correct.clone();
        match case {
            0 => bad.user_id = B.into(),
            1 => bad.active_device.as_mut().unwrap().identity_key[4] ^= 1,
            2 => bad.active_device.as_mut().unwrap().registration_id += 1,
            3 => {
                bad.active_device
                    .as_mut()
                    .unwrap()
                    .reference
                    .as_mut()
                    .unwrap()
                    .user_id = B.into()
            }
            4 => {
                bad.active_device
                    .as_mut()
                    .unwrap()
                    .reference
                    .as_mut()
                    .unwrap()
                    .generation_id = "invalid".into()
            }
            5 => {
                bad.active_device
                    .as_mut()
                    .unwrap()
                    .reference
                    .as_mut()
                    .unwrap()
                    .device_id = 2
            }
            6 => bad.active_device.as_mut().unwrap().public_material_ready = true,
            7 => bad.active_device.as_mut().unwrap().public_material_revision = 1,
            _ => bad.active_device = None,
        }
        assert!(
            p.bind_registration(&prepared.request.registration_request_id, &bad)
                .is_err(),
            "case{case}"
        );
        assert!(
            !p.store
                .transaction(|tx| tx.has_generation_records())
                .unwrap()
        );
    }
    assert!(
        p.bind_registration(&Uuid::new_v4().to_string(), &correct)
            .is_err()
    );
    p.bind_registration(&prepared.request.registration_request_id, &correct)
        .unwrap();
    let first = p.next_upload().unwrap().unwrap();
    let mut newer = correct.clone();
    newer.active_device.as_mut().unwrap().public_material_ready = true;
    newer
        .active_device
        .as_mut()
        .unwrap()
        .public_material_revision = 2;
    p.bind_registration(&prepared.request.registration_request_id, &newer)
        .unwrap();
    assert_eq!(p.next_upload().unwrap().unwrap(), first);
    let mut other = correct;
    other
        .active_device
        .as_mut()
        .unwrap()
        .reference
        .as_mut()
        .unwrap()
        .generation_id = Uuid::new_v4().to_string();
    assert!(matches!(
        p.bind_registration(&prepared.request.registration_request_id, &other),
        Err(CryptoError::Conflict)
    ));
    assert_eq!(p.next_upload().unwrap().unwrap(), first);
}

#[test]
fn bind_commit_failure_rolls_back_private_install_plan_active_and_pending_cleanup() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let prepared = p.prepare_registration().unwrap();
    let response = receipt(&prepared, A);
    fault(root.path(), "protected_records", true);
    assert!(matches!(
        p.bind_registration(&prepared.request.registration_request_id, &response),
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    assert!(
        !p.store
            .transaction(|tx| tx.has_generation_records())
            .unwrap()
    );
    assert!(
        p.store
            .transaction(
                |tx| tx.get_account(&AccountKey::new(AccountKind::ActiveDevice, "initial")?)
            )
            .unwrap()
            .is_none()
    );
    let resumed = p.prepare_registration().unwrap();
    assert_eq!(resumed.request, prepared.request);
    assert_eq!(resumed.credential(), prepared.credential());
    assert!(resumed.bound_device.is_none());
    drop(p);
    let mut p = open(root.path(), A, false);
    fault(root.path(), "protected_records", false);
    p.bind_registration(&prepared.request.registration_request_id, &response)
        .unwrap();
    p.store
        .transaction_with(|tx| {
            let pending = load_pending(tx)?.unwrap();
            assert!(pending.bound.is_some());
            assert!(pending.identity.is_empty());
            assert!(pending.credential.is_empty());
            assert!(pending.ec.is_empty());
            assert!(pending.pq.is_empty());
            let identity = tx
                .get(&RecordKey::new(G, RecordKind::Identity, "local")?)?
                .unwrap();
            assert_eq!(
                &identity[1..5],
                &prepared.request.registration_id.to_be_bytes()
            );
            assert_eq!(
                IdentityKeyPair::try_from(&identity[5..])
                    .unwrap()
                    .public_key()
                    .serialize()
                    .as_ref(),
                prepared.request.identity_key
            );
            assert!(
                load_progress(
                    tx,
                    response
                        .active_device
                        .as_ref()
                        .unwrap()
                        .reference
                        .as_ref()
                        .unwrap()
                )?
                .requests
                .len()
                    > 1
            );
            Ok::<_, CryptoError>(())
        })
        .unwrap();
    let resumed = p.prepare_registration().unwrap();
    assert_eq!(resumed.credential(), prepared.credential());
    assert!(resumed.bound_device.is_some());
}

#[test]
fn publication_resume_validates_receipt_prefix_and_preserves_consumed_key_tombstones() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let prepared = p.prepare_registration().unwrap();
    let response = receipt(&prepared, A);
    p.bind_registration(&prepared.request.registration_request_id, &response)
        .unwrap();
    let first = p.next_upload().unwrap().unwrap();
    assert!(first.encoded_len() <= 48 * 1024);
    assert!(p.ready_device().is_err());
    let own = response
        .active_device
        .as_ref()
        .unwrap()
        .reference
        .as_ref()
        .unwrap();
    let all = p
        .store
        .transaction_with(|tx| load_progress(tx, own))
        .unwrap();
    assert_eq!(all.requests.len(), 2);
    assert!(p.acknowledge_upload(&ack(&all.requests[1])).is_err());
    let correct = ack(&first);
    let mut bad = correct.clone();
    bad.batch_digest[0] ^= 1;
    assert!(p.acknowledge_upload(&bad).is_err());
    assert_eq!(p.next_upload().unwrap().unwrap(), first);
    // Publication state must not revive a prekey consumed by a concurrent inbound message.
    let private = RecordKey::new(G, RecordKind::EcPreKey, "1").unwrap();
    p.store
        .transaction(|tx| {
            let mut bytes = tx.get(&private)?.unwrap();
            bytes[1] = 1;
            tx.put(&private, &bytes)
        })
        .unwrap();
    // Explicit legacy upgrade may run between publication RPCs. Replaying the
    // publication plan/registration receipt must never restore the private key.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let worker = rt
        .block_on(crate::encryption::signal_state::CryptoCoordinator::start(
            p.into_store(),
            own.clone(),
        ))
        .unwrap();
    let mut after = None;
    let mut redacted = 0;
    loop {
        let page = rt.block_on(worker.redact_consumed_prekeys(after)).unwrap();
        redacted += page.redacted;
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(redacted, 1);
    rt.block_on(worker.shutdown()).unwrap();
    let mut p = open(root.path(), A, false);
    p.acknowledge_upload(&correct).unwrap();
    let next = p.next_upload().unwrap().unwrap();
    assert_eq!(next, all.requests[1]);
    p.acknowledge_upload(&correct).unwrap();
    assert_eq!(p.next_upload().unwrap().unwrap(), next);
    bad = correct.clone();
    bad.publication_revision += 1;
    assert!(p.acknowledge_upload(&bad).is_err());
    drop(p);
    let mut p = open(root.path(), A, false);
    assert_eq!(p.next_upload().unwrap().unwrap(), next);
    p.bind_registration(&prepared.request.registration_request_id, &response)
        .unwrap();
    assert_eq!(p.next_upload().unwrap().unwrap(), next);
    p.acknowledge_upload(&ack(&next)).unwrap();
    assert!(p.next_upload().unwrap().is_none());
    assert_eq!(p.ready_device().unwrap(), *own);
    p.acknowledge_upload(&correct).unwrap();
    assert!(p.next_upload().unwrap().is_none());
    assert_eq!(
        p.store.transaction(|tx| tx.get(&private)).unwrap().unwrap()[1],
        1
    );
    assert_eq!(
        p.store
            .transaction(|tx| tx.get(&private))
            .unwrap()
            .unwrap()
            .len(),
        35
    );
    assert!(
        p.store
            .transaction(|tx| tx.get(&RecordKey::new(G, RecordKind::Control, "published:ec:1")?))
            .unwrap()
            .is_some()
    );
}

#[test]
fn publication_commit_failure_does_not_advance_progress_or_mark_active() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    let prepared = p.prepare_registration().unwrap();
    let response = receipt(&prepared, A);
    p.bind_registration(&prepared.request.registration_request_id, &response)
        .unwrap();
    let first = p.next_upload().unwrap().unwrap();
    fault(root.path(), "protected_records", true);
    assert!(p.acknowledge_upload(&ack(&first)).is_err());
    assert_eq!(p.next_upload().unwrap().unwrap(), first);
    p.store
        .transaction_with(|tx| {
            let active = load_active(tx)?;
            let device = active.active_device.unwrap();
            assert!(!device.public_material_ready);
            assert_eq!(device.public_material_revision, 0);
            assert!(
                tx.get(&RecordKey::new(G, RecordKind::Control, "active-signed")?)?
                    .is_none()
            );
            Ok::<_, CryptoError>(())
        })
        .unwrap();
    fault(root.path(), "protected_records", false);
    p.acknowledge_upload(&ack(&first)).unwrap();
}

#[test]
fn unmanaged_generation_or_corrupt_pending_never_causes_automatic_identity_regeneration() {
    let root = tempfile::tempdir().unwrap();
    let mut p = open(root.path(), A, true);
    p.store
        .transaction(|tx| {
            tx.put(
                &RecordKey::new(G, RecordKind::Identity, "local")?,
                b"saved identity",
            )
        })
        .unwrap();
    assert!(matches!(
        p.prepare_registration(),
        Err(CryptoError::Conflict)
    ));
    let mut b = open(root.path(), B, true);
    b.prepare_registration().unwrap();
    b.store
        .transaction(|tx| {
            tx.put_account(
                &AccountKey::new(AccountKind::Registration, "initial")?,
                b"broken",
            )
        })
        .unwrap();
    assert!(matches!(
        b.prepare_registration(),
        Err(CryptoError::Invalid)
    ));
    assert_eq!(
        b.store
            .transaction(
                |tx| tx.get_account(&AccountKey::new(AccountKind::Registration, "initial")?)
            )
            .unwrap()
            .unwrap()
            .as_slice(),
        b"broken"
    );
}

fn child(root: &Path, after: bool) -> std::process::Output {
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "encryption::provisioning::tests::process_bind_worker",
            "--nocapture",
        ])
        .env("ZCHAT_PROVISION_FAULT_ROOT", root)
        .env("ZCHAT_PROVISION_AFTER", if after { "1" } else { "0" })
        .output()
        .unwrap()
}
#[test]
fn process_exit_before_after_bind_commit_keeps_pending_or_exact_bound_plan() {
    for after in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut p = open(root.path(), A, true);
        let original = p.prepare_registration().unwrap();
        drop(p);
        assert_eq!(
            child(root.path(), after).status.code(),
            Some(if after { 74 } else { 73 })
        );
        let mut p = open(root.path(), A, false);
        let resumed = p.prepare_registration().unwrap();
        assert_eq!(original.request, resumed.request);
        assert_eq!(original.credential(), resumed.credential());
        assert_eq!(resumed.bound_device.is_some(), after);
        let response = receipt(&original, A);
        let before = if after {
            Some(p.next_upload().unwrap().unwrap())
        } else {
            None
        };
        p.bind_registration(&original.request.registration_request_id, &response)
            .unwrap();
        if let Some(before) = before {
            assert_eq!(p.next_upload().unwrap().unwrap(), before);
        }
        assert!(!db(root.path()).query_row("SELECT EXISTS(SELECT 1 FROM protected_records WHERE record_id LIKE 'test-fault-spill-%')",[],|r|r.get::<_,bool>(0)).unwrap());
    }
}
#[test]
fn process_bind_worker() {
    let Some(root) = std::env::var_os("ZCHAT_PROVISION_FAULT_ROOT") else {
        return;
    };
    let mut p = open(Path::new(&root), A, false);
    let prepared = p.prepare_registration().unwrap();
    let response = receipt(&prepared, A);
    let own = response
        .active_device
        .as_ref()
        .unwrap()
        .reference
        .as_ref()
        .unwrap();
    p.store.test_exit_on_record(
        plan_key(own).unwrap(),
        std::env::var("ZCHAT_PROVISION_AFTER").unwrap() == "1",
    );
    p.bind_registration(&prepared.request.registration_request_id, &response)
        .unwrap();
    panic!("fault injection failed to exit")
}

#[tokio::test]
async fn published_keys_from_two_accounts_bootstrap_real_signal_and_reply() {
    use super::super::signal_state::{CryptoCoordinator, SessionScope};
    use crate::proto::e2ee::{
        AcquiredBundle, Conversation, ConversationMode, ConversationState, EncryptedPayload,
        TextMessage, encrypted_payload,
    };
    const BG: &str = "44444444-4444-4444-8444-444444444444";
    const CHAT: &str = "66666666-6666-4666-8666-666666666666";
    let root = tempfile::tempdir().unwrap();
    let mut owners = vec![];
    let mut devices = vec![];
    let mut published = vec![];
    for (user, generation) in [(A, G), (B, BG)] {
        let mut p = open(root.path(), user, true);
        let prepared = p.prepare_registration().unwrap();
        let mut response = receipt(&prepared, user);
        response
            .active_device
            .as_mut()
            .unwrap()
            .reference
            .as_mut()
            .unwrap()
            .generation_id = generation.into();
        p.bind_registration(&prepared.request.registration_request_id, &response)
            .unwrap();
        let mut requests = vec![];
        while let Some(request) = p.next_upload().unwrap() {
            p.acknowledge_upload(&ack(&request)).unwrap();
            requests.push(request);
        }
        let own = p.ready_device().unwrap();
        let mut device = response.active_device.unwrap();
        device.public_material_ready = true;
        device.public_material_revision = requests.len() as u64;
        owners.push(CryptoCoordinator::start(p.into_store(), own).await.unwrap());
        devices.push(device);
        published.push(requests);
    }
    let conversation = |i: usize, j: usize| Conversation {
        conversation_id: CHAT.into(),
        mode: ConversationMode::SignalPq1 as i32,
        state: ConversationState::Active as i32,
        own_device: devices[i].reference.clone(),
        peer_device: Some(devices[j].clone()),
        ..Default::default()
    };
    let a = SessionScope::from_conversation(&conversation(0, 1)).unwrap();
    let b = SessionScope::from_conversation(&conversation(1, 0)).unwrap();
    let bob = &published[1][0];
    let mut acquired = AcquiredBundle {
        acquisition_id: Uuid::new_v4().to_string(),
        conversation_id: CHAT.into(),
        device: Some(devices[1].clone()),
        signed_pre_key: bob.signed_pre_key.clone(),
        one_time_pre_key: bob.one_time_pre_keys.first().cloned(),
        kyber_pre_key: bob
            .kyber_pre_keys
            .iter()
            .find(|k| !k.is_last_resort)
            .cloned(),
        not_after_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 3600000,
    };
    acquired.acquisition_id = owners[0]
        .prepare_acquisition(a.clone())
        .await
        .unwrap()
        .request
        .acquisition_id;
    owners[0].bootstrap(a.clone(), acquired).await.unwrap();
    let message = |i: usize, j: usize, text: &str| EncryptedPayload {
        format_version: 1,
        message_id: Uuid::new_v4().to_string(),
        conversation_id: CHAT.into(),
        sender: devices[i].reference.clone(),
        recipient: devices[j].reference.clone(),
        created_at_unix_ms: 1700000000123,
        content: Some(encrypted_payload::Content::Text(TextMessage {
            text: text.into(),
        })),
    };
    let original = message(0, 1, "published keys match persisted private keys");
    let encrypted = owners[0]
        .encrypt(a.clone(), original.clone())
        .await
        .unwrap();
    assert_eq!(
        owners[1].decrypt(b.clone(), encrypted).await.unwrap(),
        original
    );
    let reply = message(1, 0, "confirmed reply");
    let encrypted = owners[1].encrypt(b, reply.clone()).await.unwrap();
    assert_eq!(owners[0].decrypt(a, encrypted).await.unwrap(), reply);
    for owner in owners {
        owner.shutdown().await.unwrap();
    }
    for (index, user) in [A, B].into_iter().enumerate() {
        let mut p = open(root.path(), user, false);
        let resumed = p.prepare_registration().unwrap();
        assert_eq!(resumed.bound_device, devices[index].reference);
        assert!(p.ready_device().is_ok());
        assert!(p.next_upload().unwrap().is_none());
    }
}

#[test]
fn saved_private_material_is_validated_before_registration_can_be_retried() {
    for mode in 0..5 {
        let root = tempfile::tempdir().unwrap();
        let mut p = open(root.path(), A, true);
        p.prepare_registration().unwrap();
        p.store
            .transaction_with(|tx| {
                let mut pending = load_pending(tx)?.unwrap();
                match mode {
                    0 => pending.signed.clear(),
                    1 => {
                        pending.pq[0][0] ^= 255;
                    }
                    2 => pending.ec[0] = pending.ec[1].clone(),
                    3 => {
                        let key = SignedPreKeyRecord::deserialize(&pending.signed)?;
                        let mut storage = key.get_storage().clone();
                        storage.private_key.zeroize();
                        storage.private_key.clear();
                        pending.signed = SignedPreKeyRecord::from_storage(storage).serialize()?;
                    }
                    _ => {
                        let key = KyberPreKeyRecord::deserialize(&pending.last_resort)?;
                        let mut storage = key.get_storage().clone();
                        storage.private_key.zeroize();
                        storage.private_key.clear();
                        pending.last_resort =
                            KyberPreKeyRecord::from_storage(storage).serialize()?;
                    }
                }
                tx.put_account(
                    &account_key(AccountKind::Registration)?,
                    &Zeroizing::new(pending.encode_to_vec()),
                )?;
                Ok::<_, CryptoError>(())
            })
            .unwrap();
        assert!(p.prepare_registration().is_err());
        assert!(
            !p.store
                .transaction(|tx| tx.has_generation_records())
                .unwrap()
        );
    }
}

#[cfg(windows)]
#[test]
fn real_dpapi_pending_bind_and_publication_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut p = DeviceProvisioning::new(ProtectedStore::create(root.path(), scope(A)).unwrap());
    let first = p.prepare_registration().unwrap();
    drop(p);
    let mut p = DeviceProvisioning::new(ProtectedStore::open(root.path(), scope(A)).unwrap());
    let second = p.prepare_registration().unwrap();
    assert_eq!(second.request, first.request);
    assert_eq!(second.credential(), first.credential());
    p.bind_registration(&first.request.registration_request_id, &receipt(&first, A))
        .unwrap();
    let upload = p.next_upload().unwrap().unwrap();
    drop(p);
    let mut p = DeviceProvisioning::new(ProtectedStore::open(root.path(), scope(A)).unwrap());
    assert_eq!(p.next_upload().unwrap().unwrap(), upload);
    while let Some(request) = p.next_upload().unwrap() {
        p.acknowledge_upload(&ack(&request)).unwrap();
    }
    assert!(p.ready_device().is_ok());
}
