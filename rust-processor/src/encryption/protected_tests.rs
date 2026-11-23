use super::*;
use std::process::Command;

const ALICE: &str = "11111111-1111-4111-8111-111111111111";
const BOB: &str = "22222222-2222-4222-8222-222222222222";
const GENERATION: &str = "33333333-3333-4333-8333-333333333333";
const REPLACEMENT: &str = "44444444-4444-4444-8444-444444444444";
const SERVER: &str = "https://chat.example.test";

// Unit tests only. No injected protector constructor is exposed by the library.
pub(super) struct TestProtector(pub(super) u8);
impl KeyProtector for TestProtector {
    fn wrap(&self, key: &[u8], namespace: &[u8; 32]) -> StoreResult<Vec<u8>> {
        seal(
            &Aes256GcmSiv::new_from_slice(&[self.0; 32]).unwrap(),
            key,
            namespace,
        )
    }
    fn unwrap(&self, bytes: &[u8], namespace: &[u8; 32]) -> StoreResult<Zeroizing<Vec<u8>>> {
        open(
            &Aes256GcmSiv::new_from_slice(&[self.0; 32]).unwrap(),
            bytes,
            namespace,
        )
    }
}

fn scope() -> StoreScope {
    StoreScope::new(SERVER, ALICE).unwrap()
}
fn key(kind: RecordKind, id: &str) -> RecordKey {
    RecordKey::new(GENERATION, kind, id).unwrap()
}
fn connect(root: &Path, scope: StoreScope, create: bool) -> ProtectedStore {
    super::test_open(root, scope, create)
}
fn read(store: &mut ProtectedStore, key: &RecordKey) -> Option<Vec<u8>> {
    store
        .transaction(|tx| tx.get(key))
        .unwrap()
        .map(|v| v.to_vec())
}
fn write(store: &mut ProtectedStore, key: &RecordKey, value: &[u8]) {
    store.transaction(|tx| tx.put(key, value)).unwrap();
}
fn raw(store: &ProtectedStore, key: &RecordKey) -> Vec<u8> {
    store.connection.query_row(
        "SELECT payload FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id=?3",
        params![key.generation.as_bytes().as_slice(), key.kind as u8, key.id], |r| r.get(0),
    ).unwrap()
}

#[test]
fn file_status_auxiliary_delete_is_atomic_and_refuses_crypto_or_projected_records() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let pending = key(RecordKind::Control, "file-progress-pending:example");
    write(&mut store, &pending, b"auxiliary");
    assert!(
        store
            .transaction(|tx| {
                tx.remove_unindexed_control(&pending)?;
                Err::<(), _>(StoreError::Aborted)
            })
            .is_err()
    );
    assert_eq!(read(&mut store, &pending).unwrap(), b"auxiliary");
    for protected in [
        key(RecordKind::Identity, "local"),
        key(RecordKind::History, "message"),
        key(RecordKind::Control, "delivery-in:message"),
    ] {
        write(&mut store, &protected, b"must remain");
        assert!(matches!(
            store.transaction(|tx| tx.remove_unindexed_control(&protected)),
            Err(StoreError::InvalidScope)
        ));
        assert_eq!(read(&mut store, &protected).unwrap(), b"must remain");
    }
    store
        .transaction(|tx| tx.remove_unindexed_control(&pending))
        .unwrap();
    assert!(read(&mut store, &pending).is_none());
    store
        .transaction(|tx| tx.remove_unindexed_control(&pending))
        .unwrap();
}

#[test]
fn schema_one_migrates_after_key_verification_without_rewriting_key_or_generation_data() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let record = key(RecordKind::Identity, "local");
    write(&mut store, &record, b"existing identity");
    let ciphertext = raw(&store, &record);
    let wrapped: Vec<u8> = store
        .connection
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap();
    // Reconstruct the previous schema: header, generation AEAD and wrapped key
    // are unchanged; schema 1 simply lacked the account records table.
    store
        .connection
        .execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE protected_attachment_download_chunks; DROP TABLE protected_attachment_downloads; ALTER TABLE protected_records DROP COLUMN attachment_ref; DROP TABLE protected_attachment_chunks; DROP TABLE protected_attachment_blobs; ALTER TABLE protected_records DROP COLUMN recovery_ref; DROP TABLE protected_recovery; DROP TABLE protected_account_records; PRAGMA user_version=1; PRAGMA foreign_keys=ON;")
        .unwrap();
    drop(store);
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(9)),
        Err(StoreError::Protection)
    ));
    let db = Connection::open(scope().database_path(root.path())).unwrap();
    assert_eq!(
        db.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        1
    );
    drop(db);
    let mut store = connect(root.path(), scope(), false);
    assert_eq!(
        store
            .connection
            .pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        SCHEMA
    );
    assert_eq!(raw(&store, &record), ciphertext);
    assert_eq!(read(&mut store, &record).unwrap(), b"existing identity");
    assert_eq!(
        store
            .connection
            .query_row::<Vec<u8>, _, _>("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
            .unwrap(),
        wrapped
    );
    let pending = AccountKey::new(AccountKind::Registration, "initial").unwrap();
    store
        .transaction(|tx| tx.put_account(&pending, b"pending identity"))
        .unwrap();
    drop(store);
    let mut store = connect(root.path(), scope(), false);
    assert_eq!(
        store
            .transaction(|tx| tx.get_account(&pending))
            .unwrap()
            .unwrap()
            .as_slice(),
        b"pending identity"
    );
}

#[test]
fn schema_three_adds_only_empty_attachment_tables_and_preserves_old_ciphertexts() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let record = key(RecordKind::Identity, "local");
    write(
        &mut store,
        &record,
        b"retained identity for schema migration",
    );
    let before = raw(&store, &record);
    let wrapped: Vec<u8> = store
        .connection
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap();
    store.connection.execute_batch("PRAGMA foreign_keys=OFF; DROP TABLE protected_attachment_download_chunks; DROP TABLE protected_attachment_downloads; ALTER TABLE protected_records DROP COLUMN attachment_ref; DROP TABLE protected_attachment_chunks; DROP TABLE protected_attachment_blobs; PRAGMA user_version=3;").unwrap();
    drop(store);
    // A failed schema-4 transaction may not publish half a migration or alter
    // old keys. The deliberately conflicting table is not permission to reset.
    let db = Connection::open(scope().database_path(root.path())).unwrap();
    db.execute_batch("CREATE TABLE protected_attachment_chunks(conflict INTEGER);")
        .unwrap();
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)),
        Err(StoreError::Database(_))
    ));
    assert_eq!(
        db.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        3
    );
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT count(*) FROM sqlite_master WHERE name='protected_attachment_blobs'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    db.execute_batch("DROP TABLE protected_attachment_chunks;")
        .unwrap();
    drop(db);
    let store = connect(root.path(), scope(), false);
    assert_eq!(
        store
            .connection
            .pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        SCHEMA
    );
    assert_eq!(raw(&store, &record), before);
    assert_eq!(
        store
            .connection
            .query_row::<Vec<u8>, _, _>("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
            .unwrap(),
        wrapped
    );
    assert_eq!(
        store
            .connection
            .query_row::<i64, _, _>("SELECT count(*) FROM protected_attachment_blobs", [], |r| r
                .get(0))
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .connection
            .query_row::<i64, _, _>(
                "SELECT count(*) FROM protected_records WHERE attachment_ref IS NOT NULL",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        0
    );
}

#[test]
fn failed_schema_migration_preserves_previous_version_and_keys() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let record = key(RecordKind::Identity, "local");
    write(&mut store, &record, b"existing identity");
    let ciphertext = raw(&store, &record);
    store
        .connection
        .execute_batch("PRAGMA user_version=1;")
        .unwrap();
    drop(store);
    // A conflicting table is inconsistent, not permission to reset the store.
    let failure =
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42))
            .err()
            .expect("conflicting schema must fail");
    assert!(matches!(failure, StoreError::Database(_)), "{failure:?}");
    let db = Connection::open(scope().database_path(root.path())).unwrap();
    assert_eq!(
        db.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row::<Vec<u8>, _, _>("SELECT payload FROM protected_records", [], |r| r.get(0))
            .unwrap(),
        ciphertext
    );
}

#[test]
fn account_records_are_aead_bound_to_account_kind_id_and_distinct_from_generations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let registration = AccountKey::new(AccountKind::Registration, "initial").unwrap();
    let active = AccountKey::new(AccountKind::ActiveDevice, "initial").unwrap();
    store
        .transaction(|tx| tx.put_account(&registration, b"sensitive pending keys"))
        .unwrap();
    let payload: Vec<u8> = store
        .connection
        .query_row("SELECT payload FROM protected_account_records", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(!payload.windows(22).any(|v| v == b"sensitive pending keys"));
    store
        .connection
        .execute(
            "INSERT INTO protected_account_records VALUES(2,'initial',?1)",
            [&payload],
        )
        .unwrap();
    assert!(matches!(
        store.transaction(|tx| tx.get_account(&active)),
        Err(StoreError::Protection)
    ));
    store
        .connection
        .execute(
            "INSERT INTO protected_account_records VALUES(1,'other',?1)",
            [&payload],
        )
        .unwrap();
    assert!(matches!(
            store.transaction(
                |tx| tx.get_account(&AccountKey::new(AccountKind::Registration, "other")?)
            ),
            Err(StoreError::Protection)
        ));
    let record = key(RecordKind::Identity, "local");
    write(&mut store, &record, b"generation identity");
    let generation_ciphertext = raw(&store, &record);
    store
        .connection
        .execute(
            "UPDATE protected_account_records SET payload=?1 WHERE kind=1 AND record_id='initial'",
            [generation_ciphertext],
        )
        .unwrap();
    assert!(matches!(
        store.transaction(|tx| tx.get_account(&registration)),
        Err(StoreError::Protection)
    ));
    let bob_scope = StoreScope::new(SERVER, BOB).unwrap();
    let mut bob = connect(root.path(), bob_scope, true);
    bob.connection
        .execute(
            "INSERT INTO protected_account_records VALUES(1,'initial',?1)",
            [payload],
        )
        .unwrap();
    assert!(matches!(
        bob.transaction(|tx| tx.get_account(&registration)),
        Err(StoreError::Protection)
    ));
}

#[test]
fn swallowed_account_error_aborts_the_whole_generation_and_account_transaction() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let registration = AccountKey::new(AccountKind::Registration, "initial").unwrap();
    let record = key(RecordKind::Identity, "local");
    let result = store.transaction(|tx| {
        tx.put(&record, b"do not commit")?;
        tx.put_account(&registration, b"do not commit")?;
        assert!(
            tx.put_account(&registration, &vec![0; MAX_RECORD_BYTES + 1])
                .is_err()
        );
        Ok(())
    });
    assert!(matches!(result, Err(StoreError::Aborted)));
    assert!(read(&mut store, &record).is_none());
    assert!(
        store
            .transaction(|tx| tx.get_account(&registration))
            .unwrap()
            .is_none()
    );
}

#[test]
fn namespaces_canonicalize_origin_but_separate_accounts_servers_and_ports() {
    let root = Path::new("unused");
    let a = scope().database_path(root);
    assert_eq!(
        a,
        StoreScope::new("https://CHAT.example.test:443/", ALICE)
            .unwrap()
            .database_path(root)
    );
    for other in [
        StoreScope::new(SERVER, BOB).unwrap(),
        StoreScope::new("https://other.example.test", ALICE).unwrap(),
        StoreScope::new("https://chat.example.test:8443", ALICE).unwrap(),
    ] {
        assert_ne!(a, other.database_path(root));
    }
    for origin in [
        "http://localhost:38080",
        "http://127.0.0.1:38080/",
        "http://[::1]:38080",
    ] {
        assert!(StoreScope::new(origin, ALICE).is_ok());
    }
    for origin in [
        "http://chat.example.test",
        "https://a:b@chat.example.test",
        "https://chat.example.test/api",
        "https://chat.example.test/?token=x",
        "https://chat.example.test/#x",
        "file:///tmp/foo",
        "http://localhost.evil.test",
    ] {
        assert!(StoreScope::new(origin, ALICE).is_err(), "{origin}");
    }
    assert!(StoreScope::new(SERVER, "alice@example.test").is_err());
    assert!(RecordKey::new(GENERATION, RecordKind::History, "../escape").is_err());
    assert!(RecordKey::new(ALICE, RecordKind::History, "").is_err());
    assert!(RecordKey::new("not-a-generation", RecordKind::History, "id").is_err());
}

#[test]
fn accounts_can_stay_open_together_and_reopen_without_regenerating_state() {
    let root = tempfile::tempdir().unwrap();
    let mut alice = connect(root.path(), scope(), true);
    let bob_scope = StoreScope::new(SERVER, BOB).unwrap();
    let mut bob = connect(root.path(), bob_scope.clone(), true);
    let other_scope = StoreScope::new("https://other.example.test", ALICE).unwrap();
    let mut other = connect(root.path(), other_scope.clone(), true);
    let identity = key(RecordKind::Identity, "local");
    let credential = key(RecordKind::DeviceCredential, "local");
    alice
        .transaction(|tx| {
            tx.put(&identity, b"alice-private-identity")?;
            tx.put(&credential, b"alice-device-credential")
        })
        .unwrap();
    write(&mut bob, &identity, b"bob-private-identity");
    write(&mut other, &identity, b"other-server-private-identity");
    let alice_bytes = raw(&alice, &identity);
    let key_before: Vec<u8> = alice
        .connection
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap();
    drop((alice, bob, other));
    for _ in 0..3 {
        let mut alice = connect(root.path(), scope(), false);
        let mut bob = connect(root.path(), bob_scope.clone(), false);
        let mut other = connect(root.path(), other_scope.clone(), false);
        assert_eq!(
            read(&mut alice, &identity).unwrap(),
            b"alice-private-identity"
        );
        assert_eq!(
            read(&mut alice, &credential).unwrap(),
            b"alice-device-credential"
        );
        assert_eq!(read(&mut bob, &identity).unwrap(), b"bob-private-identity");
        assert_eq!(
            read(&mut other, &identity).unwrap(),
            b"other-server-private-identity"
        );
        assert_eq!(raw(&alice, &identity), alice_bytes);
        let key_after: Vec<u8> = alice
            .connection
            .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
            .unwrap();
        assert_eq!(key_after, key_before);
        let count: i64 = alice
            .connection
            .query_row("SELECT COUNT(*) FROM protected_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }
}

#[test]
fn replacement_namespace_preserves_old_archive_and_has_no_shared_state() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    for kind in [
        RecordKind::Identity,
        RecordKind::Session,
        RecordKind::History,
        RecordKind::Outbox,
    ] {
        let old = key(kind, "local");
        let new = RecordKey::new(REPLACEMENT, kind, "local").unwrap();
        write(&mut store, &old, b"old generation");
        assert!(read(&mut store, &new).is_none());
        write(&mut store, &new, b"replacement generation");
        assert_eq!(read(&mut store, &old).unwrap(), b"old generation");
        assert_eq!(read(&mut store, &new).unwrap(), b"replacement generation");
    }
}

#[test]
fn encrypted_records_bind_account_generation_kind_and_record_id() {
    let root = tempfile::tempdir().unwrap();
    let mut alice = connect(root.path(), scope(), true);
    let mut bob = connect(root.path(), StoreScope::new(SERVER, BOB).unwrap(), true);
    let source = key(RecordKind::Session, "peer-one");
    write(&mut alice, &source, b"secret ratchet state");
    let bytes = raw(&alice, &source);
    // Even copying a complete ciphertext row fails under another account master key.
    for destination in [
        source.clone(),
        key(RecordKind::History, "peer-one"),
        key(RecordKind::Session, "peer-two"),
        RecordKey::new(REPLACEMENT, RecordKind::Session, "peer-one").unwrap(),
    ] {
        let target = if destination.kind as u8 == source.kind as u8
            && destination.id == source.id
            && destination.generation == source.generation
        {
            &mut bob
        } else {
            &mut alice
        };
        target
            .connection
            .execute(
                "INSERT OR REPLACE INTO protected_records(generation,kind,record_id,payload) VALUES(?1,?2,?3,?4)",
                params![
                    destination.generation.as_bytes().as_slice(),
                    destination.kind as u8,
                    destination.id,
                    bytes
                ],
            )
            .unwrap();
        assert!(matches!(
            target.transaction(|tx| tx.get(&destination)),
            Err(StoreError::Protection)
        ));
    }
    assert_eq!(read(&mut alice, &source).unwrap(), b"secret ratchet state");
}

#[test]
fn tampering_nonce_tag_body_version_and_truncation_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let record = key(RecordKind::Session, "peer");
    write(&mut store, &record, b"private state");
    let original = raw(&store, &record);
    for position in [0, 1, 12, 13, original.len() - 1] {
        let mut corrupted = original.clone();
        corrupted[position] ^= 0x80;
        store
            .connection
            .execute("UPDATE protected_records SET payload=?1", [corrupted])
            .unwrap();
        assert!(matches!(
            store.transaction(|tx| tx.get(&record)),
            Err(StoreError::Protection)
        ));
    }
    // Leave the minimum valid SQL length; cryptographic truncation still rejects it.
    store
        .connection
        .execute(
            "UPDATE protected_records SET payload=?1",
            [&original[..OVERHEAD]],
        )
        .unwrap();
    assert!(matches!(
        store.transaction(|tx| tx.get(&record)),
        Err(StoreError::Protection)
    ));
}

#[test]
fn wrong_protector_and_damaged_header_never_replace_keys_or_database() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    write(
        &mut store,
        &key(RecordKind::Identity, "local"),
        b"private identity",
    );
    drop(store);
    let path = scope().database_path(root.path());
    let before = fs::read(&path).unwrap();
    let rejected =
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(41)).err();
    assert!(
        matches!(rejected, Some(StoreError::Protection)),
        "unexpected protected-store rejection: {rejected:?}"
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(
        ProtectedStore::connect_protected(root.path(), scope(), true, &TestProtector(42)).is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), before);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute("UPDATE store_header SET wrapped_key=zeroblob(64)", [])
        .unwrap();
    drop(connection);
    let damaged = fs::read(&path).unwrap();
    assert!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)).is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), damaged);
}

#[test]
fn whole_database_copy_cannot_be_opened_as_another_account() {
    let root = tempfile::tempdir().unwrap();
    let mut alice = connect(root.path(), scope(), true);
    write(
        &mut alice,
        &key(RecordKind::Identity, "local"),
        b"alice identity",
    );
    // Even knowing/rewrapping the test master key does not change the sealed
    // namespace check. The production DPAPI protector cannot be injected here.
    let wrapped: Vec<u8> = alice
        .connection
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap();
    let master = TestProtector(42)
        .unwrap(&wrapped, &scope().namespace)
        .unwrap();
    drop(alice);
    let bob_scope = StoreScope::new(SERVER, BOB).unwrap();
    let bob_path = bob_scope.database_path(root.path());
    fs::create_dir_all(bob_path.parent().unwrap()).unwrap();
    fs::copy(scope().database_path(root.path()), &bob_path).unwrap();
    assert!(matches!(
        ProtectedStore::connect_protected(
            root.path(),
            bob_scope.clone(),
            false,
            &TestProtector(42)
        ),
        Err(StoreError::Format)
    ));
    let db = Connection::open(&bob_path).unwrap();
    let rebound = TestProtector(42)
        .wrap(&master, &bob_scope.namespace)
        .unwrap();
    db.execute(
        "UPDATE store_header SET namespace=?1,wrapped_key=?2",
        params![bob_scope.namespace.as_slice(), rebound],
    )
    .unwrap();
    drop(db);
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), bob_scope, false, &TestProtector(42)),
        Err(StoreError::Protection)
    ));
    let mut alice = connect(root.path(), scope(), false);
    assert_eq!(
        read(&mut alice, &key(RecordKind::Identity, "local")).unwrap(),
        b"alice identity"
    );
}

#[test]
fn failed_initial_protection_leaves_no_usable_plaintext_or_implicit_reset() {
    struct Denied;
    impl KeyProtector for Denied {
        fn wrap(&self, _: &[u8], _: &[u8; 32]) -> StoreResult<Vec<u8>> {
            Err(StoreError::Protection)
        }
        fn unwrap(&self, _: &[u8], _: &[u8; 32]) -> StoreResult<Zeroizing<Vec<u8>>> {
            Err(StoreError::Protection)
        }
    }
    let root = tempfile::tempdir().unwrap();
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), scope(), true, &Denied),
        Err(StoreError::Protection)
    ));
    let path = scope().database_path(root.path());
    let before = fs::read(&path).unwrap();
    assert!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)).is_err()
    );
    assert!(
        ProtectedStore::connect_protected(root.path(), scope(), true, &TestProtector(42)).is_err()
    );
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn missing_legacy_and_future_stores_are_never_silently_created_or_imported() {
    let root = tempfile::tempdir().unwrap();
    assert!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)).is_err()
    );
    assert!(!scope().database_path(root.path()).exists());
    let mut store = connect(root.path(), scope(), true);
    write(
        &mut store,
        &key(RecordKind::Identity, "local"),
        b"old identity",
    );
    store
        .connection
        .pragma_update(None, "user_version", 999)
        .unwrap();
    drop(store);
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)),
        Err(StoreError::Format)
    ));
    let legacy_root = tempfile::tempdir().unwrap();
    let path = scope().database_path(legacy_root.path());
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let legacy = Connection::open(&path).unwrap();
    legacy
        .execute_batch(
            "CREATE TABLE identity(private_key BLOB); INSERT INTO identity VALUES(x'010203');",
        )
        .unwrap();
    drop(legacy);
    let before = fs::read(&path).unwrap();
    assert!(matches!(
        ProtectedStore::connect_protected(legacy_root.path(), scope(), false, &TestProtector(42)),
        Err(StoreError::Format)
    ));
    assert_eq!(fs::read(&path).unwrap(), before);
}

#[test]
fn atomic_session_prekey_history_outbox_rollback_and_commit_survive_reopen() {
    let root = tempfile::tempdir().unwrap();
    let records = [
        key(RecordKind::Session, "peer"),
        key(RecordKind::PqPreKey, "1"),
        key(RecordKind::History, "message"),
        key(RecordKind::Outbox, "message"),
    ];
    let mut store = connect(root.path(), scope(), true);
    store
        .transaction(|tx| {
            for key in &records {
                tx.put(key, b"before")?;
            }
            Ok(())
        })
        .unwrap();
    let result: StoreResult<()> = store.transaction(|tx| {
        for key in &records {
            tx.put(key, b"after")?;
        }
        Err(StoreError::Aborted)
    });
    assert!(result.is_err());
    drop(store);
    let mut store = connect(root.path(), scope(), false);
    for record in &records {
        assert_eq!(read(&mut store, record).unwrap(), b"before");
    }
    store
        .transaction(|tx| {
            for record in &records {
                tx.put(record, b"after")?;
            }
            Ok(())
        })
        .unwrap();
    drop(store);
    let mut store = connect(root.path(), scope(), false);
    for record in &records {
        assert_eq!(read(&mut store, record).unwrap(), b"after");
    }
}

#[test]
fn swallowed_record_error_and_commit_failure_cannot_commit_partial_state() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let record = key(RecordKind::Session, "peer");
    write(&mut store, &record, b"before");
    let oversized = vec![0u8; MAX_RECORD_BYTES + 1];
    assert!(matches!(
        store.transaction(|tx| {
            tx.put(&record, b"after")?;
            assert!(matches!(
                tx.put(&key(RecordKind::Outbox, "message"), &oversized),
                Err(StoreError::RecordTooLarge)
            ));
            Ok(()) // Buggy caller swallows a failure; transaction is still poisoned.
        }),
        Err(StoreError::Aborted)
    ));
    assert_eq!(read(&mut store, &record).unwrap(), b"before");
    store.connection.execute_batch("CREATE TABLE parent(id INTEGER PRIMARY KEY);
        CREATE TABLE commit_failure(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
        CREATE TRIGGER fail_commit AFTER UPDATE ON protected_records BEGIN INSERT INTO commit_failure VALUES(1); END;").unwrap();
    assert!(matches!(
        store.transaction(|tx| tx.put(&record, b"after")),
        Err(StoreError::Database(_))
    ));
    assert_eq!(read(&mut store, &record).unwrap(), b"before");
}

#[test]
fn unwind_rolls_back_and_connection_remains_usable() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let record = key(RecordKind::Session, "peer");
    write(&mut store, &record, b"before");
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _: StoreResult<()> = store.transaction(|tx| {
            tx.put(&record, b"after")?;
            panic!("test transaction interruption");
        });
    }));
    assert!(result.is_err());
    assert_eq!(read(&mut store, &record).unwrap(), b"before");
    write(&mut store, &record, b"recovered");
}

#[test]
fn database_and_live_rollback_journal_contain_no_plaintext_values() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    let record = key(RecordKind::History, "message");
    let marker = b"TEST-ONLY-PRIVATE-MESSAGE-2c792b75";
    let secret: Vec<u8> = marker.iter().copied().cycle().take(48 * 1024).collect();
    write(&mut store, &record, &secret);
    let directory = scope()
        .database_path(root.path())
        .parent()
        .unwrap()
        .to_path_buf();
    store
        .transaction(|tx| {
            for i in 0..100 {
                tx.put(&key(RecordKind::History, &format!("message-{i}")), &secret)?;
            }
            tx.put(&record, b"changed")?;
            assert!(directory.join("state.sqlite-journal").exists());
            for entry in fs::read_dir(&directory)? {
                let entry = entry?;
                // Windows file locks are mandatory, including for the test reader.
                // Scan the database/journal, not our deliberately locked owner file.
                if entry.file_name() == "owner.lock" {
                    continue;
                }
                let data = fs::read(entry.path())?;
                assert!(!data.windows(marker.len()).any(|w| w == marker));
            }
            Ok(())
        })
        .unwrap();
    let before = raw(&store, &record);
    write(&mut store, &record, b"changed");
    assert_ne!(raw(&store, &record), before); // Fresh OS randomness on every write.
    write(&mut store, &record, &vec![0u8; MAX_RECORD_BYTES]);
    assert_eq!(read(&mut store, &record).unwrap().len(), MAX_RECORD_BYTES);
}

fn child(root: &Path, mode: &str) -> std::process::Output {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "encryption::protected::tests::subprocess_worker",
            "--nocapture",
        ])
        .env("ZCHAT_PROTECTED_TEST_ROOT", root)
        .env("ZCHAT_PROTECTED_TEST_MODE", mode)
        .output()
        .unwrap()
}

#[test]
fn lifetime_owner_lock_excludes_second_connection_and_process() {
    let root = tempfile::tempdir().unwrap();
    let store = connect(root.path(), scope(), true);
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)),
        Err(StoreError::Busy)
    ));
    assert!(child(root.path(), "busy").status.success());
    drop(store);
    drop(connect(root.path(), scope(), false));
}

#[test]
fn killed_process_recovers_old_atomic_state_and_committed_outbox() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), scope(), true);
    write(&mut store, &key(RecordKind::Session, "peer"), b"before");
    drop(store);
    assert_eq!(
        child(root.path(), "crash-before-commit").status.code(),
        Some(73)
    );
    assert!(
        scope()
            .database_path(root.path())
            .with_extension("sqlite-journal")
            .exists()
    );
    let mut store = connect(root.path(), scope(), false);
    assert_eq!(
        read(&mut store, &key(RecordKind::Session, "peer")).unwrap(),
        b"before"
    );
    assert!(read(&mut store, &key(RecordKind::Outbox, "message")).is_none());
    drop(store);
    assert_eq!(
        child(root.path(), "crash-after-commit").status.code(),
        Some(74)
    );
    let mut store = connect(root.path(), scope(), false);
    assert_eq!(
        read(&mut store, &key(RecordKind::Session, "peer")).unwrap(),
        b"after"
    );
    assert_eq!(
        read(&mut store, &key(RecordKind::Outbox, "message")).unwrap(),
        b"exact saved ciphertext"
    );
}

// Runs in a distinct process: exit intentionally skips Rust destructors/rollback.
#[test]
fn subprocess_worker() {
    let Some(root) = std::env::var_os("ZCHAT_PROTECTED_TEST_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let mode = std::env::var("ZCHAT_PROTECTED_TEST_MODE").unwrap();
    if mode == "busy" {
        assert!(matches!(
            ProtectedStore::connect_protected(&root, scope(), false, &TestProtector(42)),
            Err(StoreError::Busy)
        ));
        return;
    }
    let mut store = connect(&root, scope(), false);
    store
        .transaction(|tx| {
            tx.put(&key(RecordKind::Session, "peer"), b"after")?;
            tx.put(
                &key(RecordKind::Outbox, "message"),
                b"exact saved ciphertext",
            )?;
            if mode == "crash-before-commit" {
                for i in 0..100 {
                    tx.put(
                        &key(RecordKind::History, &format!("spill-{i}")),
                        &vec![17u8; 48 * 1024],
                    )?;
                }
                std::process::exit(73);
            }
            Ok(())
        })
        .unwrap();
    std::process::exit(74);
}

#[cfg(not(windows))]
#[test]
fn unsupported_platform_has_no_plaintext_fallback_or_filesystem_changes() {
    let root = tempfile::tempdir().unwrap();
    assert!(matches!(
        ProtectedStore::create(root.path(), scope()),
        Err(StoreError::UnsupportedPlatform)
    ));
    assert!(matches!(
        ProtectedStore::open(root.path(), scope()),
        Err(StoreError::UnsupportedPlatform)
    ));
    assert_eq!(fs::read_dir(root.path()).unwrap().count(), 0);
}

#[cfg(windows)]
#[test]
fn real_windows_dpapi_reopens_and_rejects_changed_namespace() {
    let root = tempfile::tempdir().unwrap();
    let mut store = ProtectedStore::create(root.path(), scope()).unwrap();
    let record = key(RecordKind::DeviceCredential, "local");
    write(&mut store, &record, b"test-only-device-credential");
    drop(store);
    let mut store = ProtectedStore::open(root.path(), scope()).unwrap();
    assert_eq!(
        read(&mut store, &record).unwrap(),
        b"test-only-device-credential"
    );
    let wrapped = dpapi::WindowsDpapi
        .wrap(&[3u8; 32], &scope().namespace)
        .unwrap();
    assert!(dpapi::WindowsDpapi.unwrap(&wrapped, &[7u8; 32]).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn retired_owner_releases_lock_even_when_its_descriptor_was_duplicated() {
    let root = tempfile::tempdir().unwrap();
    let store = connect(root.path(), scope(), true);
    // A dup has the same lock lifetime as an inherited descriptor during fork.
    // Keeping it alive makes the close-only retirement defect deterministic.
    let duplicate = store._owner.0.try_clone().unwrap();
    drop(store);
    let reopened =
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42))
            .unwrap_or_else(|e| panic!("retired store still owns its lock: {e:?}"));
    drop(duplicate);
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)),
        Err(StoreError::Busy)
    ));
    drop(reopened);
    let _next = connect(root.path(), scope(), false);
}

#[cfg(target_os = "linux")]
#[test]
fn inherited_guard_cannot_unlock_its_live_parent_owner() {
    let root = tempfile::tempdir().unwrap();
    let store = connect(root.path(), scope(), true);
    let inherited = OwnerLock(
        store._owner.0.try_clone().unwrap(),
        std::process::id().wrapping_add(1),
    );
    drop(inherited);
    assert!(matches!(
        ProtectedStore::connect_protected(root.path(), scope(), false, &TestProtector(42)),
        Err(StoreError::Busy)
    ));
    drop(store);
    let _next = connect(root.path(), scope(), false);
}
