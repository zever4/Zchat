use super::*;
const GENERATION: &str = "33333333-3333-4333-8333-333333333333";
const REPLACEMENT: &str = "44444444-4444-4444-8444-444444444444";

#[test]
fn schema_six_writer_fence_is_authenticated_and_preserves_existing_cache_and_keys() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), true);
    let k = record(GENERATION, "attachment-download", 0);
    let blob = Uuid::new_v4().to_string();
    let bytes = vec![7; 37];
    store
        .transaction(|tx| {
            tx.put(&k, b"existing protected cache journal")?;
            tx.create_download_manifest(&k, &blob, bytes.len())?;
            tx.append_download_chunk(&k, bytes.len(), 0, &bytes)
        })
        .unwrap();
    let raw = |s: &ProtectedStore| {
        s.connection.query_row(
        "SELECT wrapped_key,key_check,(SELECT payload FROM protected_records WHERE record_id=?1),
         (SELECT payload FROM protected_attachment_download_chunks LIMIT 1),
         (SELECT blob_id FROM protected_attachment_downloads LIMIT 1) FROM store_header",
        [&k.id], |r|Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,Vec<u8>>(1)?,r.get::<_,Vec<u8>>(2)?,r.get::<_,Vec<u8>>(3)?,r.get::<_,String>(4)?)),
    ).unwrap()
    };
    let before = raw(&store);
    // Version 5 has exactly these tables/framing; version 6 changes ONLY the
    // writer fence. This does not simulate any missing-table migration.
    store
        .connection
        .pragma_update(None, "user_version", 5)
        .unwrap();
    drop(store);
    assert!(matches!(
        ProtectedStore::connect_protected(
            root.path(),
            scope(),
            false,
            &super::super::tests::TestProtector(9)
        ),
        Err(StoreError::Protection)
    ));
    let db = Connection::open(scope().database_path(root.path())).unwrap();
    assert_eq!(
        db.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        5
    );
    drop(db);
    let mut store = connect(root.path(), false);
    assert_eq!(
        store
            .connection
            .pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        6
    );
    assert_eq!(raw(&store), before);
    assert_eq!(
        store
            .transaction(|tx| tx.collect_download(&k, bytes.len(), &Sha256::digest(&bytes)))
            .unwrap()
            .as_slice(),
        bytes
    );
}

#[test]
fn known_download_removal_releases_shared_capacity_and_swallowed_error_cannot_commit() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), true);
    let first = record(GENERATION, "attachment-download", 0);
    let blob = Uuid::new_v4().to_string();
    for n in 0..attachments::MAX_BLOBS {
        let k = record(GENERATION, "attachment-download", n);
        let id = if n == 0 {
            blob.clone()
        } else {
            Uuid::new_v4().to_string()
        };
        store
            .transaction(|tx| {
                tx.put(&k, b"domain journal placeholder")?;
                tx.create_download_manifest(&k, &id, 37)
            })
            .unwrap();
    }
    // Domain authorization/hash/AEAD are tested by the crypto worker tests;
    // this storage-only fixture exercises shared quota and transaction poison.
    assert!(
        store
            .transaction(|tx| {
                let _ = tx.remove_known_download(&first, &Uuid::new_v4().to_string(), 37);
                tx.remove_known_download(&first, &blob, 37)
            })
            .is_err()
    );
    assert_eq!(
        store
            .connection
            .query_row::<i64, _, _>(
                "SELECT count(*) FROM protected_attachment_downloads",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        attachments::MAX_BLOBS as i64
    );
    store
        .transaction(|tx| tx.remove_known_download(&first, &blob, 37))
        .unwrap();
    let outgoing = record(REPLACEMENT, "attachment-prepare", 99);
    store
        .transaction(|tx| {
            tx.put(&outgoing, b"new preparation")?;
            tx.put_attachment_blob(&outgoing, &Uuid::new_v4().to_string(), &[7; 37])
        })
        .unwrap();
}
fn scope() -> StoreScope {
    StoreScope::new(
        "https://chat.example.test",
        "11111111-1111-4111-8111-111111111111",
    )
    .unwrap()
}
fn record(generation: &str, prefix: &str, n: usize) -> RecordKey {
    RecordKey::new(
        generation,
        RecordKind::Control,
        &format!("{prefix}:00000000-0000-4000-8000-{n:012x}"),
    )
    .unwrap()
}
fn connect(root: &Path, create: bool) -> ProtectedStore {
    super::super::test_open(root, scope(), create)
}
#[test]
fn download_cache_budget_includes_preparations_staging_and_all_generations() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), true);
    for n in 0..attachments::MAX_BLOBS {
        let incoming = n % 2 == 0;
        let k = record(
            if n % 3 == 0 { GENERATION } else { REPLACEMENT },
            if incoming {
                "attachment-download"
            } else {
                "attachment-prepare"
            },
            n,
        );
        store
            .transaction(|tx| {
                tx.put(&k, b"protected test journal")?;
                if incoming {
                    tx.create_download_manifest(&k, &Uuid::new_v4().to_string(), 37)
                } else {
                    tx.put_attachment_blob(&k, &Uuid::new_v4().to_string(), &[7; 37])
                }
            })
            .unwrap();
    }
    for incoming in [false, true] {
        let k = record(
            GENERATION,
            if incoming {
                "attachment-download"
            } else {
                "attachment-prepare"
            },
            99,
        );
        assert!(matches!(
            store.transaction(|tx| {
                tx.put(&k, b"must roll back")?;
                if incoming {
                    tx.create_download_manifest(&k, &Uuid::new_v4().to_string(), 37)
                } else {
                    tx.put_attachment_blob(&k, &Uuid::new_v4().to_string(), &[7; 37])
                }
            }),
            Err(StoreError::Capacity)
        ));
        assert!(store.transaction(|tx| tx.get(&k)).unwrap().is_none());
    }
}
#[test]
fn download_cache_bytes_are_reserved_before_any_chunk_arrives() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), true);
    for n in 0..5 {
        let k = record(GENERATION, "attachment-download", n);
        store
            .transaction(|tx| {
                tx.put(&k, b"protected test journal")?;
                tx.create_download_manifest(&k, &Uuid::new_v4().to_string(), MAX_BLOB_BYTES)
            })
            .unwrap();
    }
    let k = record(REPLACEMENT, "attachment-prepare", 0);
    assert!(matches!(
        store.transaction(|tx| {
            tx.put(&k, b"must roll back")?;
            tx.put_attachment_blob(&k, &Uuid::new_v4().to_string(), &vec![7; 7 * 1024 * 1024])
        }),
        Err(StoreError::Capacity)
    ));
    assert!(store.transaction(|tx| tx.get(&k)).unwrap().is_none());
    assert_eq!(
        store
            .connection
            .query_row::<i64, _, _>(
                "SELECT count(*) FROM protected_attachment_download_chunks",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        0
    );
}
#[test]
fn download_cache_sqlite_full_and_swallowed_error_roll_back_chunk_and_offset() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), true);
    let k = record(GENERATION, "attachment-download", 0);
    store
        .transaction(|tx| {
            tx.put(&k, b"original offset")?;
            tx.create_download_manifest(&k, &Uuid::new_v4().to_string(), 600 * 1024)
        })
        .unwrap();
    let pages: i64 = store
        .connection
        .pragma_query_value(None, "page_count", |r| r.get(0))
        .unwrap();
    store
        .connection
        .pragma_update(None, "max_page_count", pages + 8)
        .unwrap();
    let result = store.transaction(|tx| {
        tx.put(&k, b"changed offset must not survive")?;
        tx.append_download_chunk(&k, 600 * 1024, 0, &vec![7; CHUNK_BYTES])
    });
    assert!(
        matches!(result,Err(StoreError::Database(rusqlite::Error::SqliteFailure(ref e,_))) if e.code==rusqlite::ErrorCode::DiskFull)
    );
    assert_eq!(
        store
            .transaction(|tx| tx.get(&k))
            .unwrap()
            .unwrap()
            .as_slice(),
        b"original offset"
    );
    assert_eq!(
        store
            .connection
            .query_row::<i64, _, _>(
                "SELECT count(*) FROM protected_attachment_download_chunks",
                [],
                |r| r.get(0)
            )
            .unwrap(),
        0
    );
    assert!(matches!(
        store.transaction(|tx| {
            tx.put(&k, b"swallowed error must poison transaction")?;
            let _ = tx.append_download_chunk(&k, 600 * 1024, 0, &[]);
            Ok(())
        }),
        Err(StoreError::Aborted)
    ));
    assert_eq!(
        store
            .transaction(|tx| tx.get(&k))
            .unwrap()
            .unwrap()
            .as_slice(),
        b"original offset"
    );
}
#[test]
fn download_cache_schema_five_migration_is_authenticated_atomic_and_preserves_spool() {
    let root = tempfile::tempdir().unwrap();
    let mut store = connect(root.path(), true);
    let k = record(GENERATION, "attachment-prepare", 0);
    let blob = Uuid::new_v4().to_string();
    let bytes = vec![7; 37];
    let hash: [u8; 32] = Sha256::digest(&bytes).into();
    store
        .transaction(|tx| {
            tx.put(&k, b"existing private descriptor")?;
            tx.put_attachment_blob(&k, &blob, &bytes)
        })
        .unwrap();
    let wrapped: Vec<u8> = store
        .connection
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap();
    store.connection.execute_batch("DROP TABLE protected_attachment_download_chunks; DROP TABLE protected_attachment_downloads; PRAGMA user_version=4;").unwrap();
    drop(store);
    // Deliberately conflicting table proves a failed migration cannot
    // publish a manifest or advance schema/key state.
    let db = Connection::open(scope().database_path(root.path())).unwrap();
    db.execute_batch("CREATE TABLE protected_attachment_download_chunks(conflict INTEGER)")
        .unwrap();
    assert!(matches!(
        ProtectedStore::connect_protected(
            root.path(),
            scope(),
            false,
            &super::super::tests::TestProtector(9)
        ),
        Err(StoreError::Protection)
    ));
    assert!(matches!(
        ProtectedStore::connect_protected(
            root.path(),
            scope(),
            false,
            &super::super::tests::TestProtector(42)
        ),
        Err(StoreError::Database(_))
    ));
    assert_eq!(
        db.pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        4
    );
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT count(*) FROM sqlite_master WHERE name='protected_attachment_downloads'",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    db.execute_batch("DROP TABLE protected_attachment_download_chunks")
        .unwrap();
    drop(db);
    let mut store = connect(root.path(), false);
    assert_eq!(
        store
            .connection
            .pragma_query_value::<i64, _>(None, "user_version", |r| r.get(0))
            .unwrap(),
        SCHEMA
    );
    assert_eq!(
        store
            .connection
            .query_row::<Vec<u8>, _, _>("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
            .unwrap(),
        wrapped
    );
    assert_eq!(
        store
            .transaction(|tx| tx.attachment_blob(&k, &blob, bytes.len(), &hash, true))
            .unwrap(),
        bytes
    );
    assert_eq!(
        store
            .transaction(|tx| tx.get(&k))
            .unwrap()
            .unwrap()
            .as_slice(),
        b"existing private descriptor"
    );
}
