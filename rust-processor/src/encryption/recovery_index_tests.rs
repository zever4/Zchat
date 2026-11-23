use super::*;
use crate::encryption::{
    contract::{MESSAGE_RETENTION_MS, envelope_digest},
    native::ExpiredAcquisition,
};
use crate::proto::e2ee::{Delivery, MessageState, MessageStatus, MessageStatuses, SendReceipt};
use rusqlite::{Connection, params};

fn path(root: &Path, user: &str) -> std::path::PathBuf {
    StoreScope::new(SERVER, user).unwrap().database_path(root)
}
fn downgrade(root: &Path, user: &str) {
    let mut s = store(root, user, false);
    s.transaction_with::<_, CryptoError>(|tx| {
        for (id, bytes) in tx.record_page(
            if user == A { AG } else { BG },
            RecordKind::Control,
            "acquire:",
            None,
        )? {
            let mut j = AcquisitionJournal::decode(bytes.as_slice()).unwrap();
            j.activity_guard_version = 0;
            j.has_message_state = false;
            tx.put(
                &RecordKey::new(if user == A { AG } else { BG }, RecordKind::Control, &id)?,
                &j.encode_to_vec(),
            )?;
        }
        Ok(())
    })
    .unwrap();
    drop(s);
    let db = Connection::open(path(root, user)).unwrap();
    // Reconstruct schema2 completely, including removal of schema5 download
    // tables. Leaving newer tables behind creates a deliberately invalid store.
    db.execute_batch("PRAGMA foreign_keys=OFF;
        DROP TABLE protected_attachment_download_chunks;
        DROP TABLE protected_attachment_downloads;
        ALTER TABLE protected_records DROP COLUMN attachment_ref;
        DROP TABLE protected_attachment_chunks;
        DROP TABLE protected_attachment_blobs;
        ALTER TABLE protected_records DROP COLUMN recovery_ref;
        DROP TABLE protected_recovery;
        DELETE FROM protected_records WHERE kind=13 AND (record_id LIKE 'recovery-index:%' OR record_id LIKE 'recovery-traffic:%');
        PRAGMA user_version=2;").unwrap();
}
fn bytes(root: &Path, user: &str) -> Vec<(Vec<u8>, i64, String, Vec<u8>)> {
    let db = Connection::open(path(root, user)).unwrap();
    let mut s=db.prepare("SELECT generation,kind,record_id,payload FROM protected_records WHERE kind IN (7,10,11,12) ORDER BY generation,kind,record_id").unwrap();
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}
fn wire(envelope: Envelope, n: i64) -> Delivery {
    Delivery {
        sequence: n,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        envelope: Some(envelope),
    }
}
async fn status(worker: &CryptoCoordinator, p: &OutgoingPlan, state: MessageState) {
    let e = p.request().envelope.as_ref().unwrap();
    let receipt = SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
    };
    worker
        .accept_message_statuses(VerifiedMessageStatuses::from_rpc(
            vec![p.clone()],
            MessageStatuses {
                messages: vec![MessageStatus {
                    observed_at_unix_ms: receipt.expires_at_unix_ms + 1,
                    receipt: Some(receipt),
                    state: state as i32,
                }],
            },
        ))
        .await
        .unwrap();
}
async fn seed(worker: &CryptoCoordinator) -> usize {
    let mut total = 0;
    for _ in 0..30 {
        let p = worker.seed_recovery_indexes().await.unwrap();
        assert!(p.scanned <= 32);
        total += p.scanned;
        if p.ready {
            return total;
        }
    }
    panic!("bounded fixture migration did not complete")
}
#[tokio::test]
async fn recovery_migration_pages_reopen_and_pending_queries_bypass_completed_history() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    for n in 1..=35 {
        let env = p
            .alice
            .encrypt(p.a.clone(), payload(&p.a, "retained migration history"))
            .await
            .unwrap();
        p.bob
            .quarantine_delivery(
                p.b.clone(),
                wire(env, n),
                QuarantineReason::InvalidCiphertext,
            )
            .await
            .unwrap();
    }
    let first = p.alice.outbox_page(None).await.unwrap();
    for plan in &first.messages {
        status(&p.alice, plan, MessageState::RecipientStored).await;
    }
    for ack in p
        .bob
        .pending_acknowledgements(None)
        .await
        .unwrap()
        .acknowledgements
    {
        p.bob
            .mark_acknowledged(VerifiedAck::from_rpc(ack))
            .await
            .unwrap();
    }
    let pending = p.alice.outbox_page(None).await.unwrap();
    assert_eq!(pending.messages.len(), 3);
    assert!(pending.next.is_none());
    assert_eq!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        3
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let before_a = bytes(p.root.path(), A);
    let before_b = bytes(p.root.path(), B);
    downgrade(p.root.path(), A);
    downgrade(p.root.path(), B);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert!(matches!(
        alice.outbox_page(None).await,
        Err(CryptoError::RecoveryIndexesNotReady)
    ));
    let db = Connection::open(path(p.root.path(), A)).unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT count(*) FROM protected_recovery", [], |r| r.get(0))
            .unwrap(),
        0
    );
    drop(db);
    let page = alice.seed_recovery_indexes().await.unwrap();
    assert_eq!(page.scanned, 32);
    assert!(!page.ready);
    alice.shutdown().await.unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert_eq!(seed(&alice).await, 39); // 3outbox +35history +1acquisition
    assert_eq!(seed(&bob).await, 70); // 35inbox +35ACK journals
    assert_eq!(alice.outbox_page(None).await.unwrap().messages.len(), 3);
    assert_eq!(
        bob.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        3
    );
    // Expired remains pending to observe a late ACK, terminal ACK leaves the index.
    let one = alice.outbox_page(None).await.unwrap().messages[0].clone();
    status(&alice, &one, MessageState::Expired).await;
    assert_eq!(alice.outbox_page(None).await.unwrap().messages.len(), 3);
    status(&alice, &one, MessageState::RecipientStored).await;
    assert_eq!(alice.outbox_page(None).await.unwrap().messages.len(), 2);
    status(&alice, &one, MessageState::Expired).await;
    assert_eq!(alice.outbox_page(None).await.unwrap().messages.len(), 2);
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
    assert_eq!(bytes(p.root.path(), A), before_a);
    assert_eq!(bytes(p.root.path(), B), before_b);
    let db = Connection::open(path(p.root.path(), A)).unwrap();
    let detail:String=db.query_row("EXPLAIN QUERY PLAN SELECT record_id FROM protected_recovery WHERE generation=?1 AND kind=11 AND pending=1 AND record_id>?2 ORDER BY record_id LIMIT 32",params![uuid::Uuid::parse_str(AG).unwrap().as_bytes().as_slice(),""],|r|r.get(3)).unwrap();
    assert!(detail.contains("protected_recovery_pending"), "{detail}");
}

#[test]
fn recovery_exit_child() {
    let Some(root) = std::env::var_os("ZCHAT_RECOVERY_EXIT_ROOT") else {
        return;
    };
    let mut e = reopen(Path::new(&root), &device(A, AG));
    let target = RecordKey::new(AG, RecordKind::Control, "recovery-index:state:v1").unwrap();
    e.store.test_exit_on_record(
        target,
        std::env::var_os("ZCHAT_RECOVERY_EXIT_AFTER").is_some(),
    );
    e.seed_recovery_indexes().unwrap();
    panic!("process-exit hook did not run");
}
#[tokio::test]
async fn recovery_cursor_projection_and_sticky_evidence_commit_together_on_process_exit() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let message = payload(&p.a, "exact ciphertext survives index migration exits");
    let encrypted = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    downgrade(p.root.path(), A);
    let before = bytes(p.root.path(), A);
    for after in [false, true] {
        let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
        cmd.args([
            "--exact",
            "encryption::signal_state::tests::refill::recovery::recovery_exit_child",
            "--nocapture",
        ])
        .env("ZCHAT_RECOVERY_EXIT_ROOT", p.root.path());
        if after {
            cmd.env("ZCHAT_RECOVERY_EXIT_AFTER", "1");
        } else {
            cmd.env_remove("ZCHAT_RECOVERY_EXIT_AFTER");
        }
        assert_eq!(
            cmd.status().unwrap().code(),
            Some(if after { 74 } else { 73 })
        );
        let db = Connection::open(path(p.root.path(), A)).unwrap();
        assert_eq!(
            db.query_row::<i64, _, _>(
                "SELECT count(*) FROM protected_records WHERE record_id='recovery-index:state:v1'",
                [],
                |r| r.get(0)
            )
            .unwrap(),
            i64::from(after)
        );
        assert_eq!(bytes(p.root.path(), A), before);
    }
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    seed(&alice).await;
    assert_eq!(
        alice
            .outgoing_plan(message.message_id)
            .await
            .unwrap()
            .request()
            .envelope
            .as_ref(),
        Some(&encrypted)
    );
    alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_missing_anchor_unclassified_write_and_deleted_projection_fail_closed() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "do not silently regenerate delivery metadata");
    p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let db = Connection::open(path(p.root.path(), A)).unwrap();
    // Normal SQLite writes with FK checks cannot detach projection from source.
    db.execute_batch("PRAGMA foreign_keys=ON").unwrap();
    assert!(
        db.execute(
            "DELETE FROM protected_recovery WHERE kind=11 AND record_id=?1",
            [&text.message_id]
        )
        .is_err()
    );
    db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
    db.execute(
        "DELETE FROM protected_recovery WHERE kind=11 AND record_id=?1",
        [&text.message_id],
    )
    .unwrap();
    drop(db);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert!(alice.outgoing_plan(text.message_id).await.is_err()); // Point lookup validates projection too.
    alice.shutdown().await.unwrap();
    let db = Connection::open(path(p.root.path(), A)).unwrap();
    db.execute(
        "DELETE FROM protected_records WHERE record_id='recovery-index:anchor:v1'",
        [],
    )
    .unwrap();
    drop(db);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert!(alice.seed_recovery_indexes().await.is_err());
    assert!(alice.outbox_page(None).await.is_err());
    alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_migration_commit_failure_and_corrupt_legacy_page_do_not_advance() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "atomic page");
    p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    downgrade(p.root.path(), A);
    // Schema-only open upgrades physical storage without populating projections.
    drop(store(p.root.path(), A, false));
    let db = Connection::open(path(p.root.path(), A)).unwrap();
    db.execute_batch("CREATE TABLE recovery_fk_parent(id INTEGER PRIMARY KEY); CREATE TABLE recovery_fk_child(id INTEGER REFERENCES recovery_fk_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER fail_recovery_cursor AFTER INSERT ON protected_records WHEN NEW.record_id='recovery-index:state:v1' BEGIN INSERT INTO recovery_fk_child VALUES(1); END;").unwrap();
    drop(db);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert!(alice.seed_recovery_indexes().await.is_err());
    alice.shutdown().await.unwrap();
    let db = Connection::open(path(p.root.path(), A)).unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT count(*) FROM protected_recovery", [], |r| r.get(0))
            .unwrap(),
        0
    );
    db.execute_batch("DROP TRIGGER fail_recovery_cursor; DROP TABLE recovery_fk_child; DROP TABLE recovery_fk_parent")
        .unwrap();
    let raw: Vec<u8> = db
        .query_row(
            "SELECT payload FROM protected_records WHERE kind=11 AND record_id=?1",
            [&text.message_id],
            |r| r.get(0),
        )
        .unwrap();
    let mut corrupt = raw.clone();
    corrupt[20] ^= 1;
    db.execute(
        "UPDATE protected_records SET payload=?1 WHERE kind=11 AND record_id=?2",
        params![corrupt, text.message_id],
    )
    .unwrap();
    drop(db);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert!(alice.seed_recovery_indexes().await.is_err());
    alice.shutdown().await.unwrap();
    let db = Connection::open(path(p.root.path(), A)).unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT count(*) FROM protected_recovery", [], |r| r.get(0))
            .unwrap(),
        0
    );
    db.execute(
        "UPDATE protected_records SET payload=?1 WHERE kind=11 AND record_id=?2",
        params![raw, text.message_id],
    )
    .unwrap();
    drop(db);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    seed(&alice).await;
    assert_eq!(alice.outbox_page(None).await.unwrap().messages.len(), 1);
    alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_pending_ack_completion_retains_confirmation_and_sticky_traffic_guard() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let pending_bob = p
        .bob
        .prepare_acquisition(p.b.clone())
        .await
        .unwrap()
        .request;
    let text = payload(&p.a, "confirmation remains durable");
    let env = p.alice.encrypt(p.a.clone(), text).await.unwrap();
    let (_, ack) = p
        .bob
        .receive_for_delivery(p.b.clone(), wire(env, 1))
        .await
        .unwrap();
    assert_eq!(p.bob.outbox_page(None).await.unwrap().messages.len(), 1);
    p.bob
        .mark_acknowledged(VerifiedAck::from_rpc(ack))
        .await
        .unwrap();
    assert!(
        p.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert_eq!(p.bob.outbox_page(None).await.unwrap().messages.len(), 1);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let db = Connection::open(path(p.root.path(), B)).unwrap();
    db.execute(
        "DELETE FROM protected_records WHERE kind=7 OR record_id LIKE 'recovery-traffic:%'",
        [],
    )
    .unwrap();
    drop(db);
    // No new acquisition/ratchet is created from an empty pending-ACK page.
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert!(matches!(
        bob.renew_acquisition(p.b, ExpiredAcquisition::test_proof(pending_bob))
            .await,
        Err(CryptoError::Conflict)
    ));
    bob.shutdown().await.unwrap();
}
