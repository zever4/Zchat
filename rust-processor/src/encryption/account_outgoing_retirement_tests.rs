//! Provisioned replacement + AccountClient retirement + read-only reopen.
use super::*;
use crate::encryption::{
    protected::{StoreScope, test_open},
    provisioning::DeviceProvisioning,
    signal_state::{ArchiveOwner, EpochAttachmentPromotion},
};
const ORIGIN: &str = "https://chat.example.test";
const AG: &str = "33333333-3333-4333-8333-333333333333";

#[tokio::test]
async fn account_retires_accepted_old_generation_spool_preserving_archive_history_and_master_key() {
    let (f, a, _) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let control = super::blob_tests::stage(&f.alice, &id).await;
    f.alice
        .accept_epoch_blob_lease(EpochObservation::test_observation(
            control.store_scope().clone(),
            control.clone(),
            v2::BlobLease {
                message_id: id.clone(),
                operation_id: control.request().operation_id.clone(),
                attempt_id: uuid::Uuid::new_v4().to_string(),
                revision: control.request().expected_revision + 1,
                reserved_at_unix_ms: 1000,
                expires_at_unix_ms: 1000 + crate::encryption::blob_control::STAGING_LEASE_MS,
                observed_at_unix_ms: 1000,
                state: v2::BlobLeaseState::Ready as i32,
                expired: false,
            },
        ))
        .await
        .unwrap();
    let EpochAttachmentPromotion::Queued(p) =
        f.alice.promote_epoch_attachment(id.clone()).await.unwrap()
    else {
        panic!()
    };
    let e = p.request().envelope.as_ref().unwrap();
    f.alice
        .accept_epoch_send_receipt(EpochObservation::test_observation(
            a.scope.clone(),
            (*p).clone(),
            v2::SendReceipt {
                message_id: id.clone(),
                envelope_digest: crate::encryption::epoch_contract::envelope_digest(e)
                    .unwrap()
                    .to_vec(),
                stored_at_unix_ms: 1_700_000_000_000,
                expires_at_unix_ms: 1_700_000_000_000
                    + crate::encryption::contract::MESSAGE_RETENTION_MS,
                crypto_epoch: e.crypto_epoch,
            },
        ))
        .await
        .unwrap();
    assert!(f.alice.seed_history_index().await.unwrap().ready);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let scope = StoreScope::new(ORIGIN, A).unwrap();
    let db = rusqlite::Connection::open(scope.database_path(f.root.path())).unwrap();
    let master: Vec<u8> = db
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap();
    drop(db);
    let mut local = super::archive_file_tests::replaced_local(f.root.path(), A, AG);
    let current = local.ready_device().unwrap();
    let worker = CryptoCoordinator::start(local.into_store(), current)
        .await
        .unwrap();
    let rpc = Arc::new(Probe::default());
    let owner = launch(worker, &rpc);
    let c = owner.client();
    phase(&c, AccountPhase::Running).await;
    assert!(
        !c.open_archived_attachment(AG, CHAT, &id)
            .await
            .unwrap()
            .as_bytes()
            .is_empty()
    );
    c.remove_local_attachment(AG, CHAT, &id).await.unwrap();
    c.remove_local_attachment(AG, CHAT, &id).await.unwrap();
    assert!(matches!(
        c.open_archived_attachment(AG, CHAT, &id).await,
        Err(AccountError::Crypto(CryptoError::AttachmentRemoved))
    ));
    c.read_views(ViewRequest::ArchiveHistory(
        AG.into(),
        CHAT.into(),
        Some(0),
        None,
    ))
    .await
    .unwrap();
    owner.shutdown().await.unwrap();
    assert!(matches!(
        c.remove_local_attachment(AG, CHAT, &id).await,
        Err(AccountError::Stopped)
    ));
    assert!(rpc.blob_events.lock().unwrap().is_empty());
    let db = rusqlite::Connection::open(scope.database_path(f.root.path())).unwrap();
    assert_eq!(
        db.query_row::<Vec<u8>, _, _>("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
            .unwrap(),
        master
    );
    assert_eq!(
        db.query_row::<i64, _, _>("SELECT count(*) FROM protected_attachment_blobs", [], |r| r
            .get(0))
            .unwrap(),
        0
    );
    drop(db);
    let before = std::fs::read(scope.database_path(f.root.path())).unwrap();
    let archived = ArchiveOwner::start(DeviceProvisioning::new(test_open(
        f.root.path(),
        scope.clone(),
        false,
    )))
    .await
    .unwrap()
    .unwrap();
    assert!(matches!(
        archived
            .client()
            .open_archived_attachment(AG, CHAT, &id)
            .await,
        Err(CryptoError::AttachmentRemoved)
    ));
    archived
        .client()
        .read_views(ViewRequest::ArchiveHistory(
            AG.into(),
            CHAT.into(),
            Some(0),
            None,
        ))
        .await
        .unwrap();
    archived.shutdown().await.unwrap();
    assert_eq!(
        std::fs::read(scope.database_path(f.root.path())).unwrap(),
        before
    );
}
