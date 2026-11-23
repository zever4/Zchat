//! Actual provisioned replacement proof and read-only reopen of outgoing files.
//! Server READY is scripted; Signal promotion and the local SQLite are real.
use super::*;
use crate::encryption::{
    protected::{StoreScope, test_open},
    provisioning::DeviceProvisioning,
    signal_state::{ArchiveOwner, EpochAttachmentPromotion},
};

const ORIGIN: &str = "https://chat.example.test";
const AG: &str = "33333333-3333-4333-8333-333333333333";

fn local(root: &std::path::Path, user: &str) -> DeviceProvisioning {
    DeviceProvisioning::new(test_open(
        root,
        StoreScope::new(ORIGIN, user).unwrap(),
        false,
    ))
}

#[tokio::test]
async fn outgoing_archive_account_and_readonly_owner_open_replaced_generation_without_resuming_outbox()
 {
    let (f, _, _) = prepared().await;
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
    let EpochAttachmentPromotion::Queued(original) =
        f.alice.promote_epoch_attachment(id.clone()).await.unwrap()
    else {
        panic!("expected promoted outgoing file");
    };
    assert!(original.needs_send());
    // Populate the existing history view before retiring this generation. A
    // read-only archive must never seed/repair that index itself.
    assert!(f.alice.seed_history_index().await.unwrap().ready);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    let mut p = super::archive_file_tests::replaced_local(f.root.path(), A, AG);
    let own = p.ready_device().unwrap();
    let worker = CryptoCoordinator::start(p.into_store(), own).await.unwrap();
    // Unknown in the replacement generation: no outgoing plan is exposed.
    assert!(
        worker
            .epoch_attachment_outgoing(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    let rpc = Arc::new(Probe::default());
    let owner = launch(worker, &rpc);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    let content = b"private-account-upload-marker".repeat(1024);
    assert!(client.open_current_attachment(CHAT, &id).await.is_err());
    let file = client
        .open_archived_attachment(AG, CHAT, &id)
        .await
        .unwrap();
    assert_eq!(file.as_bytes(), content);
    assert_eq!(file.kind(), v2::AttachmentKind::File);
    assert!(matches!(
        client.open_archived_attachment(AG, CHAT, &id).await,
        Err(AccountError::Crypto(CryptoError::Busy))
    ));
    client
        .read_views(ViewRequest::Archives(None))
        .await
        .unwrap();
    drop(file);
    assert!(matches!(
        client
            .open_archived_attachment(AG, &uuid::Uuid::new_v4().to_string(), &id)
            .await,
        Err(AccountError::Crypto(CryptoError::Conflict))
    ));
    owner.shutdown().await.unwrap();
    assert!(matches!(
        client.open_archived_attachment(AG, CHAT, &id).await,
        Err(AccountError::Stopped)
    ));
    assert!(rpc.blob_events.lock().unwrap().is_empty());
    let path = StoreScope::new(ORIGIN, A)
        .unwrap()
        .database_path(f.root.path());
    let before = std::fs::read(&path).unwrap();
    let archive = ArchiveOwner::start(local(f.root.path(), A))
        .await
        .unwrap()
        .unwrap();
    let reader = archive.client();
    let held = reader
        .open_archived_attachment(AG, CHAT, &id)
        .await
        .unwrap();
    assert_eq!(held.as_bytes(), content);
    reader
        .read_views(ViewRequest::ArchiveHistory(
            AG.into(),
            CHAT.into(),
            Some(0),
            None,
        ))
        .await
        .unwrap();
    archive.shutdown().await.unwrap();
    assert!(matches!(
        reader.open_archived_attachment(AG, CHAT, &id).await,
        Err(CryptoError::Stopped)
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    // A returned buffer does not retain the database lease across an owner stop.
    let archive = ArchiveOwner::start(local(f.root.path(), A))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        archive
            .client()
            .open_archived_attachment(AG, CHAT, &id)
            .await
            .unwrap()
            .as_bytes(),
        content
    );
    archive.shutdown().await.unwrap();
    assert_eq!(held.as_bytes(), content);
    drop(held);
    let other = ArchiveOwner::start(local(f.root.path(), B))
        .await
        .unwrap()
        .unwrap();
    assert!(
        other
            .client()
            .open_archived_attachment(AG, CHAT, &id)
            .await
            .is_err()
    );
    other.shutdown().await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
