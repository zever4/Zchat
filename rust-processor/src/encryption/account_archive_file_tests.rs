//! Real provisioning/Signal/download-to-cache followed by local-only archive
//! ownership. Scripted HTTP is only the initial download; archive has no client.
use super::download_tests::{ReadProbe, launch_read, receive};
use super::*;
use crate::encryption::{
    protected::{StoreScope, test_open},
    provisioning::DeviceProvisioning,
    signal_state::{ArchiveOwner, EpochAttachmentFile},
};
use std::path::Path;

const ORIGIN: &str = "https://chat.example.test";
const CONTENT: &[u8] = b"account-owned private attachment bytes";
const BG: &str = "44444444-4444-4444-8444-444444444444";
const NEXT: &str = "55555555-5555-4555-8555-555555555555";

fn local(root: &Path, user: &str) -> DeviceProvisioning {
    DeviceProvisioning::new(test_open(
        root,
        StoreScope::new(ORIGIN, user).unwrap(),
        false,
    ))
}
fn database(root: &Path) -> std::path::PathBuf {
    StoreScope::new(ORIGIN, B).unwrap().database_path(root)
}
async fn open_active(c: &AccountClient, generation: &str, id: &str) -> EpochAttachmentFile {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match c.open_archived_attachment(generation, CHAT, id).await {
                Ok(file) => return file,
                Err(AccountError::Crypto(CryptoError::Busy)) => tokio::task::yield_now().await,
                Err(e) => panic!("archive file failed: {e:?}"),
            }
        }
    })
    .await
    .unwrap()
}

async fn cached() -> (tempfile::TempDir, String, Arc<ReadProbe>) {
    let (f, a, b) = prepared().await;
    let (id, blob) = receive(&f, &a, &b).await;
    let body = Arc::new(ReadProbe::default());
    body.file(&id, blob);
    let rpc = Arc::new(Probe::default());
    let owner = launch_read(f.bob, &rpc, &body);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    assert!(matches!(
        client.open_archived_attachment(BG, CHAT, &id).await,
        Err(AccountError::Crypto(CryptoError::MessageUnavailable))
    ));
    assert!(body.seen.lock().unwrap().is_empty());
    client.download_attachment(&id).await.unwrap();
    super::file_ack_tests::saved(&client, &id).await;
    {
        let file = open_active(&client, BG, &id).await;
        assert_eq!(file.as_bytes(), CONTENT);
        assert!(matches!(
            client.open_cached_attachment(&id).await,
            Err(AccountError::Crypto(CryptoError::Busy))
        ));
        assert!(matches!(
            client.open_archived_attachment(BG, CHAT, &id).await,
            Err(AccountError::Crypto(CryptoError::Busy))
        ));
        // A held file buffer must not monopolize ordinary presentation reads.
        client
            .read_views(ViewRequest::Archives(None))
            .await
            .unwrap();
    }
    owner.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
    (f.root, id, body)
}

#[tokio::test]
async fn archive_file_owner_is_readonly_reopens_and_buffer_does_not_keep_database_alive() {
    let (root, id, body) = cached().await;
    let before = std::fs::read(database(root.path())).unwrap();
    let owner = ArchiveOwner::start(local(root.path(), B))
        .await
        .unwrap()
        .unwrap();
    let client = owner.client();
    let held = client
        .open_archived_attachment(BG, CHAT, &id)
        .await
        .unwrap();
    assert_eq!(held.as_bytes(), CONTENT);
    assert!(matches!(
        client.open_archived_attachment(BG, CHAT, &id).await,
        Err(CryptoError::Busy)
    ));
    client
        .read_views(ViewRequest::Archives(None))
        .await
        .unwrap();
    assert_eq!(std::fs::read(database(root.path())).unwrap(), before);
    owner.shutdown().await.unwrap();
    assert!(matches!(
        client.open_archived_attachment(BG, CHAT, &id).await,
        Err(CryptoError::Stopped)
    ));
    // Returned native bytes have no DB lease. A cloned client/buffer cannot
    // prevent shutdown or keep an old database/key/owner.lock alive.
    let again = ArchiveOwner::start(local(root.path(), B))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        again
            .client()
            .open_archived_attachment(BG, CHAT, &id)
            .await
            .unwrap()
            .as_bytes(),
        CONTENT
    );
    // Publish a file reply, but leave its caller unpolled. The following view
    // is a FIFO barrier on the SAME archive thread, so shutdown happens after
    // the plaintext was ready, not merely before admission.
    let late_client = again.client();
    let (release, held_thread) = std::sync::mpsc::channel();
    late_client
        .hold_file_test_thread(held_thread)
        .await
        .unwrap();
    let mut late = Box::pin(late_client.open_archived_attachment(BG, CHAT, &id));
    {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let mut context = Context::from_waker(Waker::noop());
        assert!(matches!(late.as_mut().poll(&mut context), Poll::Pending));
    }
    release.send(()).unwrap();
    late_client
        .read_views(ViewRequest::Archives(None))
        .await
        .unwrap();
    again.shutdown().await.unwrap();
    assert!(matches!(late.await, Err(CryptoError::Stopped)));
    assert_eq!(held.as_bytes(), CONTENT);
    drop(held);
    assert_eq!(body.seen.lock().unwrap().len(), 1);
    assert_eq!(std::fs::read(database(root.path())).unwrap(), before);
}

#[tokio::test]
async fn archive_file_known_old_generation_uses_real_replacement_proof_without_resuming_worker() {
    let (root, id, body) = cached().await;
    let mut p = replaced_local(root.path(), B, BG);
    let current = p.ready_device().unwrap();
    // The CURRENT worker must retain strict download/FILEACK own-generation gates.
    let worker = CryptoCoordinator::start(p.into_store(), current)
        .await
        .unwrap();
    assert!(
        worker
            .epoch_attachment_download_plan(id.clone())
            .await
            .is_err()
    );
    assert_eq!(
        worker
            .open_archived_epoch_attachment(BG.into(), CHAT.into(), id.clone())
            .await
            .unwrap()
            .as_bytes(),
        CONTENT
    );
    worker.shutdown().await.unwrap();
    let before = std::fs::read(database(root.path())).unwrap();
    let archive = ArchiveOwner::start(local(root.path(), B))
        .await
        .unwrap()
        .unwrap();
    let client = archive.client();
    assert_eq!(
        client
            .open_archived_attachment(BG, CHAT, &id)
            .await
            .unwrap()
            .as_bytes(),
        CONTENT
    );
    for (generation, chat, message) in [
        (NEXT, CHAT, id.as_str()),
        (A, CHAT, id.as_str()),
        (BG, NEXT, id.as_str()),
        (BG, CHAT, NEXT),
    ] {
        assert!(
            client
                .open_archived_attachment(generation, chat, message)
                .await
                .is_err()
        );
    }
    archive.shutdown().await.unwrap();
    assert_eq!(std::fs::read(database(root.path())).unwrap(), before);
    let other = ArchiveOwner::start(local(root.path(), A))
        .await
        .unwrap()
        .unwrap();
    assert!(
        other
            .client()
            .open_archived_attachment(BG, CHAT, &id)
            .await
            .is_err()
    );
    assert!(
        other
            .client()
            .open_archived_attachment("33333333-3333-4333-8333-333333333333", CHAT, &id)
            .await
            .is_err()
    );
    other.shutdown().await.unwrap();
    assert_eq!(body.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn archive_file_missing_or_corrupt_cache_fails_without_repair_or_network() {
    for kind in ["chunk", "missing chunks", "manifest", "journal"] {
        let (root, id, body) = cached().await;
        let db = rusqlite::Connection::open(database(root.path())).unwrap();
        match kind {
            "chunk" => {
                db.execute("UPDATE protected_attachment_download_chunks SET payload=zeroblob(length(payload))",[]).unwrap();
            }
            "manifest" => {
                db.execute(
                    "UPDATE protected_attachment_downloads SET blob_id=?1",
                    [uuid::Uuid::new_v4().to_string()],
                )
                .unwrap();
            }
            "missing chunks" => {
                db.execute("DELETE FROM protected_attachment_download_chunks", [])
                    .unwrap();
            }
            "journal" => {
                db.execute("UPDATE protected_records SET payload=zeroblob(length(payload)) WHERE record_id=?1",[format!("attachment-download:{id}")]).unwrap();
            }
            _ => unreachable!(),
        }
        drop(db);
        let before = std::fs::read(database(root.path())).unwrap();
        let owner = ArchiveOwner::start(local(root.path(), B))
            .await
            .unwrap()
            .unwrap();
        assert!(
            owner
                .client()
                .open_archived_attachment(BG, CHAT, &id)
                .await
                .is_err()
        );
        owner.shutdown().await.unwrap();
        assert_eq!(
            std::fs::read(database(root.path())).unwrap(),
            before,
            "{kind} was repaired"
        );
        assert_eq!(body.seen.lock().unwrap().len(), 1);
    }
}

pub(super) fn replaced_local(root: &Path, user: &str, generation: &str) -> DeviceProvisioning {
    let mut p = local(root, user);
    let attempt = p.prepare_replacement(generation, None).unwrap();
    let request = attempt.request();
    p.bind_replacement(
        &request.registration_request_id,
        &common::OwnDeviceState {
            user_id: user.into(),
            active_device: Some(common::Device {
                reference: Some(DeviceRef {
                    user_id: user.into(),
                    device_id: 1,
                    generation_id: NEXT.into(),
                }),
                registration_id: request.registration_id,
                identity_key: request.identity_key.clone(),
                public_material_ready: false,
                public_material_revision: 0,
            }),
        },
    )
    .unwrap();
    while let Some(batch) = p.next_upload().unwrap() {
        p.acknowledge_upload(&common::UploadReceipt {
            batch_id: batch.batch_id.clone(),
            batch_digest: crate::encryption::publication::publication_digest(&batch)
                .unwrap()
                .to_vec(),
            publication_revision: batch.expected_revision + 1,
        })
        .unwrap();
    }
    p
}

#[tokio::test]
async fn account_local_removal_retires_acknowledged_old_generation_without_losing_history_or_restarting_it()
 {
    let (root, id, body) = cached().await;
    let mut p = replaced_local(root.path(), B, BG);
    let own = p.ready_device().unwrap();
    let wrapped: Vec<u8> = rusqlite::Connection::open(database(root.path()))
        .unwrap()
        .query_row("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
        .unwrap();
    let worker = CryptoCoordinator::start(p.into_store(), own).await.unwrap();
    let rpc = Arc::new(Probe::default());
    let owner = launch(worker, &rpc);
    let client = owner.client();
    phase(&client, AccountPhase::Running).await;
    client.remove_local_attachment(BG, CHAT, &id).await.unwrap();
    client.remove_local_attachment(BG, CHAT, &id).await.unwrap();
    assert!(matches!(
        client.open_archived_attachment(BG, CHAT, &id).await,
        Err(AccountError::Crypto(CryptoError::AttachmentRemoved))
    ));
    let ViewPage::History(items, _) = client
        .read_views(ViewRequest::ArchiveHistory(
            BG.into(),
            CHAT.into(),
            Some(0),
            None,
        ))
        .await
        .unwrap()
    else {
        panic!("expected archived history");
    };
    assert!(items.iter().any(|item| item.message_id == id
        && item.cache_state
            == Some(crate::encryption::signal_state::AttachmentCacheState::Removed)));
    assert_eq!(client.status().phase, AccountPhase::Running);
    owner.shutdown().await.unwrap();
    assert!(matches!(
        client.remove_local_attachment(BG, CHAT, &id).await,
        Err(AccountError::Stopped)
    ));
    let db = rusqlite::Connection::open(database(root.path())).unwrap();
    assert_eq!(
        db.query_row::<i64, _, _>(
            "SELECT count(*) FROM protected_attachment_downloads",
            [],
            |r| r.get(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row::<Vec<u8>, _, _>("SELECT wrapped_key FROM store_header", [], |r| r.get(0))
            .unwrap(),
        wrapped
    );
    drop(db);
    let archive = ArchiveOwner::start(local(root.path(), B))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        archive
            .client()
            .open_archived_attachment(BG, CHAT, &id)
            .await,
        Err(CryptoError::AttachmentRemoved)
    ));
    archive.shutdown().await.unwrap();
    assert_eq!(body.seen.lock().unwrap().len(), 1);
}
