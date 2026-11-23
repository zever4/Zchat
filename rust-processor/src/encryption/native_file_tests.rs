//! Actual account-owned upload/Send/recovery/download/cache over Go TLS/PG.
//! No test-constructed crypto plans, leases or receipts; automatic owner FILEACK, no GUI.
use super::account_tests::{config, epoch, ready, settled};
use super::*;
use crate::encryption::{
    account_owner::{AccountClient, AccountError, AccountOwner, AccountPhase},
    attachment::{self, AttachmentBinding, AttachmentDescriptor},
    attachment_payload::AttachmentDraft,
    discovery::TrustedServer,
    provisioning::{DeviceProvisioning, Reconciliation},
    signal_state::{
        CryptoCoordinator, CryptoError, EpochAttachmentPreparation, EpochFileProgress,
        EpochRecoveryDecision,
    },
};
use crate::proto::e2ee_v2 as v2;
use std::{io::Read, path::Path};

const BASE: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1";
const RETRY: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2";
const SEALED: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3";
const NEXT: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4";
fn source() -> Zeroizing<Vec<u8>> {
    Zeroizing::new(b"private-owner-file-content-marker".repeat(4096))
}
async fn cached(
    client: &AccountClient,
    id: &str,
) -> crate::encryption::signal_state::EpochAttachmentFile {
    // The automatic fresh proof shares file memory with explicit cache open.
    // Only bounded Busy may be retried, never integrity/auth/storage errors.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match client.open_cached_attachment(id).await {
                Ok(file) => return file,
                Err(AccountError::Crypto(CryptoError::Busy)) => {
                    tokio::time::sleep(Duration::from_millis(5)).await
                }
                Err(e) => panic!("native cached file read failed: {e:?}"),
            }
        }
    })
    .await
    .unwrap()
}
async fn download(client: &AccountClient, id: &str) {
    // Bound only transient memory admission, not the real HTTP operation's
    // existing owner deadline (60s). A transfer is not a memory retry.
    let busy_until = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match client.download_attachment(id).await {
            Ok(()) => return,
            Err(AccountError::Crypto(CryptoError::Busy)) => {
                assert!(
                    tokio::time::Instant::now() < busy_until,
                    "file memory remained busy"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => panic!("native file download failed: {e:?}; {:?}", client.status()),
        }
    }
}
async fn promoted(client: &AccountClient, id: &str) {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert_eq!(client.status().phase, AccountPhase::Running);
            match client.outgoing(id).await {
                Ok(_) => break,
                Err(AccountError::Crypto(CryptoError::MessageUnavailable)) => (),
                Err(e) => panic!("unexpected outgoing error: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}
async fn stored(client: &AccountClient, id: &str) {
    promoted(client, id).await;
    super::account_tests::stored(client, id).await;
}
async fn stage(root: &Path, scope: &StoreScope, own: &DeviceRef, chat: &str, id: &str) -> Vec<u8> {
    let w = CryptoCoordinator::start(tests::local_store(root, scope.clone(), false), own.clone())
        .await
        .unwrap();
    let draft = AttachmentDraft {
        kind: v2::AttachmentKind::File,
        filename: Zeroizing::new("private-owner-file.bin".into()),
        mime_type: Zeroizing::new("application/octet-stream".into()),
        caption: Zeroizing::new("private-owner-file-caption".into()),
        photo_metadata_stripped: None,
    };
    let EpochAttachmentPreparation::Prepared(p) = w
        .prepare_epoch_attachment(chat.into(), id.into(), draft, source())
        .await
        .unwrap()
    else {
        panic!("authenticated catalogue missing")
    };
    let bytes = w.prepared_epoch_attachment_blob(*p).await.unwrap();
    let exact = bytes.as_bytes().to_vec();
    drop(bytes);
    w.shutdown().await.unwrap();
    exact
}
async fn owner(fields: &[&str], root: &Path, scope: &StoreScope, i: usize) -> AccountOwner {
    let ca = std::fs::read(fields[2]).unwrap();
    let mut a = TrustedServer::new(fields[1], fields[0], Some(&ca))
        .unwrap()
        .discover(fields[1], Zeroizing::new(fields[3 + i].into()))
        .await
        .unwrap();
    assert!(a.scope() == *scope);
    let mut local = DeviceProvisioning::new(tests::local_store(root, scope.clone(), false));
    assert_eq!(
        a.resume_initial(&mut local).await.unwrap(),
        Reconciliation::Ready
    );
    let o = AccountOwner::start(a, local, config()).await.unwrap();
    ready(&o.client()).await;
    o
}

// Wait for the account's own authenticated polling +protected COMMIT. No
// direct status RPC or status writer bypasses the owner in this acceptance test.
async fn sender_progress(
    client: &AccountClient,
    id: &str,
    acknowledged: bool,
) -> EpochFileProgress {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            assert_eq!(client.status().phase, AccountPhase::Running);
            match client.attachment_file_progress(id).await {
                Ok(p)
                    if p.observed_at_unix_ms().is_some()
                        && p.acknowledged_at_unix_ms().is_some() == acknowledged =>
                {
                    return p;
                }
                Ok(_) | Err(AccountError::Initializing) => (),
                Err(e) => panic!("automatic sender file progress: {e:?}"),
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|e| panic!("sender file wait {id}: {e}; {:?}", client.status()))
}

#[test]
#[ignore = "requires the Go owned-file TLS/HTTP/PostgreSQL fixture"]
fn go_account_file_worker() {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(24576)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input.lines().collect();
    assert_eq!(fields.len(), 7);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(Box::pin(run(&fields)));
    println!("zchat-native-account-file-complete:v8-owned-file-gc");
}
async fn run(fields: &[&str]) {
    let root = tempfile::tempdir().unwrap();
    let ca = std::fs::read(fields[2]).unwrap();
    let mut accounts = Vec::new();
    let mut locals = Vec::new();
    let mut scopes = Vec::new();
    let mut devices = Vec::new();
    for token in [fields[3], fields[4]] {
        let mut a = TrustedServer::new(fields[1], fields[0], Some(&ca))
            .unwrap()
            .discover(fields[1], Zeroizing::new(token.into()))
            .await
            .unwrap();
        let scope = a.scope();
        let mut local =
            DeviceProvisioning::new(tests::local_store(root.path(), scope.clone(), true));
        assert_eq!(
            a.resume_initial(&mut local).await.unwrap(),
            Reconciliation::Ready
        );
        devices.push(local.ready_device().unwrap());
        scopes.push(scope);
        locals.push(local);
        accounts.push(a);
    }
    assert_ne!(
        scopes[0].database_path(root.path()),
        scopes[1].database_path(root.path())
    );
    let wa = CryptoCoordinator::start(locals.remove(0).into_store(), devices[0].clone())
        .await
        .unwrap();
    let wb = CryptoCoordinator::start(locals.remove(0).into_store(), devices[1].clone())
        .await
        .unwrap();
    let mut aa = accounts.remove(0);
    let mut bb = accounts.remove(0);
    let ar = aa.device_client().unwrap();
    let br = bb.device_client().unwrap();
    let chat = ar
        .create_conversation(&wa, fields[5].parse().unwrap())
        .await
        .unwrap()
        .conversation_id;
    br.get_conversation(&wb, &chat).await.unwrap();
    for (w, mut client) in [
        (&wa, ar.epoch_transport().unwrap()),
        (&wb, br.epoch_transport().unwrap()),
    ] {
        while !w.seed_recovery_indexes().await.unwrap().ready {}
        let q = w.prepare_initial_epoch(chat.clone()).await.unwrap();
        w.accept_initial_epoch(client.initial_epoch_catalog(&q).await.unwrap())
            .await
            .unwrap();
    }
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
    drop(aa);
    drop(bb);

    let mut ciphertexts = Vec::new();
    ciphertexts.push((
        BASE,
        stage(root.path(), &scopes[0], &devices[0], &chat, BASE).await,
    ));
    let ob = owner(fields, root.path(), &scopes[1], 1).await;
    let bc = ob.client();
    let oa = owner(fields, root.path(), &scopes[0], 0).await;
    stored(&oa.client(), BASE).await;
    sender_progress(&oa.client(), BASE, false).await;
    oa.shutdown().await.unwrap();

    ciphertexts.push((
        RETRY,
        stage(root.path(), &scopes[0], &devices[0], &chat, RETRY).await,
    ));
    let oa = owner(fields, root.path(), &scopes[0], 0).await;
    let ac = oa.client();
    tokio::time::timeout(Duration::from_secs(25), async {
        while !Path::new(fields[6]).exists() {
            assert_eq!(ac.status().phase, AccountPhase::Running);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(!ac.outgoing(RETRY).await.unwrap().stored_on_server);
    assert!(ac.status().blob_retry_steps > 0);
    oa.shutdown().await.unwrap();
    // The exact server Send committed, but this local outbox still has no receipt.
    let w = CryptoCoordinator::start(
        tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    assert!(
        w.epoch_attachment_outgoing(RETRY.into())
            .await
            .unwrap()
            .unwrap()
            .needs_send()
    );
    w.shutdown().await.unwrap();
    std::fs::write(format!("{}.resume", fields[6]), b"resume").unwrap();
    let oa = owner(fields, root.path(), &scopes[0], 0).await;
    stored(&oa.client(), RETRY).await;
    oa.shutdown().await.unwrap();

    let _ = stage(root.path(), &scopes[0], &devices[0], &chat, SEALED).await;
    let oa = owner(fields, root.path(), &scopes[0], 0).await;
    let ac = oa.client();
    promoted(&ac, SEALED).await;
    let proposal = ac.propose_recovery(&chat).await.unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if bc.recoveries(&chat).await.unwrap().iter().any(|r| {
                r.proposal_id == proposal
                    && r.state == Some(crate::proto::e2ee::RecoveryState::Pending)
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        bc.conversation(&chat).await.unwrap().unwrap().crypto_epoch,
        0
    );
    bc.decide_recovery(&chat, &proposal, EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    epoch(&ac, &chat, 1).await;
    epoch(&bc, &chat, 1).await;
    tokio::time::timeout(Duration::from_secs(20), async {
        while !ac.outgoing(SEALED).await.unwrap().sealed_unsent {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(!ac.outgoing(SEALED).await.unwrap().stored_on_server);
    settled(&ac, true).await;
    oa.shutdown().await.unwrap();

    ciphertexts.push((
        NEXT,
        stage(root.path(), &scopes[0], &devices[0], &chat, NEXT).await,
    ));
    let oa = owner(fields, root.path(), &scopes[0], 0).await;
    stored(&oa.client(), NEXT).await;
    let mut before = Vec::new();
    for id in [NEXT, BASE, RETRY] {
        before.push(sender_progress(&oa.client(), id, false).await);
    }
    assert!(
        before
            .iter()
            .all(|s| s.needs_poll() && !s.deadline_observed())
    );
    oa.shutdown().await.unwrap();
    // stored() already observed RecipientStored: descriptor ACK committed on
    // Go BEFORE these GETs. Both epoch0 files remain readable after recovery1.
    for (id, _) in &ciphertexts {
        download(&bc, id).await;
        assert_eq!(cached(&bc, id).await.as_bytes(), source().as_slice());
        // Must reuse verified cache, not issue a second server GET.
        download(&bc, id).await;
    }
    std::fs::write(format!("{}.files-cached", fields[6]), b"ready").unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !Path::new(&format!("{}.file-ack-committed", fields[6])).exists() {
            assert_eq!(bc.status().phase, AccountPhase::Running);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert!(bc.attachment_file_ack_status(BASE).await.unwrap().is_none());
    ob.shutdown().await.unwrap();
    let reopened = owner(fields, root.path(), &scopes[1], 1).await;
    for (id, _) in &ciphertexts {
        assert_eq!(
            cached(&reopened.client(), id).await.as_bytes(),
            source().as_slice()
        );
        download(&reopened.client(), id).await;
    }
    std::fs::write(format!("{}.ack-resume", fields[6]), b"ready").unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        for (id, _) in &ciphertexts {
            loop {
                assert_eq!(reopened.client().status().phase, AccountPhase::Running);
                if reopened
                    .client()
                    .attachment_file_ack_status(id)
                    .await
                    .unwrap()
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(20), async {
        while !Path::new(&format!("{}.server-gc", fields[6])).exists() {
            assert_eq!(reopened.client().status().phase, AccountPhase::Running);
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    for (id, _) in &ciphertexts {
        assert_eq!(
            cached(&reopened.client(), id).await.as_bytes(),
            source().as_slice()
        );
        download(&reopened.client(), id).await;
    }
    reopened.shutdown().await.unwrap();
    let after_gc = owner(fields, root.path(), &scopes[1], 1).await;
    for (id, _) in &ciphertexts {
        assert_eq!(
            cached(&after_gc.client(), id).await.as_bytes(),
            source().as_slice()
        );
        download(&after_gc.client(), id).await;
    }
    after_gc.shutdown().await.unwrap();
    let oa = owner(fields, root.path(), &scopes[0], 0).await;
    let mut after = Vec::new();
    for id in [RETRY, BASE, NEXT] {
        let saved = sender_progress(&oa.client(), id, true).await;
        let old = before.iter().find(|p| p.message_id() == id).unwrap();
        assert_eq!(old.receipt(), saved.receipt());
        assert_eq!(old.expires_at_unix_ms(), saved.expires_at_unix_ms());
        assert!(!saved.needs_poll() && saved.deadline_observed());
        after.push(saved);
    }
    oa.shutdown().await.unwrap();
    // Reopen independently: polling completion and immutable receipts survived
    // the real owner shutdown; message ACK cannot recreate file pending work.
    let sender = CryptoCoordinator::start(
        tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    assert!(
        sender
            .pending_epoch_file_statuses(None)
            .await
            .unwrap()
            .files
            .is_empty()
    );
    for p in &after {
        let saved = sender
            .epoch_file_progress(p.message_id().into())
            .await
            .unwrap();
        assert_eq!(saved.revision(), p.revision());
        assert_eq!(saved.acknowledged_at_unix_ms(), p.acknowledged_at_unix_ms());
        assert_eq!(
            sender
                .epoch_outgoing_plan(p.message_id().into())
                .await
                .unwrap()
                .receipt(),
            Some(p.receipt())
        );
    }
    sender.shutdown().await.unwrap();
    // Verify the ACTUALLY received/decrypted Signal descriptor against exact
    // original ciphertext as well as the actual downloaded/reopened cache.
    let w = CryptoCoordinator::start(
        tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    for (id, exact) in &ciphertexts {
        let p = w.epoch_incoming_message((*id).into()).await.unwrap();
        let Some(v2::encrypted_payload::Content::Attachment(a)) = p.content.as_ref() else {
            panic!("not an attachment")
        };
        assert_eq!(a.filename, "private-owner-file.bin");
        let binding = AttachmentBinding::new(
            &chat,
            p.crypto_epoch.unwrap(),
            id,
            &a.blob_id,
            p.sender.as_ref().unwrap(),
            p.recipient.as_ref().unwrap(),
        )
        .unwrap();
        let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor).unwrap();
        assert_eq!(
            attachment::open(&binding, &descriptor, exact)
                .unwrap()
                .as_slice(),
            source().as_slice()
        );
        assert_eq!(p.crypto_epoch, Some(if *id == NEXT { 1 } else { 0 }));
    }
    assert!(w.epoch_incoming_message(SEALED.into()).await.is_err());
    for (id, _) in &ciphertexts {
        let local = w
            .epoch_file_ack_status((*id).into())
            .await
            .unwrap()
            .unwrap();
        let remote = after.iter().find(|s| s.message_id() == *id).unwrap();
        assert_eq!(remote.acknowledged_at_unix_ms(), Some(local));
    }
    let pending = w.pending_epoch_file_acks(None).await.unwrap();
    assert!(pending.messages.is_empty() && pending.next.is_none());
    w.shutdown().await.unwrap();
}
