use super::super::tests::store;
use super::incoming::tests::{accepted, b_conversation, delivery, payload, prepared, raw, send};
use super::tests::{index, records};
use super::*;
use crate::encryption::{attachment, attachment_payload, epoch_contract as data};
use crate::proto::e2ee::{AckDisposition, ConversationCloseReason};

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";

pub(super) fn message(s: &EpochScope) -> (v2::EncryptedPayload, attachment::PreparedAttachment) {
    let mut p = payload(s, "");
    p.format_version = attachment_payload::PAYLOAD_FORMAT;
    let blob_id = uuid::Uuid::new_v4().to_string();
    let binding = attachment::AttachmentBinding::new(
        &p.conversation_id,
        p.crypto_epoch.unwrap(),
        &p.message_id,
        &blob_id,
        p.sender.as_ref().unwrap(),
        p.recipient.as_ref().unwrap(),
    )
    .unwrap();
    let file = attachment::seal(&binding, Zeroizing::new(b"private file bytes".to_vec())).unwrap();
    p.content = Some(v2::encrypted_payload::Content::Attachment(
        v2::AttachmentMessage {
            format_version: 1,
            blob_id,
            kind: v2::AttachmentKind::File as i32,
            private_descriptor: file.descriptor().encode_for_signal().to_vec(),
            filename: "private-file-marker.bin".into(),
            mime_type: "application/octet-stream".into(),
            caption: "private attachment caption".into(),
            photo_metadata_stripped: None,
        },
    ));
    (p, file)
}
fn attachment_mut(p: &mut v2::EncryptedPayload) -> &mut v2::AttachmentMessage {
    match p.content.as_mut().unwrap() {
        v2::encrypted_payload::Content::Attachment(a) => a,
        _ => panic!("wrong message"),
    }
}
fn wire(p: &EpochOutgoingPlan, sequence: i64) -> v2::Delivery {
    let e = p.request().envelope.clone().unwrap();
    v2::Delivery {
        sequence,
        envelope_digest: data::envelope_digest(&e).unwrap().to_vec(),
        envelope: Some(e),
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
    }
}
async fn seed(w: &CryptoCoordinator) {
    while !w.seed_history_index().await.unwrap().ready {}
}
fn history(p: ViewPage) -> (Vec<HistoryItem>, Option<ViewCursor>) {
    match p {
        ViewPage::History(v, c) => (v, c),
        _ => panic!("wrong page"),
    }
}

#[tokio::test]
async fn attachment_signal_receive_reopen_exact_retry_history_and_confirmation_without_file_ack() {
    let (f, a, b) = prepared().await;
    let (p, file) = message(&a);
    let plan = f.alice.encrypt_epoch(a.clone(), p.clone()).await.unwrap();
    let exact = plan.request().clone();
    assert_eq!(
        exact.envelope.as_ref().unwrap().message_type,
        CiphertextType::PrekeySignal as i32
    );
    assert_eq!(
        f.alice
            .encrypt_epoch(a.clone(), p.clone())
            .await
            .unwrap()
            .request(),
        &exact
    );
    let before = records(f.root.path(), A);
    let mut changed = p.clone();
    attachment_mut(&mut changed).filename = "changed-name.bin".into();
    assert!(matches!(
        f.alice.encrypt_epoch(a.clone(), changed).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), before);
    let d = wire(&plan, 1);
    let (received, ack) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert_eq!(received, p);
    assert_eq!(
        ack.request().messages[0].disposition,
        AckDisposition::Stored as i32
    );
    let a_message = match received.content.as_ref().unwrap() {
        v2::encrypted_payload::Content::Attachment(a) => a,
        _ => panic!("missing attachment"),
    };
    let binding = attachment::AttachmentBinding::new(
        &received.conversation_id,
        received.crypto_epoch.unwrap(),
        &received.message_id,
        &a_message.blob_id,
        received.sender.as_ref().unwrap(),
        received.recipient.as_ref().unwrap(),
    )
    .unwrap();
    let descriptor =
        attachment::AttachmentDescriptor::decode_from_signal(&a_message.private_descriptor)
            .unwrap();
    assert_eq!(
        attachment::open(&binding, &descriptor, file.blob())
            .unwrap()
            .as_slice(),
        b"private file bytes"
    );
    let reply = f
        .bob
        .epoch_outbox_page(None)
        .await
        .unwrap()
        .messages
        .pop()
        .unwrap();
    let reply_exact = reply.request().clone();
    let committed = records(f.root.path(), B);
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert_eq!(records(f.root.path(), B), committed);
    seed(&f.alice).await;
    seed(&f.bob).await;
    let (rows, next) = history(
        f.bob
            .read_views(ViewRequest::History(p.conversation_id.clone(), None))
            .await
            .unwrap(),
    );
    assert!(next.is_none());
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].text.as_str(), "Зашифрованное вложение");
    assert_eq!(rows[0].cache_state, Some(AttachmentCacheState::Missing));
    let view = rows[0].attachment.as_ref().unwrap();
    assert_eq!(view.filename.as_str(), "private-file-marker.bin");
    assert_eq!(view.caption.as_str(), "private attachment caption");
    assert_eq!(view.plaintext_len, 18);
    match f
        .alice
        .read_views(ViewRequest::Statuses(
            p.conversation_id.clone(),
            vec![p.message_id.clone()],
        ))
        .await
        .unwrap()
    {
        ViewPage::Statuses(v) => {
            assert_eq!(v.len(), 1);
            assert!(!v[0].delivery.stored_on_server);
        }
        _ => panic!("missing attachment status"),
    }
    // The control reply is hidden from both normal and segment history. It is
    // Signal confirmation of the descriptor, not evidence of durable file cache.
    let (control, _) = f
        .alice
        .receive_epoch(a.clone(), delivery(&a, &wire(&reply, 2)))
        .await
        .unwrap();
    assert!(matches!(
        control.content,
        Some(v2::encrypted_payload::Content::SessionConfirmation(_))
    ));
    let (archived, _) = history(
        f.alice
            .read_views(ViewRequest::ArchiveHistory(
                a.own.generation_id.clone(),
                p.conversation_id.clone(),
                Some(a.epoch),
                None,
            ))
            .await
            .unwrap(),
    );
    assert_eq!(archived.len(), 1);
    assert!(archived[0].attachment.is_some());
    assert_eq!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    f.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert_eq!(
        bob.epoch_ack(p.message_id.clone()).await.unwrap().request(),
        ack.request()
    );
    assert_eq!(
        bob.epoch_outbox_page(None).await.unwrap().messages[0].request(),
        &reply_exact
    );
    assert_eq!(
        bob.epoch_incoming_message(p.message_id.clone())
            .await
            .unwrap(),
        p
    );
    bob.accept_epoch_ack(accepted(&ack)).await.unwrap();
    assert!(
        bob.epoch_ack(p.message_id.clone())
            .await
            .unwrap()
            .acknowledged()
    );
    let next = f
        .alice
        .encrypt_epoch(
            a.clone(),
            payload(&a, "Signal after attachment confirmation"),
        )
        .await
        .unwrap();
    assert_eq!(
        next.request().envelope.as_ref().unwrap().message_type,
        CiphertextType::Signal as i32
    );
    bob.receive_epoch(b.clone(), delivery(&b, &wire(&next, 3)))
        .await
        .unwrap();
    assert_eq!(bob.epoch_outbox_page(None).await.unwrap().messages.len(), 1);
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_attachment_does_not_consume_ratchet_and_valid_same_uuid_remains_sendable() {
    let (f, a, _) = prepared().await;
    let (p, _) = message(&a);
    let before = records(f.root.path(), A);
    for field in 0..4 {
        let mut invalid = p.clone();
        match field {
            0 => invalid.format_version = 2,
            1 => attachment_mut(&mut invalid).filename = "../outside".into(),
            2 => attachment_mut(&mut invalid).private_descriptor[0] = 9,
            3 => attachment_mut(&mut invalid).mime_type = "text/html".into(),
            _ => unreachable!(),
        }
        assert!(matches!(
            f.alice.encrypt_epoch(a.clone(), invalid).await,
            Err(CryptoError::Invalid)
        ));
        assert_eq!(records(f.root.path(), A), before);
    }
    assert!(f.alice.encrypt_epoch(a, p).await.is_ok());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_attachment_receive_commit_exposes_no_ack_and_retries_exact_ciphertext() {
    let (f, a, b) = prepared().await;
    let (p, _) = message(&a);
    let d = send(&f.alice, &a, p.clone(), 1).await;
    let before = records(f.root.path(), B);
    let db = raw(f.root.path());
    db.execute_batch("CREATE TABLE att_parent(id INTEGER PRIMARY KEY);CREATE TABLE att_child(id INTEGER REFERENCES att_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER att_fault AFTER INSERT ON protected_records WHEN NEW.kind=12 BEGIN INSERT INTO att_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.bob.receive_epoch(b.clone(), delivery(&b, &d)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    assert!(matches!(
        f.bob.epoch_ack(p.message_id.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    assert!(
        f.bob
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    db.execute_batch("DROP TRIGGER att_fault").unwrap();
    drop(db);
    assert_eq!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap()
            .0,
        p
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn closed_peer_dialog_retains_attachment_receive_and_history_without_new_confirmation() {
    let (f, a, b) = prepared().await;
    let (p, _) = message(&a);
    let d = send(&f.alice, &a, p.clone(), 1).await;
    let mut closed = b_conversation(&f.a, &b);
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_generation_id = b.peer.generation_id.clone();
    closed.closed_at_unix_ms = 1_700_000_000_000;
    index(&f.bob, &closed).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert!(
        f.bob
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    seed(&f.bob).await;
    let before = records(f.root.path(), B);
    let (rows, _) = history(
        f.bob
            .read_views(ViewRequest::History(p.conversation_id.clone(), None))
            .await
            .unwrap(),
    );
    assert_eq!(rows.len(), 1);
    assert!(rows[0].attachment.is_some());
    assert_eq!(records(f.root.path(), B), before);
    let (new, _) = message(&b);
    assert!(matches!(
        f.bob.encrypt_epoch(b, new).await,
        Err(CryptoError::ConversationClosed)
    ));
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn mixed_attachment_history_obeys_full_metadata_wire_budget_and_never_skips_rows() {
    let (f, a, _) = prepared().await;
    let mut expected = std::collections::HashSet::new();
    for i in 0..11 {
        let p = if i % 2 == 0 {
            let (mut p, _) = message(&a);
            attachment_mut(&mut p).caption = "\u{1}".repeat(2000);
            attachment_mut(&mut p).filename = "界".repeat(85);
            p
        } else {
            payload(&a, &format!("text-{i}"))
        };
        expected.insert(p.message_id.clone());
        f.alice.encrypt_epoch(a.clone(), p).await.unwrap();
    }
    seed(&f.alice).await;
    let before = records(f.root.path(), A);
    let mut cursor = None;
    let mut found = std::collections::HashSet::new();
    for _ in 0..20 {
        let (rows, next) = history(
            f.alice
                .read_views(ViewRequest::History(a.conversation.clone(), cursor))
                .await
                .unwrap(),
        );
        assert!(!rows.is_empty() && rows.len() <= 8);
        assert!(rows.iter().map(views::item_wire_cost).sum::<usize>() <= 14 * 1024);
        for row in rows {
            assert!(found.insert(row.message_id));
        }
        cursor = next;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert_eq!(found, expected);
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn altered_public_reference_rolls_back_signal_receive_and_returns_no_ack() {
    let (f, a, b) = prepared().await;
    let (p, _file) = message(&a);
    let plan = f.alice.encrypt_epoch(a, p).await.unwrap();
    assert_eq!(
        plan.request().envelope.as_ref().unwrap().protocol_version,
        3
    );
    let original = wire(&plan, 1);
    assert!(
        f.bob
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    let before = records(f.root.path(), B);
    for mutate in [
        (|e: &mut v2::Envelope| e.blob.as_mut().unwrap().sha256[0] ^= 1) as fn(&mut v2::Envelope),
        |e| e.blob.as_mut().unwrap().ciphertext_bytes += 1,
        |e| e.blob.as_mut().unwrap().blob_id = uuid::Uuid::new_v4().to_string(),
        |e| {
            e.protocol_version = 2;
            e.blob = None;
        },
    ] {
        let mut wrong = original.clone();
        let e = wrong.envelope.as_mut().unwrap();
        mutate(e);
        // Simulate authenticated transport carrying an internally consistent
        // outer hash. Signal-authenticated private binding must still reject it.
        wrong.envelope_digest = data::envelope_digest(e).unwrap().to_vec();
        assert!(
            f.bob
                .receive_epoch(b.clone(), delivery(&b, &wrong))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), B), before);
        assert!(
            f.bob
                .epoch_ack_page(None)
                .await
                .unwrap()
                .acknowledgements
                .is_empty()
        );
    }
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &original))
        .await
        .unwrap();
    let page = f.bob.epoch_ack_page(None).await.unwrap();
    assert_eq!(page.acknowledgements.len(), 1);
    let ack = page.acknowledgements[0].request();
    assert_eq!(ack.context.as_ref().unwrap().protocol_version, 2);
    assert_eq!(ack.messages[0].envelope_digest, original.envelope_digest);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
