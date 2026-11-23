#![cfg(feature = "e2ee")]
use libsignal_protocol::IdentityKey;
use prost::Message;
use rust_processor::{
    encryption::{
        contract::MESSAGE_RETENTION_MS, epoch_contract as data, epoch_rpc,
        recovery_contract as recovery,
    },
    proto::{e2ee as common, e2ee_v2 as v2},
};
const ID: &str = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";
fn envelope() -> v2::Envelope {
    v2::Envelope::decode(
        include_bytes!("../../proto/fixtures/attachment-envelope-v3/envelope.bin").as_slice(),
    )
    .unwrap()
}
fn send(e: v2::Envelope) -> v2::SendMessageRequest {
    v2::SendMessageRequest {
        context: Some(common::ClientContext {
            protocol_version: 2,
            device_generation_id: e.sender.as_ref().unwrap().generation_id.clone(),
        }),
        envelope: Some(e),
    }
}
fn receipt(e: &v2::Envelope) -> v2::SendReceipt {
    v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    }
}
#[test]
fn shared_attachment3_frame_hash_epoch_and_protobuf_order_match_go() {
    let e = envelope();
    let wire = e.encode_to_vec();
    assert_eq!(
        wire,
        include_bytes!("../../proto/fixtures/attachment-envelope-v3/envelope.bin")
    );
    let expected = include_bytes!("../../proto/fixtures/attachment-envelope-v3/envelope.frame");
    assert_eq!(expected.len(), 354);
    assert_eq!(data::envelope_frame(&e).unwrap(), expected);
    let hex: String = data::envelope_digest(&e)
        .unwrap()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        hex,
        "c8cd0ccf0cb367bcaf43789aaa96decad7741052b90c93c340c703f5f633493f"
    );
    assert_eq!(e.protocol_version, 3);
    assert_eq!(e.crypto_epoch, Some((1 << 53) + 17));
    assert_eq!(e.blob.as_ref().unwrap().ciphertext_bytes, 37);
    data::validate_send(&send(e.clone())).unwrap();
    data::validate_envelope_at_epoch(&e, e.crypto_epoch.unwrap()).unwrap();
    assert!(data::validate_envelope_at_epoch(&e, e.crypto_epoch.unwrap() + 1).is_err());
    assert_eq!(&wire[..2], &[8, 3]);
    let reordered = [&wire[2..], &wire[..2]].concat();
    let decoded = data::decode_envelope(&reordered).unwrap();
    assert_eq!(
        data::envelope_digest(&decoded).unwrap(),
        data::envelope_digest(&e).unwrap()
    );
    let binding = data::send_binding(&e).unwrap();
    assert_eq!(binding.protocol_version, 3);
    assert_eq!(binding.envelope_digest, data::envelope_digest(&e).unwrap());
    let receipt = receipt(&e);
    let own = e.recipient.clone().unwrap();
    epoch_rpc::validate_pending(
        &v2::PendingMessages {
            messages: vec![v2::Delivery {
                sequence: 1,
                envelope: Some(e),
                expires_at_unix_ms: receipt.expires_at_unix_ms,
                envelope_digest: receipt.envelope_digest,
            }],
        },
        &own,
    )
    .unwrap();
}
#[test]
fn stable_public_fields_bind_exact_receipt_and_digest() {
    let e = envelope();
    let digest = data::envelope_digest(&e).unwrap();
    let receipt = receipt(&e);
    for mutate in [
        (|e: &mut v2::Envelope| e.message_id = ID.into()) as fn(&mut v2::Envelope),
        |e| e.conversation_id = ID.into(),
        |e| e.crypto_epoch = Some(0),
        |e| e.sender.as_mut().unwrap().user_id = ID.into(),
        |e| e.sender.as_mut().unwrap().generation_id = ID.into(),
        |e| e.recipient.as_mut().unwrap().user_id = ID.into(),
        |e| e.recipient.as_mut().unwrap().generation_id = ID.into(),
        |e| e.message_type = common::CiphertextType::Signal as i32,
        |e| e.ciphertext.push(4),
        |e| e.blob.as_mut().unwrap().blob_id = ID.into(),
        |e| e.blob.as_mut().unwrap().ciphertext_bytes += 1,
        |e| e.blob.as_mut().unwrap().sha256[0] ^= 1,
    ] {
        let mut changed = e.clone();
        mutate(&mut changed);
        assert_ne!(data::envelope_digest(&changed).unwrap(), digest);
        assert!(data::validate_receipt(&send(changed), &receipt).is_err());
    }
}
#[test]
fn malformed_or_cross_version_public_references_fail_closed() {
    let e = envelope();
    for mutate in [
        (|e: &mut v2::Envelope| e.blob = None) as fn(&mut v2::Envelope),
        |e| e.protocol_version = 2,
        |e| e.protocol_version = 1,
        |e| e.protocol_version = 4,
        |e| e.crypto_epoch = None,
        |e| e.crypto_epoch = Some(u64::MAX),
        |e| e.blob.as_mut().unwrap().format_version = 0,
        |e| e.blob.as_mut().unwrap().format_version = 2,
        |e| e.blob.as_mut().unwrap().blob_id = "https://example.test/blob".into(),
        |e| e.blob.as_mut().unwrap().blob_id = "CCCCCCCC-CCCC-4CCC-8CCC-CCCCCCCCCCCC".into(),
        |e| e.blob.as_mut().unwrap().blob_id = "cccccccc-cccc-1ccc-8ccc-cccccccccccc".into(),
        |e| e.blob.as_mut().unwrap().ciphertext_bytes = 0,
        |e| e.blob.as_mut().unwrap().ciphertext_bytes = 36,
        |e| e.blob.as_mut().unwrap().ciphertext_bytes = 52_428_837,
        |e| e.blob.as_mut().unwrap().ciphertext_bytes = u64::MAX,
        |e| {
            e.blob.as_mut().unwrap().sha256.pop();
        },
        |e| e.blob.as_mut().unwrap().sha256.push(0),
    ] {
        let mut changed = e.clone();
        mutate(&mut changed);
        assert!(data::validate_envelope(&changed).is_err());
        assert!(data::envelope_frame(&changed).is_err());
    }
    let mut maximum = e.clone();
    maximum.blob.as_mut().unwrap().ciphertext_bytes = 52_428_836;
    data::validate_envelope(&maximum).unwrap();
    let mut text = e;
    text.protocol_version = 2;
    text.blob = None;
    data::validate_envelope(&text).unwrap();
}
#[test]
fn prost_unknown_fields_cannot_enable_future_critical_reference_format() {
    let r = envelope().blob.unwrap();
    let mut bytes = r.encode_to_vec();
    bytes.extend([0x78, 1]);
    let mut decoded = v2::BlobRef::decode(bytes.as_slice()).unwrap();
    assert_eq!(decoded, r); // prost drops unknown fields; Go rejects them.
    data::validate_blob_ref(&decoded).unwrap();
    decoded.format_version = 2;
    assert!(data::validate_blob_ref(&decoded).is_err()); // Critical meaning must bump format.
}
fn private_fixture() -> (v2::EncryptedPayload, v2::Envelope) {
    let p = v2::EncryptedPayload::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/attachment.bin").as_slice(),
    )
    .unwrap();
    let mut e = v2::Envelope::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/envelope.bin").as_slice(),
    )
    .unwrap();
    let Some(v2::encrypted_payload::Content::Attachment(a)) = &p.content else {
        panic!("wrong fixture")
    };
    let d = &a.private_descriptor;
    e.protocol_version = 3;
    e.blob = Some(v2::BlobRef {
        format_version: 1,
        blob_id: a.blob_id.clone(),
        ciphertext_bytes: u64::from_be_bytes(d[65..].try_into().unwrap()) + 36,
        sha256: d[33..65].to_vec(),
    });
    (p, e)
}
#[test]
fn private_descriptor_and_content_kind_must_match_public_reference() {
    let (p, e) = private_fixture();
    data::validate_payload_binding(&p, &e).unwrap();
    for mutate in [
        (|a: &mut v2::AttachmentMessage| a.blob_id = ID.into()) as fn(&mut v2::AttachmentMessage),
        |a| a.private_descriptor[33] ^= 1,
        |a| a.private_descriptor[65..].copy_from_slice(&43u64.to_be_bytes()),
    ] {
        let mut changed = p.clone();
        let Some(v2::encrypted_payload::Content::Attachment(a)) = changed.content.as_mut() else {
            unreachable!()
        };
        mutate(a);
        assert!(data::validate_payload_binding(&changed, &e).is_err());
    }
    let mut text = p.clone();
    text.format_version = 2;
    text.content = Some(v2::encrypted_payload::Content::Text(common::TextMessage {
        text: "text".into(),
    }));
    assert!(data::validate_payload_binding(&text, &e).is_err());
    let mut ordinary = e;
    ordinary.protocol_version = 2;
    ordinary.blob = None;
    assert!(data::validate_payload_binding(&p, &ordinary).is_err());
    data::validate_payload_binding(&text, &ordinary).unwrap();
    text.content = Some(v2::encrypted_payload::Content::SessionConfirmation(
        v2::SessionConfirmation {
            in_reply_to_message_id: ID.into(),
            crypto_epoch: text.crypto_epoch,
        },
    ));
    data::validate_payload_binding(&text, &ordinary).unwrap();
    ordinary.protocol_version = 3;
    ordinary.blob = private_fixture().1.blob;
    assert!(data::validate_payload_binding(&text, &ordinary).is_err());
}
#[test]
fn attachment_digest_binds_real_signed_control2_fence_without_widening_control_protocol() {
    // Public signed recovery fixtures use the retained route/epoch from envelope2.
    let mut e = v2::Envelope::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/envelope.bin").as_slice(),
    )
    .unwrap();
    e.protocol_version = 3;
    e.blob = envelope().blob;
    data::validate_receipt(&send(e.clone()), &receipt(&e)).unwrap();
    let binding = data::send_binding(&e).unwrap();
    let o = common::RecoveryOutcome::decode(
        include_bytes!("../../proto/fixtures/epoch-recovery-v1/accept.bin").as_slice(),
    )
    .unwrap();
    let a = IdentityKey::decode(include_bytes!(
        "../../proto/fixtures/epoch-recovery-v1/initiator.pub"
    ))
    .unwrap();
    let b = IdentityKey::decode(include_bytes!(
        "../../proto/fixtures/epoch-recovery-v1/peer.pub"
    ))
    .unwrap();
    let verified = recovery::verify_outcome(&o, o.proposal.as_ref().unwrap(), &a, &b).unwrap();
    let fence = common::EpochSealedFence {
        binding: Some(binding.clone()),
        proposal_digest: recovery::control_digest(
            o.proposal.as_ref().unwrap().control.as_ref().unwrap(),
        )
        .unwrap()
        .to_vec(),
        next_epoch: o.new_epoch,
        epoch_revision: o.epoch_revision,
    };
    recovery::validate_fence(&fence, &binding, &verified).unwrap();
    let mut wrong = binding.clone();
    wrong.protocol_version = 2;
    assert!(recovery::validate_fence(&fence, &wrong, &verified).is_err());
    e.blob.as_mut().unwrap().sha256[0] ^= 1;
    wrong = data::send_binding(&e).unwrap();
    assert!(recovery::validate_fence(&fence, &wrong, &verified).is_err());
    assert!(recovery::validate_epoch_version_binding(3, 0, 0).is_err());
    let mut control = o.proposal.as_ref().unwrap().control.clone().unwrap();
    control.protocol_version = 3;
    assert!(recovery::validate_control(&control).is_err());
    let mut req = send(e);
    req.context.as_mut().unwrap().protocol_version = 3;
    assert!(data::validate_send(&req).is_err());
    let mut acquire = v2::AcquireBundleRequest::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/acquire.bin").as_slice(),
    )
    .unwrap();
    acquire.context.as_mut().unwrap().protocol_version = 3;
    assert!(data::validate_acquisition(&acquire).is_err());
}

#[test]
fn attachment_status_request_preserves_context2_and_exact_reference_digest() {
    let r = send(envelope());
    let expected = receipt(r.envelope.as_ref().unwrap());
    let request =
        epoch_rpc::status_request(r.context.clone().unwrap(), std::slice::from_ref(&r)).unwrap();
    assert_eq!(request.context.as_ref().unwrap().protocol_version, 2);
    assert_eq!(
        request.messages[0].envelope_digest,
        expected.envelope_digest
    );
    let statuses = v2::MessageStatuses {
        messages: vec![v2::MessageStatus {
            observed_at_unix_ms: expected.stored_at_unix_ms,
            receipt: Some(expected),
            state: common::MessageState::Pending as i32,
        }],
    };
    epoch_rpc::validate_statuses(std::slice::from_ref(&r), &statuses).unwrap();
    let mut changed = r;
    changed
        .envelope
        .as_mut()
        .unwrap()
        .blob
        .as_mut()
        .unwrap()
        .sha256[0] ^= 1;
    assert!(epoch_rpc::validate_statuses(&[changed], &statuses).is_err());
}
