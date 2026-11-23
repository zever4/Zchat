#![cfg(feature = "e2ee")]
use libsignal_protocol::IdentityKey;
use prost::Message;
use rust_processor::{
    encryption::{epoch_contract as v2, recovery_contract as recovery},
    proto::{e2ee as common, e2ee_v2 as wire},
};
const ENVELOPE: &[u8] = include_bytes!("../../proto/fixtures/epoch-data-v2/envelope.bin");
const INITIAL: &[u8] = include_bytes!("../../proto/fixtures/epoch-data-v2/initial.bin");
fn envelope() -> wire::Envelope {
    wire::Envelope::decode(ENVELOPE).unwrap()
}
fn payload() -> wire::EncryptedPayload {
    wire::EncryptedPayload::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/payload.bin").as_slice(),
    )
    .unwrap()
}
fn send(e: wire::Envelope) -> wire::SendMessageRequest {
    wire::SendMessageRequest {
        context: Some(common::ClientContext {
            protocol_version: 2,
            device_generation_id: e.sender.as_ref().unwrap().generation_id.clone(),
        }),
        envelope: Some(e),
    }
}
fn bundle() -> (wire::AcquireBundleRequest, wire::AcquiredBundle) {
    (
        wire::AcquireBundleRequest::decode(
            include_bytes!("../../proto/fixtures/epoch-data-v2/acquire.bin").as_slice(),
        )
        .unwrap(),
        wire::AcquiredBundle::decode(
            include_bytes!("../../proto/fixtures/epoch-data-v2/bundle.bin").as_slice(),
        )
        .unwrap(),
    )
}

#[test]
fn shared_v2_vectors_match_go_frames_hashes_presence_and_full_width_epochs() {
    let hashes = include_str!("../../proto/fixtures/epoch-data-v2/frames.sha256");
    for (name, bytes, frame) in [
        (
            "initial",
            INITIAL,
            include_bytes!("../../proto/fixtures/epoch-data-v2/initial.frame").as_slice(),
        ),
        (
            "envelope",
            ENVELOPE,
            include_bytes!("../../proto/fixtures/epoch-data-v2/envelope.frame").as_slice(),
        ),
    ] {
        let e = v2::decode_envelope(bytes).unwrap();
        v2::validate_envelope_at_epoch(&e, e.crypto_epoch.unwrap()).unwrap();
        assert!(v2::validate_envelope_at_epoch(&e, e.crypto_epoch.unwrap() + 1).is_err());
        assert_eq!(
            e.crypto_epoch,
            Some(if name == "initial" {
                0
            } else {
                (1u64 << 53) + 17
            })
        );
        assert_eq!(v2::envelope_frame(&e).unwrap(), frame);
        let hash: String = v2::envelope_digest(&e)
            .unwrap()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        assert!(hashes.lines().any(|l| l == format!("{hash}  {name}.frame")));
        assert_eq!(&bytes[..2], &[8, 2]);
        let reordered = [&bytes[2..], &bytes[..2]].concat();
        assert_eq!(
            v2::envelope_digest(&v2::decode_envelope(&reordered).unwrap()).unwrap(),
            v2::envelope_digest(&e).unwrap()
        );
        assert_eq!(e.encode_to_vec(), bytes);
        v2::validate_send(&send(e)).unwrap();
    }
    let e = envelope();
    let digest = v2::envelope_digest(&e).unwrap();
    macro_rules! bound {
        ($e:ident,$body:block) => {{
            let mut $e = e.clone();
            $body;
            assert_ne!(v2::envelope_digest(&$e).unwrap(), digest);
        }};
    }
    bound!(c, {
        c.crypto_epoch = Some(c.crypto_epoch.unwrap() + 1);
    });
    bound!(c, {
        c.message_id = "77777777-7777-4777-8777-777777777777".into();
    });
    bound!(c, {
        c.conversation_id = c.message_id.clone();
    });
    bound!(c, {
        c.sender.as_mut().unwrap().user_id = c.message_id.clone();
    });
    bound!(c, {
        c.sender.as_mut().unwrap().generation_id = c.message_id.clone();
    });
    bound!(c, {
        c.recipient.as_mut().unwrap().user_id = c.message_id.clone();
    });
    bound!(c, {
        c.recipient.as_mut().unwrap().generation_id = c.message_id.clone();
    });
    bound!(c, {
        c.message_type = common::CiphertextType::Signal as i32;
    });
    bound!(c, {
        c.ciphertext.push(4);
    });
}

#[test]
fn explicit_initial_epoch_zero_binds_payload_bundle_and_receipt() {
    let e = v2::decode_envelope(INITIAL).unwrap();
    let mut p = payload();
    p.crypto_epoch = Some(0);
    v2::validate_payload_binding(&p, &e).unwrap();
    let (r, mut b) = bundle();
    let r = wire::AcquireBundleRequest {
        crypto_epoch: Some(0),
        ..r
    };
    b.crypto_epoch = Some(0);
    let pin = IdentityKey::decode(&b.device.as_ref().unwrap().identity_key).unwrap();
    v2::verify_pinned_bundle(&r, &b, &pin, 16383).unwrap();
    let mut s = wire::SendReceipt::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/receipt.bin").as_slice(),
    )
    .unwrap();
    s.crypto_epoch = Some(0);
    s.envelope_digest = v2::envelope_digest(&e).unwrap().to_vec();
    v2::validate_receipt(&send(e), &s).unwrap();
    assert_eq!(
        wire::EncryptedPayload::decode(p.encode_to_vec().as_slice())
            .unwrap()
            .crypto_epoch,
        Some(0)
    );
    assert_eq!(
        wire::AcquireBundleRequest::decode(r.encode_to_vec().as_slice())
            .unwrap()
            .crypto_epoch,
        Some(0)
    );
    assert_eq!(
        wire::AcquiredBundle::decode(b.encode_to_vec().as_slice())
            .unwrap()
            .crypto_epoch,
        Some(0)
    );
    assert_eq!(
        wire::SendReceipt::decode(s.encode_to_vec().as_slice())
            .unwrap()
            .crypto_epoch,
        Some(0)
    );
}

#[test]
fn v2_rejects_legacy_absent_epoch_bad_versions_and_raw_bounds() {
    let e = envelope();
    macro_rules! reject {
        ($e:ident,$body:block) => {{
            let mut $e = e.clone();
            $body;
            assert!(v2::envelope_digest(&$e).is_err());
        }};
    }
    reject!(c, {
        c.crypto_epoch = None;
    });
    reject!(c, {
        c.crypto_epoch = Some(u64::MAX);
    });
    reject!(c, {
        c.protocol_version = 1;
    });
    reject!(c, {
        c.protocol_version = 3;
    });
    reject!(c, {
        c.sender = None;
    });
    reject!(c, {
        c.sender.as_mut().unwrap().device_id = 2;
    });
    reject!(c, {
        c.recipient.as_mut().unwrap().user_id = c.sender.as_ref().unwrap().user_id.clone();
    });
    reject!(c, {
        c.recipient.as_mut().unwrap().generation_id =
            c.sender.as_ref().unwrap().generation_id.clone();
    });
    reject!(c, {
        c.message_id = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA".into();
    });
    reject!(c, {
        c.crypto_suite = 99;
    });
    reject!(c, {
        c.message_type = 99;
    });
    reject!(c, {
        c.ciphertext.clear();
    });
    reject!(c, {
        c.ciphertext = vec![0; 48 << 10];
    });
    for bytes in [vec![0x80], vec![0; (48 << 10) + 1], vec![]] {
        assert!(v2::decode_envelope(&bytes).is_err());
    }
    let old = include_bytes!("../../proto/fixtures/e2ee-v1-envelope.bin");
    assert!(v2::decode_envelope(old).is_err());
    let mut relabelled = common::Envelope::decode(old.as_slice()).unwrap();
    relabelled.protocol_version = 2;
    assert!(v2::decode_envelope(&relabelled.encode_to_vec()).is_err());
    let mut maximum = e.clone();
    maximum.crypto_epoch = Some(i64::MAX as u64);
    v2::validate_envelope(&maximum).unwrap();
    let mut r = send(e);
    r.context.as_mut().unwrap().protocol_version = 1;
    assert!(v2::validate_send(&r).is_err());
    r.context.as_mut().unwrap().protocol_version = 2;
    r.context.as_mut().unwrap().device_generation_id =
        r.envelope.as_ref().unwrap().message_id.clone();
    assert!(v2::validate_send(&r).is_err());
    // Prost discards unknown fields. They never enable legacy parsing/critical features.
    let mut raw = ENVELOPE.to_vec();
    raw.extend([0xf8, 0x03, 1]);
    assert_eq!(v2::decode_envelope(&raw).unwrap(), envelope());
}

#[test]
fn authenticated_payload_and_confirmation_cannot_cross_epochs() {
    let e = envelope();
    let p = payload();
    v2::validate_payload_binding(&p, &e).unwrap();
    macro_rules! reject {
        ($p:ident,$body:block) => {{
            let mut $p = p.clone();
            $body;
            assert!(v2::validate_payload_binding(&$p, &e).is_err());
        }};
    }
    reject!(c, {
        c.format_version = 1;
    });
    reject!(c, {
        c.crypto_epoch = None;
    });
    reject!(c, {
        c.crypto_epoch = Some(c.crypto_epoch.unwrap() + 1);
    });
    reject!(c, {
        c.message_id = c.conversation_id.clone();
    });
    reject!(c, {
        c.conversation_id = c.message_id.clone();
    });
    reject!(c, {
        c.sender.as_mut().unwrap().generation_id = c.message_id.clone();
    });
    reject!(c, {
        c.recipient.as_mut().unwrap().generation_id = c.message_id.clone();
    });
    reject!(c, {
        c.created_at_unix_ms = -1;
    });
    reject!(c, {
        c.content = None;
    });
    reject!(c, {
        c.content = Some(wire::encrypted_payload::Content::Text(
            common::TextMessage {
                text: String::new(),
            },
        ));
    });
    reject!(c, {
        c.content = Some(wire::encrypted_payload::Content::Text(
            common::TextMessage {
                text: "x".repeat(2001),
            },
        ));
    });
    let c = wire::EncryptedPayload::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/confirmation.bin").as_slice(),
    )
    .unwrap();
    v2::validate_payload_binding(&c, &e).unwrap();
    let Some(wire::encrypted_payload::Content::SessionConfirmation(original)) = &c.content else {
        panic!("wrong fixture");
    };
    for bad in [
        wire::SessionConfirmation {
            crypto_epoch: None,
            ..original.clone()
        },
        wire::SessionConfirmation {
            crypto_epoch: Some(e.crypto_epoch.unwrap() + 1),
            ..original.clone()
        },
        wire::SessionConfirmation {
            in_reply_to_message_id: e.message_id.clone(),
            ..original.clone()
        },
        wire::SessionConfirmation {
            in_reply_to_message_id: "".into(),
            ..original.clone()
        },
    ] {
        let mut c = c.clone();
        c.content = Some(wire::encrypted_payload::Content::SessionConfirmation(bad));
        assert!(v2::validate_payload_binding(&c, &e).is_err());
    }
    // Relabelling an actual v1-format payload cannot satisfy the v2 boundary.
    let old = common::EncryptedPayload {
        format_version: 1,
        message_id: e.message_id.clone(),
        conversation_id: e.conversation_id.clone(),
        sender: e.sender.clone(),
        recipient: e.recipient.clone(),
        created_at_unix_ms: 1,
        content: Some(common::encrypted_payload::Content::Text(
            common::TextMessage { text: "old".into() },
        )),
    };
    let decoded = wire::EncryptedPayload::decode(old.encode_to_vec().as_slice()).unwrap();
    assert!(v2::validate_payload_binding(&decoded, &e).is_err());
}

#[test]
fn receipts_keep_exact_epoch_digest_and_original_commit_ttl() {
    let r = send(envelope());
    let s = wire::SendReceipt::decode(
        include_bytes!("../../proto/fixtures/epoch-data-v2/receipt.bin").as_slice(),
    )
    .unwrap();
    v2::validate_receipt(&r, &s).unwrap();
    macro_rules! reject {
        ($s:ident,$body:block) => {{
            let mut $s = s.clone();
            $body;
            assert!(v2::validate_receipt(&r, &$s).is_err());
        }};
    }
    reject!(c, {
        c.crypto_epoch = None;
    });
    reject!(c, {
        c.crypto_epoch = Some(c.crypto_epoch.unwrap() + 1);
    });
    reject!(c, {
        c.message_id = r.envelope.as_ref().unwrap().conversation_id.clone();
    });
    reject!(c, {
        c.envelope_digest[0] ^= 1;
    });
    reject!(c, {
        c.expires_at_unix_ms += 1;
    });
    reject!(c, {
        c.stored_at_unix_ms = i64::MAX;
    });
    reject!(c, {
        c.stored_at_unix_ms = 0;
    });
    // No fresh deadline relative to the client clock: exact old receipt stays valid.
    v2::validate_receipt(&r, &s).unwrap();
}

#[test]
fn acquisition_replay_is_bound_to_epoch_peer_and_request() {
    let (r, b) = bundle();
    v2::validate_acquired_bundle(&r, &b).unwrap();
    macro_rules! reject {
        ($b:ident,$body:block) => {{
            let mut $b = b.clone();
            $body;
            assert!(v2::validate_acquired_bundle(&r, &$b).is_err());
        }};
    }
    reject!(c, {
        c.crypto_epoch = None;
    });
    reject!(c, {
        c.crypto_epoch = Some(c.crypto_epoch.unwrap() + 1);
    });
    reject!(c, {
        c.protocol_version = 1;
    });
    reject!(c, {
        c.acquisition_id = c.conversation_id.clone();
    });
    reject!(c, {
        c.conversation_id = c.acquisition_id.clone();
    });
    reject!(c, {
        c.device
            .as_mut()
            .unwrap()
            .reference
            .as_mut()
            .unwrap()
            .generation_id = c.acquisition_id.clone();
    });
    reject!(c, {
        c.device.as_mut().unwrap().registration_id = 16384;
    });
    reject!(c, {
        c.device.as_mut().unwrap().public_material_revision = 0;
    });
    reject!(c, {
        c.device.as_mut().unwrap().public_material_ready = false;
    });
    reject!(c, {
        c.device.as_mut().unwrap().identity_key[0] = 0;
    });
    reject!(c, {
        c.signed_pre_key = None;
    });
    reject!(c, {
        c.kyber_pre_key = None;
    });
    reject!(c, {
        c.signed_pre_key.as_mut().unwrap().signature.clear();
    });
    reject!(c, {
        c.kyber_pre_key.as_mut().unwrap().public_key[0] = 0;
    });
    reject!(c, {
        c.one_time_pre_key.as_mut().unwrap().public_key.clear();
    });
    reject!(c, {
        c.not_after_unix_ms = 0;
    });
    let mut req = r.clone();
    req.crypto_epoch = None;
    assert!(v2::validate_acquisition(&req).is_err());
    req = r.clone();
    req.crypto_epoch = Some(u64::MAX);
    assert!(v2::validate_acquisition(&req).is_err());
    req = r.clone();
    req.context.as_mut().unwrap().protocol_version = 1;
    assert!(v2::validate_acquisition(&req).is_err());
    req = r.clone();
    req.expected_peer.as_mut().unwrap().device_id = 2;
    assert!(v2::validate_acquisition(&req).is_err());
    req = r.clone();
    req.expected_peer.as_mut().unwrap().generation_id =
        req.context.as_ref().unwrap().device_generation_id.clone();
    assert!(v2::validate_acquisition(&req).is_err());
}

#[test]
fn pinned_bundle_verifies_real_libsignal_signatures_and_optional_ec_last_resort() {
    let (r, b) = bundle();
    let d = b.device.as_ref().unwrap();
    let pin = IdentityKey::decode(&d.identity_key).unwrap();
    v2::verify_pinned_bundle(&r, &b, &pin, 16383).unwrap();
    assert!(v2::verify_pinned_bundle(&r, &b, &pin, 42).is_err());
    let wrong = IdentityKey::decode(include_bytes!(
        "../../proto/fixtures/epoch-recovery-v1/peer.pub"
    ))
    .unwrap();
    assert!(v2::verify_pinned_bundle(&r, &b, &wrong, 16383).is_err());
    let mut changed = b.clone();
    changed.signed_pre_key.as_mut().unwrap().signature[0] ^= 1;
    assert!(v2::verify_pinned_bundle(&r, &changed, &pin, 16383).is_err());
    changed = b.clone();
    changed.kyber_pre_key.as_mut().unwrap().signature[0] ^= 1;
    assert!(v2::verify_pinned_bundle(&r, &changed, &pin, 16383).is_err());
    changed = b.clone();
    changed.signed_pre_key.as_mut().unwrap().public_key[1] ^= 1;
    assert!(v2::verify_pinned_bundle(&r, &changed, &pin, 16383).is_err());
    changed = b.clone();
    changed.kyber_pre_key.as_mut().unwrap().public_key[1] ^= 1;
    assert!(v2::verify_pinned_bundle(&r, &changed, &pin, 16383).is_err());
    let keys = common::UploadPreKeysRequest::decode(
        include_bytes!("../../proto/fixtures/e2ee-v1-publication.bin").as_slice(),
    )
    .unwrap();
    let mut optional = b;
    optional.one_time_pre_key = None;
    optional.kyber_pre_key = Some(keys.kyber_pre_keys[1].clone());
    v2::verify_pinned_bundle(&r, &optional, &pin, 16383).unwrap();
}

#[test]
fn real_v2_digest_binds_the_signed_recovery_fence_without_legacy_hash() {
    let e = envelope();
    let binding = v2::send_binding(&e).unwrap();
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
    let digest =
        recovery::control_digest(o.proposal.as_ref().unwrap().control.as_ref().unwrap()).unwrap();
    let mut f = common::EpochSealedFence {
        binding: Some(binding.clone()),
        proposal_digest: digest.to_vec(),
        next_epoch: o.new_epoch,
        epoch_revision: o.epoch_revision,
    };
    recovery::validate_fence(&f, &binding, &verified).unwrap();
    let old = common::Envelope::decode(
        include_bytes!("../../proto/fixtures/e2ee-v1-envelope.bin").as_slice(),
    )
    .unwrap();
    f.binding.as_mut().unwrap().envelope_digest =
        rust_processor::encryption::contract::envelope_digest(&old)
            .unwrap()
            .to_vec();
    assert!(recovery::validate_fence(&f, &binding, &verified).is_err());
}
