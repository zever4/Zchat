#![cfg(feature = "e2ee")]
use libsignal_protocol::{IdentityKey, IdentityKeyPair};
use prost::Message;
use rust_processor::{
    encryption::{contract::validate_envelope, recovery_contract::*},
    proto::e2ee::{
        Envelope, EpochSealedFence, RecoveryAction, RecoveryControl, RecoveryOutcome, RecoveryState,
    },
};
use sha2::{Digest, Sha256};

const ACCEPT: &[u8] = include_bytes!("../../proto/fixtures/epoch-recovery-v1/accept.bin");
const DECLINE: &[u8] = include_bytes!("../../proto/fixtures/epoch-recovery-v1/decline.bin");
const CANCEL: &[u8] = include_bytes!("../../proto/fixtures/epoch-recovery-v1/cancel.bin");
const A: &[u8] = include_bytes!("../../proto/fixtures/epoch-recovery-v1/initiator.pub");
const B: &[u8] = include_bytes!("../../proto/fixtures/epoch-recovery-v1/peer.pub");

fn outcome() -> RecoveryOutcome {
    RecoveryOutcome::decode(ACCEPT).unwrap()
}
fn pins() -> (IdentityKey, IdentityKey) {
    (
        IdentityKey::decode(A).unwrap(),
        IdentityKey::decode(B).unwrap(),
    )
}

#[test]
fn public_vectors_match_go_frames_full_width_digests_and_signal_signatures() {
    let (a, b) = pins();
    let hashes = include_str!("../../proto/fixtures/epoch-recovery-v1/frames.sha256");
    for (wire, name, frame) in [
        (
            ACCEPT,
            "accept.frame",
            include_bytes!("../../proto/fixtures/epoch-recovery-v1/accept.frame").as_slice(),
        ),
        (
            DECLINE,
            "decline.frame",
            include_bytes!("../../proto/fixtures/epoch-recovery-v1/decline.frame").as_slice(),
        ),
        (
            CANCEL,
            "cancel.frame",
            include_bytes!("../../proto/fixtures/epoch-recovery-v1/cancel.frame").as_slice(),
        ),
    ] {
        let o = RecoveryOutcome::decode(wire).unwrap();
        let p = o.proposal.as_ref().unwrap();
        verify_outcome(&o, p, &a, &b).unwrap();
        let c = p.control.as_ref().unwrap();
        assert_eq!(c.expected_epoch, (1u64 << 53) + 17);
        assert_eq!(c.expected_revision, (1u64 << 53) + 29);
        for (name, control, expected_frame) in [
            (
                name,
                o.decision.as_ref().unwrap().control.as_ref().unwrap(),
                frame,
            ),
            (
                "propose.frame",
                c,
                include_bytes!("../../proto/fixtures/epoch-recovery-v1/propose.frame").as_slice(),
            ),
        ] {
            assert_eq!(control_frame(control).unwrap(), expected_frame);
            let digest: String = control_digest(control)
                .unwrap()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert!(
                hashes
                    .lines()
                    .any(|line| line == format!("{digest}  {name}"))
            );
            let wire = control.encode_to_vec();
            assert_eq!(&wire[..2], &[8, 2]);
            let reordered = [&wire[2..], &wire[..2]].concat();
            let decoded = RecoveryControl::decode(reordered.as_slice()).unwrap();
            assert_eq!(
                control_digest(control).unwrap(),
                control_digest(&decoded).unwrap()
            );
        }
        // Exact public wire/signature roundtrip is suitable for persistence/retry;
        // this is not a test of the not-yet-implemented durable recovery journal.
        assert_eq!(o.encode_to_vec(), wire);
    }
}

#[test]
fn rejects_signature_bit_flips_wrong_identity_and_changed_signed_context() {
    let o = outcome();
    let p = o.proposal.as_ref().unwrap();
    let (a, b) = pins();
    assert!(verify_control(p, &b).is_err());
    for i in 0..64 {
        let mut changed = p.clone();
        changed.signature[i] ^= 1;
        assert!(verify_control(&changed, &a).is_err());
    }
    macro_rules! reject {
        ($field:ident, $body:block) => {{
            let mut changed = p.clone();
            let $field = changed.control.as_mut().unwrap();
            $body;
            assert!(verify_control(&changed, &a).is_err());
        }};
    }
    reject!(c, {
        c.expected_epoch += 1;
    });
    reject!(c, {
        c.expected_revision += 1;
    });
    reject!(c, {
        c.conversation_id = c.request_id.clone();
    });
    reject!(c, {
        c.request_id = c.conversation_id.clone();
        c.proposal_id = c.request_id.clone();
    });
    reject!(c, {
        c.peer.as_mut().unwrap().identity_fingerprint[0] ^= 1;
    });
    reject!(c, {
        c.peer
            .as_mut()
            .unwrap()
            .device
            .as_mut()
            .unwrap()
            .generation_id = c.request_id.clone();
    });
    reject!(c, {
        c.peer.as_mut().unwrap().device.as_mut().unwrap().user_id = c.request_id.clone();
    });
    reject!(c, {
        c.initiator
            .as_mut()
            .unwrap()
            .device
            .as_mut()
            .unwrap()
            .user_id = c.request_id.clone();
    });
    reject!(c, {
        c.initiator
            .as_mut()
            .unwrap()
            .device
            .as_mut()
            .unwrap()
            .generation_id = c.request_id.clone();
    });
    let d = o.decision.as_ref().unwrap();
    assert!(verify_control(d, &a).is_err());
    let mut changed = d.clone();
    changed.control.as_mut().unwrap().action = RecoveryAction::Cancel as i32;
    assert!(verify_control(&changed, &a).is_err()); // Role/action is signed too.
}

#[test]
fn native_signing_uses_pinned_actor_and_signs_frame_not_digest() {
    let mut rng = rand::rng();
    let key = IdentityKeyPair::generate(&mut rng);
    let other = IdentityKeyPair::generate(&mut rng);
    let mut c = outcome().proposal.unwrap().control.unwrap();
    c.initiator.as_mut().unwrap().identity_fingerprint =
        identity_fingerprint(key.identity_key()).to_vec();
    c.peer.as_mut().unwrap().identity_fingerprint =
        identity_fingerprint(other.identity_key()).to_vec();
    let signed = sign_control(c.clone(), &key).unwrap();
    verify_control(&signed, key.identity_key()).unwrap();
    assert!(sign_control(c.clone(), &other).is_err());
    let mut wrong = signed.clone();
    wrong.signature = key
        .private_key()
        .calculate_signature(&control_digest(&c).unwrap(), &mut rng)
        .unwrap()
        .to_vec();
    assert!(verify_control(&wrong, key.identity_key()).is_err());
    let mut frame = control_frame(&c).unwrap();
    frame[0] ^= 1;
    wrong.signature = key
        .private_key()
        .calculate_signature(&frame, &mut rng)
        .unwrap()
        .to_vec();
    assert!(verify_control(&wrong, key.identity_key()).is_err());
    c.action = RecoveryAction::Accept as i32;
    c.proposal_digest = control_digest(signed.control.as_ref().unwrap())
        .unwrap()
        .to_vec();
    c.request_id = "88888888-8888-4888-8888-888888888888".into();
    assert!(sign_control(c.clone(), &key).is_err());
    verify_control(&sign_control(c, &other).unwrap(), other.identity_key()).unwrap();
}

#[test]
fn malformed_control_versions_ranges_and_actions_fail_closed() {
    let original = outcome().proposal.unwrap().control.unwrap();
    macro_rules! reject {
        ($c:ident, $body:block) => {{
            let mut $c = original.clone();
            $body;
            assert!(validate_control(&$c).is_err());
        }};
    }
    reject!(c, {
        c.protocol_version = 1;
    });
    reject!(c, {
        c.protocol_version = 3;
    });
    reject!(c, {
        c.format_version = 2;
    });
    reject!(c, {
        c.crypto_suite = 99;
    });
    reject!(c, {
        c.action = 99;
    });
    reject!(c, {
        c.expected_epoch = i64::MAX as u64;
    });
    reject!(c, {
        c.expected_revision = u64::MAX;
    });
    reject!(c, {
        c.proposal_id = c.conversation_id.clone();
    });
    reject!(c, {
        c.proposal_digest = vec![0; 32];
    });
    reject!(c, {
        c.conversation_id = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA".into();
    });
    reject!(c, {
        c.peer = None;
    });
    reject!(c, {
        c.peer.as_mut().unwrap().device = None;
    });
    reject!(c, {
        c.peer.as_mut().unwrap().device.as_mut().unwrap().device_id = 2;
    });
    reject!(c, {
        c.peer.as_mut().unwrap().identity_fingerprint.clear();
    });
    reject!(c, {
        c.peer.as_mut().unwrap().device.as_mut().unwrap().user_id = c
            .initiator
            .as_ref()
            .unwrap()
            .device
            .as_ref()
            .unwrap()
            .user_id
            .clone();
    });
    reject!(c, {
        c.peer
            .as_mut()
            .unwrap()
            .device
            .as_mut()
            .unwrap()
            .generation_id = c
            .initiator
            .as_ref()
            .unwrap()
            .device
            .as_ref()
            .unwrap()
            .generation_id
            .clone();
    });
    let mut maximum = original;
    maximum.expected_epoch = i64::MAX as u64 - 1;
    maximum.expected_revision = i64::MAX as u64 - 1;
    validate_control(&maximum).unwrap();
    assert!(RecoveryControl::decode(&[0x80][..]).is_err());
    // Prost drops unknown fields; future critical semantics MUST bump versions.
    // Go ingress rejects them. Neither side may infer future features from them.
    let mut wire = maximum.encode_to_vec();
    wire.extend([0xf8, 0x03, 0x01]);
    assert_eq!(RecoveryControl::decode(wire.as_slice()).unwrap(), maximum);
}

#[test]
fn outcomes_reject_replayed_decisions_and_unapproved_transitions() {
    let original = outcome();
    let expected = original.proposal.as_ref().unwrap();
    let (a, b) = pins();
    macro_rules! reject {
        ($o:ident, $body:block) => {{
            let mut $o = original.clone();
            $body;
            assert!(verify_outcome(&$o, expected, &a, &b).is_err());
        }};
    }
    reject!(o, {
        o.new_epoch += 1;
    });
    reject!(o, {
        o.epoch_revision += 1;
    });
    reject!(o, {
        o.protocol_version = 1;
    });
    reject!(o, {
        o.decision = None;
    });
    reject!(o, {
        o.state = 99;
    });
    reject!(o, {
        o.decision.as_mut().unwrap().signature.clear();
    });
    reject!(o, {
        o.decision.as_mut().unwrap().signature[0] ^= 1;
    });
    reject!(o, {
        o.proposal.as_mut().unwrap().signature[0] ^= 1;
    });
    reject!(o, {
        o.decision
            .as_mut()
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .proposal_digest[0] ^= 1;
    });
    reject!(o, {
        o.decision
            .as_mut()
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .expected_epoch += 1;
    });
    reject!(o, {
        o.decision
            .as_mut()
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .expected_revision += 1;
    });
    reject!(o, {
        o.decision
            .as_mut()
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .conversation_id = "99999999-9999-4999-8999-999999999999".into();
    });
    reject!(o, {
        o.decision
            .as_mut()
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .action = RecoveryAction::Cancel as i32;
    });
    reject!(o, {
        o.decision
            .as_mut()
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .request_id = expected.control.as_ref().unwrap().proposal_id.clone();
    });
    reject!(o, {
        o.decision
            .as_mut()
            .unwrap()
            .control
            .as_mut()
            .unwrap()
            .peer
            .as_mut()
            .unwrap()
            .identity_fingerprint[0] ^= 1;
    });
    assert!(verify_outcome(&original, expected, &b, &a).is_err());
    for state in [
        RecoveryState::Pending,
        RecoveryState::Expired,
        RecoveryState::Superseded,
    ] {
        let o = RecoveryOutcome {
            protocol_version: 2,
            proposal: Some(expected.clone()),
            state: state as i32,
            decision: None,
            new_epoch: 0,
            epoch_revision: 0,
        };
        verify_outcome(&o, expected, &a, &b).unwrap();
        let mut bad = o.clone();
        bad.decision = original.decision.clone();
        assert!(validate_outcome(&bad, expected).is_err());
        assert!(verify_outcome(&o, expected, &a, &a).is_err());
    }
}

#[test]
fn bound_fence_requires_exact_send_and_signature_verified_acceptance() {
    let o = outcome();
    let (a, b) = pins();
    let verified = verify_outcome(&o, o.proposal.as_ref().unwrap(), &a, &b).unwrap();
    let fence = EpochSealedFence::decode(
        include_bytes!("../../proto/fixtures/epoch-recovery-v1/fence.bin").as_slice(),
    )
    .unwrap();
    let expected = fence.binding.as_ref().unwrap();
    validate_fence(&fence, expected, &verified).unwrap();
    macro_rules! reject {
        ($f:ident, $body:block) => {{
            let mut $f = fence.clone();
            $body;
            assert!(validate_fence(&$f, expected, &verified).is_err());
        }};
    }
    reject!(f, {
        f.binding.as_mut().unwrap().envelope_digest[0] ^= 1;
    });
    reject!(f, {
        f.binding.as_mut().unwrap().epoch += 1;
    });
    reject!(f, {
        f.binding.as_mut().unwrap().message_id = expected.conversation_id.clone();
    });
    reject!(f, {
        f.binding.as_mut().unwrap().conversation_id = expected.message_id.clone();
    });
    reject!(f, {
        f.binding
            .as_mut()
            .unwrap()
            .sender
            .as_mut()
            .unwrap()
            .generation_id = expected.message_id.clone();
    });
    reject!(f, {
        f.binding
            .as_mut()
            .unwrap()
            .recipient
            .as_mut()
            .unwrap()
            .generation_id = expected.message_id.clone();
    });
    reject!(f, {
        f.binding.as_mut().unwrap().protocol_version = 1;
    });
    reject!(f, {
        f.binding = None;
    });
    reject!(f, {
        f.proposal_digest[0] ^= 1;
    });
    reject!(f, {
        f.next_epoch += 1;
    });
    reject!(f, {
        f.epoch_revision += 1;
    });
    let mut unrelated = fence.clone();
    unrelated.binding.as_mut().unwrap().epoch += 1;
    assert!(validate_fence(&unrelated, unrelated.binding.as_ref().unwrap(), &verified).is_err());
    for wire in [DECLINE, CANCEL] {
        let o = RecoveryOutcome::decode(wire).unwrap();
        let verified = verify_outcome(&o, o.proposal.as_ref().unwrap(), &a, &b).unwrap();
        assert!(validate_fence(&fence, expected, &verified).is_err());
    }
}

#[test]
fn epoch_routing_boundary_rejects_legacy_even_at_epoch_zero() {
    for (v, epoch, actual, ok) in [
        (1, 0, 0, false),
        (1, 1, 1, false),
        (2, 0, 0, true),
        (2, 7, 7, true),
        (2, 7, 8, false),
        (0, 0, 0, false),
        (3, 0, 0, false),
        (2, i64::MAX as u64, i64::MAX as u64, true),
        (2, u64::MAX, u64::MAX, false),
    ] {
        assert_eq!(validate_epoch_version_binding(v, epoch, actual).is_ok(), ok);
    }
    let wire = include_bytes!("../../proto/fixtures/e2ee-v1-envelope.bin");
    let mut e = Envelope::decode(wire.as_slice()).unwrap();
    validate_envelope(&e).unwrap();
    assert_eq!(e.encode_to_vec(), wire);
    let hash: String = Sha256::digest(wire)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    assert_eq!(
        hash,
        "0292509a87f63fd842f62a9d24cfb2a2bdae6eb42e1b7f8b34b15d8551dce946"
    );
    e.protocol_version = 2;
    assert!(validate_envelope(&e).is_err());
}
