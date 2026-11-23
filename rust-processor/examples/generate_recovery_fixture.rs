//! Generate public-only recovery vectors. Private identities never leave RAM.
use libsignal_protocol::IdentityKeyPair;
use prost::Message;
use rust_processor::encryption::recovery_contract::{
    control_digest, control_frame, identity_fingerprint, sign_control,
};
use rust_processor::proto::e2ee::{
    CryptoSuite, DeviceRef, EpochSealedFence, EpochSendBinding, RecoveryAction, RecoveryControl,
    RecoveryOutcome, RecoveryParticipant, RecoveryState,
};
use sha2::{Digest, Sha256};
use std::{io::Write, path::Path};

fn write(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(name))?;
    file.write_all(bytes)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .ok_or("usage: generate_recovery_fixture NEW_OUTPUT_DIRECTORY")?;
    let dir = Path::new(&output);
    // Deliberately refuse to replace a reviewed vector directory.
    std::fs::create_dir(dir)?;
    let mut rng = rand::rng();
    let a = IdentityKeyPair::generate(&mut rng);
    let b = IdentityKeyPair::generate(&mut rng);
    write(dir, "initiator.pub", &a.public_key().serialize())?;
    write(dir, "peer.pub", &b.public_key().serialize())?;
    let control = RecoveryControl {
        protocol_version: 2,
        format_version: 1,
        crypto_suite: CryptoSuite::SignalPq1 as i32,
        action: RecoveryAction::Propose as i32,
        conversation_id: "66666666-6666-4666-8666-666666666666".into(),
        request_id: "77777777-7777-4777-8777-777777777777".into(),
        proposal_id: "77777777-7777-4777-8777-777777777777".into(),
        // Exercise full-width values beyond JavaScript's exact integer range.
        expected_epoch: (1u64 << 53) + 17,
        expected_revision: (1u64 << 53) + 29,
        initiator: Some(RecoveryParticipant {
            device: Some(DeviceRef {
                user_id: "11111111-1111-4111-8111-111111111111".into(),
                device_id: 1,
                generation_id: "33333333-3333-4333-8333-333333333333".into(),
            }),
            identity_fingerprint: identity_fingerprint(a.identity_key()).to_vec(),
        }),
        peer: Some(RecoveryParticipant {
            device: Some(DeviceRef {
                user_id: "22222222-2222-4222-8222-222222222222".into(),
                device_id: 1,
                generation_id: "44444444-4444-4444-8444-444444444444".into(),
            }),
            identity_fingerprint: identity_fingerprint(b.identity_key()).to_vec(),
        }),
        proposal_digest: vec![],
    };
    let proposal = sign_control(control.clone(), &a)?;
    let proposal_digest = control_digest(&control)?;
    let mut hashes = String::new();
    let mut accepted = None;
    for (name, action, state, request, signer) in [
        (
            "accept",
            RecoveryAction::Accept,
            RecoveryState::Accepted,
            "88888888-8888-4888-8888-888888888888",
            &b,
        ),
        (
            "decline",
            RecoveryAction::Decline,
            RecoveryState::Declined,
            "99999999-9999-4999-8999-999999999999",
            &b,
        ),
        (
            "cancel",
            RecoveryAction::Cancel,
            RecoveryState::Cancelled,
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            &a,
        ),
    ] {
        let mut c = control.clone();
        c.action = action as i32;
        c.request_id = request.into();
        c.proposal_digest = proposal_digest.to_vec();
        let decision = sign_control(c.clone(), signer)?;
        let frame = control_frame(&c)?;
        write(dir, &format!("{name}.frame"), &frame)?;
        let digest: String = Sha256::digest(&frame)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        hashes.push_str(&format!("{digest}  {name}.frame\n"));
        let outcome = RecoveryOutcome {
            protocol_version: 2,
            proposal: Some(proposal.clone()),
            state: state as i32,
            decision: Some(decision),
            new_epoch: if state == RecoveryState::Accepted {
                control.expected_epoch + 1
            } else {
                0
            },
            epoch_revision: if state == RecoveryState::Accepted {
                control.expected_revision + 1
            } else {
                0
            },
        };
        write(dir, &format!("{name}.bin"), &outcome.encode_to_vec())?;
        if state == RecoveryState::Accepted {
            accepted = Some(outcome);
        }
    }
    write(dir, "propose.frame", &control_frame(&control)?)?;
    hashes.push_str(&format!(
        "{}  propose.frame\n",
        proposal_digest
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
    ));
    write(dir, "frames.sha256", hashes.as_bytes())?;
    let binding = EpochSendBinding {
        protocol_version: 2,
        conversation_id: control.conversation_id.clone(),
        epoch: control.expected_epoch,
        message_id: "55555555-5555-4555-8555-555555555555".into(),
        sender: control.initiator.as_ref().unwrap().device.clone(),
        recipient: control.peer.as_ref().unwrap().device.clone(),
        envelope_digest: vec![0x42; 32],
    };
    let accepted = accepted.unwrap();
    let fence = EpochSealedFence {
        binding: Some(binding),
        proposal_digest: proposal_digest.to_vec(),
        next_epoch: accepted.new_epoch,
        epoch_revision: accepted.epoch_revision,
    };
    write(dir, "fence.bin", &fence.encode_to_vec())?;
    println!(
        "Wrote synthetic PUBLIC control vectors; no private keys, account credentials or real ciphertext"
    );
    Ok(())
}
