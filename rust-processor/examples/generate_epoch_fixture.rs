//! Deterministic public-only data vectors. No key generation/private material.
use prost::Message;
use rust_processor::{
    encryption::{contract::MESSAGE_RETENTION_MS, epoch_contract as contract},
    proto::{e2ee as common, e2ee_v2 as v2},
};
use std::{io::Write, path::Path};

fn write(dir: &Path, name: &str, b: &[u8]) -> std::io::Result<()> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join(name))?
        .write_all(b)
}
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .ok_or("usage: generate_epoch_fixture NEW_OUTPUT_DIRECTORY")?;
    let dir = Path::new(&output);
    std::fs::create_dir(dir)?;
    // Reuse only reviewed public metadata, not an old message reader or private DB.
    let keys = common::UploadPreKeysRequest::decode(
        include_bytes!("../../proto/fixtures/e2ee-v1-publication.bin").as_slice(),
    )?;
    let sender = common::DeviceRef {
        user_id: "11111111-1111-4111-8111-111111111111".into(),
        device_id: 1,
        generation_id: "33333333-3333-4333-8333-333333333333".into(),
    };
    let recipient = common::DeviceRef {
        user_id: "22222222-2222-4222-8222-222222222222".into(),
        device_id: 1,
        generation_id: "44444444-4444-4444-8444-444444444444".into(),
    };
    let context = common::ClientContext {
        protocol_version: 2,
        device_generation_id: sender.generation_id.clone(),
    };
    let e = v2::Envelope {
        blob: None,
        protocol_version: 2,
        crypto_suite: common::CryptoSuite::SignalPq1 as i32,
        message_id: "55555555-5555-4555-8555-555555555555".into(),
        conversation_id: "66666666-6666-4666-8666-666666666666".into(),
        sender: Some(sender.clone()),
        recipient: Some(recipient.clone()),
        message_type: common::CiphertextType::PrekeySignal as i32,
        ciphertext: vec![1, 2, 3],
        crypto_epoch: Some((1u64 << 53) + 17),
    };
    let mut hashes = String::new();
    for (name, epoch) in [("initial", 0), ("envelope", e.crypto_epoch.unwrap())] {
        let mut e = e.clone();
        e.crypto_epoch = Some(epoch);
        write(dir, &format!("{name}.bin"), &e.encode_to_vec())?;
        write(
            dir,
            &format!("{name}.frame"),
            &contract::envelope_frame(&e)?,
        )?;
        let digest: String = contract::envelope_digest(&e)?
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        hashes.push_str(&format!("{digest}  {name}.frame\n"));
    }
    write(dir, "frames.sha256", hashes.as_bytes())?;
    let p = v2::EncryptedPayload {
        format_version: 2,
        message_id: e.message_id.clone(),
        conversation_id: e.conversation_id.clone(),
        sender: Some(sender.clone()),
        recipient: Some(recipient.clone()),
        created_at_unix_ms: 1_700_000_000_123,
        content: Some(v2::encrypted_payload::Content::Text(common::TextMessage {
            text: "epoch-bound text".into(),
        })),
        crypto_epoch: e.crypto_epoch,
    };
    contract::validate_payload_binding(&p, &e)?;
    write(dir, "payload.bin", &p.encode_to_vec())?;
    let mut confirmation = p.clone();
    confirmation.content = Some(v2::encrypted_payload::Content::SessionConfirmation(
        v2::SessionConfirmation {
            in_reply_to_message_id: "88888888-8888-4888-8888-888888888888".into(),
            crypto_epoch: p.crypto_epoch,
        },
    ));
    contract::validate_payload_binding(&confirmation, &e)?;
    write(dir, "confirmation.bin", &confirmation.encode_to_vec())?;
    let acquisition = v2::AcquireBundleRequest {
        context: Some(context.clone()),
        conversation_id: e.conversation_id.clone(),
        acquisition_id: "77777777-7777-4777-8777-777777777777".into(),
        expected_peer: Some(recipient.clone()),
        crypto_epoch: e.crypto_epoch,
    };
    let public = rust_processor::proto::encryption::PreKeyBundleUpload::decode(
        include_bytes!("../../proto/fixtures/prekey-upload-v1.bin").as_slice(),
    )?;
    let bundle = v2::AcquiredBundle {
        acquisition_id: acquisition.acquisition_id.clone(),
        conversation_id: e.conversation_id.clone(),
        device: Some(common::Device {
            reference: Some(recipient),
            registration_id: 16383,
            identity_key: public.identity_key,
            public_material_ready: true,
            public_material_revision: 7,
        }),
        signed_pre_key: keys.signed_pre_key,
        one_time_pre_key: keys.one_time_pre_keys.first().cloned(),
        kyber_pre_key: keys.kyber_pre_keys.first().cloned(),
        not_after_unix_ms: 1_700_086_400_123,
        crypto_epoch: e.crypto_epoch,
        protocol_version: 2,
    };
    contract::validate_acquired_bundle(&acquisition, &bundle)?;
    write(dir, "acquire.bin", &acquisition.encode_to_vec())?;
    write(dir, "bundle.bin", &bundle.encode_to_vec())?;
    let receipt = v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: contract::envelope_digest(&e)?.to_vec(),
        stored_at_unix_ms: 1_700_000_000_123,
        expires_at_unix_ms: 1_700_000_000_123 + MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    };
    contract::validate_receipt(
        &v2::SendMessageRequest {
            context: Some(context),
            envelope: Some(e),
        },
        &receipt,
    )?;
    write(dir, "receipt.bin", &receipt.encode_to_vec())?;
    println!(
        "Wrote deterministic public v2 contract vectors; opaque ciphertext is NOT real encrypted text"
    );
    Ok(())
}
