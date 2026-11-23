//! Generate PUBLIC test material; private key bytes never leave this process.
use libsignal_protocol::{IdentityKeyPair, KeyPair, kem};
use prost::Message;
use rust_processor::proto::encryption::{
    KyberPreKey, OneTimePreKey, PreKeyBundleUpload, SignedPreKey,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = std::env::args_os()
        .nth(1)
        .ok_or("usage: generate_prekey_fixture OUTPUT_PATH")?;
    let mut rng = rand::rng();
    let identity = IdentityKeyPair::generate(&mut rng);
    let signed = KeyPair::generate(&mut rng).public_key.serialize().to_vec();
    let signed_signature = identity
        .private_key()
        .calculate_signature(&signed, &mut rng)?
        .to_vec();
    let ec = KeyPair::generate(&mut rng).public_key.serialize().to_vec();
    let mut kyber_pre_keys = Vec::new();
    for (key_id, is_last_resort) in [(u32::MAX - 1, false), (u32::MAX, true)] {
        let public_key = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut rng)
            .public_key
            .serialize()
            .to_vec();
        let signature = identity
            .private_key()
            .calculate_signature(&public_key, &mut rng)?
            .to_vec();
        kyber_pre_keys.push(KyberPreKey {
            key_id,
            public_key,
            signature,
            is_last_resort,
        });
    }
    let fixture = PreKeyBundleUpload {
        user_id: "11111111-1111-4111-8111-111111111111".into(),
        device_id: 1,
        registration_id: 16383,
        identity_key: identity.public_key().serialize().to_vec(),
        signed_pre_key: Some(SignedPreKey {
            key_id: u32::MAX - 2,
            public_key: signed,
            signature: signed_signature,
        }),
        one_time_pre_keys: vec![OneTimePreKey {
            key_id: 1 << 31,
            public_key: ec,
        }],
        kyber_pre_keys,
        signed_pre_key_timestamp: 1_700_000_000_123,
    };
    let bytes = fixture.encode_to_vec();
    // Never overwrite a reviewed golden fixture by accident.
    use std::io::Write;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output)?;
    file.write_all(&bytes)?;
    println!(
        "Wrote {} bytes of public protobuf test material",
        bytes.len()
    );
    Ok(())
}
