use libsignal_protocol::{IdentityKey, PublicKey, kem};
use prost::Message;
use rust_processor::proto::encryption::PreKeyBundleUpload;

const FIXTURE: &[u8] = include_bytes!("../../proto/fixtures/prekey-upload-v1.bin");

mod v1 {
    use prost::Message;
    use rust_processor::encryption::contract::{
        ContractError, MAX_REQUEST_BYTES, MAX_TEXT_BYTES, validate_envelope,
        validate_payload_binding,
    };
    use rust_processor::proto::e2ee::{
        EncryptedPayload, Envelope, SessionConfirmation, TextMessage, encrypted_payload,
    };

    fn envelope() -> Envelope {
        Envelope::decode(include_bytes!("../../proto/fixtures/e2ee-v1-envelope.bin").as_slice())
            .unwrap()
    }

    fn payload(envelope: &Envelope) -> EncryptedPayload {
        EncryptedPayload {
            format_version: 1,
            message_id: envelope.message_id.clone(),
            conversation_id: envelope.conversation_id.clone(),
            sender: envelope.sender.clone(),
            recipient: envelope.recipient.clone(),
            created_at_unix_ms: 1_700_000_000_123,
            content: Some(encrypted_payload::Content::Text(TextMessage {
                text: "hello".into(),
            })),
        }
    }

    #[test]
    fn shared_v1_wire_has_explicit_account_device_generation_and_conversation() {
        let envelope = envelope();
        assert_eq!(
            envelope.encode_to_vec(),
            include_bytes!("../../proto/fixtures/e2ee-v1-envelope.bin")
        );
        assert_eq!(envelope.message_id, "55555555-5555-4555-8555-555555555555");
        assert_eq!(
            envelope.conversation_id,
            "66666666-6666-4666-8666-666666666666"
        );
        assert_eq!(
            envelope.sender.as_ref().unwrap().user_id,
            "11111111-1111-4111-8111-111111111111"
        );
        assert_eq!(
            envelope.recipient.as_ref().unwrap().generation_id,
            "44444444-4444-4444-8444-444444444444"
        );
        assert_eq!(envelope.ciphertext, [1, 2, 3]); // Opaque wire fixture, not a real Signal ciphertext.
        validate_envelope(&envelope).unwrap();
        validate_payload_binding(&payload(&envelope), &envelope).unwrap();
    }

    #[test]
    fn envelope_receipt_digest_matches_go_independently_of_wire_order() {
        use rust_processor::encryption::contract::envelope_digest;
        let e = envelope();
        let digest = envelope_digest(&e).unwrap();
        let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            "9c2252f36ab1d07773dc5a6ca84504ef8af4a6e66e277022391ef84e49e85190"
        );
        let wire = e.encode_to_vec();
        assert_eq!(&wire[..2], &[8, 1]);
        let reordered = [&wire[2..], &wire[..2]].concat();
        let decoded = Envelope::decode(reordered.as_slice()).unwrap();
        assert_eq!(envelope_digest(&decoded).unwrap(), digest);
        let mut changed = e.clone();
        changed.recipient.as_mut().unwrap().generation_id = e.conversation_id.clone();
        assert_ne!(envelope_digest(&changed).unwrap(), digest);
        let mut changed = e;
        changed.ciphertext.push(4);
        assert_ne!(envelope_digest(&changed).unwrap(), digest);
        changed.protocol_version = 0;
        assert!(envelope_digest(&changed).is_err());
    }

    #[test]
    fn invalid_v1_frames_fail_closed_before_decrypt() {
        for (device_id, accepted) in [
            (0, false),
            (1, true),
            (127, true),
            (128, false),
            (257, false),
            (u32::MAX, false),
        ] {
            let mut e = envelope();
            e.sender.as_mut().unwrap().device_id = device_id;
            assert_eq!(validate_envelope(&e).is_ok(), accepted);
        }
        for version in [0, 2, u32::MAX] {
            let mut e = envelope();
            e.protocol_version = version;
            assert_eq!(
                validate_envelope(&e),
                Err(ContractError::UnsupportedVersion)
            );
        }
        let mut e = envelope();
        e.message_type = 3;
        assert!(validate_envelope(&e).is_err());
        let mut e = envelope();
        e.crypto_suite = 0;
        assert!(validate_envelope(&e).is_err());
        let mut e = envelope();
        e.ciphertext = vec![0; MAX_REQUEST_BYTES];
        assert!(validate_envelope(&e).is_err());
        let mut e = envelope();
        e.sender = None;
        assert!(validate_envelope(&e).is_err());
        let mut e = envelope();
        e.recipient.as_mut().unwrap().generation_id.clear();
        assert!(validate_envelope(&e).is_err());
    }

    #[test]
    fn decrypted_payload_cannot_be_relabelled_as_another_chat_message_or_device() {
        let e = envelope();
        let original = payload(&e);
        let mut p = original.clone();
        p.conversation_id = "77777777-7777-4777-8777-777777777777".into();
        assert!(validate_payload_binding(&p, &e).is_err());
        let mut p = original.clone();
        p.message_id = p.conversation_id.clone();
        assert!(validate_payload_binding(&p, &e).is_err());
        let mut p = original.clone();
        p.sender.as_mut().unwrap().generation_id = p.conversation_id.clone();
        assert!(validate_payload_binding(&p, &e).is_err());
        let mut p = original.clone();
        p.recipient.as_mut().unwrap().device_id = 2;
        assert!(validate_payload_binding(&p, &e).is_err());
        let mut p = original.clone();
        p.format_version = 2;
        assert!(validate_payload_binding(&p, &e).is_err());
        let mut p = original;
        p.created_at_unix_ms = -1;
        assert!(validate_payload_binding(&p, &e).is_err());
    }

    #[test]
    fn text_limit_counts_utf8_bytes_and_confirmation_is_a_distinct_payload() {
        let e = envelope();
        let mut p = payload(&e);
        p.content = Some(encrypted_payload::Content::Text(TextMessage {
            text: "я".repeat(MAX_TEXT_BYTES / 2),
        }));
        validate_payload_binding(&p, &e).unwrap();
        p.content = Some(encrypted_payload::Content::Text(TextMessage {
            text: "я".repeat(MAX_TEXT_BYTES / 2 + 1),
        }));
        assert!(validate_payload_binding(&p, &e).is_err());
        p.content = None;
        assert!(validate_payload_binding(&p, &e).is_err());
        p.content = Some(encrypted_payload::Content::SessionConfirmation(
            SessionConfirmation {
                in_reply_to_message_id: "77777777-7777-4777-8777-777777777777".into(),
            },
        ));
        validate_payload_binding(&p, &e).unwrap();
        p.content = Some(encrypted_payload::Content::SessionConfirmation(
            SessionConfirmation {
                in_reply_to_message_id: p.message_id.clone(),
            },
        ));
        assert!(validate_payload_binding(&p, &e).is_err());
    }
}

#[test]
fn public_wire_fixture_matches_libsignal_and_prost() {
    let upload = PreKeyBundleUpload::decode(FIXTURE).unwrap();
    assert_eq!(upload.encode_to_vec(), FIXTURE);
    assert_eq!(upload.user_id, "11111111-1111-4111-8111-111111111111");
    assert_eq!(upload.device_id, 1);
    assert_eq!(upload.registration_id, 16383);
    assert_eq!(upload.signed_pre_key_timestamp, 1_700_000_000_123);
    assert_eq!(upload.identity_key.len(), 33);
    let identity = IdentityKey::try_from(upload.identity_key.as_slice()).unwrap();
    let signed = upload.signed_pre_key.unwrap();
    assert_eq!(signed.key_id, u32::MAX - 2);
    assert_eq!(signed.public_key.len(), 33);
    assert_eq!(signed.signature.len(), 64);
    PublicKey::deserialize(&signed.public_key).unwrap();
    assert!(
        identity
            .public_key()
            .verify_signature(&signed.public_key, &signed.signature)
    );
    assert_eq!(upload.one_time_pre_keys.len(), 1);
    assert_eq!(upload.one_time_pre_keys[0].key_id, 1 << 31);
    PublicKey::deserialize(&upload.one_time_pre_keys[0].public_key).unwrap();
    assert_eq!(upload.kyber_pre_keys.len(), 2);
    for (i, key) in upload.kyber_pre_keys.iter().enumerate() {
        assert_eq!(key.key_id, u32::MAX - 1 + i as u32);
        assert_eq!(key.is_last_resort, i == 1);
        assert_eq!(key.public_key.len(), 1569);
        assert_eq!(key.signature.len(), 64);
        let public = kem::PublicKey::deserialize(&key.public_key).unwrap();
        assert_eq!(public.key_type(), kem::KeyType::Kyber1024);
        assert!(
            identity
                .public_key()
                .verify_signature(&key.public_key, &key.signature)
        );
    }
}

#[test]
fn changed_signed_public_key_or_signature_is_rejected() {
    let upload = PreKeyBundleUpload::decode(FIXTURE).unwrap();
    let identity = IdentityKey::try_from(upload.identity_key.as_slice()).unwrap();
    let signed = upload.signed_pre_key.unwrap();
    let keys = std::iter::once((signed.public_key, signed.signature)).chain(
        upload
            .kyber_pre_keys
            .into_iter()
            .map(|key| (key.public_key, key.signature)),
    );
    for (public, signature) in keys {
        let mut changed_public = public.clone();
        *changed_public.last_mut().unwrap() ^= 1;
        assert!(
            !identity
                .public_key()
                .verify_signature(&changed_public, &signature)
        );
        let mut changed_signature = signature.clone();
        changed_signature[0] ^= 1;
        assert!(
            !identity
                .public_key()
                .verify_signature(&public, &changed_signature)
        );
    }
}

// Real libsignal + our SQLite stores; no network, TTL cleanup or recovery UI.
mod delivery {
    use libsignal_protocol::{
        CiphertextMessage, DeviceId, GenericSignedPreKey, IdentityKeyStore, PreKeyBundle,
        ProtocolAddress, SessionStore, SignalProtocolError, message_decrypt, message_encrypt,
        process_prekey_bundle,
    };
    use rust_processor::encryption::Stores;
    use std::time::{Duration, SystemTime};

    struct Peer {
        store: Stores,
        address: ProtocolAddress,
        _file: tempfile::NamedTempFile,
    }

    impl Peer {
        async fn new(name: &str) -> Self {
            let file = tempfile::NamedTempFile::new().unwrap();
            let mut store = Stores::new(file.path().to_str().unwrap()).unwrap();
            store.ensure_keys_generated().await.unwrap();
            Self {
                store,
                address: ProtocolAddress::new(name.into(), DeviceId::new(1).unwrap()),
                _file: file,
            }
        }

        async fn encrypt(
            &mut self,
            remote: &ProtocolAddress,
            text: &[u8],
            now: SystemTime,
        ) -> Result<CiphertextMessage, SignalProtocolError> {
            message_encrypt(
                text,
                remote,
                &self.address,
                &mut self.store.session,
                &mut self.store.identity,
                now,
                &mut rand::rng(),
            )
            .await
        }

        async fn decrypt(
            &mut self,
            remote: &ProtocolAddress,
            message: &CiphertextMessage,
        ) -> Vec<u8> {
            message_decrypt(
                message,
                remote,
                &self.address,
                &mut self.store.session,
                &mut self.store.identity,
                &mut self.store.prekey,
                &self.store.signed_prekey,
                &mut self.store.kyber_prekey,
                &mut rand::rng(),
            )
            .await
            .unwrap()
        }
    }

    async fn pair(now: SystemTime) -> (Peer, Peer) {
        let mut alice = Peer::new("alice").await;
        let bob = Peer::new("bob").await;
        let identity = bob.store.identity.get_identity_key_pair().await.unwrap();
        let signed = bob
            .store
            .signed_prekey
            .get_active_signed_pre_key()
            .await
            .unwrap();
        let (ec_id, ec) = bob
            .store
            .prekey
            .get_unused_pre_keys(1)
            .await
            .unwrap()
            .remove(0);
        let (pq_id, pq, _) = bob
            .store
            .kyber_prekey
            .get_unused_kyber_pre_keys(1)
            .await
            .unwrap()
            .remove(0);
        let bundle = PreKeyBundle::new(
            bob.store
                .identity
                .get_local_registration_id()
                .await
                .unwrap(),
            bob.address.device_id(),
            Some((ec_id, ec.public_key)),
            signed.id().unwrap(),
            signed.public_key().unwrap(),
            signed.signature().unwrap().to_vec(),
            pq_id,
            pq.public_key().unwrap(),
            pq.signature().unwrap().to_vec(),
            *identity.identity_key(),
        )
        .unwrap();
        process_prekey_bundle(
            &bob.address,
            &alice.address,
            &mut alice.store.session,
            &mut alice.store.identity,
            &bundle,
            now,
            &mut rand::rng(),
        )
        .await
        .unwrap();
        (alice, bob)
    }

    async fn confirm(alice: &mut Peer, bob: &mut Peer, now: SystemTime) {
        let first = alice
            .encrypt(&bob.address, b"bootstrap", now)
            .await
            .unwrap();
        assert_eq!(bob.decrypt(&alice.address, &first).await, b"bootstrap");
        let reply = bob.encrypt(&alice.address, b"reply", now).await.unwrap();
        assert_eq!(alice.decrypt(&bob.address, &reply).await, b"reply");
    }

    #[tokio::test]
    async fn losing_first_prekey_message_still_allows_bootstrap() {
        let now = SystemTime::now();
        let (mut alice, mut bob) = pair(now).await;
        let first = alice.encrypt(&bob.address, b"first", now).await.unwrap();
        let second = alice.encrypt(&bob.address, b"second", now).await.unwrap();
        assert!(matches!(first, CiphertextMessage::PreKeySignalMessage(_)));
        assert!(matches!(second, CiphertextMessage::PreKeySignalMessage(_)));
        assert_eq!(bob.decrypt(&alice.address, &second).await, b"second");
        // A delayed first message can still use a stored skipped message key.
        assert_eq!(bob.decrypt(&alice.address, &first).await, b"first");
    }

    #[tokio::test]
    async fn established_session_handles_gaps_and_delayed_messages() {
        let now = SystemTime::now();
        let (mut alice, mut bob) = pair(now).await;
        confirm(&mut alice, &mut bob, now).await;
        let mut messages = Vec::new();
        for i in 0..5 {
            let message = alice
                .encrypt(&bob.address, format!("message-{i}").as_bytes(), now)
                .await
                .unwrap();
            assert!(matches!(message, CiphertextMessage::SignalMessage(_)));
            messages.push(message);
        }
        // 0,2,3 are never delivered; 1 arrives after 4.
        assert_eq!(
            bob.decrypt(&alice.address, &messages[4]).await,
            b"message-4"
        );
        assert_eq!(
            bob.decrypt(&alice.address, &messages[1]).await,
            b"message-1"
        );
        let reply = bob
            .encrypt(&alice.address, b"after-gap", now)
            .await
            .unwrap();
        assert_eq!(alice.decrypt(&bob.address, &reply).await, b"after-gap");
    }

    #[tokio::test]
    async fn only_unacknowledged_session_is_stale_after_31_days() {
        let now = SystemTime::now();
        let later = now + Duration::from_secs(31 * 24 * 60 * 60);
        let (mut alice, bob) = pair(now).await;
        let undelivered = alice
            .encrypt(&bob.address, b"undelivered-bootstrap", now)
            .await
            .unwrap();
        assert!(matches!(
            undelivered,
            CiphertextMessage::PreKeySignalMessage(_)
        ));
        let before = alice
            .store
            .session
            .load_session(&bob.address)
            .await
            .unwrap()
            .unwrap()
            .serialize()
            .unwrap();
        let err = alice
            .encrypt(&bob.address, b"too-late", later)
            .await
            .unwrap_err();
        assert!(matches!(err, SignalProtocolError::SessionNotFound(_)));
        let after = alice
            .store
            .session
            .load_session(&bob.address)
            .await
            .unwrap()
            .unwrap()
            .serialize()
            .unwrap();
        // A failing assertion must not dump private serialized session state.
        assert!(before == after, "failed send must not advance the session");

        // Independent confirmed session: never rewind either session's clock
        // or imply that the stale-session recovery path is implemented.
        let (mut confirmed_alice, mut confirmed_bob) = pair(now).await;
        confirm(&mut confirmed_alice, &mut confirmed_bob, now).await;
        let message = confirmed_alice
            .encrypt(&confirmed_bob.address, b"established-after-31-days", later)
            .await
            .unwrap();
        assert_eq!(
            confirmed_bob
                .decrypt(&confirmed_alice.address, &message)
                .await,
            b"established-after-31-days"
        );
    }
}
