use super::*;

#[derive(Clone)]
struct Fields {
    conversation: String,
    epoch: u64,
    message: String,
    blob: String,
    sender: DeviceRef,
    recipient: DeviceRef,
}

impl Fields {
    fn new() -> Self {
        let device = || DeviceRef {
            user_id: Uuid::new_v4().to_string(),
            generation_id: Uuid::new_v4().to_string(),
            device_id: 1,
        };
        Self {
            conversation: Uuid::new_v4().to_string(),
            epoch: 3,
            message: Uuid::new_v4().to_string(),
            blob: Uuid::new_v4().to_string(),
            sender: device(),
            recipient: device(),
        }
    }

    fn binding(&self) -> Result<AttachmentBinding> {
        AttachmentBinding::new(
            &self.conversation,
            self.epoch,
            &self.message,
            &self.blob,
            &self.sender,
            &self.recipient,
        )
    }
}

fn prepare(binding: &AttachmentBinding, bytes: &[u8]) -> PreparedAttachment {
    seal(binding, Zeroizing::new(bytes.to_vec())).unwrap()
}

#[test]
fn photo_voice_and_arbitrary_file_bytes_roundtrip_without_content_transformation() {
    let binding = Fields::new().binding().unwrap();
    // Sanitization is a separate preparation policy, never an implicit change
    // made by encryption. An explicit original must remain byte-for-byte exact.
    for data in [
        b"\xff\xd8EXIF GPS test fixture\xff\xd9".to_vec(),
        b"OggS synthetic voice bytes\x00\x01".to_vec(),
        (0..=255).collect::<Vec<u8>>(),
        vec![17; 1024 * 1024],
    ] {
        let prepared = prepare(&binding, &data);
        assert_eq!(prepared.blob().len(), data.len() + BLOB_OVERHEAD);
        assert_eq!(prepared.descriptor().plaintext_len(), data.len());
        assert_eq!(
            prepared.descriptor().blob_digest(),
            &<[u8; 32]>::from(Sha256::digest(prepared.blob()))
        );
        let plain = open(&binding, prepared.descriptor(), prepared.blob()).unwrap();
        assert_eq!(*plain, data);
    }
}

#[test]
fn secret_descriptor_and_exact_blob_survive_serialization_boundary() {
    let fields = Fields::new();
    let prepared = prepare(&fields.binding().unwrap(), b"saved attachment");
    let descriptor = prepared.descriptor().encode_for_signal();
    let blob = prepared.blob().to_vec();
    drop(prepared);
    let restored = AttachmentDescriptor::decode_from_signal(descriptor.as_ref()).unwrap();
    // A later upload retry uses exactly blob/digest, with NO fresh seal call.
    assert_eq!(restored.encode_for_signal().as_ref(), descriptor.as_ref());
    assert_eq!(restored.blob_len(), blob.len());
    assert_eq!(
        open(&fields.binding().unwrap(), &restored, &blob)
            .unwrap()
            .as_slice(),
        b"saved attachment"
    );
}

#[test]
fn each_new_preparation_generates_a_separate_key_and_nonce() {
    let binding = Fields::new().binding().unwrap();
    let first = prepare(&binding, b"same bytes");
    let second = prepare(&binding, b"same bytes");
    assert_ne!(
        &first.descriptor().encode_for_signal()[1..33],
        &second.descriptor().encode_for_signal()[1..33]
    );
    assert_ne!(
        &first.blob()[HEADER.len()..NONCE_END],
        &second.blob()[HEADER.len()..NONCE_END]
    );
    assert_ne!(first.blob(), second.blob());
}

#[test]
fn routing_swap_is_rejected_even_with_original_valid_key_and_digest() {
    let fields = Fields::new();
    let prepared = prepare(&fields.binding().unwrap(), b"bound attachment");
    for field in 0..9 {
        let mut other = fields.clone();
        let fresh = || Uuid::new_v4().to_string();
        match field {
            0 => other.conversation = fresh(),
            1 => other.message = fresh(),
            2 => other.blob = fresh(),
            3 => other.epoch += 1,
            4 => other.sender.user_id = fresh(),
            5 => other.sender.generation_id = fresh(),
            6 => other.recipient.user_id = fresh(),
            7 => other.recipient.generation_id = fresh(),
            8 => std::mem::swap(&mut other.sender, &mut other.recipient),
            _ => unreachable!(),
        }
        assert!(matches!(
            open(
                &other.binding().unwrap(),
                prepared.descriptor(),
                prepared.blob()
            ),
            Err(AttachmentError::Authentication)
        ));
    }
}

#[test]
fn nonce_ciphertext_and_tag_tampering_fail_aead_even_after_public_hash_is_recomputed() {
    let binding = Fields::new().binding().unwrap();
    let prepared = prepare(&binding, b"authenticate all file bytes");
    for index in [HEADER.len(), NONCE_END, prepared.blob().len() - 1] {
        let mut blob = prepared.blob().to_vec();
        blob[index] ^= 1;
        assert!(matches!(
            open(&binding, prepared.descriptor(), &blob),
            Err(AttachmentError::Authentication)
        ));
        let encoded = prepared.descriptor().encode_for_signal();
        let mut descriptor = AttachmentDescriptor::decode_from_signal(encoded.as_ref()).unwrap();
        descriptor.digest = Sha256::digest(&blob).into();
        assert!(matches!(
            open(&binding, &descriptor, &blob),
            Err(AttachmentError::Authentication)
        ));
    }
    let mut encoded = prepared.descriptor().encode_for_signal();
    encoded[1] ^= 1;
    let wrong_key = AttachmentDescriptor::decode_from_signal(encoded.as_ref()).unwrap();
    assert!(matches!(
        open(&binding, &wrong_key, prepared.blob()),
        Err(AttachmentError::Authentication)
    ));
}

#[test]
fn all_truncations_extra_bytes_and_unknown_header_are_rejected() {
    let binding = Fields::new().binding().unwrap();
    let prepared = prepare(&binding, b"whole-file verification before exposure");
    for len in 0..prepared.blob().len() {
        assert!(matches!(
            open(&binding, prepared.descriptor(), &prepared.blob()[..len]),
            Err(AttachmentError::Authentication)
        ));
    }
    let mut extra = prepared.blob().to_vec();
    extra.push(0);
    assert!(matches!(
        open(&binding, prepared.descriptor(), &extra),
        Err(AttachmentError::Authentication)
    ));
    for index in 0..HEADER.len() {
        let mut blob = prepared.blob().to_vec();
        blob[index] ^= 1;
        let encoded = prepared.descriptor().encode_for_signal();
        let mut descriptor = AttachmentDescriptor::decode_from_signal(encoded.as_ref()).unwrap();
        descriptor.digest = Sha256::digest(&blob).into();
        assert!(matches!(
            open(&binding, &descriptor, &blob),
            Err(AttachmentError::Authentication)
        ));
    }
}

#[test]
fn malformed_descriptors_and_size_limits_fail_before_blob_allocation() {
    let binding = Fields::new().binding().unwrap();
    assert!(matches!(
        seal(&binding, Zeroizing::new(vec![])),
        Err(AttachmentError::Size)
    ));
    assert!(matches!(
        seal(&binding, Zeroizing::new(vec![0; MAX_PLAINTEXT_BYTES + 1])),
        Err(AttachmentError::Size)
    ));
    let prepared = prepare(&binding, b"descriptor fixture");
    let original = prepared.descriptor().encode_for_signal();
    for len in 0..DESCRIPTOR_BYTES {
        assert!(AttachmentDescriptor::decode_from_signal(&original[..len]).is_err());
    }
    let mut extra = Zeroizing::new(original.to_vec());
    extra.push(0);
    assert!(AttachmentDescriptor::decode_from_signal(&extra).is_err());
    let mut encoded = prepared.descriptor().encode_for_signal();
    encoded[0] = 2;
    assert!(matches!(
        AttachmentDescriptor::decode_from_signal(encoded.as_ref()),
        Err(AttachmentError::Descriptor)
    ));
    for size in [0, MAX_PLAINTEXT_BYTES as u64 + 1, u64::MAX] {
        let mut encoded = prepared.descriptor().encode_for_signal();
        encoded[65..].copy_from_slice(&size.to_be_bytes());
        assert!(matches!(
            AttachmentDescriptor::decode_from_signal(encoded.as_ref()),
            Err(AttachmentError::Size)
        ));
    }
    let mut encoded = prepared.descriptor().encode_for_signal();
    encoded[65..].copy_from_slice(&(MAX_PLAINTEXT_BYTES as u64).to_be_bytes());
    let max = AttachmentDescriptor::decode_from_signal(encoded.as_ref()).unwrap();
    assert_eq!(max.blob_len(), MAX_BLOB_BYTES);
    assert!(matches!(
        open(&binding, &max, prepared.blob()),
        Err(AttachmentError::Authentication)
    ));
}

#[test]
fn invalid_noncanonical_or_cross_account_bindings_are_rejected() {
    let fields = Fields::new();
    for field in 0..9 {
        let mut invalid = fields.clone();
        match field {
            0 => invalid.conversation = Uuid::nil().to_string(),
            1 => invalid.message = "not a uuid".into(),
            2 => invalid.blob = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA".into(),
            3 => invalid.epoch = i64::MAX as u64 + 1,
            4 => invalid.sender.device_id = 2,
            5 => invalid.recipient.device_id = 0,
            6 => invalid.sender.user_id = invalid.recipient.user_id.clone(),
            7 => invalid.sender.generation_id = invalid.recipient.generation_id.clone(),
            8 => invalid.recipient.generation_id = Uuid::nil().to_string(),
            _ => unreachable!(),
        }
        assert!(matches!(invalid.binding(), Err(AttachmentError::Binding)));
    }
    let mut archived = fields;
    archived.epoch = 0;
    assert!(archived.binding().is_ok());
    archived.epoch = i64::MAX as u64;
    assert!(archived.binding().is_ok());
}
