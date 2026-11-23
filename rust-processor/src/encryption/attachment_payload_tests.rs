use super::*;
use crate::encryption::{attachment, epoch_contract};
use crate::proto::e2ee_v2::encrypted_payload::Content;

fn fixture() -> (v2::EncryptedPayload, v2::Envelope) {
    let p = v2::EncryptedPayload::decode(
        include_bytes!("../../../proto/fixtures/epoch-data-v2/attachment.bin").as_slice(),
    )
    .unwrap();
    let mut e = v2::Envelope::decode(
        include_bytes!("../../../proto/fixtures/epoch-data-v2/envelope.bin").as_slice(),
    )
    .unwrap();
    e.protocol_version = 3;
    if let Some(Content::Attachment(a)) = &p.content {
        e.blob = Some(blob_reference(a).unwrap());
    }
    (p, e)
}
fn part(p: &mut v2::EncryptedPayload) -> &mut AttachmentMessage {
    match p.content.as_mut().unwrap() {
        Content::Attachment(a) => a,
        _ => panic!("wrong fixture kind"),
    }
}
fn size(p: &mut v2::EncryptedPayload, bytes: u64) {
    part(p).private_descriptor[65..].copy_from_slice(&bytes.to_be_bytes());
}
fn check(p: &v2::EncryptedPayload, e: &v2::Envelope) -> bool {
    epoch_contract::validate_payload_binding(p, e).is_ok()
}

#[test]
fn native_preparation_draft_uses_same_metadata_contract_and_preserves_descriptor() {
    let (mut p, e) = fixture();
    let a = part(&mut p);
    let draft = AttachmentDraft {
        kind: AttachmentKind::try_from(a.kind).unwrap(),
        filename: Zeroizing::new(a.filename.clone()),
        mime_type: Zeroizing::new(a.mime_type.clone()),
        caption: Zeroizing::new(a.caption.clone()),
        photo_metadata_stripped: a.photo_metadata_stripped,
    };
    assert!(draft.validate(42).is_ok());
    assert!(draft.validate(0).is_err());
    assert!(draft.validate(attachment::MAX_PLAINTEXT_BYTES + 1).is_err());
    assert!(draft.matches(a));
    let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor).unwrap();
    let original = a.clone();
    let rebuilt = draft.into_message(a.blob_id.clone(), &descriptor);
    assert_eq!(rebuilt, original);
    p.content = Some(Content::Attachment(rebuilt));
    assert!(check(&p, &e));
}

#[test]
fn shared_private_fixture_requires_public_reference_and_preserves_text_frame() {
    let (p, e) = fixture();
    assert!(check(&p, &e));
    assert_eq!(p.format_version, 3);
    let copy = v2::EncryptedPayload::decode(p.encode_to_vec().as_slice()).unwrap();
    assert_eq!(copy, p); // Protobuf field order need not be canonical across languages.
    let mut before = p.clone();
    before.content = Some(Content::Text(crate::proto::e2ee::TextMessage {
        text: "existing text".into(),
    }));
    assert!(!check(&before, &e)); // Format3 cannot masquerade as text.
    before.format_version = 2;
    assert!(!check(&before, &e)); // Text is forbidden in an attachment envelope.
    let mut text_envelope = e.clone();
    text_envelope.protocol_version = 2;
    text_envelope.blob = None;
    assert!(check(&before, &text_envelope));
    assert!(!check(&p, &text_envelope)); // No private attachment under outer2.
    let mut wrong = p.clone();
    wrong.format_version = 2;
    assert!(!check(&wrong, &e)); // No attachment extension under the old payload format.
    let frame = epoch_contract::envelope_frame(&text_envelope).unwrap();
    assert_eq!(
        frame.as_slice(),
        include_bytes!("../../../proto/fixtures/epoch-data-v2/envelope.frame")
    );
}

#[test]
fn per_kind_media_size_policy_and_explicit_photo_metadata_presence_are_required() {
    let (mut p, mut e) = fixture();
    for (kind, mime) in [
        (AttachmentKind::File, "application/octet-stream"),
        (AttachmentKind::Photo, "image/jpeg"),
        (AttachmentKind::Photo, "image/png"),
        (AttachmentKind::Photo, "image/gif"),
        (AttachmentKind::Photo, "image/webp"),
        (AttachmentKind::Voice, "audio/ogg"),
        (AttachmentKind::Voice, "audio/webm"),
    ] {
        let a = part(&mut p);
        a.kind = kind as i32;
        a.mime_type = mime.into();
        a.photo_metadata_stripped = (kind == AttachmentKind::Photo).then_some(true);
        let max = if kind == AttachmentKind::File {
            attachment::MAX_PLAINTEXT_BYTES
        } else {
            MAX_MEDIA_BYTES
        };
        size(&mut p, max as u64);
        e.blob.as_mut().unwrap().ciphertext_bytes = (max + attachment::BLOB_OVERHEAD) as u64;
        assert!(check(&p, &e));
        if kind == AttachmentKind::Photo {
            part(&mut p).photo_metadata_stripped = Some(false); // Explicit original.
            assert!(check(&p, &e));
            part(&mut p).photo_metadata_stripped = None;
            assert!(!check(&p, &e));
            part(&mut p).photo_metadata_stripped = Some(true);
        } else {
            part(&mut p).photo_metadata_stripped = Some(false);
            assert!(!check(&p, &e));
            part(&mut p).photo_metadata_stripped = None;
        }
        size(&mut p, max as u64 + 1);
        assert!(!check(&p, &e));
    }
}

#[test]
fn malformed_versions_blob_names_mime_descriptors_and_captions_fail_closed() {
    let (original, e) = fixture();
    let mutations: &[fn(&mut v2::EncryptedPayload)] = &[
        |p| p.format_version = 1,
        |p| p.format_version = 4,
        |p| part(p).format_version = 0,
        |p| part(p).format_version = 2,
        |p| part(p).kind = 0,
        |p| part(p).kind = 99,
        |p| part(p).blob_id = "https://example.invalid/blob".into(),
        |p| part(p).blob_id = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA".into(),
        |p| part(p).filename.clear(),
        |p| part(p).filename = "../secret.txt".into(),
        |p| part(p).filename = "C:\\secret.txt".into(),
        |p| part(p).filename = "space ".into(),
        |p| part(p).filename = "name\u{202e}gpj.exe".into(),
        |p| part(p).filename = "line\nname".into(),
        |p| part(p).filename = "界".repeat(86),
        |p| part(p).mime_type = "application/octet-stream;extra=1".into(),
        |p| part(p).caption = "x".repeat(MAX_TEXT_BYTES + 1),
        |p| part(p).private_descriptor.clear(),
        |p| part(p).private_descriptor.push(0),
        |p| part(p).private_descriptor[0] = 2,
        |p| size(p, 0),
        |p| size(p, u64::MAX),
        |p| p.crypto_epoch = None,
        |p| p.crypto_epoch = Some(0),
    ];
    for change in mutations {
        let mut p = original.clone();
        change(&mut p);
        assert!(!check(&p, &e));
    }
    let mut unicode = original;
    part(&mut unicode).filename = "файл.dat".into();
    part(&mut unicode).caption = "\u{1}".repeat(MAX_TEXT_BYTES);
    assert!(check(&unicode, &e));
}

#[test]
fn private_debug_redacts_descriptor_key_filename_and_caption_through_parent_proto() {
    let (mut p, _) = fixture();
    part(&mut p).filename = "private-name-marker.bin".into();
    part(&mut p).caption = "private-caption-marker".into();
    let debug = format!("{p:?}");
    assert!(debug.contains("private fields redacted"));
    for value in [
        "private-name-marker",
        "private-caption-marker",
        "private_descriptor",
        "17, 17, 17",
    ] {
        assert!(!debug.contains(value));
    }
}

#[test]
fn validated_capsule_opens_only_the_matching_real_blob_and_summary_has_no_secret() {
    let (mut p, mut e) = fixture();
    let a = part(&mut p);
    let blob_id = a.blob_id.clone();
    let binding = AttachmentBinding::new(
        &p.conversation_id,
        p.crypto_epoch.unwrap(),
        &p.message_id,
        &blob_id,
        p.sender.as_ref().unwrap(),
        p.recipient.as_ref().unwrap(),
    )
    .unwrap();
    let prepared =
        attachment::seal(&binding, Zeroizing::new(b"actual test file".to_vec())).unwrap();
    part(&mut p).private_descriptor = prepared.descriptor().encode_for_signal().to_vec();
    e.blob = Some(blob_reference(part(&mut p)).unwrap());
    assert!(check(&p, &e));
    let Some(Content::Attachment(a)) = p.content.take() else {
        panic!("missing attachment")
    };
    let descriptor = AttachmentDescriptor::decode_from_signal(&a.private_descriptor).unwrap();
    assert_eq!(
        attachment::open(&binding, &descriptor, prepared.blob())
            .unwrap()
            .as_slice(),
        b"actual test file"
    );
    let view = summary(a).unwrap();
    assert_eq!(view.filename.as_str(), "public-fixture.bin");
    assert_eq!(view.plaintext_len, 16);
    assert_eq!(view.blob_id, blob_id);
    assert_eq!(view.kind, AttachmentKind::File);
}
