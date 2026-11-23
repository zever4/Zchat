use prost::Message;
use rust_processor::encryption::contract::MAX_REQUEST_BYTES;
use rust_processor::encryption::publication::{
    prepare_upload_batches, publication_digest, validate_upload, validate_upload_receipt,
};
use rust_processor::proto::e2ee::{UploadPreKeysRequest, UploadReceipt};
use std::collections::HashSet;

fn fixture() -> UploadPreKeysRequest {
    UploadPreKeysRequest::decode(
        include_bytes!("../../proto/fixtures/e2ee-v1-publication.bin").as_slice(),
    )
    .unwrap()
}

#[test]
fn publication_digest_matches_go_and_ignores_wire_field_order() {
    let req = fixture();
    let digest = publication_digest(&req).unwrap();
    let expected = [
        0x18, 0x32, 0xc4, 0xd7, 0x51, 0x17, 0xaf, 0x6a, 0xd4, 0x03, 0xfd, 0xe3, 0x32, 0x00, 0xa5,
        0x7e, 0xc5, 0xcb, 0x47, 0xc2, 0xc2, 0x66, 0x7d, 0x59, 0xed, 0xb3, 0x73, 0x08, 0x0f, 0x1c,
        0xb6, 0xe1,
    ];
    assert_eq!(digest, expected);
    let wire = req.encode_to_vec();
    assert_eq!(wire[0], 10); // First field is the short length-delimited context.
    let end = 2 + usize::from(wire[1]);
    let mut reordered = wire[end..].to_vec();
    reordered.extend_from_slice(&wire[..end]);
    let decoded = UploadPreKeysRequest::decode(reordered.as_slice()).unwrap();
    assert_eq!(publication_digest(&decoded).unwrap(), digest);
    let mut changed = req.clone();
    changed.expected_revision += 1;
    assert_ne!(publication_digest(&changed).unwrap(), digest);
    changed = req.clone();
    changed.kyber_pre_keys.swap(0, 1);
    assert_ne!(publication_digest(&changed).unwrap(), digest);
    changed = req.clone();
    changed.signed_pre_key.as_mut().unwrap().signature[1] ^= 1;
    assert_ne!(publication_digest(&changed).unwrap(), digest);
}

#[test]
fn publication_plan_fits_size_and_preserves_all_public_material() {
    let req = fixture();
    let ec = (0..201)
        .map(|id| {
            let mut k = req.one_time_pre_keys[0].clone();
            k.key_id = id;
            k
        })
        .collect::<Vec<_>>();
    let mut pq = (0..101)
        .map(|id| {
            let mut k = req.kyber_pre_keys[0].clone();
            k.key_id = id;
            k
        })
        .collect::<Vec<_>>();
    pq.push(req.kyber_pre_keys[1].clone());
    let plan = prepare_upload_batches(
        req.context.clone().unwrap(),
        7,
        req.signed_pre_key.clone(),
        ec.clone(),
        pq.clone(),
    )
    .unwrap();
    assert!(plan.len() > 3);
    let mut ids = HashSet::new();
    let mut received_ec = Vec::new();
    let mut received_pq = Vec::new();
    for (i, batch) in plan.iter().enumerate() {
        validate_upload(batch).unwrap();
        assert!(batch.encoded_len() <= MAX_REQUEST_BYTES);
        assert_eq!(batch.expected_revision, 7 + i as u64);
        assert!(ids.insert(&batch.batch_id));
        assert_eq!(batch.signed_pre_key.is_some(), i == 0);
        received_ec.extend(batch.one_time_pre_keys.clone());
        received_pq.extend(batch.kyber_pre_keys.clone());
        let receipt = UploadReceipt {
            batch_id: batch.batch_id.clone(),
            batch_digest: publication_digest(batch).unwrap().to_vec(),
            publication_revision: batch.expected_revision + 1,
        };
        validate_upload_receipt(batch, &receipt).unwrap();
        // Serializing/restoring the plan preserves exact retries and their digests.
        let restored = UploadPreKeysRequest::decode(batch.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            publication_digest(&restored).unwrap(),
            publication_digest(batch).unwrap()
        );
    }
    assert_eq!(received_ec, ec);
    received_pq.sort_by_key(|k| k.key_id);
    pq.sort_by_key(|k| k.key_id);
    assert_eq!(received_pq, pq);
    assert_eq!(
        plan.iter()
            .flat_map(|b| &b.kyber_pre_keys)
            .filter(|k| k.is_last_resort)
            .count(),
        1
    );
}

#[test]
fn malformed_publications_and_mismatched_receipts_are_rejected() {
    let req = fixture();
    let good = UploadReceipt {
        batch_id: req.batch_id.clone(),
        batch_digest: publication_digest(&req).unwrap().to_vec(),
        publication_revision: req.expected_revision + 1,
    };
    for i in 0..3 {
        let mut receipt = good.clone();
        match i {
            0 => receipt.batch_id = "88888888-8888-4888-8888-888888888888".into(),
            1 => receipt.batch_digest[0] ^= 1,
            _ => receipt.publication_revision += 1,
        };
        assert!(validate_upload_receipt(&req, &receipt).is_err());
    }
    for i in 0..8 {
        let mut changed = req.clone();
        match i {
            0 => changed.expected_revision = i64::MAX as u64,
            1 => changed.signed_pre_key.as_mut().unwrap().created_at_unix_ms = -1,
            2 => changed
                .one_time_pre_keys
                .push(changed.one_time_pre_keys[0].clone()),
            3 => changed.kyber_pre_keys[0].is_last_resort = true,
            4 => changed.kyber_pre_keys[0].public_key[0] = 7,
            5 => changed.context.as_mut().unwrap().protocol_version = 2,
            6 => changed.kyber_pre_keys[0]
                .signature
                .pop()
                .map(|_| ())
                .unwrap(),
            _ => {
                changed.signed_pre_key = None;
                changed.one_time_pre_keys.clear();
                changed.kyber_pre_keys.clear();
            }
        }
        assert!(validate_upload(&changed).is_err());
    }
    let mut many = req.clone();
    many.one_time_pre_keys.clear();
    many.kyber_pre_keys = (0..100)
        .map(|id| {
            let mut k = req.kyber_pre_keys[0].clone();
            k.key_id = id;
            k
        })
        .collect();
    assert!(many.encoded_len() > MAX_REQUEST_BYTES);
    assert!(validate_upload(&many).is_err());
    let pq = vec![req.kyber_pre_keys[0].clone(); 4097];
    assert!(prepare_upload_batches(req.context.clone().unwrap(), 0, None, Vec::new(), pq).is_err());
    let pq = (0..100)
        .map(|id| {
            let mut k = req.kyber_pre_keys[0].clone();
            k.key_id = id;
            k
        })
        .collect();
    assert!(
        prepare_upload_batches(
            req.context.unwrap(),
            i64::MAX as u64 - 1,
            None,
            Vec::new(),
            pq
        )
        .is_err()
    );
}
