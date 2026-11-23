use super::*;
fn vector() -> (v2::GetFileStatusesRequest, v2::FileStatuses) {
    let request = include_bytes!("../../../proto/fixtures/file-status-v1/request.bin");
    let response = include_bytes!("../../../proto/fixtures/file-status-v1/response.bin");
    let r = v2::GetFileStatusesRequest::decode(request.as_slice()).unwrap();
    let s = v2::FileStatuses::decode(response.as_slice()).unwrap();
    assert_eq!(r.encode_to_vec(), request);
    assert_eq!(s.encode_to_vec(), response);
    (r, s)
}
#[test]
fn file_status_public_vector_keeps_late_ack_expiry_replacement_and_large_epoch() {
    let (mut r, mut s) = vector();
    validate_response(&r, &s).unwrap();
    assert_eq!(r.files[0].binding.as_ref().unwrap().epoch, (1 << 53) + 17);
    assert!(s.files[0].observed_at_unix_ms >= s.files[0].expires_at_unix_ms);
    assert!(s.files[0].acknowledged_at_unix_ms.unwrap() > s.files[0].expires_at_unix_ms);
    assert!(s.files[0].recipient_replaced_at_unix_ms.is_some());
    r.files[0].binding.as_mut().unwrap().epoch = 0;
    s.files[0]
        .reference
        .as_mut()
        .unwrap()
        .binding
        .as_mut()
        .unwrap()
        .epoch = 0;
    s.files[0].receipt.as_mut().unwrap().crypto_epoch = Some(0);
    s.files[0].acknowledged_at_unix_ms = None;
    s.files[0].recipient_replaced_at_unix_ms = None;
    validate_response(&r, &s).unwrap(); // Missing ACK is not a negative proof.
}
#[test]
fn file_status_request_rejects_foreign_sender_duplicate_and_wrong_types() {
    let (original, _) = vector();
    for case in 0..13 {
        let mut r = original.clone();
        match case {
            0 => r.context = None,
            1 => r.files.clear(),
            2 => r.files[0].binding = None,
            3 => r.files[0].blob = None,
            4 => r.files[0].binding.as_mut().unwrap().protocol_version = 2,
            5 => r.files[0].binding.as_mut().unwrap().epoch = u64::MAX,
            6 => r.context.as_mut().unwrap().protocol_version = 1,
            7 => {
                r.context.as_mut().unwrap().device_generation_id = uuid::Uuid::new_v4().to_string()
            }
            8 => r.files.push(r.files[0].clone()),
            9 => {
                let mut reference = r.files[0].clone();
                let b = reference.binding.as_mut().unwrap();
                b.message_id = uuid::Uuid::new_v4().to_string();
                b.sender.as_mut().unwrap().user_id = uuid::Uuid::new_v4().to_string();
                r.files.push(reference);
            }
            10 => r.files[0].blob.as_mut().unwrap().format_version = 2,
            11 => r.files[0].blob.as_mut().unwrap().sha256.clear(),
            _ => {
                r.files[0]
                    .binding
                    .as_mut()
                    .unwrap()
                    .sender
                    .as_mut()
                    .unwrap()
                    .device_id = 2
            }
        }
        assert!(validate_request(&r).is_err(), "request case {case}");
    }
}
#[test]
fn file_status_rejects_mismatched_receipts_versions_overflow_and_impossible_times() {
    let (r, original) = vector();
    for case in 0..16 {
        let mut s = original.clone();
        let f = &mut s.files[0];
        match case {
            0 => f.format_version = 0,
            1 => f.format_version = 2,
            2 => f.reference = None,
            3 => f.reference.as_mut().unwrap().blob.as_mut().unwrap().sha256[0] ^= 1,
            4 => f.receipt = None,
            5 => f.receipt.as_mut().unwrap().crypto_epoch = None,
            6 => *f.receipt.as_mut().unwrap().crypto_epoch.as_mut().unwrap() += 1,
            7 => f.receipt.as_mut().unwrap().envelope_digest[0] ^= 1,
            8 => f.receipt.as_mut().unwrap().message_id = uuid::Uuid::new_v4().to_string(),
            9 => f.receipt.as_mut().unwrap().stored_at_unix_ms = 0,
            10 => f.receipt.as_mut().unwrap().stored_at_unix_ms = i64::MAX,
            11 => f.receipt.as_mut().unwrap().expires_at_unix_ms += 1,
            12 => f.expires_at_unix_ms += 1,
            13 => f.observed_at_unix_ms = f.receipt.as_ref().unwrap().stored_at_unix_ms - 1,
            14 => f.acknowledged_at_unix_ms = Some(0),
            _ => f.recipient_replaced_at_unix_ms = Some(f.observed_at_unix_ms + 1),
        }
        assert!(validate_response(&r, &s).is_err(), "response case {case}");
    }
}
#[test]
fn file_status_bounded_ordered_batch_allows_different_peers_and_clock_free_expiry_boundary() {
    let (mut r, mut s) = vector();
    let reference = r.files.remove(0);
    let status = s.files.remove(0);
    for _ in 0..MAX_FILE_STATUSES {
        let mut reference = reference.clone();
        let b = reference.binding.as_mut().unwrap();
        b.message_id = uuid::Uuid::new_v4().to_string();
        b.recipient.as_mut().unwrap().user_id = uuid::Uuid::new_v4().to_string();
        let mut status = status.clone();
        status.reference = Some(reference.clone());
        status.receipt.as_mut().unwrap().message_id =
            reference.binding.as_ref().unwrap().message_id.clone();
        s.files.push(status);
        r.files.push(reference);
    }
    validate_response(&r, &s).unwrap();
    s.files.swap(0, 1);
    assert!(validate_response(&r, &s).is_err());
    s.files.swap(0, 1);
    r.files.push(reference);
    assert!(validate_request(&r).is_err());
    r.files.pop();
    for f in &mut s.files {
        f.acknowledged_at_unix_ms = None;
        f.recipient_replaced_at_unix_ms = None;
        f.observed_at_unix_ms = f.expires_at_unix_ms - 1;
    }
    validate_response(&r, &s).unwrap();
    for f in &mut s.files {
        f.observed_at_unix_ms += 1;
    }
    validate_response(&r, &s).unwrap(); // Exclusive deadline, no native clock.
    s.files.pop();
    assert!(validate_response(&r, &s).is_err());
}
