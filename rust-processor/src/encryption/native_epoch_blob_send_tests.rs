use super::*;

fn request() -> v2::SendMessageRequest {
    let mut r = crate::encryption::epoch_rpc::tests::send();
    let e = r.envelope.as_mut().unwrap();
    e.protocol_version = 3;
    e.blob = Some(v2::BlobRef {
        format_version: 1,
        blob_id: Uuid::new_v4().to_string(),
        ciphertext_bytes: 64,
        sha256: vec![42; 32],
    });
    r
}
fn status(code: Code, f: &v2::BlobSendFailure) -> tonic::Status {
    tonic::Status::with_details(
        code,
        "private diagnostic must be discarded",
        StatusDetails {
            code: code as i32,
            details: vec![StatusDetail {
                type_url: "type.googleapis.com/zchat.e2ee.v2.BlobSendFailure".into(),
                value: f.encode_to_vec(),
            }],
        }
        .encode_to_vec()
        .into(),
    )
}
#[test]
fn attachment_send_failure_is_compact_exact_bounded_and_method_specific() {
    let r = request();
    for reason in [1, 2] {
        let f = v2::BlobSendFailure {
            reason,
            binding: Some(data::send_binding(r.envelope.as_ref().unwrap()).unwrap()),
        };
        assert!(f.encoded_len() < 512);
        let good = status(Code::FailedPrecondition, &f);
        assert_eq!(failure(&good, &r), Some(f.clone()));
        assert!(sealed_send(&good, &crate::encryption::epoch_rpc::tests::pins(), &r).is_none());
        for fault in 0..12 {
            let mut changed = r.clone();
            let e = changed.envelope.as_mut().unwrap();
            match fault {
                0 => e.ciphertext[0] ^= 1,
                1 => e.message_id = Uuid::new_v4().to_string(),
                2 => e.conversation_id = Uuid::new_v4().to_string(),
                3 => e.crypto_epoch = Some(1),
                4 => e.sender.as_mut().unwrap().user_id = Uuid::new_v4().to_string(),
                5 => e.recipient.as_mut().unwrap().generation_id = Uuid::new_v4().to_string(),
                6 => e.blob.as_mut().unwrap().sha256[0] ^= 1,
                7 => e.blob.as_mut().unwrap().ciphertext_bytes += 1,
                8 => e.blob.as_mut().unwrap().blob_id = Uuid::new_v4().to_string(),
                9 => {
                    changed.context.as_mut().unwrap().device_generation_id =
                        Uuid::new_v4().to_string()
                }
                10 => {
                    e.protocol_version = 2;
                    e.blob = None;
                }
                _ => e.message_type = 99,
            }
            assert!(
                failure(&good, &changed).is_none(),
                "changed request {fault}"
            );
        }
        for fault in 0..7 {
            let mut changed = f.clone();
            match fault {
                0 => changed.reason = 0,
                1 => changed.reason = 99,
                2 => changed.binding = None,
                3 => changed.binding.as_mut().unwrap().protocol_version = 2,
                4 => changed
                    .binding
                    .as_mut()
                    .unwrap()
                    .envelope_digest
                    .truncate(31),
                5 => changed.binding.as_mut().unwrap().epoch += 1,
                _ => changed.binding.as_mut().unwrap().sender = None,
            }
            assert!(failure(&status(Code::FailedPrecondition, &changed), &r).is_none());
        }
        for code in [
            Code::Unknown,
            Code::Unavailable,
            Code::Cancelled,
            Code::DeadlineExceeded,
            Code::Unauthenticated,
            Code::NotFound,
            Code::AlreadyExists,
        ] {
            assert!(failure(&status(code, &f), &r).is_none());
        }
        for fault in 0..5 {
            let mut details = StatusDetails::decode(good.details()).unwrap();
            match fault {
                0 => details.code = Code::Unavailable as i32,
                1 => details.details.clear(),
                2 => details.details.push(StatusDetail {
                    type_url: details.details[0].type_url.clone(),
                    value: details.details[0].value.clone(),
                }),
                3 => {
                    details.details[0].type_url =
                        "type.googleapis.com/zchat.e2ee.v2.BlobFailure".into()
                }
                _ => details.details[0].value = vec![255],
            }
            let bad = tonic::Status::with_details(
                Code::FailedPrecondition,
                "discard",
                details.encode_to_vec().into(),
            );
            assert!(failure(&bad, &r).is_none());
        }
    }
    for bad in [
        tonic::Status::failed_precondition("file expired"),
        tonic::Status::with_details(
            Code::FailedPrecondition,
            "discard",
            vec![0; MAX_RESPONSE_BYTES + 1].into(),
        ),
    ] {
        assert!(failure(&bad, &r).is_none());
    }
}
