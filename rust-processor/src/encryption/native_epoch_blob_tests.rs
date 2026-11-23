use super::*;
use crate::encryption::epoch_rpc::tests::pins;
fn request() -> v2::BlobLeaseRequest {
    let pins = pins();
    let c = pins.conversation();
    v2::BlobLeaseRequest {
        context: Some(common::ClientContext {
            protocol_version: 2,
            device_generation_id: c.own_device.as_ref().unwrap().generation_id.clone(),
        }),
        message_id: Uuid::new_v4().to_string(),
        conversation_id: c.conversation_id.clone(),
        crypto_epoch: Some(0),
        sender: c.own_device.clone(),
        recipient: c.peer_device.as_ref().unwrap().reference.clone(),
        blob: Some(v2::BlobRef {
            format_version: 1,
            blob_id: Uuid::new_v4().to_string(),
            ciphertext_bytes: 64,
            sha256: vec![42; 32],
        }),
        operation_id: Uuid::new_v4().to_string(),
        expected_revision: 0,
        expected_attempt_id: String::new(),
    }
}
fn status(code: Code, f: v2::BlobFailure) -> tonic::Status {
    tonic::Status::with_details(
        code,
        "private diagnostic",
        StatusDetails {
            code: code as i32,
            details: vec![StatusDetail {
                type_url: "type.googleapis.com/zchat.e2ee.v2.BlobFailure".into(),
                value: f.encode_to_vec(),
            }],
        }
        .encode_to_vec()
        .into(),
    )
}
#[test]
fn blob_request_lease_and_error_details_are_exact_bounded_and_method_specific() {
    let r = request();
    blob::validate_request(&r).unwrap();
    for mutate in 0..10 {
        let mut bad = r.clone();
        match mutate {
            0 => bad.context = None,
            1 => bad.crypto_epoch = None,
            2 => bad.sender = None,
            3 => bad.recipient = None,
            4 => bad.blob = None,
            5 => bad.operation_id = "invalid".into(),
            6 => bad.expected_revision = u64::MAX,
            7 => bad.context.as_mut().unwrap().protocol_version = 3,
            8 => bad.blob.as_mut().unwrap().sha256.pop().map(|_| ()).unwrap(),
            _ => bad.expected_revision = 1,
        };
        assert!(blob::validate_request(&bad).is_err());
    }
    let g = v2::BlobLease {
        message_id: r.message_id.clone(),
        operation_id: r.operation_id.clone(),
        attempt_id: Uuid::new_v4().to_string(),
        revision: 1,
        reserved_at_unix_ms: 1000,
        expires_at_unix_ms: 3_601_000,
        observed_at_unix_ms: 1000,
        state: v2::BlobLeaseState::Reserved as i32,
        expired: false,
    };
    for state in [1, 2, 3, 4, 5, 6] {
        let mut g = g.clone();
        g.state = state;
        blob::validate_lease(&r, &g).unwrap();
    }
    for mutate in 0..9 {
        let mut bad = g.clone();
        match mutate {
            0 => bad.message_id = Uuid::new_v4().to_string(),
            1 => bad.operation_id = Uuid::new_v4().to_string(),
            2 => bad.attempt_id = "../bad".into(),
            3 => bad.revision = 2,
            4 => bad.expires_at_unix_ms += 1,
            5 => bad.observed_at_unix_ms = 999,
            6 => bad.expired = true,
            7 => bad.state = 0,
            _ => bad.state = 99,
        };
        assert!(blob::validate_lease(&r, &bad).is_err());
    }
    let mut expired = g.clone();
    expired.observed_at_unix_ms = expired.expires_at_unix_ms;
    assert!(blob::validate_lease(&r, &expired).is_err());
    expired.expired = true;
    blob::validate_lease(&r, &expired).unwrap();
    for reason in 1..=6 {
        let f = v2::BlobFailure {
            reason,
            request: Some(r.clone()),
        };
        let code = blob::failure_code(reason, true).unwrap();
        assert_eq!(
            blob_failure(&status(code, f.clone()), &r, true),
            Some(f.clone())
        );
        assert!(blob_failure(&status(Code::Unavailable, f.clone()), &r, true).is_none());
        let mut wrong = r.clone();
        wrong.blob.as_mut().unwrap().sha256[0] += 1;
        assert!(blob_failure(&status(code, f.clone()), &wrong, true).is_none());
        if reason == 1 {
            assert!(blob_failure(&status(code, f), &r, false).is_none());
        }
    }
    for reason in [0, 77] {
        assert!(
            blob_failure(
                &status(
                    Code::NotFound,
                    v2::BlobFailure {
                        reason,
                        request: Some(r.clone())
                    }
                ),
                &r,
                true
            )
            .is_none()
        );
    }
    for s in [
        tonic::Status::not_found("missing"),
        tonic::Status::with_details(Code::NotFound, "private", vec![255].into()),
        tonic::Status::with_details(
            Code::NotFound,
            "private",
            vec![0; MAX_RESPONSE_BYTES + 1].into(),
        ),
    ] {
        assert!(blob_failure(&s, &r, true).is_none());
    }
}

#[tokio::test]
async fn blob_client_rejects_foreign_scope_account_and_context_before_network() {
    let r = request();
    let scope = StoreScope::new(
        "https://chat.example.test",
        &r.sender.as_ref().unwrap().user_id,
    )
    .unwrap();
    let plan = EpochBlobControlPlan::test_plan(scope.clone(), r.clone());
    let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let mut client = NativeEpochClient {
        client: v2::messenger_client::MessengerClient::new(channel),
        own: r.recipient.clone().unwrap(),
        scope: scope.clone(),
        jwt: Zeroizing::new("synthetic.jwt".into()),
        credential: Zeroizing::new([42; 32]),
    };
    assert!(matches!(
        client.reserve_blob(&plan).await,
        Err(RpcError::Invalid)
    ));
    client.own = r.sender.clone().unwrap();
    client.scope = StoreScope::new("https://other.example.test", &client.own.user_id).unwrap();
    assert!(matches!(
        client.observe_blob(&plan).await,
        Err(RpcError::Invalid)
    ));
    client.scope = scope;
    let mut bad = r;
    bad.context.as_mut().unwrap().device_generation_id = Uuid::new_v4().to_string();
    let bad = EpochBlobControlPlan::test_plan(client.scope.clone(), bad);
    assert!(matches!(
        client.reserve_blob(&bad).await,
        Err(RpcError::Invalid)
    ));
}

// Metadata-only interoperability. The Go fixture supplies synthetic public
// request/auth through stdin. This test-only plan is NOT a protected-store proof.
#[test]
#[ignore = "requires the isolated Go blob-control TLS/PostgreSQL fixture"]
fn go_blob_control_worker() {
    use std::io::Read;
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(16384)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input.lines().collect();
    assert_eq!(fields.len(), 5);
    let ca = std::fs::read(fields[1]).unwrap();
    let request =
        v2::BlobLeaseRequest::decode(std::fs::read(fields[2]).unwrap().as_slice()).unwrap();
    assert_eq!(fields[4].len(), 64);
    let mut credential = [0u8; 32];
    for (i, b) in credential.iter_mut().enumerate() {
        *b = u8::from_str_radix(&fields[4][i * 2..i * 2 + 2], 16).unwrap();
    }
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let own = request.sender.clone().unwrap();
            let scope = StoreScope::new("https://blob-controls.test", &own.user_id).unwrap();
            let mut registry = NativeV1Client::connect(
                NativeEndpoint::new(fields[0], Some(&ca)).unwrap(),
                &own.user_id,
                Zeroizing::new(fields[3].into()),
                Zeroizing::new(credential),
            )
            .await
            .unwrap();
            registry.bind_generation(&own).unwrap();
            registry.bind_store_scope(scope.clone());
            let mut client = registry.epoch_transport().unwrap();
            let plan = EpochBlobControlPlan::test_plan(scope.clone(), request.clone());
            let EpochBlobControlOutcome::Rejected(missing) =
                client.observe_blob(&plan).await.unwrap()
            else {
                panic!("expected exact missing observation")
            };
            assert_eq!(
                missing.response().reason,
                v2::BlobFailureReason::OperationNotObserved as i32
            );
            // Go deliberately loses the result only AFTER the real Reserve COMMIT.
            assert!(matches!(
                client.reserve_blob(&plan).await,
                Err(RpcError::Status(Code::Unavailable))
            ));
            let EpochBlobControlOutcome::Lease(found) = client.observe_blob(&plan).await.unwrap()
            else {
                panic!("committed operation lost")
            };
            let EpochBlobControlOutcome::Lease(replay) = client.reserve_blob(&plan).await.unwrap()
            else {
                panic!("exact replay rejected")
            };
            assert_eq!(replay.response().attempt_id, found.response().attempt_id);
            assert_eq!(
                replay.response().expires_at_unix_ms,
                found.response().expires_at_unix_ms
            );
            let mut changed = request.clone();
            changed.blob.as_mut().unwrap().sha256[0] ^= 1;
            let changed = EpochBlobControlPlan::test_plan(scope.clone(), changed);
            let EpochBlobControlOutcome::Rejected(conflict) =
                client.observe_blob(&changed).await.unwrap()
            else {
                panic!("wrong reference accepted")
            };
            assert_eq!(
                conflict.response().reason,
                v2::BlobFailureReason::BindingConflict as i32
            );
            let mut renew = request;
            renew.operation_id = Uuid::new_v4().to_string();
            renew.expected_revision = found.response().revision;
            renew.expected_attempt_id = found.response().attempt_id.clone();
            let renew = EpochBlobControlPlan::test_plan(scope, renew);
            let EpochBlobControlOutcome::Rejected(active) =
                client.reserve_blob(&renew).await.unwrap()
            else {
                panic!("live lease renewed")
            };
            assert_eq!(
                active.response().reason,
                v2::BlobFailureReason::ActiveLease as i32
            );
            let caps = client.capabilities().await.unwrap();
            assert!(!caps.messaging_available && !caps.recovery_available);
        });
    use zeroize::Zeroize;
    credential.zeroize();
    println!("zchat-native-blob-controls-complete:metadata-only");
}
