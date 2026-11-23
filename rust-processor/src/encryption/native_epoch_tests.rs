use super::*;
use crate::encryption::epoch_rpc::tests::{pins, sealed, send};

fn status_detail(code: Code, name: &str, body: impl Message) -> tonic::Status {
    let details = StatusDetails {
        code: code as i32,
        details: vec![StatusDetail {
            type_url: name.into(),
            value: body.encode_to_vec(),
        }],
    };
    tonic::Status::with_details(
        code,
        "private server diagnostic must be discarded",
        details.encode_to_vec().into(),
    )
}
#[test]
fn only_exact_single_v2_acquire_expiry_detail_can_authorize_renewal() {
    let p = pins();
    let c = p.conversation();
    let r = v2::AcquireBundleRequest {
        context: Some(common::ClientContext {
            protocol_version: 2,
            device_generation_id: c.own_device.as_ref().unwrap().generation_id.clone(),
        }),
        conversation_id: c.conversation_id.clone(),
        acquisition_id: Uuid::new_v4().to_string(),
        expected_peer: c.peer_device.as_ref().unwrap().reference.clone(),
        crypto_epoch: Some(0),
    };
    let name = "type.googleapis.com/zchat.e2ee.v2.AcquisitionExpiredFailure";
    let f = v2::AcquisitionExpiredFailure {
        request: Some(r.clone()),
    };
    assert_eq!(
        expired_acquisition(
            &status_detail(Code::FailedPrecondition, name, f.clone()),
            &r
        ),
        Some(f.clone())
    );
    for code in [
        Code::Unavailable,
        Code::NotFound,
        Code::DeadlineExceeded,
        Code::Cancelled,
        Code::Aborted,
        Code::Unauthenticated,
    ] {
        assert!(expired_acquisition(&status_detail(code, name, f.clone()), &r).is_none());
    }
    for fault in 0..6 {
        let mut f = f.clone();
        match fault {
            0 => f.request = None,
            1 => f.request.as_mut().unwrap().acquisition_id = Uuid::new_v4().to_string(),
            2 => f.request.as_mut().unwrap().crypto_epoch = None,
            3 => f.request.as_mut().unwrap().crypto_epoch = Some(1),
            4 => {
                f.request
                    .as_mut()
                    .unwrap()
                    .context
                    .as_mut()
                    .unwrap()
                    .protocol_version = 1
            }
            _ => {
                f.request
                    .as_mut()
                    .unwrap()
                    .expected_peer
                    .as_mut()
                    .unwrap()
                    .generation_id = Uuid::new_v4().to_string()
            }
        }
        assert!(
            expired_acquisition(&status_detail(Code::FailedPrecondition, name, f), &r).is_none()
        );
    }
    for s in [
        tonic::Status::failed_precondition("expired"),
        status_detail(
            Code::FailedPrecondition,
            "type.googleapis.com/zchat.e2ee.v1.Failure",
            common::Failure {
                reason: common::FailureReason::AcquisitionExpired as i32,
            },
        ),
        status_detail(Code::FailedPrecondition, "wrong.Type", f.clone()),
    ] {
        assert!(expired_acquisition(&s, &r).is_none());
    }
    for fields in [
        vec![],
        (0..2)
            .map(|_| StatusDetail {
                type_url: name.into(),
                value: f.encode_to_vec(),
            })
            .collect(),
    ] {
        let s = tonic::Status::with_details(
            Code::FailedPrecondition,
            "discard",
            StatusDetails {
                code: Code::FailedPrecondition as i32,
                details: fields,
            }
            .encode_to_vec()
            .into(),
        );
        assert!(expired_acquisition(&s, &r).is_none());
    }
    for bytes in [vec![255], vec![0; MAX_RESPONSE_BYTES + 1]] {
        assert!(
            expired_acquisition(
                &tonic::Status::with_details(Code::FailedPrecondition, "discard", bytes.into()),
                &r
            )
            .is_none()
        );
    }
}
#[test]
fn sealed_rpc_detail_is_accepted_only_for_exact_send_and_signature_checked_transition() {
    let p = pins();
    let r = send();
    let f = sealed();
    let name = "type.googleapis.com/zchat.e2ee.v2.EpochSealedFailure";
    let s = status_detail(Code::FailedPrecondition, name, f.clone());
    assert_eq!(sealed_send(&s, &p, &r), Some(f.clone()));
    let mut wrong = r.clone();
    wrong.envelope.as_mut().unwrap().ciphertext.push(1);
    assert!(sealed_send(&s, &p, &wrong).is_none());
    for code in [
        Code::Aborted,
        Code::Unavailable,
        Code::Cancelled,
        Code::DeadlineExceeded,
        Code::NotFound,
    ] {
        assert!(sealed_send(&status_detail(code, name, f.clone()), &p, &r).is_none());
    }
    for change in 0..4 {
        let mut f = f.clone();
        match change {
            0 => {
                f.recovery
                    .as_mut()
                    .unwrap()
                    .decision
                    .as_mut()
                    .unwrap()
                    .signature[0] ^= 1
            }
            1 => f.recovery = None,
            2 => f.fence.as_mut().unwrap().binding.as_mut().unwrap().epoch += 1,
            _ => f.fence = None,
        };
        assert!(sealed_send(&status_detail(Code::FailedPrecondition, name, f), &p, &r).is_none());
    }
    let detail = StatusDetail {
        type_url: name.into(),
        value: f.encode_to_vec(),
    };
    for details in [
        StatusDetails {
            code: Code::Aborted as i32,
            details: vec![detail],
        },
        StatusDetails {
            code: Code::FailedPrecondition as i32,
            details: vec![
                StatusDetail {
                    type_url: name.into(),
                    value: f.encode_to_vec(),
                },
                StatusDetail {
                    type_url: name.into(),
                    value: f.encode_to_vec(),
                },
            ],
        },
        StatusDetails {
            code: Code::FailedPrecondition as i32,
            details: vec![],
        },
    ] {
        let s = tonic::Status::with_details(
            Code::FailedPrecondition,
            "discard",
            details.encode_to_vec().into(),
        );
        assert!(sealed_send(&s, &p, &r).is_none());
    }
    for bytes in [vec![0xff], vec![0; MAX_RESPONSE_BYTES + 1]] {
        let s = tonic::Status::with_details(Code::FailedPrecondition, "discard", bytes.into());
        assert!(sealed_send(&s, &p, &r).is_none());
    }
    assert!(
        sealed_send(
            &status_detail(Code::FailedPrecondition, "wrong.Type", f),
            &p,
            &r
        )
        .is_none()
    );
}
#[test]
fn recovery_conflicts_are_advisory_and_malformed_details_keep_only_generic_code() {
    let name = "type.googleapis.com/zchat.e2ee.v2.RecoveryConflict";
    let good = v2::RecoveryConflict {
        active_epoch: Some(0),
        epoch_revision: Some(0),
        pending_proposal_id: Uuid::new_v4().to_string(),
    };
    let err = EpochRecoveryError::from_status(status_detail(Code::Aborted, name, good.clone()));
    let EpochRecoveryError::Conflict(c) = err else {
        panic!("missing advisory conflict")
    };
    assert_eq!(c.active_epoch(), 0);
    assert_eq!(
        c.pending_proposal(),
        Some(good.pending_proposal_id.as_str())
    );
    for change in 0..5 {
        let mut bad = good.clone();
        match change {
            0 => bad.active_epoch = None,
            1 => bad.epoch_revision = None,
            2 => bad.active_epoch = Some(u64::MAX),
            3 => bad.pending_proposal_id = "not uuid".into(),
            _ => bad.epoch_revision = Some(u64::MAX),
        };
        let err = EpochRecoveryError::from_status(status_detail(Code::Aborted, name, bad));
        assert_eq!(
            err,
            EpochRecoveryError::Rpc(RpcError::Status(Code::Aborted))
        );
        assert!(!format!("{err:?} {err}").contains("private server"));
    }
    assert_eq!(
        EpochRecoveryError::from_status(status_detail(Code::FailedPrecondition, name, good)),
        EpochRecoveryError::Rpc(RpcError::Status(Code::FailedPrecondition))
    );
    let err =
        EpochRecoveryError::from_status(tonic::Status::unavailable("secret server diagnostic"));
    assert_eq!(
        err,
        EpochRecoveryError::Rpc(RpcError::Status(Code::Unavailable))
    );
    assert!(!format!("{err:?} {err}").contains("secret"));
}
#[tokio::test]
async fn v2_inherits_bound_registry_channel_scope_credentials_and_request_deadline() {
    let p = pins();
    let channel = Endpoint::from_static("http://127.0.0.1:1").connect_lazy();
    let mut registry = NativeV1Client {
        client: MessengerClient::new(channel.clone()),
        channel,
        account: p
            .conversation()
            .own_device
            .as_ref()
            .unwrap()
            .user_id
            .clone(),
        jwt: Zeroizing::new("synthetic.jwt.token".into()),
        credential: Some(Zeroizing::new([42; 32])),
        generation: None,
        store_scope: None,
    };
    assert!(registry.epoch_transport().is_err());
    registry
        .bind_generation(p.conversation().own_device.as_ref().unwrap())
        .unwrap();
    assert!(registry.epoch_transport().is_err());
    registry.bind_store_scope(StoreScope::new("https://epoch.test", &registry.account).unwrap());
    let epoch = registry.epoch_transport().unwrap();
    assert_eq!(epoch.context().protocol_version, 2);
    assert_eq!(registry.context().unwrap().protocol_version, 1);
    assert!(&epoch.scope == registry.store_scope.as_ref().unwrap());
    let req = epoch
        .request(v2::GetPendingMessagesRequest {
            context: Some(epoch.context()),
        })
        .unwrap();
    assert_eq!(
        req.metadata().get("authorization").unwrap(),
        "Bearer synthetic.jwt.token"
    );
    assert!(req.metadata().get("authorization").unwrap().is_sensitive());
    assert_eq!(
        req.metadata()
            .get_bin("x-zchat-device-credential-bin")
            .unwrap()
            .to_bytes()
            .unwrap()
            .as_ref(),
        &[42; 32]
    );
    assert!(req.metadata().get("grpc-timeout").is_some());
    let mut context = epoch.context();
    context.protocol_version = 1;
    assert!(epoch.check_context(Some(&context)).is_err());
    registry.credential = None;
    assert!(registry.epoch_transport().is_err());
    // No network request/file creation occurred; no new server/account/CA input.
}

#[cfg(test)]
#[path = "native_epoch_interop_tests.rs"]
mod interop;
