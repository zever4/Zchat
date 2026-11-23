use super::super::{
    discovery::{DiscoveryError, TrustedServer},
    protected::{ProtectedStore, StoreScope},
    provisioning::{DeviceProvisioning, Reconciliation},
    signal_state::{CryptoCoordinator, SessionScope},
};
use super::*;
use crate::proto::e2ee::{EncryptedPayload, TextMessage, encrypted_payload};
use std::io::Read;

#[test]
fn replacement_request_debug_never_exposes_password() {
    let mut request = ReplaceOwnDeviceRequest::default();
    request.current_password = "synthetic password must be redacted".into();
    assert_eq!(
        format!("{request:?}"),
        "ReplaceOwnDeviceRequest([redacted])"
    );
    assert_eq!(
        format!("{:?}", request.clone()),
        "ReplaceOwnDeviceRequest([redacted])"
    );
}

#[test]
fn persistence_receipt_binds_exact_envelope_and_fixed_deadline_including_expired_replay() {
    use super::super::contract::{MESSAGE_RETENTION_MS, envelope_digest};
    use crate::proto::e2ee::Envelope;
    let envelope =
        Envelope::decode(include_bytes!("../../../proto/fixtures/e2ee-v1-envelope.bin").as_slice())
            .unwrap();
    let request = SendMessageRequest {
        context: Some(ClientContext {
            protocol_version: 1,
            device_generation_id: envelope.sender.as_ref().unwrap().generation_id.clone(),
        }),
        envelope: Some(envelope.clone()),
    };
    let receipt = SendReceipt {
        message_id: envelope.message_id.clone(),
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
    };
    // Old absolute deadline is intentionally valid: it is evidence, not a new lease.
    assert!(validate_send_receipt(&request, &receipt).is_ok());
    for change in 0..6 {
        let mut bad = receipt.clone();
        match change {
            0 => bad.message_id = Uuid::new_v4().to_string(),
            1 => bad.envelope_digest[0] ^= 1,
            2 => bad.envelope_digest.clear(),
            3 => bad.stored_at_unix_ms = 0,
            4 => bad.expires_at_unix_ms += 1,
            _ => bad.stored_at_unix_ms = i64::MAX,
        }
        assert!(validate_send_receipt(&request, &bad).is_err());
    }
    let mut bad = request.clone();
    bad.envelope.as_mut().unwrap().ciphertext.push(4);
    assert!(validate_send_receipt(&bad, &receipt).is_err());
    bad = request;
    bad.context.as_mut().unwrap().device_generation_id = Uuid::new_v4().to_string();
    assert!(validate_send(&bad).is_err());
}

// Production opaque proofs, real protected worker COMMIT, lost ACK reply recovery.
async fn receive_and_ack(
    api: &mut NativeV1Client,
    worker: &CryptoCoordinator,
    scope: SessionScope,
    expected: &EncryptedPayload,
) -> DurableAck {
    let page = api.pending().await.unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page, api.pending().await.unwrap());
    let (payload, proof) = worker
        .receive_delivery(scope, page.messages[0].clone())
        .await
        .unwrap();
    assert_eq!(payload, *expected);
    assert_eq!(page, api.pending().await.unwrap());
    drop(api.acknowledge(&proof).await.unwrap()); // Remote COMMIT, local receipt lost.
    let mut recovered = worker
        .pending_acknowledgements(None)
        .await
        .unwrap()
        .acknowledgements;
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].request(), proof.request());
    let verified = api.acknowledge(&recovered.remove(0)).await.unwrap();
    worker.mark_acknowledged(verified).await.unwrap();
    assert!(
        worker
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert!(api.pending().await.unwrap().messages.is_empty());
    proof
}

async fn assert_stored(
    api: &mut NativeV1Client,
    worker: &CryptoCoordinator,
    receipt: &SendReceipt,
) {
    use crate::proto::e2ee::MessageState;
    let plan = worker
        .outgoing_plan(receipt.message_id.clone())
        .await
        .unwrap();
    let verified = api.refresh_outgoing(&[plan]).await.unwrap();
    let progress = worker.accept_message_statuses(verified).await.unwrap();
    assert_eq!(progress[0].receipt(), Some(receipt));
    assert_eq!(
        progress[0].status().unwrap().state,
        MessageState::RecipientStored as i32
    );
}

fn reason_status(code: Code, reason: i32) -> tonic::Status {
    let detail = StatusDetails {
        code: code as i32,
        details: vec![StatusDetail {
            type_url: "type.googleapis.com/zchat.e2ee.v1.Failure".into(),
            value: Failure { reason }.encode_to_vec(),
        }],
    };
    tonic::Status::with_details(
        code,
        "server diagnostic must not be retained",
        detail.encode_to_vec().into(),
    )
}

#[test]
fn typed_rpc_failures_preserve_code_and_reason_without_server_diagnostics() {
    for (code, reason) in [
        (Code::Unauthenticated, FailureReason::SessionInvalid),
        (
            Code::Unauthenticated,
            FailureReason::DeviceCredentialInvalid,
        ),
        (Code::Unauthenticated, FailureReason::PasswordInvalid),
        (Code::FailedPrecondition, FailureReason::GenerationChanged),
        (Code::FailedPrecondition, FailureReason::AcquisitionExpired),
    ] {
        let error = RpcError::from_status(reason_status(code, reason as i32));
        assert_eq!(error, RpcError::Failure { code, reason });
        assert!(!format!("{error:?} {error}").contains("server diagnostic"));
    }
}

#[test]
fn unknown_malformed_and_inconsistent_details_retain_only_generic_code() {
    let code = Code::Unauthenticated;
    for status in [
        tonic::Status::new(code, "old server"),
        reason_status(code, 0),
        reason_status(code, 99),
        reason_status(code, FailureReason::AcquisitionExpired as i32),
        tonic::Status::with_details(code, "bad", vec![255].into()),
        tonic::Status::with_details(code, "oversized", vec![0; 2049].into()),
    ] {
        assert_eq!(RpcError::from_status(status), RpcError::Status(code));
    }
    for mutation in 0..4 {
        let valid = reason_status(code, FailureReason::SessionInvalid as i32);
        let mut details = StatusDetails::decode(valid.details()).unwrap();
        match mutation {
            0 => details.code = Code::FailedPrecondition as i32,
            1 => details.details[0].type_url = "type.googleapis.com/other.Failure".into(),
            2 => details.details[0].value = vec![255],
            _ => details.details.push(StatusDetail {
                type_url: "other".into(),
                value: vec![],
            }),
        }
        let status = tonic::Status::with_details(code, "bad", details.encode_to_vec().into());
        assert_eq!(RpcError::from_status(status), RpcError::Status(code));
    }
}

pub(super) fn local_store(
    root: &std::path::Path,
    scope: StoreScope,
    create: bool,
) -> ProtectedStore {
    #[cfg(windows)]
    {
        if create {
            ProtectedStore::create(root, scope)
        } else {
            ProtectedStore::open(root, scope)
        }
        .unwrap()
    }
    #[cfg(not(windows))]
    {
        super::super::protected::test_open(root, scope, create)
    }
}

#[test]
fn endpoints_require_tls_or_loopback_and_reject_credential_urls() {
    for address in [
        "http://example.test",
        "http://localhost.evil.test",
        "https://u:p@example.test",
        "https://example.test/?token=secret",
        "https://example.test/#token",
        "https://example.test/rpc",
        "file:///tmp/a",
    ] {
        assert!(NativeEndpoint::new(address, None).is_err(), "{address}");
    }
    for address in [
        "https://example.test",
        "http://localhost:50052",
        "http://127.0.0.1:50052",
        "http://[::1]:50052",
    ] {
        assert!(NativeEndpoint::new(address, None).is_ok());
    }
    assert!(NativeEndpoint::new("http://localhost", Some(b"CA")).is_err());
}

#[tokio::test]
async fn invalid_credentials_fail_before_connecting() {
    assert!(matches!(
        NativeV1Client::connect(
            NativeEndpoint::new("http://127.0.0.1:1", None).unwrap(),
            "not-an-account",
            Zeroizing::new("token".into()),
            Zeroizing::new([1; 32])
        )
        .await,
        Err(RpcError::Invalid)
    ));
    assert!(matches!(
        NativeV1Client::connect(
            NativeEndpoint::new("http://127.0.0.1:1", None).unwrap(),
            "11111111-1111-4111-8111-111111111111",
            Zeroizing::new("token\r\ninjected".into()),
            Zeroizing::new([1; 32])
        )
        .await,
        Err(RpcError::Invalid)
    ));
}

/// Invoked explicitly by Go's opt-in PostgreSQL integration test. Secrets arrive
/// on stdin, never command arguments/environment or persisted fixture files.
/// Explicitly ignored in ordinary Rust runs; only the Go harness supplies input.
#[test]
#[ignore = "requires the Go TLS/PostgreSQL interoperability fixture"]
fn go_interop_worker() {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(32768)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input.lines().collect();
    assert_eq!(fields.len(), 8);
    let ca = std::fs::read(fields[1]).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let root = tempfile::tempdir().unwrap();
        let mut stores = vec![];
        let mut clients = vec![];
        let mut devices = vec![];
        let mut bob_batch = None;
        let mut scopes = vec![];
        let origin = "https://interop.example.test";
        for (index, token) in [fields[2], fields[3]].into_iter().enumerate() {
            let config = || TrustedServer::new(origin, fields[0], Some(&ca)).unwrap();
            let mut discovered = config()
                .discover(origin, Zeroizing::new(token.into()))
                .await
                .unwrap();
            assert!(discovered.state().active_device.is_none());
            assert!(discovered.device_client().is_err());
            let scope = discovered.scope();
            let mut provisioning =
                DeviceProvisioning::new(local_store(root.path(), scope.clone(), true));
            assert_eq!(
                discovered.reconcile(&mut provisioning).unwrap(),
                Reconciliation::Unregistered
            );
            // Wrong server with the same account UUID must fail before any RPC or mutation.
            let foreign_scope =
                StoreScope::new("https://foreign.example.test", &discovered.state().user_id).unwrap();
            let mut foreign =
                DeviceProvisioning::new(local_store(root.path(), foreign_scope.clone(), true));
            let before = std::fs::read(foreign_scope.database_path(root.path())).unwrap();
            assert!(matches!(
                discovered.resume_initial(&mut foreign).await,
                Err(DiscoveryError::Scope)
            ));
            assert_eq!(
                std::fs::read(foreign_scope.database_path(root.path())).unwrap(),
                before
            );
            drop(foreign);
            let attempt = provisioning.prepare_registration().unwrap();
            // Deliberately interrupt after a real server COMMIT. No account UUID
            // came from the fixture: even this low-level probe uses discovery.
            let mut probe = NativeV1Client::connect(
                NativeEndpoint::new(fields[0], Some(&ca)).unwrap(),
                &discovered.state().user_id,
                Zeroizing::new(token.into()),
                Zeroizing::new(*attempt.credential()),
            )
            .await
            .unwrap();
            let capabilities = probe.capabilities().await.unwrap();
            assert!(capabilities.device_registration_available && !capabilities.messaging_available);
            let registered = probe.register(&attempt.request).await.unwrap();
            if index == 1 {
                provisioning
                    .bind_registration(&attempt.request.registration_request_id, &registered)
                    .unwrap();
                probe
                    .bind_generation(
                        registered
                            .active_device
                            .as_ref()
                            .unwrap()
                            .reference
                            .as_ref()
                            .unwrap(),
                    )
                    .unwrap();
                let batch = provisioning.next_upload().unwrap().unwrap();
                probe.upload(&batch).await.unwrap(); // Discard the publication receipt.
                bob_batch = Some(batch);
            } // Alice discards Register receipt; Bob discards Upload receipt.
            drop((probe, provisioning, discovered));
            let mut discovered = config()
                .discover(origin, Zeroizing::new(token.into()))
                .await
                .unwrap();
            let mut provisioning =
                DeviceProvisioning::new(local_store(root.path(), discovered.scope(), false));
            assert_eq!(
                discovered.reconcile(&mut provisioning).unwrap(),
                if index == 0 {
                    Reconciliation::RegistrationPending
                } else {
                    Reconciliation::PublicationPending
                }
            );
            assert_eq!(
                discovered.resume_initial(&mut provisioning).await.unwrap(),
                Reconciliation::Ready
            );
            assert_eq!(
                discovered.resume_initial(&mut provisioning).await.unwrap(),
                Reconciliation::Ready
            );
            assert_eq!(
                provisioning.prepare_registration().unwrap().request,
                attempt.request
            );
            let own = provisioning.ready_device().unwrap();
            devices.push(own);
            scopes.push(scope);
            stores.push(provisioning.into_store());
            clients.push(discovered);
        }
        assert_ne!(
            scopes[0].database_path(root.path()),
            scopes[1].database_path(root.path())
        );
        let bob_batch = bob_batch.unwrap();
        let mut alice_account = clients.remove(0);
        let mut bob_account = clients.remove(0);
        let alice_api = alice_account.device_client().unwrap();
        let bob_api = bob_account.device_client().unwrap();
        let alice = CryptoCoordinator::start(stores.remove(0), devices[0].clone())
            .await
            .unwrap();
        let bob_store = stores.remove(0);
        let peer_id = fields[4].parse().unwrap();
        let create = alice.prepare_conversation_creation(peer_id).await.unwrap();
        // Real server COMMIT, response deliberately discarded before local accept.
        drop(
            alice_api
                .create_prepared_conversation(&create)
                .await
                .unwrap(),
        );
        alice.shutdown().await.unwrap();
        let alice = CryptoCoordinator::start(
            local_store(root.path(), scopes[0].clone(), false),
            devices[0].clone(),
        )
        .await
        .unwrap();
        assert_eq!(
            alice
                .prepare_conversation_creation(peer_id)
                .await
                .unwrap()
                .request(),
            create.request()
        );
        let conversation = alice_api
            .create_conversation(&alice, peer_id)
            .await
            .unwrap();
        assert_eq!(
            alice_api
                .create_conversation(&alice, peer_id)
                .await
                .unwrap(),
            conversation
        );
        let bob = CryptoCoordinator::start(bob_store, devices[1].clone())
            .await
            .unwrap();
        let peer = bob_api
            .get_conversation(&bob, &conversation.conversation_id)
            .await
            .unwrap();
        let a = alice
            .conversation_scope(conversation.conversation_id.clone())
            .await
            .unwrap();
        let b = bob
            .conversation_scope(peer.conversation_id.clone())
            .await
            .unwrap();
        let alice_id = fields[7].parse().unwrap();
        let bob_create = bob.prepare_conversation_creation(alice_id).await.unwrap();
        assert!(matches!(
            alice_api.create_prepared_conversation(&bob_create).await,
            Err(RpcError::Invalid)
        ));
        // Lose Bob's real COMMIT reply until AFTER Alice replaces her installation.
        drop(
            bob_api
                .create_prepared_conversation(&bob_create)
                .await
                .unwrap(),
        );
        let foreign = CryptoCoordinator::start(
            local_store(
                root.path(),
                StoreScope::new("https://wrong-server.example.test", &devices[0].user_id).unwrap(),
                true,
            ),
            devices[0].clone(),
        )
        .await
        .unwrap();
        let wrong_server = foreign
            .prepare_conversation_creation(peer_id)
            .await
            .unwrap();
        assert!(matches!(
            alice_api.create_prepared_conversation(&wrong_server).await,
            Err(RpcError::Invalid)
        ));
        let wrong_lookup = foreign
            .prepare_conversation_lookup(conversation.conversation_id.clone())
            .await
            .unwrap();
        assert!(matches!(
            alice_api.get_prepared_conversation(&wrong_lookup).await,
            Err(RpcError::Invalid)
        ));
        let bob_lookup = bob
            .prepare_conversation_lookup(conversation.conversation_id.clone())
            .await
            .unwrap();
        assert!(matches!(
            alice_api.get_prepared_conversation(&bob_lookup).await,
            Err(RpcError::Invalid)
        ));
        foreign.shutdown().await.unwrap();
        let old = alice.prepare_acquisition(a.clone()).await.unwrap();
        assert!(!old.applied);
        let old_bundle = alice_api.acquire(&old.request).await.unwrap();
        assert_eq!(alice_api.acquire(&old.request).await.unwrap(), old_bundle);
        // The Go fixture expires this exact reservation before the third RPC.
        // Only its typed server response may authorize the local CAS operation.
        let expired = match alice_api.acquire_for_bootstrap(&old.request).await.unwrap() {
            AcquisitionOutcome::Expired(proof) => proof,
            AcquisitionOutcome::Bundle(_) => panic!("expected server-confirmed expiry"),
        };
        let plan = alice
            .renew_acquisition(a.clone(), expired.clone())
            .await
            .unwrap();
        assert_eq!(
            alice
                .renew_acquisition(a.clone(), expired)
                .await
                .unwrap()
                .request,
            plan.request
        );
        assert_ne!(old.request.acquisition_id, plan.request.acquisition_id);
        assert!(matches!(
            alice.bootstrap(a.clone(), old_bundle).await,
            Err(super::super::signal_state::CryptoError::Conflict)
        ));
        let bundle = match alice_api
            .acquire_for_bootstrap(&plan.request)
            .await
            .unwrap()
        {
            AcquisitionOutcome::Bundle(bundle) => bundle,
            AcquisitionOutcome::Expired(_) => panic!("new reservation unexpectedly expired"),
        };
        assert_eq!(alice_api.acquire(&plan.request).await.unwrap(), bundle);
        alice.bootstrap(a.clone(), bundle).await.unwrap();
        assert!(alice.prepare_acquisition(a.clone()).await.unwrap().applied);
        let text = |sender: usize, recipient: usize, value: &str| EncryptedPayload {
            format_version: 1,
            message_id: Uuid::new_v4().to_string(),
            conversation_id: conversation.conversation_id.clone(),
            sender: Some(devices[sender].clone()),
            recipient: Some(devices[recipient].clone()),
            created_at_unix_ms: 1700000000123,
            content: Some(encrypted_payload::Content::Text(TextMessage {
                text: value.into(),
            })),
        };
        let message = text(0, 1, "Rust public keys through real Go PostgreSQL");
        let encrypted = alice.encrypt(a.clone(), message.clone()).await.unwrap();
        let message_send = SendMessageRequest {
            context: Some(alice_api.context().unwrap()),
            envelope: Some(encrypted.clone()),
        };
        // Persist real Signal ciphertext through Go/PG, then lose its reply.
        let outgoing = alice
            .outgoing_plan(message.message_id.clone())
            .await
            .unwrap();
        assert_eq!(outgoing.request(), &message_send);
        assert!(matches!(
            bob_api.send_outbox(&outgoing).await,
            Err(RpcError::Invalid)
        ));
        assert!(matches!(
            bob_api
                .refresh_outgoing(std::slice::from_ref(&outgoing))
                .await,
            Err(RpcError::Invalid)
        ));
        drop(alice_api.send_outbox(&outgoing).await.unwrap());
        alice.shutdown().await.unwrap();
        let alice = CryptoCoordinator::start(
            local_store(root.path(), scopes[0].clone(), false),
            devices[0].clone(),
        )
        .await
        .unwrap();
        let outgoing = alice
            .outgoing_plan(message.message_id.clone())
            .await
            .unwrap();
        assert!(outgoing.progress().receipt().is_none());
        assert_eq!(outgoing.request(), &message_send);
        let verified = alice_api.send_outbox(&outgoing).await.unwrap();
        let progress = alice.accept_send_receipt(verified).await.unwrap();
        let message_receipt = progress.receipt().unwrap().clone();
        // Production receive commits the automatic confirmation before the
        // deliberately lost ACK reply below; replay never generates another UUID.
        let initial = bob_api.pending().await.unwrap();
        bob.receive_for_delivery(b.clone(), initial.messages[0].clone())
            .await
            .unwrap();
        let bob_ack = receive_and_ack(bob_api, &bob, b.clone(), &message).await;
        assert!(matches!(
            alice_api.acknowledge(&bob_ack).await,
            Err(RpcError::Invalid)
        ));
        assert_stored(alice_api, &alice, &message_receipt).await;
        let mut confirmations = bob.outbox_page(None).await.unwrap().messages;
        assert_eq!(confirmations.len(), 1);
        let confirmation = confirmations.remove(0);
        let confirmation_envelope = confirmation.request().envelope.clone().unwrap();
        let report =
            crate::encryption::delivery_runner::DeliveryRunner::new(&mut *bob_api, Default::default())
                .unwrap()
                .poll_once(&bob)
                .await
                .unwrap();
        assert_eq!(report.sent, 1);
        assert!(report.issues.is_empty());
        let confirmation_receipt = bob
            .outgoing_plan(confirmation_envelope.message_id.clone())
            .await
            .unwrap()
            .progress()
            .receipt()
            .unwrap()
            .clone();
        let report = crate::encryption::delivery_runner::DeliveryRunner::new(
            &mut *alice_api,
            Default::default(),
        )
        .unwrap()
        .poll_once(&alice)
        .await
        .unwrap();
        assert_eq!((report.received, report.acknowledged), (1, 1));
        assert!(report.issues.is_empty());
        let confirmation_payload = alice
            .decrypt(a.clone(), confirmation_envelope)
            .await
            .unwrap();
        assert!(
            matches!(confirmation_payload.content, Some(encrypted_payload::Content::SessionConfirmation(ref reply)) if reply.in_reply_to_message_id == message.message_id)
        );
        assert!(alice.outbox_page(None).await.unwrap().messages.is_empty());
        // Pending-only recovery hides completed work, never its retained exact
        // ciphertext/receipt. A confirmation must not create another reply.
        assert_eq!(alice.outgoing_plan(message.message_id.clone()).await.unwrap().request(), &message_send);
        assert_stored(bob_api, &bob, &confirmation_receipt).await;
        // Re-upload after the peer's one-time keys were consumed on the server.
        // An exact publication retry must not make those keys available again.
        bob_api.upload(&bob_batch).await.unwrap();
        let reply = text(1, 0, "confirmed Signal reply");
        let reply_envelope = bob.encrypt(b.clone(), reply.clone()).await.unwrap();
        let reply_send = SendMessageRequest {
            context: Some(bob_api.context().unwrap()),
            envelope: Some(reply_envelope.clone()),
        };
        let outgoing = bob.outgoing_plan(reply.message_id.clone()).await.unwrap();
        assert_eq!(outgoing.request(), &reply_send);
        let report =
            crate::encryption::delivery_runner::DeliveryRunner::new(&mut *bob_api, Default::default())
                .unwrap()
                .poll_once(&bob)
                .await
                .unwrap();
        assert_eq!(report.sent, 1);
        assert!(report.issues.is_empty());
        let reply_receipt = bob
            .outgoing_plan(reply.message_id.clone())
            .await
            .unwrap()
            .progress()
            .receipt()
            .unwrap()
            .clone();
        let report = crate::encryption::delivery_runner::DeliveryRunner::new(
            &mut *alice_api,
            Default::default(),
        )
        .unwrap()
        .poll_once(&alice)
        .await
        .unwrap();
        assert_eq!((report.received, report.acknowledged), (1, 1));
        assert!(report.issues.is_empty());
        assert_eq!(
            alice
                .decrypt(a.clone(), reply_envelope.clone())
                .await
                .unwrap(),
            reply
        );
        assert_stored(bob_api, &bob, &reply_receipt).await;
        // Accepted ciphertext stays queued until AFTER Alice replaces her keys.
        let late_message = text(
            0,
            1,
            "accepted before sender replacement; delivered to the old closed dialogue",
        );
        let late_envelope = alice
            .encrypt(a.clone(), late_message.clone())
            .await
            .unwrap();
        let late_plan = alice
            .outgoing_plan(late_message.message_id.clone())
            .await
            .unwrap();
        alice
            .accept_send_receipt(alice_api.send_outbox(&late_plan).await.unwrap())
            .await
            .unwrap();

        let mut bad = NativeV1Client::connect(
            NativeEndpoint::new(fields[0], Some(&ca)).unwrap(),
            &devices[0].user_id,
            Zeroizing::new("invalid.jwt.token".into()),
            Zeroizing::new([1; 32]),
        )
        .await
        .unwrap();
        assert!(matches!(
            bad.capabilities().await,
            Err(RpcError::Failure {
                code: Code::Unauthenticated,
                reason: FailureReason::SessionInvalid
            })
        ));
        let mut bad = NativeV1Client::connect(
            NativeEndpoint::new(fields[0], Some(&ca)).unwrap(),
            &devices[1].user_id,
            Zeroizing::new(fields[3].into()),
            Zeroizing::new([1; 32]),
        )
        .await
        .unwrap();
        bad.bind_generation(&devices[1]).unwrap();
        assert!(matches!(
            bad.upload(&bob_batch).await,
            Err(RpcError::Failure {
                code: Code::Unauthenticated,
                reason: FailureReason::DeviceCredentialInvalid
            })
        ));
        let mut foreign = bob_batch.clone();
        foreign.context.as_mut().unwrap().device_generation_id = devices[0].generation_id.clone();
        assert!(matches!(
            bob_api.upload(&foreign).await,
            Err(RpcError::Invalid)
        ));
        let mut changed = devices[0].clone();
        changed.generation_id = Uuid::new_v4().to_string();
        assert!(alice_api.bind_generation(&changed).is_err());
        // A trusted CA is mandatory; never silently fall back to plaintext.
        assert!(matches!(
            NativeV1Client::connect(
                NativeEndpoint::new(fields[0], None).unwrap(),
                &devices[0].user_id,
                Zeroizing::new(fields[2].into()),
                Zeroizing::new([1; 32])
            )
            .await,
            Err(RpcError::Transport)
        ));
        while !alice.seed_prekey_ids().await.unwrap().ready {}
        let refill = alice.prepare_prekey_refill(3, 40).await.unwrap();
        alice.materialize_prekey_refill(refill.operation_id.clone()).await.unwrap();
        let refill_upload = alice.next_prekey_refill_upload().await.unwrap().unwrap();
        assert_eq!(refill_upload.request().one_time_pre_keys[0].key_id, 33);
        assert_eq!(refill_upload.request().kyber_pre_keys[0].key_id, 34);
        assert!(matches!(bob_api.upload_refill(&refill_upload).await, Err(RpcError::Invalid)));
        // Actual remote COMMIT twice via exact replay, deliberately lose both
        // local proofs. The next JWT login must recover the multi-batch refill.
        drop(alice_api.upload_refill(&refill_upload).await.unwrap());
        drop(alice_api.upload_refill(&refill_upload).await.unwrap());
        alice.shutdown().await.unwrap();
        bob.shutdown().await.unwrap();
        // A distinct JWT/login session discovers the same account/store and
        // preserves ratchet/outbox. The fixture still supplies no account UUID.
        let mut login = TrustedServer::new(origin, fields[0], Some(&ca))
            .unwrap()
            .discover(origin, Zeroizing::new(fields[6].into()))
            .await
            .unwrap();
        assert!(login.scope() == scopes[0]);
        let mut restored = DeviceProvisioning::new(local_store(root.path(), login.scope(), false));
        assert_eq!(
            login.resume_initial(&mut restored).await.unwrap(),
            Reconciliation::Ready
        );
        assert!(restored.next_upload().unwrap().is_none());
        assert_eq!(restored.ready_device().unwrap(), devices[0]);
        let alice = CryptoCoordinator::start(restored.into_store(), devices[0].clone())
            .await
            .unwrap();
        let completed_refill = alice.prepare_prekey_refill(3, 40).await.unwrap();
        assert_eq!(completed_refill.operation_id, refill.operation_id);
        assert!(completed_refill.complete);
        // A new refill is an explicit completed-operation CAS. Test opaque
        // worker proof acceptance and a lost local reply independently of login.
        let second_refill = alice.prepare_next_prekey_refill(refill.operation_id.clone(), 2, 0).await.unwrap();
        alice.materialize_prekey_refill(second_refill.operation_id.clone()).await.unwrap();
        let upload = alice.next_prekey_refill_upload().await.unwrap().unwrap();
        assert_eq!(upload.request().expected_revision, refill_upload.request().expected_revision + 2);
        drop(login.device_client().unwrap().upload_refill(&upload).await.unwrap());
        alice.accept_prekey_refill_upload(login.device_client().unwrap().upload_refill(&upload).await.unwrap()).await.unwrap();
        assert!(alice.next_prekey_refill_upload().await.unwrap().is_none());
        println!("zchat-native-refill-complete:v1");
        // Rotation shares the refill head/revision; keep all old private keys.
        let rotation = alice.prepare_next_prekey_rotation(second_refill.operation_id.clone(), true, true).await.unwrap();
        alice.materialize_prekey_refill(rotation.operation_id.clone()).await.unwrap();
        let rotation_upload = alice.next_prekey_refill_upload().await.unwrap().unwrap();
        assert_eq!(rotation_upload.request().signed_pre_key.as_ref().unwrap().key_id, 2);
        assert_eq!(rotation_upload.request().kyber_pre_keys[0].key_id, 74);
        assert!(rotation_upload.request().kyber_pre_keys[0].is_last_resort);
        drop(login.device_client().unwrap().upload_refill(&rotation_upload).await.unwrap());
        // Actual new active pointers committed remotely, but local receipt lost.
        alice.shutdown().await.unwrap();
        let mut restored = DeviceProvisioning::new(local_store(root.path(), login.scope(), false));
        assert_eq!(login.resume_initial(&mut restored).await.unwrap(), Reconciliation::Ready);
        assert_eq!(restored.ready_device().unwrap(), devices[0]);
        let alice = CryptoCoordinator::start(restored.into_store(), devices[0].clone()).await.unwrap();
        assert!(alice.current_prekey_publication().await.unwrap().unwrap().complete);
        let old_rotation_proof = login.device_client().unwrap().upload_refill(&rotation_upload).await.unwrap();
        let next_rotation = alice.prepare_next_prekey_rotation(rotation.operation_id.clone(), true, false).await.unwrap();
        alice.materialize_prekey_refill(next_rotation.operation_id.clone()).await.unwrap();
        let next_upload = alice.next_prekey_refill_upload().await.unwrap().unwrap();
        assert_eq!(next_upload.request().signed_pre_key.as_ref().unwrap().key_id, 3);
        assert!(next_upload.request().kyber_pre_keys.is_empty());
        alice.accept_prekey_refill_upload(login.device_client().unwrap().upload_refill(&next_upload).await.unwrap()).await.unwrap();
        alice.accept_prekey_refill_upload(old_rotation_proof).await.unwrap();
        let post_rotation_refill = alice.prepare_next_prekey_refill(next_rotation.operation_id.clone(), 1, 1).await.unwrap();
        alice.materialize_prekey_refill(post_rotation_refill.operation_id.clone()).await.unwrap();
        let post_upload = alice.next_prekey_refill_upload().await.unwrap().unwrap();
        assert_eq!(post_upload.request().kyber_pre_keys[0].key_id, 75);
        assert!(!post_upload.request().kyber_pre_keys[0].is_last_resort);
        alice.accept_prekey_refill_upload(login.device_client().unwrap().upload_refill(&post_upload).await.unwrap()).await.unwrap();
        println!("zchat-native-rotation-complete:v1");
        // Owner inventory and automatic low-water planning through real TLS/Go.
        // Controlled high threshold in this fixture, not a production policy change.
        use crate::encryption::prekey_maintenance::{MaintenanceConfig, MaintenanceStep, PreKeyMaintenance, RefillPolicy};
        let (_stop, mut stop_receiver) = tokio::sync::watch::channel(false);
        let mut maintenance = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
        assert!(matches!(maintenance.tick(&alice, login.device_client().unwrap(), &mut stop_receiver).await.unwrap(), MaintenanceStep::Healthy));
        let bob = CryptoCoordinator::start(local_store(root.path(), scopes[1].clone(), false), devices[1].clone()).await.unwrap();
        while !bob.seed_prekey_ids().await.unwrap().ready {}
        let before = bob_api.prekey_inventory().await.unwrap();
        assert_eq!((before.value().retained_ec, before.value().retained_pq), (32, 33));
        assert!(before.value().available_ec < 32 && before.value().available_pq < 32);
        let mut maintenance = PreKeyMaintenance::new(MaintenanceConfig { policy: RefillPolicy { low_water: 32, target: 32 }, ..Default::default() }).unwrap();
        let MaintenanceStep::Planned(p) = maintenance.tick(&bob, bob_api, &mut stop_receiver).await.unwrap() else { panic!("expected low-water plan") };
        assert_eq!(p.ec_count, 32-before.value().available_ec);
        assert_eq!(p.pq_count, 32-before.value().available_pq);
        assert!(!p.rotate_signed);
        // Harness loses the first response AFTER PostgreSQL COMMIT. Restarted
        // maintenance must retry its exact saved batch without another inventory.
        let mut maintenance = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
        assert!(matches!(maintenance.tick(&bob, bob_api, &mut stop_receiver).await.unwrap(), MaintenanceStep::Retry(_)));
        let pending = bob.next_prekey_refill_upload().await.unwrap().unwrap();
        let mut maintenance = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
        assert!(matches!(maintenance.tick(&bob, bob_api, &mut stop_receiver).await.unwrap(), MaintenanceStep::Uploaded { complete: true }));
        assert_eq!(bob.current_prekey_publication().await.unwrap().unwrap().operation_id, p.operation_id);
        // Exact committed request replay cannot revive already consumed keys.
        bob.accept_prekey_refill_upload(bob_api.upload_refill(&pending).await.unwrap()).await.unwrap();
        let after = bob_api.prekey_inventory().await.unwrap();
        assert_eq!((after.value().available_ec, after.value().available_pq), (32, 32));
        assert_eq!(after.value().publication_revision, 3);
        assert_eq!(after.value().active_signed_id, before.value().active_signed_id);
        assert_eq!(after.value().active_last_resort_id, before.value().active_last_resort_id);
        println!("zchat-native-maintenance-complete:v1");
        bob.shutdown().await.unwrap();
        let resumed = alice.prepare_acquisition(a.clone()).await.unwrap();
        assert!(resumed.applied);
        assert_eq!(resumed.request, plan.request);
        assert_eq!(alice.encrypt(a, message).await.unwrap(), encrypted);
        let restored_progress = alice
            .outgoing_plan(message_receipt.message_id.clone())
            .await
            .unwrap();
        assert_eq!(
            restored_progress.progress().receipt(),
            Some(&message_receipt)
        );
        assert_eq!(
            restored_progress.progress().status().unwrap().state,
            crate::proto::e2ee::MessageState::RecipientStored as i32
        );
        assert_eq!(
            login
                .device_client()
                .unwrap()
                .send(&message_send)
                .await
                .unwrap(),
            message_receipt
        );
        alice.shutdown().await.unwrap();
        // A second installation must use an explicit password/CAS action.
        let other_root = tempfile::tempdir().unwrap();
        let mut other = DeviceProvisioning::new(local_store(other_root.path(), login.scope(), true));
        let before = std::fs::read(login.scope().database_path(other_root.path())).unwrap();
        let mut new_login = TrustedServer::new(origin, fields[0], Some(&ca))
            .unwrap()
            .discover(origin, Zeroizing::new(fields[2].into()))
            .await
            .unwrap();
        assert!(matches!(
            new_login.resume_initial(&mut other).await.unwrap(),
            Reconciliation::ReplacementRequired { .. }
        ));
        assert_eq!(
            std::fs::read(login.scope().database_path(other_root.path())).unwrap(),
            before
        );
        let replacement = new_login
            .begin_replacement(&mut other, &devices[0].generation_id, None)
            .await
            .unwrap();
        let prepared = std::fs::read(login.scope().database_path(other_root.path())).unwrap();
        assert!(matches!(
            new_login
                .resume_replacement(
                    &mut other,
                    Zeroizing::new("incorrect synthetic password".into())
                )
                .await,
            Err(DiscoveryError::Rpc(RpcError::Failure {
                code: Code::Unauthenticated,
                reason: FailureReason::PasswordInvalid
            }))
        ));
        assert_eq!(
            std::fs::read(login.scope().database_path(other_root.path())).unwrap(),
            prepared
        );
        assert!(new_login.device_client().is_err());
        // Drop a real response after COMMIT, before local binding.
        let mut probe = new_login.client_for_test(&replacement);
        let replaced = probe
            .replace(&replacement, Zeroizing::new(fields[5].into()))
            .await
            .unwrap();
        drop((probe, other, new_login));
        let mut new_login = TrustedServer::new(origin, fields[0], Some(&ca))
            .unwrap()
            .discover(origin, Zeroizing::new(fields[6].into()))
            .await
            .unwrap();
        let mut other =
            DeviceProvisioning::new(local_store(other_root.path(), new_login.scope(), false));
        assert_eq!(
            other.replacement_attempt().unwrap().request(),
            replacement.request()
        );
        let before = std::fs::read(new_login.scope().database_path(other_root.path())).unwrap();
        assert!(matches!(
            new_login.resume_initial(&mut other).await.unwrap(),
            Reconciliation::ReplacementPending { .. }
        ));
        assert_eq!(
            std::fs::read(new_login.scope().database_path(other_root.path())).unwrap(),
            before
        );
        assert_eq!(
            new_login
                .resume_replacement(&mut other, Zeroizing::new(fields[5].into()))
                .await
                .unwrap(),
            Reconciliation::Ready
        );
        let first_replaced = other.ready_device().unwrap();
        assert_eq!(
            replaced.active_device.unwrap().reference.unwrap(),
            first_replaced
        );
        let mut old = DeviceProvisioning::new(local_store(root.path(), login.scope(), false));
        let before = std::fs::read(login.scope().database_path(root.path())).unwrap();
        assert_eq!(
            login.resume_initial(&mut old).await.unwrap(),
            Reconciliation::DeviceUnavailable {
                local_device: devices[0].clone(),
                active_device: Some(first_replaced.clone())
            }
        );
        assert!(login.device_client().is_err());
        assert_eq!(
            std::fs::read(login.scope().database_path(root.path())).unwrap(),
            before
        );
        drop(old);
        // A distinct ordinary login must authenticate the replacement using its
        // credential + exact upload replay, without another password/Replace RPC.
        drop((new_login, other));
        let mut new_login = TrustedServer::new(origin, fields[0], Some(&ca))
            .unwrap()
            .discover(origin, Zeroizing::new(fields[2].into()))
            .await
            .unwrap();
        let mut other =
            DeviceProvisioning::new(local_store(other_root.path(), new_login.scope(), false));
        assert_eq!(
            new_login.resume_initial(&mut other).await.unwrap(),
            Reconciliation::Ready
        );
        let bob = CryptoCoordinator::start(
            local_store(root.path(), scopes[1].clone(), false),
            devices[1].clone(),
        )
        .await
        .unwrap();
        // Get closes the authoritative index before Bob receives the delayed
        // original Create reply. No local Create result has been adopted yet.
        let closed = bob_api
            .get_conversation(&bob, &conversation.conversation_id)
            .await
            .unwrap();
        let receiving = bob
            .conversation_scope(closed.conversation_id.clone())
            .await
            .unwrap();
        assert!(matches!(
            bob.prepare_acquisition(receiving.clone()).await,
            Err(super::super::signal_state::CryptoError::ConversationClosed)
        ));
        let page = bob_api.pending().await.unwrap();
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0].envelope.as_ref(), Some(&late_envelope));
        let report =
            crate::encryption::delivery_runner::DeliveryRunner::new(&mut *bob_api, Default::default())
                .unwrap()
                .poll_once(&bob)
                .await
                .unwrap();
        assert_eq!((report.received, report.acknowledged), (1, 1));
        assert!(report.issues.is_empty());
        assert_eq!(
            bob.decrypt(receiving, late_envelope.clone()).await.unwrap(),
            late_message
        );
        assert_eq!(
            bob_api.create_conversation(&bob, alice_id).await.unwrap(),
            closed
        );
        assert_eq!(closed.conversation_id, conversation.conversation_id);
        assert_eq!(
            closed.state,
            crate::proto::e2ee::ConversationState::Closed as i32
        );
        assert_eq!(closed.closed_generation_id, devices[0].generation_id);
        assert!(SessionScope::from_conversation(&closed).is_err());
        assert_eq!(bob_api.send(&reply_send).await.unwrap(), reply_receipt);
        let mut closed_send = reply_send.clone();
        closed_send.envelope.as_mut().unwrap().message_id = Uuid::new_v4().to_string();
        assert!(matches!(
            bob_api.send(&closed_send).await,
            Err(RpcError::Status(Code::FailedPrecondition))
        ));
        assert_eq!(
            bob.prepare_conversation_creation(alice_id)
                .await
                .unwrap()
                .request(),
            bob_create.request()
        );
        assert_eq!(
            bob_api.create_conversation(&bob, alice_id).await.unwrap(),
            closed
        );
        let new_create = bob
            .prepare_new_conversation_creation(
                alice_id,
                bob_create.request().creation_request_id.clone(),
            )
            .await
            .unwrap();
        let created = bob_api
            .create_prepared_conversation(&new_create)
            .await
            .unwrap();
        let new_bob = bob.accept_conversation_creation(created).await.unwrap();
        assert_ne!(new_bob.conversation_id, closed.conversation_id);
        assert_eq!(
            new_bob
                .peer_device
                .as_ref()
                .unwrap()
                .reference
                .as_ref()
                .unwrap(),
            &first_replaced
        );
        let api = new_login.device_client().unwrap();
        let alice = CryptoCoordinator::start(other.into_store(), first_replaced.clone())
            .await
            .unwrap();
        let conversation = api.create_conversation(&alice, peer_id).await.unwrap();
        assert_eq!(conversation.conversation_id, new_bob.conversation_id);
        let bob_conversation = bob_api
            .get_conversation(&bob, &conversation.conversation_id)
            .await
            .unwrap();
        let a2 = alice
            .conversation_scope(conversation.conversation_id.clone())
            .await
            .unwrap();
        let b2 = bob
            .conversation_scope(bob_conversation.conversation_id.clone())
            .await
            .unwrap();
        let acquisition = alice.prepare_acquisition(a2.clone()).await.unwrap();
        let bundle = api.acquire(&acquisition.request).await.unwrap();
        alice.bootstrap(a2.clone(), bundle).await.unwrap();
        let text = EncryptedPayload {
            format_version: 1,
            message_id: Uuid::new_v4().to_string(),
            conversation_id: conversation.conversation_id,
            sender: Some(first_replaced.clone()),
            recipient: Some(devices[1].clone()),
            created_at_unix_ms: 1700000000123,
            content: Some(encrypted_payload::Content::Text(TextMessage {
                text: "history in an installation that will be replaced locally".into(),
            })),
        };
        let ciphertext = alice.encrypt(a2, text.clone()).await.unwrap();
        let outgoing = alice.outgoing_plan(text.message_id.clone()).await.unwrap();
        assert_eq!(outgoing.request().envelope.as_ref(), Some(&ciphertext));
        let verified = api.send_outbox(&outgoing).await.unwrap();
        let receipt = alice
            .accept_send_receipt(verified)
            .await
            .unwrap()
            .receipt()
            .unwrap()
            .clone();
        receive_and_ack(bob_api, &bob, b2, &text).await;
        assert_stored(api, &alice, &receipt).await;
        // Fault probe: a nonconforming sender persists syntactically valid but
        // undecryptable Signal bytes. Normal send_outbox never constructs this.
        use crate::proto::e2ee::{Envelope, MessageReference, MessageState};
        let bad_send = SendMessageRequest {
            context: Some(api.context().unwrap()),
            envelope: Some(Envelope {
                protocol_version: 1,
                crypto_suite: 1,
                message_id: Uuid::new_v4().to_string(),
                conversation_id: outgoing
                    .request()
                    .envelope
                    .as_ref()
                    .unwrap()
                    .conversation_id
                    .clone(),
                sender: Some(first_replaced.clone()),
                recipient: Some(devices[1].clone()),
                message_type: 1,
                ciphertext: vec![1],
            }),
        };
        let bad_receipt = api.send(&bad_send).await.unwrap();
        let page = bob_api.pending().await.unwrap();
        assert_eq!(page.messages.len(), 1);
        let bad_scope = bob
            .conversation_scope(bob_conversation.conversation_id.clone())
            .await
            .unwrap();
        assert!(
            bob.receive_delivery(bad_scope.clone(), page.messages[0].clone())
                .await
                .is_err()
        );
        let proof = bob
            .quarantine_delivery(
                bad_scope,
                page.messages[0].clone(),
                super::super::signal_state::QuarantineReason::InvalidCiphertext,
            )
            .await
            .unwrap();
        drop(bob_api.acknowledge(&proof).await.unwrap());
        let recovered = bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .remove(0);
        bob.mark_acknowledged(bob_api.acknowledge(&recovered).await.unwrap())
            .await
            .unwrap();
        let status = api
            .statuses(&GetMessageStatusesRequest {
                context: Some(api.context().unwrap()),
                messages: vec![MessageReference {
                    message_id: bad_receipt.message_id,
                    envelope_digest: bad_receipt.envelope_digest,
                }],
            })
            .await
            .unwrap();
        assert_eq!(
            status.messages[0].state,
            MessageState::RecipientRejected as i32
        );

        alice.shutdown().await.unwrap();
        bob.shutdown().await.unwrap();
        // A second replacement inside the same protected DB archives real Signal
        // history/outbox. Snapshot exact old-generation ciphertext bytes first.
        let path = new_login.scope().database_path(other_root.path());
        let snapshot = || {
            let db = rusqlite::Connection::open(&path).unwrap();
            let id = Uuid::parse_str(&first_replaced.generation_id).unwrap();
            db.prepare("SELECT kind,record_id,payload FROM protected_records WHERE generation=?1 AND record_id!='archived-device' ORDER BY kind,record_id").unwrap()
                                .query_map([id.as_bytes().as_slice()], |r| Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,Vec<u8>>(2)?))).unwrap().collect::<std::result::Result<Vec<_>,_>>().unwrap()
        };
        let before = snapshot();
        let mut other =
            DeviceProvisioning::new(local_store(other_root.path(), new_login.scope(), false));
        let before_cas = std::fs::read(&path).unwrap();
        assert!(matches!(
            new_login
                .begin_replacement(&mut other, &devices[0].generation_id, None)
                .await,
            Err(DiscoveryError::ExpectedGenerationChanged)
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before_cas);
        let second = new_login
            .begin_replacement(&mut other, &first_replaced.generation_id, None)
            .await
            .unwrap();
        let mut probe = new_login.client_for_test(&second);
        let receipt = probe
            .replace(&second, Zeroizing::new(fields[5].into()))
            .await
            .unwrap();
        other
            .bind_replacement(&second.request().registration_request_id, &receipt)
            .unwrap();
        probe
            .bind_generation(
                receipt
                    .active_device
                    .as_ref()
                    .unwrap()
                    .reference
                    .as_ref()
                    .unwrap(),
            )
            .unwrap();
        probe
            .upload(&other.next_upload().unwrap().unwrap())
            .await
            .unwrap(); // Lose the receipt after COMMIT.
        drop((probe, other, new_login));
        let mut new_login = TrustedServer::new(origin, fields[0], Some(&ca))
            .unwrap()
            .discover(origin, Zeroizing::new(fields[6].into()))
            .await
            .unwrap();
        let mut other =
            DeviceProvisioning::new(local_store(other_root.path(), new_login.scope(), false));
        // Local binding survived, so publication recovery needs no password.
        assert_eq!(
            new_login.resume_initial(&mut other).await.unwrap(),
            Reconciliation::Ready
        );
        assert_eq!(snapshot(), before);
        let latest = other.ready_device().unwrap();
        assert_ne!(latest.generation_id, first_replaced.generation_id);
        drop(other);
        assert!(matches!(
            CryptoCoordinator::start(
                local_store(other_root.path(), new_login.scope(), false),
                first_replaced.clone()
            )
            .await,
            Err(super::super::signal_state::CryptoError::ArchivedGeneration)
        ));
        // The earlier CAS operation is now retired. Replay must not replace the
        // latest installation or make the original generation active again.
        let mut stale = login.client_for_test(&replacement);
        assert!(matches!(
            stale
                .replace(&replacement, Zeroizing::new(fields[5].into()))
                .await,
            Err(RpcError::Failure {
                code: Code::FailedPrecondition,
                reason: FailureReason::GenerationChanged
            })
        ));
        // Authentication failure produces no discovered account/store handle.
        assert!(matches!(
            TrustedServer::new(origin, fields[0], Some(&ca))
                .unwrap()
                .discover(origin, Zeroizing::new("invalid.jwt.token".into()))
                .await,
            Err(DiscoveryError::Rpc(RpcError::Failure {
                code: Code::Unauthenticated,
                reason: FailureReason::SessionInvalid
            }))
        ));
        // The next replacement closes Bob's explicitly created new chat too.
        let bob = CryptoCoordinator::start(
            local_store(root.path(), scopes[1].clone(), false),
            devices[1].clone(),
        )
        .await
        .unwrap();
        let closed = bob_api.create_conversation(&bob, alice_id).await.unwrap();
        assert_eq!(closed.conversation_id, new_bob.conversation_id);
        assert_eq!(closed.closed_generation_id, first_replaced.generation_id);
        assert_eq!(
            closed.state,
            crate::proto::e2ee::ConversationState::Closed as i32
        );
        assert_eq!(
            bob.prepare_conversation_creation(alice_id)
                .await
                .unwrap()
                .request(),
            new_create.request()
        );
        bob.shutdown().await.unwrap();
    });
    println!("zchat-native-interop-complete:v1");
}

#[test]
fn inbox_contract_binds_recipient_digest_sequence_and_bounds() {
    use super::super::contract::{
        MAX_INBOX_MESSAGES, MAX_RESPONSE_BYTES, envelope_digest, validate_pending,
    };
    use crate::proto::e2ee::{Delivery, Envelope};
    let envelope =
        Envelope::decode(include_bytes!("../../../proto/fixtures/e2ee-v1-envelope.bin").as_slice())
            .unwrap();
    let own = envelope.recipient.clone().unwrap();
    let page = PendingMessages {
        messages: vec![Delivery {
            sequence: 1,
            envelope: Some(envelope.clone()),
            expires_at_unix_ms: 1,
            envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        }],
    };
    assert!(validate_pending(&page, &own).is_ok());
    assert!(validate_pending(&PendingMessages::default(), &own).is_ok());
    for change in 0..8 {
        let mut bad = page.clone();
        match change {
            0 => bad.messages[0].envelope = None,
            1 => bad.messages[0].sequence = 0,
            2 => bad.messages[0].expires_at_unix_ms = 0,
            3 => bad.messages[0].envelope_digest[0] ^= 1,
            4 => {
                bad.messages[0]
                    .envelope
                    .as_mut()
                    .unwrap()
                    .recipient
                    .as_mut()
                    .unwrap()
                    .generation_id = Uuid::new_v4().to_string()
            }
            5 => {
                bad.messages[0]
                    .envelope
                    .as_mut()
                    .unwrap()
                    .sender
                    .as_mut()
                    .unwrap()
                    .device_id = 2
            }
            6 => {
                let mut duplicate = bad.messages[0].clone();
                duplicate.sequence = 2;
                bad.messages.push(duplicate);
            }
            _ => {
                let mut duplicate = bad.messages[0].clone();
                duplicate.envelope.as_mut().unwrap().message_id = Uuid::new_v4().to_string();
                duplicate.envelope_digest = envelope_digest(duplicate.envelope.as_ref().unwrap())
                    .unwrap()
                    .to_vec();
                bad.messages.push(duplicate);
            }
        }
        assert!(validate_pending(&bad, &own).is_err(), "change {change}");
    }
    let mut max = PendingMessages::default();
    for i in 0..MAX_INBOX_MESSAGES + 1 {
        let mut delivery = page.messages[0].clone();
        delivery.sequence = i as i64 + 1;
        delivery.envelope.as_mut().unwrap().message_id = Uuid::new_v4().to_string();
        delivery.envelope_digest = envelope_digest(delivery.envelope.as_ref().unwrap())
            .unwrap()
            .to_vec();
        max.messages.push(delivery);
        assert_eq!(validate_pending(&max, &own).is_ok(), i < MAX_INBOX_MESSAGES);
    }
    let mut large = page.clone();
    large.messages[0].envelope.as_mut().unwrap().ciphertext =
        vec![3; super::super::contract::MAX_REQUEST_BYTES - 512];
    large.messages[0].envelope_digest =
        envelope_digest(large.messages[0].envelope.as_ref().unwrap())
            .unwrap()
            .to_vec();
    assert!(validate_pending(&large, &own).is_ok());
    let mut second = large.messages[0].clone();
    second.sequence = 2;
    second.envelope.as_mut().unwrap().message_id = Uuid::new_v4().to_string();
    second.envelope_digest = envelope_digest(second.envelope.as_ref().unwrap())
        .unwrap()
        .to_vec();
    large.messages.push(second);
    assert!(large.encoded_len() > MAX_RESPONSE_BYTES);
    assert!(validate_pending(&large, &own).is_err());
}

#[test]
fn statuses_bind_fixed_receipt_order_and_allow_late_ack() {
    use super::super::contract::{
        MESSAGE_RETENTION_MS, validate_status_request, validate_statuses,
    };
    use crate::proto::e2ee::{MessageReference, MessageState, MessageStatus};
    let reference = MessageReference {
        message_id: Uuid::new_v4().to_string(),
        envelope_digest: vec![7; 32],
    };
    let request = GetMessageStatusesRequest {
        context: Some(ClientContext {
            protocol_version: 1,
            device_generation_id: Uuid::new_v4().to_string(),
        }),
        messages: vec![reference.clone()],
    };
    let receipt = SendReceipt {
        message_id: reference.message_id.clone(),
        envelope_digest: reference.envelope_digest.clone(),
        stored_at_unix_ms: 1,
        expires_at_unix_ms: 1 + MESSAGE_RETENTION_MS,
    };
    let response = MessageStatuses {
        messages: vec![MessageStatus {
            receipt: Some(receipt.clone()),
            state: MessageState::Pending as i32,
            observed_at_unix_ms: 1,
        }],
    };
    assert!(validate_statuses(&request, &response).is_ok());
    for state in [
        MessageState::Expired,
        MessageState::RecipientStored,
        MessageState::RecipientRejected,
        MessageState::RecipientReplaced,
    ] {
        let mut outcome = response.clone();
        outcome.messages[0].state = state as i32;
        outcome.messages[0].observed_at_unix_ms = receipt.expires_at_unix_ms + 1000;
        assert!(validate_statuses(&request, &outcome).is_ok());
    }
    for change in 0..9 {
        let mut bad = response.clone();
        match change {
            0 => bad.messages.clear(),
            1 => bad.messages[0].receipt = None,
            2 => bad.messages[0].receipt.as_mut().unwrap().envelope_digest[0] ^= 1,
            3 => bad.messages[0].receipt.as_mut().unwrap().message_id = Uuid::new_v4().to_string(),
            4 => bad.messages[0].receipt.as_mut().unwrap().expires_at_unix_ms += 1,
            5 => bad.messages[0].observed_at_unix_ms = 0,
            6 => bad.messages[0].state = 99,
            7 => bad.messages[0].state = MessageState::Expired as i32,
            _ => bad.messages[0].observed_at_unix_ms += 1,
        };
        assert!(
            validate_statuses(&request, &bad).is_err(),
            "change {change}"
        );
    }
    let mut duplicate = request.clone();
    duplicate.messages.push(reference.clone());
    assert!(validate_status_request(&duplicate).is_err());
    let mut two = request.clone();
    let mut other = reference.clone();
    other.message_id = Uuid::new_v4().to_string();
    two.messages.push(other.clone());
    let mut ordered = response.clone();
    let mut second = response.messages[0].clone();
    second.receipt.as_mut().unwrap().message_id = other.message_id;
    ordered.messages.push(second);
    assert!(validate_statuses(&two, &ordered).is_ok());
    ordered.messages.swap(0, 1);
    assert!(validate_statuses(&two, &ordered).is_err());
    let mut max = request;
    for _ in 1..100 {
        max.messages.push(MessageReference {
            message_id: Uuid::new_v4().to_string(),
            envelope_digest: vec![0; 32],
        })
    }
    assert!(validate_status_request(&max).is_ok());
    max.messages.push(reference);
    assert!(validate_status_request(&max).is_err());
}

#[test]
fn ack_contract_rejects_ambiguous_or_unbounded_claims() {
    use super::super::contract::validate_acknowledgements;
    use crate::proto::e2ee::{AcknowledgeMessagesRequest, Acknowledgement};
    let request = AcknowledgeMessagesRequest {
        context: Some(ClientContext {
            protocol_version: 1,
            device_generation_id: Uuid::new_v4().to_string(),
        }),
        messages: vec![Acknowledgement {
            message_id: Uuid::new_v4().to_string(),
            envelope_digest: vec![3; 32],
            disposition: 1,
        }],
    };
    assert!(validate_acknowledgements(&request).is_ok());
    for change in 0..7 {
        let mut bad = request.clone();
        match change {
            0 => bad.context = None,
            1 => bad.messages.clear(),
            2 => bad.messages[0].envelope_digest.clear(),
            3 => bad.messages[0].message_id.clear(),
            4 => bad.messages[0].disposition = 0,
            5 => bad.messages.push(bad.messages[0].clone()),
            _ => bad.context.as_mut().unwrap().protocol_version = 2,
        };
        assert!(validate_acknowledgements(&bad).is_err());
    }
    let mut rejected = request;
    rejected.messages[0].disposition = 2;
    assert!(validate_acknowledgements(&rejected).is_ok());
}
