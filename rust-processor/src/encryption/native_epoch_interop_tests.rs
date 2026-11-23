//! Opt-in real Go/TLS/PostgreSQL protected initial0 send/receive/ACK acceptance.
//! Signed recovery + protected epoch transition, exact old-send settlement and real epoch1 crypto.
use super::*;
use crate::encryption::{
    discovery::TrustedServer,
    protected::{RecordKey, RecordKind},
    provisioning::{DeviceProvisioning, Reconciliation},
    recovery_contract as recovery,
    signal_state::{CryptoCoordinator, EpochQuarantineReason, EpochRecoveryDecision},
};
use libsignal_protocol::IdentityKeyPair;
use std::io::Read;

async fn pending_id(client: &mut NativeEpochClient, id: &str) -> EpochDelivery {
    client
        .pending_deliveries()
        .await
        .unwrap()
        .into_iter()
        .find(|d| d.response().envelope.as_ref().unwrap().message_id == id)
        .unwrap()
}

fn text_payload(
    s: &crate::encryption::signal_state::EpochScope,
    text: &str,
) -> v2::EncryptedPayload {
    v2::EncryptedPayload {
        format_version: 2,
        message_id: Uuid::new_v4().to_string(),
        conversation_id: s.conversation_id().into(),
        sender: Some(s.own_device().clone()),
        recipient: Some(s.peer_device().clone()),
        created_at_unix_ms: 1_700_000_000_002,
        content: Some(v2::encrypted_payload::Content::Text(common::TextMessage {
            text: text.into(),
        })),
        crypto_epoch: Some(s.crypto_epoch()),
    }
}
async fn commit_send(
    client: &mut NativeEpochClient,
    worker: &CryptoCoordinator,
    plan: &crate::encryption::signal_state::EpochOutgoingPlan,
) {
    let EpochPreparedSendOutcome::Accepted(receipt) = client.send_prepared(plan).await.unwrap()
    else {
        panic!("expected durable receipt")
    };
    worker.accept_epoch_send_receipt(*receipt).await.unwrap();
}

#[test]
#[ignore = "requires the Go v2 TLS/PostgreSQL fixture"]
fn go_epoch_transport_worker() {
    let mut input = Zeroizing::new(String::new());
    std::io::stdin()
        .take(24576)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input.lines().collect();
    assert_eq!(fields.len(), 5);
    let ca = std::fs::read(fields[1]).unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    // Execute phases sequentially at the runtime root. A giant enclosing debug
    // generator otherwise adds its stack frame to every nested crypto/RPC poll.
    let RunnerFixture {
        root,
        scopes,
        devices,
        mut accounts,
        mut clients,
        workers,
        chat,
    } = runtime.block_on(Box::pin(initial_epoch_fixture(&fields, &ca)));
    runtime.block_on(Box::pin(runner_recovery_fixture(
        root.path(),
        &scopes,
        &devices,
        (
            accounts.0.device_client().unwrap(),
            accounts.1.device_client().unwrap(),
        ),
        (&mut clients.0, &mut clients.1),
        workers,
        &chat,
    )));
    println!("zchat-native-epoch-transport-complete:v2");
}
struct RunnerFixture {
    root: tempfile::TempDir,
    scopes: Vec<StoreScope>,
    devices: Vec<common::DeviceRef>,
    accounts: (
        crate::encryption::discovery::DiscoveredAccount,
        crate::encryption::discovery::DiscoveredAccount,
    ),
    clients: (NativeEpochClient, NativeEpochClient),
    workers: (CryptoCoordinator, CryptoCoordinator),
    chat: String,
}
async fn initial_epoch_fixture(fields: &[&str], ca: &[u8]) -> RunnerFixture {
    let root = tempfile::tempdir().unwrap();
    let origin = "https://epoch-interop.test";
    let mut accounts = Vec::new();
    let mut stores = Vec::new();
    let mut devices = Vec::new();
    let mut identities = Vec::new();
    let mut scopes = Vec::new();
    for token in [fields[2], fields[3]] {
        let mut account = TrustedServer::new(origin, fields[0], Some(ca))
            .unwrap()
            .discover(origin, Zeroizing::new(token.into()))
            .await
            .unwrap();
        assert!(account.state().active_device.is_none());
        let scope = account.scope();
        let mut local = DeviceProvisioning::new(crate::encryption::native::tests::local_store(
            root.path(),
            scope.clone(),
            true,
        ));
        assert_eq!(
            account.resume_initial(&mut local).await.unwrap(),
            Reconciliation::Ready
        );
        let own = local.ready_device().unwrap();
        let mut store = local.into_store();
        // Test-only signer for adversarial/late raw controls below. Normal
        // proposal and acceptance use the protected account worker lane.
        let bytes = store
            .transaction(|tx| {
                Ok(tx
                    .get(&RecordKey::new(
                        &own.generation_id,
                        RecordKind::Identity,
                        "local",
                    )?)?
                    .unwrap())
            })
            .unwrap();
        assert_eq!(bytes[0], 1);
        let identity = IdentityKeyPair::try_from(&bytes[5..]).unwrap();
        drop(bytes);
        scopes.push(scope);
        devices.push(own);
        identities.push(identity);
        stores.push(store);
        accounts.push(account);
    }
    assert_ne!(
        scopes[0].database_path(root.path()),
        scopes[1].database_path(root.path())
    );
    let wa = CryptoCoordinator::start(stores.remove(0), devices[0].clone())
        .await
        .unwrap();
    let wb = CryptoCoordinator::start(stores.remove(0), devices[1].clone())
        .await
        .unwrap();
    let mut aa = accounts.remove(0);
    let mut bb = accounts.remove(0);
    let ra = aa.device_client().unwrap();
    let rb = bb.device_client().unwrap();
    let chat = ra
        .create_conversation(&wa, fields[4].parse().unwrap())
        .await
        .unwrap();
    let peer_view = rb
        .get_conversation(&wb, &chat.conversation_id)
        .await
        .unwrap();
    let pa = EpochConversation::new(
        chat.clone(),
        *identities[0].identity_key(),
        *identities[1].identity_key(),
    )
    .unwrap();
    let pb = EpochConversation::new(
        peer_view,
        *identities[1].identity_key(),
        *identities[0].identity_key(),
    )
    .unwrap();
    let mut a = ra.epoch_transport().unwrap();
    let mut b = rb.epoch_transport().unwrap();
    let caps = a.capabilities().await.unwrap();
    assert_eq!(caps.protocol_version, 2);
    assert!(!caps.messaging_available && !caps.recovery_available);
    // First actual reply has missing optional epoch0; it must be rejected.
    assert!(matches!(
        a.catalog(&pa, None, 1).await,
        Err(RpcError::Invalid)
    ));
    let query = wa
        .prepare_initial_epoch(chat.conversation_id.clone())
        .await
        .unwrap();
    assert!(
        wa.epoch_scope(chat.conversation_id.clone(), 0)
            .await
            .is_err()
    );
    let initial = a.initial_epoch_catalog(&query).await.unwrap();
    assert_eq!(initial.response().active_epoch, Some(0));
    assert_eq!(initial.response().epochs[0].epoch, Some(0));
    let local_epoch = wa.accept_initial_epoch(initial).await.unwrap();
    assert_eq!(local_epoch.crypto_epoch(), 0);
    let query = wb
        .prepare_initial_epoch(chat.conversation_id.clone())
        .await
        .unwrap();
    let incoming = b.initial_epoch_catalog(&query).await.unwrap();
    let peer_epoch = wb.accept_initial_epoch(incoming).await.unwrap();
    assert_ne!(
        local_epoch.session_record_id(),
        peer_epoch.session_record_id()
    );
    // Server flips a real signed-prekey signature in the first reply only.
    let acquisition = wa
        .prepare_epoch_acquisition(local_epoch.clone())
        .await
        .unwrap();
    assert!(matches!(
        a.acquire_prepared(&acquisition).await,
        Err(RpcError::Invalid)
    ));
    // A wrong-epoch detail is a generic error, never renewal authority.
    assert!(matches!(
        a.acquire_prepared(&acquisition).await,
        Err(RpcError::Status(Code::FailedPrecondition))
    ));
    let EpochAcquisitionOutcome::Expired(expired) = a.acquire_prepared(&acquisition).await.unwrap()
    else {
        panic!("expected exact authoritative expiry");
    };
    let renewed = wa.renew_epoch_acquisition(*expired).await.unwrap();
    let mut expected = acquisition.request().clone();
    assert_ne!(renewed.request().acquisition_id, expected.acquisition_id);
    expected.acquisition_id = renewed.request().acquisition_id.clone();
    assert_eq!(renewed.request(), &expected);
    let EpochAcquisitionOutcome::Expired(expired) = a.acquire_prepared(&acquisition).await.unwrap()
    else {
        panic!("expected expiry of SAME original request");
    };
    assert_eq!(
        wa.renew_epoch_acquisition(*expired)
            .await
            .unwrap()
            .request(),
        &expected
    );
    // Lose the local renewal result, reopen the same protected account.
    wa.shutdown().await.unwrap();
    let wa = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let acquisition = wa
        .prepare_epoch_acquisition(local_epoch.clone())
        .await
        .unwrap();
    assert_eq!(acquisition.request(), &expected);
    let EpochAcquisitionOutcome::Bundle(bundle) = a.acquire_prepared(&acquisition).await.unwrap()
    else {
        panic!("expected acquired bundle");
    };
    assert_eq!(bundle.response().crypto_epoch, Some(0));
    wa.bootstrap_epoch(*bundle).await.unwrap();
    let applied = wa
        .prepare_epoch_acquisition(local_epoch.clone())
        .await
        .unwrap();
    assert!(applied.applied());
    assert_eq!(applied.request(), acquisition.request());
    println!("zchat-native-epoch0-protected-acquisition-renewal:v2");
    let acquisition = acquisition.request().clone();
    let outgoing = wa
        .encrypt_epoch(
            local_epoch.clone(),
            v2::EncryptedPayload {
                format_version: 2,
                message_id: Uuid::new_v4().to_string(),
                conversation_id: chat.conversation_id.clone(),
                sender: Some(devices[0].clone()),
                recipient: Some(devices[1].clone()),
                created_at_unix_ms: 1_700_000_000_000,
                content: Some(v2::encrypted_payload::Content::Text(common::TextMessage {
                    text: "durable real v2 outbox interop".into(),
                })),
                crypto_epoch: Some(0),
            },
        )
        .await
        .unwrap();
    let old = outgoing.request().clone();
    assert!(matches!(
        a.send_prepared(&outgoing).await,
        Err(RpcError::Status(Code::Unavailable))
    ));
    wa.shutdown().await.unwrap();
    let wa = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let restored = wa
        .epoch_outgoing_plan(old.envelope.as_ref().unwrap().message_id.clone())
        .await
        .unwrap();
    assert_eq!(restored.request(), &old);
    assert!(restored.receipt().is_none());
    let EpochPreparedSendOutcome::Accepted(committed) = a.send_prepared(&restored).await.unwrap()
    else {
        panic!("expected prepared receipt");
    };
    let original = committed.response().clone();
    wa.accept_epoch_send_receipt(*committed).await.unwrap();
    let known = wa
        .epoch_outgoing_plan(old.envelope.as_ref().unwrap().message_id.clone())
        .await
        .unwrap();
    assert!(!known.needs_send());
    assert!(matches!(
        a.send_prepared(&known).await,
        Err(RpcError::Invalid)
    ));
    assert!(matches!(
        a.refresh_prepared(std::slice::from_ref(&known)).await,
        Err(RpcError::Invalid)
    ));
    let pending = wa
        .accept_epoch_statuses(
            a.refresh_prepared(std::slice::from_ref(&known))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        pending[0].status().unwrap().state,
        common::MessageState::Pending as i32
    );
    assert!(pending[0].needs_status_poll());

    // Receive the PreKey before server sealing, but keep its server
    // copy pending to exercise retained exact receive/ACK afterwards.
    assert!(matches!(
        b.pending_deliveries().await,
        Err(RpcError::Invalid)
    ));
    let mut initial_delivery = b.pending_deliveries().await.unwrap();
    assert_eq!(initial_delivery.len(), 1);
    wb.receive_epoch(peer_epoch.clone(), initial_delivery.remove(0))
        .await
        .unwrap();
    let mut replies = wb.epoch_outbox_page(None).await.unwrap().messages;
    assert_eq!(replies.len(), 1);
    let reply = replies.remove(0);
    let exact_reply = reply.request().clone();
    assert!(matches!(
        b.send_prepared(&reply).await.unwrap(),
        EpochPreparedSendOutcome::Accepted(_)
    ));
    // Deliberately lose the successful receipt before local COMMIT.
    wb.shutdown().await.unwrap();
    let wb = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    let restored_reply = wb
        .epoch_outgoing_plan(exact_reply.envelope.as_ref().unwrap().message_id.clone())
        .await
        .unwrap();
    assert_eq!(restored_reply.request(), &exact_reply);
    assert!(restored_reply.needs_send());
    let EpochPreparedSendOutcome::Accepted(receipt) =
        b.send_prepared(&restored_reply).await.unwrap()
    else {
        panic!("confirmation receipt")
    };
    wb.accept_epoch_send_receipt(*receipt).await.unwrap();
    let mut controls = a.pending_deliveries().await.unwrap();
    assert_eq!(controls.len(), 1);
    let (control, ack) = wa
        .receive_epoch(local_epoch.clone(), controls.remove(0))
        .await
        .unwrap();
    assert!(
        matches!(control.content, Some(v2::encrypted_payload::Content::SessionConfirmation(ref c)) if c.in_reply_to_message_id == old.envelope.as_ref().unwrap().message_id && c.crypto_epoch == Some(0))
    );
    wa.accept_epoch_ack(a.acknowledge_prepared(&ack).await.unwrap())
        .await
        .unwrap();
    wb.accept_epoch_statuses(
        b.refresh_prepared(std::slice::from_ref(&restored_reply))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert!(
        wb.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    // No confirmation loop: Alice has only her original text in outbox.
    assert_eq!(wa.epoch_outbox_page(None).await.unwrap().messages.len(), 1);
    // Deliberately invalid peer ciphertext through the REAL v2 server.
    // No protected outgoing plan is claimed for this negative fixture.
    let mut bad = old.clone();
    let envelope = bad.envelope.as_mut().unwrap();
    envelope.message_id = Uuid::new_v4().to_string();
    envelope.message_type = common::CiphertextType::Signal as i32;
    envelope.ciphertext = vec![1];
    let bad_id = envelope.message_id.clone();
    assert!(matches!(
        a.send(&pa, &bad).await.unwrap(),
        EpochSendOutcome::Accepted(_)
    ));
    let bad_delivery = pending_id(&mut b, &bad_id).await;
    let retained = bad_delivery.response().clone();
    assert!(
        wb.receive_epoch(peer_epoch.clone(), bad_delivery)
            .await
            .is_err()
    );
    assert!(wb.epoch_ack(bad_id.clone()).await.is_err()); // No automatic rejection.
    let rejected = wb
        .quarantine_epoch(
            peer_epoch.clone(),
            pending_id(&mut b, &bad_id).await,
            EpochQuarantineReason::InvalidCiphertext,
        )
        .await
        .unwrap();
    assert!(rejected.rejected() && !rejected.acknowledged());
    assert!(matches!(
        wb.receive_epoch(peer_epoch.clone(), pending_id(&mut b, &bad_id).await)
            .await,
        Err(crate::encryption::signal_state::CryptoError::Quarantined)
    ));
    // Lose the successful rejected ACK result before local COMMIT.
    b.acknowledge_prepared(&rejected).await.unwrap();
    wb.shutdown().await.unwrap();
    let wb = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    let rejected = wb.epoch_ack(bad_id.clone()).await.unwrap();
    assert!(rejected.rejected() && !rejected.acknowledged());
    assert_eq!(
        wb.epoch_quarantined_message(bad_id.clone())
            .await
            .unwrap()
            .delivery(),
        &retained
    );
    wb.accept_epoch_ack(b.acknowledge_prepared(&rejected).await.unwrap())
        .await
        .unwrap();
    assert!(wb.epoch_ack(bad_id.clone()).await.unwrap().acknowledged());
    assert!(matches!(
        wb.epoch_incoming_message(bad_id.clone()).await,
        Err(crate::encryption::signal_state::CryptoError::Quarantined)
    ));
    let rejected_status = a
        .refresh_outgoing(std::slice::from_ref(&bad))
        .await
        .unwrap();
    assert_eq!(
        rejected_status.response().messages[0].state,
        common::MessageState::RecipientRejected as i32
    );
    println!("zchat-native-epoch0-protected-quarantine:v2");
    let following = wa
        .encrypt_epoch(
            local_epoch.clone(),
            v2::EncryptedPayload {
                format_version: 2,
                message_id: Uuid::new_v4().to_string(),
                conversation_id: chat.conversation_id.clone(),
                sender: Some(devices[0].clone()),
                recipient: Some(devices[1].clone()),
                created_at_unix_ms: 1_700_000_000_001,
                content: Some(v2::encrypted_payload::Content::Text(common::TextMessage {
                    text: "Signal after automatic confirmation".into(),
                })),
                crypto_epoch: Some(0),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        following.request().envelope.as_ref().unwrap().message_type,
        common::CiphertextType::Signal as i32
    );
    let EpochPreparedSendOutcome::Accepted(receipt) = a.send_prepared(&following).await.unwrap()
    else {
        panic!("following receipt")
    };
    // The server committed this old Signal; deliberately lose its receipt locally.
    drop(receipt);
    let unsent = wa
        .encrypt_epoch(
            local_epoch.clone(),
            text_payload(&local_epoch, "old unknown send fenced during recovery"),
        )
        .await
        .unwrap();
    println!("zchat-native-epoch0-automatic-confirmation:v2");

    let intent = wa
        .prepare_epoch_recovery(local_epoch.clone())
        .await
        .unwrap();
    let proposal = intent.proposal().clone();
    assert!(
        wa.epoch_scope(chat.conversation_id.clone(), 0)
            .await
            .unwrap()
            .sending_frozen()
    );
    assert!(matches!(
        a.submit_recovery(&intent).await,
        Err(EpochRecoveryError::Rpc(RpcError::Status(Code::Unavailable)))
    ));
    wa.shutdown().await.unwrap();
    let wa = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let replay = wa
        .prepare_epoch_recovery(local_epoch.clone())
        .await
        .unwrap();
    assert_eq!(intent.request(), replay.request());
    let pending = a.submit_recovery(&replay).await.unwrap();
    let pending_record = pending.response().clone();
    assert_eq!(
        wa.accept_epoch_recovery_result(pending)
            .await
            .unwrap()
            .state(),
        Some(common::RecoveryState::Pending)
    );
    let discovery = b.pending_recoveries("", 1).await.unwrap();
    assert_eq!(discovery.proposals.len(), 1);
    assert!(!discovery.proposals[0].initiated_by_own_device);
    let id = proposal.control.as_ref().unwrap().proposal_id.as_str();
    let query = wb
        .prepare_epoch_recovery_query(peer_epoch.clone(), id.into())
        .await
        .unwrap();
    assert!(matches!(
        b.query_recovery(&query).await,
        Err(RpcError::Invalid)
    ));
    let incoming = b.query_recovery(&query).await.unwrap();
    assert_eq!(incoming.response(), &pending_record);
    assert!(
        !wb.accept_epoch_recovery_query(incoming)
            .await
            .unwrap()
            .sending_frozen()
    );
    let mut competing = proposal.control.clone().unwrap();
    competing.request_id = Uuid::new_v4().to_string();
    competing.proposal_id = competing.request_id.clone();
    let competing = recovery::sign_control(competing, &identities[0]).unwrap();
    let Err(EpochRecoveryError::Conflict(conflict)) = a.propose_recovery(&pa, &competing).await
    else {
        panic!("expected advisory conflict")
    };
    assert_eq!(conflict.pending_proposal(), Some(id));
    // This is the explicit trusted-owner acceptance action, not automatic consent.
    let decision = wb
        .prepare_epoch_recovery_decision(
            peer_epoch.clone(),
            id.into(),
            EpochRecoveryDecision::Accept,
        )
        .await
        .unwrap();
    assert!(matches!(
        b.submit_recovery(&decision).await,
        Err(EpochRecoveryError::Rpc(RpcError::Status(Code::Unavailable)))
    ));
    wb.shutdown().await.unwrap();
    let wb = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    let retry = wb
        .prepare_epoch_recovery_decision(
            peer_epoch.clone(),
            id.into(),
            EpochRecoveryDecision::Accept,
        )
        .await
        .unwrap();
    assert_eq!(retry.request(), decision.request());
    let accepted = b.submit_recovery(&retry).await.unwrap();
    let accepted_record = accepted.response().clone();
    assert_eq!(accepted_record.outcome.as_ref().unwrap().new_epoch, 1);
    assert!(
        wb.accept_epoch_recovery_result(accepted)
            .await
            .unwrap()
            .sending_frozen()
    );
    let query = wa
        .prepare_epoch_recovery_query(local_epoch.clone(), id.into())
        .await
        .unwrap();
    let exact = a.query_recovery(&query).await.unwrap();
    assert_eq!(exact.response(), &accepted_record);
    assert!(
        wa.accept_epoch_recovery_query(exact)
            .await
            .unwrap()
            .sending_frozen()
    );
    println!("zchat-native-protected-recovery-intent:v2");
    let mut cancel = proposal.control.clone().unwrap();
    cancel.action = common::RecoveryAction::Cancel as i32;
    cancel.request_id = Uuid::new_v4().to_string();
    cancel.proposal_digest = recovery::control_digest(proposal.control.as_ref().unwrap())
        .unwrap()
        .to_vec();
    let cancel = recovery::sign_control(cancel, &identities[0]).unwrap();
    assert_eq!(
        a.decide_recovery(&pa, &proposal, &cancel)
            .await
            .unwrap()
            .response(),
        &accepted_record
    );
    let EpochSendOutcome::Accepted(replay) = a.send(&pa, &old).await.unwrap() else {
        panic!("accepted replay became fence")
    };
    assert_eq!(&original, replay.response());
    let qa = wa
        .prepare_epoch_transition(local_epoch.clone(), id.into())
        .await
        .unwrap();
    let qb = wb
        .prepare_epoch_transition(peer_epoch.clone(), id.into())
        .await
        .unwrap();
    wa.accept_epoch_transition_catalog(a.transition_catalog(&qa).await.unwrap())
        .await
        .unwrap();
    wb.accept_epoch_transition_catalog(b.transition_catalog(&qb).await.unwrap())
        .await
        .unwrap();
    let progress = wa
        .advance_epoch_transition(local_epoch.clone(), id.into())
        .await
        .unwrap();
    assert_eq!(
        progress.blocked_message(),
        Some(
            unsent
                .request()
                .envelope
                .as_ref()
                .unwrap()
                .message_id
                .as_str()
        )
    );
    // The first fence has a forged signature: generic failure cannot settle the outbox.
    assert!(matches!(
        a.send_prepared(&unsent).await,
        Err(RpcError::Status(Code::FailedPrecondition))
    ));
    let EpochPreparedSendOutcome::Sealed(fence) = a.send_prepared(&unsent).await.unwrap() else {
        panic!("bound fence")
    };
    wa.accept_epoch_send_fence(*fence).await.unwrap();
    let progress = wa
        .advance_epoch_transition(local_epoch.clone(), id.into())
        .await
        .unwrap();
    assert_eq!(
        progress.blocked_message(),
        Some(
            following
                .request()
                .envelope
                .as_ref()
                .unwrap()
                .message_id
                .as_str()
        )
    );
    // Exact old retry finds the original COMMIT before the sealed epoch gate.
    let EpochPreparedSendOutcome::Accepted(receipt) = a.send_prepared(&following).await.unwrap()
    else {
        panic!("retained receipt")
    };
    wa.accept_epoch_send_receipt(*receipt).await.unwrap();
    assert!(
        wa.advance_epoch_transition(local_epoch.clone(), id.into())
            .await
            .unwrap()
            .activated()
    );
    assert!(
        wb.advance_epoch_transition(peer_epoch.clone(), id.into())
            .await
            .unwrap()
            .activated()
    );
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
    let wa = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let wb = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    let a1 = wa
        .epoch_scope(chat.conversation_id.clone(), 1)
        .await
        .unwrap();
    let b1 = wb
        .epoch_scope(chat.conversation_id.clone(), 1)
        .await
        .unwrap();
    assert!(
        wa.epoch_scope(chat.conversation_id.clone(), 0)
            .await
            .unwrap()
            .sealed()
    );
    assert!(
        wb.epoch_scope(chat.conversation_id.clone(), 0)
            .await
            .unwrap()
            .sealed()
    );
    let settled = wa
        .epoch_outgoing_plan(
            unsent
                .request()
                .envelope
                .as_ref()
                .unwrap()
                .message_id
                .clone(),
        )
        .await
        .unwrap();
    assert_eq!(settled.request(), unsent.request());
    assert!(
        !settled.needs_send()
            && !settled.needs_status_poll()
            && settled.receipt().is_none()
            && settled.sealed_failure().is_some()
    );
    assert!(matches!(
        a.acquire(&pa, &acquisition).await,
        Err(RpcError::Status(Code::FailedPrecondition))
    ));
    let next = wa.prepare_epoch_acquisition(a1.clone()).await.unwrap();
    assert!(wb.prepare_epoch_acquisition(b1.clone()).await.is_err());
    let peer_next = v2::AcquireBundleRequest {
        context: Some(b.context()),
        acquisition_id: Uuid::new_v4().to_string(),
        expected_peer: Some(devices[0].clone()),
        ..next.request().clone()
    };
    assert!(matches!(
        b.acquire(&pb, &peer_next).await,
        Err(RpcError::Status(Code::FailedPrecondition))
    ));
    let EpochAcquisitionOutcome::Bundle(bundle) = a.acquire_prepared(&next).await.unwrap() else {
        panic!("new epoch bundle")
    };
    wa.bootstrap_epoch(*bundle).await.unwrap();
    let new = wa
        .encrypt_epoch(a1.clone(), text_payload(&a1, "real encrypted epoch1 text"))
        .await
        .unwrap();
    assert_eq!(
        new.request().envelope.as_ref().unwrap().message_type,
        common::CiphertextType::PrekeySignal as i32
    );
    commit_send(&mut a, &wa, &new).await;
    let page = a.catalog(&pa, None, 1).await.unwrap();
    assert_eq!(page.response().active_epoch, Some(1));
    assert_eq!(page.response().next_after_epoch, Some(0));
    assert!(page.response().epochs[0].sealed);
    assert_eq!(
        a.catalog(&pa, Some(0), 1).await.unwrap().response().epochs[0].epoch,
        Some(1)
    );
    assert!(
        b.pending_recoveries("", 32)
            .await
            .unwrap()
            .proposals
            .is_empty()
    );
    let sends = [old.clone(), new.request().clone()];
    let states = a.refresh_outgoing(&sends).await.unwrap();
    assert_eq!(states.request()[0].epoch, 0);
    assert_eq!(states.request()[1].epoch, 1);
    let mut inbox = b.pending_deliveries().await.unwrap();
    assert_eq!(inbox.len(), 3);
    let old_delivery = inbox.remove(0);
    let following_delivery = inbox.remove(0);
    let fresh = inbox.remove(0);
    assert_eq!(
        old_delivery
            .response()
            .envelope
            .as_ref()
            .unwrap()
            .crypto_epoch,
        Some(0)
    );
    assert_eq!(
        fresh.response().envelope.as_ref().unwrap().crypto_epoch,
        Some(1)
    );
    assert!(wb.receive_epoch(peer_epoch.clone(), fresh).await.is_err());
    let new_id = new.request().envelope.as_ref().unwrap().message_id.clone();
    let (plain, proof) = wb
        .receive_epoch(b1.clone(), pending_id(&mut b, &new_id).await)
        .await
        .unwrap();
    assert!(
        matches!(plain.content,Some(v2::encrypted_payload::Content::Text(ref t)) if t.text=="real encrypted epoch1 text")
    );
    wb.accept_epoch_ack(b.acknowledge_prepared(&proof).await.unwrap())
        .await
        .unwrap();
    let mut replies = wb.epoch_outbox_page(None).await.unwrap().messages;
    assert_eq!(replies.len(), 1);
    let confirmation = replies.remove(0);
    assert_eq!(
        confirmation
            .request()
            .envelope
            .as_ref()
            .unwrap()
            .crypto_epoch,
        Some(1)
    );
    commit_send(&mut b, &wb, &confirmation).await;
    let reply_id = confirmation
        .request()
        .envelope
        .as_ref()
        .unwrap()
        .message_id
        .clone();
    let (_, proof) = wa
        .receive_epoch(a1.clone(), pending_id(&mut a, &reply_id).await)
        .await
        .unwrap();
    wa.accept_epoch_ack(a.acknowledge_prepared(&proof).await.unwrap())
        .await
        .unwrap();
    wb.accept_epoch_statuses(
        b.refresh_prepared(std::slice::from_ref(&confirmation))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    // New ratchet exists before replaying/decrypting old epoch ciphertext below.
    let (received, proof) = wb
        .receive_epoch(peer_epoch.clone(), old_delivery)
        .await
        .unwrap();
    assert!(
        matches!(received.content.as_ref(),Some(v2::encrypted_payload::Content::Text(t)) if t.text=="durable real v2 outbox interop")
    );
    let received_id = proof.message_id().to_string();
    wb.shutdown().await.unwrap();
    let wb = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        wb.epoch_incoming_message(received_id.clone())
            .await
            .unwrap(),
        received
    );
    let proof = wb.epoch_ack(received_id.clone()).await.unwrap();
    assert!(matches!(
        b.acknowledge_prepared(&proof).await,
        Err(RpcError::Status(Code::Unavailable))
    ));
    assert_eq!(
        wb.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    wb.shutdown().await.unwrap();
    let wb = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    let proof = wb.epoch_ack(received_id.clone()).await.unwrap();
    wb.accept_epoch_ack(b.acknowledge_prepared(&proof).await.unwrap())
        .await
        .unwrap();
    wb.accept_epoch_ack(b.acknowledge_prepared(&proof).await.unwrap())
        .await
        .unwrap();
    assert!(
        wb.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert!(
        wb.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    // A fresh accepted Signal text remains receivable after server seal.
    let (plain, ack) = wb
        .receive_epoch(peer_epoch.clone(), following_delivery)
        .await
        .unwrap();
    assert!(
        matches!(plain.content,Some(v2::encrypted_payload::Content::Text(ref t)) if t.text=="Signal after automatic confirmation")
    );
    wb.accept_epoch_ack(b.acknowledge_prepared(&ack).await.unwrap())
        .await
        .unwrap();
    assert!(
        wb.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    wa.accept_epoch_statuses(
        a.refresh_prepared(std::slice::from_ref(&following))
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    let signal = wa
        .encrypt_epoch(
            a1.clone(),
            text_payload(&a1, "new Signal after old epoch receive"),
        )
        .await
        .unwrap();
    assert_eq!(
        signal.request().envelope.as_ref().unwrap().message_type,
        common::CiphertextType::Signal as i32
    );
    commit_send(&mut a, &wa, &signal).await;
    let signal_id = signal
        .request()
        .envelope
        .as_ref()
        .unwrap()
        .message_id
        .clone();
    let (plain, proof) = wb
        .receive_epoch(b1, pending_id(&mut b, &signal_id).await)
        .await
        .unwrap();
    assert!(
        matches!(plain.content,Some(v2::encrypted_payload::Content::Text(ref t)) if t.text=="new Signal after old epoch receive")
    );
    wb.accept_epoch_ack(b.acknowledge_prepared(&proof).await.unwrap())
        .await
        .unwrap();
    wa.accept_epoch_statuses(a.refresh_prepared(&[new.clone(), signal]).await.unwrap())
        .await
        .unwrap();
    assert!(
        wb.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    println!("zchat-native-protected-epoch-transition:v2");
    assert!(b.pending().await.unwrap().messages.is_empty());
    let states = a.refresh_outgoing(&sends).await.unwrap();
    assert!(
        states
            .response()
            .messages
            .iter()
            .all(|m| m.state == common::MessageState::RecipientStored as i32)
    );
    let completed = wa
        .accept_epoch_statuses(
            a.refresh_prepared(std::slice::from_ref(&known))
                .await
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        completed[0].status().unwrap().state,
        common::MessageState::RecipientStored as i32
    );
    assert!(!completed[0].needs_send() && !completed[0].needs_status_poll());
    assert!(
        wa.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert_eq!(a.context().device_generation_id, devices[0].generation_id);
    assert_eq!(b.context().device_generation_id, devices[1].generation_id);
    let query = wa
        .prepare_initial_epoch(chat.conversation_id.clone())
        .await
        .unwrap();
    let ahead = a.initial_epoch_catalog(&query).await.unwrap();
    assert!(matches!(
        wa.accept_initial_epoch(ahead).await,
        Err(crate::encryption::signal_state::CryptoError::EpochRecoveryRequired)
    ));
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
    let reopened = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let persisted = reopened
        .epoch_scope(chat.conversation_id.clone(), 0)
        .await
        .unwrap();
    assert_eq!(
        persisted.session_record_id(),
        local_epoch.session_record_id()
    );
    assert!(persisted.sealed());
    assert!(
        !reopened
            .epoch_scope(chat.conversation_id.clone(), 1)
            .await
            .unwrap()
            .sealed()
    );
    let applied = reopened.prepare_epoch_acquisition(persisted).await.unwrap();
    assert!(applied.applied());
    assert_eq!(applied.request(), &acquisition);
    let restored = reopened
        .epoch_outgoing_plan(old.envelope.as_ref().unwrap().message_id.clone())
        .await
        .unwrap();
    assert_eq!(restored.request(), &old);
    assert_eq!(restored.receipt(), Some(&original));
    assert_eq!(
        restored.status().unwrap().state,
        common::MessageState::RecipientStored as i32
    );
    assert!(!restored.needs_send() && !restored.needs_status_poll());
    assert!(
        reopened
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    println!("zchat-native-epoch0-protected-status:v2");

    assert!(
        reopened
            .outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let recipient = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root.path(), scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    assert_eq!(
        recipient
            .epoch_incoming_message(received_id.clone())
            .await
            .unwrap(),
        received
    );
    assert!(
        recipient
            .epoch_ack(received_id)
            .await
            .unwrap()
            .acknowledged()
    );
    assert!(
        recipient
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    println!("zchat-native-epoch0-protected-receive-ack:v2");
    println!("zchat-native-epoch0-durable-outbox:v2");
    println!("zchat-native-epoch0-signal-bootstrap:v2");
    println!("zchat-native-epoch0-local-commit:v2");

    RunnerFixture {
        root,
        scopes,
        devices,
        accounts: (aa, bb),
        clients: (a, b),
        workers: (reopened, recipient),
        chat: chat.conversation_id,
    }
}

// Explicit UI-like actions stay outside the runner. All subsequent delivery,
// saved command replay, transition progress and new bootstrap use production APIs.
async fn runner_recovery_fixture(
    root: &std::path::Path,
    scopes: &[StoreScope],
    devices: &[common::DeviceRef],
    registry: (&mut NativeV1Client, &mut NativeV1Client),
    clients: (&mut NativeEpochClient, &mut NativeEpochClient),
    workers: (CryptoCoordinator, CryptoCoordinator),
    chat: &str,
) {
    use crate::encryption::{
        delivery_runner::RunnerConfig,
        epoch_runner::{EpochDeliveryRunner, MAX_ROUND_RPCS, NativeEpochDelivery},
    };
    let (wa, wb) = workers;
    let (a, b) = clients;
    let a1 = wa.active_epoch_scope(chat.into()).await.unwrap().unwrap();
    let b1 = wb.active_epoch_scope(chat.into()).await.unwrap().unwrap();
    assert_eq!(a1.crypto_epoch(), 1);
    let accepted_text = text_payload(&a1, "runner retains old committed text");
    let old = wa
        .encrypt_epoch(a1.clone(), accepted_text.clone())
        .await
        .unwrap();
    assert!(matches!(
        a.send_prepared(&old).await.unwrap(),
        EpochPreparedSendOutcome::Accepted(_)
    ));
    // Lose that receipt locally and leave another message entirely unsent.
    let unsent = wa
        .encrypt_epoch(a1.clone(), text_payload(&a1, "runner exact old fence"))
        .await
        .unwrap();
    let p = wa.prepare_epoch_recovery(a1.clone()).await.unwrap();
    let id = p.proposal().control.as_ref().unwrap().proposal_id.clone();
    assert!(matches!(
        a.submit_recovery(&p).await,
        Err(EpochRecoveryError::Rpc(RpcError::Status(Code::Unavailable)))
    ));
    let wakeups = registry.0.inbox_wakeups().unwrap();
    let mut ra = EpochDeliveryRunner::new(
        NativeEpochDelivery::new(registry.0).unwrap(),
        RunnerConfig {
            poll_interval: std::time::Duration::from_millis(20),
            ..RunnerConfig::default()
        },
    )
    .unwrap();
    let mut rb = EpochDeliveryRunner::new(
        NativeEpochDelivery::new(registry.1).unwrap(),
        RunnerConfig::default(),
    )
    .unwrap();
    let report = rb.poll_once(&wb).await.unwrap();
    assert!(report.rpc_attempts <= MAX_ROUND_RPCS);
    assert!(
        report
            .recovery
            .iter()
            .any(|n| n.snapshot.proposal_id() == id && n.snapshot.decision().is_none())
    );
    assert!(
        !wb.epoch_scope(chat.into(), 1)
            .await
            .unwrap()
            .sending_frozen()
    );
    let decision = wb
        .prepare_epoch_recovery_decision(b1, id, EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    assert!(matches!(
        b.submit_recovery(&decision).await,
        Err(EpochRecoveryError::Rpc(RpcError::Status(Code::Unavailable)))
    ));
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
    let wa = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root, scopes[0].clone(), false),
        devices[0].clone(),
    )
    .await
    .unwrap();
    let wb = CryptoCoordinator::start(
        crate::encryption::native::tests::local_store(root, scopes[1].clone(), false),
        devices[1].clone(),
    )
    .await
    .unwrap();
    let mut activated = 0;
    let mut sealed = 0;
    for _ in 0..8 {
        let r = ra.poll_once(&wa).await.unwrap();
        assert!(r.rpc_attempts <= MAX_ROUND_RPCS);
        activated += r.activated;
        sealed += r.sealed;
        if let Some(s) = wa.active_epoch_scope(chat.into()).await.unwrap()
            && s.crypto_epoch() == 2
            && wa.prepare_epoch_acquisition(s).await.unwrap().applied()
        {
            break;
        }
    }
    assert_eq!(activated, 1);
    assert_eq!(sealed, 1);
    let a2 = wa.epoch_scope(chat.into(), 2).await.unwrap();
    let fresh = text_payload(&a2, "real epoch2 text through production loop");
    let fresh_id = fresh.message_id.clone();
    wa.encrypt_epoch(a2.clone(), fresh.clone()).await.unwrap();
    // Go closes actual TLS sockets AFTER the first epoch2 Send COMMIT.
    let r = ra.poll_once(&wa).await.unwrap();
    assert!(r.rpc_attempts <= MAX_ROUND_RPCS);
    tokio::time::timeout(std::time::Duration::from_secs(35), async {
        for _ in 0..16 {
            let br = rb.poll_once(&wb).await.unwrap();
            let ar = ra.poll_once(&wa).await.unwrap();
            assert!(ar.rpc_attempts <= MAX_ROUND_RPCS && br.rpc_attempts <= MAX_ROUND_RPCS);
            // The first old ACK also loses its TLS connection after server COMMIT.
            if wb.epoch_incoming_message(fresh_id.clone()).await.is_ok()
                && wa
                    .epoch_outbox_page(None)
                    .await
                    .unwrap()
                    .messages
                    .is_empty()
                && wb
                    .epoch_outbox_page(None)
                    .await
                    .unwrap()
                    .messages
                    .is_empty()
                && wa
                    .epoch_ack_page(None)
                    .await
                    .unwrap()
                    .acknowledgements
                    .is_empty()
                && wb
                    .epoch_ack_page(None)
                    .await
                    .unwrap()
                    .acknowledgements
                    .is_empty()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(wb.epoch_incoming_message(fresh_id).await.unwrap(), fresh);
    assert_eq!(
        wb.epoch_incoming_message(accepted_text.message_id.clone())
            .await
            .unwrap(),
        accepted_text
    );
    let failed = wa
        .epoch_outgoing_plan(
            unsent
                .request()
                .envelope
                .as_ref()
                .unwrap()
                .message_id
                .clone(),
        )
        .await
        .unwrap();
    assert_eq!(failed.request(), unsent.request());
    assert!(
        failed.sealed_failure().is_some() && failed.receipt().is_none() && !failed.needs_send()
    );
    assert_eq!(
        wb.active_epoch_scope(chat.into())
            .await
            .unwrap()
            .unwrap()
            .crypto_epoch(),
        2
    );
    let final_text = text_payload(&a2, "Signal after runner reconnect");
    let final_id = final_text.message_id.clone();
    let signal = wa.encrypt_epoch(a2, final_text.clone()).await.unwrap();
    assert_eq!(
        signal.request().envelope.as_ref().unwrap().message_type,
        common::CiphertextType::Signal as i32
    );
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    {
        // Unavailable optional hints must not stop durable polling. The Go
        // fixture records the attempted stream, while this run sends final text.
        let running = ra.run_with_wakeups(&wa, stop, wakeups);
        tokio::pin!(running);
        let receive = async {
            for _ in 0..64 {
                rb.poll_once(&wb).await.unwrap();
                if wb.epoch_incoming_message(final_id.clone()).await.is_ok()
                    && wb.epoch_ack(final_id.clone()).await.unwrap().acknowledged()
                {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
            panic!("bounded runner did not deliver final Signal");
        };
        tokio::select! {
            result=&mut running=>panic!("runner stopped before delivery: {result:?}"),
            result=tokio::time::timeout(std::time::Duration::from_secs(10),receive)=>result.unwrap(),
        }
        stop_tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), running)
            .await
            .unwrap()
            .unwrap();
    }
    ra.poll_once(&wa).await.unwrap();
    assert_eq!(
        wb.epoch_incoming_message(final_id).await.unwrap(),
        final_text
    );
    assert!(
        wa.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert!(
        wb.epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert!(
        wa.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert!(
        wb.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    wa.shutdown().await.unwrap();
    wb.shutdown().await.unwrap();
    println!("zchat-native-epoch-runner-complete:v2");
}
