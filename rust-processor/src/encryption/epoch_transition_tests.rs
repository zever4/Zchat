use super::super::super::incoming::tests::{delivery, payload, prepared};
use super::super::super::tests::{Fixture, fixture, initialize, records};
use super::super::tests::{observed, query, response};
use super::*;
use crate::encryption::epoch_contract as data;
use crate::encryption::signal_state::tests::store;
use rusqlite::Connection;
const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";

#[path = "epoch_attachment_retirement_transition_tests.rs"]
mod attachment_retirement;
async fn accepted(f: &Fixture, a: &EpochScope, b: &EpochScope) -> v2::RecoveryRecord {
    accept_between(&f.alice, a, &f.bob, b).await
}

#[cfg(feature = "compression")]
#[tokio::test]
async fn photo_input_tests_epoch_transition_rejects_old_codec_result_without_retargeting_uuid() {
    use crate::{
        compression::PhotoChoice, encryption::photo_preparation::prepare_photo_attachment,
    };
    let (f, a, b) = prepared().await;
    let id = uuid::Uuid::new_v4().to_string();
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), id.clone())
        .await
        .unwrap()
        .unwrap();
    let r = accepted(&f, &a, &b).await;
    let proposal = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    let mut activated = false;
    for _ in 0..8 {
        if f.alice
            .advance_epoch_transition(a.clone(), proposal.clone())
            .await
            .unwrap()
            .activated()
        {
            activated = true;
            break;
        }
    }
    assert!(activated);
    let photo = || {
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::DynamicImage::new_rgb8(2, 3)
            .write_to(&mut bytes, image::ImageFormat::Png)
            .unwrap();
        prepare_photo_attachment(
            &bytes.into_inner(),
            Zeroizing::new("photo.png".into()),
            Zeroizing::new(String::new()),
            PhotoChoice::default(),
        )
        .unwrap()
    };
    let before = records(f.root.path(), A);
    let error = f
        .alice
        .finish_epoch_photo_input(ticket, photo())
        .await
        .err()
        .expect("old epoch must reject late input");
    assert!(
        matches!(error, CryptoError::EpochSendingFrozen),
        "old epoch rejection: {error:?}"
    );
    assert_eq!(records(f.root.path(), A), before);
    assert!(
        f.alice
            .prepared_epoch_attachment(id.clone())
            .await
            .unwrap()
            .is_none()
    );
    // A fresh explicit input may use epoch1; the late epoch0 result cannot.
    let ticket = f
        .alice
        .new_epoch_photo_input(CHAT.into(), id)
        .await
        .unwrap()
        .unwrap();
    let super::super::super::EpochAttachmentPreparation::Prepared(p) = f
        .alice
        .finish_epoch_photo_input(ticket, photo())
        .await
        .unwrap()
    else {
        panic!("missing fresh input");
    };
    assert_eq!(p.crypto_epoch(), 1);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn saved_attachment_download_plan_keeps_epoch_zero_after_both_peers_activate_epoch_one() {
    let (f, a, b) = prepared().await;
    let (p, file) = super::super::super::attachment_tests::message(&a);
    let id = p.message_id.clone();
    let sent = f.alice.encrypt_epoch(a.clone(), p).await.unwrap();
    let d = wire(&sent, 1);
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let old = f
        .bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    let r = accepted(&f, &a, &b).await;
    let proposal = control(&r).unwrap().proposal_id.clone();
    for (worker, scope) in [(&f.alice, &a), (&f.bob, &b)] {
        prepare(worker, scope, &r).await;
        for p in worker.epoch_outbox_page(None).await.unwrap().messages {
            store_receipt(worker, scope, &p).await;
        }
        let mut activated = false;
        for _ in 0..8 {
            if worker
                .advance_epoch_transition(scope.clone(), proposal.clone())
                .await
                .unwrap()
                .activated()
            {
                activated = true;
                break;
            }
        }
        assert!(activated);
        assert_eq!(
            worker
                .active_epoch_scope(CHAT.into())
                .await
                .unwrap()
                .unwrap()
                .crypto_epoch(),
            1
        );
    }
    let retained = f
        .bob
        .epoch_attachment_download_plan(id.clone())
        .await
        .unwrap();
    assert_eq!(retained.crypto_epoch(), 0);
    assert_eq!(
        retained.read_request().binding(),
        old.read_request().binding()
    );
    assert_eq!(retained.read_request().blob(), old.read_request().blob());
    // Real signed0->1 activation must not block FILEACK for a received epoch0 file.
    use super::super::super::attachment_download::tests::{append, begin, finish};
    let mut run = begin(&f.bob, retained).await;
    append(&f.bob, &mut run, file.blob()).await.unwrap();
    finish(&f.bob, run).await.unwrap();
    let proof = f.bob.epoch_file_ack(id.clone()).await.unwrap();
    assert_eq!(proof.request().files[0].binding.as_ref().unwrap().epoch, 0);
    assert!(
        f.bob
            .pending_epoch_file_acks(None)
            .await
            .unwrap()
            .messages
            .contains(&id)
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}
async fn accept_between(
    wa: &CryptoCoordinator,
    a: &EpochScope,
    wb: &CryptoCoordinator,
    b: &EpochScope,
) -> v2::RecoveryRecord {
    let p = wa.prepare_epoch_recovery(a.clone()).await.unwrap();
    query(wb, b, &response(&p, None, common::RecoveryState::Pending))
        .await
        .unwrap();
    let d = wb
        .prepare_epoch_recovery_decision(
            b.clone(),
            p.proposal().control.as_ref().unwrap().proposal_id.clone(),
            EpochRecoveryDecision::Accept,
        )
        .await
        .unwrap();
    let r = response(&p, Some(&d), common::RecoveryState::Accepted);
    wa.accept_epoch_recovery_result(observed(&p, &r))
        .await
        .unwrap();
    wb.accept_epoch_recovery_result(observed(&d, &r))
        .await
        .unwrap();
    r
}

#[path = "epoch_transition_fault_tests.rs"]
mod faults;
fn catalog(q: &EpochTransitionQuery) -> v2::ConversationEpochs {
    let c = control(&q.accepted).unwrap();
    let at = q.accepted.resolved_at_unix_ms.unwrap();
    // Tests below construct initial0 transitions; later tests supply retained row.
    assert_eq!(c.expected_epoch, 0);
    v2::ConversationEpochs {
        protocol_version: 2,
        conversation: Some(q.pins.conversation().clone()),
        active_epoch: Some(1),
        epoch_revision: Some(1),
        next_after_epoch: None,
        epochs: vec![
            v2::EpochRecord {
                epoch: Some(0),
                revision: Some(0),
                sealed: true,
                created_at_unix_ms: 1_700_000_000_000,
                sealed_at_unix_ms: Some(at),
                sealed_by_proposal_id: c.proposal_id.clone(),
                ..Default::default()
            },
            v2::EpochRecord {
                epoch: Some(1),
                revision: Some(1),
                created_at_unix_ms: at,
                created_by_proposal_id: c.proposal_id.clone(),
                bootstrap_device: c.initiator.as_ref().and_then(|p| p.device.clone()),
                ..Default::default()
            },
        ],
    }
}
fn observation(
    q: &EpochTransitionQuery,
    page: v2::ConversationEpochs,
) -> EpochObservation<EpochTransitionQuery, v2::ConversationEpochs> {
    EpochObservation::test_observation(q.scope.scope.clone(), q.clone(), page)
}
async fn prepare(
    w: &CryptoCoordinator,
    s: &EpochScope,
    r: &v2::RecoveryRecord,
) -> EpochTransitionQuery {
    let q = w
        .prepare_epoch_transition(s.clone(), control(r).unwrap().proposal_id.clone())
        .await
        .unwrap();
    let p = w
        .accept_epoch_transition_catalog(observation(&q, catalog(&q)))
        .await
        .unwrap();
    assert!(!p.activated());
    q
}
fn receipt(p: &EpochOutgoingPlan) -> v2::SendReceipt {
    let e = p.request().envelope.as_ref().unwrap();
    v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1_700_000_000_000,
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    }
}
async fn store_receipt(w: &CryptoCoordinator, s: &EpochScope, p: &EpochOutgoingPlan) {
    w.accept_epoch_send_receipt(EpochObservation::test_observation(
        s.scope.clone(),
        p.clone(),
        receipt(p),
    ))
    .await
    .unwrap();
}
fn sealed(p: &EpochOutgoingPlan, r: &v2::RecoveryRecord) -> v2::EpochSealedFailure {
    let o = r.outcome.as_ref().unwrap();
    v2::EpochSealedFailure {
        fence: Some(common::EpochSealedFence {
            binding: Some(data::send_binding(p.request().envelope.as_ref().unwrap()).unwrap()),
            proposal_digest: control::control_digest(control(r).unwrap())
                .unwrap()
                .to_vec(),
            next_epoch: o.new_epoch,
            epoch_revision: o.epoch_revision,
        }),
        recovery: Some(o.clone()),
    }
}
fn fence(
    s: &EpochScope,
    p: &EpochOutgoingPlan,
    r: &v2::EpochSealedFailure,
) -> EpochObservation<EpochOutgoingPlan, v2::EpochSealedFailure> {
    EpochObservation::test_observation(s.scope.clone(), p.clone(), r.clone())
}
fn db(root: &std::path::Path, user: &str) -> Connection {
    Connection::open(
        StoreScope::new("https://chat.example.test", user)
            .unwrap()
            .database_path(root),
    )
    .unwrap()
}
async fn bootstrap(w: &CryptoCoordinator, s: &EpochScope, b: &AcquiredBundle) {
    let p = w.prepare_epoch_acquisition(s.clone()).await.unwrap();
    let value = v2::AcquiredBundle {
        protocol_version: 2,
        conversation_id: s.conversation.clone(),
        acquisition_id: p.request().acquisition_id.clone(),
        device: b.device.clone(),
        signed_pre_key: b.signed_pre_key.clone(),
        one_time_pre_key: b.one_time_pre_key.clone(),
        kyber_pre_key: b.kyber_pre_key.clone(),
        not_after_unix_ms: b.not_after_unix_ms,
        crypto_epoch: Some(s.epoch),
    };
    w.bootstrap_epoch(EpochObservation::test_observation(
        s.scope.clone(),
        p,
        value,
    ))
    .await
    .unwrap();
}
fn wire(p: &EpochOutgoingPlan, n: i64) -> v2::Delivery {
    v2::Delivery {
        sequence: n,
        envelope: p.request().envelope.clone(),
        envelope_digest: receipt(p).envelope_digest,
        expires_at_unix_ms: receipt(p).expires_at_unix_ms,
    }
}

#[tokio::test]
async fn unsent_attachment_preparation_survives_recovery_but_old_plan_cannot_upload_in_new_epoch() {
    use crate::encryption::attachment_payload::AttachmentDraft;
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let draft = || AttachmentDraft {
        kind: v2::AttachmentKind::File,
        filename: Zeroizing::new("private-recovery-file.bin".into()),
        mime_type: Zeroizing::new("application/octet-stream".into()),
        caption: Zeroizing::new(String::new()),
        photo_metadata_stripped: None,
    };
    let bytes = || Zeroizing::new(b"retained old attachment source".to_vec());
    let message = uuid::Uuid::new_v4().to_string();
    let old = f
        .alice
        .prepare_epoch_attachment(CHAT.into(), message.clone(), draft(), bytes())
        .await
        .unwrap();
    let EpochAttachmentPreparation::Prepared(old) = old else {
        panic!("missing preparation")
    };
    let blob_id = old.blob_id().to_owned();
    let digest = *old.blob_digest();
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    prepare(&f.bob, &b, &r).await;
    // Prepared is known-not-Sent, so it cannot turn into an unknown-send audit
    // item. Recovery still requires both signatures and catalogue acceptance.
    assert!(
        f.alice
            .advance_epoch_transition(a, id.clone())
            .await
            .unwrap()
            .activated()
    );
    assert!(
        f.bob
            .advance_epoch_transition(b, id)
            .await
            .unwrap()
            .activated()
    );
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice.prepared_epoch_attachment_blob(*old).await,
        Err(CryptoError::Conflict)
    ));
    let saved = f
        .alice
        .prepared_epoch_attachment(message.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.crypto_epoch(), 0);
    assert_eq!(saved.blob_id(), blob_id);
    assert_eq!(saved.blob_digest(), &digest);
    let EpochAttachmentPreparation::Prepared(replay) = f
        .alice
        .prepare_epoch_attachment(CHAT.into(), message, draft(), bytes())
        .await
        .unwrap()
    else {
        panic!("missing exact replay")
    };
    assert_eq!(replay.crypto_epoch(), 0);
    assert_eq!(replay.blob_id(), blob_id);
    assert_eq!(records(f.root.path(), A), before);
    let EpochAttachmentPreparation::Prepared(new) = f
        .alice
        .prepare_epoch_attachment(
            CHAT.into(),
            uuid::Uuid::new_v4().to_string(),
            draft(),
            bytes(),
        )
        .await
        .unwrap()
    else {
        panic!("missing new preparation")
    };
    assert_eq!(new.crypto_epoch(), 1);
    assert_ne!(new.blob_id(), blob_id);
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn promoted_attachment_receipt_or_bound3_fence_settles_across_signed_recovery_without_file() {
    use crate::encryption::{attachment_payload::AttachmentDraft, blob_control};
    for use_receipt in [true, false] {
        let (f, a, b) = prepared().await;
        let message = uuid::Uuid::new_v4().to_string();
        f.alice
            .prepare_epoch_attachment(
                CHAT.into(),
                message.clone(),
                AttachmentDraft {
                    kind: v2::AttachmentKind::File,
                    filename: Zeroizing::new("retained-promoted.bin".into()),
                    mime_type: Zeroizing::new("application/octet-stream".into()),
                    caption: Zeroizing::new(String::new()),
                    photo_metadata_stripped: None,
                },
                Zeroizing::new(b"retained promoted attachment".to_vec()),
            )
            .await
            .unwrap();
        let lease = f
            .alice
            .epoch_attachment_control(message.clone())
            .await
            .unwrap();
        let grant = v2::BlobLease {
            message_id: message.clone(),
            operation_id: lease.request().operation_id.clone(),
            attempt_id: uuid::Uuid::new_v4().to_string(),
            revision: 1,
            reserved_at_unix_ms: 1000,
            expires_at_unix_ms: 1000 + blob_control::STAGING_LEASE_MS,
            observed_at_unix_ms: 1000,
            state: v2::BlobLeaseState::Ready as i32,
            expired: false,
        };
        f.alice
            .accept_epoch_blob_lease(EpochObservation::test_observation(
                a.scope.clone(),
                lease,
                grant,
            ))
            .await
            .unwrap();
        let EpochAttachmentPromotion::Queued(p) = f
            .alice
            .promote_epoch_attachment(message.clone())
            .await
            .unwrap()
        else {
            panic!("missing promoted outbox")
        };
        let exact = p.request().clone();
        let r = accepted(&f, &a, &b).await;
        let proposal = control(&r).unwrap().proposal_id.clone();
        prepare(&f.alice, &a, &r).await;
        let blocked = f
            .alice
            .advance_epoch_transition(a.clone(), proposal.clone())
            .await
            .unwrap();
        assert!(!blocked.activated());
        assert_eq!(blocked.blocked_message(), Some(message.as_str()));
        // Test-only external loss. Settlement cannot require surviving chunks.
        db(f.root.path(), A)
            .execute("DELETE FROM protected_attachment_chunks", [])
            .unwrap();
        let retained = f
            .alice
            .epoch_attachment_outgoing(message.clone())
            .await
            .unwrap()
            .unwrap();
        if use_receipt {
            store_receipt(&f.alice, &a, &retained).await;
        } else {
            let proof = sealed(&retained, &r);
            assert_eq!(
                proof
                    .fence
                    .as_ref()
                    .unwrap()
                    .binding
                    .as_ref()
                    .unwrap()
                    .protocol_version,
                3
            );
            f.alice
                .accept_epoch_send_fence(fence(&a, &retained, &proof))
                .await
                .unwrap();
        }
        assert!(
            f.alice
                .advance_epoch_transition(a.clone(), proposal)
                .await
                .unwrap()
                .activated()
        );
        assert_eq!(
            f.alice
                .active_epoch_scope(CHAT.into())
                .await
                .unwrap()
                .unwrap()
                .crypto_epoch(),
            1
        );
        while !f.alice.seed_history_index().await.unwrap().ready {}
        let before = records(f.root.path(), A);
        let old = f
            .alice
            .epoch_attachment_outgoing(message.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(old.request(), &exact);
        assert!(!old.needs_send());
        let EpochAttachmentPromotion::Queued(retry) =
            f.alice.promote_epoch_attachment(message).await.unwrap()
        else {
            panic!("old outbox must not bootstrap epoch1")
        };
        assert_eq!(retry.request(), &exact);
        let ViewPage::History(rows, _) = f
            .alice
            .read_views(ViewRequest::ArchiveHistory(
                a.own.generation_id,
                CHAT.into(),
                Some(0),
                None,
            ))
            .await
            .unwrap()
        else {
            panic!("missing archived attachment")
        };
        assert_eq!(rows.len(), 1);
        assert!(rows[0].attachment.is_some());
        assert_eq!(records(f.root.path(), A), before);
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn empty_transition_is_atomic_idempotent_and_only_proposer_bootstraps() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    let q = prepare(&f.alice, &a, &r).await;
    prepare(&f.bob, &b, &r).await;
    let before = records(f.root.path(), A);
    assert!(f.alice.epoch_scope(CHAT.into(), 1).await.is_err());
    let done = f
        .alice
        .advance_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    assert!(done.activated());
    assert_eq!(done.total(), 0);
    f.bob
        .advance_epoch_transition(b.clone(), id.clone())
        .await
        .unwrap();
    let a1 = f.alice.epoch_scope(CHAT.into(), 1).await.unwrap();
    let b1 = f.bob.epoch_scope(CHAT.into(), 1).await.unwrap();
    assert!(!a1.sealed() && !a1.sending_frozen());
    let overview = f.alice.epoch_recovery_overview(CHAT.into()).await.unwrap();
    assert_eq!(overview.crypto_epoch, Some(1));
    assert!(!overview.sending_frozen && overview.proposals.is_empty());
    let unchanged = records(f.root.path(), A);
    assert!(
        f.alice
            .prepare_epoch_recovery_if(a.clone(), None)
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), unchanged);

    assert_ne!(a1.session_record_id(), a.session_record_id());
    assert_ne!(a1.acquisition_record_id(), a.acquisition_record_id());
    assert!(f.alice.epoch_scope(CHAT.into(), 0).await.unwrap().sealed());
    let after = records(f.root.path(), A);
    assert!(
        f.alice
            .accept_epoch_transition_catalog(observation(&q, catalog(&q)))
            .await
            .unwrap()
            .activated()
    );
    assert!(
        f.alice
            .advance_epoch_transition(a.clone(), id.clone())
            .await
            .unwrap()
            .activated()
    );
    assert_eq!(records(f.root.path(), A), after);
    for p in before.into_iter().filter(|p| p.0 < 6) {
        assert!(after.contains(&p));
    }
    assert!(f.bob.prepare_epoch_acquisition(b1).await.is_err());
    assert!(f.alice.prepare_epoch_acquisition(a).await.is_err());
    assert!(f.alice.prepare_epoch_acquisition(a1).await.is_ok());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn unknown_send_blocks_activation_receipt_unblocks_without_peer_ack() {
    let (f, a, b) = prepared().await;
    let text = payload(&a, "receipt is enough, peer can stay offline");
    let p = f
        .alice
        .encrypt_epoch(a.clone(), text.clone())
        .await
        .unwrap();
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    let blocked = f
        .alice
        .advance_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    assert!(!blocked.activated());
    assert_eq!(blocked.blocked_message(), Some(text.message_id.as_str()));
    assert_eq!(blocked.remaining(), 1);
    assert!(f.alice.epoch_scope(CHAT.into(), 1).await.is_err());
    store_receipt(&f.alice, &a, &p).await;
    assert!(
        f.alice
            .advance_epoch_transition(a.clone(), id)
            .await
            .unwrap()
            .activated()
    );
    let old = f
        .alice
        .epoch_outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    assert_eq!(old.request(), p.request());
    assert!(!old.needs_send() && old.needs_status_poll());
    let crate::encryption::signal_state::EpochTextPreparation::Queued(replayed) = f
        .alice
        .prepare_epoch_text(
            CHAT.into(),
            text.message_id.clone(),
            Zeroizing::new("receipt is enough, peer can stay offline".into()),
        )
        .await
        .unwrap()
    else {
        panic!("old committed text must not bootstrap the new epoch")
    };
    assert_eq!(replayed.crypto_epoch, 0);
    assert_eq!(replayed.created_at_unix_ms, text.created_at_unix_ms);
    assert!(replayed.stored_on_server);
    while !f.alice.seed_history_index().await.unwrap().ready {}
    let before_read = records(f.root.path(), A);
    let ViewPage::History(rows, next) = f
        .alice
        .read_views(ViewRequest::History(CHAT.into(), None))
        .await
        .unwrap()
    else {
        panic!("wrong history page")
    };
    assert_eq!(rows.len(), 1);
    assert!(next.is_none());
    assert_eq!(rows[0].message_id, text.message_id);
    assert_eq!(rows[0].crypto_epoch, 0);
    assert!(records(f.root.path(), A) == before_read);
    assert_eq!(
        f.alice
            .encrypt_epoch(a.clone(), text)
            .await
            .unwrap()
            .request(),
        p.request()
    );
    assert!(
        f.alice
            .encrypt_epoch(a.clone(), payload(&a, "old fresh rejected"))
            .await
            .is_err()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn exact_fence_settles_only_after_local_acceptance_and_never_becomes_receipt() {
    let (f, a, b) = prepared().await;
    let p = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "never accepted on server"))
        .await
        .unwrap();
    let proposal = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    query(
        &f.bob,
        &b,
        &response(&proposal, None, common::RecoveryState::Pending),
    )
    .await
    .unwrap();
    let id = proposal
        .proposal()
        .control
        .as_ref()
        .unwrap()
        .proposal_id
        .clone();
    let decision = f
        .bob
        .prepare_epoch_recovery_decision(b.clone(), id.clone(), EpochRecoveryDecision::Accept)
        .await
        .unwrap();
    let r = response(&proposal, Some(&decision), common::RecoveryState::Accepted);
    let proof = sealed(&p, &r);
    let before = records(f.root.path(), A);
    assert!(
        f.alice
            .accept_epoch_send_fence(fence(&a, &p, &proof))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice
        .accept_epoch_recovery_result(observed(&proposal, &r))
        .await
        .unwrap();
    let mut bad = proof.clone();
    bad.fence
        .as_mut()
        .unwrap()
        .binding
        .as_mut()
        .unwrap()
        .envelope_digest[0] ^= 1;
    assert!(
        f.alice
            .accept_epoch_send_fence(fence(&a, &p, &bad))
            .await
            .is_err()
    );
    f.alice
        .accept_epoch_send_fence(fence(&a, &p, &proof))
        .await
        .unwrap();
    let after = records(f.root.path(), A);
    f.alice
        .accept_epoch_send_fence(fence(&a, &p, &proof))
        .await
        .unwrap();
    assert_eq!(records(f.root.path(), A), after);
    let settled = f
        .alice
        .epoch_outgoing_plan(p.request().envelope.as_ref().unwrap().message_id.clone())
        .await
        .unwrap();
    assert!(!settled.needs_send() && !settled.needs_status_poll());
    assert!(settled.receipt().is_none());
    assert_eq!(settled.sealed_failure(), Some(&proof));
    assert!(
        f.alice
            .accept_epoch_send_receipt(EpochObservation::test_observation(
                a.scope.clone(),
                p.clone(),
                receipt(&p)
            ))
            .await
            .is_err()
    );
    prepare(&f.alice, &a, &r).await;
    assert!(
        f.alice
            .advance_epoch_transition(a.clone(), id)
            .await
            .unwrap()
            .activated()
    );
    let retained = f
        .alice
        .epoch_outgoing_plan(p.request().envelope.as_ref().unwrap().message_id.clone())
        .await
        .unwrap();
    assert_eq!(retained.request(), p.request());
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn old_prekey_after_new_epoch_uses_old_ratchet_and_never_creates_confirmation() {
    let (f, a, b) = prepared().await;
    let old_text = payload(&a, "late old PreKey");
    let old = f
        .alice
        .encrypt_epoch(a.clone(), old_text.clone())
        .await
        .unwrap();
    store_receipt(&f.alice, &a, &old).await;
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    prepare(&f.bob, &b, &r).await;
    f.alice
        .advance_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    f.bob.advance_epoch_transition(b.clone(), id).await.unwrap();
    let a1 = f.alice.epoch_scope(CHAT.into(), 1).await.unwrap();
    let b1 = f.bob.epoch_scope(CHAT.into(), 1).await.unwrap();
    bootstrap(&f.alice, &a1, &f.next_bundle).await;
    let new_text = payload(&a1, "new epoch, independent ratchet");
    let new = f
        .alice
        .encrypt_epoch(a1.clone(), new_text.clone())
        .await
        .unwrap();
    assert_eq!(
        f.bob
            .receive_epoch(b1.clone(), delivery(&b1, &wire(&new, 2)))
            .await
            .unwrap()
            .0,
        new_text
    );
    let replies = f.bob.epoch_outbox_page(None).await.unwrap().messages;
    assert_eq!(replies.len(), 1);
    assert_eq!(
        replies[0].request().envelope.as_ref().unwrap().crypto_epoch,
        Some(1)
    );
    let before_new_session = records(f.root.path(), B)
        .into_iter()
        .find(|r| r.1 == b1.session_record_id())
        .unwrap();
    let (received, ack) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &wire(&old, 1)))
        .await
        .unwrap();
    assert_eq!(received, old_text);
    assert_eq!(
        f.bob.epoch_outbox_page(None).await.unwrap().messages.len(),
        1
    );
    assert!(records(f.root.path(), B).contains(&before_new_session));
    let confirm = &replies[0];
    f.alice
        .receive_epoch(a1.clone(), delivery(&a1, &wire(confirm, 3)))
        .await
        .unwrap();
    let following = f
        .alice
        .encrypt_epoch(
            a1,
            payload(
                &f.alice.epoch_scope(CHAT.into(), 1).await.unwrap(),
                "Signal in new epoch",
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        following.request().envelope.as_ref().unwrap().message_type,
        common::CiphertextType::Signal as i32
    );
    f.bob
        .accept_epoch_ack(EpochObservation::test_observation(
            b.scope.clone(),
            ack,
            common::Empty {},
        ))
        .await
        .unwrap();
    f.bob.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_incoming_message(old_text.message_id).await.unwrap(),
        received
    );
    assert!(w.epoch_scope(CHAT.into(), 0).await.unwrap().sealed());
    assert!(!w.epoch_scope(CHAT.into(), 1).await.unwrap().sealed());
    w.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}
#[tokio::test]
async fn bounded_audit_is_not_authorized_by_empty_sql_pending_page() {
    let (f, a, b) = prepared().await;
    let mut messages = vec![];
    for i in 0..35 {
        let p = f
            .alice
            .encrypt_epoch(a.clone(), payload(&a, &format!("audit {i}")))
            .await
            .unwrap();
        if i != 0 {
            store_receipt(&f.alice, &a, &p).await;
        }
        messages.push(p);
    }
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    let step = f
        .alice
        .advance_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    assert!(!step.activated());
    assert_eq!(step.remaining(), 3);
    assert!(step.blocked_message().is_none());
    // Resume from the protected cursor, without auditing the same32 entries again.
    f.alice.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
        .await
        .unwrap();
    let step = w
        .advance_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    assert_eq!(step.remaining(), 1);
    assert_eq!(
        step.blocked_message(),
        Some(
            messages[0]
                .request()
                .envelope
                .as_ref()
                .unwrap()
                .message_id
                .as_str()
        )
    );
    let db = db(f.root.path(), A);
    db.execute("UPDATE protected_recovery SET pending=0 WHERE kind=11", [])
        .unwrap();
    assert!(w.epoch_outbox_page(None).await.unwrap().messages.is_empty());
    assert!(w.advance_epoch_transition(a.clone(), id).await.is_err());
    assert!(w.epoch_scope(CHAT.into(), 1).await.is_err());
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn failed_activation_commit_preserves_old_root_rows_freeze_and_retry() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    let db = db(f.root.path(), A);
    db.execute_batch("CREATE TABLE transition_parent(id INTEGER PRIMARY KEY);CREATE TABLE transition_child(id INTEGER REFERENCES transition_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER transition_commit_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE '%:row:0000000000000001' BEGIN INSERT INTO transition_child VALUES(1);END;").unwrap();
    let before = records(f.root.path(), A);
    assert!(matches!(
        f.alice
            .advance_epoch_transition(a.clone(), id.clone())
            .await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert!(f.alice.epoch_scope(CHAT.into(), 1).await.is_err());
    assert!(
        f.alice
            .epoch_scope(CHAT.into(), 0)
            .await
            .unwrap()
            .sending_frozen()
    );
    db.execute_batch("DROP TRIGGER transition_commit_fault")
        .unwrap();
    assert!(
        f.alice
            .advance_epoch_transition(a, id)
            .await
            .unwrap()
            .activated()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn archive_segments_verify_signed_transition_and_filter_history_without_changing_ratchets_or_outbox()
 {
    let (f, a, b) = prepared().await;
    let p = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "epoch zero retained"))
        .await
        .unwrap();
    store_receipt(&f.alice, &a, &p).await;
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    prepare(&f.bob, &b, &r).await;
    assert!(
        f.alice
            .advance_epoch_transition(a.clone(), id.clone())
            .await
            .unwrap()
            .activated()
    );
    assert!(
        f.bob
            .advance_epoch_transition(b, id)
            .await
            .unwrap()
            .activated()
    );
    let a1 = f.alice.epoch_scope(CHAT.into(), 1).await.unwrap();
    bootstrap(&f.alice, &a1, &f.next_bundle).await;
    f.alice
        .encrypt_epoch(a1.clone(), payload(&a1, "epoch one retained"))
        .await
        .unwrap();
    while !f.alice.seed_history_index().await.unwrap().ready {}
    let before = records(f.root.path(), A);
    let generation = a.own.generation_id.clone();
    match f
        .alice
        .read_views(ViewRequest::Segments(generation.clone(), CHAT.into(), None))
        .await
        .unwrap()
    {
        ViewPage::Segments(v, c) => {
            assert!(c.is_none());
            assert_eq!(v.len(), 2);
            assert_eq!((v[0].crypto_epoch, v[0].sealed), (1, false));
            assert_eq!((v[1].crypto_epoch, v[1].sealed), (0, true));
            assert_eq!(v[1].sealed_at_unix_ms, Some(v[0].created_at_unix_ms));
        }
        _ => panic!("wrong page"),
    }
    for (epoch, count) in [(None, 2), (Some(0), 1), (Some(1), 1)] {
        match f
            .alice
            .read_views(ViewRequest::ArchiveHistory(
                generation.clone(),
                CHAT.into(),
                epoch,
                None,
            ))
            .await
            .unwrap()
        {
            ViewPage::History(v, c) => {
                assert!(c.is_none());
                assert_eq!(v.len(), count);
                assert!(epoch.is_none_or(|epoch| v.iter().all(|v| v.crypto_epoch == epoch)));
            }
            _ => panic!("wrong page"),
        }
    }
    assert!(
        f.alice
            .read_views(ViewRequest::ArchiveHistory(
                generation.clone(),
                CHAT.into(),
                Some(2),
                None
            ))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    let mut s = store(f.root.path(), A, false);
    s.transaction(|tx| {
        tx.put(
            &RecordKey::new(
                &generation,
                RecordKind::Control,
                &format!("epoch-v2:{CHAT}:row-anchor:0000000000000000"),
            )?,
            &[1; 32],
        )
    })
    .unwrap();
    drop(s);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own)
        .await
        .unwrap();
    let before = records(f.root.path(), A);
    assert!(
        w.read_views(ViewRequest::Segments(generation.clone(), CHAT.into(), None))
            .await
            .is_err()
    );
    assert!(
        w.read_views(ViewRequest::ArchiveHistory(
            generation,
            CHAT.into(),
            Some(0),
            None
        ))
        .await
        .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    // Remove both the old row and its anchor: the surviving current root is
    // valid, but the segment inventory must still reject the missing epoch0.
    let path = StoreScope::new("https://chat.example.test", A)
        .unwrap()
        .database_path(f.root.path());
    let db = Connection::open(path).unwrap();
    db.execute(
        "DELETE FROM protected_records WHERE generation=?1 AND kind=13 AND record_id IN (?2,?3)",
        rusqlite::params![
            uuid::Uuid::parse_str(&a1.own.generation_id)
                .unwrap()
                .as_bytes()
                .as_slice(),
            format!("epoch-v2:{CHAT}:row:0000000000000000"),
            format!("epoch-v2:{CHAT}:row-anchor:0000000000000000")
        ],
    )
    .unwrap();
    drop(db);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), a1.own.clone())
        .await
        .unwrap();
    assert!(w.active_epoch_scope(CHAT.into()).await.unwrap().is_some());
    let before = records(f.root.path(), A);
    assert!(
        w.read_views(ViewRequest::Segments(
            a1.own.generation_id,
            CHAT.into(),
            None
        ))
        .await
        .is_err()
    );
    assert_eq!(records(f.root.path(), A), before);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
