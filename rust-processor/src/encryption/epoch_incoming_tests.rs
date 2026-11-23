use super::super::super::tests::store;
use super::super::tests::{Fixture, fixture, index, initialize, records, rewrite_frozen, wrapped};
use super::*;
use crate::proto::e2ee::{ConversationCloseReason, TextMessage};
use rusqlite::Connection;
use uuid::Uuid;
const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
pub(in crate::encryption::signal_state::epoch_state) async fn prepared()
-> (Fixture, EpochScope, EpochScope) {
    prepare_fixture(fixture().await).await
}
pub(in crate::encryption::signal_state::epoch_state) async fn prepared_at(
    origin: &str,
) -> (Fixture, EpochScope, EpochScope) {
    prepare_fixture(super::super::tests::fixture_at(origin).await).await
}
async fn prepare_fixture(f: Fixture) -> (Fixture, EpochScope, EpochScope) {
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let p = f.alice.prepare_epoch_acquisition(a.clone()).await.unwrap();
    let old = &f.bundle;
    // Test fixture adapter only; production never converts a v1 data DTO.
    let bundle = v2::AcquiredBundle {
        protocol_version: 2,
        conversation_id: p.request().conversation_id.clone(),
        acquisition_id: p.request().acquisition_id.clone(),
        device: old.device.clone(),
        signed_pre_key: old.signed_pre_key.clone(),
        one_time_pre_key: old.one_time_pre_key.clone(),
        kyber_pre_key: old.kyber_pre_key.clone(),
        not_after_unix_ms: old.not_after_unix_ms,
        crypto_epoch: p.request().crypto_epoch,
    };
    f.alice
        .bootstrap_epoch(EpochObservation::test_observation(
            a.scope.clone(),
            p,
            bundle,
        ))
        .await
        .unwrap();
    (f, a, b)
}
pub(in crate::encryption::signal_state::epoch_state) fn payload(
    s: &EpochScope,
    text: &str,
) -> v2::EncryptedPayload {
    v2::EncryptedPayload {
        format_version: 2,
        message_id: Uuid::new_v4().to_string(),
        conversation_id: s.conversation.clone(),
        sender: Some(s.own.clone()),
        recipient: Some(s.peer.clone()),
        created_at_unix_ms: 1_700_000_000_000,
        content: Some(v2::encrypted_payload::Content::Text(TextMessage {
            text: text.into(),
        })),
        crypto_epoch: Some(s.epoch),
    }
}
pub(in crate::encryption::signal_state::epoch_state) async fn send(
    w: &CryptoCoordinator,
    s: &EpochScope,
    p: v2::EncryptedPayload,
    sequence: i64,
) -> v2::Delivery {
    let plan = w.encrypt_epoch(s.clone(), p).await.unwrap();
    let e = plan.request().envelope.clone().unwrap();
    v2::Delivery {
        sequence,
        envelope_digest: data::envelope_digest(&e).unwrap().to_vec(),
        envelope: Some(e),
        expires_at_unix_ms: 1_700_000_000_000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
    }
}
pub(in crate::encryption::signal_state::epoch_state) fn delivery(
    s: &EpochScope,
    d: &v2::Delivery,
) -> EpochDelivery {
    EpochObservation::test_observation(
        s.scope.clone(),
        v2::GetPendingMessagesRequest {
            context: Some(ClientContext {
                protocol_version: 2,
                device_generation_id: s.own.generation_id.clone(),
            }),
        },
        d.clone(),
    )
}
pub(in crate::encryption::signal_state::epoch_state) fn accepted(
    p: &EpochDurableAck,
) -> EpochObservation<EpochDurableAck, common::Empty> {
    EpochObservation::test_observation(p.scope.clone(), p.clone(), common::Empty {})
}
pub(in crate::encryption::signal_state::epoch_state) fn raw(root: &std::path::Path) -> Connection {
    Connection::open(
        StoreScope::new("https://chat.example.test", B)
            .unwrap()
            .database_path(root),
    )
    .unwrap()
}

#[tokio::test]
async fn receive_commit_reopen_duplicate_and_ack_preserve_history_and_private_keys() {
    let (f, a, b) = prepared().await;
    let p = payload(&a, "recipient-v2-sensitive-history-marker");
    let d = send(&f.alice, &a, p.clone(), 1).await;
    let sender = records(f.root.path(), A);
    let master = wrapped(f.root.path(), B);
    let (actual, ack) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert_eq!(actual, p);
    assert!(!ack.acknowledged());
    assert_eq!(
        ack.request.messages[0].disposition,
        common::AckDisposition::Stored as i32
    );
    let committed = records(f.root.path(), B);
    assert_eq!(records(f.root.path(), A), sender);
    assert_eq!(wrapped(f.root.path(), B), master);
    assert_eq!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap()
            .0,
        p
    );
    assert_eq!(records(f.root.path(), B), committed);
    assert_eq!(
        f.bob
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    assert!(
        f.bob
            .pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    f.bob.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_incoming_message(p.message_id.clone())
            .await
            .unwrap(),
        p
    );
    let ack = w.epoch_ack(p.message_id.clone()).await.unwrap();
    w.accept_epoch_ack(accepted(&ack)).await.unwrap();
    let after = records(f.root.path(), B);
    w.accept_epoch_ack(accepted(&ack)).await.unwrap();
    assert_eq!(records(f.root.path(), B), after);
    assert!(
        w.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert!(w.receive_epoch(b, delivery(&a, &d)).await.is_err()); // Wrong native origin/account context.
    w.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), ack.own.clone())
        .await
        .unwrap();
    assert!(
        w.epoch_ack(p.message_id.clone())
            .await
            .unwrap()
            .acknowledged()
    );
    assert_eq!(
        w.epoch_incoming_message(p.message_id.clone())
            .await
            .unwrap(),
        p
    );
    assert_eq!(records(f.root.path(), B), after);
    w.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_receive_commit_returns_no_plaintext_or_ack_and_restores_prekeys_ratchet() {
    let (f, a, b) = prepared().await;
    let p = payload(&a, "rollback receive");
    let d = send(&f.alice, &a, p.clone(), 1).await;
    let before = records(f.root.path(), B);
    let db = raw(f.root.path());
    db.execute_batch("CREATE TABLE recv_parent(id INTEGER PRIMARY KEY);CREATE TABLE recv_child(id INTEGER REFERENCES recv_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER recv_fault AFTER INSERT ON protected_records WHEN NEW.kind=12 BEGIN INSERT INTO recv_child VALUES(1);END;").unwrap();
    assert!(matches!(
        f.bob.receive_epoch(b.clone(), delivery(&b, &d)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    assert!(matches!(
        f.bob.epoch_ack(p.message_id.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    db.execute_batch("DROP TRIGGER recv_fault").unwrap();
    drop(db);
    assert_eq!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap()
            .0,
        p
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_ack_commit_keeps_same_pending_request_and_rejects_forged_proof() {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "lost ACK local commit"), 1).await;
    let (_, p) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let before = records(f.root.path(), B);
    let db = raw(f.root.path());
    db.execute_batch(&format!("CREATE TABLE ack_parent(id INTEGER PRIMARY KEY);CREATE TABLE ack_child(id INTEGER REFERENCES ack_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER ack_fault AFTER INSERT ON protected_records WHEN NEW.record_id='delivery-acked:{}' BEGIN INSERT INTO ack_child VALUES(1);END;",p.id)).unwrap();
    assert!(matches!(
        f.bob.accept_epoch_ack(accepted(&p)).await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), B), before);
    let mut bad = p.clone();
    bad.request.messages[0].envelope_digest[0] ^= 1;
    assert!(f.bob.accept_epoch_ack(accepted(&bad)).await.is_err());
    assert!(f.alice.accept_epoch_ack(accepted(&p)).await.is_err());
    let foreign = StoreScope::new("https://other.example.test", B).unwrap();
    assert!(
        f.bob
            .accept_epoch_ack(EpochObservation::test_observation(
                foreign,
                p.clone(),
                common::Empty {}
            ))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    db.execute_batch("DROP TRIGGER ack_fault").unwrap();
    drop(db);
    f.bob.accept_epoch_ack(accepted(&p)).await.unwrap();
    assert!(
        f.bob
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn forged_outer_binding_or_corrupt_ciphertext_rolls_back_signal_and_consumed_keys() {
    let (f, a, b) = prepared().await;
    let p = payload(&a, "authenticated inner binding");
    let d = send(&f.alice, &a, p.clone(), 1).await;
    let before = records(f.root.path(), B);
    // PreKey registration_id is wrapper metadata; don't trust it merely because
    // the embedded Signal message authenticates. Protobuf's last value wins.
    let mut registration = d.clone();
    let e = registration.envelope.as_mut().unwrap();
    e.ciphertext.extend_from_slice(&[0x28, 1]);
    assert_eq!(
        PreKeySignalMessage::try_from(e.ciphertext.as_slice())
            .unwrap()
            .registration_id(),
        1
    );
    assert_ne!(b.peer_registration, 1);
    registration.envelope_digest = data::envelope_digest(e).unwrap().to_vec();
    assert!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &registration))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    let mut header = d.clone();
    header.envelope.as_mut().unwrap().message_id = Uuid::new_v4().to_string();
    header.envelope_digest = data::envelope_digest(header.envelope.as_ref().unwrap())
        .unwrap()
        .to_vec();
    assert!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &header))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    let mut bad = d.clone();
    let e = bad.envelope.as_mut().unwrap();
    let last = e.ciphertext.len() - 1;
    e.ciphertext[last] ^= 1;
    bad.envelope_digest = data::envelope_digest(e).unwrap().to_vec();
    assert!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &bad))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    assert_eq!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap()
            .0,
        p
    );
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn wrong_origin_account_context_and_epoch_cannot_mint_receive_proof() {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "bindings"), 1).await;
    let before = records(f.root.path(), B);
    assert!(
        f.alice
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .is_err()
    );
    assert!(
        f.bob
            .receive_epoch(b.clone(), delivery(&a, &d))
            .await
            .is_err()
    );
    let mut obs = delivery(&b, &d);
    let mut request = obs.request().clone();
    request.context.as_mut().unwrap().protocol_version = 1;
    obs = EpochObservation::test_observation(b.scope.clone(), request, d.clone());
    assert!(f.bob.receive_epoch(b.clone(), obs).await.is_err());
    let mut e = d.clone();
    e.envelope.as_mut().unwrap().crypto_epoch = Some(1);
    e.envelope_digest = data::envelope_digest(e.envelope.as_ref().unwrap())
        .unwrap()
        .to_vec();
    assert!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &e))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_delivery_cannot_change_sequence_expiry_or_saved_ciphertext() {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "immutable delivery"), 1).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let before = records(f.root.path(), B);
    for seq in [false, true] {
        let mut changed = d.clone();
        if seq {
            changed.sequence += 1;
        } else {
            changed.expires_at_unix_ms += 1;
        }
        assert!(matches!(
            f.bob.receive_epoch(b.clone(), delivery(&b, &changed)).await,
            Err(CryptoError::Conflict)
        ));
        assert_eq!(records(f.root.path(), B), before);
    }
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_ratchet_can_send_reply_but_shared_uuid_cannot_change_direction() {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "hello Bob"), 1).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let p = payload(&b, "hello Alice");
    let reply = send(&f.bob, &b, p.clone(), 2).await;
    assert_eq!(
        f.alice
            .receive_epoch(a.clone(), delivery(&a, &reply))
            .await
            .unwrap()
            .0,
        p
    );
    let pending = payload(&b, "Bob outgoing not delivered to Alice yet");
    f.bob
        .encrypt_epoch(b.clone(), pending.clone())
        .await
        .unwrap();
    let mut collision = payload(&a, "same UUID owned by Bob outgoing");
    collision.message_id = pending.message_id;
    let d = send(&f.alice, &a, collision, 3).await;
    let before = records(f.root.path(), B);
    assert!(matches!(
        f.bob.receive_epoch(b.clone(), delivery(&b, &d)).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), B), before);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn freeze_or_peer_closure_keeps_accepted_receive_available() {
    for frozen in [true, false] {
        let (f, a, b) = prepared().await;
        let d = send(
            &f.alice,
            &a,
            payload(&a, "retained accepted old delivery"),
            1,
        )
        .await;
        f.bob.shutdown().await.unwrap();
        let mut c = f.a.clone();
        c.own_device = Some(b.own.clone());
        c.peer_device = f.a.peer_device.clone();
        if frozen {
            // rewrite_frozen uses only the authenticated own/chat locator.
            rewrite_frozen(f.root.path(), &c, true);
        }
        let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        if !frozen {
            let mut conversation = b_conversation(&f.a, &b);
            conversation.state = ConversationState::Closed as i32;
            conversation.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            conversation.closed_generation_id = b.peer.generation_id.clone();
            conversation.closed_at_unix_ms = 1_700_000_000_200;
            index(&w, &conversation).await;
        }
        let (_, ack) = w.receive_epoch(b.clone(), delivery(&b, &d)).await.unwrap();
        w.accept_epoch_ack(accepted(&ack)).await.unwrap();
        assert!(
            w.epoch_ack_page(None)
                .await
                .unwrap()
                .acknowledgements
                .is_empty()
        );
        w.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}
pub(in crate::encryption::signal_state::epoch_state) fn b_conversation(
    original: &Conversation,
    b: &EpochScope,
) -> Conversation {
    let mut c = original.clone();
    c.own_device = Some(b.own.clone());
    c.peer_device = Some(common::Device {
        reference: Some(b.peer.clone()),
        identity_key: b.peer_identity.serialize().to_vec(),
        registration_id: b.peer_registration,
        public_material_ready: true,
        public_material_revision: 1,
    });
    c
}

#[tokio::test]
async fn missing_message_component_or_ack_anchor_never_repairs_or_authorizes_ack() {
    for case in 0..6 {
        let (f, a, b) = prepared().await;
        let p = payload(&a, "point recovery");
        let d = send(&f.alice, &a, p.clone(), 1).await;
        let (_, ack) = f
            .bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        if case == 5 {
            f.bob.accept_epoch_ack(accepted(&ack)).await.unwrap();
        }
        f.bob.shutdown().await.unwrap();
        let db = raw(f.root.path());
        db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
        let (kind, id) = match case {
            0 => (10, ack.id.clone()),
            1 => (12, ack.id.clone()),
            2 => (13, format!("message-v2:{}", p.message_id)),
            3 => (13, format!("delivery-in:{}", ack.id)),
            4 => (
                13,
                format!("epoch-v2:{}:traffic:0000000000000000", b.conversation),
            ),
            _ => (13, format!("delivery-acked:{}", ack.id)),
        };
        db.execute(
            "DELETE FROM protected_records WHERE kind=?1 AND record_id=?2",
            rusqlite::params![kind, id],
        )
        .unwrap();
        drop(db);
        let before = records(f.root.path(), B);
        let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        assert!(w.receive_epoch(b.clone(), delivery(&b, &d)).await.is_err());
        assert!(w.epoch_ack(p.message_id.clone()).await.is_err());
        assert!(w.accept_epoch_ack(accepted(&ack)).await.is_err());
        assert_eq!(records(f.root.path(), B), before);
        w.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn lost_ratchet_never_bootstraps_again_but_committed_history_can_repeat_ack() {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "keep history"), 1).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let next = send(&f.alice, &a, payload(&a, "must not repair ratchet"), 2).await;
    f.bob.shutdown().await.unwrap();
    let db = raw(f.root.path());
    db.execute(
        "DELETE FROM protected_records WHERE kind=7 AND record_id=?1",
        [&b.session_id],
    )
    .unwrap();
    drop(db);
    let before = records(f.root.path(), B);
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert!(
        w.receive_epoch(b.clone(), delivery(&b, &next))
            .await
            .is_err()
    );
    assert!(w.prepare_epoch_acquisition(b.clone()).await.is_err());
    assert_eq!(records(f.root.path(), B), before);
    w.receive_epoch(b.clone(), delivery(&b, &d)).await.unwrap();
    assert_eq!(records(f.root.path(), B), before);
    w.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn inbound_wins_inflight_acquisition_without_inventing_or_renewing_nonce() {
    let (f, a, b) = prepared().await;
    let pending = f.bob.prepare_epoch_acquisition(b.clone()).await.unwrap();
    let nonce = pending.request().acquisition_id.clone();
    let d = send(
        &f.alice,
        &a,
        payload(&a, "inbound before outbound acquire reply"),
        1,
    )
    .await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    assert!(matches!(
        f.bob.prepare_epoch_acquisition(b.clone()).await,
        Err(CryptoError::SessionExists)
    ));
    f.bob.shutdown().await.unwrap();
    let mut db = store(f.root.path(), B, false);
    db.transaction(|tx| {
        let k = RecordKey::new(
            &b.own.generation_id,
            RecordKind::Acquisition,
            &b.acquisition_id,
        )?;
        let bytes = tx.get(&k)?.unwrap();
        assert!(bytes.windows(nonce.len()).any(|s| s == nonce.as_bytes()));
        Ok(())
    })
    .unwrap();
    drop(db);
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_ack_pages_and_mixed_direction_explicit_reseed_preserve_terminal_state() {
    let (f, a, b) = prepared().await;
    let mut expected = std::collections::BTreeSet::new();
    for n in 0..65 {
        let d = send(
            &f.alice,
            &a,
            payload(&a, &format!("bounded receive {n}")),
            n + 1,
        )
        .await;
        let (_, p) = f
            .bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .unwrap();
        if n < 10 {
            f.bob.accept_epoch_ack(accepted(&p)).await.unwrap();
        } else {
            expected.insert(p.message_id().to_string());
        }
    }
    f.bob
        .encrypt_epoch(b.clone(), payload(&b, "mixed direction history"))
        .await
        .unwrap();
    let first = f.bob.epoch_ack_page(None).await.unwrap();
    assert_eq!(first.acknowledgements.len(), 32);
    assert!(f.alice.epoch_ack_page(first.next.clone()).await.is_err());
    f.bob.shutdown().await.unwrap();
    let db = raw(f.root.path());
    db.execute_batch("PRAGMA foreign_keys=OFF;UPDATE protected_records SET recovery_ref=NULL;DELETE FROM protected_recovery;DELETE FROM protected_records WHERE record_id IN ('recovery-index:state:v1','recovery-index:anchor:v1');").unwrap();
    drop(db);
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        w.epoch_ack_page(None).await,
        Err(CryptoError::RecoveryIndexesNotReady)
    ));
    let mut rounds = 0;
    loop {
        let page = w.seed_recovery_indexes().await.unwrap();
        assert!(page.scanned <= 32);
        rounds += 1;
        if page.ready {
            break;
        }
        assert!(rounds < 25);
    }
    let mut cursor = None;
    let mut found = std::collections::BTreeSet::new();
    loop {
        let page = w.epoch_ack_page(cursor).await.unwrap();
        assert!(page.acknowledgements.len() <= 32);
        for p in page.acknowledgements {
            assert!(found.insert(p.message_id().to_string()));
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(found, expected);
    let mut cursor = None;
    let mut outgoing = std::collections::BTreeSet::new();
    loop {
        let page = w.epoch_outbox_page(cursor).await.unwrap();
        assert!(page.messages.len() <= 32);
        for p in page.messages {
            assert!(outgoing.insert(p.request().envelope.as_ref().unwrap().message_id.clone()));
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(outgoing.len(), 66); //65 automatic replies and one explicit text.
    assert!(
        w.pending_acknowledgements(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    w.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[test]
fn incoming_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_INCOMING_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_INCOMING_CRASH_AFTER").unwrap() == "1";
    let ack = std::env::var("ZCHAT_INCOMING_CRASH_ACK").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let d = v2::Delivery::decode(
                std::fs::read(root.join("public-delivery.pb"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let e = d.envelope.as_ref().unwrap();
            let own = e.recipient.clone().unwrap();
            let mut db = store(root, B, false);
            let id = format!(
                "v2:{}:{:016x}:{}",
                e.conversation_id,
                e.crypto_epoch.unwrap(),
                e.message_id
            );
            db.test_exit_on_record(
                if ack {
                    marker_key(&own, &id).unwrap()
                } else {
                    record_key(&own, RecordKind::Inbox, &id).unwrap()
                },
                after,
            );
            let w = CryptoCoordinator::start(db, own).await.unwrap();
            if ack {
                let p = w.epoch_ack(e.message_id.clone()).await.unwrap();
                let _ = w.accept_epoch_ack(accepted(&p)).await;
            } else {
                let s = w.epoch_scope(e.conversation_id.clone(), 0).await.unwrap();
                let _ = w.receive_epoch(s.clone(), delivery(&s, &d)).await;
            }
            panic!("incoming crash hook did not fire");
        });
}
async fn crash(ack_phase: bool) {
    for after in [false, true] {
        let (f, a, b) = prepared().await;
        let p = payload(&a, "synthetic real receive crash");
        let d = send(&f.alice, &a, p.clone(), 1).await;
        if ack_phase {
            f.bob
                .receive_epoch(b.clone(), delivery(&b, &d))
                .await
                .unwrap();
        }
        std::fs::write(f.root.path().join("public-delivery.pb"), d.encode_to_vec()).unwrap();
        f.bob.shutdown().await.unwrap();
        let before = records(f.root.path(), B);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::epoch_state::incoming::tests::incoming_crash_child",
                "--nocapture",
            ])
            .env("ZCHAT_INCOMING_CRASH_PATH", f.root.path())
            .env("ZCHAT_INCOMING_CRASH_AFTER", if after { "1" } else { "0" })
            .env(
                "ZCHAT_INCOMING_CRASH_ACK",
                if ack_phase { "1" } else { "0" },
            )
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        if !after {
            assert_eq!(records(f.root.path(), B), before);
        }
        if ack_phase {
            assert_eq!(
                w.epoch_ack(p.message_id.clone())
                    .await
                    .unwrap()
                    .acknowledged(),
                after
            );
        } else if !after {
            assert!(w.epoch_ack(p.message_id.clone()).await.is_err());
        }
        assert_eq!(
            w.receive_epoch(b.clone(), delivery(&b, &d))
                .await
                .unwrap()
                .0,
            p
        );
        assert_eq!(
            w.epoch_ack_page(None).await.unwrap().acknowledgements.len(),
            if ack_phase && after { 0 } else { 1 }
        );
        w.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn real_process_exit_before_after_receive_commit_recovers_atomic_history_and_ack() {
    crash(false).await;
}
#[tokio::test]
async fn real_process_exit_before_after_ack_commit_recovers_pending_or_terminal() {
    crash(true).await;
}
