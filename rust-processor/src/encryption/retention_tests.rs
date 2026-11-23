//! Retention/recovery boundaries against the pinned libsignal, not a mock cipher.
//! Future times are explicit private encrypt inputs: no OS clock or protocol
//! limits are changed. Missing-key experiments affect only disposable fixtures.
use super::*;
use crate::encryption::contract::{MESSAGE_RETENTION_MS, envelope_digest};
use crate::proto::e2ee::{Delivery, MessageState, MessageStatus, MessageStatuses, SendReceipt};
use libsignal_protocol::SessionUsabilityRequirements;
use std::{path::Path, time::Duration};

const DAY: Duration = Duration::from_secs(86_400);

fn delivery_at(envelope: Envelope, accepted: SystemTime) -> Delivery {
    Delivery {
        sequence: 1,
        expires_at_unix_ms: accepted.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
            + MESSAGE_RETENTION_MS,
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        envelope: Some(envelope),
    }
}

// Caller first joins the account owner. The same transaction primitive used by
// ordinary sending and automatic confirmations runs with a controlled time.
async fn encrypt_at(
    root: &Path,
    scope: &SessionScope,
    text: &EncryptedPayload,
    now: SystemTime,
) -> CryptoResult<Envelope> {
    let (root, scope, text) = (root.to_owned(), scope.clone(), text.clone());
    tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        store(&root, &scope.own.user_id, false)
            .transaction_with(|tx| encrypt_message_at(tx, &runtime, &scope, &text, now))
    })
    .await
    .unwrap()
}

fn reopen(root: &Path, own: &DeviceRef) -> impl Future<Output = CryptoCoordinator> {
    let state = store(root, &own.user_id, false);
    let own = own.clone();
    async move { CryptoCoordinator::start(state, own).await.unwrap() }
}

#[tokio::test]
async fn late_first_prekey_uses_original_private_keys_after_the_acquisition_window() {
    for (ec, lr) in [(true, false), (false, true)] {
        let p = pair(ec, lr).await;
        let now = SystemTime::now();
        let deadline = p.acquisition.not_after_unix_ms;
        let text = payload(
            &p.a,
            "delayed first message still needs the original prekeys",
        );
        p.alice
            .bootstrap(p.a.clone(), p.acquisition.clone())
            .await
            .unwrap();
        p.alice.shutdown().await.unwrap();
        p.bob.shutdown().await.unwrap();
        let original_signed = saved(
            p.root.path(),
            &p.b,
            RecordKind::SignedPreKey,
            &SIGNED.to_string(),
        )
        .unwrap();
        let original_pq = saved(
            p.root.path(),
            &p.b,
            RecordKind::PqPreKey,
            &(if lr { LR } else { PQ }).to_string(),
        )
        .unwrap();
        // An already bootstrapped, unconfirmed session can create a PreKey on
        // day29, although the acquisition expired on day1 or earlier.
        let accepted = now + DAY * 29;
        assert!(accepted.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64 > deadline);
        let envelope = encrypt_at(p.root.path(), &p.a, &text, accepted)
            .await
            .unwrap();
        assert_eq!(envelope.message_type, CiphertextType::PrekeySignal as i32);
        let bob = reopen(p.root.path(), &p.b.own).await;
        let (received, proof) = bob
            .receive_for_delivery(p.b.clone(), delivery_at(envelope.clone(), accepted))
            .await
            .unwrap();
        assert_eq!(received, text);
        assert_eq!(proof.request().messages[0].disposition, 1);
        // Reopen/replay does not consume a second prekey or generate new keys.
        bob.shutdown().await.unwrap();
        assert_eq!(
            saved(
                p.root.path(),
                &p.b,
                RecordKind::SignedPreKey,
                &SIGNED.to_string()
            )
            .unwrap(),
            original_signed
        );
        let used = saved(
            p.root.path(),
            &p.b,
            RecordKind::PqPreKey,
            &(if lr { LR } else { PQ }).to_string(),
        )
        .unwrap();
        if lr {
            assert_eq!(used, original_pq);
        } else {
            assert_eq!(
                used,
                prekey_state::tombstone(RecordKind::PqPreKey, PQ, &original_pq[3..])
            );
            assert_eq!(used.len(), 35);
        }
        let bob = reopen(p.root.path(), &p.b.own).await;
        assert_eq!(
            bob.receive_for_delivery(p.b.clone(), delivery_at(envelope, accepted))
                .await
                .unwrap()
                .0,
            text
        );
        bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn clock_jump_preserves_uncertain_outbox_and_late_confirmation_recovers_stale_sending() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let now = SystemTime::now();
    let old = payload(&p.a, "may already be on the server");
    let original = p.alice.encrypt(p.a.clone(), old.clone()).await.unwrap();
    p.alice.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a).unwrap();
    let rejected = payload(&p.a, "new text cannot use a stale unconfirmed session");
    assert!(matches!(
        encrypt_at(p.root.path(), &p.a, &rejected, now + DAY * 31).await,
        Err(CryptoError::Signal(SignalProtocolError::SessionNotFound(_)))
    ));
    assert_eq!(session(p.root.path(), &p.a).unwrap(), before);
    assert!(
        saved(
            p.root.path(),
            &p.a,
            RecordKind::Outbox,
            &rejected.message_id
        )
        .is_none()
    );
    assert!(
        saved(
            p.root.path(),
            &p.a,
            RecordKind::History,
            &rejected.message_id
        )
        .is_none()
    );
    assert_eq!(
        encrypt_at(p.root.path(), &p.a, &old, now + DAY * 1000)
            .await
            .unwrap(),
        original
    );
    assert_eq!(session(p.root.path(), &p.a).unwrap(), before);
    // A backwards jump permits new unconfirmed encryption: libsignal's age
    // limit uses wall time, not a trusted monotonic expiry/GC guarantee.
    let backwards = payload(&p.a, "clock rollback cannot justify private-key deletion");
    assert_eq!(
        encrypt_at(p.root.path(), &p.a, &backwards, now - DAY * 90)
            .await
            .unwrap()
            .message_type,
        CiphertextType::PrekeySignal as i32
    );
    let alice = reopen(p.root.path(), &p.a.own).await;
    assert!(
        alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .applied
    );
    let (_, _) = p
        .bob
        .receive_for_delivery(p.b.clone(), delivery_at(original, now))
        .await
        .unwrap();
    let replies = p.bob.outbox_page(None).await.unwrap();
    assert_eq!(replies.messages.len(), 1);
    let reply = replies.messages[0].request().envelope.clone().unwrap();
    alice
        .receive_for_delivery(p.a.clone(), delivery_at(reply, now))
        .await
        .unwrap();
    alice.shutdown().await.unwrap();
    // The same session, after an actual Signal reply, has no unconfirmed age limit.
    let after_confirmation = payload(&p.a, "late confirmation recovered the existing ratchet");
    let normal = encrypt_at(p.root.path(), &p.a, &after_confirmation, now + DAY * 1000)
        .await
        .unwrap();
    assert_eq!(normal.message_type, CiphertextType::Signal as i32);
    assert_eq!(
        p.bob.decrypt(p.b.clone(), normal).await.unwrap(),
        after_confirmation
    );
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn losing_first_prekey_and_one_confirmation_does_not_require_new_bootstrap() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let lost = payload(&p.a, "simulate first server copy expiring without delivery");
    let lost = p.alice.encrypt(p.a.clone(), lost).await.unwrap();
    assert_eq!(lost.message_type, CiphertextType::PrekeySignal as i32);
    let second = payload(&p.a, "second prekey establishes the receiver session");
    let e2 = p.alice.encrypt(p.a.clone(), second.clone()).await.unwrap();
    let second_delivery = delivery_at(e2, SystemTime::now());
    p.bob
        .receive_for_delivery(p.b.clone(), second_delivery.clone())
        .await
        .unwrap();
    let first_reply = p
        .bob
        .outbox_page(None)
        .await
        .unwrap()
        .messages
        .pop()
        .unwrap()
        .request()
        .envelope
        .clone()
        .unwrap();
    // Lose that reply too. Sender keeps using the same unconfirmed session.
    let third = payload(&p.a, "another prekey yields a recoverable confirmation");
    let e3 = p.alice.encrypt(p.a.clone(), third).await.unwrap();
    assert_eq!(e3.message_type, CiphertextType::PrekeySignal as i32);
    p.bob
        .receive_for_delivery(p.b.clone(), delivery_at(e3, SystemTime::now()))
        .await
        .unwrap();
    let replies = p.bob.outbox_page(None).await.unwrap();
    assert_eq!(replies.messages.len(), 2);
    let latest = replies
        .messages
        .iter()
        .map(|p| p.request().envelope.as_ref().unwrap())
        .find(|e| e.message_id != first_reply.message_id)
        .unwrap();
    p.alice
        .receive_for_delivery(p.a.clone(), delivery_at(latest.clone(), SystemTime::now()))
        .await
        .unwrap();
    // The earlier missing reply is also accepted out of order without a loop.
    p.alice
        .receive_for_delivery(p.a.clone(), delivery_at(first_reply, SystemTime::now()))
        .await
        .unwrap();
    let next = payload(&p.a, "normal message after a lost confirmation");
    let next_envelope = p.alice.encrypt(p.a.clone(), next.clone()).await.unwrap();
    assert_eq!(next_envelope.message_type, CiphertextType::Signal as i32);
    assert_eq!(
        p.bob.decrypt(p.b.clone(), next_envelope).await.unwrap(),
        next
    );
    assert_eq!(
        p.bob
            .receive_for_delivery(p.b.clone(), second_delivery)
            .await
            .unwrap()
            .0,
        second
    );
    assert_eq!(p.bob.outbox_page(None).await.unwrap().messages.len(), 2);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn deleting_any_required_original_private_prekey_breaks_late_first_delivery_without_ack() {
    for (kind, id) in [
        (RecordKind::EcPreKey, EC),
        (RecordKind::SignedPreKey, SIGNED),
        (RecordKind::PqPreKey, PQ),
    ] {
        let p = pair(true, false).await;
        p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
        let text = payload(&p.a, "private key retention must cover first delivery");
        let envelope = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
        p.bob.shutdown().await.unwrap();
        let key = p.b.key(kind, &id.to_string()).unwrap();
        let original = saved(p.root.path(), &p.b, kind, &id.to_string()).unwrap();
        let path = StoreScope::new(SERVER, B)
            .unwrap()
            .database_path(p.root.path());
        // There is deliberately no production delete API. Fault only this disposable DB.
        let db = rusqlite::Connection::open(path).unwrap();
        assert_eq!(
            db.execute(
                "DELETE FROM protected_records WHERE generation=?1 AND kind=?2 AND record_id=?3",
                rusqlite::params![
                    Uuid::parse_str(BG).unwrap().as_bytes().as_slice(),
                    kind as u8,
                    id.to_string()
                ]
            )
            .unwrap(),
            1
        );
        drop(db);
        let bob = reopen(p.root.path(), &p.b.own).await;
        assert!(matches!(
            bob.receive_for_delivery(
                p.b.clone(),
                delivery_at(envelope.clone(), SystemTime::now())
            )
            .await,
            Err(CryptoError::Signal(_))
        ));
        assert!(
            bob.pending_acknowledgements(None)
                .await
                .unwrap()
                .acknowledgements
                .is_empty()
        );
        bob.shutdown().await.unwrap();
        assert!(session(p.root.path(), &p.b).is_none());
        assert!(saved(p.root.path(), &p.b, RecordKind::History, &text.message_id).is_none());
        assert!(saved(p.root.path(), &p.b, RecordKind::Inbox, &text.message_id).is_none());
        store(p.root.path(), B, false)
            .transaction(|tx| tx.put(&key, &original))
            .unwrap();
        let bob = reopen(p.root.path(), &p.b.own).await;
        assert_eq!(
            bob.receive_for_delivery(p.b.clone(), delivery_at(envelope, SystemTime::now()))
                .await
                .unwrap()
                .0,
            text
        );
        bob.shutdown().await.unwrap();
        p.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn expired_delivery_status_is_not_a_signal_confirmation_or_permission_to_rebootstrap() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let now = SystemTime::now();
    let text = payload(
        &p.a,
        "receipt status is independent of the Signal handshake",
    );
    let envelope = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    let plan = p
        .alice
        .outgoing_plan(text.message_id.clone())
        .await
        .unwrap();
    let stored = now.duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
    let receipt = SendReceipt {
        message_id: text.message_id.clone(),
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        stored_at_unix_ms: stored,
        expires_at_unix_ms: stored + MESSAGE_RETENTION_MS,
    };
    p.alice
        .accept_message_statuses(VerifiedMessageStatuses::from_rpc(
            vec![plan],
            MessageStatuses {
                messages: vec![MessageStatus {
                    receipt: Some(receipt.clone()),
                    state: MessageState::Expired as i32,
                    observed_at_unix_ms: receipt.expires_at_unix_ms,
                }],
            },
        ))
        .await
        .unwrap();
    let retained = p.alice.outbox_page(None).await.unwrap();
    assert_eq!(retained.messages.len(), 1);
    assert_eq!(
        retained.messages[0].progress().status().unwrap().state,
        MessageState::Expired as i32
    );
    assert!(
        p.alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .applied
    );
    p.alice.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a).unwrap();
    let record = SessionRecord::deserialize(&before).unwrap();
    assert!(
        record
            .has_usable_sender_chain(now, SessionUsabilityRequirements::NotStale)
            .unwrap()
    );
    assert!(
        !record
            .has_usable_sender_chain(now + DAY * 31, SessionUsabilityRequirements::NotStale)
            .unwrap()
    );
    let next = payload(
        &p.a,
        "requires explicit recovery policy if every confirmation was lost",
    );
    assert!(matches!(
        encrypt_at(p.root.path(), &p.a, &next, now + DAY * 31).await,
        Err(CryptoError::Signal(SignalProtocolError::SessionNotFound(_)))
    ));
    assert_eq!(session(p.root.path(), &p.a).unwrap(), before);
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn forward_jump_rejection_and_skipped_key_eviction_are_separate_recoverable_boundaries() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let first = payload(&p.a, "establish an authenticated confirmed session");
    let first = p.alice.encrypt(p.a.clone(), first).await.unwrap();
    p.bob
        .receive_for_delivery(p.b.clone(), delivery_at(first, SystemTime::now()))
        .await
        .unwrap();
    let reply = p
        .bob
        .outbox_page(None)
        .await
        .unwrap()
        .messages
        .pop()
        .unwrap()
        .request()
        .envelope
        .clone()
        .unwrap();
    p.alice
        .receive_for_delivery(p.a.clone(), delivery_at(reply, SystemTime::now()))
        .await
        .unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.b);
    let (root, a) = (p.root.path().to_owned(), p.a.clone());
    // Fixture generates legitimate messages, not modified counter headers/MACs.
    // Omitted messages deliberately have no delivery/outbox in this crypto-only
    // gap experiment. Use libsignal's real in-memory stores to generate the gap,
    // then persist the final sender state once in the protected fixture. Avoid
    //25002 history/outbox inserts or session AEAD rewrites in this crypto test.
    let samples = tokio::task::spawn_blocking(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        store(&root, A, false)
            .transaction_with::<_, CryptoError>(|tx| {
                let view = View::prototype(tx, &a)?;
                let mut sessions = libsignal_protocol::InMemSessionStore::new();
                let mut identities = libsignal_protocol::InMemIdentityKeyStore::new(
                    runtime.block_on(view.get_identity_key_pair())?,
                    runtime.block_on(view.get_local_registration_id())?,
                );
                runtime.block_on(identities.save_identity(&a.address(), &a.peer_identity))?;
                runtime.block_on(sessions.store_session(
                    &a.address(),
                    &runtime.block_on(view.load_session(&a.address()))?.unwrap(),
                ))?;
                let mut samples = Vec::new();
                for index in 0..=25_001 {
                    let text = payload(&a, "gap sample");
                    let encrypted = runtime.block_on(message_encrypt(
                        &text.encode_to_vec(),
                        &a.address(),
                        &address(&a.own),
                        &mut sessions,
                        &mut identities,
                        SystemTime::now(),
                        &mut rand::rng(),
                    ))?;
                    assert!(matches!(encrypted, CiphertextMessage::SignalMessage(_)));
                    if [0, 2, 2002, 25_001].contains(&index) {
                        let message = SignalMessage::try_from(encrypted.serialize())?;
                        assert_eq!(message.counter(), index);
                        samples.push((
                            text.clone(),
                            a.envelope(
                                &text.message_id,
                                encrypted.serialize().to_vec(),
                                CiphertextType::Signal,
                            ),
                        ));
                    }
                }
                runtime.block_on(
                    view.clone().store_session(
                        &a.address(),
                        &runtime
                            .block_on(sessions.load_session(&a.address()))?
                            .unwrap(),
                    ),
                )?;
                Ok(samples)
            })
            .unwrap()
    })
    .await
    .unwrap();
    let bob = reopen(p.root.path(), &p.b.own).await;
    // A valid message beyond25k cannot be decrypted yet; failed receive must roll back.
    assert!(
        bob.decrypt(p.b.clone(), samples[3].1.clone())
            .await
            .is_err()
    );
    bob.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.b), before);
    assert!(
        saved(
            p.root.path(),
            &p.b,
            RecordKind::Inbox,
            &samples[3].0.message_id
        )
        .is_none()
    );
    let bob = reopen(p.root.path(), &p.b.own).await;
    assert_eq!(
        bob.decrypt(p.b.clone(), samples[2].1.clone())
            .await
            .unwrap(),
        samples[2].0
    );
    bob.shutdown().await.unwrap();
    let after_bridge = session(p.root.path(), &p.b);
    let bob = reopen(p.root.path(), &p.b.own).await;
    //2002 skipped keys exceed the pinned2000 retained keys:0,1 were evicted.
    assert!(
        bob.decrypt(p.b.clone(), samples[0].1.clone())
            .await
            .is_err()
    );
    bob.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.b), after_bridge);
    let bob = reopen(p.root.path(), &p.b.own).await;
    assert_eq!(
        bob.decrypt(p.b.clone(), samples[1].1.clone())
            .await
            .unwrap(),
        samples[1].0
    );
    // A nearer valid message advanced the chain, so the same far ciphertext now works.
    assert_eq!(
        bob.decrypt(p.b.clone(), samples[3].1.clone())
            .await
            .unwrap(),
        samples[3].0
    );
    bob.shutdown().await.unwrap();
}
