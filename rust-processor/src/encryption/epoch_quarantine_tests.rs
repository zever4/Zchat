use super::super::super::super::tests::store;
use super::super::super::tests::{index, records, rewrite_frozen, wrapped};
use super::super::tests::{accepted, b_conversation, delivery, payload, prepared, raw, send};
use super::*;
use crate::proto::e2ee::ConversationCloseReason;

const B: &str = "22222222-2222-4222-8222-222222222222";
const WHY: EpochQuarantineReason = EpochQuarantineReason::InvalidCiphertext;
fn damaged(mut d: v2::Delivery) -> v2::Delivery {
    let e = d.envelope.as_mut().unwrap();
    e.ciphertext = vec![1];
    d.envelope_digest = data::envelope_digest(e).unwrap().to_vec();
    d
}
fn uuid(d: &v2::Delivery) -> String {
    d.envelope.as_ref().unwrap().message_id.clone()
}
fn crypto(root: &std::path::Path) -> Vec<(i64, String, Vec<u8>)> {
    records(root, B)
        .into_iter()
        .filter(|(k, _, _)| *k <= RecordKind::Publication as i64)
        .collect()
}
async fn reject(w: &CryptoCoordinator, s: &EpochScope, d: &v2::Delivery) -> EpochDurableAck {
    w.quarantine_epoch(s.clone(), delivery(s, d), WHY)
        .await
        .unwrap()
}

#[tokio::test]
async fn failed_receive_is_not_rejection_explicit_quarantine_preserves_ciphertext_keys_and_reopens()
{
    let (f, a, b) = prepared().await;
    let d = damaged(
        send(
            &f.alice,
            &a,
            payload(&a, "retained damaged v2 ciphertext"),
            1,
        )
        .await,
    );
    let before = records(f.root.path(), B);
    let keys = crypto(f.root.path());
    let master = wrapped(f.root.path(), B);
    assert!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &d))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    assert!(
        f.bob
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    let proof = reject(&f.bob, &b, &d).await;
    assert!(proof.rejected() && !proof.acknowledged());
    assert_eq!(
        proof.request().context.as_ref().unwrap().protocol_version,
        2
    );
    assert_eq!(
        proof.request().messages[0].envelope_digest,
        d.envelope_digest
    );
    assert_eq!(crypto(f.root.path()), keys);
    assert_eq!(wrapped(f.root.path(), B), master);
    let saved = f.bob.epoch_quarantined_message(uuid(&d)).await.unwrap();
    assert_eq!(saved.delivery(), &d);
    assert!(saved.reason() == WHY);
    assert!(matches!(
        f.bob.epoch_incoming_message(uuid(&d)).await,
        Err(CryptoError::Quarantined)
    ));
    assert!(matches!(
        f.bob.receive_epoch(b.clone(), delivery(&b, &d)).await,
        Err(CryptoError::Quarantined)
    ));
    assert!(
        f.bob
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let committed = records(f.root.path(), B);
    assert_eq!(reject(&f.bob, &b, &d).await.request(), proof.request());
    assert_eq!(records(f.root.path(), B), committed);
    f.bob.shutdown().await.unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    let proof = bob.epoch_ack(uuid(&d)).await.unwrap();
    assert!(proof.rejected());
    assert_eq!(
        bob.epoch_quarantined_message(uuid(&d))
            .await
            .unwrap()
            .delivery(),
        &d
    );
    bob.accept_epoch_ack(accepted(&proof)).await.unwrap();
    assert!(bob.epoch_ack(uuid(&d)).await.unwrap().acknowledged());
    assert!(
        bob.epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    assert_eq!(
        bob.epoch_quarantined_message(uuid(&d))
            .await
            .unwrap()
            .delivery(),
        &d
    );
    assert_eq!(crypto(f.root.path()), keys);
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn quarantine_blocks_outbound_bootstrap_but_next_first_prekey_can_create_inbound_session() {
    let (f, a, b) = prepared().await;
    let acquisition = f.bob.prepare_epoch_acquisition(b.clone()).await.unwrap();
    let nonce = acquisition.request().acquisition_id.clone();
    let bad = damaged(send(&f.alice, &a, payload(&a, "first bad message"), 1).await);
    let before = crypto(f.root.path());
    reject(&f.bob, &b, &bad).await;
    assert_eq!(crypto(f.root.path()), before);
    assert!(matches!(
        f.bob.prepare_epoch_acquisition(b.clone()).await,
        Err(CryptoError::Conflict)
    ));
    let text = payload(&a, "first successful receive after quarantine");
    let good = send(&f.alice, &a, text.clone(), 2).await;
    assert_eq!(
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &good))
            .await
            .unwrap()
            .0,
        text
    );
    assert_eq!(
        f.bob.epoch_outbox_page(None).await.unwrap().messages.len(),
        1
    );
    assert!(matches!(
        f.bob.prepare_epoch_acquisition(b.clone()).await,
        Err(CryptoError::SessionExists)
    ));
    assert_eq!(
        f.bob
            .epoch_quarantined_message(uuid(&bad))
            .await
            .unwrap()
            .delivery(),
        &bad
    );
    f.bob.shutdown().await.unwrap();
    let mut db = store(f.root.path(), B, false);
    db.transaction(|tx| {
        let k = RecordKey::new(
            &b.own.generation_id,
            RecordKind::Acquisition,
            &b.acquisition_id,
        )?;
        let bytes = tx.get(&k)?.unwrap();
        assert!(bytes.windows(nonce.len()).any(|x| x == nonce.as_bytes()));
        Ok(())
    })
    .unwrap();
    drop(db);
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn quarantine_cannot_mask_lost_ratchet_crypto_marker_or_partial_activity_replay() {
    for fault in 0..5 {
        let (f, a, b) = prepared().await;
        let bad = damaged(send(&f.alice, &a, payload(&a, "before real crypto"), 1).await);
        reject(&f.bob, &b, &bad).await;
        let qid = format!(
            "epoch-v2:{}:quarantine-traffic:{:016x}",
            b.conversation, b.epoch
        );
        let old = records(f.root.path(), B)
            .into_iter()
            .find(|(_, id, _)| id == &qid)
            .unwrap()
            .2;
        let good = send(&f.alice, &a, payload(&a, "established session"), 2).await;
        f.bob
            .receive_epoch(b.clone(), delivery(&b, &good))
            .await
            .unwrap();
        let next = send(&f.alice, &a, payload(&a, "cannot reset lost ratchet"), 3).await;
        f.bob.shutdown().await.unwrap();
        let db = raw(f.root.path());
        db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
        db.execute(
            "DELETE FROM protected_records WHERE kind=?1 AND record_id=?2",
            rusqlite::params![RecordKind::Session as i64, b.session_id],
        )
        .unwrap();
        match fault {
            1 | 4 => {
                db.execute(
                    "DELETE FROM protected_records WHERE record_id=?1",
                    [format!(
                        "epoch-v2:{}:traffic:{:016x}",
                        b.conversation, b.epoch
                    )],
                )
                .unwrap();
            }
            2 => {
                db.execute(
                    "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
                    rusqlite::params![old, qid],
                )
                .unwrap();
            }
            3 => {
                db.execute("DELETE FROM protected_records WHERE record_id=?1", [&qid])
                    .unwrap();
            }
            _ => (),
        }
        if fault == 4 {
            db.execute(
                "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
                rusqlite::params![old, qid],
            )
            .unwrap();
        }
        drop(db);
        let before = records(f.root.path(), B);
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        assert!(
            bob.receive_epoch(b.clone(), delivery(&b, &next))
                .await
                .is_err(),
            "fault{fault}"
        );
        assert!(
            bob.prepare_epoch_acquisition(b.clone()).await.is_err(),
            "fault{fault}"
        );
        assert_eq!(records(f.root.path(), B), before);
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn rejection_binds_native_origin_account_context_peer_epoch_digest_and_size() {
    let (f, a, b) = prepared().await;
    let d = damaged(send(&f.alice, &a, payload(&a, "quarantine binding"), 1).await);
    let before = records(f.root.path(), B);
    assert!(
        f.alice
            .quarantine_epoch(b.clone(), delivery(&b, &d), WHY)
            .await
            .is_err()
    );
    let foreign = StoreScope::new("https://other.example.test", B).unwrap();
    let wrong =
        EpochObservation::test_observation(foreign, delivery(&b, &d).request().clone(), d.clone());
    assert!(f.bob.quarantine_epoch(b.clone(), wrong, WHY).await.is_err());
    for fault in 0..5 {
        let mut bad = d.clone();
        match fault {
            0 => bad.envelope_digest[0] ^= 1,
            1 => bad.envelope.as_mut().unwrap().crypto_epoch = Some(1),
            2 => {
                bad.envelope
                    .as_mut()
                    .unwrap()
                    .sender
                    .as_mut()
                    .unwrap()
                    .generation_id = uuid::Uuid::new_v4().to_string()
            }
            3 => bad.envelope.as_mut().unwrap().ciphertext = vec![0; 70_000],
            _ => (),
        }
        if fault != 0 && fault != 3 {
            bad.envelope_digest = data::envelope_digest(bad.envelope.as_ref().unwrap())
                .unwrap()
                .to_vec();
        }
        let mut req = delivery(&b, &bad).request().clone();
        if fault == 4 {
            req.context.as_mut().unwrap().protocol_version = 1;
        }
        let observed = EpochObservation::test_observation(b.scope.clone(), req, bad);
        assert!(
            f.bob
                .quarantine_epoch(b.clone(), observed, WHY)
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), B), before);
    }
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn disposition_reason_and_exact_delivery_are_immutable_and_outgoing_uuid_cannot_be_reused() {
    let (f, a, b) = prepared().await;
    let d = send(&f.alice, &a, payload(&a, "already Stored"), 1).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    let before = records(f.root.path(), B);
    assert!(matches!(
        f.bob
            .quarantine_epoch(b.clone(), delivery(&b, &d), WHY)
            .await,
        Err(CryptoError::Conflict)
    ));
    assert!(f.bob.epoch_quarantined_message(uuid(&d)).await.is_err());
    assert_eq!(records(f.root.path(), B), before);
    let bad = damaged(send(&f.alice, &a, payload(&a, "new bad message"), 2).await);
    let proof = reject(&f.bob, &b, &bad).await;
    let before = records(f.root.path(), B);
    assert!(matches!(
        f.bob
            .quarantine_epoch(
                b.clone(),
                delivery(&b, &bad),
                EpochQuarantineReason::PayloadBinding
            )
            .await,
        Err(CryptoError::Conflict)
    ));
    for fault in 0..3 {
        let mut changed = bad.clone();
        match fault {
            0 => changed.sequence += 1,
            1 => changed.expires_at_unix_ms += 1,
            _ => {
                changed.envelope.as_mut().unwrap().ciphertext.push(2);
                changed.envelope_digest = data::envelope_digest(changed.envelope.as_ref().unwrap())
                    .unwrap()
                    .to_vec();
            }
        }
        assert!(
            f.bob
                .quarantine_epoch(b.clone(), delivery(&b, &changed), WHY)
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), B), before);
    }
    assert_eq!(reject(&f.bob, &b, &bad).await.request(), proof.request());
    let mut own = payload(&b, "cannot overwrite rejected incoming");
    own.message_id = uuid(&bad);
    assert!(f.bob.encrypt_epoch(b.clone(), own).await.is_err());
    let outgoing = payload(&b, "owned outgoing UUID");
    f.bob
        .encrypt_epoch(b.clone(), outgoing.clone())
        .await
        .unwrap();
    let mut incoming = payload(&a, "same UUID other direction");
    incoming.message_id = outgoing.message_id;
    let collision = damaged(send(&f.alice, &a, incoming, 3).await);
    let before = records(f.root.path(), B);
    assert!(
        f.bob
            .quarantine_epoch(b.clone(), delivery(&b, &collision), WHY)
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), B), before);
    f.bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn quarantine_and_ack_commit_failures_preserve_retryable_exact_state() {
    for ack_phase in [false, true] {
        let (f, a, b) = prepared().await;
        let d = damaged(send(&f.alice, &a, payload(&a, "atomic rejection"), 1).await);
        let proof = if ack_phase {
            Some(reject(&f.bob, &b, &d).await)
        } else {
            None
        };
        let before = records(f.root.path(), B);
        let db = raw(f.root.path());
        let prefix = if ack_phase {
            "delivery-acked:v2:%"
        } else {
            "delivery-in:v2:%"
        };
        db.execute_batch(&format!("CREATE TABLE quar_parent(id INTEGER PRIMARY KEY);CREATE TABLE quar_child(id INTEGER REFERENCES quar_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER quar_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE '{prefix}' BEGIN INSERT INTO quar_child VALUES(1);END;")).unwrap();
        let result = if let Some(p) = &proof {
            f.bob.accept_epoch_ack(accepted(p)).await
        } else {
            f.bob
                .quarantine_epoch(b.clone(), delivery(&b, &d), WHY)
                .await
                .map(|_| ())
        };
        assert!(matches!(result, Err(CryptoError::Storage(_))));
        assert_eq!(records(f.root.path(), B), before);
        if !ack_phase {
            assert!(f.bob.epoch_ack(uuid(&d)).await.is_err());
        } else {
            assert!(!f.bob.epoch_ack(uuid(&d)).await.unwrap().acknowledged());
        }
        db.execute_batch("DROP TRIGGER quar_fault").unwrap();
        let proof = reject(&f.bob, &b, &d).await;
        f.bob.accept_epoch_ack(accepted(&proof)).await.unwrap();
        let committed = records(f.root.path(), B);
        f.bob.accept_epoch_ack(accepted(&proof)).await.unwrap();
        assert_eq!(records(f.root.path(), B), committed);
        f.bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn concurrent_receive_and_explicit_reject_commit_only_one_disposition_in_either_order() {
    for reject_first in [false, true] {
        let (f, a, b) = prepared().await;
        // Valid ciphertext: explicit trusted rejection is a policy decision,
        // not an automatic classification or a second decrypt attempt.
        let text = payload(&a, "one committed incoming disposition");
        let d = send(&f.alice, &a, text.clone(), 1).await;
        let receive = f.bob.receive_epoch(b.clone(), delivery(&b, &d));
        let reject = f.bob.quarantine_epoch(b.clone(), delivery(&b, &d), WHY);
        let (received, rejected) = if reject_first {
            let (q, p) = tokio::join!(reject, receive);
            (p, q)
        } else {
            tokio::join!(receive, reject)
        };
        let proof = f.bob.epoch_ack(uuid(&d)).await.unwrap();
        assert_eq!(proof.rejected(), reject_first);
        if reject_first {
            assert!(matches!(received, Err(CryptoError::Quarantined)));
            assert!(rejected.unwrap().rejected());
            assert!(
                f.bob
                    .epoch_outbox_page(None)
                    .await
                    .unwrap()
                    .messages
                    .is_empty()
            );
        } else {
            assert_eq!(received.unwrap().0, text);
            assert!(matches!(rejected, Err(CryptoError::Conflict)));
            assert_eq!(
                f.bob.epoch_outbox_page(None).await.unwrap().messages.len(),
                1
            );
        }
        assert_eq!(
            f.bob
                .epoch_ack_page(None)
                .await
                .unwrap()
                .acknowledgements
                .len(),
            1
        );
        f.bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn incomplete_or_invalid_quarantine_cohort_never_repairs_or_mints_ack() {
    for fault in 0..9 {
        let (f, a, b) = prepared().await;
        let d = damaged(send(&f.alice, &a, payload(&a, "rejected cohort"), 1).await);
        let proof = reject(&f.bob, &b, &d).await;
        if fault == 4 {
            f.bob.accept_epoch_ack(accepted(&proof)).await.unwrap();
        }
        f.bob.shutdown().await.unwrap();
        if fault <= 4 {
            // Match public kind/id without reaching into RecordKey internals.
            let (kind, id) = match fault {
                0 => (RecordKind::Inbox, proof.id.clone()),
                1 => (RecordKind::Control, format!("message-v2:{}", uuid(&d))),
                2 => (RecordKind::Control, format!("delivery-in:{}", proof.id)),
                3 => (
                    RecordKind::Control,
                    format!(
                        "epoch-v2:{}:quarantine-traffic:{:016x}",
                        b.conversation, b.epoch
                    ),
                ),
                _ => (RecordKind::Control, format!("delivery-acked:{}", proof.id)),
            };
            let db = raw(f.root.path());
            db.execute_batch("PRAGMA foreign_keys=OFF").unwrap();
            db.execute(
                "DELETE FROM protected_records WHERE kind=?1 AND record_id=?2",
                rusqlite::params![kind as i64, id],
            )
            .unwrap();
        } else {
            let mut db = store(f.root.path(), B, false);
            db.transaction(|tx| {
                let key = journal_key(&b.own, &proof.id).unwrap();
                let bytes = tx.get(&key)?.unwrap();
                let mut j = Journal::decode(bytes.as_slice()).unwrap();
                match fault {
                    5 => j.quarantine_reason = None,
                    6 => j.quarantine_reason = Some(4),
                    7 => j.quarantine_reason = Some(0),
                    _ => j.confirmation_required = Some(true),
                }
                tx.put(&key, &j.encode_to_vec())?;
                Ok(())
            })
            .unwrap();
        }
        let before = records(f.root.path(), B);
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        assert!(bob.epoch_ack(uuid(&d)).await.is_err(), "fault{fault}");
        assert!(
            bob.epoch_quarantined_message(uuid(&d)).await.is_err(),
            "fault{fault}"
        );
        assert!(
            bob.receive_epoch(b.clone(), delivery(&b, &d))
                .await
                .is_err(),
            "fault{fault}"
        );
        assert!(
            bob.quarantine_epoch(b.clone(), delivery(&b, &d), WHY)
                .await
                .is_err(),
            "fault{fault}"
        );
        assert!(
            bob.accept_epoch_ack(accepted(&proof)).await.is_err(),
            "fault{fault}"
        );
        assert_eq!(records(f.root.path(), B), before);
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn mixed_stored_rejected_pages_and_explicit_bounded_reseed_preserve_terminal_acks() {
    let (f, a, b) = prepared().await;
    let mut expected = std::collections::BTreeSet::new();
    let mut rejected_ids = Vec::new();
    for n in 0..65 {
        let d = damaged(
            send(
                &f.alice,
                &a,
                payload(&a, &format!("bounded reject {n}")),
                n + 1,
            )
            .await,
        );
        let reason = [
            WHY,
            EpochQuarantineReason::PayloadBinding,
            EpochQuarantineReason::UntrustedIdentity,
        ][n as usize % 3];
        let p = f
            .bob
            .quarantine_epoch(b.clone(), delivery(&b, &d), reason)
            .await
            .unwrap();
        if n < 10 {
            f.bob.accept_epoch_ack(accepted(&p)).await.unwrap();
        } else {
            expected.insert(uuid(&d));
        }
        rejected_ids.push(uuid(&d));
    }
    let d = send(&f.alice, &a, payload(&a, "mixed real Stored"), 66).await;
    f.bob
        .receive_epoch(b.clone(), delivery(&b, &d))
        .await
        .unwrap();
    expected.insert(uuid(&d));
    assert_eq!(
        f.bob
            .epoch_ack_page(None)
            .await
            .unwrap()
            .acknowledgements
            .len(),
        32
    );
    f.bob.shutdown().await.unwrap();
    raw(f.root.path()).execute_batch("PRAGMA foreign_keys=OFF;UPDATE protected_records SET recovery_ref=NULL;DELETE FROM protected_recovery;DELETE FROM protected_records WHERE record_id IN ('recovery-index:state:v1','recovery-index:anchor:v1');").unwrap();
    let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        bob.epoch_ack_page(None).await,
        Err(CryptoError::RecoveryIndexesNotReady)
    ));
    for round in 0..30 {
        let p = bob.seed_recovery_indexes().await.unwrap();
        assert!(p.scanned <= 32);
        if p.ready {
            break;
        }
        assert!(round < 29);
    }
    let mut found = std::collections::BTreeSet::new();
    let mut cursor = None;
    loop {
        let page = bob.epoch_ack_page(cursor).await.unwrap();
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
    assert_eq!(bob.epoch_outbox_page(None).await.unwrap().messages.len(), 1);
    for id in rejected_ids {
        assert!(bob.epoch_quarantined_message(id).await.is_ok());
    }
    // Only the one stored text and its automatic confirmation have History rows.
    assert_eq!(
        records(f.root.path(), B)
            .iter()
            .filter(|(k, _, _)| *k == RecordKind::History as i64)
            .count(),
        2
    );
    bob.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn durable_freeze_and_peer_closure_allow_rejection_without_new_signal_or_reply() {
    for frozen in [true, false] {
        let (f, a, b) = prepared().await;
        let d = damaged(send(&f.alice, &a, payload(&a, "old accepted ciphertext"), 1).await);
        let keys = crypto(f.root.path());
        f.bob.shutdown().await.unwrap();
        if frozen {
            let mut c = f.a.clone();
            c.own_device = Some(b.own.clone());
            rewrite_frozen(f.root.path(), &c, true);
        }
        let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
            .await
            .unwrap();
        if !frozen {
            let mut c = b_conversation(&f.a, &b);
            c.state = ConversationState::Closed as i32;
            c.close_reason = ConversationCloseReason::DeviceReplaced as i32;
            c.closed_generation_id = b.peer.generation_id.clone();
            c.closed_at_unix_ms = 1_700_000_000_001;
            index(&bob, &c).await;
        }
        let p = reject(&bob, &b, &d).await;
        bob.accept_epoch_ack(accepted(&p)).await.unwrap();
        assert_eq!(crypto(f.root.path()), keys);
        assert!(
            bob.epoch_outbox_page(None)
                .await
                .unwrap()
                .messages
                .is_empty()
        );
        assert!(
            bob.epoch_ack_page(None)
                .await
                .unwrap()
                .acknowledgements
                .is_empty()
        );
        bob.shutdown().await.unwrap();
        f.alice.shutdown().await.unwrap();
    }
}

#[test]
fn quarantine_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_EPOCH_QUAR_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_EPOCH_QUAR_CRASH_AFTER").unwrap() == "1";
    let ack_phase = std::env::var("ZCHAT_EPOCH_QUAR_CRASH_ACK").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let d = v2::Delivery::decode(
                std::fs::read(root.join("public-quarantine.pb"))
                    .unwrap()
                    .as_slice(),
            )
            .unwrap();
            let e = d.envelope.as_ref().unwrap();
            let own = e.recipient.clone().unwrap();
            let id = format!(
                "v2:{}:{:016x}:{}",
                e.conversation_id,
                e.crypto_epoch.unwrap(),
                e.message_id
            );
            let mut db = store(root, B, false);
            db.test_exit_on_record(
                if ack_phase {
                    marker_key(&own, &id).unwrap()
                } else {
                    journal_key(&own, &id).unwrap()
                },
                after,
            );
            let bob = CryptoCoordinator::start(db, own).await.unwrap();
            if ack_phase {
                let p = bob.epoch_ack(uuid(&d)).await.unwrap();
                let _ = bob.accept_epoch_ack(accepted(&p)).await;
            } else {
                let s = bob.epoch_scope(e.conversation_id.clone(), 0).await.unwrap();
                let _ = bob.quarantine_epoch(s.clone(), delivery(&s, &d), WHY).await;
            }
            panic!("quarantine crash hook did not fire");
        });
}

#[tokio::test]
async fn process_exit_before_after_quarantine_and_rejected_ack_commits_recovers_exact_state() {
    for ack_phase in [false, true] {
        for after in [false, true] {
            let (f, a, b) = prepared().await;
            let d = damaged(send(&f.alice, &a, payload(&a, "quarantine crash"), 1).await);
            if ack_phase {
                reject(&f.bob, &b, &d).await;
            }
            std::fs::write(
                f.root.path().join("public-quarantine.pb"),
                d.encode_to_vec(),
            )
            .unwrap();
            f.bob.shutdown().await.unwrap();
            let before = records(f.root.path(), B);
            let result=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","encryption::signal_state::epoch_state::incoming::quarantine::tests::quarantine_crash_child","--nocapture"])
            .env("ZCHAT_EPOCH_QUAR_CRASH_PATH",f.root.path()).env("ZCHAT_EPOCH_QUAR_CRASH_AFTER",if after{"1"}else{"0"}).env("ZCHAT_EPOCH_QUAR_CRASH_ACK",if ack_phase{"1"}else{"0"}).status().unwrap();
            assert_eq!(result.code(), Some(if after { 74 } else { 73 }));
            let bob = CryptoCoordinator::start(store(f.root.path(), B, false), b.own.clone())
                .await
                .unwrap();
            if !after {
                assert_eq!(records(f.root.path(), B), before);
            }
            if !ack_phase && !after {
                assert!(bob.epoch_ack(uuid(&d)).await.is_err());
            }
            let proof = reject(&bob, &b, &d).await;
            assert!(proof.rejected());
            assert_eq!(proof.acknowledged(), ack_phase && after);
            assert_eq!(
                bob.epoch_quarantined_message(uuid(&d))
                    .await
                    .unwrap()
                    .delivery(),
                &d
            );
            bob.accept_epoch_ack(accepted(&proof)).await.unwrap();
            assert!(
                bob.epoch_ack_page(None)
                    .await
                    .unwrap()
                    .acknowledgements
                    .is_empty()
            );
            bob.shutdown().await.unwrap();
            f.alice.shutdown().await.unwrap();
        }
    }
}
