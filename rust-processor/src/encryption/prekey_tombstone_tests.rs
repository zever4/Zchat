//! Real Signal/protected-store tests for consumed-key redaction and replay guards.
use super::*;
use crate::encryption::contract::{MESSAGE_RETENTION_MS, envelope_digest};
use crate::proto::e2ee::Delivery;

fn wire(envelope: Envelope) -> Delivery {
    Delivery {
        sequence: 1,
        expires_at_unix_ms: 1_700_000_000_000 + MESSAGE_RETENTION_MS,
        envelope_digest: envelope_digest(&envelope).unwrap().to_vec(),
        envelope: Some(envelope),
    }
}
fn with_view<T>(
    root: &std::path::Path,
    scope: &SessionScope,
    apply: impl FnOnce(&tokio::runtime::Runtime, &mut View<'_, '_>) -> SignalResult<T>,
) -> CryptoResult<T> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    store(root, &scope.own.user_id, false).transaction_with(|tx| {
        let mut view = View::prototype(tx, scope)?;
        Ok(apply(&rt, &mut view)?)
    })
}
fn key_bytes(root: &std::path::Path, b: &SessionScope, kind: RecordKind, id: u32) -> Vec<u8> {
    saved(root, b, kind, &id.to_string()).unwrap()
}
fn assert_consumed(
    root: &std::path::Path,
    b: &SessionScope,
    kind: RecordKind,
    id: u32,
    old: &[u8],
) {
    let bytes = key_bytes(root, b, kind, id);
    assert_eq!(bytes, prekey_state::tombstone(kind, id, &old[3..]));
    assert_eq!(bytes.len(), 35);
    assert!(matches!(
        prekey_state::decode(kind, id, &bytes).unwrap(),
        prekey_state::Saved::Consumed(_)
    ));
}
fn capture_material(root: &std::path::Path, scope: &SessionScope) -> DeviceMaterial {
    let identity = saved(root, scope, RecordKind::Identity, "local").unwrap();
    DeviceMaterial {
        identity: IdentityKeyPair::try_from(&identity[5..]).unwrap(),
        registration_id: u32::from_be_bytes(identity[1..5].try_into().unwrap()),
        credential: Zeroizing::new(
            saved(root, scope, RecordKind::DeviceCredential, "local")
                .unwrap()
                .try_into()
                .unwrap(),
        ),
        signed: SignedPreKeyRecord::deserialize(
            &key_bytes(root, scope, RecordKind::SignedPreKey, SIGNED)[3..],
        )
        .unwrap(),
        ec: vec![
            PreKeyRecord::deserialize(&key_bytes(root, scope, RecordKind::EcPreKey, EC)[3..])
                .unwrap(),
        ],
        pq: [(PQ, false), (LR, true)]
            .into_iter()
            .map(|(id, lr)| {
                (
                    KyberPreKeyRecord::deserialize(
                        &key_bytes(root, scope, RecordKind::PqPreKey, id)[3..],
                    )
                    .unwrap(),
                    lr,
                )
            })
            .collect(),
    }
}
async fn reopen(root: &std::path::Path, own: &DeviceRef) -> CryptoCoordinator {
    CryptoCoordinator::start(store(root, &own.user_id, false), own.clone())
        .await
        .unwrap()
}

#[test]
fn receive_reopen_exact_install_and_save_keep_only_consumed_fingerprints() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let p = rt.block_on(pair(true, false));
    rt.block_on(p.alice.bootstrap(p.a.clone(), p.acquisition.clone()))
        .unwrap();
    let text = payload(&p.a, "erase consumed keys in the same receive commit");
    let envelope = rt
        .block_on(p.alice.encrypt(p.a.clone(), text.clone()))
        .unwrap();
    rt.block_on(p.alice.shutdown()).unwrap();
    rt.block_on(p.bob.shutdown()).unwrap();
    let original = capture_material(p.root.path(), &p.b);
    let ec = key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC);
    let pq = key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, PQ);
    let lr = key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, LR);
    let signed = key_bytes(p.root.path(), &p.b, RecordKind::SignedPreKey, SIGNED);
    let delivery = wire(envelope);
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    let (received, proof) = rt
        .block_on(bob.receive_for_delivery(p.b.clone(), delivery.clone()))
        .unwrap();
    assert_eq!(received, text);
    assert_eq!(proof.request().messages[0].disposition, 1);
    rt.block_on(bob.install_prepared(copy_material(&original)))
        .unwrap();
    rt.block_on(bob.shutdown()).unwrap();
    assert_consumed(p.root.path(), &p.b, RecordKind::EcPreKey, EC, &ec);
    assert_consumed(p.root.path(), &p.b, RecordKind::PqPreKey, PQ, &pq);
    assert_eq!(key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, LR), lr);
    assert_eq!(
        key_bytes(p.root.path(), &p.b, RecordKind::SignedPreKey, SIGNED),
        signed
    );
    with_view(p.root.path(), &p.b, |r, v| {
        r.block_on(v.save_pre_key(EC.into(), &original.ec[0]))?;
        r.block_on(v.save_kyber_pre_key(PQ.into(), &original.pq[0].0))?;
        r.block_on(v.save_kyber_pre_key(LR.into(), &original.pq[1].0))
    })
    .unwrap();
    assert!(matches!(
        with_view(p.root.path(), &p.b, |r, v| r
            .block_on(v.get_pre_key(EC.into()))),
        Err(CryptoError::Signal(SignalProtocolError::InvalidPreKeyId))
    ));
    assert!(matches!(
        with_view(p.root.path(), &p.b, |r, v| r
            .block_on(v.get_kyber_pre_key(PQ.into()))),
        Err(CryptoError::Signal(
            SignalProtocolError::InvalidKyberPreKeyId
        ))
    ));
    assert!(
        with_view(p.root.path(), &p.b, |r, v| r
            .block_on(v.remove_pre_key(EC.into())))
        .is_err()
    );
    let base = KeyPair::generate(&mut rand::rng()).public_key;
    assert!(
        with_view(p.root.path(), &p.b, |r, v| r.block_on(
            v.mark_kyber_pre_key_used(PQ.into(), SIGNED.into(), &base)
        ))
        .is_err()
    );
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    rt.block_on(bob.install_prepared(original)).unwrap();
    assert_eq!(
        rt.block_on(bob.receive_for_delivery(p.b.clone(), delivery))
            .unwrap()
            .0,
        text
    );
    assert_eq!(
        rt.block_on(bob.pending_acknowledgements(None))
            .unwrap()
            .acknowledgements
            .len(),
        1
    );
    rt.block_on(bob.shutdown()).unwrap();
    assert_consumed(p.root.path(), &p.b, RecordKind::EcPreKey, EC, &ec);
    assert_consumed(p.root.path(), &p.b, RecordKind::PqPreKey, PQ, &pq);
    assert_eq!(key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, LR), lr);
}

#[test]
fn conflicting_same_id_save_or_install_cannot_revive_a_consumed_key() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let p = rt.block_on(pair(true, false));
    rt.block_on(p.bob.shutdown()).unwrap();
    let mut original = capture_material(p.root.path(), &p.b);
    let ec = key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC);
    let pq = key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, PQ);
    store(p.root.path(), B, false)
        .transaction(|tx| {
            tx.put(
                &p.b.key(RecordKind::EcPreKey, &EC.to_string())?,
                &prekey_state::tombstone(RecordKind::EcPreKey, EC, &ec[3..]),
            )?;
            tx.put(
                &p.b.key(RecordKind::PqPreKey, &PQ.to_string())?,
                &prekey_state::tombstone(RecordKind::PqPreKey, PQ, &pq[3..]),
            )
        })
        .unwrap();
    let wrong = PreKeyRecord::new(EC.into(), &KeyPair::generate(&mut rand::rng()));
    assert!(
        with_view(p.root.path(), &p.b, |r, v| r
            .block_on(v.save_pre_key(EC.into(), &wrong)))
        .is_err()
    );
    let replacement = material();
    let changed = KyberPreKeyRecord::new(
        PQ.into(),
        original.pq[0].0.timestamp().unwrap(),
        &replacement.pq[0].0.key_pair().unwrap(),
        &replacement.pq[0].0.signature().unwrap(),
    );
    assert!(
        with_view(p.root.path(), &p.b, |r, v| r
            .block_on(v.save_kyber_pre_key(PQ.into(), &changed)))
        .is_err()
    );
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    original.pq[0].1 = true;
    original.pq[1].1 = false;
    assert!(matches!(
        rt.block_on(bob.install_prepared(original)),
        Err(CryptoError::Conflict)
    ));
    rt.block_on(bob.shutdown()).unwrap();
    rt.block_on(p.alice.shutdown()).unwrap();
    assert_consumed(p.root.path(), &p.b, RecordKind::EcPreKey, EC, &ec);
    assert_consumed(p.root.path(), &p.b, RecordKind::PqPreKey, PQ, &pq);
}

#[test]
fn failed_receive_commit_keeps_original_keys_and_returns_no_ack() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let p = rt.block_on(pair(true, false));
    rt.block_on(p.alice.bootstrap(p.a.clone(), p.acquisition.clone()))
        .unwrap();
    let text = payload(&p.a, "rollback must retain keys for the legitimate retry");
    let delivery = wire(
        rt.block_on(p.alice.encrypt(p.a.clone(), text.clone()))
            .unwrap(),
    );
    rt.block_on(p.bob.shutdown()).unwrap();
    let ec = key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC);
    let pq = key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, PQ);
    commit_fault(p.root.path(), B, RecordKind::Inbox as u8, true);
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    assert!(matches!(
        rt.block_on(bob.receive_for_delivery(p.b.clone(), delivery.clone())),
        Err(CryptoError::Storage(_))
    ));
    assert!(
        rt.block_on(bob.pending_acknowledgements(None))
            .unwrap()
            .acknowledgements
            .is_empty()
    );
    rt.block_on(bob.shutdown()).unwrap();
    assert_eq!(key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC), ec);
    assert_eq!(key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, PQ), pq);
    assert!(session(p.root.path(), &p.b).is_none());
    assert!(saved(p.root.path(), &p.b, RecordKind::History, &text.message_id).is_none());
    commit_fault(p.root.path(), B, RecordKind::Inbox as u8, false);
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    assert_eq!(
        rt.block_on(bob.receive_for_delivery(p.b.clone(), delivery))
            .unwrap()
            .0,
        text
    );
    rt.block_on(bob.shutdown()).unwrap();
    rt.block_on(p.alice.shutdown()).unwrap();
    assert_consumed(p.root.path(), &p.b, RecordKind::EcPreKey, EC, &ec);
    assert_consumed(p.root.path(), &p.b, RecordKind::PqPreKey, PQ, &pq);
}

#[test]
fn malformed_versions_flags_ids_and_foreign_fingerprints_fail_closed() {
    let ec = material().ec.remove(0).serialize().unwrap();
    let valid = prekey_state::tombstone(RecordKind::EcPreKey, EC, &ec);
    let mut invalid = vec![
        vec![],
        vec![2, 1, 0],
        vec![2, 1, 0, 1],
        vec![2, 0, 0],
        vec![3, 1, 0],
        vec![1, 2, 0, 1],
        vec![1, 1, 1, 1],
        vec![1, 0, 0, 1],
    ];
    for (offset, value) in [(0, 3), (1, 0), (2, 1)] {
        let mut b = valid.clone();
        b[offset] = value;
        invalid.push(b);
    }
    let mut long = valid.clone();
    long.push(0);
    invalid.push(long);
    for value in invalid {
        assert!(prekey_state::decode(RecordKind::EcPreKey, EC, &value).is_err());
    }
    assert!(prekey_state::decode(RecordKind::SignedPreKey, EC, &valid).is_err());
    // A protobuf containing only the matching ID is not a private key record.
    for kind in [RecordKind::EcPreKey, RecordKind::PqPreKey] {
        assert!(prekey_state::decode(kind, 0, &[1, 1, 0, 8, 0]).is_err());
    }
    let live = [&[1, 0, 0], ec.as_slice()].concat();
    assert!(prekey_state::decode(RecordKind::EcPreKey, EC + 1, &live).is_err());
    assert_ne!(
        valid,
        prekey_state::tombstone(RecordKind::EcPreKey, EC + 1, &ec)
    );
    assert_ne!(
        valid,
        prekey_state::tombstone(RecordKind::PqPreKey, EC, &ec)
    );
}

#[test]
fn legacy_redaction_pages_are_bounded_scoped_restartable_and_leave_live_lr_signed_untouched() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let p = rt.block_on(pair(true, false));
    rt.block_on(p.bob.shutdown()).unwrap();
    let live = key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC);
    let lr = key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, LR);
    let signed = key_bytes(p.root.path(), &p.b, RecordKind::SignedPreKey, SIGNED);
    store(p.root.path(), B, false)
        .transaction(|tx| {
            for id in 1..=37u32 {
                let record = PreKeyRecord::new(id.into(), &KeyPair::generate(&mut rand::rng()));
                tx.put(
                    &p.b.key(RecordKind::EcPreKey, &id.to_string())?,
                    &[&[1, 1, 0], record.serialize().unwrap().as_slice()].concat(),
                )?;
            }
            let mut pq = tx
                .get(&p.b.key(RecordKind::PqPreKey, &PQ.to_string())?)?
                .unwrap();
            pq[1] = 1;
            tx.put(&p.b.key(RecordKind::PqPreKey, &PQ.to_string())?, &pq)
        })
        .unwrap();
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    let first = rt.block_on(bob.redact_consumed_prekeys(None)).unwrap();
    assert_eq!(first.scanned, 32);
    assert!(first.redacted > 0 && first.redacted <= first.scanned);
    let first_redacted = first.redacted;
    let cursor = first.next.unwrap();
    assert!(matches!(
        rt.block_on(p.alice.redact_consumed_prekeys(Some(cursor.clone()))),
        Err(CryptoError::Conflict)
    ));
    rt.block_on(bob.shutdown()).unwrap();
    let other_generation = rt.block_on(reopen(p.root.path(), &device(B, AG)));
    assert!(matches!(
        rt.block_on(other_generation.redact_consumed_prekeys(Some(cursor.clone()))),
        Err(CryptoError::Conflict)
    ));
    rt.block_on(other_generation.shutdown()).unwrap();
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    let second = rt
        .block_on(bob.redact_consumed_prekeys(Some(cursor.clone())))
        .unwrap();
    assert_eq!(first_redacted + second.redacted, 37);
    assert!(second.scanned <= 32);
    let third = rt
        .block_on(bob.redact_consumed_prekeys(second.next))
        .unwrap();
    assert_eq!(third.scanned, 2);
    assert_eq!(third.redacted, 1);
    assert!(third.next.is_none());
    let retry = rt
        .block_on(bob.redact_consumed_prekeys(Some(cursor)))
        .unwrap();
    assert_eq!(retry.redacted, 0);
    let mut after = None;
    let mut scanned = 0;
    loop {
        let page = rt.block_on(bob.redact_consumed_prekeys(after)).unwrap();
        assert_eq!(page.redacted, 0);
        assert!(page.scanned <= 32);
        scanned += page.scanned;
        after = page.next;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(scanned, 40);
    rt.block_on(bob.shutdown()).unwrap();
    rt.block_on(p.alice.shutdown()).unwrap();
    assert_eq!(
        key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC),
        live
    );
    assert_eq!(key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, LR), lr);
    assert_eq!(
        key_bytes(p.root.path(), &p.b, RecordKind::SignedPreKey, SIGNED),
        signed
    );
    assert_eq!(
        key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, PQ).len(),
        35
    );
}

fn legacy_fault(root: &std::path::Path, install: bool) {
    let path = StoreScope::new(SERVER, B).unwrap().database_path(root);
    let db = rusqlite::Connection::open(path).unwrap();
    if install {
        db.execute_batch("CREATE TABLE redaction_parent(id INTEGER PRIMARY KEY); CREATE TABLE redaction_child(id INTEGER REFERENCES redaction_parent(id) DEFERRABLE INITIALLY DEFERRED); CREATE TRIGGER redaction_failure AFTER UPDATE ON protected_records WHEN NEW.kind=3 BEGIN INSERT INTO redaction_child VALUES(1); END;").unwrap();
    } else {
        db.execute_batch("DROP TRIGGER redaction_failure; DROP TABLE redaction_child; DROP TABLE redaction_parent;").unwrap();
    }
}
#[test]
fn legacy_redaction_commit_failure_and_corrupt_page_do_not_partially_erase_keys() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let p = rt.block_on(pair(true, false));
    rt.block_on(p.bob.shutdown()).unwrap();
    let mut old = key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC);
    old[1] = 1;
    store(p.root.path(), B, false)
        .transaction(|tx| tx.put(&p.b.key(RecordKind::EcPreKey, &EC.to_string())?, &old))
        .unwrap();
    legacy_fault(p.root.path(), true);
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    assert!(matches!(
        rt.block_on(bob.redact_consumed_prekeys(None)),
        Err(CryptoError::Storage(_))
    ));
    rt.block_on(bob.shutdown()).unwrap();
    assert_eq!(
        key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC),
        old
    );
    legacy_fault(p.root.path(), false);
    // Same page: a valid earlier row would be rewritten before the corrupt later row.
    store(p.root.path(), B, false)
        .transaction(|tx| tx.put(&p.b.key(RecordKind::EcPreKey, "4294967295")?, &[2, 1, 0]))
        .unwrap();
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    assert!(rt.block_on(bob.redact_consumed_prekeys(None)).is_err());
    rt.block_on(bob.shutdown()).unwrap();
    assert_eq!(
        key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC),
        old
    );
    rt.block_on(p.alice.shutdown()).unwrap();
}

#[test]
fn exact_save_of_legacy_used_material_upgrades_it_without_reviving_and_lr_guard_survives() {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let p = rt.block_on(pair(true, false));
    rt.block_on(p.bob.shutdown()).unwrap();
    let original = capture_material(p.root.path(), &p.b);
    let mut ec = key_bytes(p.root.path(), &p.b, RecordKind::EcPreKey, EC);
    let mut pq = key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, PQ);
    ec[1] = 1;
    pq[1] = 1;
    let lr = key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, LR);
    store(p.root.path(), B, false)
        .transaction(|tx| {
            tx.put(&p.b.key(RecordKind::EcPreKey, &EC.to_string())?, &ec)?;
            tx.put(&p.b.key(RecordKind::PqPreKey, &PQ.to_string())?, &pq)
        })
        .unwrap();
    with_view(p.root.path(), &p.b, |r, v| {
        r.block_on(v.save_pre_key(EC.into(), &original.ec[0]))
    })
    .unwrap();
    let bob = rt.block_on(reopen(p.root.path(), &p.b.own));
    rt.block_on(bob.install_prepared(copy_material(&original)))
        .unwrap();
    rt.block_on(bob.shutdown()).unwrap();
    assert_consumed(p.root.path(), &p.b, RecordKind::EcPreKey, EC, &ec);
    assert_consumed(p.root.path(), &p.b, RecordKind::PqPreKey, PQ, &pq);
    let base = KeyPair::generate(&mut rand::rng()).public_key;
    with_view(p.root.path(), &p.b, |r, v| {
        r.block_on(v.mark_kyber_pre_key_used(LR.into(), SIGNED.into(), &base))
    })
    .unwrap();
    assert!(matches!(
        with_view(p.root.path(), &p.b, |r, v| r.block_on(
            v.mark_kyber_pre_key_used(LR.into(), SIGNED.into(), &base)
        )),
        Err(CryptoError::Signal(SignalProtocolError::DuplicatedMessage(
            ..
        )))
    ));
    assert_eq!(key_bytes(p.root.path(), &p.b, RecordKind::PqPreKey, LR), lr);
    rt.block_on(p.alice.shutdown()).unwrap();
}
