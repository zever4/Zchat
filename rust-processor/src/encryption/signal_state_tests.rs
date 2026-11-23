use super::super::protected::{StoreScope, test_open};
use super::*;
use crate::proto::e2ee::{
    Device, KyberPreKey, OneTimePreKey, SignedPreKey, TextMessage, encrypted_payload,
};
use libsignal_protocol::{KeyPair, Timestamp};
use uuid::Uuid;

#[path = "renewal_tests.rs"]
mod renewal;

#[path = "delivery_tests.rs"]
mod delivery;

#[path = "lifecycle_tests.rs"]
mod lifecycle;

#[path = "runner_tests.rs"]
mod runner;

#[path = "confirmation_tests.rs"]
mod confirmation;

#[path = "retention_tests.rs"]
mod retention;

#[path = "prekey_tombstone_tests.rs"]
mod tombstones;

#[path = "prekey_id_tests.rs"]
mod key_ids;

#[path = "prekey_refill_tests.rs"]
mod refill;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const AG: &str = "33333333-3333-4333-8333-333333333333";
const BG: &str = "44444444-4444-4444-8444-444444444444";
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
const SERVER: &str = "https://chat.example.test";
const EC: u32 = 2147483648;
const SIGNED: u32 = 4294967293;
const PQ: u32 = 4294967294;
const LR: u32 = 4294967295;

#[tokio::test]
async fn durable_closure_blocks_stale_sending_scopes_without_rewriting_history_or_outbox() {
    let p = pair(true, false).await;
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let text = payload(&p.a, "keep the original ratchet/history/outbox");
    let encrypted = p.alice.encrypt(p.a.clone(), text.clone()).await.unwrap();
    let creation = p.alice.prepare_conversation_creation(42).await.unwrap();
    let active = Conversation {
        conversation_id: CHAT.into(),
        mode: ConversationMode::SignalPq1 as i32,
        own_device: Some(p.a.own.clone()),
        peer_device: Some(Device {
            reference: Some(p.a.peer.clone()),
            registration_id: p.a.peer_registration,
            identity_key: p.a.peer_identity.serialize().to_vec(),
            public_material_ready: true,
            public_material_revision: 1,
        }),
        state: ConversationState::Active as i32,
        ..Default::default()
    };
    p.alice
        .accept_conversation_creation(VerifiedConversationCreation::from_rpc(
            creation.clone(),
            active.clone(),
        ))
        .await
        .unwrap();
    let mut closed = active;
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = crate::proto::e2ee::ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_generation_id = BG.into();
    closed.closed_at_unix_ms = 1700000000123;
    closed.peer_device.as_mut().unwrap().public_material_ready = false;
    p.alice
        .accept_conversation_creation(VerifiedConversationCreation::from_rpc(creation, closed))
        .await
        .unwrap();
    assert!(matches!(
        p.alice
            .encrypt(p.a.clone(), payload(&p.a, "must not send"))
            .await,
        Err(CryptoError::ConversationClosed)
    ));
    assert!(matches!(
        p.alice.encrypt(p.a.clone(), text.clone()).await,
        Err(CryptoError::ConversationClosed)
    ));
    assert!(matches!(
        p.alice.prepare_acquisition(p.a.clone()).await,
        Err(CryptoError::ConversationClosed)
    ));
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert_eq!(
        saved(p.root.path(), &p.a, RecordKind::Outbox, &text.message_id).unwrap(),
        encrypted.encode_to_vec()
    );
    assert_eq!(
        saved(p.root.path(), &p.a, RecordKind::History, &text.message_id).unwrap(),
        text.encode_to_vec()
    );
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), p.a.own.clone())
        .await
        .unwrap();
    assert!(matches!(
        alice.prepare_acquisition(p.a).await,
        Err(CryptoError::ConversationClosed)
    ));
    alice.shutdown().await.unwrap();
}

#[test]
fn acquisition_clock_skew_does_not_extend_expiry_or_allow_unbounded_leases() {
    let now = 1_700_000_000_000;
    // PostgreSQL ahead of the desktop clock by two seconds (observed on WSL/Windows).
    assert!(check_acquisition_deadline(now + 86_402_000, now).is_ok());
    assert!(check_acquisition_deadline(now + 86_460_000, now).is_ok());
    assert!(matches!(
        check_acquisition_deadline(now + 86_460_001, now),
        Err(CryptoError::AcquisitionClockSkew)
    ));
    assert!(matches!(
        check_acquisition_deadline(i64::MAX, now),
        Err(CryptoError::AcquisitionClockSkew)
    ));
    assert!(check_acquisition_deadline(now + 1, now).is_ok());
    assert!(matches!(
        check_acquisition_deadline(now, now),
        Err(CryptoError::Expired)
    ));
    assert!(matches!(
        check_acquisition_deadline(now - 1, now),
        Err(CryptoError::Expired)
    ));
    assert!(check_acquisition_deadline(0, now).is_err());
}

#[tokio::test]
async fn fresh_bundle_from_a_slightly_faster_server_bootstraps_real_signal() {
    let p = pair(true, false).await;
    let mut bundle = p.acquisition;
    bundle.not_after_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
        + 86_430_000;
    p.alice
        .bootstrap(p.a.clone(), bundle.clone())
        .await
        .unwrap();
    // Applied replay must still retain the exact original server deadline.
    p.alice.bootstrap(p.a.clone(), bundle).await.unwrap();
    let message = payload(&p.a, "clock skew leaves the authenticated exchange intact");
    let envelope = p.alice.encrypt(p.a, message.clone()).await.unwrap();
    assert_eq!(p.bob.decrypt(p.b, envelope).await.unwrap(), message);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

fn device(user: &str, generation: &str) -> DeviceRef {
    DeviceRef {
        user_id: user.into(),
        device_id: 1,
        generation_id: generation.into(),
    }
}
pub(super) fn material() -> DeviceMaterial {
    let mut rng = rand::rng();
    let identity = IdentityKeyPair::generate(&mut rng);
    let pair = KeyPair::generate(&mut rng);
    let signature = identity
        .private_key()
        .calculate_signature(&pair.public_key.serialize(), &mut rng)
        .unwrap();
    let signed = SignedPreKeyRecord::new(
        SIGNED.into(),
        Timestamp::from_epoch_millis(1700000000123),
        &pair,
        &signature,
    );
    let ec = vec![PreKeyRecord::new(EC.into(), &KeyPair::generate(&mut rng))];
    let pq = [(PQ, false), (LR, true)]
        .into_iter()
        .map(|(id, lr)| {
            let pair = kem::KeyPair::generate(kem::KeyType::Kyber1024, &mut rng);
            let signature = identity
                .private_key()
                .calculate_signature(&pair.public_key.serialize(), &mut rng)
                .unwrap();
            (
                KyberPreKeyRecord::new(
                    id.into(),
                    Timestamp::from_epoch_millis(1700000000123),
                    &pair,
                    &signature,
                ),
                lr,
            )
        })
        .collect();
    DeviceMaterial {
        identity,
        registration_id: 16383,
        credential: Zeroizing::new([19; 32]),
        signed,
        ec,
        pq,
    }
}
pub(super) fn public(reference: DeviceRef, material: &DeviceMaterial) -> Device {
    Device {
        reference: Some(reference),
        registration_id: material.registration_id,
        identity_key: material.identity.public_key().serialize().to_vec(),
        public_material_ready: true,
        public_material_revision: 1,
    }
}
fn scope(own: DeviceRef, peer: Device) -> SessionScope {
    SessionScope::from_conversation(&Conversation {
        conversation_id: CHAT.into(),
        mode: ConversationMode::SignalPq1 as i32,
        own_device: Some(own),
        peer_device: Some(peer),
        state: ConversationState::Active as i32,
        ..Default::default()
    })
    .unwrap()
}
pub(super) fn acquired(
    peer: Device,
    material: &DeviceMaterial,
    ec: bool,
    lr: bool,
) -> AcquiredBundle {
    let pq = material.pq.iter().find(|(_, last)| *last == lr).unwrap();
    AcquiredBundle {
        acquisition_id: Uuid::new_v4().to_string(),
        conversation_id: CHAT.into(),
        device: Some(peer),
        signed_pre_key: Some(SignedPreKey {
            key_id: SIGNED,
            public_key: material.signed.public_key().unwrap().serialize().to_vec(),
            signature: material.signed.signature().unwrap(),
            created_at_unix_ms: 1700000000123,
        }),
        one_time_pre_key: ec.then(|| OneTimePreKey {
            key_id: EC,
            public_key: material.ec[0].public_key().unwrap().serialize().to_vec(),
        }),
        kyber_pre_key: Some(KyberPreKey {
            key_id: if lr { LR } else { PQ },
            public_key: pq.0.public_key().unwrap().serialize().to_vec(),
            signature: pq.0.signature().unwrap(),
            is_last_resort: lr,
        }),
        not_after_unix_ms: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 3600000,
    }
}
fn payload(scope: &SessionScope, text: &str) -> EncryptedPayload {
    EncryptedPayload {
        format_version: 1,
        message_id: Uuid::new_v4().to_string(),
        conversation_id: scope.conversation.clone(),
        sender: Some(scope.own.clone()),
        recipient: Some(scope.peer.clone()),
        created_at_unix_ms: 1700000000123,
        content: Some(encrypted_payload::Content::Text(TextMessage {
            text: text.into(),
        })),
    }
}
pub(super) fn store(root: &std::path::Path, user: &str, create: bool) -> ProtectedStore {
    test_open(root, StoreScope::new(SERVER, user).unwrap(), create)
}
fn saved(
    root: &std::path::Path,
    scope: &SessionScope,
    kind: RecordKind,
    id: &str,
) -> Option<Vec<u8>> {
    store(root, &scope.own.user_id, false)
        .transaction(|tx| tx.get(&scope.key(kind, id)?))
        .unwrap()
        .map(|v| v.to_vec())
}
fn session(root: &std::path::Path, scope: &SessionScope) -> Option<Vec<u8>> {
    store(root, &scope.own.user_id, false)
        .transaction(|tx| tx.get(&scope.peer_key(RecordKind::Session, true)?))
        .unwrap()
        .map(|v| v.to_vec())
}
struct Pair {
    root: tempfile::TempDir,
    alice: CryptoCoordinator,
    bob: CryptoCoordinator,
    a: SessionScope,
    b: SessionScope,
    acquisition: AcquiredBundle,
}
async fn pair(ec: bool, lr: bool) -> Pair {
    let root = tempfile::tempdir().unwrap();
    let am = material();
    let bm = material();
    let ap = public(device(A, AG), &am);
    let bp = public(device(B, BG), &bm);
    let a = scope(device(A, AG), bp.clone());
    let b = scope(device(B, BG), ap);
    let mut acquisition = acquired(bp, &bm, ec, lr);
    let alice = CryptoCoordinator::start(store(root.path(), A, true), device(A, AG))
        .await
        .unwrap();
    let bob = CryptoCoordinator::start(store(root.path(), B, true), device(B, BG))
        .await
        .unwrap();
    alice.install_prepared(am).await.unwrap();
    bob.install_prepared(bm).await.unwrap();
    acquisition.acquisition_id = alice
        .prepare_acquisition(a.clone())
        .await
        .unwrap()
        .request
        .acquisition_id;
    Pair {
        root,
        alice,
        bob,
        a,
        b,
        acquisition,
    }
}

#[tokio::test]
async fn real_signal_roundtrip_reopen_exact_outbox_and_inbox_replay() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let message = payload(&p.a, "private text; never log this");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    assert_eq!(envelope.message_type, CiphertextType::PrekeySignal as i32);
    assert_eq!(
        p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap(),
        envelope
    );
    assert_eq!(
        p.bob.decrypt(p.b.clone(), envelope.clone()).await.unwrap(),
        message
    );
    assert_eq!(
        p.bob.decrypt(p.b.clone(), envelope.clone()).await.unwrap(),
        message
    );
    let reply = payload(&p.b, "confirmation by real peer reply");
    let reply_envelope = p.bob.encrypt(p.b.clone(), reply.clone()).await.unwrap();
    assert_eq!(
        p.alice.decrypt(p.a.clone(), reply_envelope).await.unwrap(),
        reply
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a).unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    // Exact retry even after a reply/reopen must not rewind an established ratchet.
    alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    assert_eq!(
        alice.encrypt(p.a.clone(), message.clone()).await.unwrap(),
        envelope
    );
    assert_eq!(bob.decrypt(p.b.clone(), envelope).await.unwrap(), message);
    alice.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.a).unwrap(), before);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let next = payload(&p.a, "normal signal after reopen");
    let next_envelope = alice.encrypt(p.a.clone(), next.clone()).await.unwrap();
    assert_eq!(next_envelope.message_type, CiphertextType::Signal as i32);
    assert_eq!(bob.decrypt(p.b.clone(), next_envelope).await.unwrap(), next);
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
    assert!(saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1] == 1);
    assert!(saved(p.root.path(), &p.b, RecordKind::PqPreKey, &PQ.to_string()).unwrap()[1] == 1);
}

#[tokio::test]
async fn signatures_and_same_generation_identity_change_do_not_mutate_state() {
    let p = pair(true, false).await;
    for pq in [false, true] {
        let mut invalid = p.acquisition.clone();
        if pq {
            invalid.kyber_pre_key.as_mut().unwrap().signature[0] ^= 1;
        } else {
            invalid.signed_pre_key.as_mut().unwrap().signature[0] ^= 1;
        }
        assert!(matches!(
            p.alice.bootstrap(p.a.clone(), invalid).await,
            Err(CryptoError::Signal(
                SignalProtocolError::SignatureValidationFailed
            ))
        ));
    }
    p.alice.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.a).is_none());
    let pinned = store(p.root.path(), A, false)
        .transaction(|tx| tx.get(&p.a.peer_key(RecordKind::PeerIdentity, false)?))
        .unwrap();
    assert!(pinned.is_none());
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    alice.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a);
    let changed = material();
    let changed_public = public(device(B, BG), &changed);
    let changed_scope = scope(device(A, AG), changed_public.clone());
    let changed_bundle = acquired(changed_public, &changed, true, false);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert!(matches!(
        alice.bootstrap(changed_scope, changed_bundle).await,
        // The durable journal detects substitution even before libsignal runs.
        Err(CryptoError::Conflict)
    ));
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.a), before);
    let pinned = store(p.root.path(), A, false)
        .transaction(|tx| tx.get(&p.a.peer_key(RecordKind::PeerIdentity, false)?))
        .unwrap()
        .unwrap();
    assert_eq!(pinned.as_slice(), p.a.peer_identity.serialize().as_ref());
}

#[tokio::test]
async fn acquisition_request_is_durable_and_applied_replay_never_advances_ratchet() {
    let p = pair(true, false).await;
    let first = p.alice.prepare_acquisition(p.a.clone()).await.unwrap();
    assert!(!first.applied);
    assert_eq!(first.request.acquisition_id, p.acquisition.acquisition_id);
    assert_eq!(
        p.alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .request,
        first.request
    );
    p.alice.shutdown().await.unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert_eq!(
        alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .request,
        first.request
    );
    let mut foreign = p.acquisition.clone();
    foreign.acquisition_id = Uuid::new_v4().to_string();
    assert!(matches!(
        alice.bootstrap(p.a.clone(), foreign).await,
        Err(CryptoError::Conflict)
    ));
    alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let message = payload(&p.a, "no ratchet rewind after acquired response replay");
    let encrypted = alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    assert_eq!(
        p.bob.decrypt(p.b.clone(), encrypted.clone()).await.unwrap(),
        message
    );
    let applied = alice.prepare_acquisition(p.a.clone()).await.unwrap();
    assert!(applied.applied);
    assert_eq!(applied.request, first.request);
    alice.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert!(
        alice
            .prepare_acquisition(p.a.clone())
            .await
            .unwrap()
            .applied
    );
    alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    assert_eq!(
        alice.encrypt(p.a.clone(), message).await.unwrap(),
        encrypted
    );
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.a), before);
}

#[tokio::test]
async fn unrequested_bundle_and_inbound_session_never_trigger_fresh_bootstrap() {
    let p = pair(true, false).await;
    let mut alternate = p.a.clone();
    alternate.conversation = Uuid::new_v4().to_string();
    let mut unsolicited = p.acquisition.clone();
    unsolicited.conversation_id = alternate.conversation.clone();
    assert!(matches!(
        p.alice.bootstrap(alternate.clone(), unsolicited).await,
        Err(CryptoError::Invalid)
    ));
    p.alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    let pending = p.bob.prepare_acquisition(p.b.clone()).await.unwrap();
    let message = payload(&p.a, "inbound may win the bootstrap race");
    let encrypted = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    p.bob.decrypt(p.b.clone(), encrypted).await.unwrap();
    assert!(matches!(
        p.bob.prepare_acquisition(p.b.clone()).await,
        Err(CryptoError::SessionExists)
    ));
    let mut snapshot = acquired(
        child_scope(&p.b).peer_device.unwrap(),
        &material(),
        true,
        false,
    );
    snapshot.acquisition_id = pending.request.acquisition_id;
    // Owner/identity match, so the existing-session guard must run before the
    // deliberately unrelated prekeys/signatures can reset that session.
    assert!(matches!(
        p.bob.bootstrap(p.b.clone(), snapshot).await,
        Err(CryptoError::SessionExists)
    ));
    let reply = payload(&p.b, "original inbound session remains usable");
    let response = p.bob.encrypt(p.b.clone(), reply.clone()).await.unwrap();
    assert_eq!(p.alice.decrypt(p.a, response).await.unwrap(), reply);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn acquisition_commit_failures_keep_the_pending_request_and_no_applied_state() {
    let p = pair(true, false).await;
    commit_fault(p.root.path(), A, RecordKind::Acquisition as u8, true);
    assert!(
        p.alice
            .bootstrap(p.a.clone(), p.acquisition.clone())
            .await
            .is_err()
    );
    let plan = p.alice.prepare_acquisition(p.a.clone()).await.unwrap();
    assert!(!plan.applied);
    assert_eq!(plan.request.acquisition_id, p.acquisition.acquisition_id);
    p.alice.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.a).is_none());
    assert!(
        saved(
            p.root.path(),
            &p.a,
            RecordKind::Acquisition,
            &p.acquisition.acquisition_id
        )
        .is_none()
    );
    commit_fault(p.root.path(), A, RecordKind::Acquisition as u8, false);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    alice.bootstrap(p.a.clone(), p.acquisition).await.unwrap();
    assert!(alice.prepare_acquisition(p.a).await.unwrap().applied);
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn prepare_commit_failure_returns_no_request_and_expiry_does_not_renew_it() {
    let p = pair(true, false).await;
    let mut another = p.a.clone();
    another.conversation = Uuid::new_v4().to_string();
    commit_fault(p.root.path(), A, RecordKind::Control as u8, true);
    assert!(p.alice.prepare_acquisition(another.clone()).await.is_err());
    p.alice.shutdown().await.unwrap();
    let pending = store(p.root.path(), A, false)
        .transaction(|tx| tx.get(&another.acquisition_key()?))
        .unwrap();
    assert!(pending.is_none());
    commit_fault(p.root.path(), A, RecordKind::Control as u8, false);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let mut expired = p.acquisition;
    expired.not_after_unix_ms = 1;
    assert!(matches!(
        alice.bootstrap(p.a.clone(), expired.clone()).await,
        Err(CryptoError::Expired)
    ));
    let plan = alice.prepare_acquisition(p.a).await.unwrap();
    assert!(!plan.applied);
    assert_eq!(plan.request.acquisition_id, expired.acquisition_id);
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn process_exit_before_after_acquisition_prepare_commit_recovers_request() {
    for after in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let a = scope(device(A, AG), public(device(B, BG), &material()));
        let alice = CryptoCoordinator::start(store(root.path(), A, true), device(A, AG))
            .await
            .unwrap();
        alice.install_prepared(material()).await.unwrap();
        alice.shutdown().await.unwrap();
        std::fs::write(
            root.path().join("child-conversation.bin"),
            child_scope(&a).encode_to_vec(),
        )
        .unwrap();
        let child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "encryption::signal_state::tests::signal_crash_worker",
                "--nocapture",
            ])
            .env("ZCHAT_SIGNAL_CHILD_ROOT", root.path())
            .env("ZCHAT_SIGNAL_CHILD_ID", Uuid::new_v4().to_string())
            .env("ZCHAT_SIGNAL_CHILD_RECEIVE", "no")
            .env("ZCHAT_SIGNAL_CHILD_AFTER", if after { "yes" } else { "no" })
            .env("ZCHAT_SIGNAL_CHILD_ACQUIRE", "yes")
            .output()
            .unwrap();
        assert_eq!(child.status.code(), Some(if after { 74 } else { 73 }));
        let saved = store(root.path(), A, false)
            .transaction_with(|tx| load_acquisition(tx, &a))
            .unwrap();
        assert_eq!(saved.is_some(), after);
        let alice = CryptoCoordinator::start(store(root.path(), A, false), device(A, AG))
            .await
            .unwrap();
        let resumed = alice.prepare_acquisition(a.clone()).await.unwrap();
        if let Some(saved) = saved {
            assert_eq!(resumed.request, saved);
        }
        assert!(!resumed.applied);
        alice.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn altered_payload_routing_rolls_back_decrypt_prekeys_and_session() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let message = payload(&p.a, "authenticated binding");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    let mut changed = envelope.clone();
    changed.message_id = Uuid::new_v4().to_string();
    assert!(matches!(
        p.bob.decrypt(p.b.clone(), changed).await,
        Err(CryptoError::Invalid)
    ));
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.b).is_none());
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1],
        0
    );
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::PqPreKey, &PQ.to_string()).unwrap()[1],
        0
    );
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert_eq!(bob.decrypt(p.b.clone(), envelope).await.unwrap(), message);
    bob.shutdown().await.unwrap();
    p.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_ciphertext_then_valid_retry_does_not_burn_prekeys() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let message = payload(&p.a, "retry after bad ciphertext");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    let mut damaged = envelope.clone();
    let index = damaged.ciphertext.len() - 1;
    damaged.ciphertext[index] ^= 1;
    assert!(p.bob.decrypt(p.b.clone(), damaged).await.is_err());
    assert_eq!(p.bob.decrypt(p.b.clone(), envelope).await.unwrap(), message);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn optional_ec_and_last_resort_pq_work_and_detect_bootstrap_reuse() {
    let p = pair(false, true).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let message = payload(&p.a, "no EC prekey, PQ last resort");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    assert_eq!(p.bob.decrypt(p.b.clone(), envelope).await.unwrap(), message);
    p.bob.shutdown().await.unwrap();
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::PqPreKey, &LR.to_string()).unwrap()[1],
        0
    );
    // Exercise the actual KyberPreKeyStore callback with the same last-resort tuple.
    let mut state = store(p.root.path(), B, false);
    let bscope = p.b.clone();
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let base = KeyPair::generate(&mut rand::rng()).public_key;
        state
            .transaction_with::<_, CryptoError>(|tx| {
                let mut view = View::prototype(tx, &bscope)?;
                rt.block_on(view.mark_kyber_pre_key_used(LR.into(), SIGNED.into(), &base))?;
                Ok(())
            })
            .unwrap();
        assert!(
            state
                .transaction_with::<_, CryptoError>(|tx| {
                    let mut view = View::prototype(tx, &bscope)?;
                    rt.block_on(view.mark_kyber_pre_key_used(LR.into(), SIGNED.into(), &base))?;
                    Ok(())
                })
                .is_err()
        );
    })
    .await
    .unwrap();
    p.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn account_generation_conversation_and_identifier_conflicts_fail_closed() {
    let p = pair(true, false).await;
    let mut wrong = p.a.clone();
    wrong.own = device(B, BG);
    assert!(
        p.alice
            .bootstrap(wrong, p.acquisition.clone())
            .await
            .is_err()
    );
    let mut expired = p.acquisition.clone();
    expired.not_after_unix_ms = 1;
    assert!(matches!(
        p.alice.bootstrap(p.a.clone(), expired).await,
        Err(CryptoError::Expired)
    ));
    let mut wrong_peer = p.acquisition.clone();
    wrong_peer
        .device
        .as_mut()
        .unwrap()
        .reference
        .as_mut()
        .unwrap()
        .generation_id = Uuid::new_v4().to_string();
    assert!(p.alice.bootstrap(p.a.clone(), wrong_peer).await.is_err());
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let mut conflict = p.acquisition.clone();
    conflict.not_after_unix_ms += 1;
    assert!(matches!(
        p.alice.bootstrap(p.a.clone(), conflict).await,
        Err(CryptoError::Conflict)
    ));
    let message = payload(&p.a, "original");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    let mut changed = message.clone();
    changed.created_at_unix_ms += 1;
    assert!(matches!(
        p.alice.encrypt(p.a.clone(), changed).await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(
        p.alice.encrypt(p.a.clone(), message).await.unwrap(),
        envelope
    );
    let mut other_chat = p.a.clone();
    other_chat.conversation = Uuid::new_v4().to_string();
    assert!(
        p.alice
            .encrypt(
                other_chat.clone(),
                payload(&other_chat, "no shared session")
            )
            .await
            .is_err()
    );
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &other_chat).is_none());
    let mut changed_peer = p.a.clone();
    changed_peer.peer.generation_id = Uuid::new_v4().to_string();
    assert!(session(p.root.path(), &changed_peer).is_none());
}

#[tokio::test]
async fn lost_first_prekey_message_and_out_of_order_delivery_survive_saved_state() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let first = payload(&p.a, "first delayed");
    let later = payload(&p.a, "second first");
    let one = p.alice.encrypt(p.a.clone(), first.clone()).await.unwrap();
    let two = p.alice.encrypt(p.a.clone(), later.clone()).await.unwrap();
    assert_eq!(p.bob.decrypt(p.b.clone(), two).await.unwrap(), later);
    assert_eq!(p.bob.decrypt(p.b.clone(), one).await.unwrap(), first);
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn current_account_cannot_start_worker_on_other_account_store() {
    let root = tempfile::tempdir().unwrap();
    assert!(matches!(
        CryptoCoordinator::start(store(root.path(), A, true), device(B, BG)).await,
        Err(CryptoError::Invalid)
    ));
    // Ownership is released on constructor failure; no background worker leaked.
    drop(store(root.path(), A, false));
}

#[tokio::test]
async fn queue_full_and_disconnected_worker_return_explicit_errors() {
    let (sender, receiver) = mpsc::sync_channel(1);
    let (_done, stopped) = oneshot::channel();
    let coordinator = CryptoCoordinator {
        attachment_budget: std::sync::Arc::new(tokio::sync::Semaphore::new(1)),
        sender,
        stopped,
        view_instance: uuid::Uuid::new_v4(),
    };
    let (reply, _result) = oneshot::channel();
    coordinator
        .sender
        .try_send(Command::Install(material(), reply))
        .unwrap_or_else(|_| panic!("empty queue"));
    assert!(matches!(
        coordinator.install_prepared(material()).await,
        Err(CryptoError::Busy)
    ));
    drop(receiver);
    assert!(matches!(
        coordinator.install_prepared(material()).await,
        Err(CryptoError::Stopped)
    ));
}

fn commit_fault(root: &std::path::Path, user: &str, kind: u8, install: bool) {
    let path = StoreScope::new(SERVER, user).unwrap().database_path(root);
    let db = rusqlite::Connection::open(path).unwrap();
    if install {
        db.execute_batch(&format!("CREATE TABLE fault_parent(id INTEGER PRIMARY KEY);
            CREATE TABLE fault_child(id INTEGER REFERENCES fault_parent(id) DEFERRABLE INITIALLY DEFERRED);
            CREATE TRIGGER fault_after_crypto AFTER INSERT ON protected_records WHEN NEW.kind={kind}
            BEGIN INSERT INTO fault_child VALUES(1); END;")).unwrap();
    } else {
        db.execute_batch(
            "DROP TRIGGER fault_after_crypto; DROP TABLE fault_child; DROP TABLE fault_parent;",
        )
        .unwrap();
    }
}

#[tokio::test]
async fn commit_failure_rolls_back_real_encrypt_ratchet_history_and_outbox() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    p.alice.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a);
    commit_fault(p.root.path(), A, RecordKind::Outbox as u8, true);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let message = payload(&p.a, "do not expose ciphertext before commit");
    assert!(matches!(
        alice.encrypt(p.a.clone(), message.clone()).await,
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    alice.shutdown().await.unwrap();
    assert_eq!(session(p.root.path(), &p.a), before);
    assert!(
        saved(
            p.root.path(),
            &p.a,
            RecordKind::History,
            &message.message_id
        )
        .is_none()
    );
    assert!(saved(p.root.path(), &p.a, RecordKind::Outbox, &message.message_id).is_none());
    commit_fault(p.root.path(), A, RecordKind::Outbox as u8, false);
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let envelope = alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    assert_eq!(p.bob.decrypt(p.b.clone(), envelope).await.unwrap(), message);
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn commit_failure_rolls_back_real_decrypt_and_prekey_consumption() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let message = payload(&p.a, "do not return plaintext before commit");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    p.bob.shutdown().await.unwrap();
    commit_fault(p.root.path(), B, RecordKind::Inbox as u8, true);
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert!(matches!(
        bob.decrypt(p.b.clone(), envelope.clone()).await,
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.b).is_none());
    assert!(
        saved(
            p.root.path(),
            &p.b,
            RecordKind::History,
            &message.message_id
        )
        .is_none()
    );
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1],
        0
    );
    assert_eq!(
        saved(p.root.path(), &p.b, RecordKind::PqPreKey, &PQ.to_string()).unwrap()[1],
        0
    );
    commit_fault(p.root.path(), B, RecordKind::Inbox as u8, false);
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert_eq!(bob.decrypt(p.b.clone(), envelope).await.unwrap(), message);
    p.alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

fn copy_material(source: &DeviceMaterial) -> DeviceMaterial {
    DeviceMaterial {
        identity: source.identity,
        registration_id: source.registration_id,
        credential: source.credential.clone(),
        signed: SignedPreKeyRecord::deserialize(&source.signed.serialize().unwrap()).unwrap(),
        ec: source
            .ec
            .iter()
            .map(|p| PreKeyRecord::deserialize(&p.serialize().unwrap()).unwrap())
            .collect(),
        pq: source
            .pq
            .iter()
            .map(|(p, lr)| {
                (
                    KyberPreKeyRecord::deserialize(&p.serialize().unwrap()).unwrap(),
                    *lr,
                )
            })
            .collect(),
    }
}

#[tokio::test]
async fn prepared_install_is_atomic_and_exact_retry_preserves_consumed_flags() {
    let root = tempfile::tempdir().unwrap();
    let original = material();
    let mut duplicate = copy_material(&original);
    duplicate
        .ec
        .push(PreKeyRecord::deserialize(&duplicate.ec[0].serialize().unwrap()).unwrap());
    let bob = CryptoCoordinator::start(store(root.path(), B, true), device(B, BG))
        .await
        .unwrap();
    assert!(matches!(
        bob.install_prepared(duplicate).await,
        Err(CryptoError::Invalid)
    ));
    bob.shutdown().await.unwrap();
    let absent = store(root.path(), B, false)
        .transaction(|tx| tx.get(&RecordKey::new(BG, RecordKind::Identity, "local")?))
        .unwrap();
    assert!(absent.is_none());
    let bp = public(device(B, BG), &original);
    let am = material();
    let ap = public(device(A, AG), &am);
    let a = scope(device(A, AG), bp.clone());
    let b = scope(device(B, BG), ap);
    let mut bundle = acquired(bp, &original, true, false);
    let bob = CryptoCoordinator::start(store(root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    bob.install_prepared(copy_material(&original))
        .await
        .unwrap();
    let alice = CryptoCoordinator::start(store(root.path(), A, true), device(A, AG))
        .await
        .unwrap();
    alice.install_prepared(am).await.unwrap();
    bundle.acquisition_id = alice
        .prepare_acquisition(a.clone())
        .await
        .unwrap()
        .request
        .acquisition_id;
    alice.bootstrap(a.clone(), bundle).await.unwrap();
    let message = payload(&a, "consume once");
    let envelope = alice.encrypt(a, message.clone()).await.unwrap();
    bob.decrypt(b.clone(), envelope).await.unwrap();
    bob.install_prepared(original).await.unwrap();
    bob.shutdown().await.unwrap();
    alice.shutdown().await.unwrap();
    assert_eq!(
        saved(root.path(), &b, RecordKind::EcPreKey, &EC.to_string()).unwrap()[1],
        1
    );
    assert_eq!(
        saved(root.path(), &b, RecordKind::PqPreKey, &PQ.to_string()).unwrap()[1],
        1
    );
}

#[tokio::test]
async fn incomplete_or_oversized_bundle_and_missing_session_fail_without_downgrade() {
    let p = pair(true, false).await;
    let mut no_pq = p.acquisition.clone();
    no_pq.kyber_pre_key = None;
    assert!(p.alice.bootstrap(p.a.clone(), no_pq).await.is_err());
    let mut oversized = p.acquisition.clone();
    oversized
        .kyber_pre_key
        .as_mut()
        .unwrap()
        .public_key
        .resize(9000, 0);
    assert!(matches!(
        p.alice.bootstrap(p.a.clone(), oversized).await,
        Err(CryptoError::Invalid)
    ));
    assert!(
        p.alice
            .encrypt(p.a.clone(), payload(&p.a, "requires explicit bootstrap"))
            .await
            .is_err()
    );
    let mut wrong = p.acquisition.clone();
    wrong.one_time_pre_key.as_mut().unwrap().public_key.push(0);
    assert!(p.alice.bootstrap(p.a.clone(), wrong).await.is_err());
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert!(session(p.root.path(), &p.a).is_none());
}

#[tokio::test]
async fn applied_bootstrap_receipt_never_repairs_corrupt_session_implicitly() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    p.alice.shutdown().await.unwrap();
    store(p.root.path(), A, false)
        .transaction(|tx| {
            tx.put(
                &p.a.peer_key(RecordKind::Session, true)?,
                b"invalid serialized session",
            )
        })
        .unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    assert!(alice.bootstrap(p.a.clone(), p.acquisition).await.is_err());
    alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    assert_eq!(
        session(p.root.path(), &p.a).unwrap(),
        b"invalid serialized session"
    );
}

#[test]
fn malformed_callback_cannot_be_swallowed_to_commit_partial_state() {
    let root = tempfile::tempdir().unwrap();
    let peer = public(device(B, BG), &material());
    let a = scope(device(A, AG), peer);
    let mut store = store(root.path(), A, true);
    let record = RecordKey::new(AG, RecordKind::EcPreKey, &EC.to_string()).unwrap();
    store
        .transaction(|tx| tx.put(&record, &[1, 0, 0, 0, 0]))
        .unwrap();
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert!(matches!(
        store.transaction(|tx| {
            let view = View::prototype(tx, &a)?;
            assert!(rt.block_on(view.get_pre_key(EC.into())).is_err());
            drop(view);
            tx.put(
                &a.key(RecordKind::History, "should-not-commit")?,
                b"partial state",
            )?;
            Ok(())
        }),
        Err(StoreError::Aborted)
    ));
    assert!(
        store
            .transaction(|tx| tx.get(&a.key(RecordKind::History, "should-not-commit")?))
            .unwrap()
            .is_none()
    );
}

fn child_scope(scope: &SessionScope) -> Conversation {
    Conversation {
        conversation_id: scope.conversation.clone(),
        mode: ConversationMode::SignalPq1 as i32,
        own_device: Some(scope.own.clone()),
        peer_device: Some(Device {
            reference: Some(scope.peer.clone()),
            registration_id: scope.peer_registration,
            identity_key: scope.peer_identity.serialize().to_vec(),
            public_material_ready: true,
            public_material_revision: 1,
        }),
        state: ConversationState::Active as i32,
        ..Default::default()
    }
}
fn crash_child(
    root: &std::path::Path,
    scope: &SessionScope,
    id: &str,
    receive: bool,
    after: bool,
) -> std::process::Output {
    // Public routing/key metadata and ciphertext only; no private key/plaintext files.
    std::fs::write(
        root.join("child-conversation.bin"),
        child_scope(scope).encode_to_vec(),
    )
    .unwrap();
    std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "encryption::signal_state::tests::signal_crash_worker",
            "--nocapture",
        ])
        .env("ZCHAT_SIGNAL_CHILD_ROOT", root)
        .env("ZCHAT_SIGNAL_CHILD_ID", id)
        .env(
            "ZCHAT_SIGNAL_CHILD_RECEIVE",
            if receive { "yes" } else { "no" },
        )
        .env("ZCHAT_SIGNAL_CHILD_AFTER", if after { "yes" } else { "no" })
        .output()
        .unwrap()
}

#[tokio::test]
async fn process_exit_before_and_after_real_encrypt_commit_recovers_exact_outbox() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let before = session(p.root.path(), &p.a);
    let mut message = payload(&p.a, "synthetic crash fixture");
    message.message_id = Uuid::new_v4().to_string();
    assert_eq!(
        crash_child(p.root.path(), &p.a, &message.message_id, false, false)
            .status
            .code(),
        Some(73)
    );
    assert_eq!(session(p.root.path(), &p.a), before);
    assert!(saved(p.root.path(), &p.a, RecordKind::Outbox, &message.message_id).is_none());
    assert!(
        saved(
            p.root.path(),
            &p.a,
            RecordKind::History,
            &message.message_id
        )
        .is_none()
    );
    assert_eq!(
        crash_child(p.root.path(), &p.a, &message.message_id, false, true)
            .status
            .code(),
        Some(74)
    );
    assert_ne!(session(p.root.path(), &p.a), before);
    let saved = saved(p.root.path(), &p.a, RecordKind::Outbox, &message.message_id).unwrap();
    let envelope = Envelope::decode(saved.as_slice()).unwrap();
    let alice = CryptoCoordinator::start(store(p.root.path(), A, false), device(A, AG))
        .await
        .unwrap();
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert_eq!(alice.encrypt(p.a, message.clone()).await.unwrap(), envelope);
    assert_eq!(bob.decrypt(p.b, envelope).await.unwrap(), message);
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn process_exit_before_and_after_real_decrypt_preserves_keys_and_inbox() {
    let p = pair(true, false).await;
    p.alice
        .bootstrap(p.a.clone(), p.acquisition.clone())
        .await
        .unwrap();
    let message = payload(&p.a, "synthetic inbound crash fixture");
    let envelope = p.alice.encrypt(p.a.clone(), message.clone()).await.unwrap();
    p.alice.shutdown().await.unwrap();
    p.bob.shutdown().await.unwrap();
    let original_keys =
        [(RecordKind::EcPreKey, EC), (RecordKind::PqPreKey, PQ)].map(|(kind, id)| {
            (
                kind,
                id,
                saved(p.root.path(), &p.b, kind, &id.to_string()).unwrap(),
            )
        });
    std::fs::write(
        p.root.path().join("child-envelope.bin"),
        envelope.encode_to_vec(),
    )
    .unwrap();
    assert_eq!(
        crash_child(p.root.path(), &p.b, &message.message_id, true, false)
            .status
            .code(),
        Some(73)
    );
    assert!(session(p.root.path(), &p.b).is_none());
    for (kind, id, original) in &original_keys {
        assert_eq!(
            &saved(p.root.path(), &p.b, *kind, &id.to_string()).unwrap(),
            original
        );
    }
    assert!(saved(p.root.path(), &p.b, RecordKind::Inbox, &message.message_id).is_none());
    assert_eq!(
        crash_child(p.root.path(), &p.b, &message.message_id, true, true)
            .status
            .code(),
        Some(74)
    );
    assert!(session(p.root.path(), &p.b).is_some());
    for (kind, id, original) in &original_keys {
        assert_eq!(
            saved(p.root.path(), &p.b, *kind, &id.to_string()).unwrap(),
            prekey_state::tombstone(*kind, *id, &original[3..])
        );
    }
    let bob = CryptoCoordinator::start(store(p.root.path(), B, false), device(B, BG))
        .await
        .unwrap();
    assert_eq!(bob.decrypt(p.b, envelope).await.unwrap(), message);
    bob.shutdown().await.unwrap();
}

#[test]
fn signal_crash_worker() {
    let Some(root) = std::env::var_os("ZCHAT_SIGNAL_CHILD_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let id = std::env::var("ZCHAT_SIGNAL_CHILD_ID").unwrap();
    let receive = std::env::var("ZCHAT_SIGNAL_CHILD_RECEIVE").unwrap() == "yes";
    let after = std::env::var("ZCHAT_SIGNAL_CHILD_AFTER").unwrap() == "yes";
    let scope = SessionScope::from_conversation(
        &Conversation::decode(
            std::fs::read(root.join("child-conversation.bin"))
                .unwrap()
                .as_slice(),
        )
        .unwrap(),
    )
    .unwrap();
    let mut state = store(&root, &scope.own.user_id, false);
    let acquire = std::env::var_os("ZCHAT_SIGNAL_CHILD_ACQUIRE").is_some();
    state.test_exit_on_record(
        if acquire {
            scope.acquisition_key().unwrap()
        } else {
            scope
                .key(
                    if receive {
                        RecordKind::Inbox
                    } else {
                        RecordKind::Outbox
                    },
                    &id,
                )
                .unwrap()
        },
        after,
    );
    let mut engine = Engine {
        store: state,
        own: scope.own.clone(),
        runtime: tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    };
    if acquire {
        engine.prepare_acquisition(&scope).unwrap();
    } else if receive {
        let envelope = Envelope::decode(
            std::fs::read(root.join("child-envelope.bin"))
                .unwrap()
                .as_slice(),
        )
        .unwrap();
        engine.decrypt(&scope, &envelope).unwrap();
    } else {
        let mut message = payload(&scope, "synthetic crash fixture");
        message.message_id = id;
        engine.encrypt(&scope, &message).unwrap();
    }
    panic!("test fault hook did not terminate child");
}

#[cfg(windows)]
#[tokio::test]
async fn real_windows_dpapi_signal_worker_roundtrip_and_reopen() {
    let root = tempfile::tempdir().unwrap();
    let am = material();
    let bm = material();
    let ap = public(device(A, AG), &am);
    let bp = public(device(B, BG), &bm);
    let a = scope(device(A, AG), bp.clone());
    let b = scope(device(B, BG), ap);
    let mut bundle = acquired(bp, &bm, true, false);
    let ascope = StoreScope::new(SERVER, A).unwrap();
    let bscope = StoreScope::new(SERVER, B).unwrap();
    let alice = CryptoCoordinator::start(
        ProtectedStore::create(root.path(), ascope.clone()).unwrap(),
        device(A, AG),
    )
    .await
    .unwrap();
    let bob = CryptoCoordinator::start(
        ProtectedStore::create(root.path(), bscope.clone()).unwrap(),
        device(B, BG),
    )
    .await
    .unwrap();
    alice.install_prepared(am).await.unwrap();
    bob.install_prepared(bm).await.unwrap();
    bundle.acquisition_id = alice
        .prepare_acquisition(a.clone())
        .await
        .unwrap()
        .request
        .acquisition_id;
    alice.bootstrap(a.clone(), bundle).await.unwrap();
    let message = payload(&a, "actual Windows DPAPI plus libsignal");
    let envelope = alice.encrypt(a.clone(), message.clone()).await.unwrap();
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
    let alice = CryptoCoordinator::start(
        ProtectedStore::open(root.path(), ascope).unwrap(),
        device(A, AG),
    )
    .await
    .unwrap();
    let bob = CryptoCoordinator::start(
        ProtectedStore::open(root.path(), bscope).unwrap(),
        device(B, BG),
    )
    .await
    .unwrap();
    assert_eq!(alice.encrypt(a, message.clone()).await.unwrap(), envelope);
    assert_eq!(bob.decrypt(b, envelope).await.unwrap(), message);
    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}
