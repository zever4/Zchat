use super::*;
use crate::encryption::protected::test_open;
use crate::proto::e2ee::{ConversationCloseReason, Device};
use std::path::Path;

const A: &str = "11111111-1111-4111-8111-111111111111";
const B: &str = "22222222-2222-4222-8222-222222222222";
const G: &str = "33333333-3333-4333-8333-333333333333";
const H: &str = "44444444-4444-4444-8444-444444444444";
const C: &str = "66666666-6666-4666-8666-666666666666";
const SERVER: &str = "https://chat.example.test";
const PEER: i64 = 42;

fn own() -> DeviceRef {
    DeviceRef {
        user_id: A.into(),
        device_id: 1,
        generation_id: G.into(),
    }
}
fn scope() -> StoreScope {
    StoreScope::new(SERVER, A).unwrap()
}
fn engine(root: &Path, create: bool) -> Engine {
    Engine {
        store: test_open(root, scope(), create),
        own: own(),
        runtime: tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap(),
    }
}
fn response() -> Conversation {
    Conversation {
        conversation_id: C.into(),
        mode: ConversationMode::SignalPq1 as i32,
        own_device: Some(own()),
        peer_device: Some(Device {
            reference: Some(DeviceRef {
                user_id: B.into(),
                device_id: 1,
                generation_id: H.into(),
            }),
            registration_id: 123,
            identity_key: IdentityKeyPair::generate(&mut rand::rng())
                .public_key()
                .serialize()
                .to_vec(),
            public_material_ready: true,
            public_material_revision: 1,
        }),
        state: ConversationState::Active as i32,
        ..Default::default()
    }
}
fn closed(mut response: Conversation) -> Conversation {
    response.state = ConversationState::Closed as i32;
    response.close_reason = ConversationCloseReason::DeviceReplaced as i32;
    response.closed_generation_id = H.into();
    response.closed_at_unix_ms = 1_700_000_000_000;
    response.peer_device.as_mut().unwrap().public_material_ready = false;
    response
}
fn verified(
    attempt: &ConversationCreation,
    response: Conversation,
) -> VerifiedConversationCreation {
    VerifiedConversationCreation::from_rpc(attempt.clone(), response)
}
fn index(e: &mut Engine) -> Option<Conversation> {
    e.store
        .transaction(|tx| tx.get(&conversation_key(&own(), C)?))
        .unwrap()
        .map(|bytes| Conversation::decode(bytes.as_slice()).unwrap())
}

#[tokio::test]
async fn concurrent_preparation_and_reopen_keep_one_exact_request_per_account_peer() {
    let root = tempfile::tempdir().unwrap();
    let worker = CryptoCoordinator::start(test_open(root.path(), scope(), true), own())
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        worker.prepare_conversation_creation(PEER),
        worker.prepare_conversation_creation(PEER)
    );
    let a = a.unwrap();
    assert_eq!(a.request(), b.unwrap().request());
    assert_eq!(
        a.request().context.as_ref().unwrap().device_generation_id,
        G
    );
    assert_ne!(
        a.request(),
        worker
            .prepare_conversation_creation(PEER + 1)
            .await
            .unwrap()
            .request()
    );
    assert!(worker.prepare_conversation_creation(0).await.is_err());
    let foreign_scope = StoreScope::new(SERVER, B).unwrap();
    let foreign = CryptoCoordinator::start(
        test_open(root.path(), foreign_scope, true),
        DeviceRef {
            user_id: B.into(),
            ..own()
        },
    )
    .await
    .unwrap();
    assert_ne!(
        a.request(),
        foreign
            .prepare_conversation_creation(PEER)
            .await
            .unwrap()
            .request()
    );
    foreign.shutdown().await.unwrap();
    worker.shutdown().await.unwrap();
    let worker = CryptoCoordinator::start(test_open(root.path(), scope(), false), own())
        .await
        .unwrap();
    assert_eq!(
        worker
            .prepare_conversation_creation(PEER)
            .await
            .unwrap()
            .request(),
        a.request()
    );
    worker.shutdown().await.unwrap();
}

#[test]
fn persisted_result_replays_after_restart_and_cannot_change_binding() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    let a = e.prepare_conversation(PEER, None).unwrap();
    let result = response();
    assert_eq!(
        e.accept_conversation(verified(&a, result.clone())).unwrap(),
        result
    );
    drop(e);
    let mut e = engine(root.path(), false);
    let retry = e.prepare_conversation(PEER, None).unwrap();
    assert_eq!(retry.request(), a.request());
    assert_eq!(
        e.accept_conversation(verified(&retry, result.clone()))
            .unwrap(),
        result
    );
    for change in 0..5 {
        let mut bad = result.clone();
        match change {
            0 => bad.conversation_id = uuid::Uuid::new_v4().to_string(),
            1 => {
                bad.peer_device
                    .as_mut()
                    .unwrap()
                    .reference
                    .as_mut()
                    .unwrap()
                    .user_id = uuid::Uuid::new_v4().to_string()
            }
            2 => {
                bad.peer_device
                    .as_mut()
                    .unwrap()
                    .reference
                    .as_mut()
                    .unwrap()
                    .generation_id = uuid::Uuid::new_v4().to_string()
            }
            3 => bad.peer_device.as_mut().unwrap().registration_id += 1,
            _ => {
                bad.peer_device.as_mut().unwrap().identity_key =
                    response().peer_device.unwrap().identity_key
            }
        }
        assert!(matches!(
            e.accept_conversation(verified(&a, bad)),
            Err(CryptoError::Conflict)
        ));
        assert_eq!(index(&mut e).unwrap(), result);
    }
}

#[test]
fn receipts_require_exact_request_owner_and_server_scope() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    let a = e.prepare_conversation(PEER, None).unwrap();
    let result = response();
    for change in 0..6 {
        let mut bad = a.clone();
        match change {
            0 => bad.request.creation_request_id = uuid::Uuid::new_v4().to_string(),
            1 => bad.request.peer_user_id += 1,
            2 => bad.request.context.as_mut().unwrap().protocol_version += 1,
            3 => bad.own.user_id = B.into(),
            4 => bad.own.generation_id = H.into(),
            _ => bad.store_scope = StoreScope::new("https://other.example.test", A).unwrap(),
        }
        assert!(
            e.accept_conversation(verified(&bad, result.clone()))
                .is_err()
        );
        assert!(index(&mut e).is_none());
    }
    let mut bad = result.clone();
    bad.own_device.as_mut().unwrap().generation_id = H.into();
    assert!(e.accept_conversation(verified(&a, bad)).is_err());
    assert_eq!(
        e.accept_conversation(verified(&a, result.clone())).unwrap(),
        result
    );
}

#[test]
fn invalid_closed_and_unknown_states_are_rejected_without_persistence() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    let a = e.prepare_conversation(PEER, None).unwrap();
    let valid = closed(response());
    for change in 0..10 {
        let mut bad = valid.clone();
        match change {
            0 => bad.state = 0,
            1 => bad.state = 99,
            2 => bad.close_reason = 0,
            3 => bad.closed_at_unix_ms = 0,
            4 => bad.closed_generation_id = uuid::Uuid::new_v4().to_string(),
            5 => bad.conversation_id = "not-a-uuid".into(),
            6 => bad.peer_device.as_mut().unwrap().registration_id = 16384,
            7 => bad.peer_device.as_mut().unwrap().identity_key[0] = 1,
            8 => {
                bad.peer_device
                    .as_mut()
                    .unwrap()
                    .reference
                    .as_mut()
                    .unwrap()
                    .user_id = A.into()
            }
            _ => {
                bad.peer_device
                    .as_mut()
                    .unwrap()
                    .reference
                    .as_mut()
                    .unwrap()
                    .device_id = 2
            }
        }
        assert!(e.accept_conversation(verified(&a, bad)).is_err());
        assert!(index(&mut e).is_none());
    }
    assert!(SessionScope::from_conversation(&valid).is_err());
    assert_eq!(
        e.accept_conversation(verified(&a, valid.clone())).unwrap(),
        valid
    );
}

#[test]
fn closure_is_monotonic_even_when_active_responses_arrive_late() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    let a = e.prepare_conversation(PEER, None).unwrap();
    let mut active = response();
    active
        .peer_device
        .as_mut()
        .unwrap()
        .public_material_revision = 5;
    e.accept_conversation(verified(&a, active.clone())).unwrap();
    let mut old = active.clone();
    old.peer_device.as_mut().unwrap().public_material_revision = 1;
    assert_eq!(e.accept_conversation(verified(&a, old)).unwrap(), active);
    let close = closed(active.clone());
    e.accept_conversation(verified(&a, close.clone())).unwrap();
    assert_eq!(e.accept_conversation(verified(&a, active)).unwrap(), close);
    let mut bad = close.clone();
    bad.closed_at_unix_ms += 1;
    assert!(e.accept_conversation(verified(&a, bad)).is_err());
    assert_eq!(index(&mut e).unwrap(), close);
}

#[test]
fn closed_first_reply_reuses_old_request_until_explicit_new_dialogue_cas() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    let a = e.prepare_conversation(PEER, None).unwrap();
    let id = a.request().creation_request_id.clone();
    assert!(e.prepare_conversation(PEER, Some(&id)).is_err());
    let close = closed(response());
    e.accept_conversation(verified(&a, close.clone())).unwrap();
    assert_eq!(
        e.prepare_conversation(PEER, None).unwrap().request(),
        a.request()
    );
    let archived = e
        .store
        .transaction(|tx| tx.get(&request_key(&own(), &id)?))
        .unwrap()
        .unwrap();
    assert!(e.prepare_conversation(PEER, Some(H)).is_err());
    let next = e.prepare_conversation(PEER, Some(&id)).unwrap();
    assert_ne!(next.request(), a.request());
    assert_eq!(
        e.prepare_conversation(PEER, Some(&id)).unwrap().request(),
        next.request()
    );
    assert!(matches!(
        e.accept_conversation(verified(&a, close.clone())),
        Err(CryptoError::Conflict)
    ));
    assert_eq!(
        e.store
            .transaction(|tx| tx.get(&request_key(&own(), &id)?))
            .unwrap()
            .unwrap()
            .as_slice(),
        archived.as_slice()
    );
    // A fresh request still cannot revive the closed ID, even from an active reply.
    let mut stale_active = close;
    stale_active.state = ConversationState::Active as i32;
    stale_active.close_reason = 0;
    stale_active.closed_at_unix_ms = 0;
    stale_active.closed_generation_id.clear();
    stale_active
        .peer_device
        .as_mut()
        .unwrap()
        .public_material_ready = true;
    assert_eq!(
        e.accept_conversation(verified(&next, stale_active))
            .unwrap()
            .state,
        ConversationState::Closed as i32
    );
}

#[test]
fn selected_closed_chat_cas_replays_one_successor_and_rejects_stale_consent() {
    let root = tempfile::tempdir().unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let (old, a, close) = runtime.block_on(async {
        let w = CryptoCoordinator::start(test_open(root.path(), scope(), true), own())
            .await
            .unwrap();
        let old = w.prepare_conversation_creation(PEER).await.unwrap();
        assert!(
            w.prepare_conversation_after_closed(PEER, C.into())
                .await
                .is_err()
        );
        let active = response();
        w.accept_conversation_creation(verified(&old, active.clone()))
            .await
            .unwrap();
        assert!(
            w.prepare_conversation_after_closed(PEER, C.into())
                .await
                .is_err()
        );
        let close = closed(active);
        w.accept_conversation_creation(verified(&old, close.clone()))
            .await
            .unwrap();
        for (peer, chat) in [(PEER + 1, C), (PEER, H), (PEER, "../invalid")] {
            assert!(
                w.prepare_conversation_after_closed(peer, chat.into())
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            w.prepare_conversation_creation(PEER)
                .await
                .unwrap()
                .request(),
            old.request()
        );
        let (a, b) = tokio::join!(
            w.prepare_conversation_after_closed(PEER, C.into()),
            w.prepare_conversation_after_closed(PEER, C.into())
        );
        let a = a.unwrap();
        assert_eq!(a.request(), b.unwrap().request());
        assert_ne!(a.request(), old.request());
        w.shutdown().await.unwrap();
        (old, a, close)
    });
    let mut e = engine(root.path(), false);
    assert_eq!(
        e.prepare_conversation_after_closed(PEER, C)
            .unwrap()
            .request(),
        a.request()
    );
    let mut next = response();
    next.conversation_id = uuid::Uuid::new_v4().to_string();
    e.accept_conversation(verified(&a, next.clone())).unwrap();
    assert_eq!(
        e.prepare_conversation_after_closed(PEER, C)
            .unwrap()
            .request(),
        a.request()
    );
    // A delayed receipt for the old attempt cannot move the advanced peer slot.
    assert!(matches!(
        e.accept_conversation(verified(&old, close.clone())),
        Err(CryptoError::Conflict)
    ));
    assert_eq!(index(&mut e).unwrap(), close);
    e.accept_conversation(verified(&a, closed(next.clone())))
        .unwrap();
    let third = e
        .prepare_conversation_after_closed(PEER, &next.conversation_id)
        .unwrap();
    assert_ne!(third.request(), a.request());
    assert!(matches!(
        e.prepare_conversation_after_closed(PEER, C),
        Err(CryptoError::Conflict)
    ));
    assert_eq!(
        e.prepare_conversation(PEER, None).unwrap().request(),
        third.request()
    );
    assert_eq!(index(&mut e).unwrap(), close);
}

#[test]
fn closed_successor_reply_does_not_advance_again_on_the_same_consent() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    let old = e.prepare_conversation(PEER, None).unwrap();
    let close = closed(response());
    e.accept_conversation(verified(&old, close.clone()))
        .unwrap();
    let next = e.prepare_conversation_after_closed(PEER, C).unwrap();
    e.accept_conversation(verified(&next, close.clone()))
        .unwrap();
    assert_eq!(
        e.prepare_conversation_after_closed(PEER, C)
            .unwrap()
            .request(),
        next.request()
    );
    assert_eq!(index(&mut e).unwrap(), close);
}

#[test]
fn corrupt_or_missing_journal_does_not_generate_a_new_request() {
    for missing in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut e = engine(root.path(), true);
        let a = e.prepare_conversation(PEER, None).unwrap();
        e.store
            .transaction(|tx| {
                if missing {
                    tx.put(&head_key(&own(), PEER)?, H.as_bytes())
                } else {
                    tx.put(
                        &request_key(&own(), &a.request().creation_request_id)?,
                        b"invalid",
                    )
                }
            })
            .unwrap();
        assert!(e.prepare_conversation(PEER, None).is_err());
        assert!(index(&mut e).is_none());
    }
}

fn fault(root: &Path, event: &str, enabled: bool) {
    let connection = rusqlite::Connection::open(scope().database_path(root)).unwrap();
    if enabled {
        connection.execute_batch(&format!("CREATE TABLE parent(id INTEGER PRIMARY KEY);
          CREATE TABLE child(id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
          CREATE TRIGGER fail_commit AFTER {event} ON protected_records BEGIN INSERT INTO child VALUES(1); END;")).unwrap();
    } else {
        connection
            .execute_batch("DROP TRIGGER fail_commit; DROP TABLE child; DROP TABLE parent;")
            .unwrap();
    }
}

#[test]
fn preparation_commit_failure_returns_no_attempt_and_no_half_head() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    fault(root.path(), "INSERT", true);
    assert!(matches!(
        e.prepare_conversation(PEER, None),
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    assert!(
        e.store
            .transaction_with(|tx| load_head(tx, &own(), PEER))
            .unwrap()
            .is_none()
    );
    fault(root.path(), "INSERT", false);
    assert!(e.prepare_conversation(PEER, None).is_ok());
}

#[test]
fn reply_commit_failure_rolls_back_both_journal_and_conversation_index() {
    let root = tempfile::tempdir().unwrap();
    let mut e = engine(root.path(), true);
    let a = e.prepare_conversation(PEER, None).unwrap();
    let result = response();
    fault(root.path(), "UPDATE", true);
    assert!(matches!(
        e.accept_conversation(verified(&a, result.clone())),
        Err(CryptoError::Storage(StoreError::Database(_)))
    ));
    assert!(index(&mut e).is_none());
    assert!(
        e.store
            .transaction_with(|tx| load_journal(tx, &own(), PEER, &a.request().creation_request_id))
            .unwrap()
            .response
            .is_none()
    );
    fault(root.path(), "UPDATE", false);
    drop(e);
    let mut e = engine(root.path(), false);
    assert_eq!(
        e.prepare_conversation(PEER, None).unwrap().request(),
        a.request()
    );
    assert_eq!(
        e.accept_conversation(verified(&a, result.clone())).unwrap(),
        result
    );
}

#[test]
fn process_exit_before_and_after_creation_commits_recovers_atomic_state() {
    for operation in ["prepare", "accept"] {
        for after in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let mut e = engine(root.path(), true);
            let result = response();
            std::fs::write(root.path().join("response.bin"), result.encode_to_vec()).unwrap();
            let request = (operation == "accept").then(|| {
                e.prepare_conversation(PEER, None)
                    .unwrap()
                    .request()
                    .clone()
            });
            drop(e);
            let child = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "encryption::signal_state::conversation_creation::tests::creation_crash_worker",
                    "--nocapture",
                ])
                .env("ZCHAT_CREATION_TEST_ROOT", root.path())
                .env("ZCHAT_CREATION_TEST_OP", operation)
                .env(
                    "ZCHAT_CREATION_TEST_AFTER",
                    if after { "yes" } else { "no" },
                )
                .output()
                .unwrap();
            assert_eq!(
                child.status.code(),
                Some(if after { 74 } else { 73 }),
                "{}",
                String::from_utf8_lossy(&child.stdout)
            );
            let mut e = engine(root.path(), false);
            let head = e
                .store
                .transaction_with(|tx| load_head(tx, &own(), PEER))
                .unwrap();
            if operation == "prepare" {
                assert_eq!(head.is_some(), after);
            } else {
                let a = e.prepare_conversation(PEER, None).unwrap();
                assert_eq!(Some(a.request()), request.as_ref());
                assert_eq!(index(&mut e).is_some(), after);
                assert_eq!(
                    e.accept_conversation(verified(&a, result.clone())).unwrap(),
                    result
                );
            }
        }
    }
}

#[test]
fn creation_crash_worker() {
    let Some(root) = std::env::var_os("ZCHAT_CREATION_TEST_ROOT") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let mut e = engine(&root, false);
    let operation = std::env::var("ZCHAT_CREATION_TEST_OP").unwrap();
    let after = std::env::var("ZCHAT_CREATION_TEST_AFTER").unwrap() == "yes";
    if operation == "prepare" {
        e.store
            .test_exit_on_record(head_key(&own(), PEER).unwrap(), after);
        e.prepare_conversation(PEER, None).unwrap();
    } else {
        let a = e.prepare_conversation(PEER, None).unwrap();
        let result =
            Conversation::decode(std::fs::read(root.join("response.bin")).unwrap().as_slice())
                .unwrap();
        e.store
            .test_exit_on_record(conversation_key(&own(), C).unwrap(), after);
        e.accept_conversation(verified(&a, result)).unwrap();
    }
    panic!("creation crash hook did not terminate child");
}
