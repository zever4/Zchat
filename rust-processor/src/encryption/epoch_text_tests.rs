use super::super::super::tests::store;
use super::super::{
    outgoing::tests::prepared,
    tests::{fixture, index, records, rewrite_frozen},
};
use super::*;
use uuid::Uuid;
const CHAT: &str = "66666666-6666-4666-8666-666666666666";
const A: &str = "11111111-1111-4111-8111-111111111111";
async fn send(w: &CryptoCoordinator, id: &str, text: &str) -> CryptoResult<EpochTextPreparation> {
    w.prepare_epoch_text(CHAT.into(), id.into(), Zeroizing::new(text.into()))
        .await
}
fn accepted(result: EpochTextPreparation) -> EpochTextQueued {
    match result {
        EpochTextPreparation::Queued(v) => v,
        _ => panic!("text was not committed"),
    }
}
#[tokio::test]
async fn explicit_text_without_session_is_read_only_and_utf8_limit_is_bytes() {
    let f = fixture().await;
    while !f.alice.seed_recovery_indexes().await.unwrap().ready {}
    let id = Uuid::new_v4().to_string();
    let before = records(f.root.path(), A);
    assert!(matches!(
        send(&f.alice, &id, "first").await.unwrap(),
        EpochTextPreparation::BootstrapRequired
    ));
    for invalid in [
        String::new(),
        "a".repeat(2001),
        "я".repeat(1001),
        "🦀".repeat(501),
    ] {
        assert!(matches!(
            send(&f.alice, &id, &invalid).await,
            Err(CryptoError::Invalid)
        ));
    }
    assert!(matches!(
        send(&f.alice, "not-uuid", "x").await,
        Err(CryptoError::Invalid)
    ));
    assert!(records(f.root.path(), A) == before);
    assert!(
        f.alice
            .active_epoch_scope(CHAT.into())
            .await
            .unwrap()
            .is_none()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn text_retry_reopen_and_frozen_closed_replay_preserve_original_cipher_and_time() {
    let (f, s) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let text = "я".repeat(1000);
    let first = accepted(send(&f.alice, &id, &text).await.unwrap());
    assert!(!first.stored_on_server && !first.sealed_unsent);
    let cipher = f
        .alice
        .epoch_outgoing_plan(id.clone())
        .await
        .unwrap()
        .request()
        .clone();
    let after = records(f.root.path(), A);
    assert_eq!(accepted(send(&f.alice, &id, &text).await.unwrap()), first);
    assert!(matches!(
        send(&f.alice, &id, "different").await,
        Err(CryptoError::Conflict)
    ));
    let wrong_chat = Uuid::new_v4().to_string();
    assert!(matches!(
        f.alice
            .prepare_epoch_text(wrong_chat, id.clone(), Zeroizing::new(text.clone()))
            .await,
        Err(CryptoError::Conflict)
    ));
    assert_eq!(records(f.root.path(), A), after);
    f.alice.shutdown().await.unwrap();
    rewrite_frozen(f.root.path(), &f.a, true);
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert_eq!(accepted(send(&w, &id, &text).await.unwrap()), first);
    assert!(matches!(
        send(&w, &Uuid::new_v4().to_string(), "new").await,
        Err(CryptoError::EpochSendingFrozen)
    ));
    let mut closed = f.a.clone();
    closed.state = ConversationState::Closed as i32;
    closed.close_reason = crate::proto::e2ee::ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_at_unix_ms = 1_700_000_000_000;
    closed.closed_generation_id = s.peer.generation_id.clone();
    index(&w, &closed).await;
    assert_eq!(accepted(send(&w, &id, &text).await.unwrap()), first);
    assert!(matches!(
        send(&w, &Uuid::new_v4().to_string(), "new").await,
        Err(CryptoError::ConversationClosed)
    ));
    w.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), A, false), s.own.clone())
        .await
        .unwrap();
    assert_eq!(accepted(send(&w, &id, &text).await.unwrap()), first);
    assert_eq!(w.epoch_outgoing_plan(id).await.unwrap().request(), &cipher);
    w.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn failed_text_commit_rolls_back_ratchet_uuid_and_history_then_retry_succeeds() {
    let (f, _) = prepared().await;
    let id = Uuid::new_v4().to_string();
    let before = records(f.root.path(), A);
    let db = super::super::outgoing::tests::db(f.root.path());
    db.execute_batch("CREATE TABLE txt_parent(id INTEGER PRIMARY KEY);CREATE TABLE txt_child(id INTEGER REFERENCES txt_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER txt_fault AFTER INSERT ON protected_records WHEN NEW.kind=11 BEGIN INSERT INTO txt_child VALUES(1);END;").unwrap();
    assert!(matches!(
        send(&f.alice, &id, "must commit all").await,
        Err(CryptoError::Storage(_))
    ));
    assert_eq!(records(f.root.path(), A), before);
    assert!(matches!(
        f.alice.epoch_outgoing_plan(id.clone()).await,
        Err(CryptoError::MessageUnavailable)
    ));
    db.execute_batch("DROP TRIGGER txt_fault").unwrap();
    assert_eq!(
        accepted(send(&f.alice, &id, "must commit all").await.unwrap()).message_id,
        id
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
