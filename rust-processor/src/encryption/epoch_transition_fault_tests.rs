use super::*;

#[tokio::test]
async fn historical_transition_survives_key_metadata_refresh_and_later_peer_closure() {
    let (f, a, b) = prepared().await;
    let text = payload(&a, "accepted before peer closure");
    let old = f
        .alice
        .encrypt_epoch(a.clone(), text.clone())
        .await
        .unwrap();
    store_receipt(&f.alice, &a, &old).await;
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    prepare(&f.alice, &a, &r).await;
    let qb = prepare(&f.bob, &b, &r).await;
    let mut refreshed = qb.pins.conversation().clone();
    refreshed
        .peer_device
        .as_mut()
        .unwrap()
        .public_material_revision += 1;
    super::super::super::super::tests::index(&f.bob, &refreshed).await;
    // Resume with a fresh catalogue after publication metadata changes.
    let q = f
        .bob
        .prepare_epoch_transition(b.clone(), id.clone())
        .await
        .unwrap();
    f.bob
        .accept_epoch_transition_catalog(observation(&q, catalog(&q)))
        .await
        .unwrap();
    f.alice
        .advance_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    f.bob
        .advance_epoch_transition(b.clone(), id.clone())
        .await
        .unwrap();
    assert!(f.bob.epoch_scope(CHAT.into(), 0).await.unwrap().sealed());
    refreshed.state = ConversationState::Closed as i32;
    refreshed.close_reason = common::ConversationCloseReason::DeviceReplaced as i32;
    refreshed.closed_generation_id = b.peer.generation_id.clone();
    refreshed.closed_at_unix_ms = 1_700_000_000_200;
    super::super::super::super::tests::index(&f.bob, &refreshed).await;
    let archived = f.bob.epoch_scope(CHAT.into(), 0).await.unwrap();
    assert!(archived.sealed() && archived.receive_only());
    let (received, proof) = f
        .bob
        .receive_epoch(b.clone(), delivery(&b, &wire(&old, 1)))
        .await
        .unwrap();
    assert_eq!(received, text);
    f.bob
        .accept_epoch_ack(EpochObservation::test_observation(
            b.scope.clone(),
            proof,
            common::Empty {},
        ))
        .await
        .unwrap();
    assert!(
        f.bob
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    let new = f.bob.epoch_scope(CHAT.into(), 1).await.unwrap();
    assert!(matches!(
        f.bob
            .encrypt_epoch(new.clone(), payload(&new, "closed"))
            .await,
        Err(CryptoError::ConversationClosed)
    ));
    assert!(
        f.bob
            .advance_epoch_transition(b, id)
            .await
            .unwrap()
            .activated()
    );
    f.bob.shutdown().await.unwrap();
    let w = CryptoCoordinator::start(store(f.root.path(), B, false), new.own.clone())
        .await
        .unwrap();
    assert_eq!(
        w.epoch_incoming_message(text.message_id).await.unwrap(),
        received
    );
    w.shutdown().await.unwrap();
    f.alice.shutdown().await.unwrap();
}

#[tokio::test]
async fn catalogue_and_owner_validation_precede_writes_including_completed_retry() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let proposal = f.alice.prepare_epoch_recovery(a.clone()).await.unwrap();
    let id = proposal
        .proposal()
        .control
        .as_ref()
        .unwrap()
        .proposal_id
        .clone();
    assert!(
        f.alice
            .prepare_epoch_transition(a.clone(), id.clone())
            .await
            .is_err()
    );
    let r = accepted(&f, &a, &b).await;
    let q = f
        .alice
        .prepare_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    for complete in [false, true] {
        if complete {
            prepare(&f.alice, &a, &r).await;
            f.alice
                .advance_epoch_transition(a.clone(), id.clone())
                .await
                .unwrap();
        }
        let before = records(f.root.path(), A);
        for mutation in 0..8 {
            let mut page = catalog(&q);
            match mutation {
                0 => page.epochs[0].created_at_unix_ms += 1,
                1 => page.epochs[0].sealed_by_proposal_id = uuid::Uuid::new_v4().to_string(),
                2 => page.epochs[1].bootstrap_device = Some(a.peer.clone()),
                3 => page.epochs[1].created_by_proposal_id = uuid::Uuid::new_v4().to_string(),
                4 => page.epoch_revision = Some(2),
                5 => page.epochs[1].created_at_unix_ms += 1,
                6 => page.conversation.as_mut().unwrap().own_device = Some(a.peer.clone()),
                _ => page.next_after_epoch = Some(1),
            }
            assert!(
                f.alice
                    .accept_epoch_transition_catalog(observation(&q, page))
                    .await
                    .is_err(),
                "mutation {mutation} completed {complete}"
            );
        }
        assert!(
            f.bob
                .accept_epoch_transition_catalog(observation(&q, catalog(&q)))
                .await
                .is_err()
        );
        assert_eq!(records(f.root.path(), A), before);
    }
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn closure_or_preexisting_next_epoch_marker_cannot_be_overwritten() {
    for marker in [
        None,
        Some("traffic"),
        Some("quarantine-traffic"),
        Some("acquire"),
    ] {
        let f = fixture().await;
        let a = initialize(&f.alice).await;
        let b = initialize(&f.bob).await;
        let r = accepted(&f, &a, &b).await;
        let id = control(&r).unwrap().proposal_id.clone();
        prepare(&f.alice, &a, &r).await;
        if let Some(marker) = marker {
            f.alice.shutdown().await.unwrap();
            let mut local = store(f.root.path(), A, false);
            local
                .transaction(|tx| {
                    tx.put(
                        &key(&a.own, CHAT, &format!("{marker}:0000000000000001")).unwrap(),
                        b"retained orphan",
                    )
                })
                .unwrap();
            drop(local);
        } else {
            let mut c = f.a.clone();
            c.state = ConversationState::Closed as i32;
            c.close_reason = common::ConversationCloseReason::DeviceReplaced as i32;
            c.closed_generation_id = a.peer.generation_id.clone();
            c.closed_at_unix_ms = 1_700_000_000_200;
            super::super::super::super::tests::index(&f.alice, &c).await;
            f.alice.shutdown().await.unwrap();
        }
        let before = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
            .await
            .unwrap();
        assert!(w.advance_epoch_transition(a, id).await.is_err());
        assert_eq!(records(f.root.path(), A), before);
        assert!(w.epoch_scope(CHAT.into(), 1).await.is_err());
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn partial_chain_or_progress_replay_and_archive_deletion_fail_closed() {
    for mode in 0..6 {
        let (f, a, b) = prepared().await;
        let p = f
            .alice
            .encrypt_epoch(a.clone(), payload(&a, "never omit this UUID"))
            .await
            .unwrap();
        store_receipt(&f.alice, &a, &p).await;
        if mode == 5 {
            let tail = f
                .alice
                .encrypt_epoch(a.clone(), payload(&a, "retained tail"))
                .await
                .unwrap();
            store_receipt(&f.alice, &a, &tail).await;
        }
        let r = accepted(&f, &a, &b).await;
        let id = control(&r).unwrap().proposal_id.clone();
        let before = records(f.root.path(), A);
        prepare(&f.alice, &a, &r).await;
        if matches!(mode, 3 | 4) {
            f.alice
                .advance_epoch_transition(a.clone(), id.clone())
                .await
                .unwrap();
        }
        f.alice.shutdown().await.unwrap();
        let db = db(f.root.path(), A);
        let target = match mode {
            0 | 5 => format!("epoch-v2:{CHAT}:outgoing-chain:0000000000000000:0000000000000001"),
            1 | 2 => format!("epoch-v2:{CHAT}:recovery-head:0000000000000000"),
            3 => format!("epoch-v2:{CHAT}:row-anchor:0000000000000000"),
            _ => format!("epoch-v2:{CHAT}:row:0000000000000000"),
        };
        if mode == 2 {
            let old = &before.iter().find(|v| v.1 == target).unwrap().2;
            assert_eq!(
                db.execute(
                    "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
                    rusqlite::params![old, target]
                )
                .unwrap(),
                1
            );
        } else {
            assert_eq!(
                db.execute("DELETE FROM protected_records WHERE record_id=?1", [target])
                    .unwrap(),
                1
            );
        }
        let damaged = records(f.root.path(), A);
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
            .await
            .unwrap();
        assert!(w.advance_epoch_transition(a, id).await.is_err());
        // With a valid tail, only the explicit walk discovers the missing middle
        // link. It still rolls the whole step back and cannot activate epoch1.
        assert_eq!(w.epoch_scope(CHAT.into(), 0).await.is_ok(), mode == 5);
        assert_eq!(records(f.root.path(), A), damaged);
        w.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn sealed_fence_commit_failure_and_journal_replay_preserve_unknown_outcome() {
    let (f, a, b) = prepared().await;
    let p = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "unknown send"))
        .await
        .unwrap();
    let r = accepted(&f, &a, &b).await;
    let failure = sealed(&p, &r);
    let old = records(f.root.path(), A);
    let db = db(f.root.path(), A);
    db.execute_batch("CREATE TABLE fence_parent(id INTEGER PRIMARY KEY);CREATE TABLE fence_child(id INTEGER REFERENCES fence_parent(id) DEFERRABLE INITIALLY DEFERRED);CREATE TRIGGER fence_commit_fault AFTER INSERT ON protected_records WHEN NEW.record_id LIKE 'delivery-sealed:%' BEGIN INSERT INTO fence_child VALUES(1);END;").unwrap();
    assert!(
        f.alice
            .accept_epoch_send_fence(fence(&a, &p, &failure))
            .await
            .is_err()
    );
    assert_eq!(records(f.root.path(), A), old);
    let uuid = p.request().envelope.as_ref().unwrap().message_id.clone();
    assert!(
        f.alice
            .epoch_outgoing_plan(uuid.clone())
            .await
            .unwrap()
            .needs_send()
    );
    db.execute_batch("DROP TRIGGER fence_commit_fault").unwrap();
    f.alice
        .accept_epoch_send_fence(fence(&a, &p, &failure))
        .await
        .unwrap();
    let id = format!("delivery-out:{}", outgoing::record_id(&a, &uuid).unwrap());
    let bytes = &old.iter().find(|x| x.1 == id).unwrap().2;
    db.execute(
        "UPDATE protected_records SET payload=?1 WHERE record_id=?2",
        rusqlite::params![bytes, id],
    )
    .unwrap();
    let damaged = records(f.root.path(), A);
    assert!(f.alice.epoch_outgoing_plan(uuid).await.is_err());
    assert_eq!(records(f.root.path(), A), damaged);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[test]
fn activation_crash_child() {
    let Ok(root) = std::env::var("ZCHAT_TRANSITION_CRASH_PATH") else {
        return;
    };
    let after = std::env::var("ZCHAT_TRANSITION_CRASH_AFTER").unwrap() == "1";
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            let root = std::path::Path::new(&root);
            let own = DeviceRef {
                user_id: A.into(),
                device_id: 1,
                generation_id: "33333333-3333-4333-8333-333333333333".into(),
            };
            let mut local = store(root, A, false);
            local.test_exit_on_record(key(&own, CHAT, "row:0000000000000001").unwrap(), after);
            let w = CryptoCoordinator::start(local, own).await.unwrap();
            let s = w.epoch_scope(CHAT.into(), 0).await.unwrap();
            let id = std::fs::read_to_string(root.join("public-proposal-id.txt")).unwrap();
            let _ = w.advance_epoch_transition(s, id).await;
            panic!("activation crash hook did not fire");
        });
}
#[tokio::test]
async fn process_exit_before_after_activation_commit_is_atomic_and_retryable() {
    for after in [false, true] {
        let f = fixture().await;
        let a = initialize(&f.alice).await;
        let b = initialize(&f.bob).await;
        let r = accepted(&f, &a, &b).await;
        let id = control(&r).unwrap().proposal_id.clone();
        prepare(&f.alice, &a, &r).await;
        std::fs::write(f.root.path().join("public-proposal-id.txt"), &id).unwrap();
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
        let before = records(f.root.path(), A);
        let status=std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact","encryption::signal_state::epoch_state::recovery::transition::tests::faults::activation_crash_child","--nocapture"])
            .env("ZCHAT_TRANSITION_CRASH_PATH",f.root.path()).env("ZCHAT_TRANSITION_CRASH_AFTER",if after {"1"}else{"0"}).status().unwrap();
        assert_eq!(status.code(), Some(if after { 74 } else { 73 }));
        let w = CryptoCoordinator::start(store(f.root.path(), A, false), a.own.clone())
            .await
            .unwrap();
        assert_eq!(w.epoch_scope(CHAT.into(), 0).await.unwrap().sealed(), after);
        assert_eq!(w.epoch_scope(CHAT.into(), 1).await.is_ok(), after);
        if !after {
            assert_eq!(records(f.root.path(), A), before);
        }
        let (one, two) = tokio::join!(
            w.advance_epoch_transition(a.clone(), id.clone()),
            w.advance_epoch_transition(a, id)
        );
        assert!(one.unwrap().activated() && two.unwrap().activated());
        w.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn second_epoch_with_reverse_proposer_keeps_old_scopes_and_retires_competing_intent() {
    let f = fixture().await;
    let a = initialize(&f.alice).await;
    let b = initialize(&f.bob).await;
    let competing = f.bob.prepare_epoch_recovery(b.clone()).await.unwrap();
    let old_pending = response(&competing, None, common::RecoveryState::Pending);
    let r = accepted(&f, &a, &b).await;
    let id = control(&r).unwrap().proposal_id.clone();
    let q = prepare(&f.alice, &a, &r).await;
    let first = catalog(&q);
    prepare(&f.bob, &b, &r).await;
    f.alice
        .advance_epoch_transition(a.clone(), id.clone())
        .await
        .unwrap();
    f.bob
        .advance_epoch_transition(b.clone(), id.clone())
        .await
        .unwrap();
    let before = records(f.root.path(), B);
    let old = f
        .bob
        .accept_epoch_recovery_result(observed(&competing, &old_pending))
        .await
        .unwrap();
    assert_eq!(old.retired_by(), Some(id.as_str()));
    assert_eq!(records(f.root.path(), B), before);
    assert!(f.bob.prepare_epoch_recovery(b.clone()).await.is_err());
    let a1 = f.alice.epoch_scope(CHAT.into(), 1).await.unwrap();
    let b1 = f.bob.epoch_scope(CHAT.into(), 1).await.unwrap();
    let second = accept_between(&f.bob, &b1, &f.alice, &a1).await;
    let c = control(&second).unwrap();
    for (w, s) in [(&f.alice, &a1), (&f.bob, &b1)] {
        let q = w
            .prepare_epoch_transition(s.clone(), c.proposal_id.clone())
            .await
            .unwrap();
        let mut page = first.clone();
        page.conversation = Some(q.pins.conversation().clone());
        page.active_epoch = Some(2);
        page.epoch_revision = Some(2);
        let mut old = first.epochs[1].clone();
        old.sealed = true;
        old.sealed_by_proposal_id = c.proposal_id.clone();
        old.sealed_at_unix_ms = second.resolved_at_unix_ms;
        page.epochs = vec![
            old,
            v2::EpochRecord {
                epoch: Some(2),
                revision: Some(2),
                created_by_proposal_id: c.proposal_id.clone(),
                created_at_unix_ms: second.resolved_at_unix_ms.unwrap(),
                bootstrap_device: Some(b.own.clone()),
                ..Default::default()
            },
        ];
        w.accept_epoch_transition_catalog(observation(&q, page))
            .await
            .unwrap();
        assert!(
            w.advance_epoch_transition(s.clone(), c.proposal_id.clone())
                .await
                .unwrap()
                .activated()
        );
        assert!(w.epoch_scope(CHAT.into(), 0).await.unwrap().sealed());
        assert!(w.epoch_scope(CHAT.into(), 1).await.unwrap().sealed());
    }
    assert!(
        f.alice
            .prepare_epoch_acquisition(f.alice.epoch_scope(CHAT.into(), 2).await.unwrap())
            .await
            .is_err()
    );
    assert!(
        f.bob
            .prepare_epoch_acquisition(f.bob.epoch_scope(CHAT.into(), 2).await.unwrap())
            .await
            .is_ok()
    );
    let b2 = f.bob.epoch_scope(CHAT.into(), 2).await.unwrap();
    let a2 = f.alice.epoch_scope(CHAT.into(), 2).await.unwrap();
    bootstrap(&f.bob, &b2, &f.reverse_bundle).await;
    let plain = payload(&b2, "Bob is the second recovery proposer");
    let sent = f
        .bob
        .encrypt_epoch(b2.clone(), plain.clone())
        .await
        .unwrap();
    let (got, _) = f
        .alice
        .receive_epoch(
            a2,
            delivery(
                &f.alice.epoch_scope(CHAT.into(), 2).await.unwrap(),
                &wire(&sent, 1),
            ),
        )
        .await
        .unwrap();
    assert_eq!(got, plain);
    // Old completed retries cannot roll the active root back from2 to1.
    let before = records(f.root.path(), A);
    assert!(
        f.alice
            .advance_epoch_transition(a, id)
            .await
            .unwrap()
            .activated()
    );
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
