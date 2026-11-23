use super::*;
const CHAT: &str = "11111111-1111-4111-8111-111111111111";
const PID: &str = "22222222-2222-4222-8222-222222222222";
fn proposal() -> serde_json::Value {
    serde_json::json!({"action":"propose","conversationId":CHAT,"expectedEpoch":"0","expectedOwnProposalId":null,"confirmed":true})
}
#[test]
fn explicit_consent_and_all_cas_fields_are_required_and_canonical() {
    assert!(
        serde_json::from_value::<RecoveryRequest>(proposal())
            .unwrap()
            .validate()
            .is_ok()
    );
    for field in ["confirmed", "expectedEpoch", "expectedOwnProposalId"] {
        let mut value = proposal();
        value.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<RecoveryRequest>(value).is_err(),
            "missing {field}"
        );
    }
    for (field, bad) in [
        ("confirmed", serde_json::json!(false)),
        ("expectedEpoch", serde_json::json!("01")),
        ("expectedEpoch", serde_json::json!("9223372036854775808")),
        ("expectedEpoch", serde_json::json!("-1")),
        ("expectedOwnProposalId", serde_json::json!("../scope")),
        ("conversationId", serde_json::json!("1")),
    ] {
        let mut value = proposal();
        value[field] = bad;
        assert!(
            serde_json::from_value::<RecoveryRequest>(value)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    for number in ["0", "9007199254740993", "9223372036854775807"] {
        let mut value = proposal();
        value["expectedEpoch"] = number.into();
        assert!(
            serde_json::from_value::<RecoveryRequest>(value)
                .unwrap()
                .validate()
                .is_ok()
        );
    }
    let mut value = proposal();
    value["expectedEpoch"] = serde_json::json!(0);
    assert!(serde_json::from_value::<RecoveryRequest>(value).is_err());
}
#[test]
fn requests_reject_scope_keys_proofs_and_implicit_actions() {
    for extra in [
        "scope",
        "generation",
        "signature",
        "requestId",
        "identityKey",
        "server",
        "fromEpoch",
    ] {
        let mut value = proposal();
        value[extra] = "forged".into();
        assert!(
            serde_json::from_value::<RecoveryRequest>(value).is_err(),
            "extra {extra}"
        );
    }
    for decision in ["accept", "decline", "cancel"] {
        let value = serde_json::json!({"action":"decide","conversationId":CHAT,"expectedEpoch":"0","proposalId":PID,"decision":decision,"confirmed":true});
        assert!(
            serde_json::from_value::<RecoveryRequest>(value.clone())
                .unwrap()
                .validate()
                .is_ok()
        );
        let mut bad = value;
        bad["confirmed"] = false.into();
        assert!(
            serde_json::from_value::<RecoveryRequest>(bad)
                .unwrap()
                .validate()
                .is_err()
        );
    }
    assert!(
        serde_json::from_value::<RecoveryRequest>(
            serde_json::json!({"action":"reset","conversationId":CHAT})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<RecoveryRequest>(
            serde_json::json!({"action":"get","conversationId":CHAT,"confirmed":true})
        )
        .is_err()
    );
}
#[cfg(feature = "experimental-e2ee")]
#[test]
fn reply_is_small_public_metadata_and_local_acceptance_has_no_completion_claim() {
    let read = RecoveryReply::State {
        conversation_id: CHAT.into(),
        crypto_epoch: Some("9223372036854775807".into()),
        receive_only: false,
        sending_frozen: true,
        proposals: vec![Proposal {
            proposal_id: PID.into(),
            state: Some("pending".into()),
            own_proposal: true,
            decision: Some(Decision::Cancel),
            sending_frozen: true,
        }],
    };
    let value = serde_json::to_value(Reply {
        context: 2,
        recovery: read,
    })
    .unwrap();
    assert_eq!(value["recovery"].as_object().unwrap().len(), 6);
    assert_eq!(
        value["recovery"]["proposals"][0].as_object().unwrap().len(),
        5
    );
    assert!(serde_json::to_vec(&value).unwrap().len() < 2048);
    let queued = serde_json::to_value(RecoveryReply::Queued {
        conversation_id: CHAT.into(),
        crypto_epoch: "0".into(),
        operation_id: PID.into(),
    })
    .unwrap();
    assert_eq!(queued.as_object().unwrap().len(), 4);
    assert!(queued.get("activated").is_none() && queued.get("acknowledged").is_none());
}
