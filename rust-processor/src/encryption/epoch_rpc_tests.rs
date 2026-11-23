use super::*;
use crate::proto::{e2ee as common, e2ee_v2 as wire};

pub(crate) fn signed_record() -> wire::RecoveryRecord {
    wire::RecoveryRecord {
        outcome: Some(
            common::RecoveryOutcome::decode(
                include_bytes!("../../../proto/fixtures/epoch-recovery-v1/accept.bin").as_slice(),
            )
            .unwrap(),
        ),
        created_at_unix_ms: 1000,
        expires_at_unix_ms: 2000,
        resolved_at_unix_ms: Some(1500),
    }
}
pub(crate) fn pins() -> EpochConversation {
    let r = signed_record();
    let p = r.outcome.unwrap().proposal.unwrap().control.unwrap();
    let own = IdentityKey::try_from(
        include_bytes!("../../../proto/fixtures/epoch-recovery-v1/initiator.pub").as_slice(),
    )
    .unwrap();
    let peer = IdentityKey::try_from(
        include_bytes!("../../../proto/fixtures/epoch-recovery-v1/peer.pub").as_slice(),
    )
    .unwrap();
    EpochConversation::new(
        common::Conversation {
            conversation_id: p.conversation_id,
            mode: 1,
            own_device: p.initiator.unwrap().device,
            peer_device: Some(common::Device {
                reference: p.peer.unwrap().device,
                registration_id: 16383,
                identity_key: peer.serialize().to_vec(),
                public_material_ready: true,
                public_material_revision: 1,
            }),
            state: common::ConversationState::Active as i32,
            ..Default::default()
        },
        own,
        peer,
    )
    .unwrap()
}
pub(crate) fn send() -> wire::SendMessageRequest {
    let p = pins();
    let mut e = wire::Envelope::decode(
        include_bytes!("../../../proto/fixtures/epoch-data-v2/envelope.bin").as_slice(),
    )
    .unwrap();
    e.conversation_id = p.conversation.conversation_id.clone();
    e.sender = Some(p.own().clone());
    e.recipient = Some(p.peer().clone());
    wire::SendMessageRequest {
        context: Some(common::ClientContext {
            protocol_version: 2,
            device_generation_id: p.own().generation_id.clone(),
        }),
        envelope: Some(e),
    }
}
pub(crate) fn sealed() -> wire::EpochSealedFailure {
    let r = send();
    let o = signed_record().outcome.unwrap();
    let p = o.proposal.as_ref().unwrap().control.as_ref().unwrap();
    wire::EpochSealedFailure {
        fence: Some(common::EpochSealedFence {
            binding: Some(data::send_binding(r.envelope.as_ref().unwrap()).unwrap()),
            proposal_digest: control::control_digest(p).unwrap().to_vec(),
            next_epoch: o.new_epoch,
            epoch_revision: o.epoch_revision,
        }),
        recovery: Some(o),
    }
}
fn receipt(r: &wire::SendMessageRequest) -> wire::SendReceipt {
    let e = r.envelope.as_ref().unwrap();
    wire::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1000,
        expires_at_unix_ms: 1000 + contract::MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    }
}

#[test]
fn pending_keeps_old_and_new_epochs_but_rejects_corruption_and_wrong_recipient() {
    let r = send();
    let e = r.envelope.unwrap();
    let own = e.recipient.as_ref().unwrap();
    let row = wire::Delivery {
        sequence: 1,
        envelope: Some(e.clone()),
        expires_at_unix_ms: 2000,
        envelope_digest: data::envelope_digest(&e).unwrap().to_vec(),
    };
    let mut page = wire::PendingMessages {
        messages: vec![row.clone()],
    };
    validate_pending(&page, own).unwrap();
    let mut new = row.clone();
    new.sequence = 2;
    let changed = new.envelope.as_mut().unwrap();
    changed.crypto_epoch = Some(changed.crypto_epoch.unwrap() + 1);
    changed.message_id = uuid::Uuid::new_v4().to_string();
    new.envelope_digest = data::envelope_digest(changed).unwrap().to_vec();
    page.messages.push(new);
    validate_pending(&page, own).unwrap();
    for change in 0..7 {
        let mut bad = page.clone();
        match change {
            0 => bad.messages[1].sequence = 1,
            1 => bad.messages[0].envelope_digest[0] ^= 1,
            2 => bad.messages[0].envelope.as_mut().unwrap().crypto_epoch = None,
            3 => bad.messages[0].envelope.as_mut().unwrap().protocol_version = 1,
            4 => bad.messages[0].expires_at_unix_ms = 0,
            5 => bad.messages[0].envelope = None,
            _ => bad.messages = vec![row.clone(); 33],
        };
        assert!(validate_pending(&bad, own).is_err(), "case{change}");
    }
    assert!(validate_pending(&page, e.sender.as_ref().unwrap()).is_err());
    let mut huge = row;
    huge.envelope.as_mut().unwrap().ciphertext = vec![1; MAX_RESPONSE_BYTES];
    assert!(
        validate_pending(
            &wire::PendingMessages {
                messages: vec![huge]
            },
            own
        )
        .is_err()
    );
}

#[test]
fn statuses_bind_retained_epoch_not_only_uuid_and_digest_and_allow_late_ack() {
    let r = send();
    let good = wire::MessageStatuses {
        messages: vec![wire::MessageStatus {
            receipt: Some(receipt(&r)),
            state: common::MessageState::Pending as i32,
            observed_at_unix_ms: 1000,
        }],
    };
    validate_statuses(std::slice::from_ref(&r), &good).unwrap();
    for change in 0..9 {
        let mut bad = good.clone();
        let m = &mut bad.messages[0];
        let s = m.receipt.as_mut().unwrap();
        match change {
            0 => s.crypto_epoch = None,
            1 => s.crypto_epoch = Some(s.crypto_epoch.unwrap() + 1),
            2 => s.envelope_digest[0] ^= 1,
            3 => s.expires_at_unix_ms += 1,
            4 => m.state = 999,
            5 => m.observed_at_unix_ms = 999,
            6 => m.observed_at_unix_ms = 1001,
            7 => m.state = common::MessageState::Expired as i32,
            _ => s.stored_at_unix_ms = i64::MAX,
        };
        assert!(
            validate_statuses(std::slice::from_ref(&r), &bad).is_err(),
            "case{change}"
        );
    }
    for state in [
        common::MessageState::Expired,
        common::MessageState::RecipientStored,
        common::MessageState::RecipientRejected,
        common::MessageState::RecipientReplaced,
    ] {
        let mut late = good.clone();
        late.messages[0].state = state as i32;
        late.messages[0].observed_at_unix_ms = good.messages[0]
            .receipt
            .as_ref()
            .unwrap()
            .expires_at_unix_ms
            + 1;
        validate_statuses(std::slice::from_ref(&r), &late).unwrap();
    }
    let mut other = r.clone();
    other.envelope.as_mut().unwrap().message_id = uuid::Uuid::new_v4().to_string();
    let mut pair = good.clone();
    let mut second = good.messages[0].clone();
    second.receipt = Some(receipt(&other));
    pair.messages.push(second);
    validate_statuses(&[r.clone(), other.clone()], &pair).unwrap();
    pair.messages.swap(0, 1);
    assert!(validate_statuses(&[r.clone(), other], &pair).is_err());
    assert!(status_request(r.context.clone().unwrap(), &[r.clone(), r.clone()]).is_err());
    assert!(status_request(r.context.clone().unwrap(), &vec![r.clone(); 101]).is_err());
    let mut old = r;
    old.context.as_mut().unwrap().protocol_version = 1;
    assert!(validate_statuses(&[old], &good).is_err());
}

#[test]
fn recovery_signatures_roles_and_exact_proposals_are_verified_without_creating_scopes() {
    let pins = pins();
    let good = signed_record();
    let p = good.outcome.as_ref().unwrap().proposal.as_ref().unwrap();
    let decision = good.outcome.as_ref().unwrap().decision.as_ref().unwrap();
    pins.verify_record(&good, Some(p)).unwrap();
    pins.verify_command(p, p, true).unwrap();
    assert!(pins.verify_command(p, decision, false).is_err());
    let mut peer = pins.conversation.clone();
    peer.own_device = Some(pins.peer().clone());
    peer.peer_device = Some(common::Device {
        reference: Some(pins.own().clone()),
        registration_id: 16383,
        identity_key: pins.own_pin.serialize().to_vec(),
        public_material_ready: true,
        public_material_revision: 1,
    });
    let peer = EpochConversation::new(peer, pins.peer_pin, pins.own_pin).unwrap();
    peer.verify_command(p, decision, false).unwrap();
    for change in 0..7 {
        let mut bad = good.clone();
        match change {
            0 => {
                bad.outcome
                    .as_mut()
                    .unwrap()
                    .proposal
                    .as_mut()
                    .unwrap()
                    .signature[0] ^= 1
            }
            1 => {
                bad.outcome
                    .as_mut()
                    .unwrap()
                    .decision
                    .as_mut()
                    .unwrap()
                    .signature[0] ^= 1
            }
            2 => bad.resolved_at_unix_ms = None,
            3 => bad.resolved_at_unix_ms = Some(999),
            4 => bad.expires_at_unix_ms = 1000,
            5 => bad.outcome.as_mut().unwrap().new_epoch += 1,
            _ => {
                bad.outcome
                    .as_mut()
                    .unwrap()
                    .proposal
                    .as_mut()
                    .unwrap()
                    .control
                    .as_mut()
                    .unwrap()
                    .peer
                    .as_mut()
                    .unwrap()
                    .identity_fingerprint[0] ^= 1
            }
        };
        assert!(pins.verify_record(&bad, None).is_err(), "case{change}");
    }
    let mut mismatch = p.clone();
    mismatch.signature[0] ^= 1;
    assert!(pins.verify_record(&good, Some(&mismatch)).is_err());
    let mut pending = good;
    let o = pending.outcome.as_mut().unwrap();
    o.decision = None;
    o.state = common::RecoveryState::Pending as i32;
    o.new_epoch = 0;
    o.epoch_revision = 0;
    pending.resolved_at_unix_ms = None;
    pins.verify_record(&pending, None).unwrap();
    pending.outcome.as_mut().unwrap().state = common::RecoveryState::Expired as i32;
    pending.resolved_at_unix_ms = Some(1999);
    assert!(pins.verify_record(&pending, None).is_err());
    pending.resolved_at_unix_ms = Some(2000);
    pins.verify_record(&pending, None).unwrap();
}

#[test]
fn sealed_send_requires_both_signatures_and_exact_uuid_ciphertext_epoch_and_routing() {
    let p = pins();
    let r = send();
    let good = sealed();
    p.verify_sealed(&r, &good).unwrap();
    for change in 0..7 {
        let mut bad = good.clone();
        match change {
            0 => {
                bad.recovery
                    .as_mut()
                    .unwrap()
                    .decision
                    .as_mut()
                    .unwrap()
                    .signature[0] ^= 1
            }
            1 => {
                bad.recovery
                    .as_mut()
                    .unwrap()
                    .proposal
                    .as_mut()
                    .unwrap()
                    .signature[0] ^= 1
            }
            2 => {
                bad.fence
                    .as_mut()
                    .unwrap()
                    .binding
                    .as_mut()
                    .unwrap()
                    .message_id = uuid::Uuid::new_v4().to_string()
            }
            3 => bad.fence.as_mut().unwrap().binding.as_mut().unwrap().epoch += 1,
            4 => bad.fence.as_mut().unwrap().proposal_digest[0] ^= 1,
            5 => bad.fence.as_mut().unwrap().next_epoch += 1,
            _ => bad.recovery = None,
        };
        assert!(p.verify_sealed(&r, &bad).is_err(), "case{change}");
    }
    let mut changed = r;
    changed.envelope.as_mut().unwrap().ciphertext.push(7);
    assert!(p.verify_sealed(&changed, &good).is_err());
}

fn catalog_page(p: &EpochConversation) -> wire::ConversationEpochs {
    let id = uuid::Uuid::new_v4().to_string();
    wire::ConversationEpochs {
        protocol_version: 2,
        conversation: Some(p.conversation.clone()),
        active_epoch: Some(1),
        epoch_revision: Some(1),
        epochs: vec![
            wire::EpochRecord {
                epoch: Some(0),
                revision: Some(0),
                sealed: true,
                sealed_by_proposal_id: id.clone(),
                created_at_unix_ms: 1000,
                sealed_at_unix_ms: Some(2000),
                ..Default::default()
            },
            wire::EpochRecord {
                epoch: Some(1),
                revision: Some(1),
                created_by_proposal_id: id,
                bootstrap_device: Some(p.own().clone()),
                created_at_unix_ms: 2000,
                ..Default::default()
            },
        ],
        next_after_epoch: None,
    }
}
#[test]
fn catalog_checks_presence_paging_head_links_and_closed_conversation_monotonicity() {
    let pins = pins();
    let request = wire::GetConversationEpochsRequest {
        context: Some(send().context.unwrap()),
        conversation_id: pins.conversation.conversation_id.clone(),
        after_epoch: None,
        limit: 32,
    };
    let good = catalog_page(&pins);
    validate_catalog(&request, &good, &pins).unwrap();
    for change in 0..12 {
        let mut bad = good.clone();
        match change {
            0 => bad.active_epoch = None,
            1 => bad.epoch_revision = None,
            2 => bad.epochs[0].epoch = None,
            3 => bad.epochs[0].revision = Some(1),
            4 => bad.epochs[1].created_by_proposal_id = uuid::Uuid::new_v4().to_string(),
            5 => bad.epochs[0].sealed = false,
            6 => bad.epochs[1].sealed = true,
            7 => bad.epochs[1].bootstrap_device = None,
            8 => bad.epochs[1].created_at_unix_ms += 1,
            9 => bad.next_after_epoch = Some(1),
            10 => {
                bad.epochs.remove(0);
            }
            _ => {
                bad.conversation
                    .as_mut()
                    .unwrap()
                    .peer_device
                    .as_mut()
                    .unwrap()
                    .identity_key[1] ^= 1
            }
        };
        assert!(
            validate_catalog(&request, &bad, &pins).is_err(),
            "case{change}"
        );
    }
    let mut first = request.clone();
    first.limit = 1;
    let mut page = good.clone();
    page.epochs.truncate(1);
    page.next_after_epoch = Some(0);
    validate_catalog(&first, &page, &pins).unwrap();
    first.after_epoch = Some(0);
    page.epochs = vec![good.epochs[1].clone()];
    page.next_after_epoch = None;
    validate_catalog(&first, &page, &pins).unwrap();
    first.after_epoch = Some(1);
    page.epochs.clear();
    validate_catalog(&first, &page, &pins).unwrap();
    first.after_epoch = Some(u64::MAX);
    assert!(validate_catalog(&first, &page, &pins).is_err());
    let mut closed = pins.conversation.clone();
    closed.state = common::ConversationState::Closed as i32;
    closed.close_reason = common::ConversationCloseReason::DeviceReplaced as i32;
    closed.closed_generation_id = pins.peer().generation_id.clone();
    closed.closed_at_unix_ms = 3000;
    let retained = EpochConversation::new(closed.clone(), pins.own_pin, pins.peer_pin).unwrap();
    assert!(validate_catalog(&request, &good, &retained).is_err());
    let mut archived = good;
    archived.conversation = Some(closed);
    validate_catalog(&request, &archived, &retained).unwrap();
}

#[test]
fn recovery_discovery_is_bounded_sorted_advisory_and_empty_pages_remain_valid() {
    let req = wire::GetPendingEpochRecoveriesRequest {
        context: send().context,
        after_conversation_id: String::new(),
        limit: 1,
    };
    let mut page = wire::PendingEpochRecoveries::default();
    validate_recovery_page(&req, &page).unwrap();
    let id = "11111111-1111-4111-8111-111111111111".to_string();
    page.proposals.push(wire::PendingRecoveryReference {
        conversation_id: id.clone(),
        proposal_id: uuid::Uuid::new_v4().to_string(),
        expected_epoch: 0,
        expected_revision: 0,
        initiated_by_own_device: false,
        created_at_unix_ms: 1000,
        expires_at_unix_ms: 2000,
    });
    page.next_conversation_id = id;
    validate_recovery_page(&req, &page).unwrap();
    for change in 0..6 {
        let mut bad = page.clone();
        match change {
            0 => bad.proposals.push(bad.proposals[0].clone()),
            1 => bad.next_conversation_id = uuid::Uuid::new_v4().to_string(),
            2 => bad.proposals[0].proposal_id.clear(),
            3 => bad.proposals[0].expected_epoch = u64::MAX,
            4 => bad.proposals[0].expires_at_unix_ms = 1000,
            _ => bad.proposals.clear(),
        };
        assert!(validate_recovery_page(&req, &bad).is_err(), "case{change}");
    }
    let mut next = req;
    next.after_conversation_id = page.next_conversation_id.clone();
    assert!(validate_recovery_page(&next, &page).is_err());
}

#[test]
fn ack_requires_v2_context_valid_unique_bindings_and_known_disposition() {
    let r = send();
    let e = r.envelope.unwrap();
    let mut ack = common::AcknowledgeMessagesRequest {
        context: r.context,
        messages: vec![common::Acknowledgement {
            message_id: e.message_id.clone(),
            envelope_digest: data::envelope_digest(&e).unwrap().to_vec(),
            disposition: common::AckDisposition::Stored as i32,
        }],
    };
    validate_ack(&ack).unwrap();
    ack.context.as_mut().unwrap().protocol_version = 1;
    assert!(validate_ack(&ack).is_err());
    ack.context.as_mut().unwrap().protocol_version = 2;
    ack.messages[0].disposition = 999;
    assert!(validate_ack(&ack).is_err());
    ack.messages[0].disposition = 2;
    ack.messages.push(ack.messages[0].clone());
    assert!(validate_ack(&ack).is_err());
}

#[test]
fn capabilities_and_retained_metadata_reject_downgrade_limits_and_revision_overflow() {
    let good = wire::Capabilities {
        protocol_version: 2,
        max_active_devices: 1,
        crypto_suites: vec![1],
        limits: Some(common::Limits {
            max_request_bytes: MAX_REQUEST_BYTES as u32,
            max_response_bytes: MAX_RESPONSE_BYTES as u32,
            max_text_bytes: contract::MAX_TEXT_BYTES as u32,
            max_keys_per_kind_per_upload: 100,
            max_acknowledgements: MAX_ACKNOWLEDGEMENTS as u32,
            max_inbox_messages: MAX_INBOX_MESSAGES as u32,
        }),
        messaging_available: false,
        recovery_available: false,
    };
    validate_capabilities(&good).unwrap();
    for change in 0..6 {
        let mut bad = good.clone();
        match change {
            0 => bad.protocol_version = 1,
            1 => bad.limits = None,
            2 => bad.limits.as_mut().unwrap().max_request_bytes += 1,
            3 => bad.limits.as_mut().unwrap().max_keys_per_kind_per_upload = 101,
            4 => bad.max_active_devices = 2,
            _ => bad.crypto_suites.push(1),
        };
        assert!(validate_capabilities(&bad).is_err(), "case{change}");
    }
    let p = pins();
    let mut c = p.conversation.clone();
    c.peer_device.as_mut().unwrap().public_material_revision = u64::MAX;
    assert!(EpochConversation::new(c, p.own_pin, p.peer_pin).is_err());
    let mut page = catalog_page(&p);
    page.conversation
        .as_mut()
        .unwrap()
        .peer_device
        .as_mut()
        .unwrap()
        .public_material_revision = u64::MAX;
    let r = wire::GetConversationEpochsRequest {
        context: send().context,
        conversation_id: p.conversation.conversation_id.clone(),
        limit: 32,
        after_epoch: None,
    };
    assert!(validate_catalog(&r, &page, &p).is_err());
}
