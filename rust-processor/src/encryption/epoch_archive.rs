//! Retained epoch catalogue rows. Every row has a positive digest anchor; archived
//! scopes never overwrite the active root or share a Signal/acquisition slot.
use super::*;
pub(super) fn row_key(own: &DeviceRef, chat: &str, epoch: u64) -> CryptoResult<RecordKey> {
    if epoch > i64::MAX as u64 {
        return Err(CryptoError::Invalid);
    }
    key(own, chat, &format!("row:{epoch:016x}"))
}
fn anchor_key(own: &DeviceRef, chat: &str, epoch: u64) -> CryptoResult<RecordKey> {
    key(own, chat, &format!("row-anchor:{epoch:016x}"))
}
fn hash(row: &EpochRow) -> [u8; 32] {
    digest(b"zchat.e2ee.epoch-row.v2\0", &row.encode_to_vec())
}
pub(super) fn save(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    row: &EpochRow,
) -> CryptoResult<()> {
    let epoch = row
        .epoch
        .as_ref()
        .and_then(|e| e.epoch)
        .ok_or(CryptoError::Invalid)?;
    tx.put(
        &row_key(own, &row.conversation_id, epoch)?,
        &row.encode_to_vec(),
    )?;
    tx.put(&anchor_key(own, &row.conversation_id, epoch)?, &hash(row))?;
    Ok(())
}
pub(super) fn catalog_request(
    own: &DeviceRef,
    chat: &str,
    first: u64,
    limit: u32,
) -> v2::GetConversationEpochsRequest {
    v2::GetConversationEpochsRequest {
        context: Some(ClientContext {
            protocol_version: 2,
            device_generation_id: own.generation_id.clone(),
        }),
        conversation_id: chat.into(),
        after_epoch: first.checked_sub(1),
        limit,
    }
}
pub(super) fn read(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    c: &Conversation,
    public: IdentityKey,
    epoch: u64,
    active: u64,
) -> CryptoResult<EpochRow> {
    let chat = &c.conversation_id;
    let bytes = tx
        .get(&row_key(own, chat, epoch)?)?
        .ok_or(CryptoError::Invalid)?;
    let anchor = tx
        .get(&anchor_key(own, chat, epoch)?)?
        .ok_or(CryptoError::Invalid)?;
    if bytes.len() > 32768 || anchor.as_slice() != digest(b"zchat.e2ee.epoch-row.v2\0", &bytes) {
        return Err(CryptoError::Conflict);
    }
    let row = EpochRow::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    let e = row.epoch.as_ref().ok_or(CryptoError::Invalid)?;
    if row.format != FORMAT
        || row.conversation_id != *chat
        || row.own.as_ref() != Some(own)
        || row.peer.as_ref() != c.peer_device.as_ref().and_then(|p| p.reference.as_ref())
        || e.epoch != Some(epoch)
        || e.revision != Some(epoch)
        || epoch > active
        || e.sealed != (epoch < active)
    {
        return Err(CryptoError::Conflict);
    }
    let pins = pins(c, public)?;
    if epoch == 0 {
        if row.created_by.is_some()
            || !e.created_by_proposal_id.is_empty()
            || e.bootstrap_device.is_some()
        {
            return Err(CryptoError::Conflict);
        }
    } else {
        let r = row.created_by.as_ref().ok_or(CryptoError::Invalid)?;
        recovery::require_accepted(tx, own, &pins, r)?;
        let o = r.outcome.as_ref().ok_or(CryptoError::Invalid)?;
        let p = o
            .proposal
            .as_ref()
            .and_then(|p| p.control.as_ref())
            .ok_or(CryptoError::Invalid)?;
        if o.new_epoch != epoch
            || o.epoch_revision != e.revision.unwrap()
            || e.created_by_proposal_id != p.proposal_id
            || e.bootstrap_device != p.initiator.as_ref().and_then(|p| p.device.clone())
            || Some(e.created_at_unix_ms) != r.resolved_at_unix_ms
        {
            return Err(CryptoError::Conflict);
        }
    }
    if e.sealed {
        let r = row.sealed_by.as_ref().ok_or(CryptoError::Invalid)?;
        recovery::require_accepted(tx, own, &pins, r)?;
        let o = r.outcome.as_ref().ok_or(CryptoError::Invalid)?;
        let p = o
            .proposal
            .as_ref()
            .and_then(|p| p.control.as_ref())
            .ok_or(CryptoError::Invalid)?;
        if p.expected_epoch != epoch
            || p.expected_revision != e.revision.unwrap()
            || e.sealed_by_proposal_id != p.proposal_id
            || e.sealed_at_unix_ms != r.resolved_at_unix_ms
            || row.recovery_digest.len() != 32
        {
            return Err(CryptoError::Conflict);
        }
        chain::validate_tail(row.outgoing_count, &row.outgoing_head)?;
    } else if row.sealed_by.is_some()
        || row.outgoing_count.is_some()
        || !row.outgoing_head.is_empty()
        || !row.recovery_digest.is_empty()
    {
        return Err(CryptoError::Invalid);
    }
    // Validate exactly one retained row against the actual head. This doesn't
    // fabricate an unsealed snapshot for an old row or accept a gap in a page.
    let page = v2::ConversationEpochs {
        protocol_version: 2,
        conversation: Some(c.clone()),
        active_epoch: Some(active),
        epoch_revision: Some(active),
        epochs: vec![e.clone()],
        next_after_epoch: if epoch < active { Some(epoch) } else { None },
    };
    epoch_rpc::validate_catalog(&catalog_request(own, chat, epoch, 1), &page, &pins)
        .map_err(|_| CryptoError::Invalid)?;
    Ok(row)
}
pub(super) fn load_at(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    c: &Conversation,
    public: IdentityKey,
    registration: u32,
    epoch: u64,
) -> CryptoResult<Loaded> {
    let mut l =
        load(tx, own, c, public, registration)?.ok_or(CryptoError::ConversationUnavailable)?;
    let active = l.root.active_epoch.ok_or(CryptoError::Invalid)?;
    if epoch > active {
        return Err(CryptoError::EpochRecoveryRequired);
    }
    if epoch == active {
        return Ok(l);
    }
    let row = read(tx, own, c, public, epoch, active)?;
    // A read-only view of a retained epoch, never passed to save_root.
    l.root.active_epoch = Some(epoch);
    l.root.revision = row.epoch.as_ref().and_then(|e| e.revision);
    l.root.send_frozen = Some(true);
    l.root.row_digest = hash(&row).to_vec();
    l.root.outgoing_count = row.outgoing_count;
    l.root.outgoing_head = row.outgoing_head.clone();
    l.root.recovery_digest = row.recovery_digest.clone();
    l.row = row;
    chain::validate_root(tx, own, &l.root)?;
    recovery::validate_head(tx, own, &l.root, &pins(c, public)?)?;
    Ok(l)
}
