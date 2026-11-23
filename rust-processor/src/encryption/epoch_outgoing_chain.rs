//! Authenticated inventory of ALL outgoing messages in one epoch. It is append-only
//! and anchored by the protected root. Explicit transition audits follow exact
//! sequence/hash links in bounded steps; SQL pending flags cannot omit work.
use super::*;
pub(super) const EMPTY: [u8; 32] = [0; 32];
#[derive(Message)]
pub(super) struct Entry {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(uint64, tag = "2")]
    pub(super) sequence: u64,
    #[prost(string, tag = "3")]
    pub(super) message: String,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) envelope_digest: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub(super) previous: Vec<u8>,
}
fn entry_key(s: &EpochScope, sequence: u64) -> CryptoResult<RecordKey> {
    key(
        &s.own,
        &s.conversation,
        &format!("outgoing-chain:{:016x}:{sequence:016x}", s.epoch),
    )
}
pub(super) fn hash(e: &Entry) -> [u8; 32] {
    digest(b"zchat.e2ee.outgoing-chain.v2\0", &e.encode_to_vec())
}
pub(super) fn validate_tail(count: Option<u64>, hash: &[u8]) -> CryptoResult<()> {
    let n = count
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    if hash.len() != 32 || (n == 0) != (hash == EMPTY) {
        return Err(CryptoError::Invalid);
    }
    Ok(())
}
pub(super) fn read(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    sequence: u64,
) -> CryptoResult<Entry> {
    read_at(tx, &s.own, &s.conversation, s.epoch, sequence)
}
fn read_at(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    chat: &str,
    epoch: u64,
    sequence: u64,
) -> CryptoResult<Entry> {
    if sequence == 0 || sequence > i64::MAX as u64 {
        return Err(CryptoError::Invalid);
    }
    let b = tx
        .get(&key(
            own,
            chat,
            &format!("outgoing-chain:{epoch:016x}:{sequence:016x}"),
        )?)?
        .ok_or(CryptoError::Invalid)?;
    if b.len() > 256 {
        return Err(CryptoError::Invalid);
    }
    let e = Entry::decode(b.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if e.format != FORMAT
        || e.sequence != sequence
        || !epoch_rpc::uuid4(&e.message)
        || e.envelope_digest.len() != 32
    {
        return Err(CryptoError::Conflict);
    }
    validate_tail(Some(sequence - 1), &e.previous)?;
    Ok(e)
}
// O(1) root check, never an implicit history scan. Intermediate links are
// authenticated by the explicit bounded transition walk.
pub(super) fn validate_root(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    root: &Root,
) -> CryptoResult<()> {
    validate_tail(root.outgoing_count, &root.outgoing_head)?;
    let count = root.outgoing_count.ok_or(CryptoError::Invalid)?;
    if count > 0 {
        let e = read_at(
            tx,
            own,
            &root
                .conversation
                .as_ref()
                .ok_or(CryptoError::Invalid)?
                .conversation_id,
            root.active_epoch.ok_or(CryptoError::Invalid)?,
            count,
        )?;
        if hash(&e).as_slice() != root.outgoing_head {
            return Err(CryptoError::Conflict);
        }
    }
    Ok(())
}
pub(super) fn append(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    e: &v2::Envelope,
) -> CryptoResult<(u64, Vec<u8>)> {
    let c = conversation_lifecycle::load(tx, &s.own, &s.conversation)?
        .ok_or(CryptoError::ConversationUnavailable)?;
    let (public, registration) = identity(tx, &s.own)?;
    let mut l = load(tx, &s.own, &c, public, registration)?.ok_or(CryptoError::Invalid)?;
    if l.root.active_epoch != Some(s.epoch) || s.sealed || l.root.send_frozen != Some(false) {
        return Err(CryptoError::EpochSendingFrozen);
    }
    let sequence = l
        .root
        .outgoing_count
        .ok_or(CryptoError::Invalid)?
        .checked_add(1)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or(CryptoError::Invalid)?;
    let k = entry_key(s, sequence)?;
    if tx.get(&k)?.is_some() {
        return Err(CryptoError::Conflict);
    }
    let entry = Entry {
        format: FORMAT,
        sequence,
        message: e.message_id.clone(),
        envelope_digest: crate::encryption::epoch_contract::envelope_digest(e)
            .map_err(|_| CryptoError::Invalid)?
            .to_vec(),
        previous: l.root.outgoing_head,
    };
    let hash = hash(&entry).to_vec();
    tx.put(&k, &entry.encode_to_vec())?;
    l.root.outgoing_count = Some(sequence);
    l.root.outgoing_head = hash.clone();
    save_root(tx, &s.own, &mut l.root)?;
    Ok((sequence, hash))
}
pub(super) fn validate_message(
    tx: &mut ProtectedTransaction<'_>,
    s: &EpochScope,
    e: &v2::Envelope,
    sequence: Option<u64>,
    expected: &[u8],
) -> CryptoResult<()> {
    let sequence = sequence.ok_or(CryptoError::Invalid)?;
    if sequence > s.outgoing_count {
        return Err(CryptoError::Conflict);
    }
    let entry = read(tx, s, sequence)?;
    if hash(&entry).as_slice() != expected
        || entry.message != e.message_id
        || entry.envelope_digest
            != crate::encryption::epoch_contract::envelope_digest(e)
                .map_err(|_| CryptoError::Invalid)?
                .as_slice()
    {
        return Err(CryptoError::Conflict);
    }
    Ok(())
}
