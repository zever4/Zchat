//! Explicit bounded migration and monotonic conversation activity evidence.
//! SQL projections accelerate pending work; encrypted journals remain authority.
use super::*;

const STATE: &str = "recovery-index:state:v1";
const ANCHOR: &str = "recovery-index:anchor:v1";
const PHASES: [(RecordKind, &str); 5] = [
    (RecordKind::Outbox, ""),
    (RecordKind::Inbox, ""),
    (RecordKind::History, ""),
    (RecordKind::Control, "acquire:"),
    (RecordKind::Control, "delivery-in:"),
];
#[derive(Message)]
struct State {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(uint32, tag = "2")]
    phase: u32,
    #[prost(string, tag = "3")]
    after: String,
}
pub struct RecoveryIndexPage {
    pub scanned: usize,
    pub ready: bool,
}
fn key(own: &DeviceRef, id: &str) -> Result<RecordKey, StoreError> {
    RecordKey::new(&own.generation_id, RecordKind::Control, id)
}
fn state(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<Option<State>> {
    match (tx.get(&key(own, STATE)?)?, tx.get(&key(own, ANCHOR)?)?) {
        (None, None) => Ok(None),
        (Some(bytes), Some(anchor)) if anchor.as_slice() == [1] => {
            let state = State::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
            if state.format != 1 || state.phase > 5 || state.phase == 5 && !state.after.is_empty() {
                return Err(CryptoError::Invalid);
            }
            if !state.after.is_empty() {
                let (kind, prefix) = PHASES[state.phase as usize];
                RecordKey::new(&own.generation_id, kind, &state.after)?;
                if !state.after.starts_with(prefix) {
                    return Err(CryptoError::Invalid);
                }
            }
            Ok(Some(state))
        }
        _ => Err(CryptoError::Invalid),
    }
}
fn save(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef, s: &State) -> CryptoResult<()> {
    tx.put(&key(own, STATE)?, &s.encode_to_vec())?;
    Ok(())
}
pub(super) fn ready(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<bool> {
    Ok(state(tx, own)?.is_some_and(|s| s.phase == 5))
}
pub(super) fn require_ready(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
) -> CryptoResult<()> {
    match state(tx, own)? {
        None if !tx.has_recovery_records(&own.generation_id)?
            && !tx.has_recovery_projection(&own.generation_id)? =>
        {
            // A genuinely empty generation needs no historical migration. This
            // occurs in an explicit operation, never by opening the database.
            tx.put(&key(own, ANCHOR)?, &[1])?;
            save(
                tx,
                own,
                &State {
                    format: 1,
                    phase: 5,
                    after: String::new(),
                },
            )?;
        }
        Some(s) if s.phase == 5 => (),
        _ => return Err(CryptoError::RecoveryIndexesNotReady),
    }
    if tx.unfinished_recovery(&own.generation_id)? {
        return Err(CryptoError::Invalid);
    }
    Ok(())
}
fn activity_key(own: &DeviceRef, conversation: &str) -> CryptoResult<RecordKey> {
    RecordKey::new(conversation, RecordKind::Control, "uuid")?;
    Ok(key(own, &format!("recovery-traffic:{conversation}"))?)
}
pub(super) fn has_traffic(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    conversation: &str,
) -> CryptoResult<bool> {
    match tx.get(&activity_key(own, conversation)?)? {
        None => Ok(false),
        Some(bytes) if bytes.as_slice() == [1] => Ok(true),
        _ => Err(CryptoError::Invalid),
    }
}
pub(super) fn observe(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    envelope: &Envelope,
) -> CryptoResult<()> {
    let peer = if envelope.sender.as_ref() == Some(own) {
        envelope.recipient.as_ref()
    } else if envelope.recipient.as_ref() == Some(own) {
        envelope.sender.as_ref()
    } else {
        return Err(CryptoError::Conflict);
    }
    .ok_or(CryptoError::Invalid)?;
    tx.put(&activity_key(own, &envelope.conversation_id)?, &[1])?;
    let key = acquisition_key_for(own, &envelope.conversation_id, peer)?;
    if let Some(bytes) = tx.get(&key)? {
        let mut journal =
            AcquisitionJournal::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
        validate_acquisition_journal(&journal, own, &envelope.conversation_id, peer)?;
        if ready(tx, own)? {
            tx.check_recovery_projection(&key, false)?;
        }
        journal.activity_guard_version = 1;
        journal.has_message_state = true;
        tx.put(&key, &journal.encode_to_vec())?;
        tx.project_recovery(&key, false)?;
    }
    Ok(())
}
fn history_envelope(bytes: &[u8]) -> CryptoResult<Envelope> {
    let p = EncryptedPayload::decode(bytes).map_err(|_| CryptoError::Invalid)?;
    let e = Envelope {
        protocol_version: 1,
        crypto_suite: CryptoSuite::SignalPq1 as i32,
        message_id: p.message_id.clone(),
        conversation_id: p.conversation_id.clone(),
        sender: p.sender.clone(),
        recipient: p.recipient.clone(),
        message_type: CiphertextType::Signal as i32,
        ciphertext: vec![1],
    };
    validate_payload_binding(&p, &e).map_err(|_| CryptoError::Invalid)?;
    Ok(e)
}
impl Engine {
    pub(super) fn seed_recovery_indexes(&mut self) -> CryptoResult<RecoveryIndexPage> {
        let own = &self.own;
        let store_scope = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            let mut s = match state(tx, own)? {
                Some(s) => s,
                None => {
                    if tx.has_recovery_projection(&own.generation_id)? {
                        return Err(CryptoError::Invalid);
                    }
                    tx.put(&key(own, ANCHOR)?, &[1])?;
                    let initial = State {
                        format: 1,
                        phase: 0,
                        after: String::new(),
                    };
                    // Helpers in this same transaction must see a consistent
                    // root/anchor before reading or rewriting legacy journals.
                    save(tx, own, &initial)?;
                    initial
                }
            };
            if s.phase == 5 {
                require_ready(tx, own)?;
                return Ok(RecoveryIndexPage {
                    scanned: 0,
                    ready: true,
                });
            }
            let (kind, prefix) = PHASES[s.phase as usize];
            let rows = tx.record_page(
                &own.generation_id,
                kind,
                prefix,
                (!s.after.is_empty()).then_some(s.after.as_str()),
            )?;
            for (id, bytes) in &rows {
                let record = RecordKey::new(&own.generation_id, kind, id)?;
                if id.starts_with("v2:") && kind != RecordKind::Control {
                    epoch_state::index_data_record(tx, own, &store_scope, kind, id, bytes)?;
                    continue;
                }
                if kind == RecordKind::Control && prefix == "acquire:" {
                    let mut journal = AcquisitionJournal::decode(bytes.as_slice())
                        .map_err(|_| CryptoError::Invalid)?;
                    let req = journal.request.as_ref().ok_or(CryptoError::Invalid)?;
                    validate_acquisition_journal(
                        &journal,
                        own,
                        &req.conversation_id,
                        req.expected_peer.as_ref().ok_or(CryptoError::Invalid)?,
                    )?;
                    if acquisition_key_for(
                        own,
                        &req.conversation_id,
                        req.expected_peer.as_ref().unwrap(),
                    )? != record
                    {
                        return Err(CryptoError::Conflict);
                    }
                    journal.has_message_state |= has_traffic(tx, own, &req.conversation_id)?;
                    journal.activity_guard_version = 1;
                    tx.put(&record, &journal.encode_to_vec())?;
                    tx.project_recovery(&record, false)?;
                } else if kind == RecordKind::Control {
                    if id.starts_with("delivery-in:v2:") {
                        epoch_state::index_ack_record(tx, own, &store_scope, id)?;
                        continue;
                    }
                    delivery::index_incoming(
                        tx,
                        own,
                        id.strip_prefix(prefix).ok_or(CryptoError::Invalid)?,
                    )?;
                } else {
                    let e = if kind == RecordKind::History {
                        history_envelope(bytes)?
                    } else {
                        let e =
                            Envelope::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
                        validate_envelope(&e).map_err(|_| CryptoError::Invalid)?;
                        e
                    };
                    if e.message_id != *id
                        || kind == RecordKind::Outbox && e.sender.as_ref() != Some(own)
                        || kind == RecordKind::Inbox && e.recipient.as_ref() != Some(own)
                    {
                        return Err(CryptoError::Conflict);
                    }
                    observe(tx, own, &e)?;
                    if kind == RecordKind::Outbox {
                        delivery::index_outgoing(tx, own, id)?;
                    } else {
                        tx.project_recovery(&record, false)?;
                    }
                }
            }
            let scanned = rows.len();
            if scanned == 32 {
                s.after = rows.last().unwrap().0.clone();
            } else {
                s.phase += 1;
                s.after.clear();
            }
            save(tx, own, &s)?;
            if s.phase == 5 && tx.unfinished_recovery(&own.generation_id)? {
                return Err(CryptoError::Invalid);
            }
            Ok(RecoveryIndexPage {
                scanned,
                ready: s.phase == 5,
            })
        })
    }
}
