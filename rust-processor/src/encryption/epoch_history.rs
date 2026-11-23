//! Authenticated display index, not Signal/delivery authority. No schema/key
//! changes. New text entries commit with their history; old v2 history is seeded
//! explicitly in <=32-source-row transactions before the account becomes ready.
//! Like other record keys, order keys are plaintext metadata (chat/epoch/time/UUID).
//! Payload, digest and backlinks use the store's AEAD; the index contains no text.
use super::*;
const STATE: &str = "ui-h2-state";
const ENTRIES: &str = "ui-h2:";
const LINKS: &str = "ui-h2ref:";
#[derive(Message)]
struct Seed {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    after: String,
    #[prost(bool, optional, tag = "3")]
    ready: Option<bool>,
}
#[derive(Message, PartialEq)]
struct Entry {
    #[prost(uint32, tag = "1")]
    format: u32,
    #[prost(string, tag = "2")]
    history_id: String,
    #[prost(bytes = "vec", tag = "3")]
    digest: Vec<u8>,
}
pub struct HistorySeedProgress {
    pub processed: usize,
    pub ready: bool,
}
fn key(own: &DeviceRef, id: &str) -> CryptoResult<RecordKey> {
    Ok(RecordKey::new(&own.generation_id, RecordKind::Control, id)?)
}
fn seed(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<Option<Seed>> {
    let Some(bytes) = tx.get(&key(own, STATE)?)? else {
        if tx.has_record_prefix(&own.generation_id, RecordKind::Control, ENTRIES)?
            || tx.has_record_prefix(&own.generation_id, RecordKind::Control, LINKS)?
        {
            return Err(CryptoError::Invalid);
        }
        return Ok(None);
    };
    let s = Seed::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
    if s.format != 1 || s.ready.is_none() {
        return Err(CryptoError::Invalid);
    }
    if !s.after.is_empty() {
        outgoing::parse_id(&s.after)?;
    }
    Ok(Some(s))
}
pub(super) fn require_ready(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
) -> CryptoResult<()> {
    if !ready(tx, own)? {
        return Err(CryptoError::HistoryIndexNotReady);
    }
    Ok(())
}
pub(super) fn ready(tx: &mut ProtectedTransaction<'_>, own: &DeviceRef) -> CryptoResult<bool> {
    Ok(seed(tx, own)?.is_some_and(|s| s.ready == Some(true)))
}
pub(super) fn prefix(chat: &str) -> CryptoResult<String> {
    if !epoch_rpc::uuid4(chat) {
        return Err(CryptoError::Invalid);
    }
    Ok(format!("{ENTRIES}{chat}:"))
}
fn entry_id(p: &v2::EncryptedPayload) -> CryptoResult<String> {
    let epoch = p.crypto_epoch.ok_or(CryptoError::Invalid)?;
    if epoch > i64::MAX as u64 || p.created_at_unix_ms < 0 || !epoch_rpc::uuid4(&p.message_id) {
        return Err(CryptoError::Invalid);
    }
    Ok(format!(
        "{}{:016x}:{:016x}:{}",
        prefix(&p.conversation_id)?,
        epoch,
        p.created_at_unix_ms,
        p.message_id
    ))
}
fn expected(id: &str, p: &v2::EncryptedPayload) -> CryptoResult<Entry> {
    let (chat, epoch, uuid) = outgoing::parse_id(id)?;
    if chat != p.conversation_id || Some(epoch) != p.crypto_epoch || uuid != p.message_id {
        return Err(CryptoError::Conflict);
    }
    Ok(Entry {
        format: 1,
        history_id: id.into(),
        digest: digest(
            b"zchat.e2ee.ui-history.v2\0",
            &Zeroizing::new(p.encode_to_vec()),
        )
        .to_vec(),
    })
}
pub(super) fn check(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    p: &v2::EncryptedPayload,
) -> CryptoResult<()> {
    let e = expected(id, p)?;
    let index = entry_id(p)?;
    for k in [index, format!("{LINKS}{}", p.message_id)] {
        let bytes = tx.get(&key(own, &k)?)?.ok_or(CryptoError::Invalid)?;
        let actual = Entry::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
        if actual != e {
            return Err(CryptoError::Conflict);
        }
    }
    Ok(())
}
pub(super) fn index(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    id: &str,
    p: &v2::EncryptedPayload,
) -> CryptoResult<()> {
    if !crate::encryption::epoch_contract::user_message(p) {
        return Ok(());
    }
    let e = expected(id, p)?;
    let index = key(own, &entry_id(p)?)?;
    let link = key(own, &format!("{LINKS}{}", p.message_id))?;
    if seed(tx, own)?.is_none() {
        tx.put(
            &key(own, STATE)?,
            &Seed {
                format: 1,
                after: String::new(),
                ready: Some(false),
            }
            .encode_to_vec(),
        )?;
    }
    match (tx.get(&index)?, tx.get(&link)?) {
        (None, None) => {
            let bytes = e.encode_to_vec();
            tx.put(&index, &bytes)?;
            tx.put(&link, &bytes)?;
        }
        (Some(a), Some(b))
            if a.as_slice() == b.as_slice()
                && Entry::decode(a.as_slice()).is_ok_and(|a| a == e) => {}
        _ => return Err(CryptoError::Conflict),
    }
    Ok(())
}
/// Full protected delivery cohort is checked before returning ANY plaintext.
pub(super) fn payload(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    id: &str,
) -> CryptoResult<(
    v2::EncryptedPayload,
    Option<MessageProgress>,
    Option<v2::Envelope>,
)> {
    let (_, _, uuid) = outgoing::parse_id(id)?;
    let link = outgoing::load_link(tx, own, uuid)?;
    if link.id != id {
        return Err(CryptoError::Conflict);
    }
    if link.outgoing == Some(true) {
        let (p, status) = outgoing::view_payload(tx, own, store, id)?;
        Ok((p, Some(status), None))
    } else {
        let (p, envelope) = incoming::view_payload(tx, own, store, id)?;
        Ok((p, None, Some(envelope)))
    }
}
pub(super) fn item(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    index_id: &str,
    bytes: &[u8],
) -> CryptoResult<HistoryItem> {
    let e = Entry::decode(bytes).map_err(|_| CryptoError::Invalid)?;
    let (mut p, delivery, incoming) = payload(tx, own, store, &e.history_id)?;
    if expected(&e.history_id, &p)? != e || entry_id(&p)? != index_id {
        return Err(CryptoError::Conflict);
    }
    check(tx, own, &e.history_id, &p)?;
    let cache_state = match (&p.content, incoming.as_ref()) {
        (Some(v2::encrypted_payload::Content::Attachment(_)), Some(envelope)) => Some(
            attachment_download::history_cache_state(tx, own, &p, envelope)?,
        ),
        (Some(v2::encrypted_payload::Content::Attachment(_)), None) => Some(
            if attachment_prepare::retirement::is_removed(tx, own, &p.message_id)? {
                AttachmentCacheState::Removed
            } else {
                AttachmentCacheState::Saved
            },
        ),
        _ => None,
    };
    let (text, attachment) = match p.content.take() {
        Some(v2::encrypted_payload::Content::Text(text)) => (Zeroizing::new(text.text), None),
        Some(v2::encrypted_payload::Content::Attachment(a)) => (
            Zeroizing::new("Зашифрованное вложение".into()),
            Some(
                crate::encryption::attachment_payload::summary(a)
                    .map_err(|_| CryptoError::Invalid)?,
            ),
        ),
        _ => return Err(CryptoError::Invalid),
    };
    Ok(HistoryItem {
        message_id: p.message_id,
        conversation_id: p.conversation_id,
        crypto_epoch: p.crypto_epoch.ok_or(CryptoError::Invalid)?,
        created_at_unix_ms: p.created_at_unix_ms,
        sender_user_id: p.sender.ok_or(CryptoError::Invalid)?.user_id,
        text,
        attachment,
        cache_state,
        delivery,
    })
}
impl Engine {
    pub(super) fn seed_history_index(&mut self) -> CryptoResult<HistorySeedProgress> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| {
            recovery_indexes::require_ready(tx, own)?;
            let mut s = seed(tx, own)?.unwrap_or(Seed {
                format: 1,
                after: String::new(),
                ready: Some(false),
            });
            if s.ready == Some(true) {
                return Ok(HistorySeedProgress {
                    processed: 0,
                    ready: true,
                });
            }
            let rows = tx.record_page_limited(
                &own.generation_id,
                RecordKind::History,
                "v2:",
                if s.after.is_empty() {
                    None
                } else {
                    Some(&s.after)
                },
                false,
                (32, 16 * 1024),
            )?;
            // Seed state must exist before index() classifies any existing entry.
            tx.put(&key(own, STATE)?, &s.encode_to_vec())?;
            for (id, bytes) in &rows {
                let (p, ..) = payload(tx, own, &store, id)?;
                let decoded = v2::EncryptedPayload::decode(bytes.as_slice())
                    .map_err(|_| CryptoError::Invalid)?;
                if p != decoded {
                    return Err(CryptoError::Conflict);
                }
                index(tx, own, id, &p)?;
                s.after = id.clone();
            }
            s.ready = Some(rows.len() < 32);
            tx.put(&key(own, STATE)?, &s.encode_to_vec())?;
            Ok(HistorySeedProgress {
                processed: rows.len(),
                ready: s.ready == Some(true),
            })
        })
    }
}
impl CryptoCoordinator {
    pub async fn seed_history_index(&self) -> CryptoResult<HistorySeedProgress> {
        self.request(|r| Command::Epoch(EpochCommand::SeedHistory(r)))
            .await
    }
}
