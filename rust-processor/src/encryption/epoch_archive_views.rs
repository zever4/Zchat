//! Presentation only: retained local generations and crypto epochs. Never opens
//! an old worker, creates an index, imports keys, resumes delivery or mints a
//! mutation scope outside the private cohort validators.
use super::*;

pub struct ViewArchive {
    pub generation_id: String,
    pub replaced_by_generation_id: Option<String>,
    pub history_ready: bool,
}
pub struct ViewSegment {
    pub crypto_epoch: u64,
    pub sealed: bool,
    pub created_at_unix_ms: i64,
    pub sealed_at_unix_ms: Option<i64>,
}
#[derive(Clone, PartialEq, Eq)]
pub(super) enum Query {
    Generations,
    Dialogs(String),
    Segments(String, String),
    History(String, String, Option<u64>),
}
pub(super) fn target(
    tx: &mut ProtectedTransaction<'_>,
    active: &DeviceRef,
    generation: &str,
) -> CryptoResult<(DeviceRef, Option<String>)> {
    if !epoch_rpc::uuid4(generation) {
        return Err(CryptoError::Invalid);
    }
    let own = DeviceRef {
        generation_id: generation.into(),
        ..active.clone()
    };
    let replacement = if own == *active {
        None
    } else {
        Some(crate::encryption::provisioning::read_archived_device(tx, &own)?.generation_id)
    };
    Ok((own, replacement))
}
fn boundary(
    c: Option<ViewCursor>,
    instance: uuid::Uuid,
    own: &DeviceRef,
    store: &StoreScope,
    query: &Query,
) -> CryptoResult<Option<String>> {
    let Some(c) = c else { return Ok(None) };
    if c.instance != instance || c.own != *own || c.store != *store {
        return Err(CryptoError::Conflict);
    }
    match c.position {
        Position::Archive(q, after) if q == *query => Ok(Some(after)),
        _ => Err(CryptoError::Conflict),
    }
}
fn next(
    instance: uuid::Uuid,
    own: &DeviceRef,
    store: &StoreScope,
    query: Query,
    last: Option<String>,
) -> Option<ViewCursor> {
    last.map(|last| cursor(instance, own, store, Position::Archive(query, last)))
}
pub(super) fn read(
    tx: &mut ProtectedTransaction<'_>,
    active: &DeviceRef,
    store: &StoreScope,
    instance: uuid::Uuid,
    request: ViewRequest,
) -> CryptoResult<ViewPage> {
    match request {
        ViewRequest::Archives(c) => {
            let query = Query::Generations;
            let after = boundary(c, instance, active, store, &query)?;
            let rows = tx.generation_page(after.as_deref(), ITEMS + 1)?;
            let more = rows.len() > ITEMS;
            let mut items = Vec::new();
            let mut last = None;
            for id in rows.into_iter().take(ITEMS) {
                let (own, replaced_by_generation_id) = target(tx, active, &id)?;
                items.push(ViewArchive {
                    generation_id: id.clone(),
                    replaced_by_generation_id,
                    history_ready: history::ready(tx, &own)?,
                });
                last = Some(id);
            }
            Ok(ViewPage::Archives(
                items,
                next(
                    instance,
                    active,
                    store,
                    query,
                    if more { last } else { None },
                ),
            ))
        }
        ViewRequest::ArchiveDialogs(generation, c) => {
            let query = Query::Dialogs(generation.clone());
            let after = boundary(c, instance, active, store, &query)?;
            let (own, _) = target(tx, active, &generation)?;
            let rows = tx.record_page_limited(
                &generation,
                RecordKind::Control,
                "conversation:",
                after.as_deref(),
                false,
                (ITEMS + 1, 2048),
            )?;
            let more = rows.len() > ITEMS;
            let mut items = Vec::new();
            let mut last = None;
            for (id, bytes) in rows.into_iter().take(ITEMS) {
                let value =
                    Conversation::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
                conversation_lifecycle::validate(&value, &own)?;
                if id != format!("conversation:{}", value.conversation_id) {
                    return Err(CryptoError::Conflict);
                }
                items.push(dialog(value)?);
                last = Some(id);
            }
            Ok(ViewPage::Dialogs(
                items,
                next(
                    instance,
                    active,
                    store,
                    query,
                    if more { last } else { None },
                ),
            ))
        }
        ViewRequest::Segments(generation, chat, c) => {
            let query = Query::Segments(generation.clone(), chat.clone());
            let before = boundary(c, instance, active, store, &query)?;
            let (own, _) = target(tx, active, &generation)?;
            let value = conversation_lifecycle::load(tx, &own, &chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let (public, registration) = identity(tx, &own)?;
            let Some(l) = load(tx, &own, &value, public, registration)? else {
                return Ok(ViewPage::Segments(Vec::new(), None));
            };
            let active_epoch = l.root.active_epoch.ok_or(CryptoError::Invalid)?;
            let prefix = format!("{}row:", super::super::prefix(&chat)?);
            let mut expected = match &before {
                None => Some(active_epoch),
                Some(id) => {
                    let suffix = id.strip_prefix(&prefix).ok_or(CryptoError::Conflict)?;
                    if suffix.len() != 16 {
                        return Err(CryptoError::Conflict);
                    }
                    let epoch =
                        u64::from_str_radix(suffix, 16).map_err(|_| CryptoError::Conflict)?;
                    if epoch > active_epoch {
                        return Err(CryptoError::Conflict);
                    }
                    epoch.checked_sub(1)
                }
            };
            let rows = tx.record_page_limited(
                &generation,
                RecordKind::Control,
                &prefix,
                before.as_deref(),
                true,
                (ITEMS + 1, 32768),
            )?;
            let more = rows.len() > ITEMS;
            let mut items = Vec::new();
            let mut last = None;
            for (id, bytes) in rows.into_iter().take(ITEMS) {
                let decoded =
                    EpochRow::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
                let epoch = decoded
                    .epoch
                    .as_ref()
                    .and_then(|e| e.epoch)
                    .ok_or(CryptoError::Invalid)?;
                if id != format!("{prefix}{epoch:016x}") || expected != Some(epoch) {
                    return Err(CryptoError::Conflict);
                }
                let row = archive::read(tx, &own, &value, public, epoch, active_epoch)?;
                if decoded != row {
                    return Err(CryptoError::Conflict);
                }
                let e = row.epoch.ok_or(CryptoError::Invalid)?;
                items.push(ViewSegment {
                    crypto_epoch: epoch,
                    sealed: e.sealed,
                    created_at_unix_ms: e.created_at_unix_ms,
                    sealed_at_unix_ms: e.sealed_at_unix_ms,
                });
                last = Some(id);
                expected = epoch.checked_sub(1);
            }
            // A truncated/deleted row must not silently hide a retained epoch.
            if !more && expected.is_some() {
                return Err(CryptoError::Conflict);
            }
            Ok(ViewPage::Segments(
                items,
                next(
                    instance,
                    active,
                    store,
                    query,
                    if more { last } else { None },
                ),
            ))
        }
        ViewRequest::ArchiveHistory(generation, chat, epoch, c) => {
            let query = Query::History(generation.clone(), chat.clone(), epoch);
            let before = boundary(c, instance, active, store, &query)?;
            let (own, _) = target(tx, active, &generation)?;
            history::require_ready(tx, &own)?;
            let value = conversation_lifecycle::load(tx, &own, &chat)?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let mut prefix = history::prefix(&chat)?;
            if let Some(epoch) = epoch {
                if epoch > i64::MAX as u64 {
                    return Err(CryptoError::Invalid);
                }
                let (public, registration) = identity(tx, &own)?;
                archive::load_at(tx, &own, &value, public, registration, epoch)?;
                prefix.push_str(&format!("{epoch:016x}:"));
            }
            let (items, last) = history_page(tx, &own, store, &prefix, before.as_deref())?;
            Ok(ViewPage::History(
                items,
                next(instance, active, store, query, last),
            ))
        }
        _ => Err(CryptoError::Invalid),
    }
}

#[cfg(test)]
#[path = "epoch_archive_views_tests.rs"]
mod tests;
