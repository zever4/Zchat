//! Bounded local presentation reads. Cursor positions are opaque and bound to
//! the worker lifetime, store, generation, query kind and conversation. A page
//! never bootstraps, sends, acknowledges, repairs or changes cryptographic state.
use super::*;
use crate::proto::e2ee as common;
pub(super) const ITEMS: usize = 8;
pub(super) const HISTORY_WIRE_BUDGET: usize = 14 * 1024;
#[path = "epoch_archive_views.rs"]
mod archives;
pub use archives::{ViewArchive, ViewSegment};
pub(super) fn archive_own(
    tx: &mut ProtectedTransaction<'_>,
    active: &DeviceRef,
    generation: &str,
) -> CryptoResult<DeviceRef> {
    archives::target(tx, active, generation).map(|(own, _)| own)
}
#[derive(Clone, PartialEq, Eq)]
enum Position {
    Dialogs(String),
    History(String, String),
    PendingAttachments(String, String),
    Archive(archives::Query, String),
}
#[derive(Clone, PartialEq, Eq)]
pub struct ViewCursor {
    instance: uuid::Uuid,
    own: DeviceRef,
    store: StoreScope,
    position: Position,
}
pub enum ViewRequest {
    Dialogs(Option<ViewCursor>),
    History(String, Option<ViewCursor>),
    PendingAttachments(String, Option<ViewCursor>),
    Statuses(String, Vec<String>),
    Archives(Option<ViewCursor>),
    ArchiveDialogs(String, Option<ViewCursor>),
    Segments(String, String, Option<ViewCursor>),
    ArchiveHistory(String, String, Option<u64>, Option<ViewCursor>),
}
impl ViewRequest {
    pub fn is_archive(&self) -> bool {
        matches!(
            self,
            Self::Archives(_)
                | Self::ArchiveDialogs(..)
                | Self::Segments(..)
                | Self::ArchiveHistory(..)
        )
    }
}
pub struct ViewDialog {
    pub conversation_id: String,
    pub peer_user_id: String,
    pub state: common::ConversationState,
    pub close_reason: Option<common::ConversationCloseReason>,
    pub closed_at_unix_ms: i64,
}
pub struct MessageProgress {
    pub stored_on_server: bool,
    pub state: Option<common::MessageState>,
    pub removal: Option<common::MessageState>,
    pub sealed_unsent: bool,
}
// Intentionally no Debug/Serialize for a plaintext page.
pub struct HistoryItem {
    pub message_id: String,
    pub conversation_id: String,
    pub crypto_epoch: u64,
    pub created_at_unix_ms: i64,
    pub sender_user_id: String,
    pub text: Zeroizing<String>,
    // Key-free metadata. The text field also carries a fallback for attachments.
    pub attachment: Option<crate::encryption::attachment_payload::AttachmentSummary>,
    pub cache_state: Option<AttachmentCacheState>,
    pub delivery: Option<MessageProgress>,
}
pub struct MessageViewStatus {
    pub message_id: String,
    pub crypto_epoch: u64,
    pub delivery: MessageProgress,
}
pub struct PendingAttachment {
    pub message_id: String,
    pub crypto_epoch: u64,
    pub created_at_unix_ms: i64,
    pub attachment: crate::encryption::attachment_payload::AttachmentSummary,
}
pub enum ViewPage {
    Dialogs(Vec<ViewDialog>, Option<ViewCursor>),
    History(Vec<HistoryItem>, Option<ViewCursor>),
    PendingAttachments(Vec<PendingAttachment>, Option<ViewCursor>),
    Statuses(Vec<MessageViewStatus>),
    Archives(Vec<ViewArchive>, Option<ViewCursor>),
    Segments(Vec<ViewSegment>, Option<ViewCursor>),
}
fn dialog(v: Conversation) -> CryptoResult<ViewDialog> {
    Ok(ViewDialog {
        conversation_id: v.conversation_id,
        peer_user_id: v
            .peer_device
            .and_then(|p| p.reference)
            .ok_or(CryptoError::Invalid)?
            .user_id,
        state: common::ConversationState::try_from(v.state).map_err(|_| CryptoError::Invalid)?,
        close_reason: if v.close_reason == 0 {
            None
        } else {
            Some(
                common::ConversationCloseReason::try_from(v.close_reason)
                    .map_err(|_| CryptoError::Invalid)?,
            )
        },
        closed_at_unix_ms: v.closed_at_unix_ms,
    })
}
fn cursor(
    instance: uuid::Uuid,
    own: &DeviceRef,
    store: &StoreScope,
    position: Position,
) -> ViewCursor {
    ViewCursor {
        instance,
        own: own.clone(),
        store: store.clone(),
        position,
    }
}
fn boundary(
    c: Option<ViewCursor>,
    instance: uuid::Uuid,
    own: &DeviceRef,
    store: &StoreScope,
    chat: Option<&str>,
) -> CryptoResult<Option<String>> {
    let Some(c) = c else {
        return Ok(None);
    };
    if c.instance != instance || c.own != *own || c.store != *store {
        return Err(CryptoError::Conflict);
    }
    match (c.position, chat) {
        (Position::Dialogs(after), None) => Ok(Some(after)),
        (Position::History(old, before), Some(chat)) if old == chat => Ok(Some(before)),
        _ => Err(CryptoError::Conflict),
    }
}
/// Conservative JSON string byte cost, including worst-case control escaping.
/// IDs/status/field names add <1024 bytes/item; the adapter checks exact wire size.
fn wire_cost(text: &str) -> usize {
    1024 + text
        .chars()
        .map(|c| match c {
            '"' | '\\' => 2,
            c if c <= '\u{1f}' => 6,
            c => c.len_utf8(),
        })
        .sum::<usize>()
}
pub(super) fn item_wire_cost(item: &HistoryItem) -> usize {
    wire_cost(&item.text) + item.attachment.as_ref().map_or(0, attachment_wire_cost)
}
pub(super) fn attachment_wire_cost(
    a: &crate::encryption::attachment_payload::AttachmentSummary,
) -> usize {
    // Extra field names, kind, size and optional metadata policy.
    256 + [&a.filename, &a.mime_type, &a.caption]
        .into_iter()
        .map(|value| wire_cost(value) - 1024)
        .sum::<usize>()
}
fn history_page(
    tx: &mut ProtectedTransaction<'_>,
    own: &DeviceRef,
    store: &StoreScope,
    prefix: &str,
    before: Option<&str>,
) -> CryptoResult<(Vec<HistoryItem>, Option<String>)> {
    let rows = tx.record_page_limited(
        &own.generation_id,
        RecordKind::Control,
        prefix,
        before,
        true,
        (ITEMS + 1, 512),
    )?;
    let mut more = rows.len() > ITEMS;
    let mut items = Vec::new();
    let mut cost = 0;
    let mut last = None;
    for (id, bytes) in rows.into_iter().take(ITEMS) {
        let item = history::item(tx, own, store, &id, &bytes)?;
        let size = item_wire_cost(&item);
        if size > HISTORY_WIRE_BUDGET {
            return Err(CryptoError::Invalid);
        }
        if cost + size > HISTORY_WIRE_BUDGET {
            more = true;
            break;
        }
        cost += size;
        items.push(item);
        last = Some(id);
    }
    Ok((
        items,
        if more {
            Some(last.ok_or(CryptoError::Invalid)?)
        } else {
            None
        },
    ))
}
impl Engine {
    pub(in crate::encryption::signal_state) fn read_views(
        &mut self,
        instance: uuid::Uuid,
        query: ViewRequest,
    ) -> CryptoResult<ViewPage> {
        let own = &self.own;
        let store = self.store.scope().clone();
        self.store.transaction_with(|tx| match query {
            ViewRequest::Dialogs(c) => {
                let after = boundary(c, instance, own, &store, None)?;
                let rows = tx.record_page_limited(
                    &own.generation_id,
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
                    let chat = id
                        .strip_prefix("conversation:")
                        .ok_or(CryptoError::Invalid)?;
                    let c =
                        Conversation::decode(bytes.as_slice()).map_err(|_| CryptoError::Invalid)?;
                    conversation_lifecycle::validate(&c, own)?;
                    if c.conversation_id != chat {
                        return Err(CryptoError::Conflict);
                    }
                    items.push(dialog(c)?);
                    last = Some(id);
                }
                let next = if more {
                    Some(cursor(
                        instance,
                        own,
                        &store,
                        Position::Dialogs(last.ok_or(CryptoError::Invalid)?),
                    ))
                } else {
                    None
                };
                Ok(ViewPage::Dialogs(items, next))
            }
            ViewRequest::History(chat, c) => {
                history::require_ready(tx, own)?;
                conversation_lifecycle::load(tx, own, &chat)?
                    .ok_or(CryptoError::ConversationUnavailable)?;
                let before = boundary(c, instance, own, &store, Some(&chat))?;
                let (items, next) =
                    history_page(tx, own, &store, &history::prefix(&chat)?, before.as_deref())?;
                let next =
                    next.map(|last| cursor(instance, own, &store, Position::History(chat, last)));
                Ok(ViewPage::History(items, next))
            }
            ViewRequest::Statuses(chat, ids) => {
                history::require_ready(tx, own)?;
                if !epoch_rpc::uuid4(&chat)
                    || ids.is_empty()
                    || ids.len() > ITEMS
                    || ids.iter().any(|id| !epoch_rpc::uuid4(id))
                    || ids.iter().collect::<std::collections::HashSet<_>>().len() != ids.len()
                {
                    return Err(CryptoError::Invalid);
                }
                conversation_lifecycle::load(tx, own, &chat)?
                    .ok_or(CryptoError::ConversationUnavailable)?;
                let mut items = Vec::new();
                for uuid in ids {
                    let link = outgoing::load_link(tx, own, &uuid)?;
                    if outgoing::parse_id(&link.id)?.0 != chat || link.outgoing != Some(true) {
                        return Err(CryptoError::Conflict);
                    }
                    let (mut p, status) = outgoing::view_payload(tx, own, &store, &link.id)?;
                    if !crate::encryption::epoch_contract::user_message(&p) {
                        return Err(CryptoError::Conflict);
                    }
                    history::check(tx, own, &link.id, &p)?;
                    match p.content.take() {
                        Some(v2::encrypted_payload::Content::Text(t)) => {
                            drop(Zeroizing::new(t.text));
                        }
                        Some(v2::encrypted_payload::Content::Attachment(a)) => drop(a),
                        _ => return Err(CryptoError::Conflict),
                    }
                    items.push(MessageViewStatus {
                        message_id: uuid,
                        crypto_epoch: p.crypto_epoch.ok_or(CryptoError::Invalid)?,
                        delivery: status,
                    });
                }
                Ok(ViewPage::Statuses(items))
            }
            ViewRequest::PendingAttachments(chat, after) => {
                let after = match after {
                    None => None,
                    Some(c) if c.instance == instance && c.own == *own && c.store == store => {
                        match c.position {
                            Position::PendingAttachments(bound, after) if bound == chat => {
                                Some(after)
                            }
                            _ => return Err(CryptoError::Conflict),
                        }
                    }
                    Some(_) => return Err(CryptoError::Conflict),
                };
                let (items, next) =
                    attachment_prepare::pending_page(tx, own, &chat, after.as_deref())?;
                let next = next.map(|last| {
                    cursor(
                        instance,
                        own,
                        &store,
                        Position::PendingAttachments(chat, last),
                    )
                });
                Ok(ViewPage::PendingAttachments(items, next))
            }
            query => archives::read(tx, own, &store, instance, query),
        })
    }
}
impl CryptoCoordinator {
    pub async fn read_views(&self, query: ViewRequest) -> CryptoResult<ViewPage> {
        self.request(|r| Command::Epoch(EpochCommand::ReadViews(self.view_instance, query, r)))
            .await
    }
}

#[cfg(test)]
#[path = "epoch_views_tests.rs"]
mod tests;
