//! Exact archive query/response binding; metadata4KiB, history16KiB and leases
//! are enforced by the SAME pages::execute/admission path as current history.
use super::*;
use rust_processor::encryption::signal_state::{ViewPage, ViewRequest};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Generation {
    generation_id: String,
    replaced_by_generation_id: Option<String>,
    history_ready: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    crypto_epoch: String,
    sealed: bool,
    created_at_unix_ms: String,
    sealed_at_unix_ms: Option<String>,
}
pub(super) async fn read(
    state: &DesktopE2ee,
    context: u64,
    input: PageRequest,
) -> Result<Page, Reason> {
    let cursor = state.cursors.resolve(context, &input)?;
    let query = match &input {
        PageRequest::Archives { .. } => ViewRequest::Archives(cursor),
        PageRequest::ArchiveDialogs { generation_id, .. } => {
            ViewRequest::ArchiveDialogs(generation_id.clone(), cursor)
        }
        PageRequest::Segments {
            generation_id,
            conversation_id,
            ..
        } => ViewRequest::Segments(generation_id.clone(), conversation_id.clone(), cursor),
        PageRequest::ArchiveHistory {
            generation_id,
            conversation_id,
            crypto_epoch,
            ..
        } => ViewRequest::ArchiveHistory(
            generation_id.clone(),
            conversation_id.clone(),
            crypto_epoch
                .as_deref()
                .map(str::parse)
                .transpose()
                .map_err(|_| Reason::Invalid)?,
            cursor,
        ),
        _ => return Err(Reason::Invalid),
    };
    let result = state.manager.read_views(context, query).await?;
    let (page, kind, next) = match (input, result) {
        (PageRequest::Archives { .. }, ViewPage::Archives(items, next)) => (
            Page::Archives {
                items: items
                    .into_iter()
                    .map(|v| Generation {
                        generation_id: v.generation_id,
                        replaced_by_generation_id: v.replaced_by_generation_id,
                        history_ready: v.history_ready,
                    })
                    .collect(),
                next_cursor: None,
            },
            cursors::Kind::Archives,
            next,
        ),
        (PageRequest::ArchiveDialogs { generation_id, .. }, ViewPage::Dialogs(items, next)) => (
            Page::ArchiveDialogs {
                generation_id: generation_id.clone(),
                items: dialog_items(items)?,
                next_cursor: None,
            },
            cursors::Kind::ArchiveDialogs(generation_id),
            next,
        ),
        (
            PageRequest::Segments {
                generation_id,
                conversation_id,
                ..
            },
            ViewPage::Segments(items, next),
        ) => (
            Page::Segments {
                generation_id: generation_id.clone(),
                conversation_id: conversation_id.clone(),
                items: items
                    .into_iter()
                    .map(|v| Segment {
                        crypto_epoch: v.crypto_epoch.to_string(),
                        sealed: v.sealed,
                        created_at_unix_ms: v.created_at_unix_ms.to_string(),
                        sealed_at_unix_ms: v.sealed_at_unix_ms.map(|v| v.to_string()),
                    })
                    .collect(),
                next_cursor: None,
            },
            cursors::Kind::Segments(generation_id, conversation_id),
            next,
        ),
        (
            PageRequest::ArchiveHistory {
                generation_id,
                conversation_id,
                crypto_epoch,
                ..
            },
            ViewPage::History(items, next),
        ) => (
            Page::ArchiveHistory {
                generation_id: generation_id.clone(),
                conversation_id: conversation_id.clone(),
                crypto_epoch: crypto_epoch.clone(),
                items: history_items(items)?,
                next_cursor: None,
            },
            cursors::Kind::ArchiveHistory(generation_id, conversation_id, crypto_epoch),
            next,
        ),
        _ => return Err(Reason::Runtime),
    };
    let token = state.cursors.save(context, kind, next)?;
    let mut page = page;
    match &mut page {
        Page::Archives { next_cursor, .. }
        | Page::ArchiveDialogs { next_cursor, .. }
        | Page::Segments { next_cursor, .. }
        | Page::ArchiveHistory { next_cursor, .. } => *next_cursor = token,
        _ => return Err(Reason::Runtime),
    }
    Ok(page)
}
