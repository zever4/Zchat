//! Bounded read-only local pages. Only history replies receive a16KiB allowance;
//! normal metadata/status replies retain4KiB and the SAME8-slot admission pool.
use super::{DesktopE2ee, Reason};
use serde::Deserialize;
#[cfg(feature = "experimental-e2ee")]
use serde::Serialize;
use tauri::ipc::Response;
#[cfg(feature = "experimental-e2ee")]
#[path = "page_cursors.rs"]
mod cursors;
#[cfg(feature = "experimental-e2ee")]
pub use cursors::CursorStore;
#[cfg(feature = "experimental-e2ee")]
#[path = "archive_pages.rs"]
mod archives;
#[cfg(not(feature = "experimental-e2ee"))]
#[derive(Default)]
pub struct CursorStore {
    _disabled: (),
}
#[cfg(not(feature = "experimental-e2ee"))]
impl CursorStore {
    pub fn invalidate(&self, _context: u64) {}
}
#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum PageRequest {
    Dialogs {
        cursor: Option<String>,
    },
    History {
        conversation_id: String,
        cursor: Option<String>,
    },
    PendingAttachments {
        conversation_id: String,
        cursor: Option<String>,
    },
    Statuses {
        conversation_id: String,
        message_ids: Vec<String>,
    },
    Archives {
        cursor: Option<String>,
    },
    ArchiveDialogs {
        generation_id: String,
        cursor: Option<String>,
    },
    Segments {
        generation_id: String,
        conversation_id: String,
        cursor: Option<String>,
    },
    ArchiveHistory {
        generation_id: String,
        conversation_id: String,
        crypto_epoch: Option<String>,
        cursor: Option<String>,
    },
}
impl PageRequest {
    pub fn validate(&self) -> Result<(), Reason> {
        let valid = super::admission::valid_id;
        let check_cursor = |c: &Option<String>| c.as_deref().is_none_or(valid);
        let okay = match self {
            Self::Dialogs { cursor } => check_cursor(cursor),
            Self::Archives { cursor } => check_cursor(cursor),
            Self::ArchiveDialogs {
                generation_id,
                cursor,
            } => valid(generation_id) && check_cursor(cursor),
            Self::Segments {
                generation_id,
                conversation_id,
                cursor,
            } => valid(generation_id) && valid(conversation_id) && check_cursor(cursor),
            Self::ArchiveHistory {
                generation_id,
                conversation_id,
                crypto_epoch,
                cursor,
            } => {
                valid(generation_id)
                    && valid(conversation_id)
                    && check_cursor(cursor)
                    && crypto_epoch.as_deref().is_none_or(|e| {
                        e.len() <= 19
                            && e.parse::<i64>().is_ok_and(|v| v >= 0 && v.to_string() == e)
                    })
            }
            Self::History {
                conversation_id,
                cursor,
            }
            | Self::PendingAttachments {
                conversation_id,
                cursor,
            } => valid(conversation_id) && check_cursor(cursor),
            Self::Statuses {
                conversation_id,
                message_ids,
            } => {
                valid(conversation_id)
                    && !message_ids.is_empty()
                    && message_ids.len() <= 8
                    && message_ids.iter().all(|id| valid(id))
                    && message_ids
                        .iter()
                        .collect::<std::collections::HashSet<_>>()
                        .len()
                        == message_ids.len()
            }
        };
        if okay { Ok(()) } else { Err(Reason::Invalid) }
    }
    #[cfg(any(feature = "experimental-e2ee", test))]
    pub fn is_archive(&self) -> bool {
        matches!(
            self,
            Self::Archives { .. }
                | Self::ArchiveDialogs { .. }
                | Self::Segments { .. }
                | Self::ArchiveHistory { .. }
        )
    }
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Delivery {
    stored_on_server: bool,
    state: Option<String>,
    removal: Option<String>,
    sealed_unsent: bool,
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HistoryItem {
    message_id: String,
    conversation_id: String,
    crypto_epoch: String,
    created_at_unix_ms: String,
    sender_user_id: String,
    #[serde(serialize_with = "serialize_text")]
    text: zeroize::Zeroizing<String>,
    outgoing: bool,
    delivery: Option<Delivery>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attachment: Option<AttachmentMetadata>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_state: Option<&'static str>,
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AttachmentMetadata {
    kind: &'static str,
    #[serde(serialize_with = "serialize_text")]
    filename: zeroize::Zeroizing<String>,
    #[serde(serialize_with = "serialize_text")]
    caption: zeroize::Zeroizing<String>,
    #[serde(serialize_with = "serialize_text")]
    mime_type: zeroize::Zeroizing<String>,
    size: usize,
    photo_metadata_stripped: Option<bool>,
}
#[cfg(feature = "experimental-e2ee")]
impl TryFrom<rust_processor::encryption::attachment_payload::AttachmentSummary>
    for AttachmentMetadata
{
    type Error = Reason;
    fn try_from(
        value: rust_processor::encryption::attachment_payload::AttachmentSummary,
    ) -> Result<Self, Reason> {
        use rust_processor::proto::e2ee_v2::AttachmentKind;
        Ok(Self {
            kind: match value.kind {
                AttachmentKind::Photo => "photo",
                AttachmentKind::File => "file",
                AttachmentKind::Voice => "voice",
                _ => return Err(Reason::Runtime),
            },
            filename: value.filename,
            caption: value.caption,
            mime_type: value.mime_type,
            size: value.plaintext_len,
            photo_metadata_stripped: value.photo_metadata_stripped,
        })
    }
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PendingAttachment {
    message_id: String,
    crypto_epoch: String,
    created_at_unix_ms: String,
    attachment: AttachmentMetadata,
}
#[cfg(feature = "experimental-e2ee")]
pub(super) fn serialize_text<S: serde::Serializer>(
    v: &zeroize::Zeroizing<String>,
    s: S,
) -> Result<S::Ok, S::Error> {
    s.serialize_str(v)
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StatusItem {
    message_id: String,
    crypto_epoch: String,
    delivery: Delivery,
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum Page {
    Dialogs {
        items: Vec<super::dialogs::Dialog>,
        next_cursor: Option<String>,
    },
    History {
        items: Vec<HistoryItem>,
        next_cursor: Option<String>,
    },
    PendingAttachments {
        conversation_id: String,
        items: Vec<PendingAttachment>,
        next_cursor: Option<String>,
    },
    Statuses {
        items: Vec<StatusItem>,
        next_cursor: Option<String>,
    },
    Archives {
        items: Vec<archives::Generation>,
        next_cursor: Option<String>,
    },
    ArchiveDialogs {
        generation_id: String,
        items: Vec<super::dialogs::Dialog>,
        next_cursor: Option<String>,
    },
    Segments {
        generation_id: String,
        conversation_id: String,
        items: Vec<archives::Segment>,
        next_cursor: Option<String>,
    },
    ArchiveHistory {
        generation_id: String,
        conversation_id: String,
        crypto_epoch: Option<String>,
        items: Vec<HistoryItem>,
        next_cursor: Option<String>,
    },
}
#[cfg(feature = "experimental-e2ee")]
#[derive(Serialize)]
struct Reply {
    context: u64,
    page: Page,
}
#[cfg(feature = "experimental-e2ee")]
pub(crate) async fn execute(
    state: &DesktopE2ee,
    context: u64,
    request_id: String,
    input: PageRequest,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    input.validate()?;
    let archive = input.is_archive();
    state.manager.ensure_views_ready(context, archive)?;
    let history = matches!(
        &input,
        PageRequest::History { .. }
            | PageRequest::ArchiveHistory { .. }
            | PageRequest::PendingAttachments { .. }
    );
    let guard = state.admission.begin(context, request_id)?;
    let page = tokio::select! {
        biased;
        _=guard.cancel.cancelled()=>return Err(Reason::Stale),
        r=tokio::time::timeout(std::time::Duration::from_secs(30),read(state,context,input))=>r.map_err(|_|Reason::NativeWait)??,
    };
    check_page()?;
    state.manager.ensure_views_ready(context, archive)?;
    let reply = Reply { context, page };
    if history {
        guard.deliver_history(context, &reply)
    } else {
        guard.deliver(context, &reply)
    }
}
#[cfg(not(feature = "experimental-e2ee"))]
pub(crate) async fn execute(
    state: &DesktopE2ee,
    context: u64,
    _request_id: String,
    input: PageRequest,
    check_page: impl Fn() -> Result<(), Reason>,
) -> Result<Response, Reason> {
    check_page()?;
    input.validate()?;
    state.manager.ensure_dialog_ready(context)?;
    Err(Reason::NotReady)
}
#[cfg(feature = "experimental-e2ee")]
fn delivery(
    v: rust_processor::encryption::signal_state::MessageProgress,
) -> Result<Delivery, Reason> {
    use rust_processor::proto::e2ee::MessageState;
    let state = |v| match v {
        MessageState::Pending => Ok("pending".into()),
        MessageState::RecipientStored => Ok("recipient_stored".into()),
        MessageState::RecipientRejected => Ok("recipient_rejected".into()),
        MessageState::Expired => Ok("expired".into()),
        MessageState::RecipientReplaced => Ok("recipient_replaced".into()),
        _ => Err(Reason::Runtime),
    };
    Ok(Delivery {
        stored_on_server: v.stored_on_server,
        state: v.state.map(state).transpose()?,
        removal: v.removal.map(state).transpose()?,
        sealed_unsent: v.sealed_unsent,
    })
}
#[cfg(feature = "experimental-e2ee")]
async fn read(state: &DesktopE2ee, context: u64, input: PageRequest) -> Result<Page, Reason> {
    use rust_processor::encryption::signal_state::{ViewPage, ViewRequest};
    if input.is_archive() {
        return archives::read(state, context, input).await;
    }
    let cursor = state.cursors.resolve(context, &input)?;
    let query = match &input {
        PageRequest::Dialogs { .. } => ViewRequest::Dialogs(cursor),
        PageRequest::History {
            conversation_id, ..
        } => ViewRequest::History(conversation_id.clone(), cursor),
        PageRequest::PendingAttachments {
            conversation_id, ..
        } => ViewRequest::PendingAttachments(conversation_id.clone(), cursor),
        PageRequest::Statuses {
            conversation_id,
            message_ids,
        } => ViewRequest::Statuses(conversation_id.clone(), message_ids.clone()),
        _ => return Err(Reason::Invalid),
    };
    let result = state.manager.read_views(context, query).await?;
    match (input, result) {
        (PageRequest::Dialogs { .. }, ViewPage::Dialogs(items, next)) => {
            let items = dialog_items(items)?;
            Ok(Page::Dialogs {
                items,
                next_cursor: state.cursors.save(context, cursors::Kind::Dialogs, next)?,
            })
        }
        (
            PageRequest::History {
                conversation_id, ..
            },
            ViewPage::History(items, next),
        ) => {
            let items = history_items(items)?;
            Ok(Page::History {
                items,
                next_cursor: state.cursors.save(
                    context,
                    cursors::Kind::History(conversation_id),
                    next,
                )?,
            })
        }
        (
            PageRequest::PendingAttachments {
                conversation_id, ..
            },
            ViewPage::PendingAttachments(items, next),
        ) => {
            let items = items
                .into_iter()
                .map(|v| {
                    Ok(PendingAttachment {
                        message_id: v.message_id,
                        crypto_epoch: v.crypto_epoch.to_string(),
                        created_at_unix_ms: v.created_at_unix_ms.to_string(),
                        attachment: v.attachment.try_into()?,
                    })
                })
                .collect::<Result<_, Reason>>()?;
            let next_cursor = state.cursors.save(
                context,
                cursors::Kind::PendingAttachments(conversation_id.clone()),
                next,
            )?;
            Ok(Page::PendingAttachments {
                conversation_id,
                items,
                next_cursor,
            })
        }
        (PageRequest::Statuses { .. }, ViewPage::Statuses(items)) => Ok(Page::Statuses {
            items: items
                .into_iter()
                .map(|v| {
                    Ok(StatusItem {
                        message_id: v.message_id,
                        crypto_epoch: v.crypto_epoch.to_string(),
                        delivery: delivery(v.delivery)?,
                    })
                })
                .collect::<Result<_, Reason>>()?,
            next_cursor: None,
        }),
        _ => Err(Reason::Runtime),
    }
}
#[cfg(feature = "experimental-e2ee")]
fn dialog_items(
    items: Vec<rust_processor::encryption::signal_state::ViewDialog>,
) -> Result<Vec<super::dialogs::Dialog>, Reason> {
    use rust_processor::proto::e2ee::{ConversationCloseReason, ConversationState};
    items
        .into_iter()
        .map(|v| {
            Ok(super::dialogs::Dialog {
                conversation_id: v.conversation_id,
                peer_user_id: v.peer_user_id,
                state: match v.state {
                    ConversationState::Active => "active",
                    ConversationState::Closed => "closed",
                    _ => return Err(Reason::Runtime),
                }
                .into(),
                close_reason: match v.close_reason {
                    None => None,
                    Some(ConversationCloseReason::DeviceReplaced) => Some("device_replaced".into()),
                    _ => return Err(Reason::Runtime),
                },
                closed_at_unix_ms: v.closed_at_unix_ms.to_string(),
            })
        })
        .collect::<Result<_, Reason>>()
}
#[cfg(feature = "experimental-e2ee")]
fn history_items(
    items: Vec<rust_processor::encryption::signal_state::HistoryItem>,
) -> Result<Vec<HistoryItem>, Reason> {
    use rust_processor::encryption::signal_state::AttachmentCacheState;
    items
        .into_iter()
        .map(|v| {
            Ok(HistoryItem {
                message_id: v.message_id,
                conversation_id: v.conversation_id,
                crypto_epoch: v.crypto_epoch.to_string(),
                created_at_unix_ms: v.created_at_unix_ms.to_string(),
                sender_user_id: v.sender_user_id,
                text: v.text,
                outgoing: v.delivery.is_some(),
                delivery: v.delivery.map(delivery).transpose()?,
                attachment: v.attachment.map(AttachmentMetadata::try_from).transpose()?,
                cache_state: v.cache_state.map(|state| match state {
                    AttachmentCacheState::Missing => "missing",
                    AttachmentCacheState::Saved => "saved",
                    AttachmentCacheState::Removed => "removed",
                }),
            })
        })
        .collect::<Result<_, Reason>>()
}
#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "experimental-e2ee")]
    #[test]
    fn attachment_projection_fits_json_budget_and_excludes_blob_identity() {
        use rust_processor::{
            encryption::{
                attachment_payload::AttachmentSummary, signal_state::HistoryItem as CoreItem,
            },
            proto::e2ee_v2::AttachmentKind,
        };
        use zeroize::Zeroizing;
        let summary = AttachmentSummary {
            blob_id: "private-blob-id-not-for-renderer".into(),
            kind: AttachmentKind::Photo,
            filename: Zeroizing::new("a".repeat(255)),
            mime_type: Zeroizing::new("image/png".into()),
            caption: Zeroizing::new("\u{0001}".repeat(2000)),
            plaintext_len: 1024,
            photo_metadata_stripped: Some(false),
        };
        let id = "11111111-1111-4111-8111-111111111111";
        let items = history_items(vec![CoreItem {
            message_id: id.into(),
            conversation_id: id.into(),
            crypto_epoch: 0,
            created_at_unix_ms: 1_800_000_000_000,
            sender_user_id: id.into(),
            text: Zeroizing::new("Зашифрованное вложение".into()),
            attachment: Some(summary),
            cache_state: Some(
                rust_processor::encryption::signal_state::AttachmentCacheState::Saved,
            ),
            delivery: None,
        }])
        .unwrap();
        let serialized = serde_json::to_string(&Reply {
            context: 2,
            page: Page::History {
                items,
                next_cursor: Some(id.into()),
            },
        })
        .unwrap();
        assert!(serialized.len() < super::super::admission::MAX_HISTORY_REPLY - 64);
        assert!(!serialized.contains("private-blob-id"));
        let reply: serde_json::Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(reply["page"]["items"][0]["cacheState"], "saved");
        let metadata = &reply["page"]["items"][0]["attachment"];
        assert_eq!(metadata.as_object().unwrap().len(), 6);
        assert_eq!(metadata["caption"].as_str().unwrap().len(), 2000);
        assert_eq!(metadata["photoMetadataStripped"], false);
    }
    #[test]
    fn input_limits_ids_and_unknown_scope_fields_fail_before_reading() {
        let id = "11111111-1111-4111-8111-111111111111";
        for value in [
            serde_json::json!({"kind":"dialogs","cursor":"forged"}),
            serde_json::json!({"kind":"history","conversationId":id,"scope":"forged"}),
            serde_json::json!({"kind":"statuses","conversationId":id,"messageIds":[id,id]}),
        ] {
            assert!(match serde_json::from_value::<PageRequest>(value) {
                Ok(v) => v.validate().is_err(),
                Err(_) => true,
            });
        }
        assert!(
            serde_json::from_value::<PageRequest>(
                serde_json::json!({"kind":"history","conversationId":id,"cursor":null})
            )
            .unwrap()
            .validate()
            .is_ok()
        );
    }
    #[test]
    fn archive_inputs_bind_canonical_generation_chat_epoch_and_reject_extra_authority() {
        let id = "11111111-1111-4111-8111-111111111111";
        for epoch in ["0", "9223372036854775807"] {
            let v:PageRequest=serde_json::from_value(serde_json::json!({"kind":"archive_history","generationId":id,"conversationId":id,"cryptoEpoch":epoch,"cursor":null})).unwrap();
            assert!(v.is_archive() && v.validate().is_ok());
        }
        for epoch in ["", "00", "-0", "-1", "+1", "1.0", "9223372036854775808"] {
            let v:PageRequest=serde_json::from_value(serde_json::json!({"kind":"archive_history","generationId":id,"conversationId":id,"cryptoEpoch":epoch})).unwrap();
            assert!(v.validate().is_err());
        }
        for v in [
            serde_json::json!({"kind":"archives","accountId":id}),
            serde_json::json!({"kind":"archive_dialogs","generationId":"forged"}),
            serde_json::json!({"kind":"segments","generationId":id,"conversationId":"forged"}),
            serde_json::json!({"kind":"archive_history","generationId":id,"conversationId":id,"cryptoEpoch":0}),
            serde_json::json!({"kind":"archive_history","generationId":id,"conversationId":id,"scope":"forged"}),
        ] {
            assert!(match serde_json::from_value::<PageRequest>(v) {
                Ok(v) => v.validate().is_err(),
                Err(_) => true,
            });
        }
    }
}
