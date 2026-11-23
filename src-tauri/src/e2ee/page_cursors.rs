//! Only positions, no plaintext/keys. Opaque tokens expire and are bounded by
//! LRU. A stale context cannot evict cursors belonging to a newer context.
use super::{super::Reason, PageRequest};
use rust_processor::encryption::signal_state::ViewCursor;
use std::{
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
const CAPACITY: usize = 32;
const TTL: Duration = Duration::from_secs(120);
#[derive(Clone, PartialEq, Eq)]
pub(super) enum Kind {
    Dialogs,
    History(String),
    PendingAttachments(String),
    Archives,
    ArchiveDialogs(String),
    Segments(String, String),
    ArchiveHistory(String, String, Option<String>),
}
struct Lease<T> {
    kind: Kind,
    cursor: T,
    used: Instant,
}
struct State<T> {
    context: u64,
    entries: HashMap<String, Lease<T>>,
}
pub struct Tokens<T>(Mutex<State<T>>);
pub type CursorStore = Tokens<ViewCursor>;
impl<T> Default for Tokens<T> {
    fn default() -> Self {
        Self(Mutex::new(State {
            context: 0,
            entries: HashMap::new(),
        }))
    }
}
impl<T> State<T> {
    fn bind(&mut self, context: u64) -> Result<(), Reason> {
        if context < self.context {
            return Err(Reason::Stale);
        }
        if context > self.context {
            self.entries.clear();
            self.context = context;
        }
        self.entries.retain(|_, v| v.used.elapsed() < TTL);
        Ok(())
    }
}
impl<T: Clone + PartialEq> Tokens<T> {
    pub fn invalidate(&self, context: u64) {
        let _ = self.0.lock().expect("page cursors").bind(context);
    }
    pub(super) fn resolve(&self, context: u64, input: &PageRequest) -> Result<Option<T>, Reason> {
        let (kind, token) = match input {
            PageRequest::Dialogs { cursor } => (Kind::Dialogs, cursor),
            PageRequest::History {
                conversation_id,
                cursor,
            } => (Kind::History(conversation_id.clone()), cursor),
            PageRequest::PendingAttachments {
                conversation_id,
                cursor,
            } => (Kind::PendingAttachments(conversation_id.clone()), cursor),
            PageRequest::Statuses { .. } => return Ok(None),
            PageRequest::Archives { cursor } => (Kind::Archives, cursor),
            PageRequest::ArchiveDialogs {
                generation_id,
                cursor,
            } => (Kind::ArchiveDialogs(generation_id.clone()), cursor),
            PageRequest::Segments {
                generation_id,
                conversation_id,
                cursor,
            } => (
                Kind::Segments(generation_id.clone(), conversation_id.clone()),
                cursor,
            ),
            PageRequest::ArchiveHistory {
                generation_id,
                conversation_id,
                crypto_epoch,
                cursor,
            } => (
                Kind::ArchiveHistory(
                    generation_id.clone(),
                    conversation_id.clone(),
                    crypto_epoch.clone(),
                ),
                cursor,
            ),
        };
        let mut s = self.0.lock().expect("page cursors");
        s.bind(context)?;
        let Some(token) = token else {
            return Ok(None);
        };
        let v = s.entries.get_mut(token).ok_or(Reason::Stale)?;
        if v.kind != kind {
            return Err(Reason::Invalid);
        }
        v.used = Instant::now();
        Ok(Some(v.cursor.clone()))
    }
    pub(super) fn save(
        &self,
        context: u64,
        kind: Kind,
        cursor: Option<T>,
    ) -> Result<Option<String>, Reason> {
        let mut s = self.0.lock().expect("page cursors");
        s.bind(context)?;
        let Some(cursor) = cursor else {
            return Ok(None);
        };
        if let Some((token, v)) = s
            .entries
            .iter_mut()
            .find(|(_, v)| v.kind == kind && v.cursor == cursor)
        {
            v.used = Instant::now();
            return Ok(Some(token.clone()));
        }
        if s.entries.len() == CAPACITY {
            let oldest = s
                .entries
                .iter()
                .min_by_key(|(_, v)| v.used)
                .map(|(k, _)| k.clone())
                .ok_or(Reason::Runtime)?;
            s.entries.remove(&oldest);
        }
        let token = uuid::Uuid::new_v4().to_string();
        s.entries.insert(
            token.clone(),
            Lease {
                kind,
                cursor,
                used: Instant::now(),
            },
        );
        Ok(Some(token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(token: &str) -> PageRequest {
        PageRequest::Dialogs {
            cursor: Some(token.into()),
        }
    }
    #[test]
    fn repeated_position_keeps_token_kind_and_newer_context_cannot_be_evicted_by_stale_requests() {
        let r = Tokens::<u64>::default();
        let token = r.save(3, Kind::Dialogs, Some(7)).unwrap().unwrap();
        assert_eq!(
            r.save(3, Kind::Dialogs, Some(7)).unwrap(),
            Some(token.clone())
        );
        assert_eq!(r.resolve(3, &request(&token)).unwrap(), Some(7));
        assert_eq!(
            r.resolve(
                3,
                &PageRequest::History {
                    conversation_id: "other".into(),
                    cursor: Some(token.clone())
                }
            ),
            Err(Reason::Invalid)
        );
        let new = r.save(4, Kind::Dialogs, Some(8)).unwrap().unwrap();
        assert_eq!(r.resolve(3, &request(&token)), Err(Reason::Stale));
        assert_eq!(r.save(3, Kind::Dialogs, Some(9)), Err(Reason::Stale));
        assert_eq!(r.resolve(4, &request(&new)).unwrap(), Some(8));
        assert_eq!(r.resolve(4, &request(&token)), Err(Reason::Stale));
    }
    #[test]
    fn expiry_and_lru_are_bounded_and_never_return_another_position() {
        let r = Tokens::<u64>::default();
        let first = r.save(1, Kind::Dialogs, Some(0)).unwrap().unwrap();
        r.0.lock().unwrap().entries.get_mut(&first).unwrap().used =
            Instant::now() - Duration::from_secs(1);
        for i in 1..=CAPACITY {
            r.save(1, Kind::Dialogs, Some(i as u64)).unwrap();
        }
        assert_eq!(r.0.lock().unwrap().entries.len(), CAPACITY);
        assert_eq!(r.resolve(1, &request(&first)), Err(Reason::Stale));
        let token = r.save(1, Kind::Dialogs, Some(50)).unwrap().unwrap();
        r.0.lock().unwrap().entries.get_mut(&token).unwrap().used =
            Instant::now() - TTL - Duration::from_secs(1);
        assert_eq!(r.resolve(1, &request(&token)), Err(Reason::Stale));
        assert_eq!(r.0.lock().unwrap().entries.len(), CAPACITY - 1);
    }
    #[test]
    fn archive_cursor_cannot_cross_generation_chat_epoch_query_or_auth_context() {
        let r = Tokens::<u64>::default();
        let kind = Kind::ArchiveHistory("g".into(), "c".into(), Some("0".into()));
        let token = r.save(3, kind, Some(7)).unwrap().unwrap();
        let query = |g: &str, c: &str, e: Option<&str>| PageRequest::ArchiveHistory {
            generation_id: g.into(),
            conversation_id: c.into(),
            crypto_epoch: e.map(str::to_owned),
            cursor: Some(token.clone()),
        };
        assert_eq!(r.resolve(3, &query("g", "c", Some("0"))).unwrap(), Some(7));
        for q in [
            query("other", "c", Some("0")),
            query("g", "other", Some("0")),
            query("g", "c", None),
            query("g", "c", Some("1")),
            PageRequest::Archives {
                cursor: Some(token.clone()),
            },
            PageRequest::History {
                conversation_id: "c".into(),
                cursor: Some(token.clone()),
            },
        ] {
            assert_eq!(r.resolve(3, &q), Err(Reason::Invalid));
        }
        r.invalidate(4);
        assert_eq!(
            r.resolve(4, &query("g", "c", Some("0"))),
            Err(Reason::Stale)
        );
    }
}
