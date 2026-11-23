//! Bound native invokes AND completed replies waiting for renderer acknowledgement.
//! This is not a limit on Tauri's own pre-deserialization/WebView allocations.
use super::Reason;
use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

const CAPACITY: usize = 8;
const MAX_REPLY: usize = 4096;
#[cfg(any(feature = "experimental-e2ee", test))]
pub(super) const MAX_HISTORY_REPLY: usize = 16 * 1024;
const REPLY_LEASE: Duration = Duration::from_secs(30);
#[derive(Default)]
pub struct Admission(Mutex<HashMap<String, Slot>>, AtomicU64);
struct Slot {
    lease: u64,
    context: u64,
    delivered: Option<Instant>,
    cancel: CancellationToken,
}
pub struct Guard<'a> {
    admission: &'a Admission,
    lease: u64,
    id: String,
    pub cancel: CancellationToken,
    delivered: bool,
}
impl Admission {
    pub fn begin(&self, context: u64, id: String) -> Result<Guard<'_>, Reason> {
        if !valid_id(&id) {
            return Err(Reason::Invalid);
        }
        let mut slots = self.0.lock().expect("E2EE admission lock");
        slots.retain(|_, slot| {
            let keep = slot.delivered.is_none_or(|at| at.elapsed() < REPLY_LEASE);
            if !keep {
                slot.cancel.cancel();
            }
            keep
        });
        if slots.len() >= CAPACITY || slots.contains_key(&id) {
            return Err(Reason::Busy);
        }
        let cancel = CancellationToken::new();
        let lease = self
            .1
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Reason::Runtime)?;
        slots.insert(
            id.clone(),
            Slot {
                lease,
                context,
                delivered: None,
                cancel: cancel.clone(),
            },
        );
        Ok(Guard {
            admission: self,
            lease,
            id,
            cancel,
            delivered: false,
        })
    }
    pub fn acknowledge(&self, context: u64, id: &str, reply_lease: &str) -> Result<(), Reason> {
        if reply_lease.len() > 20 {
            return Err(Reason::Invalid);
        }
        let lease = reply_lease.parse::<u64>().map_err(|_| Reason::Invalid)?;
        if lease.to_string() != reply_lease {
            return Err(Reason::Invalid);
        }
        if !valid_id(id) {
            return Err(Reason::Invalid);
        }
        let mut slots = self.0.lock().expect("E2EE admission lock");
        if slots
            .get(id)
            .is_some_and(|s| s.context == context && s.lease == lease && s.delivered.is_some())
        {
            slots.remove(id);
        }
        Ok(())
    }
    pub fn cancel(&self, context: u64, id: &str) -> Result<(), Reason> {
        if !valid_id(id) {
            return Err(Reason::Invalid);
        }
        if let Some(slot) = self.0.lock().expect("E2EE admission lock").get(id)
            && slot.context == context
        {
            slot.cancel.cancel();
        }
        Ok(())
    }
    pub fn invalidate(&self) {
        for (_, slot) in self.0.lock().expect("E2EE admission lock").drain() {
            slot.cancel.cancel();
        }
    }
}
impl Guard<'_> {
    pub fn deliver<T: serde::Serialize>(
        self,
        context: u64,
        value: &T,
    ) -> Result<tauri::ipc::Response, Reason> {
        self.deliver_bounded(context, value, MAX_REPLY)
    }
    #[cfg(any(feature = "experimental-e2ee", test))]
    pub(super) fn deliver_history<T: serde::Serialize>(
        self,
        context: u64,
        value: &T,
    ) -> Result<tauri::ipc::Response, Reason> {
        self.deliver_bounded(context, value, MAX_HISTORY_REPLY)
    }
    #[cfg(feature = "experimental-e2ee")]
    pub(super) fn deliver_preview<T: serde::Serialize>(
        self,
        context: u64,
        value: &T,
    ) -> Result<tauri::ipc::Response, Reason> {
        self.deliver_bounded(
            context,
            value,
            super::local_files::MAX_PNG.div_ceil(3) * 4 + 512,
        )
    }
    fn deliver_bounded<T: serde::Serialize>(
        mut self,
        context: u64,
        value: &T,
        limit: usize,
    ) -> Result<tauri::ipc::Response, Reason> {
        let bytes = self.reply_header(value, limit)?;
        self.record_delivery(context)?;
        Ok(tauri::ipc::Response::new(
            String::from_utf8(bytes).map_err(|_| Reason::Runtime)?,
        ))
    }
    #[cfg(feature = "experimental-e2ee")]
    pub(super) fn deliver_audio<T: serde::Serialize>(
        mut self,
        context: u64,
        value: &T,
        audio: &[u8],
    ) -> Result<tauri::ipc::Response, Reason> {
        if audio.is_empty()
            || audio.len() > rust_processor::encryption::attachment_payload::MAX_MEDIA_BYTES
        {
            return Err(Reason::VoiceSourceTooLarge);
        }
        let header = self.reply_header(value, 512)?;
        let mut frame = Vec::with_capacity(4 + header.len() + audio.len());
        frame.extend_from_slice(&(header.len() as u32).to_le_bytes());
        frame.extend_from_slice(&header);
        frame.extend_from_slice(audio);
        self.record_delivery(context)?;
        Ok(tauri::ipc::Response::new(frame))
    }
    fn reply_header<T: serde::Serialize>(
        &self,
        value: &T,
        limit: usize,
    ) -> Result<Vec<u8>, Reason> {
        #[derive(serde::Serialize)]
        #[serde(rename_all = "camelCase")]
        struct Reply<'a, T> {
            #[serde(flatten)]
            snapshot: &'a T,
            reply_lease: String,
        }
        let bytes = serde_json::to_vec(&Reply {
            snapshot: value,
            reply_lease: self.lease.to_string(),
        })
        .map_err(|_| Reason::Runtime)?;
        if bytes.len() > limit {
            return Err(Reason::Runtime);
        }
        Ok(bytes)
    }
    fn record_delivery(&mut self, context: u64) -> Result<(), Reason> {
        let mut slots = self.admission.0.lock().expect("E2EE admission lock");
        let slot = slots.get_mut(&self.id).ok_or(Reason::Stale)?;
        // An invalidated guard must never release or replace a reused ID's slot.
        if slot.lease != self.lease || self.cancel.is_cancelled() || slot.cancel.is_cancelled() {
            return Err(Reason::Stale);
        }
        slot.context = context;
        slot.delivered = Some(Instant::now());
        self.delivered = true;
        Ok(())
    }
}
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        if !self.delivered {
            let mut slots = self.admission.0.lock().expect("E2EE admission lock");
            if slots.get(&self.id).is_some_and(|s| s.lease == self.lease) {
                // IDs cannot be reused while this non-cancelled lease exists.
                slots.remove(&self.id);
            }
        }
    }
}
pub(super) fn valid_id(id: &str) -> bool {
    id.len() == 36
        && id.bytes().enumerate().all(|(i, b)| {
            if matches!(i, 8 | 13 | 18 | 23) {
                b == b'-'
            } else if i == 14 {
                b == b'4'
            } else if i == 19 {
                matches!(b, b'8' | b'9' | b'a' | b'b')
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(context: u64) -> super::super::Snapshot {
        #[cfg(feature = "experimental-e2ee")]
        let phase = super::super::Phase::Idle;
        #[cfg(not(feature = "experimental-e2ee"))]
        let phase = super::super::Phase::Disabled;
        super::super::Snapshot::new(context, phase, None)
    }
    fn id(n: usize) -> String {
        format!("00000000-0000-4000-8000-{n:012}")
    }
    #[test]
    fn completed_replies_hold_capacity_until_ack_and_drop_releases_unfinished() {
        let a = Admission::default();
        for n in 0..CAPACITY {
            a.begin(1, id(n)).unwrap().deliver(1, &snapshot(1)).unwrap();
        }
        assert!(matches!(a.begin(1, id(9)), Err(Reason::Busy)));
        a.acknowledge(2, &id(0), "0").unwrap();
        assert!(matches!(a.begin(1, id(9)), Err(Reason::Busy)));
        a.acknowledge(1, &id(0), "0").unwrap();
        drop(a.begin(1, id(9)).unwrap());
        assert_eq!(a.0.lock().unwrap().len(), 7);
        a.invalidate();
        assert!(a.0.lock().unwrap().is_empty());
    }
    #[test]
    fn invalid_and_duplicate_ids_and_premature_ack_are_rejected() {
        let a = Admission::default();
        assert!(matches!(a.begin(1, "x".into()), Err(Reason::Invalid)));
        let g = a.begin(1, id(0)).unwrap();
        a.acknowledge(1, &id(0), "0").unwrap();
        assert!(matches!(a.begin(1, id(0)), Err(Reason::Busy)));
        a.cancel(2, &id(0)).unwrap();
        assert!(!g.cancel.is_cancelled());
        a.cancel(1, &id(0)).unwrap();
        assert!(g.cancel.is_cancelled());
        drop(g);
    }
    #[test]
    fn invalidated_old_guard_cannot_remove_or_deliver_a_reused_request_id() {
        let a = Admission::default();
        let old = a.begin(1, id(0)).unwrap();
        a.invalidate();
        let current = a.begin(2, id(0)).unwrap();
        assert!(matches!(old.deliver(1, &snapshot(1)), Err(Reason::Stale)));
        assert_eq!(a.0.lock().unwrap().len(), 1);
        drop(current);
        assert!(a.0.lock().unwrap().is_empty());
    }
    #[test]
    fn reply_expiry_recovers_capacity_but_stale_admission_does_not_evict_current_replies() {
        let a = Admission::default();
        for n in 0..CAPACITY {
            a.begin(2, id(n)).unwrap().deliver(2, &snapshot(2)).unwrap();
        }
        assert!(matches!(a.begin(1, id(9)), Err(Reason::Busy)));
        a.0.lock().unwrap().get_mut(&id(0)).unwrap().delivered =
            Some(Instant::now() - REPLY_LEASE - Duration::from_secs(1));
        drop(a.begin(2, id(9)).unwrap());
        assert_eq!(a.0.lock().unwrap().len(), 7);
    }
    #[test]
    fn old_reply_ack_cannot_release_a_later_reply_with_the_same_id() {
        let a = Admission::default();
        a.begin(1, id(0)).unwrap().deliver(1, &snapshot(1)).unwrap();
        a.acknowledge(1, &id(0), "0").unwrap();
        a.begin(1, id(0)).unwrap().deliver(1, &snapshot(1)).unwrap();
        a.acknowledge(1, &id(0), "0").unwrap();
        assert_eq!(a.0.lock().unwrap().len(), 1);
        a.acknowledge(1, &id(0), "1").unwrap();
        assert!(a.0.lock().unwrap().is_empty());
    }
    #[test]
    fn oversized_reply_and_invalid_control_ids_release_or_reject_without_leaking_slots() {
        let a = Admission::default();
        let mut value = snapshot(1);
        value.runtime_phase = Some("x".repeat(MAX_REPLY));
        assert!(matches!(
            a.begin(1, id(0)).unwrap().deliver(1, &value),
            Err(Reason::Runtime)
        ));
        assert!(a.0.lock().unwrap().is_empty());
        assert_eq!(a.acknowledge(1, "bad", "0"), Err(Reason::Invalid));
        assert_eq!(a.cancel(1, "bad"), Err(Reason::Invalid));
    }
    #[test]
    fn history_has_separate_byte_limit_but_shares_the_normal_reply_capacity() {
        let a = Admission::default();
        let text = serde_json::json!({"text":"\u{0001}".repeat(2000)});
        assert!(matches!(
            a.begin(1, id(0)).unwrap().deliver(1, &text),
            Err(Reason::Runtime)
        ));
        a.begin(1, id(0))
            .unwrap()
            .deliver_history(1, &text)
            .unwrap();
        for n in 1..CAPACITY {
            a.begin(1, id(n)).unwrap().deliver(1, &snapshot(1)).unwrap();
        }
        assert!(matches!(a.begin(1, id(9)), Err(Reason::Busy)));
        a.invalidate();
        let too_large = serde_json::json!({"text":"\u{0001}".repeat(3000)});
        assert!(matches!(
            a.begin(1, id(0)).unwrap().deliver_history(1, &too_large),
            Err(Reason::Runtime)
        ));
        assert!(a.0.lock().unwrap().is_empty());
    }

    #[cfg(feature = "experimental-e2ee")]
    #[test]
    fn voice_reply_frame_keeps_exact_audio_bytes_and_original_admission_lease() {
        use tauri::ipc::{InvokeResponseBody, IpcResponse};
        let admission = Admission::default();
        let id = "11111111-1111-4111-8111-111111111111";
        let audio = [0, 255, 14];
        let reply = admission
            .begin(2, id.into())
            .unwrap()
            .deliver_audio(
                2,
                &serde_json::json!({"context":2,"result":{"kind":"voice","mimeType":"audio/webm"}}),
                &audio,
            )
            .unwrap();
        let InvokeResponseBody::Raw(bytes) = reply.body().unwrap() else {
            panic!("expected binary response");
        };
        let offset = 4 + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(bytes[offset..], audio);
        let metadata: serde_json::Value = serde_json::from_slice(&bytes[4..offset]).unwrap();
        assert_eq!(metadata["context"], 2);
        assert!(matches!(admission.begin(2, id.into()), Err(Reason::Busy)));
        admission
            .acknowledge(2, id, metadata["replyLease"].as_str().unwrap())
            .unwrap();
        assert!(admission.begin(2, id.into()).is_ok());
    }
}
