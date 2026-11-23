use super::{Phase, Reason, Snapshot};
use std::sync::Mutex;

pub struct SessionManager(Mutex<(u64, bool)>);
impl SessionManager {
    pub fn new() -> Self {
        Self(Mutex::new((1, false)))
    }
    pub fn context(&self) -> u64 {
        self.0.lock().expect("E2EE disabled context").0
    }
    pub fn snapshot(&self, context: u64) -> Result<Snapshot, Reason> {
        let s = self.0.lock().expect("E2EE disabled context");
        if context != s.0 {
            return Err(Reason::Stale);
        }
        Ok(Snapshot::new(
            context,
            if s.1 { Phase::Closed } else { Phase::Disabled },
            None,
        ))
    }
    pub(super) fn ensure_dialog_ready(&self, context: u64) -> Result<(), Reason> {
        self.snapshot(context)?;
        Err(Reason::NotReady)
    }
    pub(super) async fn dialog(
        &self,
        context: u64,
        _request: super::dialogs::DialogRequest,
    ) -> Result<super::dialogs::Dialog, Reason> {
        self.ensure_dialog_ready(context)?;
        Err(Reason::NotReady)
    }
    pub(super) async fn recovery(
        &self,
        context: u64,
        _operation: super::recovery::RecoveryRequest,
    ) -> Result<super::recovery::RecoveryReply, Reason> {
        self.ensure_dialog_ready(context)?;
        Err(Reason::NotReady)
    }
    pub(super) async fn send_text(
        &self,
        context: u64,
        _request: super::text::TextRequest,
    ) -> Result<super::text::TextQueued, Reason> {
        self.ensure_dialog_ready(context)?;
        Err(Reason::NotReady)
    }
    pub fn bind(
        &self,
        expected: u64,
        _origin: String,
        _jwt: Option<String>,
    ) -> Result<u64, Reason> {
        let mut s = self.0.lock().expect("E2EE disabled context");
        if s.1 {
            return Err(Reason::Closed);
        }
        if s.0 != expected {
            return Err(Reason::Stale);
        }
        s.0 =
            s.0.checked_add(1)
                .filter(|n| *n <= 9_007_199_254_740_991)
                .ok_or(Reason::Runtime)?;
        Ok(s.0)
    }
    pub fn snapshot_for_binding(&self, context: u64) -> Result<Snapshot, Reason> {
        self.snapshot(context)
    }
    pub fn retire(&self, context: u64) -> Result<u64, Reason> {
        self.bind(context, String::new(), None)
    }
    pub fn setup(&self, context: u64) -> Result<u64, Reason> {
        self.snapshot(context)?;
        Err(Reason::Invalid)
    }
    pub fn replace(
        &self,
        context: u64,
        _password: zeroize::Zeroizing<String>,
        _confirmed: bool,
    ) -> Result<u64, Reason> {
        self.snapshot(context)?;
        Err(Reason::Invalid)
    }
    pub fn retry(&self, context: u64) -> Result<u64, Reason> {
        self.snapshot(context)?;
        Err(Reason::Invalid)
    }
    pub fn clear(&self) -> u64 {
        let mut s = self.0.lock().expect("E2EE disabled context");
        if let Some(n) = s.0.checked_add(1).filter(|n| *n <= 9_007_199_254_740_991) {
            s.0 = n;
        } else {
            s.1 = true;
        }
        s.0
    }
    pub async fn wait_released(&self, context: u64) -> Result<Snapshot, Reason> {
        self.snapshot(context)
    }
    pub async fn shutdown(&self) {
        self.clear();
        self.0.lock().expect("E2EE disabled context").1 = true;
    }
}
