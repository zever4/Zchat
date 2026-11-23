//! Optional authenticated hint stream. No messages/ACKs or protected store writes.
//! Dropping this future drops its stream; it never spawns a task.
use super::*;
use crate::proto::e2ee::StreamMessagesRequest;
use std::sync::Arc;
use tokio::sync::{Notify, watch};

pub struct InboxWakeups {
    client: MessengerClient<Channel>,
    context: ClientContext,
    jwt: Zeroizing<String>,
    credential: Zeroizing<[u8; 32]>,
}
impl InboxWakeups {
    pub(super) fn new(native: &NativeV1Client) -> Result<Self> {
        if native.store_scope.is_none() {
            return Err(RpcError::Invalid);
        }
        Ok(Self {
            client: native.client.clone().max_decoding_message_size(64),
            context: native.context()?,
            jwt: Zeroizing::new(native.jwt.to_string()),
            credential: Zeroizing::new(**native.credential.as_ref().ok_or(RpcError::Invalid)?),
        })
    }
    async fn listen(&mut self, hints: &Notify, failures: &mut u32) -> Result<()> {
        let mut request = authenticated_request(
            StreamMessagesRequest {
                context: Some(self.context.clone()),
            },
            &self.jwt,
            Some(&self.credential),
        )?;
        // Header establishment still has the channel's10s timeout. The stream is
        // explicitly bounded; the server rotates it after50s even for slow peers.
        request.set_timeout(Duration::from_secs(60));
        let mut stream = self
            .client
            .watch_inbox(request)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        loop {
            let hint = tokio::time::timeout(Duration::from_secs(20), stream.message())
                .await
                .map_err(|_| RpcError::Transport)?
                .map_err(RpcError::from_status)?
                .ok_or(RpcError::Transport)?;
            if hint.protocol_version != 1 {
                return Err(RpcError::Invalid);
            }
            *failures = 0;
            hints.notify_one(); // A single permit coalesces arbitrary duplicates.
        }
    }
    /// All hint failures fall back to the independent poll lane. Authentication
    /// reconciliation is decided by durable RPCs, never by a lossy notification.
    pub async fn run(&mut self, hints: Arc<Notify>, mut stop: watch::Receiver<bool>) {
        let mut failures = 0u32;
        loop {
            tokio::select! {
                biased;
                _ = super::super::delivery_runner::cancelled(&mut stop) => return,
                _ = self.listen(&hints, &mut failures) => {},
            }
            failures = failures.saturating_add(1);
            let max = (1u64 << failures.saturating_sub(1).min(5)).min(30) * 1000;
            let delay = Duration::from_millis(rand::random_range(max.div_ceil(2)..=max));
            tokio::select! {
                biased;
                _ = super::super::delivery_runner::cancelled(&mut stop) => return,
                _ = tokio::time::sleep(delay) => {},
            }
        }
    }
}
