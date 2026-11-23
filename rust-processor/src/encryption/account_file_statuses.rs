//! Independent sender-file polling on the ONE account metadata lane. IDs and
//! backoff are bounded hints; only protected journals contain durable facts.
use super::*;
use crate::encryption::{native::RpcError, signal_state::EpochFileStatusCursor};
use std::collections::{HashMap, VecDeque};

const RETRY_CAPACITY: usize = 64;
struct Retry {
    failures: u32,
    due: Instant,
}
pub(super) struct Scheduler {
    config: RunnerConfig,
    seeded: bool,
    due: Instant,
    after: Option<EpochFileStatusCursor>,
    queue: VecDeque<String>,
    retry: HashMap<String, Retry>,
    inflight: Option<String>,
}
pub(super) enum Step {
    Stopped,
    Complete {
        observed: bool,
        health: FileStatusHealth,
    },
}
fn stopped(stop: &watch::Receiver<bool>) -> bool {
    *stop.borrow() || stop.has_changed().is_err()
}
impl Scheduler {
    pub(super) fn new(config: RunnerConfig) -> Self {
        Self {
            config,
            seeded: false,
            due: Instant::now(),
            after: None,
            queue: VecDeque::new(),
            retry: HashMap::new(),
            inflight: None,
        }
    }
    pub(super) fn next_due(&self) -> Instant {
        self.due
    }
    fn postpone(&mut self, id: &str) {
        if let Some(r) = self.retry.get_mut(id) {
            let max = self.config.delay(r.failures).as_millis() as u64;
            r.due =
                Instant::now() + Duration::from_millis(rand::random_range(max.div_ceil(2)..=max));
        }
    }
    fn attempt(&mut self, id: &str) {
        if self.retry.len() == RETRY_CAPACITY && !self.retry.contains_key(id) {
            let oldest = self
                .retry
                .iter()
                .min_by_key(|(_, r)| r.due)
                .map(|(id, _)| id.clone())
                .unwrap();
            self.retry.remove(&oldest); // Never removes protected pending work.
        }
        let r = self.retry.entry(id.to_owned()).or_insert(Retry {
            failures: 0,
            due: Instant::now(),
        });
        r.failures = r.failures.saturating_add(1);
        self.postpone(id);
        self.inflight = Some(id.to_owned());
    }
    pub(super) async fn tick<T: EpochDeliveryTransport>(
        &mut self,
        worker: &CryptoCoordinator,
        transport: &mut T,
        stop: &mut watch::Receiver<bool>,
    ) -> Result<Step> {
        let work_stop = stop.clone();
        let timeout = self.config.round_timeout;
        let result = tokio::select! {
            biased;
            _=crate::encryption::delivery_runner::cancelled(stop)=>return Ok(Step::Stopped),
            r=tokio::time::timeout(timeout,self.once(worker,transport,&work_stop))=>r,
        };
        match result {
            Ok(r) => {
                self.inflight = None;
                r
            }
            Err(_) => {
                if let Some(id) = self.inflight.take() {
                    self.postpone(&id);
                }
                // Metadata only: no attachment permit/body was acquired. Do not
                // join unrelated HTTP/file work. Closed queued worker replies are
                // skipped; an already executing protected TX remains atomic.
                self.due = Instant::now() + self.config.poll_interval;
                Ok(Step::Complete {
                    observed: false,
                    health: FileStatusHealth::TimedOut,
                })
            }
        }
    }
    async fn once<T: EpochDeliveryTransport>(
        &mut self,
        w: &CryptoCoordinator,
        t: &mut T,
        stop: &watch::Receiver<bool>,
    ) -> Result<Step> {
        self.due = Instant::now() + self.config.poll_interval;
        if !self.seeded {
            // One resumable <=32-source-row TX per background turn, never a
            // startup scan/repair or a loop holding up normal account readiness.
            let page = match w.seed_epoch_file_status_index().await {
                Ok(p) => p,
                Err(CryptoError::Busy) => {
                    return Ok(Step::Complete {
                        observed: false,
                        health: FileStatusHealth::Busy,
                    });
                }
                Err(e) => return Err(e.into()),
            };
            self.seeded = page.ready;
            if !page.ready {
                self.due = Instant::now();
            }
            return Ok(Step::Complete {
                observed: false,
                health: FileStatusHealth::Initializing,
            });
        }
        if self.queue.is_empty() {
            let page = match w.pending_epoch_file_statuses(self.after.clone()).await {
                Ok(p) => p,
                Err(CryptoError::Busy) => {
                    return Ok(Step::Complete {
                        observed: false,
                        health: FileStatusHealth::Busy,
                    });
                }
                Err(e) => return Err(e.into()),
            };
            self.after = page.next;
            self.queue = page
                .files
                .into_iter()
                .map(|p| p.request().envelope.as_ref().unwrap().message_id.clone())
                .collect();
        }
        let mut earliest = None;
        let mut selected = None;
        for _ in 0..self.queue.len() {
            let id = self.queue.pop_front().unwrap();
            if let Some(r) = self.retry.get(&id)
                && r.due > Instant::now()
            {
                earliest = Some(earliest.map_or(r.due, |at: Instant| at.min(r.due)));
                self.queue.push_back(id);
            } else {
                selected = Some(id);
                break;
            }
        }
        let Some(id) = selected else {
            if let Some(at) = earliest {
                self.due = self.due.min(at);
            }
            return Ok(Step::Complete {
                observed: false,
                health: FileStatusHealth::Waiting,
            });
        };
        self.attempt(&id);
        let result = async {
            if stopped(stop) {
                return Ok(Step::Stopped);
            }
            let progress = match w.epoch_file_progress(id.clone()).await {
                Ok(p) => p,
                Err(CryptoError::Busy) => {
                    return Ok(Step::Complete {
                        observed: false,
                        health: FileStatusHealth::Busy,
                    });
                }
                Err(e) => return Err(e.into()),
            };
            if !progress.needs_poll() {
                self.retry.remove(&id);
                return Ok(Step::Complete {
                    observed: false,
                    health: FileStatusHealth::Healthy,
                });
            }
            // Reload current own closure/history/outbox, not a retained page
            // snapshot. Neither receipt nor a renderer UUID alone authorizes RPC.
            let plan = w.epoch_outgoing_plan(id.clone()).await?;
            if stopped(stop) {
                return Ok(Step::Stopped);
            }
            // One file per RPC avoids an opaque all-or-none NotFound for one
            // reference starving every other file in a batch. At most one RPC
            // per bounded turn; no per-file tasks/clients or protocol retry here.
            let observed = match t.file_statuses(std::slice::from_ref(&plan)).await {
                Ok(o) => o,
                Err(e) => {
                    if super::commands::fatal_rpc(&e)
                        || matches!(
                            &e,
                            RpcError::Status(tonic::Code::PermissionDenied)
                                | RpcError::Failure {
                                    code: tonic::Code::PermissionDenied,
                                    ..
                                }
                        )
                    {
                        return Err(e.into());
                    }
                    return Ok(Step::Complete {
                        observed: false,
                        health: if matches!(e, RpcError::LocalDeadline) {
                            FileStatusHealth::LocalDeadline
                        } else {
                            FileStatusHealth::Retry
                        },
                    });
                }
            };
            if stopped(stop) {
                return Ok(Step::Stopped);
            }
            let facts = match w.accept_epoch_file_statuses(observed).await {
                Ok(p) => p,
                Err(CryptoError::Busy) => {
                    return Ok(Step::Complete {
                        observed: false,
                        health: FileStatusHealth::Busy,
                    });
                }
                Err(e) => return Err(e.into()),
            };
            let [fact] = facts.as_slice() else {
                return Err(CryptoError::Invalid.into());
            };
            if fact.needs_poll() {
                // A successful Pending/Expired snapshot resets error backoff,
                // not its original receipt/TTL. Expiry still permits late ACK.
                self.retry.get_mut(&id).unwrap().failures = 0;
                self.postpone(&id);
            } else {
                self.retry.remove(&id);
            }
            Ok(Step::Complete {
                observed: true,
                health: FileStatusHealth::Healthy,
            })
        }
        .await;
        if self
            .queue
            .iter()
            .any(|id| self.retry.get(id).is_none_or(|r| r.due <= Instant::now()))
        {
            self.due = Instant::now();
        }
        result
    }
}
