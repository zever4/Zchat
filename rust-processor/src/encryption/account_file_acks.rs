//! One FILEACK per background turn on the existing authenticated metadata lane.
//! Only bounded UUID/retry hints live here; protected journals remain the truth.
use super::*;
use crate::encryption::{native::RpcError, signal_state::EpochFileAckCursor};
use std::collections::{HashMap, VecDeque};

const RETRY_CAPACITY: usize = 64;
struct Retry {
    failures: u32,
    due: Instant,
}
pub(super) struct Scheduler {
    config: RunnerConfig,
    due: Instant,
    after: Option<EpochFileAckCursor>,
    queue: VecDeque<String>,
    retry: HashMap<String, Retry>,
    inflight: Option<String>,
}
pub(super) enum Step {
    Stopped,
    Complete { saved: bool, health: FileAckHealth },
}
fn stopped(stop: &watch::Receiver<bool>) -> bool {
    *stop.borrow() || stop.has_changed().is_err()
}
impl Scheduler {
    pub(super) fn new(config: RunnerConfig) -> Self {
        Self {
            config,
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
        let Some(retry) = self.retry.get_mut(id) else {
            return;
        };
        let max = self.config.delay(retry.failures).as_millis() as u64;
        retry.due =
            Instant::now() + Duration::from_millis(rand::random_range(max.div_ceil(2)..=max));
    }
    fn attempt(&mut self, id: &str) {
        if self.retry.len() == RETRY_CAPACITY && !self.retry.contains_key(id) {
            let oldest = self
                .retry
                .iter()
                .min_by_key(|(_, r)| r.due)
                .map(|(id, _)| id.clone())
                .unwrap();
            self.retry.remove(&oldest); // Hint eviction never deletes durable intent.
        }
        let r = self.retry.entry(id.to_owned()).or_insert(Retry {
            failures: 0,
            due: Instant::now(),
        });
        r.failures = r.failures.saturating_add(1);
        self.postpone(id);
        self.inflight = Some(id.to_owned()); // A timeout may follow remote COMMIT.
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
            Ok(result) => {
                self.inflight = None;
                result
            }
            Err(_) => {
                if let Some(id) = self.inflight.take() {
                    self.postpone(&id);
                }
                // A cancelled queued Prepare still owns the one file permit.
                // Join that worker command before allowing the next turn. Owner
                // stop can drop this await; independent cleanup joins the store.
                worker.join_attachment_work().await?;
                self.due = Instant::now() + self.config.poll_interval;
                Ok(Step::Complete {
                    saved: false,
                    health: FileAckHealth::TimedOut,
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
        if self.queue.is_empty() {
            let page = match w.pending_epoch_file_acks(self.after.clone()).await {
                Ok(p) => p,
                Err(CryptoError::Busy) => {
                    return Ok(Step::Complete {
                        saved: false,
                        health: FileAckHealth::Busy,
                    });
                }
                Err(e) => return Err(e.into()),
            };
            self.after = page.next;
            self.queue = page.messages.into();
            // Completed/incomplete rows can use the whole page. Continue on a
            // later bounded turn; an empty page is never global absence.
        }
        let mut earliest = None;
        let mut selected = None;
        for _ in 0..self.queue.len() {
            let id = self.queue.pop_front().unwrap();
            if let Some(retry) = self.retry.get(&id)
                && retry.due > Instant::now()
            {
                earliest = Some(earliest.map_or(retry.due, |at: Instant| at.min(retry.due)));
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
                saved: false,
                health: FileAckHealth::Waiting,
            });
        };
        self.attempt(&id);
        let outcome = async {
            if stopped(stop) {
                return Ok(Step::Stopped);
            }
            // Metadata status never grants ACK authority; it only skips a hint
            // already settled since the candidate page was fetched.
            let saved = match w.epoch_file_ack_status(id.clone()).await {
                Ok(at) => at,
                Err(CryptoError::Busy) => {
                    return Ok(Step::Complete {
                        saved: false,
                        health: FileAckHealth::Busy,
                    });
                }
                Err(e) => return Err(e.into()),
            };
            if saved.is_some() {
                self.retry.remove(&id);
                return Ok(Step::Complete {
                    saved: false,
                    health: FileAckHealth::Healthy,
                });
            }
            let proof = match w.epoch_file_ack(id.clone()).await {
                Ok(p) => p,
                Err(CryptoError::Busy) => {
                    return Ok(Step::Complete {
                        saved: false,
                        health: FileAckHealth::Busy,
                    });
                }
                Err(e) => return Err(e.into()),
            };
            if stopped(stop) {
                return Ok(Step::Stopped);
            }
            let observed = match t.acknowledge_file(&proof).await {
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
                        saved: false,
                        health: if matches!(e, RpcError::LocalDeadline) {
                            FileAckHealth::LocalDeadline
                        } else {
                            FileAckHealth::Retry
                        },
                    });
                }
            };
            if stopped(stop) {
                return Ok(Step::Stopped);
            }
            match w.accept_epoch_file_ack(observed).await {
                Ok(()) => {
                    self.retry.remove(&id);
                    Ok(Step::Complete {
                        saved: true,
                        health: FileAckHealth::Healthy,
                    })
                }
                Err(CryptoError::Busy) => Ok(Step::Complete {
                    saved: false,
                    health: FileAckHealth::Busy,
                }),
                Err(e) => Err(e.into()),
            }
        }
        .await;
        // Keep noncompleted IDs only as bounded backoff hints. Revisit them
        // through the authoritative page on wrap/restart, never forget intent.
        if self
            .queue
            .iter()
            .any(|id| self.retry.get(id).is_none_or(|r| r.due <= Instant::now()))
        {
            self.due = Instant::now();
        }
        outcome
    }
}
