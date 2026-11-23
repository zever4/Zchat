//! Read-only discovery and replay of durable EXPLICIT recovery commands. No
//! prepare-proposal/prepare-decision call exists anywhere in the runner.
use super::*;
impl<T: EpochDeliveryTransport> EpochDeliveryRunner<T> {
    pub(super) async fn recover(
        &mut self,
        w: &CryptoCoordinator,
        r: &mut EpochRoundReport,
    ) -> Result<(), RunnerError> {
        let page = match w.epoch_conversation_page(self.chat_after.clone()).await {
            Ok(p) => p,
            Err(e) => return r.delivery.local_busy(Operation::Recovery, e),
        };
        for s in page.scopes.iter().take(CHAT_BUDGET) {
            if r.rpc_attempts == MAX_ROUND_RPCS {
                return Ok(());
            }
            self.chat_after = Some(s.conversation_cursor());
            let snapshots = w.current_epoch_recoveries(s.clone()).await?;
            let needs_sync = snapshots.iter().any(|p| {
                matches!(
                    p.state(),
                    None | Some(
                        common::RecoveryState::Pending
                            | common::RecoveryState::Accepted
                            | common::RecoveryState::Superseded
                    )
                )
            });
            if needs_sync {
                if let Some(current) = self.route(w, s.conversation_id(), r).await? {
                    self.sync(w, &current, r).await?;
                }
            } else {
                self.bootstrap(w, s, r).await?;
            }
        }
        if page.scopes.len() <= CHAT_BUDGET {
            self.chat_after = page.next;
        }
        if !Self::reserve(r) {
            return Ok(());
        }
        let page = match self.transport.references(&self.remote_after).await {
            Ok(p) => p,
            Err(e) => {
                r.delivery.rpc(Operation::Recovery, e)?;
                return Ok(());
            }
        };
        if page.request().after_conversation_id != self.remote_after || page.request().limit != 32 {
            return Err(RpcError::Invalid.into());
        }
        super::super::epoch_rpc::validate_recovery_page(page.request(), page.response())
            .map_err(|_| RpcError::Invalid)?;
        for reference in page.response().proposals.iter().take(CHAT_BUDGET) {
            if r.rpc_attempts == MAX_ROUND_RPCS {
                return Ok(());
            }
            self.remote_after = reference.conversation_id.clone();
            let Some(s) = self.route(w, &reference.conversation_id, r).await? else {
                continue;
            };
            if s.crypto_epoch() != reference.expected_epoch {
                r.delivery
                    .crypto(Operation::Recovery, CryptoError::EpochRecoveryRequired)?;
                continue;
            }
            if let Some(snapshot) = self.query(w, &s, &reference.proposal_id, r).await? {
                let accepted = snapshot.state() == Some(common::RecoveryState::Accepted);
                r.recovery.push(RecoveryNotice {
                    conversation_id: s.conversation_id().into(),
                    snapshot,
                });
                if accepted {
                    self.transition(w, &s, &reference.proposal_id, r).await?;
                }
            }
        }
        if page.response().proposals.len() <= CHAT_BUDGET {
            self.remote_after = page.response().next_conversation_id.clone();
        }
        Ok(())
    }
    async fn query(
        &mut self,
        w: &CryptoCoordinator,
        s: &EpochScope,
        id: &str,
        r: &mut EpochRoundReport,
    ) -> Result<Option<EpochRecoverySnapshot>, RunnerError> {
        let q = w.prepare_epoch_recovery_query(s.clone(), id.into()).await?;
        if !Self::reserve(r) {
            return Ok(None);
        }
        let o = match self.transport.query(&q).await {
            Ok(o) => o,
            Err(e) => {
                r.delivery.rpc(Operation::Recovery, e)?;
                return Ok(None);
            }
        };
        match w.accept_epoch_recovery_query(o).await {
            Ok(s) => Ok(Some(s)),
            Err(e) => {
                r.delivery.crypto(Operation::Recovery, e)?;
                Ok(None)
            }
        }
    }
    async fn sync(
        &mut self,
        w: &CryptoCoordinator,
        s: &EpochScope,
        r: &mut EpochRoundReport,
    ) -> Result<(), RunnerError> {
        let snapshots = w.current_epoch_recoveries(s.clone()).await?;
        for previous in snapshots {
            if r.rpc_attempts == MAX_ROUND_RPCS {
                return Ok(());
            }
            let id = previous.proposal_id().to_string();
            let snapshot = if previous.state() == Some(common::RecoveryState::Accepted) {
                previous
            } else if matches!(
                previous.state(),
                None | Some(common::RecoveryState::Pending | common::RecoveryState::Superseded)
            ) {
                if let Some(intent) = w.resume_epoch_recovery(s.clone(), id.clone()).await? {
                    if !Self::reserve(r) {
                        return Ok(());
                    }
                    match self.transport.submit(&intent).await {
                        Ok(o) => match w.accept_epoch_recovery_result(o).await {
                            Ok(snapshot) => snapshot,
                            Err(e) => {
                                r.delivery.crypto(Operation::Recovery, e)?;
                                continue;
                            }
                        },
                        Err(EpochRecoveryError::Conflict(_)) => {
                            r.delivery
                                .rpc(Operation::Recovery, RpcError::Status(Code::Aborted))?;
                            continue;
                        }
                        Err(EpochRecoveryError::Rpc(e)) => {
                            // At most two current intents: a transient/conflicting
                            // own proposal must not starve a saved peer decision.
                            r.delivery.rpc(Operation::Recovery, e)?;
                            continue;
                        }
                    }
                } else {
                    let Some(snapshot) = self.query(w, s, &id, r).await? else {
                        continue;
                    };
                    snapshot
                }
            } else {
                previous
            };
            let accepted = snapshot.state() == Some(common::RecoveryState::Accepted);
            r.recovery.push(RecoveryNotice {
                conversation_id: s.conversation_id().into(),
                snapshot,
            });
            if accepted {
                self.transition(w, s, &id, r).await?;
                // The next snapshot belongs to the old scope; re-read current
                // refs on the next round rather than acting with stale epochs.
                return Ok(());
            }
        }
        Ok(())
    }
    async fn transition(
        &mut self,
        w: &CryptoCoordinator,
        s: &EpochScope,
        id: &str,
        r: &mut EpochRoundReport,
    ) -> Result<(), RunnerError> {
        let q = match w.prepare_epoch_transition(s.clone(), id.into()).await {
            Ok(q) => q,
            Err(e) => {
                r.delivery.crypto(Operation::Transition, e)?;
                return Ok(());
            }
        };
        if !Self::reserve(r) {
            return Ok(());
        }
        let o = match self.transport.catalog(&q).await {
            Ok(o) => o,
            Err(e) => {
                r.delivery.rpc(Operation::Transition, e)?;
                return Ok(());
            }
        };
        if let Err(e) = w.accept_epoch_transition_catalog(o).await {
            r.delivery.crypto(Operation::Transition, e)?;
            return Ok(());
        }
        let p = match w.advance_epoch_transition(s.clone(), id.into()).await {
            Ok(p) => p,
            Err(e) => {
                r.delivery.crypto(Operation::Transition, e)?;
                return Ok(());
            }
        };
        if p.activated() {
            r.activated += 1;
            let new = w
                .epoch_scope(s.conversation_id().into(), p.to_epoch())
                .await?;
            self.bootstrap(w, &new, r).await?;
        } else if let Some(id) = p.blocked_message() {
            let plan = w.epoch_outgoing_plan(id.into()).await?;
            self.send_plan(w, &plan, r).await?;
        }
        Ok(())
    }
    async fn bootstrap(
        &mut self,
        w: &CryptoCoordinator,
        s: &EpochScope,
        r: &mut EpochRoundReport,
    ) -> Result<(), RunnerError> {
        if !s.bootstrap_initiator() || s.receive_only() || s.sending_frozen() {
            return Ok(());
        }
        let plan = match w.prepare_epoch_acquisition(s.clone()).await {
            Ok(p) if p.applied() => return Ok(()),
            Ok(p) => p,
            Err(CryptoError::SessionExists) => return Ok(()),
            Err(e) => {
                r.delivery.crypto(Operation::Bootstrap, e)?;
                return Ok(());
            }
        };
        // Refresh lifecycle before requesting fresh material. A peer replacement
        // is discovered by authenticated registry lookup, never by a timer/reset.
        let Some(current) = self.route(w, s.conversation_id(), r).await? else {
            return Ok(());
        };
        if current.crypto_epoch() != s.crypto_epoch()
            || current.receive_only()
            || current.sending_frozen()
        {
            return Ok(());
        }
        if !Self::reserve(r) {
            return Ok(());
        }
        match self.transport.acquire(&plan).await {
            Ok(EpochAcquisitionOutcome::Bundle(o)) => match w.bootstrap_epoch(*o).await {
                Ok(()) => r.bootstrapped += 1,
                Err(e) => {
                    r.delivery.crypto(Operation::Bootstrap, e)?;
                }
            },
            Ok(EpochAcquisitionOutcome::Expired(o)) => {
                // CAS saves one successor. Acquire that durable plan next tick.
                if let Err(e) = w.renew_epoch_acquisition(*o).await {
                    r.delivery.crypto(Operation::Bootstrap, e)?;
                }
            }
            Err(e) => {
                r.delivery.rpc(Operation::Bootstrap, e)?;
            }
        }
        Ok(())
    }
}
