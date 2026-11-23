//! Only worker-minted, committed intent bytes reach the recovery command RPC.
use super::*;
use crate::encryption::signal_state::{EpochRecoveryIntent, EpochRecoveryQuery};

impl NativeEpochClient {
    pub async fn submit_recovery(
        &mut self,
        intent: &EpochRecoveryIntent,
    ) -> std::result::Result<
        EpochObservation<EpochRecoveryIntent, v2::RecoveryRecord>,
        EpochRecoveryError,
    > {
        if intent.store_scope() != &self.scope {
            return Err(RpcError::Invalid.into());
        }
        self.check_context(intent.request().context.as_ref())?;
        let command = intent.request().command.as_ref().ok_or(RpcError::Invalid)?;
        let propose = command
            .control
            .as_ref()
            .is_some_and(|c| c.action == common::RecoveryAction::Propose as i32);
        let result = self
            .command(intent.pins(), intent.proposal(), command, propose)
            .await?;
        if result.request() != intent.request() {
            return Err(RpcError::Invalid.into());
        }
        Ok(self.observed(intent.clone(), result.response))
    }

    pub async fn query_recovery(
        &mut self,
        query: &EpochRecoveryQuery,
    ) -> Result<EpochObservation<EpochRecoveryQuery, v2::RecoveryRecord>> {
        if query.store_scope() != &self.scope {
            return Err(RpcError::Invalid);
        }
        self.check_context(query.request().context.as_ref())?;
        let result = self
            .get_recovery(query.pins(), &query.request().proposal_id, query.expected())
            .await?;
        if result.request() != query.request() {
            return Err(RpcError::Invalid);
        }
        Ok(self.observed(query.clone(), result.response))
    }
}

impl NativeEpochClient {
    pub async fn transition_catalog(
        &mut self,
        query: &crate::encryption::signal_state::EpochTransitionQuery,
    ) -> Result<
        EpochObservation<
            crate::encryption::signal_state::EpochTransitionQuery,
            v2::ConversationEpochs,
        >,
    > {
        if query.store_scope() != &self.scope {
            return Err(RpcError::Invalid);
        }
        self.check_context(query.request().context.as_ref())?;
        let result = self
            .catalog(
                query.pins(),
                query.request().after_epoch,
                query.request().limit,
            )
            .await?;
        if result.request() != query.request() {
            return Err(RpcError::Invalid);
        }
        Ok(self.observed(query.clone(), result.response))
    }
}
