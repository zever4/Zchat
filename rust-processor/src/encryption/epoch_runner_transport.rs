use super::*;
use std::borrow::BorrowMut;
pub type RecoveryReferences =
    EpochObservation<v2::GetPendingEpochRecoveriesRequest, v2::PendingEpochRecoveries>;
pub type RecoveryReply = EpochObservation<EpochRecoveryQuery, v2::RecoveryRecord>;
pub type IntentReply = EpochObservation<EpochRecoveryIntent, v2::RecoveryRecord>;
pub type TransitionReply = EpochObservation<EpochTransitionQuery, v2::ConversationEpochs>;
/// Trusted native adapter, never renderer IPC. Implementations must preserve
/// the native account/channel, binding validation and response limits.
#[async_trait::async_trait]
pub trait EpochDeliveryTransport: Send {
    async fn file_statuses(
        &mut self,
        plans: &[EpochOutgoingPlan],
    ) -> Result<crate::encryption::native::EpochFileStatuses, RpcError>;
    async fn acknowledge_file(
        &mut self,
        _: &crate::encryption::signal_state::EpochDurableFileAck,
    ) -> Result<
        EpochObservation<
            crate::encryption::signal_state::EpochDurableFileAck,
            v2::FileAcknowledgements,
        >,
        RpcError,
    > {
        Err(RpcError::Status(Code::Unimplemented))
    }
    /// Internal account blob requests share this serial authenticated RPC lane.
    async fn observe_blob(
        &mut self,
        _: &crate::encryption::signal_state::EpochBlobControlPlan,
    ) -> Result<crate::encryption::native::EpochBlobControlOutcome, RpcError> {
        Err(RpcError::Status(Code::Unimplemented))
    }
    async fn reserve_blob(
        &mut self,
        _: &crate::encryption::signal_state::EpochBlobControlPlan,
    ) -> Result<crate::encryption::native::EpochBlobControlOutcome, RpcError> {
        Err(RpcError::Status(Code::Unimplemented))
    }
    /// Explicit foreground creation only; polling never invokes this method.
    async fn create(
        &mut self,
        p: &ConversationCreation,
    ) -> Result<VerifiedConversationCreation, RpcError>;
    async fn send(&mut self, p: &EpochOutgoingPlan) -> Result<EpochPreparedSendOutcome, RpcError>;
    async fn statuses(&mut self, p: &[EpochOutgoingPlan]) -> Result<EpochStatuses, RpcError>;
    async fn pending(&mut self) -> Result<Vec<EpochDelivery>, RpcError>;
    async fn acknowledge(
        &mut self,
        p: &EpochDurableAck,
    ) -> Result<EpochObservation<EpochDurableAck, common::Empty>, RpcError>;
    async fn lookup(
        &mut self,
        p: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError>;
    async fn initial(
        &mut self,
        p: &EpochCatalogQuery,
    ) -> Result<EpochObservation<EpochCatalogQuery, v2::ConversationEpochs>, RpcError>;
    async fn references(&mut self, after: &str) -> Result<RecoveryReferences, RpcError>;
    async fn query(&mut self, p: &EpochRecoveryQuery) -> Result<RecoveryReply, RpcError>;
    async fn submit(&mut self, p: &EpochRecoveryIntent) -> Result<IntentReply, EpochRecoveryError>;
    async fn catalog(&mut self, p: &EpochTransitionQuery) -> Result<TransitionReply, RpcError>;
    async fn acquire(
        &mut self,
        p: &EpochAcquisitionPlan,
    ) -> Result<EpochAcquisitionOutcome, RpcError>;
}
/// Own the registry in an account runtime, or borrow it exclusively for a scoped
/// caller. Both forms retain the same authenticated channel/account/generation.
pub struct NativeEpochDelivery<R> {
    registry: R,
    epoch: NativeEpochClient,
}
impl<R: BorrowMut<NativeV1Client>> NativeEpochDelivery<R> {
    pub fn new(mut registry: R) -> Result<Self, RpcError> {
        Ok(Self {
            epoch: registry.borrow_mut().epoch_transport()?,
            registry,
        })
    }
    pub(in crate::encryption) fn blob_transport(
        &self,
        endpoint: crate::encryption::native::BlobHttpEndpoint,
    ) -> Result<crate::encryption::native::NativeBlobHttp, crate::encryption::native::BlobHttpError>
    {
        self.epoch.blob_transport(endpoint)
    }
}
#[async_trait::async_trait]
impl<R: BorrowMut<NativeV1Client> + Send> EpochDeliveryTransport for NativeEpochDelivery<R> {
    async fn file_statuses(
        &mut self,
        plans: &[EpochOutgoingPlan],
    ) -> Result<crate::encryption::native::EpochFileStatuses, RpcError> {
        self.epoch.file_statuses_prepared(plans).await
    }
    async fn acknowledge_file(
        &mut self,
        proof: &crate::encryption::signal_state::EpochDurableFileAck,
    ) -> Result<
        EpochObservation<
            crate::encryption::signal_state::EpochDurableFileAck,
            v2::FileAcknowledgements,
        >,
        RpcError,
    > {
        self.epoch.acknowledge_files_prepared(proof).await
    }
    async fn observe_blob(
        &mut self,
        plan: &crate::encryption::signal_state::EpochBlobControlPlan,
    ) -> Result<crate::encryption::native::EpochBlobControlOutcome, RpcError> {
        self.epoch.observe_blob(plan).await
    }
    async fn reserve_blob(
        &mut self,
        plan: &crate::encryption::signal_state::EpochBlobControlPlan,
    ) -> Result<crate::encryption::native::EpochBlobControlOutcome, RpcError> {
        self.epoch.reserve_blob(plan).await
    }
    async fn create(
        &mut self,
        p: &ConversationCreation,
    ) -> Result<VerifiedConversationCreation, RpcError> {
        self.registry
            .borrow_mut()
            .create_prepared_conversation(p)
            .await
    }
    async fn send(&mut self, p: &EpochOutgoingPlan) -> Result<EpochPreparedSendOutcome, RpcError> {
        self.epoch.send_prepared(p).await
    }
    async fn statuses(&mut self, p: &[EpochOutgoingPlan]) -> Result<EpochStatuses, RpcError> {
        self.epoch.refresh_prepared(p).await
    }
    async fn pending(&mut self) -> Result<Vec<EpochDelivery>, RpcError> {
        self.epoch.pending_deliveries().await
    }
    async fn acknowledge(
        &mut self,
        p: &EpochDurableAck,
    ) -> Result<EpochObservation<EpochDurableAck, common::Empty>, RpcError> {
        self.epoch.acknowledge_prepared(p).await
    }
    async fn lookup(
        &mut self,
        p: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup, RpcError> {
        self.registry
            .borrow_mut()
            .get_prepared_conversation(p)
            .await
    }
    async fn initial(
        &mut self,
        p: &EpochCatalogQuery,
    ) -> Result<EpochObservation<EpochCatalogQuery, v2::ConversationEpochs>, RpcError> {
        self.epoch.initial_epoch_catalog(p).await
    }
    async fn references(&mut self, after: &str) -> Result<RecoveryReferences, RpcError> {
        self.epoch.recovery_references(after).await
    }
    async fn query(&mut self, p: &EpochRecoveryQuery) -> Result<RecoveryReply, RpcError> {
        self.epoch.query_recovery(p).await
    }
    async fn submit(&mut self, p: &EpochRecoveryIntent) -> Result<IntentReply, EpochRecoveryError> {
        self.epoch.submit_recovery(p).await
    }
    async fn catalog(&mut self, p: &EpochTransitionQuery) -> Result<TransitionReply, RpcError> {
        self.epoch.transition_catalog(p).await
    }
    async fn acquire(
        &mut self,
        p: &EpochAcquisitionPlan,
    ) -> Result<EpochAcquisitionOutcome, RpcError> {
        self.epoch.acquire_prepared(p).await
    }
}

#[async_trait::async_trait]
impl<R: BorrowMut<NativeV1Client> + Send>
    crate::encryption::prekey_maintenance::PreKeyMaintenanceTransport for NativeEpochDelivery<R>
{
    async fn inventory(
        &mut self,
    ) -> Result<crate::encryption::prekey_maintenance::VerifiedPreKeyInventory, RpcError> {
        self.registry.borrow_mut().prekey_inventory().await
    }
    async fn upload(
        &mut self,
        plan: &crate::encryption::signal_state::PreKeyRefillUpload,
    ) -> Result<crate::encryption::signal_state::VerifiedPreKeyRefillUpload, RpcError> {
        self.registry.borrow_mut().upload_refill(plan).await
    }
}
