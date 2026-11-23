//! Native v2 wire transport. No local writes, retry, consent, scope activation or
//! outbox settlement. Public observations cannot be submitted to the v1 worker.
use super::super::signal_state::{EpochDurableAck, EpochOutgoingPlan};
use super::super::{
    epoch_contract as data,
    epoch_rpc::{self as boundary, EpochConversation},
};
use super::*;
use crate::proto::{e2ee as common, e2ee_v2 as v2};

#[path = "native_epoch_recovery.rs"]
mod recovery;

#[path = "native_epoch_blob.rs"]
mod blob;
pub use blob::{EpochBlobControlOutcome, EpochBlobLease};

#[path = "native_epoch_file_status.rs"]
mod file_status;
pub type EpochFileStatuses = EpochObservation<Vec<EpochOutgoingPlan>, v2::FileStatuses>;

#[path = "native_epoch_blob_send.rs"]
mod blob_send;
pub type EpochBlobSendFailure = EpochObservation<EpochOutgoingPlan, v2::BlobSendFailure>;

#[path = "native_blob_http.rs"]
mod blob_http;
pub use blob_http::{
    BlobCiphertextChunk, BlobCiphertextSink, BlobGetResponse, BlobHeadResponse, BlobHttpEndpoint,
    BlobHttpError, BlobPutResponse, BlobReadError, BlobReadRequest, BlobSinkError,
    BlobTransferCancellation, NativeBlobHttp, NativeBlobUploadTransport,
};

pub type EpochDelivery = EpochObservation<v2::GetPendingMessagesRequest, v2::Delivery>;
pub type EpochStatuses = EpochObservation<Vec<EpochOutgoingPlan>, v2::MessageStatuses>;
pub type EpochExpiredAcquisition = EpochObservation<
    super::super::signal_state::EpochAcquisitionPlan,
    v2::AcquisitionExpiredFailure,
>;

pub enum EpochAcquisitionOutcome {
    Bundle(
        Box<EpochObservation<super::super::signal_state::EpochAcquisitionPlan, v2::AcquiredBundle>>,
    ),
    Expired(Box<EpochExpiredAcquisition>),
}

pub enum EpochPreparedSendOutcome {
    Accepted(Box<EpochObservation<EpochOutgoingPlan, v2::SendReceipt>>),
    /// Verified remote fence only; still requires protected recovery intent/CAS
    /// before settling an uncertain local outbox. No automatic reset/unfreeze.
    Sealed(Box<EpochObservation<EpochOutgoingPlan, v2::EpochSealedFailure>>),
    /// Exact authenticated Ready/expiry failure, not nonacceptance or renewal.
    BlobUnavailable(Box<EpochBlobSendFailure>),
}

/// Authenticated, validated RPC observation, NOT protected local COMMIT evidence.
/// No Deserialize/public constructor/Debug: no renderer input or payload logging.
pub struct EpochObservation<Q, R> {
    scope: StoreScope,
    request: Q,
    response: R,
}
impl<Q, R> EpochObservation<Q, R> {
    #[cfg(test)]
    pub(in crate::encryption) fn test_observation(
        scope: StoreScope,
        request: Q,
        response: R,
    ) -> Self {
        Self {
            scope,
            request,
            response,
        }
    }
    pub fn store_scope(&self) -> &StoreScope {
        &self.scope
    }
    pub fn request(&self) -> &Q {
        &self.request
    }
    pub fn response(&self) -> &R {
        &self.response
    }
}
pub enum EpochSendOutcome {
    Accepted(Box<EpochObservation<v2::SendMessageRequest, v2::SendReceipt>>),
    /// Both signatures and exact Send binding verified. Still requires the
    /// protected owner's matching intent/CAS/COMMIT before any settlement.
    Sealed(Box<EpochObservation<v2::SendMessageRequest, v2::EpochSealedFailure>>),
    BlobUnavailable(Box<EpochObservation<v2::SendMessageRequest, v2::BlobSendFailure>>),
}

#[derive(Debug, PartialEq, Eq)]
pub struct EpochConflict {
    epoch: u64,
    revision: u64,
    pending: Option<String>,
}
impl EpochConflict {
    pub fn active_epoch(&self) -> u64 {
        self.epoch
    }
    pub fn revision(&self) -> u64 {
        self.revision
    }
    pub fn pending_proposal(&self) -> Option<&str> {
        self.pending.as_deref()
    }
}
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum EpochRecoveryError {
    #[error("recovery RPC failed; the operation outcome may be unknown")]
    Rpc(#[from] RpcError),
    #[error("recovery head or pending proposal changed; refresh authoritative state")]
    Conflict(EpochConflict),
}

fn detail(status: &tonic::Status, name: &str) -> Option<Vec<u8>> {
    if status.details().len() > MAX_RESPONSE_BYTES {
        return None;
    }
    let parsed = StatusDetails::decode(status.details()).ok()?;
    if parsed.code != status.code() as i32 || parsed.details.len() != 1 {
        return None;
    }
    let item = parsed.details.into_iter().next()?;
    (item.type_url == name).then_some(item.value)
}
fn sealed_send(
    status: &tonic::Status,
    pins: &EpochConversation,
    r: &v2::SendMessageRequest,
) -> Option<v2::EpochSealedFailure> {
    if status.code() != Code::FailedPrecondition {
        return None;
    }
    let bytes = detail(
        status,
        "type.googleapis.com/zchat.e2ee.v2.EpochSealedFailure",
    )?;
    let failure = v2::EpochSealedFailure::decode(bytes.as_slice()).ok()?;
    pins.verify_sealed(r, &failure).ok()?;
    Some(failure)
}
fn expired_acquisition(
    status: &tonic::Status,
    request: &v2::AcquireBundleRequest,
) -> Option<v2::AcquisitionExpiredFailure> {
    if status.code() != Code::FailedPrecondition {
        return None;
    }
    let bytes = detail(
        status,
        "type.googleapis.com/zchat.e2ee.v2.AcquisitionExpiredFailure",
    )?;
    let failure = v2::AcquisitionExpiredFailure::decode(bytes.as_slice()).ok()?;
    boundary::validate_expired_acquisition(request, &failure).ok()?;
    Some(failure)
}
impl EpochRecoveryError {
    fn from_status(status: tonic::Status) -> Self {
        let conflict = || -> Option<EpochConflict> {
            if status.code() != Code::Aborted {
                return None;
            }
            let bytes = detail(
                &status,
                "type.googleapis.com/zchat.e2ee.v2.RecoveryConflict",
            )?;
            let c = v2::RecoveryConflict::decode(bytes.as_slice()).ok()?;
            let epoch = c.active_epoch?;
            let revision = c.epoch_revision?;
            if epoch > i64::MAX as u64
                || revision > i64::MAX as u64
                || !c.pending_proposal_id.is_empty() && !boundary::uuid4(&c.pending_proposal_id)
            {
                return None;
            }
            Some(EpochConflict {
                epoch,
                revision,
                pending: (!c.pending_proposal_id.is_empty()).then_some(c.pending_proposal_id),
            })
        };
        if let Some(conflict) = conflict() {
            return Self::Conflict(conflict);
        }
        Self::Rpc(RpcError::from_status(status))
    }
}

/// Separate generated v2 client over the SAME trusted registry channel. Account,
/// generation, HTTP store namespace, JWT and credential come only from discovery/
/// provisioning. This object never chooses a new endpoint or changes generation.
pub struct NativeEpochClient {
    client: v2::messenger_client::MessengerClient<Channel>,
    own: DeviceRef,
    scope: StoreScope,
    jwt: Zeroizing<String>,
    credential: Zeroizing<[u8; 32]>,
}
impl NativeV1Client {
    pub fn epoch_transport(&self) -> Result<NativeEpochClient> {
        let c = self.context()?;
        Ok(NativeEpochClient {
            client: v2::messenger_client::MessengerClient::new(self.channel.clone())
                .max_encoding_message_size(MAX_REQUEST_BYTES)
                .max_decoding_message_size(MAX_RESPONSE_BYTES),
            own: DeviceRef {
                user_id: self.account.clone(),
                device_id: 1,
                generation_id: c.device_generation_id,
            },
            scope: self.store_scope.clone().ok_or(RpcError::Invalid)?,
            jwt: Zeroizing::new(self.jwt.to_string()),
            credential: Zeroizing::new(**self.credential.as_ref().ok_or(RpcError::Invalid)?),
        })
    }
}
impl NativeEpochClient {
    pub async fn recovery_references(
        &mut self,
        after: &str,
    ) -> Result<EpochObservation<v2::GetPendingEpochRecoveriesRequest, v2::PendingEpochRecoveries>>
    {
        let page = self.pending_recoveries(after, 32).await?;
        Ok(self.observed(
            v2::GetPendingEpochRecoveriesRequest {
                context: Some(self.context()),
                after_conversation_id: after.into(),
                limit: 32,
            },
            page,
        ))
    }
    pub async fn initial_epoch_catalog(
        &mut self,
        query: &super::super::signal_state::EpochCatalogQuery,
    ) -> Result<
        EpochObservation<super::super::signal_state::EpochCatalogQuery, v2::ConversationEpochs>,
    > {
        if query.store_scope() != &self.scope {
            return Err(RpcError::Invalid);
        }
        let observed = self.catalog(query.pins(), None, 1).await?;
        if &observed.request != query.request() {
            return Err(RpcError::Invalid);
        }
        Ok(self.observed(query.clone(), observed.response))
    }
    pub fn context(&self) -> ClientContext {
        ClientContext {
            protocol_version: 2,
            device_generation_id: self.own.generation_id.clone(),
        }
    }
    fn check_context(&self, c: Option<&ClientContext>) -> Result<()> {
        if c != Some(&self.context()) {
            Err(RpcError::Invalid)
        } else {
            Ok(())
        }
    }
    fn check_peer(&self, pins: &EpochConversation) -> Result<()> {
        if pins.conversation().own_device.as_ref() != Some(&self.own) {
            Err(RpcError::Invalid)
        } else {
            Ok(())
        }
    }
    fn request<T: Message>(&self, value: T) -> Result<Request<T>> {
        authenticated_request(value, &self.jwt, Some(&self.credential))
    }
    fn observed<Q, R>(&self, request: Q, response: R) -> EpochObservation<Q, R> {
        EpochObservation {
            scope: self.scope.clone(),
            request,
            response,
        }
    }

    pub async fn capabilities(&mut self) -> Result<v2::Capabilities> {
        let req = authenticated_request(Empty {}, &self.jwt, None)?;
        let value = self
            .client
            .get_capabilities(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        boundary::validate_capabilities(&value).map_err(|_| RpcError::Invalid)?;
        Ok(value)
    }
    pub async fn send(
        &mut self,
        pins: &EpochConversation,
        r: &v2::SendMessageRequest,
    ) -> Result<EpochSendOutcome> {
        self.check_peer(pins)?;
        self.check_context(r.context.as_ref())?;
        pins.check_send(r).map_err(|_| RpcError::Invalid)?;
        let req = self.request(r.clone())?;
        let result = self.client.send_message(req).await;
        match result {
            Ok(response) => {
                let receipt = response.into_inner();
                data::validate_receipt(r, &receipt).map_err(|_| RpcError::Invalid)?;
                Ok(EpochSendOutcome::Accepted(Box::new(
                    self.observed(r.clone(), receipt),
                )))
            }
            Err(status) => {
                if let Some(failure) = sealed_send(&status, pins, r) {
                    return Ok(EpochSendOutcome::Sealed(Box::new(
                        self.observed(r.clone(), failure),
                    )));
                }
                if let Some(failure) = blob_send::failure(&status, r) {
                    return Ok(EpochSendOutcome::BlobUnavailable(Box::new(
                        self.observed(r.clone(), failure),
                    )));
                }
                Err(RpcError::from_status(status))
            }
        }
    }
    pub async fn acquire(
        &mut self,
        pins: &EpochConversation,
        r: &v2::AcquireBundleRequest,
    ) -> Result<EpochObservation<v2::AcquireBundleRequest, v2::AcquiredBundle>> {
        self.check_peer(pins)?;
        self.check_context(r.context.as_ref())?;
        pins.check_acquire(r).map_err(|_| RpcError::Invalid)?;
        let req = self.request(r.clone())?;
        let bundle = self
            .client
            .acquire_bundle(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        pins.verify_bundle(r, &bundle)
            .map_err(|_| RpcError::Invalid)?;
        // An exact retained snapshot may be expired: only the protected
        // bootstrap operation checks lease against its clock just before use.
        Ok(self.observed(r.clone(), bundle))
    }

    /// Owner-minted, already committed acquisition only. A wire observation
    /// becomes an applied local fact only after bootstrap_epoch commits it.
    pub async fn acquire_prepared(
        &mut self,
        plan: &super::super::signal_state::EpochAcquisitionPlan,
    ) -> Result<EpochAcquisitionOutcome> {
        if plan.store_scope() != &self.scope || plan.applied() {
            return Err(RpcError::Invalid);
        }
        self.check_peer(plan.pins())?;
        self.check_context(plan.request().context.as_ref())?;
        plan.pins()
            .check_acquire(plan.request())
            .map_err(|_| RpcError::Invalid)?;
        let request = self.request(plan.request().clone())?;
        match self.client.acquire_bundle(request).await {
            Ok(response) => {
                let bundle = response.into_inner();
                plan.pins()
                    .verify_bundle(plan.request(), &bundle)
                    .map_err(|_| RpcError::Invalid)?;
                Ok(EpochAcquisitionOutcome::Bundle(Box::new(
                    self.observed(plan.clone(), bundle),
                )))
            }
            Err(status) => {
                if let Some(failure) = expired_acquisition(&status, plan.request()) {
                    return Ok(EpochAcquisitionOutcome::Expired(Box::new(
                        self.observed(plan.clone(), failure),
                    )));
                }
                Err(RpcError::from_status(status))
            }
        }
    }
    pub async fn send_prepared(
        &mut self,
        plan: &EpochOutgoingPlan,
    ) -> Result<EpochPreparedSendOutcome> {
        if plan.store_scope() != &self.scope || !plan.needs_send() {
            return Err(RpcError::Invalid);
        }
        match self.send(plan.pins(), plan.request()).await? {
            EpochSendOutcome::Accepted(b) => Ok(EpochPreparedSendOutcome::Accepted(Box::new(
                self.observed(plan.clone(), b.response),
            ))),
            EpochSendOutcome::Sealed(b) => Ok(EpochPreparedSendOutcome::Sealed(Box::new(
                self.observed(plan.clone(), b.response),
            ))),
            EpochSendOutcome::BlobUnavailable(b) => Ok(EpochPreparedSendOutcome::BlobUnavailable(
                Box::new(self.observed(plan.clone(), b.response)),
            )),
        }
    }
    pub async fn pending(&mut self) -> Result<v2::PendingMessages> {
        let req = self.request(v2::GetPendingMessagesRequest {
            context: Some(self.context()),
        })?;
        let result = self
            .client
            .get_pending_messages(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        boundary::validate_pending(&result, &self.own).map_err(|_| RpcError::Invalid)?;
        Ok(result)
    }
    /// Authenticated per-delivery observations are not durable receive proofs.
    pub async fn pending_deliveries(&mut self) -> Result<Vec<EpochDelivery>> {
        let request = v2::GetPendingMessagesRequest {
            context: Some(self.context()),
        };
        let page = self.pending().await?;
        Ok(page
            .messages
            .into_iter()
            .map(|d| self.observed(request.clone(), d))
            .collect())
    }
    pub async fn refresh_outgoing(
        &mut self,
        sends: &[v2::SendMessageRequest],
    ) -> Result<EpochObservation<Vec<common::EpochSendBinding>, v2::MessageStatuses>> {
        let request =
            boundary::status_request(self.context(), sends).map_err(|_| RpcError::Invalid)?;
        if sends
            .iter()
            .any(|r| r.envelope.as_ref().and_then(|e| e.sender.as_ref()) != Some(&self.own))
        {
            return Err(RpcError::Invalid);
        }
        let req = self.request(request)?;
        let result = self
            .client
            .get_message_statuses(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        boundary::validate_statuses(sends, &result).map_err(|_| RpcError::Invalid)?;
        let bindings = sends
            .iter()
            .map(|r| {
                data::send_binding(r.envelope.as_ref().unwrap()).map_err(|_| RpcError::Invalid)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(self.observed(bindings, result))
    }
    /// Status observations are tied to committed v2 outbox plans. No store
    /// mutation/implicit Send; the owner commits them with accept_epoch_statuses.
    pub async fn refresh_prepared(&mut self, plans: &[EpochOutgoingPlan]) -> Result<EpochStatuses> {
        if plans.is_empty()
            || plans.len() > boundary::MAX_PREPARED_STATUS_PLANS
            || plans
                .iter()
                .any(|p| p.store_scope() != &self.scope || p.sealed_failure().is_some())
        {
            return Err(RpcError::Invalid);
        }
        let sends: Vec<_> = plans.iter().map(|p| p.request().clone()).collect();
        let b = self.refresh_outgoing(&sends).await?;
        if plans.iter().zip(&b.response.messages).any(|(p, s)| {
            p.receipt()
                .is_some_and(|old| s.receipt.as_ref() != Some(old))
        }) {
            return Err(RpcError::Invalid);
        }
        Ok(self.observed(plans.to_vec(), b.response))
    }
    async fn acknowledge_request(&mut self, r: &common::AcknowledgeMessagesRequest) -> Result<()> {
        self.check_context(r.context.as_ref())?;
        boundary::validate_ack(r).map_err(|_| RpcError::Invalid)?;
        let req = self.request(r.clone())?;
        self.client
            .acknowledge_messages(req)
            .await
            .map_err(RpcError::from_status)?;
        Ok(())
    }
    /// Only a protected v2 receive/explicit quarantine/point reload can mint this proof. Raw delivery
    /// observations, renderer IDs and v1 DurableAck have incompatible types.
    pub async fn acknowledge_prepared(
        &mut self,
        p: &EpochDurableAck,
    ) -> Result<EpochObservation<EpochDurableAck, common::Empty>> {
        if p.store_scope() != &self.scope || p.own_device() != &self.own {
            return Err(RpcError::Invalid);
        }
        self.acknowledge_request(p.request()).await?;
        Ok(self.observed(p.clone(), common::Empty {}))
    }
    async fn command(
        &mut self,
        pins: &EpochConversation,
        proposal: &common::SignedRecoveryControl,
        command: &common::SignedRecoveryControl,
        propose: bool,
    ) -> std::result::Result<
        EpochObservation<v2::RecoveryCommandRequest, v2::RecoveryRecord>,
        EpochRecoveryError,
    > {
        self.check_peer(pins)?;
        pins.verify_command(proposal, command, propose)
            .map_err(|_| RpcError::Invalid)?;
        let request = v2::RecoveryCommandRequest {
            context: Some(self.context()),
            command: Some(command.clone()),
        };
        let req = self.request(request.clone())?;
        let result = if propose {
            self.client.propose_epoch_recovery(req).await
        } else {
            self.client.decide_epoch_recovery(req).await
        }
        .map_err(EpochRecoveryError::from_status)?
        .into_inner();
        pins.verify_record(&result, Some(proposal))
            .map_err(|_| RpcError::Invalid)?;
        if result
            .outcome
            .as_ref()
            .and_then(|o| o.decision.as_ref())
            .is_some_and(|d| {
                d.control.as_ref().unwrap().request_id
                    == command.control.as_ref().unwrap().request_id
                    && d != command
            })
        {
            return Err(RpcError::Invalid.into());
        }
        // A late valid decision may observe a different already-terminal
        // decision for this exact proposal. It never means this action won.
        Ok(self.observed(request, result))
    }
    #[cfg(test)]
    pub async fn propose_recovery(
        &mut self,
        pins: &EpochConversation,
        proposal: &common::SignedRecoveryControl,
    ) -> std::result::Result<
        EpochObservation<v2::RecoveryCommandRequest, v2::RecoveryRecord>,
        EpochRecoveryError,
    > {
        self.command(pins, proposal, proposal, true).await
    }
    #[cfg(test)]
    pub async fn decide_recovery(
        &mut self,
        pins: &EpochConversation,
        proposal: &common::SignedRecoveryControl,
        decision: &common::SignedRecoveryControl,
    ) -> std::result::Result<
        EpochObservation<v2::RecoveryCommandRequest, v2::RecoveryRecord>,
        EpochRecoveryError,
    > {
        self.command(pins, proposal, decision, false).await
    }
    pub async fn get_recovery(
        &mut self,
        pins: &EpochConversation,
        proposal_id: &str,
        expected: Option<&common::SignedRecoveryControl>,
    ) -> Result<EpochObservation<v2::GetEpochRecoveryRequest, v2::RecoveryRecord>> {
        self.check_peer(pins)?;
        if !boundary::uuid4(proposal_id)
            || expected.is_some_and(|p| {
                p.control.as_ref().is_none_or(|c| {
                    c.proposal_id != proposal_id
                        || c.conversation_id != pins.conversation().conversation_id
                })
            })
        {
            return Err(RpcError::Invalid);
        }
        let request = v2::GetEpochRecoveryRequest {
            context: Some(self.context()),
            conversation_id: pins.conversation().conversation_id.clone(),
            proposal_id: proposal_id.into(),
        };
        let req = self.request(request.clone())?;
        let result = self
            .client
            .get_epoch_recovery(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        pins.verify_record(&result, expected)
            .map_err(|_| RpcError::Invalid)?;
        if result
            .outcome
            .as_ref()
            .unwrap()
            .proposal
            .as_ref()
            .unwrap()
            .control
            .as_ref()
            .unwrap()
            .proposal_id
            != proposal_id
        {
            return Err(RpcError::Invalid);
        }
        Ok(self.observed(request, result))
    }
    /// Advisory refs only; an empty page never authorizes unfreeze/reset.
    pub async fn pending_recoveries(
        &mut self,
        after: &str,
        limit: u32,
    ) -> Result<v2::PendingEpochRecoveries> {
        if !(1..=32).contains(&limit) || !after.is_empty() && !boundary::uuid4(after) {
            return Err(RpcError::Invalid);
        }
        let request = v2::GetPendingEpochRecoveriesRequest {
            context: Some(self.context()),
            after_conversation_id: after.into(),
            limit,
        };
        let req = self.request(request.clone())?;
        let result = self
            .client
            .get_pending_epoch_recoveries(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        boundary::validate_recovery_page(&request, &result).map_err(|_| RpcError::Invalid)?;
        Ok(result)
    }
    pub async fn catalog(
        &mut self,
        pins: &EpochConversation,
        after: Option<u64>,
        limit: u32,
    ) -> Result<EpochObservation<v2::GetConversationEpochsRequest, v2::ConversationEpochs>> {
        self.check_peer(pins)?;
        if !(1..=32).contains(&limit) || after.is_some_and(|e| e > i64::MAX as u64) {
            return Err(RpcError::Invalid);
        }
        let request = v2::GetConversationEpochsRequest {
            context: Some(self.context()),
            conversation_id: pins.conversation().conversation_id.clone(),
            after_epoch: after,
            limit,
        };
        let req = self.request(request.clone())?;
        let result = self
            .client
            .get_conversation_epochs(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        boundary::validate_catalog(&request, &result, pins).map_err(|_| RpcError::Invalid)?;
        Ok(self.observed(request, result))
    }
}

#[cfg(test)]
#[path = "native_epoch_tests.rs"]
mod tests;
