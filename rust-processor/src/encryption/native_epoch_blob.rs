//! Native-only blob metadata RPCs over the existing authenticated TLS channel.
use super::*;
use crate::encryption::{blob_control as blob, signal_state::EpochBlobControlPlan};
use crate::encryption::{file_ack, signal_state::EpochDurableFileAck};

pub type EpochBlobLease = EpochObservation<EpochBlobControlPlan, v2::BlobLease>;
pub enum EpochBlobControlOutcome {
    Lease(Box<EpochBlobLease>),
    /// Request-bound metadata failure, never a Send/unsent proof or permission
    /// to reset UUID/key/session. Missing does not permit operation renewal.
    Rejected(Box<EpochObservation<EpochBlobControlPlan, v2::BlobFailure>>),
}
fn blob_failure(
    status: &tonic::Status,
    request: &v2::BlobLeaseRequest,
    observe: bool,
) -> Option<v2::BlobFailure> {
    let bytes = detail(status, "type.googleapis.com/zchat.e2ee.v2.BlobFailure")?;
    let f = v2::BlobFailure::decode(bytes.as_slice()).ok()?;
    blob::validate_failure(request, &f, observe).ok()?;
    (blob::failure_code(f.reason, observe) == Some(status.code())).then_some(f)
}
impl NativeEpochClient {
    /// Only a freshly verified protected cache can mint this proof. This call
    /// never retries, clears pending locally or uses a renderer-created request.
    pub async fn acknowledge_files_prepared(
        &mut self,
        proof: &EpochDurableFileAck,
    ) -> Result<EpochObservation<EpochDurableFileAck, v2::FileAcknowledgements>> {
        if proof.store_scope() != &self.scope || proof.own_device() != &self.own {
            return Err(RpcError::Invalid);
        }
        let request = proof.request();
        self.check_context(request.context.as_ref())?;
        file_ack::validate_request(request).map_err(|_| RpcError::Invalid)?;
        let reply = self
            .client
            .acknowledge_files(self.request(request.clone())?)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        file_ack::validate_receipts(request, &reply).map_err(|_| RpcError::Invalid)?;
        Ok(self.observed(proof.clone(), reply))
    }
    #[cfg(test)]
    pub(in crate::encryption) fn test_metadata_transport(
        scope: StoreScope,
        own: DeviceRef,
    ) -> Self {
        Self {
            client: v2::messenger_client::MessengerClient::new(
                Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
            own,
            scope,
            jwt: Zeroizing::new("synthetic.jwt".into()),
            credential: Zeroizing::new([42; 32]),
        }
    }
    pub async fn reserve_blob(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome> {
        self.blob_control(plan, false).await
    }
    pub async fn observe_blob(
        &mut self,
        plan: &EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome> {
        self.blob_control(plan, true).await
    }
    async fn blob_control(
        &mut self,
        plan: &EpochBlobControlPlan,
        observe: bool,
    ) -> Result<EpochBlobControlOutcome> {
        let r = plan.request();
        if plan.store_scope() != &self.scope || r.sender.as_ref() != Some(&self.own) {
            return Err(RpcError::Invalid);
        }
        self.check_context(r.context.as_ref())?;
        blob::validate_request(r).map_err(|_| RpcError::Invalid)?;
        let req = self.request(r.clone())?;
        let result = if observe {
            self.client.observe_blob(req).await
        } else {
            self.client.reserve_blob(req).await
        };
        match result {
            Ok(response) => {
                let grant = response.into_inner();
                blob::validate_lease(r, &grant).map_err(|_| RpcError::Invalid)?;
                Ok(EpochBlobControlOutcome::Lease(Box::new(
                    self.observed(plan.clone(), grant),
                )))
            }
            Err(status) => {
                if let Some(failure) = blob_failure(&status, r, observe) {
                    return Ok(EpochBlobControlOutcome::Rejected(Box::new(
                        self.observed(plan.clone(), failure),
                    )));
                }
                Err(RpcError::from_status(status))
            }
        }
    }
}

#[cfg(test)]
#[path = "native_epoch_blob_tests.rs"]
mod tests;
