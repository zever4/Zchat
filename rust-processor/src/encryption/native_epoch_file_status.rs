//! Read-only sender observations on the existing trusted channel. No unknown
//! Send settlement, protected mutation, retry, upload permission or renderer DTO.
use super::*;
use crate::encryption::file_status as contract;

impl NativeEpochClient {
    fn file_status_request(
        &self,
        plans: &[EpochOutgoingPlan],
    ) -> Result<v2::GetFileStatusesRequest> {
        if plans.is_empty() || plans.len() > contract::MAX_FILE_STATUSES {
            return Err(RpcError::Invalid);
        }
        let mut request = v2::GetFileStatusesRequest {
            context: Some(self.context()),
            files: Vec::with_capacity(plans.len()),
        };
        for p in plans {
            if p.store_scope() != &self.scope || p.sealed_failure().is_some() {
                return Err(RpcError::Invalid);
            }
            self.check_peer(p.pins())?;
            let send = p.request();
            self.check_context(send.context.as_ref())?;
            data::validate_send(send).map_err(|_| RpcError::Invalid)?;
            let receipt = p.receipt().ok_or(RpcError::Invalid)?;
            data::validate_receipt(send, receipt).map_err(|_| RpcError::Invalid)?;
            let e = send.envelope.as_ref().ok_or(RpcError::Invalid)?;
            if e.protocol_version != 3 || e.sender.as_ref() != Some(&self.own) {
                return Err(RpcError::Invalid);
            }
            request.files.push(v2::FileStatusReference {
                binding: Some(data::send_binding(e).map_err(|_| RpcError::Invalid)?),
                blob: e.blob.clone(),
            });
        }
        contract::validate_request(&request).map_err(|_| RpcError::Invalid)?;
        Ok(request)
    }
    fn file_status_observation(
        &self,
        plans: &[EpochOutgoingPlan],
        response: v2::FileStatuses,
    ) -> Result<EpochFileStatuses> {
        let request = self.file_status_request(plans)?;
        contract::validate_response(&request, &response).map_err(|_| RpcError::Invalid)?;
        if plans
            .iter()
            .zip(&response.files)
            .any(|(p, s)| s.receipt.as_ref() != p.receipt())
        {
            return Err(RpcError::Invalid); // No timestamp/TTL change of known Send.
        }
        Ok(self.observed(plans.to_vec(), response))
    }
    /// Only retained protected ACCEPTED attachment plans, never raw public refs.
    /// Old epochs/descriptor outcomes do not stop this read. It does not persist
    /// the result; a later protected acceptance must recheck its current context.
    pub async fn file_statuses_prepared(
        &mut self,
        plans: &[EpochOutgoingPlan],
    ) -> Result<EpochFileStatuses> {
        let request = self.file_status_request(plans)?;
        let response = self
            .client
            .get_file_statuses(self.request(request)?)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        self.file_status_observation(plans, response)
    }
    #[cfg(test)]
    pub(in crate::encryption) fn test_file_status_observation(
        &self,
        plans: &[EpochOutgoingPlan],
        response: v2::FileStatuses,
    ) -> Result<EpochFileStatuses> {
        self.file_status_observation(plans, response)
    }
}
