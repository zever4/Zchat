//! Native account discovery and initial-registration recovery. This is not IPC.
//! The native application selects a trusted HTTP/gRPC/CA tuple, then authenticates
//! before choosing a filesystem namespace. No UUID, path, CA or device credential
//! is taken from renderer claims. GUI wiring is a later slice.
use super::{
    native::{BlobHttpEndpoint, NativeEndpoint, NativeV1Client, RpcError},
    protected::StoreScope,
    provisioning::{DeviceProvisioning, Reconciliation, ReplacementAttempt},
    signal_state::CryptoError,
};
use crate::proto::e2ee::OwnDeviceState;
use url::Url;
use zeroize::Zeroizing;

#[derive(Debug, thiserror::Error)]
pub enum DiscoveryError {
    #[error("native E2EE server/account binding does not match")]
    Scope,
    #[error("native E2EE RPC failed")]
    Rpc(#[from] RpcError),
    #[error("local E2EE state could not be reconciled; existing keys are preserved")]
    Crypto(#[from] CryptoError),
    #[error("initial E2EE provisioning is not ready")]
    NotReady,
    #[error("the active generation no longer matches the explicitly selected replacement target")]
    ExpectedGenerationChanged,
}
type Result<T> = std::result::Result<T, DiscoveryError>;

fn origin(value: &str) -> Result<String> {
    // Reuse the transport's strict origin policy, including loopback-only HTTP.
    NativeEndpoint::new(value, None)?;
    Ok(Url::parse(value)
        .map_err(|_| DiscoveryError::Scope)?
        .origin()
        .ascii_serialization())
}

/// Construct only from native configuration. It explicitly binds an HTTP origin
/// to an RPC endpoint and its CA; no port guessing, remote discovery or redirects.
/// A separate RPC host is allowed only by this explicit trusted configuration.
/// Deliberately no Deserialize/IPC constructor.
pub struct TrustedServer {
    http_origin: String,
    endpoint: NativeEndpoint,
    blob_endpoint: BlobHttpEndpoint,
}
impl TrustedServer {
    #[cfg(test)]
    pub(in crate::encryption) fn blob_endpoint_for_test(self) -> BlobHttpEndpoint {
        self.blob_endpoint
    }
    pub fn new(http_origin: &str, grpc_origin: &str, extra_ca: Option<&[u8]>) -> Result<Self> {
        let http_origin = origin(http_origin)?;
        let grpc_origin = origin(grpc_origin)?;
        if http_origin.starts_with("https:") && !grpc_origin.starts_with("https:") {
            return Err(DiscoveryError::Scope);
        }
        let blob_endpoint = BlobHttpEndpoint::new(
            &http_origin,
            if http_origin.starts_with("https:") {
                extra_ca
            } else {
                None
            },
        )
        .map_err(|_| DiscoveryError::Scope)?;
        Ok(Self {
            http_origin,
            endpoint: NativeEndpoint::new(&grpc_origin, extra_ca)?,
            blob_endpoint,
        })
    }

    /// selected_http_origin must be the current native HTTP transport selection,
    /// not a renderer-provided override. Reject a stale/mismatched configuration
    /// before making a connection or releasing the JWT.
    pub async fn discover(
        self,
        selected_http_origin: &str,
        jwt: Zeroizing<String>,
    ) -> Result<DiscoveredAccount> {
        if origin(selected_http_origin)? != self.http_origin {
            return Err(DiscoveryError::Scope);
        }
        let (mut client, state) = NativeV1Client::discover(self.endpoint, jwt).await?;
        let scope = StoreScope::new(&self.http_origin, &state.user_id)
            .map_err(|_| DiscoveryError::Scope)?;
        client.bind_store_scope(scope.clone());
        Ok(DiscoveredAccount {
            client,
            state,
            scope,
            ready: false,
            blob_endpoint: self.blob_endpoint,
        })
    }
}

/// Owns the authenticated transport and a scope derived from its server response.
/// Auth failure cannot produce this object or select/create any account database.
pub struct DiscoveredAccount {
    client: NativeV1Client,
    state: OwnDeviceState,
    scope: StoreScope,
    ready: bool,
    blob_endpoint: BlobHttpEndpoint,
}
impl DiscoveredAccount {
    #[cfg(test)]
    pub(super) fn client_for_test(&self, attempt: &ReplacementAttempt) -> NativeV1Client {
        self.client.for_replacement(attempt.credential())
    }
    pub fn scope(&self) -> StoreScope {
        self.scope.clone()
    }
    pub fn state(&self) -> &OwnDeviceState {
        &self.state
    }
    pub fn reconcile(&self, local: &mut DeviceProvisioning) -> Result<Reconciliation> {
        if local.scope() != &self.scope {
            return Err(DiscoveryError::Scope);
        }
        Ok(local.reconcile(&self.state)?)
    }

    /// Explicit initial setup/resume, off the UI thread. Existing pending keys and
    /// exact request IDs are reused. A missing/foreign server generation returns
    /// a blocked observation without registration, publication or local mutation.
    /// Every network await is outside the local SQL transaction. Cancellation or
    /// an unknown RPC outcome leaves durable requests for the next invocation.
    pub async fn resume_initial(
        &mut self,
        local: &mut DeviceProvisioning,
    ) -> Result<Reconciliation> {
        self.ready = false;
        if local.scope() != &self.scope {
            return Err(DiscoveryError::Scope);
        }
        self.state = self.client.own_device().await?;
        let observed = self.reconcile(local)?;
        if matches!(
            observed,
            Reconciliation::ReplacementRequired { .. }
                | Reconciliation::DeviceUnavailable { .. }
                | Reconciliation::ReplacementPending { .. }
        ) {
            return Ok(observed);
        }
        let attempt = local.prepare_registration()?;
        self.client.use_credential(attempt.credential());
        let own = if let Some(bound) = attempt.bound_device {
            bound
        } else {
            let receipt = self.client.register(&attempt.request).await?;
            local.bind_registration(&attempt.request.registration_request_id, &receipt)?;
            receipt
                .active_device
                .and_then(|d| d.reference)
                .ok_or(RpcError::Invalid)?
        };
        self.client.bind_generation(&own)?;
        finish_publication(&mut self.client, local).await?;
        self.state = self.client.own_device().await?;
        let observed = self.reconcile(local)?;
        self.ready = observed == Reconciliation::Ready;
        Ok(observed)
    }

    /// Explicit user action. Do not rebase a CAS after refreshing GetOwn. Changing
    /// an existing pending target additionally compares its displayed request ID.
    pub async fn begin_replacement(
        &mut self,
        local: &mut DeviceProvisioning,
        expected_generation: &str,
        superseded_request_id: Option<&str>,
    ) -> Result<ReplacementAttempt> {
        self.ready = false;
        if local.scope() != &self.scope {
            return Err(DiscoveryError::Scope);
        }
        self.state = self.client.own_device().await?;
        if self
            .state
            .active_device
            .as_ref()
            .and_then(|d| d.reference.as_ref())
            .is_none_or(|own| own.generation_id != expected_generation)
        {
            return Err(DiscoveryError::ExpectedGenerationChanged);
        }
        Ok(local.prepare_replacement(expected_generation, superseded_request_id)?)
    }

    /// Re-enter the password for the exact saved operation, including after an
    /// unknown remote COMMIT. Once binding commits, ordinary login uses device
    /// authorization and publication receipts without asking for a password.
    pub async fn resume_replacement(
        &mut self,
        local: &mut DeviceProvisioning,
        password: Zeroizing<String>,
    ) -> Result<Reconciliation> {
        self.ready = false;
        if local.scope() != &self.scope {
            return Err(DiscoveryError::Scope);
        }
        let attempt = local.replacement_attempt()?;
        let mut client = self.client.for_replacement(attempt.credential());
        let receipt = client.replace(&attempt, password).await?;
        local.bind_replacement(&attempt.request().registration_request_id, &receipt)?;
        let own = receipt
            .active_device
            .and_then(|d| d.reference)
            .ok_or(RpcError::Invalid)?;
        client.bind_generation(&own)?;
        // New binding is durable, even if the following publication RPC fails.
        self.client = client;
        finish_publication(&mut self.client, local).await?;
        self.state = self.client.own_device().await?;
        let observed = self.reconcile(local)?;
        self.ready = observed == Reconciliation::Ready;
        Ok(observed)
    }

    /// Ready means initial keys are published, not that E2EE messaging is enabled.
    /// Subsequent server operations still authorize JWT/credential/generation.
    pub fn device_client(&mut self) -> Result<&mut NativeV1Client> {
        if !self.ready {
            return Err(DiscoveryError::NotReady);
        }
        Ok(&mut self.client)
    }

    /// Native runtime handoff, off the UI thread. Preserve the same credential,
    /// connection and protected store; this does not authorize a new generation.
    pub(in crate::encryption) fn into_ready_client(
        self,
        local: &mut DeviceProvisioning,
    ) -> Result<(
        NativeV1Client,
        crate::proto::e2ee::DeviceRef,
        BlobHttpEndpoint,
    )> {
        if !self.ready || self.reconcile(local)? != Reconciliation::Ready {
            return Err(DiscoveryError::NotReady);
        }
        let own = local.ready_device()?;
        if self.client.context()?.device_generation_id != own.generation_id {
            return Err(DiscoveryError::Scope);
        }
        Ok((self.client, own, self.blob_endpoint))
    }
}

async fn finish_publication(
    client: &mut NativeV1Client,
    local: &mut DeviceProvisioning,
) -> Result<()> {
    let mut sent = false;
    while let Some(request) = local.next_upload()? {
        let receipt = client.upload(&request).await?;
        local.acknowledge_upload(&receipt)?;
        sent = true;
    }
    if !sent {
        let request = local.publication_probe()?;
        let receipt = client.upload(&request).await?;
        local.acknowledge_upload(&receipt)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "discovery_tests.rs"]
mod tests;
