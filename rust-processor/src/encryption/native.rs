//! Common authenticated registry and retained v1 DATA regression transport.
//! Production DATA uses `epoch_transport`; v1 DATA endpoints are retired in Go.
//! Trusted native code only, never renderer IPC.
//! Callers persist requests/private material before RPC and commit verified replies
//! through the account owner. No implicit retry, identity replacement, key deletion,
//! or authentication recovery occurs in this layer.
use super::{
    contract::{
        MAX_REQUEST_BYTES, MAX_RESPONSE_BYTES, envelope_digest, validate_acknowledgements,
        validate_conversation, validate_pending, validate_send, validate_send_receipt,
        validate_status_request, validate_statuses,
    },
    prekey_maintenance::VerifiedPreKeyInventory,
    protected::StoreScope,
    provisioning::ReplacementAttempt,
    publication::{validate_upload, validate_upload_receipt},
    signal_state::{
        ConversationCreation, ConversationLookup, CryptoCoordinator, CryptoError, DurableAck,
        OutgoingPlan, PreKeyRefillUpload, VerifiedAck, VerifiedConversationCreation,
        VerifiedConversationLookup, VerifiedMessageStatuses, VerifiedPreKeyRefillUpload,
        VerifiedSendReceipt,
    },
};
use crate::proto::e2ee::{
    AcquireBundleRequest, AcquiredBundle, Capabilities, ClientContext, Conversation,
    CreateConversationRequest, DeviceRef, Empty, Failure, FailureReason, GetConversationRequest,
    GetMessageStatusesRequest, GetPendingMessagesRequest, GetPreKeyInventoryRequest,
    MessageReference, MessageStatuses, OwnDeviceState, PendingMessages, RegisterDeviceRequest,
    ReplaceOwnDeviceRequest, SendMessageRequest, SendReceipt, UploadPreKeysRequest, UploadReceipt,
    messenger_client::MessengerClient,
};
use prost::Message;
use sha2::{Digest, Sha256};
use std::{error::Error, time::Duration};
use tonic::{
    Code, Request,
    metadata::{Ascii, MetadataValue},
    transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
};
use uuid::{Uuid, Variant};
use zeroize::{Zeroize, Zeroizing};

// Redact the generated password-bearing type and clear every dropped instance,
// including client/codec clones. The password never enters a local journal.
impl std::fmt::Debug for ReplaceOwnDeviceRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ReplaceOwnDeviceRequest([redacted])")
    }
}
impl Drop for ReplaceOwnDeviceRequest {
    fn drop(&mut self) {
        self.current_password.zeroize();
    }
}

const TIMEOUT: Duration = Duration::from_secs(10);
#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcError {
    #[error("invalid native E2EE configuration, request or response")]
    Invalid,
    #[error("E2EE connection failed; the operation outcome may be unknown")]
    Transport,
    /// Proven by the local transport timer, never by a remote status/message.
    /// The server may already have committed: retry only the saved operation.
    #[error("native E2EE RPC deadline exceeded; the operation outcome may be unknown")]
    LocalDeadline,
    // Missing/unknown/malformed details retain a generic code. Never infer key
    // deletion, device replacement or acquisition renewal from an error alone.
    #[error("E2EE RPC failed with code {0:?}")]
    Status(Code),
    #[error("E2EE RPC failed with code {code:?}, reason {reason:?}")]
    Failure { code: Code, reason: FailureReason },
}

#[derive(Debug, thiserror::Error)]
pub enum ConversationCreationError {
    #[error("conversation creation RPC failed; retry the saved operation")]
    Rpc(#[from] RpcError),
    #[error("conversation creation could not be committed locally")]
    Crypto(#[from] CryptoError),
}

#[derive(Debug, thiserror::Error)]
pub enum ConversationLookupError {
    #[error("conversation lookup RPC failed; local state is unchanged")]
    Rpc(#[from] RpcError),
    #[error("conversation lookup could not be committed locally")]
    Crypto(#[from] CryptoError),
}

// Wire-compatible subset of google.rpc.Status/Any. Unknown fields (including
// server-supplied diagnostic text) are ignored, not retained in public errors.
#[derive(Message)]
struct StatusDetails {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(message, repeated, tag = "3")]
    details: Vec<StatusDetail>,
}
#[derive(Message)]
struct StatusDetail {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}
impl RpcError {
    fn from_status(status: tonic::Status) -> Self {
        // tonic0.14 maps its transport timer to Cancelled but retains the typed
        // local source. A peer cannot send this Rust source over gRPC. Inspect a
        // bounded chain, discard diagnostics, and never infer a timer from text,
        // a remote code, or elapsed wall-clock time. Dropping a caller future is
        // the owner's stop path and does not manufacture this RPC error.
        let mut source = status.source();
        for _ in 0..16 {
            let Some(error) = source else { break };
            if error.is::<tonic::TimeoutExpired>() {
                return Self::LocalDeadline;
            }
            source = error.source();
        }
        let code = status.code();
        let fallback = Self::Status(code);
        if status.details().len() > 2048 {
            return fallback;
        }
        let Ok(details) = StatusDetails::decode(status.details()) else {
            return fallback;
        };
        if details.code != code as i32 || details.details.len() != 1 {
            return fallback;
        }
        let detail = &details.details[0];
        if detail.type_url != "type.googleapis.com/zchat.e2ee.v1.Failure" {
            return fallback;
        }
        let Ok(failure) = Failure::decode(detail.value.as_slice()) else {
            return fallback;
        };
        let Ok(reason) = FailureReason::try_from(failure.reason) else {
            return fallback;
        };
        let expected = match reason {
            FailureReason::SessionInvalid
            | FailureReason::DeviceCredentialInvalid
            | FailureReason::PasswordInvalid => Code::Unauthenticated,
            FailureReason::GenerationChanged | FailureReason::AcquisitionExpired => {
                Code::FailedPrecondition
            }
            FailureReason::Unspecified => return fallback,
        };
        if code != expected {
            return fallback;
        }
        Self::Failure { code, reason }
    }
}
type Result<T> = std::result::Result<T, RpcError>;
fn validate_jwt(jwt: &str) -> Result<()> {
    if jwt.is_empty()
        || jwt.len() > 8192
        || jwt
            .bytes()
            .any(|b| !b.is_ascii_alphanumeric() && !matches!(b, b'.' | b'-' | b'_'))
    {
        return Err(RpcError::Invalid);
    }
    Ok(())
}

pub(super) fn validate_own(state: &OwnDeviceState) -> Result<()> {
    if !uuid4(&state.user_id) {
        return Err(RpcError::Invalid);
    }
    if let Some(device) = &state.active_device {
        let own = device.reference.as_ref().ok_or(RpcError::Invalid)?;
        if own.user_id != state.user_id
            || own.device_id != 1
            || !uuid4(&own.generation_id)
            || !(1..=16383).contains(&device.registration_id)
            || device.identity_key.len() != 33
            || device.identity_key[0] != 5
            || device.public_material_revision > i64::MAX as u64
            || device.public_material_ready && device.public_material_revision == 0
        {
            return Err(RpcError::Invalid);
        }
    }
    Ok(())
}

fn authenticated_request<T: Message>(
    message: T,
    jwt: &str,
    credential: Option<&[u8; 32]>,
) -> Result<Request<T>> {
    if message.encoded_len() > MAX_REQUEST_BYTES {
        return Err(RpcError::Invalid);
    }
    let mut request = Request::new(message);
    request.set_timeout(TIMEOUT);
    let header = Zeroizing::new(format!("Bearer {jwt}"));
    let mut auth =
        MetadataValue::<Ascii>::try_from(header.as_str()).map_err(|_| RpcError::Invalid)?;
    auth.set_sensitive(true);
    request.metadata_mut().insert("authorization", auth);
    if let Some(credential) = credential {
        let mut credential = MetadataValue::from_bytes(credential);
        credential.set_sensitive(true);
        request
            .metadata_mut()
            .insert_bin("x-zchat-device-credential-bin", credential);
    }
    Ok(request)
}

async fn connect(endpoint: NativeEndpoint) -> Result<(MessengerClient<Channel>, Channel)> {
    let channel = endpoint
        .endpoint
        .connect()
        .await
        .map_err(|_| RpcError::Transport)?;
    Ok((
        MessengerClient::new(channel.clone())
            .max_encoding_message_size(MAX_REQUEST_BYTES)
            .max_decoding_message_size(MAX_RESPONSE_BYTES),
        channel,
    ))
}

fn uuid4(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|u| {
        u.get_version_num() == 4 && u.get_variant() == Variant::RFC4122 && u.to_string() == value
    })
}

/// Endpoint/trust anchor come from trusted native configuration. HTTPS verifies
/// certificate chain and hostname; HTTP is permitted only for loopback development.
pub struct NativeEndpoint {
    endpoint: Endpoint,
}
impl NativeEndpoint {
    pub fn new(origin: &str, extra_ca_pem: Option<&[u8]>) -> Result<Self> {
        let url = url::Url::parse(origin).map_err(|_| RpcError::Invalid)?;
        let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
            || !(url.scheme() == "https" || url.scheme() == "http" && local)
            || extra_ca_pem
                .is_some_and(|ca| ca.is_empty() || ca.len() > 65536 || url.scheme() != "https")
        {
            return Err(RpcError::Invalid);
        }
        let mut endpoint = Endpoint::from_shared(url.origin().ascii_serialization())
            .map_err(|_| RpcError::Invalid)?
            .connect_timeout(Duration::from_secs(5))
            .timeout(TIMEOUT)
            .concurrency_limit(8)
            .buffer_size(8);
        if url.scheme() == "https" {
            let mut tls = ClientTlsConfig::new()
                .with_native_roots()
                .timeout(Duration::from_secs(5));
            if let Some(ca) = extra_ca_pem {
                tls = tls.ca_certificate(Certificate::from_pem(ca));
            }
            endpoint = endpoint.tls_config(tls).map_err(|_| RpcError::Invalid)?;
        }
        Ok(Self { endpoint })
    }
}

// No Debug/Clone: this owner contains native-only authentication secrets.
pub struct NativeV1Client {
    client: MessengerClient<Channel>,
    channel: Channel,
    account: String,
    jwt: Zeroizing<String>,
    credential: Option<Zeroizing<[u8; 32]>>,
    generation: Option<String>,
    store_scope: Option<StoreScope>,
}

/// Created only from the typed response to this exact authenticated Acquire RPC.
/// Not serializable or renderer-constructible; local CAS/session checks are still
/// mandatory. A local clock error or generic FailedPrecondition cannot mint it.
#[derive(Clone)]
pub struct ExpiredAcquisition {
    request: AcquireBundleRequest,
}
impl ExpiredAcquisition {
    pub(super) fn request(&self) -> &AcquireBundleRequest {
        &self.request
    }
    #[cfg(test)]
    pub(super) fn test_proof(request: AcquireBundleRequest) -> Self {
        Self { request }
    }
}
pub enum AcquisitionOutcome {
    Bundle(AcquiredBundle),
    Expired(ExpiredAcquisition),
}
impl NativeV1Client {
    // Only fault probes need a caller-supplied UUID/credential. Production enters
    // through TrustedServer::discover and binds a verified protected store.
    #[cfg(test)]
    async fn connect(
        endpoint: NativeEndpoint,
        account: &str,
        jwt: Zeroizing<String>,
        credential: Zeroizing<[u8; 32]>,
    ) -> Result<Self> {
        if !uuid4(account) {
            return Err(RpcError::Invalid);
        }
        validate_jwt(&jwt)?;
        let (client, channel) = connect(endpoint).await?;
        Ok(Self {
            client,
            channel,
            account: account.into(),
            jwt,
            credential: Some(credential),
            generation: None,
            store_scope: None,
        })
    }
    /// JWT-only discovery precedes all filesystem access. No account identifier
    /// is accepted from the renderer or decoded from unverified JWT claims.
    pub(super) async fn discover(
        endpoint: NativeEndpoint,
        jwt: Zeroizing<String>,
    ) -> Result<(Self, OwnDeviceState)> {
        validate_jwt(&jwt)?;
        let (mut client, channel) = connect(endpoint).await?;
        let state = client
            .get_own_device(authenticated_request(Empty {}, &jwt, None)?)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        validate_own(&state)?;
        Ok((
            Self {
                client,
                channel,
                account: state.user_id.clone(),
                jwt,
                credential: None,
                generation: None,
                store_scope: None,
            },
            state,
        ))
    }
    pub(super) fn use_credential(&mut self, credential: &[u8; 32]) {
        self.credential = Some(Zeroizing::new(*credential));
    }
    pub(super) fn bind_store_scope(&mut self, scope: StoreScope) {
        self.store_scope = Some(scope);
    }
    pub(super) fn for_replacement(&self, credential: &[u8; 32]) -> Self {
        Self {
            client: self.client.clone(),
            channel: self.channel.clone(),
            account: self.account.clone(),
            jwt: Zeroizing::new(self.jwt.to_string()),
            credential: Some(Zeroizing::new(*credential)),
            generation: None,
            store_scope: self.store_scope.clone(),
        }
    }
    /// Only after the matching local generation bind has committed. A client
    /// instance cannot silently switch to a replacement installation.
    pub fn bind_generation(&mut self, own: &DeviceRef) -> Result<()> {
        if own.user_id != self.account
            || own.device_id != 1
            || !uuid4(&own.generation_id)
            || self
                .generation
                .as_ref()
                .is_some_and(|g| g != &own.generation_id)
        {
            return Err(RpcError::Invalid);
        }
        self.generation = Some(own.generation_id.clone());
        Ok(())
    }
    pub fn context(&self) -> Result<ClientContext> {
        Ok(ClientContext {
            protocol_version: 1,
            device_generation_id: self.generation.clone().ok_or(RpcError::Invalid)?,
        })
    }
    fn check_context(&self, context: Option<&ClientContext>) -> Result<()> {
        if context != Some(&self.context()?) {
            return Err(RpcError::Invalid);
        }
        Ok(())
    }
    fn request<T: Message>(&self, message: T) -> Result<Request<T>> {
        authenticated_request(
            message,
            &self.jwt,
            Some(self.credential.as_deref().ok_or(RpcError::Invalid)?),
        )
    }
    fn check_own(&self, state: &OwnDeviceState) -> Result<()> {
        if state.user_id != self.account {
            return Err(RpcError::Invalid);
        }
        validate_own(state)
    }
    pub async fn capabilities(&mut self) -> Result<Capabilities> {
        let req = authenticated_request(Empty {}, &self.jwt, None)?;
        let result = self
            .client
            .get_capabilities(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        if result.protocol_version != 1
            || result.max_active_devices != 1
            || !result.crypto_suites.contains(&1)
        {
            return Err(RpcError::Invalid);
        }
        Ok(result)
    }
    pub async fn own_device(&mut self) -> Result<OwnDeviceState> {
        let req = authenticated_request(Empty {}, &self.jwt, None)?;
        let result = self
            .client
            .get_own_device(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        self.check_own(&result)?;
        Ok(result)
    }
    pub async fn prekey_inventory(&mut self) -> Result<VerifiedPreKeyInventory> {
        let context = self.context()?;
        let own = DeviceRef {
            user_id: self.account.clone(),
            device_id: 1,
            generation_id: context.device_generation_id.clone(),
        };
        let scope = self.store_scope.clone().ok_or(RpcError::Invalid)?;
        let request = self.request(GetPreKeyInventoryRequest {
            context: Some(context),
        })?;
        let result = self
            .client
            .get_pre_key_inventory(request)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        VerifiedPreKeyInventory::from_rpc(scope, &own, result)
    }
    pub async fn register(&mut self, request: &RegisterDeviceRequest) -> Result<OwnDeviceState> {
        self.validate_registration(request)?;
        let req = self.request(request.clone())?;
        let result = self
            .client
            .register_device(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        self.check_registration(&result, request)?;
        Ok(result)
    }
    fn validate_registration(&self, request: &RegisterDeviceRequest) -> Result<()> {
        let mut hash = Sha256::new();
        hash.update(b"zchat.e2ee.device.v1\0");
        hash.update(self.credential.as_deref().ok_or(RpcError::Invalid)?);
        if request.protocol_version != 1
            || !uuid4(&request.registration_request_id)
            || !(1..=16383).contains(&request.registration_id)
            || request.identity_key.len() != 33
            || request.identity_key[0] != 5
            || request.credential_hash.as_slice() != hash.finalize().as_slice()
        {
            return Err(RpcError::Invalid);
        }
        Ok(())
    }
    fn check_registration(
        &self,
        result: &OwnDeviceState,
        request: &RegisterDeviceRequest,
    ) -> Result<()> {
        self.check_own(result)?;
        let device = result.active_device.as_ref().ok_or(RpcError::Invalid)?;
        if device.identity_key != request.identity_key
            || device.registration_id != request.registration_id
        {
            return Err(RpcError::Invalid);
        }
        Ok(())
    }
    pub(super) async fn replace(
        &mut self,
        attempt: &ReplacementAttempt,
        mut password: Zeroizing<String>,
    ) -> Result<OwnDeviceState> {
        let prepared = attempt.request();
        self.validate_registration(prepared)?;
        if !uuid4(attempt.expected_generation_id()) || password.is_empty() || password.len() > 72 {
            return Err(RpcError::Invalid);
        }
        let request = self.request(ReplaceOwnDeviceRequest {
            protocol_version: prepared.protocol_version,
            replacement_request_id: prepared.registration_request_id.clone(),
            expected_generation_id: attempt.expected_generation_id().into(),
            registration_id: prepared.registration_id,
            identity_key: prepared.identity_key.clone(),
            credential_hash: prepared.credential_hash.clone(),
            current_password: std::mem::take(&mut *password),
        })?;
        let result = self
            .client
            .replace_own_device(request)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        self.check_registration(&result, prepared)?;
        if result
            .active_device
            .as_ref()
            .and_then(|d| d.reference.as_ref())
            .is_none_or(|own| own.generation_id == attempt.expected_generation_id())
        {
            return Err(RpcError::Invalid);
        }
        Ok(result)
    }
    pub async fn upload(&mut self, request: &UploadPreKeysRequest) -> Result<UploadReceipt> {
        self.check_context(request.context.as_ref())?;
        validate_upload(request).map_err(|_| RpcError::Invalid)?;
        let req = self.request(request.clone())?;
        let result = self
            .client
            .upload_pre_keys(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        validate_upload_receipt(request, &result).map_err(|_| RpcError::Invalid)?;
        Ok(result)
    }
    /// Publish only a worker plan tied to this trusted server/account/installation.
    pub async fn upload_refill(
        &mut self,
        upload: &PreKeyRefillUpload,
    ) -> Result<VerifiedPreKeyRefillUpload> {
        if self.store_scope.as_ref() != Some(upload.scope())
            || upload.own().user_id != self.account
            || Some(&upload.own().generation_id) != self.generation.as_ref()
        {
            return Err(RpcError::Invalid);
        }
        let receipt = self.upload(upload.request()).await?;
        Ok(VerifiedPreKeyRefillUpload::from_rpc(
            upload.clone(),
            receipt,
        ))
    }
    fn conversation(&self, result: Conversation, id: Option<&str>) -> Result<Conversation> {
        if id.is_some_and(|id| id != result.conversation_id)
            || result.own_device.as_ref().is_none_or(|own| {
                own.user_id != self.account
                    || own.device_id != 1
                    || Some(&own.generation_id) != self.generation.as_ref()
            })
        {
            return Err(RpcError::Invalid);
        }
        validate_conversation(&result).map_err(|_| RpcError::Invalid)?;
        Ok(result)
    }
    /// Normal entry point: COMMIT the exact request, make the RPC outside SQLite,
    /// then COMMIT the verified response before returning it to the application.
    pub async fn create_conversation(
        &mut self,
        worker: &CryptoCoordinator,
        peer_user_id: i64,
    ) -> std::result::Result<Conversation, ConversationCreationError> {
        let attempt = worker.prepare_conversation_creation(peer_user_id).await?;
        let verified = self.create_prepared_conversation(&attempt).await?;
        Ok(worker.accept_conversation_creation(verified).await?)
    }
    /// Also used after an explicit new-dialogue CAS. The opaque attempt comes
    /// only from the account worker; callers cannot replace its peer/request ID.
    pub async fn create_prepared_conversation(
        &mut self,
        attempt: &ConversationCreation,
    ) -> Result<VerifiedConversationCreation> {
        if attempt.own().user_id != self.account
            || self.store_scope.as_ref() != Some(attempt.store_scope())
        {
            return Err(RpcError::Invalid);
        }
        let response = self.create_conversation_request(attempt.request()).await?;
        Ok(VerifiedConversationCreation::from_rpc(
            attempt.clone(),
            response,
        ))
    }
    async fn create_conversation_request(
        &mut self,
        request: &CreateConversationRequest,
    ) -> Result<Conversation> {
        self.check_context(request.context.as_ref())?;
        if !uuid4(&request.creation_request_id) || request.peer_user_id <= 0 {
            return Err(RpcError::Invalid);
        }
        let req = self.request(request.clone())?;
        let result = self
            .client
            .create_conversation(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        self.conversation(result, None)
    }
    /// Persist an authoritative response before returning it. This path is also
    /// used to resolve an incoming delivery's conversation ID without Create.
    pub async fn get_conversation(
        &mut self,
        worker: &CryptoCoordinator,
        conversation_id: &str,
    ) -> std::result::Result<Conversation, ConversationLookupError> {
        let attempt = worker
            .prepare_conversation_lookup(conversation_id.into())
            .await?;
        let verified = self.get_prepared_conversation(&attempt).await?;
        Ok(worker.accept_conversation_lookup(verified).await?)
    }
    pub async fn get_prepared_conversation(
        &mut self,
        attempt: &ConversationLookup,
    ) -> Result<VerifiedConversationLookup> {
        if attempt.own().user_id != self.account
            || self.store_scope.as_ref() != Some(attempt.store_scope())
        {
            return Err(RpcError::Invalid);
        }
        let response = self.get_conversation_request(attempt.request()).await?;
        Ok(VerifiedConversationLookup::from_rpc(
            attempt.clone(),
            response,
        ))
    }
    async fn get_conversation_request(
        &mut self,
        request: &GetConversationRequest,
    ) -> Result<Conversation> {
        self.check_context(request.context.as_ref())?;
        if !uuid4(&request.conversation_id) {
            return Err(RpcError::Invalid);
        }
        let req = self.request(request.clone())?;
        let result = self
            .client
            .get_conversation(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        self.conversation(result, Some(&request.conversation_id))
    }
    /// Raw transport is private. Production sends use a durable worker plan.
    async fn send(&mut self, request: &SendMessageRequest) -> Result<SendReceipt> {
        self.check_context(request.context.as_ref())?;
        validate_send(request).map_err(|_| RpcError::Invalid)?;
        if self.store_scope.is_none()
            || request
                .envelope
                .as_ref()
                .and_then(|e| e.sender.as_ref())
                .is_none_or(|sender| sender.user_id != self.account || sender.device_id != 1)
        {
            return Err(RpcError::Invalid);
        }
        let req = self.request(request.clone())?;
        let result = self
            .client
            .send_message(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        validate_send_receipt(request, &result).map_err(|_| RpcError::Invalid)?;
        Ok(result)
    }

    fn check_outgoing(&self, plan: &OutgoingPlan) -> Result<()> {
        if self.store_scope.as_ref() != Some(plan.store_scope()) {
            return Err(RpcError::Invalid);
        }
        self.check_context(plan.request().context.as_ref())?;
        validate_send(plan.request()).map_err(|_| RpcError::Invalid)?;
        if plan
            .request()
            .envelope
            .as_ref()
            .and_then(|e| e.sender.as_ref())
            .is_none_or(|own| own.user_id != self.account || own.device_id != 1)
        {
            return Err(RpcError::Invalid);
        }
        Ok(())
    }

    /// A lost response keeps the same plan/UUID/ciphertext. Only worker receipt
    /// acceptance commits local progress; this adapter never changes the outbox.
    pub async fn send_outbox(&mut self, plan: &OutgoingPlan) -> Result<VerifiedSendReceipt> {
        self.check_outgoing(plan)?;
        let receipt = self.send(plan.request()).await?;
        Ok(VerifiedSendReceipt::from_rpc(plan.clone(), receipt))
    }

    pub async fn refresh_outgoing(
        &mut self,
        plans: &[OutgoingPlan],
    ) -> Result<VerifiedMessageStatuses> {
        let mut references = Vec::new();
        // Bound before cloning/materializing any caller-provided batch.
        if plans.is_empty() || plans.len() > super::contract::MAX_ACKNOWLEDGEMENTS {
            return Err(RpcError::Invalid);
        }
        for plan in plans {
            self.check_outgoing(plan)?;
            let envelope = plan.request().envelope.as_ref().ok_or(RpcError::Invalid)?;
            references.push(MessageReference {
                message_id: envelope.message_id.clone(),
                envelope_digest: envelope_digest(envelope)
                    .map_err(|_| RpcError::Invalid)?
                    .to_vec(),
            });
        }
        let request = GetMessageStatusesRequest {
            context: Some(self.context()?),
            messages: references,
        };
        let response = self.statuses(&request).await?;
        Ok(VerifiedMessageStatuses::from_rpc(plans.to_vec(), response))
    }

    /// Fetch earliest unacknowledged data. No cursor advance, auto-ACK, local
    /// history mutation or worker/session creation happens in this adapter.
    pub async fn pending(&mut self) -> Result<PendingMessages> {
        if self.store_scope.is_none() {
            return Err(RpcError::Invalid);
        }
        let context = self.context()?;
        let own = DeviceRef {
            user_id: self.account.clone(),
            device_id: 1,
            generation_id: context.device_generation_id.clone(),
        };
        let request = self.request(GetPendingMessagesRequest {
            context: Some(context),
        })?;
        let result = self
            .client
            .get_pending_messages(request)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        validate_pending(&result, &own).map_err(|_| RpcError::Invalid)?;
        Ok(result)
    }

    /// Separate optional hint connection using this verified account/generation.
    /// Run it with DeliveryRunner::run_with_wakeups; durable polling continues.
    pub fn inbox_wakeups(&self) -> Result<InboxWakeups> {
        InboxWakeups::new(self)
    }

    pub async fn statuses(
        &mut self,
        request: &GetMessageStatusesRequest,
    ) -> Result<MessageStatuses> {
        self.check_context(request.context.as_ref())?;
        if self.store_scope.is_none() {
            return Err(RpcError::Invalid);
        }
        validate_status_request(request).map_err(|_| RpcError::Invalid)?;
        let req = self.request(request.clone())?;
        let result = self
            .client
            .get_message_statuses(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        validate_statuses(request, &result).map_err(|_| RpcError::Invalid)?;
        Ok(result)
    }

    /// Only an opaque post-COMMIT worker proof can reach the production ACK RPC.
    /// On transport failure, replay it or recover it from the protected journal.
    pub async fn acknowledge(&mut self, proof: &DurableAck) -> Result<VerifiedAck> {
        let request = proof.request();
        self.check_context(request.context.as_ref())?;
        if self.store_scope.as_ref() != Some(proof.store_scope()) {
            return Err(RpcError::Invalid);
        }
        validate_acknowledgements(request).map_err(|_| RpcError::Invalid)?;
        let req = self.request(request.clone())?;
        self.client
            .acknowledge_messages(req)
            .await
            .map_err(RpcError::from_status)?;
        Ok(VerifiedAck::from_rpc(proof.clone()))
    }
    pub async fn acquire(&mut self, request: &AcquireBundleRequest) -> Result<AcquiredBundle> {
        self.check_context(request.context.as_ref())?;
        let peer = request.expected_peer.as_ref().ok_or(RpcError::Invalid)?;
        if !uuid4(&request.acquisition_id)
            || !uuid4(&request.conversation_id)
            || !uuid4(&peer.user_id)
            || !uuid4(&peer.generation_id)
            || peer.device_id != 1
            || peer.user_id == self.account
        {
            return Err(RpcError::Invalid);
        }
        let req = self.request(request.clone())?;
        let result = self
            .client
            .acquire_bundle(req)
            .await
            .map_err(RpcError::from_status)?
            .into_inner();
        if result.encoded_len() > 8192
            || result.acquisition_id != request.acquisition_id
            || result.conversation_id != request.conversation_id
            || result.device.as_ref().and_then(|d| d.reference.as_ref()) != Some(peer)
            || result.signed_pre_key.is_none()
            || result.kyber_pre_key.is_none()
            || result.not_after_unix_ms <= 0
        {
            return Err(RpcError::Invalid);
        }
        // Exact snapshots may have expired; the worker checks lease immediately
        // before fresh bootstrap and verifies signatures/trust within its TX.
        Ok(result)
    }

    pub async fn acquire_for_bootstrap(
        &mut self,
        request: &AcquireBundleRequest,
    ) -> Result<AcquisitionOutcome> {
        match self.acquire(request).await {
            Ok(bundle) => Ok(AcquisitionOutcome::Bundle(bundle)),
            Err(RpcError::Failure {
                code: Code::FailedPrecondition,
                reason: FailureReason::AcquisitionExpired,
            }) => Ok(AcquisitionOutcome::Expired(ExpiredAcquisition {
                request: request.clone(),
            })),
            Err(error) => Err(error),
        }
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "native_deadline_tests.rs"]
mod deadline_tests;

#[cfg(test)]
#[path = "native_account_tests.rs"]
mod account_tests;

#[cfg(test)]
#[path = "native_file_tests.rs"]
mod file_tests;

#[path = "native_wakeups.rs"]
mod wakeups;
pub use wakeups::InboxWakeups;

#[path = "native_epoch.rs"]
mod epoch;
pub use epoch::{
    BlobCiphertextChunk, BlobCiphertextSink, BlobGetResponse, BlobHeadResponse, BlobHttpEndpoint,
    BlobHttpError, BlobPutResponse, BlobReadError, BlobReadRequest, BlobSinkError,
    BlobTransferCancellation, EpochAcquisitionOutcome, EpochBlobControlOutcome, EpochBlobLease,
    EpochBlobSendFailure, EpochConflict, EpochDelivery, EpochExpiredAcquisition, EpochFileStatuses,
    EpochObservation, EpochPreparedSendOutcome, EpochRecoveryError, EpochSendOutcome,
    EpochStatuses, NativeBlobHttp, NativeBlobUploadTransport, NativeEpochClient,
};

#[cfg(test)]
#[path = "outage_tests.rs"]
mod outage_tests;

#[cfg(test)]
#[path = "wakeup_tests.rs"]
mod wakeup_tests;
