//! Bounded native-only account requests. No raw worker, scope, proof or credential
//! leaves this interface. First-send bootstrap uses the same serial network lane.
use super::*;
use crate::{
    encryption::{
        epoch_rpc,
        signal_state::{EpochRecoveryDecision, EpochTextPreparation, EpochTextQueued},
    },
    proto::{e2ee as common, e2ee_v2 as v2},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, oneshot};
use zeroize::Zeroizing;

#[path = "account_downloads.rs"]
mod downloads;
pub(super) use downloads::serve as download;
#[cfg(feature = "compression")]
#[path = "account_photos.rs"]
pub(super) mod photos;

struct TextRequest {
    chat: String,
    message: String,
    text: Zeroizing<String>,
}

/// Public conversation metadata committed to the protected index. No key,
/// scope, ratchet or authorization proof is exposed to a renderer adapter.
#[derive(Clone, Debug)]
pub struct DialogView {
    pub conversation_id: String,
    pub peer_user_id: String,
    pub state: common::ConversationState,
    pub close_reason: Option<common::ConversationCloseReason>,
    pub closed_at_unix_ms: i64,
}
impl DialogView {
    fn committed(v: common::Conversation) -> Result<Self> {
        Ok(Self {
            conversation_id: v.conversation_id,
            peer_user_id: v
                .peer_device
                .and_then(|p| p.reference)
                .ok_or(CryptoError::Invalid)?
                .user_id,
            state: common::ConversationState::try_from(v.state)
                .map_err(|_| CryptoError::Invalid)?,
            close_reason: if v.close_reason == 0 {
                None
            } else {
                Some(
                    common::ConversationCloseReason::try_from(v.close_reason)
                        .map_err(|_| CryptoError::Invalid)?,
                )
            },
            closed_at_unix_ms: v.closed_at_unix_ms,
        })
    }
}
#[derive(Clone, Debug)]
pub struct ConversationView {
    pub crypto_epoch: u64,
    pub receive_only: bool,
    pub sending_frozen: bool,
}
#[derive(Clone, Debug)]
pub struct RecoveryView {
    pub proposal_id: String,
    pub state: Option<common::RecoveryState>,
    pub own_proposal: bool,
    pub decision: Option<EpochRecoveryDecision>,
    pub sending_frozen: bool,
    pub retired_by: Option<String>,
}
#[derive(Clone, Debug)]
pub struct OutgoingView {
    pub crypto_epoch: u64,
    pub stored_on_server: bool,
    pub status: Option<common::MessageState>,
    pub removal: Option<common::MessageState>,
    pub sealed_unsent: bool,
}
/// Plaintext is returned only for a user text, never a service confirmation.
/// No Debug/serialization: the future IPC adapter chooses explicit UI fields.
pub struct ReceivedText {
    pub message_id: String,
    pub conversation_id: String,
    pub crypto_epoch: u64,
    pub sender_user_id: String,
    pub created_at_unix_ms: i64,
    pub text: String,
}
enum Request {
    AttachmentCatalog(String),
    AttachmentInput(String, String),
    #[cfg(feature = "compression")]
    Photo(Box<photos::Input>),
    PreparedAttachment(String),
    FileProgress(String),
    FileAckStatus(String),
    Download {
        conversation: Option<String>,
        message: String,
    },
    OpenAttachment(String),
    OpenArchiveAttachment(Option<String>, String, String),
    RemoveAttachment(String, String, String),
    Read(crate::encryption::signal_state::ViewRequest),
    Send(TextRequest),
    Create(i64),
    CreateAfterClosed(i64, String),
    Lookup(String),
    Conversation(String),
    Recoveries(String),
    RecoveryOverview(String),
    Propose(String, Option<(u64, Option<String>)>),
    Decide(String, String, EpochRecoveryDecision, Option<u64>),
    Outgoing(String),
    Received(String),
}
enum Response {
    AttachmentCatalog,
    AttachmentInput(crate::encryption::signal_state::EpochAttachmentInputState),
    #[cfg(feature = "compression")]
    Photo(crate::encryption::signal_state::EpochAttachmentPreparation),
    PreparedAttachment(Option<crate::encryption::signal_state::EpochAttachmentPlan>),
    FileProgress(Box<crate::encryption::signal_state::EpochFileProgress>),
    FileAckStatus(Option<i64>),
    AttachmentCached,
    AttachmentRemoved,
    AttachmentFile(crate::encryption::signal_state::EpochAttachmentFile),
    Read(crate::encryption::signal_state::ViewPage),
    Text(EpochTextQueued),
    Dialog(DialogView),
    Conversation(Option<ConversationView>),
    Recoveries(Vec<RecoveryView>),
    RecoveryOverview(crate::encryption::signal_state::EpochRecoveryOverview),
    Operation(String),
    Outgoing(OutgoingView),
    Received(Option<ReceivedText>),
}
pub(super) struct Command {
    request: Request,
    reply: oneshot::Sender<Result<Response>>,
    _permit: OwnedSemaphorePermit,
}
/// Clones can request local work but cannot extend the account owner's lifetime.
#[derive(Clone)]
pub struct AccountClient {
    sender: mpsc::Sender<Command>,
    network: mpsc::Sender<Command>,
    downloads: Option<mpsc::Sender<Command>>,
    #[cfg(feature = "compression")]
    photos: Option<mpsc::Sender<Command>>,
    budget: Arc<Semaphore>,
    status: watch::Receiver<AccountStatus>,
    stop: watch::Receiver<bool>,
}
/// Latest-value subscription without exposing a watch borrow guard that a
/// caller could accidentally hold across shutdown or another await.
pub struct AccountUpdates(watch::Receiver<AccountStatus>);
impl AccountUpdates {
    pub async fn changed(&mut self) -> Option<AccountStatus> {
        self.0.changed().await.ok()?;
        Some(*self.0.borrow_and_update())
    }
}
impl AccountClient {
    /// Explicit new attachment input may discover an initial authenticated
    /// catalogue; this never acquires/reset ratchets or signs recovery consent.
    pub async fn ensure_attachment_catalog(&self, chat: &str) -> Result<()> {
        if self.conversation(chat).await?.is_some() {
            return Ok(());
        }
        match self.request(Request::AttachmentCatalog(id(chat)?)).await? {
            Response::AttachmentCatalog => Ok(()),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn attachment_input_state(
        &self,
        chat: &str,
        message: &str,
    ) -> Result<crate::encryption::signal_state::EpochAttachmentInputState> {
        match self
            .request(Request::AttachmentInput(id(chat)?, id(message)?))
            .await?
        {
            Response::AttachmentInput(state) => Ok(state),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Protected sender metadata only; this never starts a network request or
    /// returns a file/private descriptor/Send permission.
    pub async fn attachment_file_progress(
        &self,
        message: &str,
    ) -> Result<crate::encryption::signal_state::EpochFileProgress> {
        match self.request(Request::FileProgress(id(message)?)).await? {
            Response::FileProgress(p) => Ok(*p),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Saved server statement for a verified local cache; no implicit download
    /// or proof preparation. Uses the same scoped local command admission.
    pub async fn attachment_file_ack_status(&self, message: &str) -> Result<Option<i64>> {
        match self.request(Request::FileAckStatus(id(message)?)).await? {
            Response::FileAckStatus(at) => Ok(at),
            _ => Err(AccountError::Runtime),
        }
    }
    pub(super) fn new(
        sender: mpsc::Sender<Command>,
        network: mpsc::Sender<Command>,
        downloads: Option<mpsc::Sender<Command>>,
        status: watch::Receiver<AccountStatus>,
        stop: watch::Receiver<bool>,
    ) -> Self {
        Self {
            sender,
            network,
            downloads,
            #[cfg(feature = "compression")]
            photos: None,
            budget: Arc::new(Semaphore::new(COMMAND_CAPACITY)),
            status,
            stop,
        }
    }
    #[cfg(feature = "compression")]
    pub(super) fn with_photos(mut self, photos: mpsc::Sender<Command>) -> Self {
        self.photos = Some(photos);
        self
    }
    pub fn status(&self) -> AccountStatus {
        *self.status.borrow()
    }
    pub fn subscribe(&self) -> AccountUpdates {
        AccountUpdates(self.status.clone())
    }
    async fn request(&self, request: Request) -> Result<Response> {
        if self.stopping() {
            return Err(AccountError::Stopped);
        }
        match self.status().phase {
            AccountPhase::RecoveryIndexes | AccountPhase::HistoryIndex | AccountPhase::KeyIds => {
                return Err(AccountError::Initializing);
            }
            AccountPhase::Stopped | AccountPhase::Failed => return Err(AccountError::Stopped),
            AccountPhase::Running => {}
        }
        let permit = self
            .budget
            .clone()
            .try_acquire_owned()
            .map_err(|_| AccountError::Busy)?;
        #[cfg(feature = "compression")]
        let photo_sender = if matches!(request, Request::Photo(_)) {
            Some(
                self.photos
                    .as_ref()
                    .ok_or(AccountError::AttachmentUnavailable)?,
            )
        } else {
            None
        };
        #[cfg(not(feature = "compression"))]
        let photo_sender: Option<&mpsc::Sender<Command>> = None;
        let sender = if let Some(sender) = photo_sender {
            sender
        } else if matches!(request, Request::Download { .. }) {
            self.downloads
                .as_ref()
                .ok_or(AccountError::AttachmentUnavailable)?
        } else if matches!(
            request,
            Request::Create(_)
                | Request::CreateAfterClosed(..)
                | Request::Lookup(_)
                | Request::AttachmentCatalog(_)
        ) {
            &self.network
        } else {
            &self.sender
        };
        let (reply, response) = oneshot::channel();
        sender
            .try_send(Command {
                request,
                reply,
                _permit: permit,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => AccountError::Busy,
                mpsc::error::TrySendError::Closed(_) => AccountError::Stopped,
            })?;
        let mut stop = self.stop.clone();
        let result = tokio::select! {
            biased;
            // Preserve an already completed typed error; successful replies
            // are fenced again below, so no late plaintext/cache result escapes.
            r=response=>r.map_err(|_|AccountError::Stopped)?,
            _=super::super::delivery_runner::cancelled(&mut stop)=>return Err(AccountError::Stopped),
        };
        if self.stopping() && result.is_ok() {
            return Err(AccountError::Stopped);
        }
        result
    }
    fn stopping(&self) -> bool {
        *self.stop.borrow() || self.stop.has_changed().is_err()
    }
    /// Resolve a retained, unpromoted input by UUID without opening a codec or
    /// supplying source bytes. Promoted messages use `outgoing` instead.
    pub async fn prepared_attachment(
        &self,
        message: &str,
    ) -> Result<Option<crate::encryption::signal_state::EpochAttachmentPlan>> {
        match self
            .request(Request::PreparedAttachment(id(message)?))
            .await?
        {
            Response::PreparedAttachment(plan) => Ok(plan),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Explicit bounded download/retry of ONE saved incoming message. Success
    /// means local verified-cache COMMIT, never a server file ACK or Send.
    /// Explicit transfer/cache reuse. Shared file-memory admission can return
    /// Crypto(Busy), including while a background FILEACK proof is verified.
    pub async fn download_attachment(&self, message: &str) -> Result<()> {
        self.download_attachment_inner(None, message).await
    }
    pub async fn download_attachment_in(&self, conversation: &str, message: &str) -> Result<()> {
        self.download_attachment_inner(Some(id(conversation)?), message)
            .await
    }
    async fn download_attachment_inner(
        &self,
        conversation: Option<String>,
        message: &str,
    ) -> Result<()> {
        match self
            .request(Request::Download {
                conversation,
                message: id(message)?,
            })
            .await?
        {
            Response::AttachmentCached => Ok(()),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Local-only, incoming current-own-generation cache. No network fallback,
    /// key export or unverified bytes; the native buffer keeps the memory permit.
    pub async fn open_cached_attachment(
        &self,
        message: &str,
    ) -> Result<crate::encryption::signal_state::EpochAttachmentFile> {
        match self.request(Request::OpenAttachment(id(message)?)).await? {
            Response::AttachmentFile(file) => Ok(file),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Local-only saved file, including retained own-generation history.
    /// This does not resume that generation or grant download/file-ACK authority.
    pub async fn open_archived_attachment(
        &self,
        generation: &str,
        conversation: &str,
        message: &str,
    ) -> Result<crate::encryption::signal_state::EpochAttachmentFile> {
        self.open_local_attachment(Some(id(generation)?), conversation, message)
            .await
    }
    /// Open a verified local copy in the running account's own generation.
    pub async fn open_current_attachment(
        &self,
        conversation: &str,
        message: &str,
    ) -> Result<crate::encryption::signal_state::EpochAttachmentFile> {
        self.open_local_attachment(None, conversation, message)
            .await
    }
    async fn open_local_attachment(
        &self,
        generation: Option<String>,
        conversation: &str,
        message: &str,
    ) -> Result<crate::encryption::signal_state::EpochAttachmentFile> {
        match self
            .request(Request::OpenArchiveAttachment(
                generation,
                id(conversation)?,
                id(message)?,
            ))
            .await?
        {
            Response::AttachmentFile(file) => Ok(file),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Explicit native removal of known incoming/outgoing copies, only after
    /// durable FILEACK / terminal Send evidence. Busy/Pending retain the bytes.
    pub async fn remove_local_attachment(
        &self,
        generation: &str,
        conversation: &str,
        message: &str,
    ) -> Result<()> {
        match self
            .request(Request::RemoveAttachment(
                id(generation)?,
                id(conversation)?,
                id(message)?,
            ))
            .await?
        {
            Response::AttachmentRemoved => Ok(()),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Explicit user action; the same saved peer-slot request is replayed after
    /// transport loss/reopen. A closed result stays closed, never auto-new-dialog.
    pub async fn create_dialog(&self, peer: i64) -> Result<DialogView> {
        if peer <= 0 {
            return Err(AccountError::Invalid);
        }
        match self.request(Request::Create(peer)).await? {
            Response::Dialog(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Explicit consent for this exact closed chat, not automatic Get/Send repair.
    /// The protected worker resolves the expected request and commits/replays its
    /// direct successor. This shares the existing network lane/admission budget.
    pub async fn create_dialog_after_closed(&self, peer: i64, chat: &str) -> Result<DialogView> {
        if peer <= 0 {
            return Err(AccountError::Invalid);
        }
        match self
            .request(Request::CreateAfterClosed(peer, id(chat)?))
            .await?
        {
            Response::Dialog(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Authenticated lookup, persisted before returning metadata. No bootstrap.
    pub async fn get_dialog(&self, chat: &str) -> Result<DialogView> {
        match self.request(Request::Lookup(id(chat)?)).await? {
            Response::Dialog(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn read_views(
        &self,
        query: crate::encryption::signal_state::ViewRequest,
    ) -> Result<crate::encryption::signal_state::ViewPage> {
        match self.request(Request::Read(query)).await? {
            Response::Read(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Stable UUID is supplied once by the caller and reused on retries. Success
    /// means protected local COMMIT, not server/recipient ACK. An established
    /// session queues offline; its first bootstrap requires network access.
    pub async fn send_text(
        &self,
        chat: &str,
        message: &str,
        text: &str,
    ) -> Result<EpochTextQueued> {
        if text.is_empty() || text.len() > crate::encryption::contract::MAX_TEXT_BYTES {
            return Err(AccountError::Invalid);
        }
        let request = TextRequest {
            chat: id(chat)?,
            message: id(message)?,
            text: Zeroizing::new(text.into()),
        };
        match self.request(Request::Send(request)).await? {
            Response::Text(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn conversation(&self, chat: &str) -> Result<Option<ConversationView>> {
        match self.request(Request::Conversation(id(chat)?)).await? {
            Response::Conversation(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn recoveries(&self, chat: &str) -> Result<Vec<RecoveryView>> {
        match self.request(Request::Recoveries(id(chat)?)).await? {
            Response::Recoveries(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn recovery_overview(
        &self,
        chat: &str,
    ) -> Result<crate::encryption::signal_state::EpochRecoveryOverview> {
        match self.request(Request::RecoveryOverview(id(chat)?)).await? {
            Response::RecoveryOverview(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Renderer intent is bound to the observed epoch and own proposal reference.
    /// An unknown local outcome is resolved by read-only overview, not retargeting.
    pub async fn propose_recovery_at(
        &self,
        chat: &str,
        epoch: u64,
        own: Option<&str>,
    ) -> Result<String> {
        if epoch > i64::MAX as u64 {
            return Err(AccountError::Invalid);
        }
        let own = own.map(id).transpose()?;
        match self
            .request(Request::Propose(id(chat)?, Some((epoch, own))))
            .await?
        {
            Response::Operation(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn decide_recovery_at(
        &self,
        chat: &str,
        epoch: u64,
        proposal: &str,
        decision: EpochRecoveryDecision,
    ) -> Result<String> {
        if epoch > i64::MAX as u64 {
            return Err(AccountError::Invalid);
        }
        match self
            .request(Request::Decide(
                id(chat)?,
                id(proposal)?,
                decision,
                Some(epoch),
            ))
            .await?
        {
            Response::Operation(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Explicit user action; returns the durable operation ID, not a server ACK.
    pub async fn propose_recovery(&self, chat: &str) -> Result<String> {
        match self.request(Request::Propose(id(chat)?, None)).await? {
            Response::Operation(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    /// Never called by polling. The worker signs and freezes before later RPC.
    pub async fn decide_recovery(
        &self,
        chat: &str,
        proposal: &str,
        decision: EpochRecoveryDecision,
    ) -> Result<String> {
        match self
            .request(Request::Decide(id(chat)?, id(proposal)?, decision, None))
            .await?
        {
            Response::Operation(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn outgoing(&self, message: &str) -> Result<OutgoingView> {
        match self.request(Request::Outgoing(id(message)?)).await? {
            Response::Outgoing(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
    pub async fn received_text(&self, message: &str) -> Result<Option<ReceivedText>> {
        match self.request(Request::Received(id(message)?)).await? {
            Response::Received(v) => Ok(v),
            _ => Err(AccountError::Runtime),
        }
    }
}
fn id(value: &str) -> Result<String> {
    if !epoch_rpc::uuid4(value) {
        return Err(AccountError::Invalid);
    }
    Ok(value.into())
}
pub(super) async fn serve(
    worker: &CryptoCoordinator,
    mut requests: mpsc::Receiver<Command>,
    network: mpsc::Sender<Command>,
    mut stop: watch::Receiver<bool>,
    wake: &Notify,
) -> Result<()> {
    loop {
        let Some(mut command) = (tokio::select! {
            biased;
            _ = super::super::delivery_runner::cancelled(&mut stop) => return Ok(()),
            c = requests.recv() => c,
        }) else {
            return Ok(());
        };
        if command.reply.is_closed() {
            continue;
        }
        if let Request::Send(input) = &command.request {
            let result = tokio::select! {
                biased;
                _ = super::super::delivery_runner::cancelled(&mut stop) => return Ok(()),
                _ = command.reply.closed() => continue,
                r = local_text(worker, input) => r,
            };
            match result {
                Ok(EpochTextPreparation::BootstrapRequired) => {
                    // Transfer the SAME reply and admission permit; do not wait
                    // on RPC in the local lane or spawn a task per message.
                    if let Err(e) = network.try_send(command) {
                        let reason = match &e {
                            mpsc::error::TrySendError::Full(_) => AccountError::Busy,
                            mpsc::error::TrySendError::Closed(_) => AccountError::Stopped,
                        };
                        let _ = e.into_inner().reply.send(Err(reason));
                    }
                }
                Ok(EpochTextPreparation::Queued(v)) => {
                    wake.notify_one();
                    let _ = command.reply.send(Ok(Response::Text(v)));
                }
                Err(e) => {
                    let _ = command.reply.send(Err(e));
                }
            }
            tokio::task::yield_now().await;
            continue;
        }
        let mutation = matches!(command.request, Request::Propose(..) | Request::Decide(..));
        let result = tokio::select! {
            biased;
            _ = super::super::delivery_runner::cancelled(&mut stop) => return Ok(()),
            _ = command.reply.closed() => continue,
            r = handle(worker, command.request) => r,
        };
        if mutation && result.is_ok() {
            wake.notify_one(); // One coalesced permit, never a per-message task.
        }
        let _ = command.reply.send(result);
        tokio::task::yield_now().await;
    }
}
async fn handle(worker: &CryptoCoordinator, request: Request) -> Result<Response> {
    match request {
        Request::AttachmentInput(chat, message) => Ok(Response::AttachmentInput(
            worker.epoch_attachment_input_state(chat, message).await?,
        )),
        #[cfg(feature = "compression")]
        Request::Photo(_) => Err(AccountError::Invalid),
        Request::PreparedAttachment(uuid) => Ok(Response::PreparedAttachment(
            worker.prepared_epoch_attachment(uuid).await?,
        )),
        Request::FileProgress(uuid) => {
            let result = worker
                .epoch_file_progress(uuid)
                .await
                .map_err(|e| match e {
                    CryptoError::FileStatusIndexNotReady => AccountError::Initializing,
                    e => e.into(),
                })?;
            Ok(Response::FileProgress(Box::new(result)))
        }
        Request::FileAckStatus(uuid) => Ok(Response::FileAckStatus(
            worker.epoch_file_ack_status(uuid).await?,
        )),
        Request::Download { .. } => Err(AccountError::Invalid),
        Request::OpenAttachment(uuid) => {
            let cache = worker.cached_epoch_attachment(uuid).await?;
            Ok(Response::AttachmentFile(
                worker.open_cached_epoch_attachment(cache).await?,
            ))
        }
        Request::OpenArchiveAttachment(generation, chat, uuid) => Ok(Response::AttachmentFile(
            worker
                .open_local_epoch_attachment(generation, chat, uuid)
                .await?,
        )),
        Request::RemoveAttachment(generation, chat, uuid) => {
            worker
                .remove_local_epoch_attachment(generation, chat, uuid)
                .await?;
            Ok(Response::AttachmentRemoved)
        }
        Request::Read(query) => Ok(Response::Read(worker.read_views(query).await?)),
        Request::Create(_)
        | Request::CreateAfterClosed(..)
        | Request::Lookup(_)
        | Request::AttachmentCatalog(_)
        | Request::Send(_) => Err(AccountError::Invalid),
        Request::Conversation(chat) => Ok(Response::Conversation(
            worker
                .active_epoch_scope(chat)
                .await?
                .map(|s| ConversationView {
                    crypto_epoch: s.crypto_epoch(),
                    receive_only: s.receive_only(),
                    sending_frozen: s.sending_frozen(),
                }),
        )),
        Request::Outgoing(uuid) => {
            let p = worker.epoch_outgoing_plan(uuid).await?;
            let env = p.request().envelope.as_ref().ok_or(CryptoError::Invalid)?;
            Ok(Response::Outgoing(OutgoingView {
                crypto_epoch: env.crypto_epoch.ok_or(CryptoError::Invalid)?,
                stored_on_server: p.receipt().is_some(),
                status: p
                    .status()
                    .and_then(|v| common::MessageState::try_from(v.state).ok()),
                removal: p
                    .removal()
                    .and_then(|v| common::MessageState::try_from(v.state).ok()),
                sealed_unsent: p.sealed_failure().is_some(),
            }))
        }
        Request::Received(uuid) => {
            let p = worker.epoch_incoming_message(uuid).await?;
            let Some(v2::encrypted_payload::Content::Text(text)) = p.content else {
                return Ok(Response::Received(None));
            };
            Ok(Response::Received(Some(ReceivedText {
                message_id: p.message_id,
                conversation_id: p.conversation_id,
                crypto_epoch: p.crypto_epoch.ok_or(CryptoError::Invalid)?,
                sender_user_id: p.sender.ok_or(CryptoError::Invalid)?.user_id,
                created_at_unix_ms: p.created_at_unix_ms,
                text: text.text,
            })))
        }
        Request::Recoveries(chat) => {
            let scope = worker
                .active_epoch_scope(chat)
                .await?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let snapshots = worker.current_epoch_recoveries(scope).await?;
            Ok(Response::Recoveries(
                snapshots
                    .into_iter()
                    .map(|s| RecoveryView {
                        proposal_id: s.proposal_id().into(),
                        state: s.state(),
                        own_proposal: s.own_proposal(),
                        decision: s.decision(),
                        sending_frozen: s.sending_frozen(),
                        retired_by: s.retired_by().map(str::to_owned),
                    })
                    .collect(),
            ))
        }
        Request::RecoveryOverview(chat) => Ok(Response::RecoveryOverview(
            worker.epoch_recovery_overview(chat).await?,
        )),
        Request::Propose(chat, expected) => {
            let scope = worker
                .active_epoch_scope(chat)
                .await?
                .ok_or(CryptoError::ConversationUnavailable)?;
            let p = if let Some((epoch, own)) = expected {
                if scope.crypto_epoch() != epoch {
                    return Err(CryptoError::Conflict.into());
                }
                worker.prepare_epoch_recovery_if(scope, own).await?
            } else {
                worker.prepare_epoch_recovery(scope).await?
            };
            Ok(Response::Operation(
                p.request()
                    .command
                    .as_ref()
                    .and_then(|c| c.control.as_ref())
                    .ok_or(CryptoError::Invalid)?
                    .request_id
                    .clone(),
            ))
        }
        Request::Decide(chat, proposal, decision, expected_epoch) => {
            let scope = worker
                .active_epoch_scope(chat)
                .await?
                .ok_or(CryptoError::ConversationUnavailable)?;
            if expected_epoch.is_some_and(|epoch| scope.crypto_epoch() != epoch) {
                return Err(CryptoError::Conflict.into());
            }
            let p = worker
                .prepare_epoch_recovery_decision(scope, proposal, decision)
                .await?;
            Ok(Response::Operation(
                p.request()
                    .command
                    .as_ref()
                    .and_then(|c| c.control.as_ref())
                    .ok_or(CryptoError::Invalid)?
                    .request_id
                    .clone(),
            ))
        }
    }
}

/// Only the account's network future receives this queue. Local requests use a
/// separate receiver but share the SAME total admission permits, so a stalled
/// foreground RPC neither monopolizes local reads nor creates extra network tasks.
pub(super) async fn network<T: EpochDeliveryTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    mut command: Command,
    stop: &mut watch::Receiver<bool>,
    wake: &Notify,
) -> Result<()> {
    if command.reply.is_closed() {
        return Ok(());
    }
    let operation = async {
        let v = match command.request {
            Request::AttachmentCatalog(chat) => {
                ensure_catalog(worker, transport, chat).await?;
                return Ok(Response::AttachmentCatalog);
            }
            Request::Send(input) => {
                return Ok(Response::Text(send_text(worker, transport, input).await?));
            }
            Request::Create(peer) => {
                let p = worker.prepare_conversation_creation(peer).await?;
                let verified = transport.create(&p).await?;
                worker.accept_conversation_creation(verified).await?
            }
            Request::CreateAfterClosed(peer, chat) => {
                let p = worker.prepare_conversation_after_closed(peer, chat).await?;
                let verified = transport.create(&p).await?;
                worker.accept_conversation_creation(verified).await?
            }
            Request::Lookup(chat) => {
                let p = worker.prepare_conversation_lookup(chat).await?;
                let verified = transport.lookup(&p).await?;
                worker.accept_conversation_lookup(verified).await?
            }
            _ => return Err(AccountError::Invalid),
        };
        Ok(Response::Dialog(DialogView::committed(v)?))
    };
    let result = tokio::select! {
        biased;
        _ = super::super::delivery_runner::cancelled(stop) => return Ok(()),
        _ = command.reply.closed() => return Ok(()),
        r = tokio::time::timeout(Duration::from_secs(15), operation) => r.unwrap_or(Err(AccountError::CommandTimeout)),
    };
    // Peer-specific permission/not-found errors are normal command failures, not
    // evidence that this account's credentials or its whole owner are invalid.
    let fatal = match &result {
        Err(AccountError::Rpc(e)) if fatal_rpc(e) => Some(e.clone()),
        _ => None,
    };
    if matches!(&result, Ok(Response::Text(_))) {
        wake.notify_one(); // Existing durable outbox loop owns the Send RPC.
    }
    let _ = command.reply.send(result);
    if let Some(e) = fatal {
        return Err(e.into());
    }
    Ok(())
}
async fn local_text(
    worker: &CryptoCoordinator,
    input: &TextRequest,
) -> Result<EpochTextPreparation> {
    Ok(worker
        .prepare_epoch_text(
            input.chat.clone(),
            input.message.clone(),
            input.text.clone(),
        )
        .await?)
}
async fn send_text<T: EpochDeliveryTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    input: TextRequest,
) -> Result<EpochTextQueued> {
    // Receive/delivery may have established a session while this command waited.
    if let EpochTextPreparation::Queued(v) = local_text(worker, &input).await? {
        return Ok(v);
    }
    let scope = ensure_catalog(worker, transport, input.chat.clone()).await?;
    // Higher epochs bootstrap only on the agreed proposer's side. Receiving a
    // peer's PreKey can also supply a session; neither case permits a reset.
    if scope.crypto_epoch() > 0 && !scope.bootstrap_initiator() {
        if let EpochTextPreparation::Queued(v) = local_text(worker, &input).await? {
            return Ok(v);
        }
        return Err(AccountError::SessionPending);
    }
    match worker.prepare_epoch_acquisition(scope).await {
        Ok(mut plan) => {
            if !plan.applied() {
                for attempt in 0..2 {
                    match transport.acquire(&plan).await? {
                        super::super::native::EpochAcquisitionOutcome::Bundle(bundle) => {
                            worker.bootstrap_epoch(*bundle).await?;
                            break;
                        }
                        super::super::native::EpochAcquisitionOutcome::Expired(proof) => {
                            plan = worker.renew_epoch_acquisition(*proof).await?;
                            if attempt == 1 {
                                return Err(CryptoError::Expired.into());
                            }
                        }
                    }
                }
            }
        }
        Err(CryptoError::SessionExists) => (),
        Err(e) => return Err(e.into()),
    }
    match local_text(worker, &input).await? {
        EpochTextPreparation::Queued(v) => Ok(v),
        EpochTextPreparation::BootstrapRequired => Err(AccountError::SessionPending),
    }
}
async fn ensure_catalog<T: EpochDeliveryTransport>(
    worker: &CryptoCoordinator,
    transport: &mut T,
    chat: String,
) -> Result<super::super::signal_state::EpochScope> {
    let lookup = worker.prepare_conversation_lookup(chat.clone()).await?;
    worker
        .accept_conversation_lookup(transport.lookup(&lookup).await?)
        .await?;
    let scope = match worker.active_epoch_scope(chat.clone()).await? {
        Some(s) => s,
        None => {
            let query = worker.prepare_initial_epoch(chat).await?;
            worker
                .accept_initial_epoch(transport.initial(&query).await?)
                .await?
        }
    };
    Ok(scope)
}
pub(super) fn fatal_rpc(e: &super::super::native::RpcError) -> bool {
    use crate::encryption::native::RpcError;
    use tonic::Code;
    matches!(
        e,
        RpcError::Invalid
            | RpcError::Failure {
                reason: common::FailureReason::GenerationChanged,
                ..
            }
            | RpcError::Status(Code::Unauthenticated | Code::Unimplemented)
            | RpcError::Failure {
                code: Code::Unauthenticated | Code::Unimplemented,
                ..
            }
    )
}
