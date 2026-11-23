//! One native input lane for files and photos. Both the process-wide input slot
//! and account file-memory permit stay with any non-interruptible codec.
use super::*;
use crate::{
    compression::{MAX_INPUT_BYTES, PhotoChoice},
    encryption::{
        attachment::MAX_PLAINTEXT_BYTES,
        attachment_payload::{AttachmentDraft, MAX_MEDIA_BYTES},
        photo_preparation::{
            PhotoAttachmentError, PreparedPhotoAttachment, prepare_photo_attachment,
        },
        signal_state::EpochAttachmentPreparation,
        voice_format::VoiceFormat,
    },
    proto::e2ee_v2::AttachmentKind,
};
use std::sync::LazyLock;

static INPUT_SLOT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

/// Native owned source, not a renderer descriptor or retained retry token.
/// No caller-supplied MIME, metadata-clean flag, path, key or account binding.
pub struct PhotoSource {
    pub bytes: Zeroizing<Vec<u8>>,
    pub filename: Zeroizing<String>,
    pub caption: Zeroizing<String>,
    pub choice: PhotoChoice,
}

pub struct FileSource {
    pub bytes: Zeroizing<Vec<u8>>,
    pub filename: Zeroizing<String>,
    pub caption: Zeroizing<String>,
}

/// One-shot native admission, obtained BEFORE copying a binary IPC body. Holds
/// no store/key lease and cannot be transferred to another account client.
pub struct AttachmentAdmission {
    owner: Arc<Semaphore>,
    chat: String,
    message: String,
    bytes: usize,
    expected_epoch: Option<u64>,
    kind: AttachmentKind,
    cpu: OwnedSemaphorePermit,
}
pub type PhotoAdmission = AttachmentAdmission;

pub(super) struct Input {
    chat: String,
    message: String,
    source: Source,
    expected_epoch: Option<u64>,
    cpu: OwnedSemaphorePermit,
}
enum Source {
    Photo(PhotoSource),
    File(FileSource),
    Voice(Zeroizing<Vec<u8>>, Zeroizing<String>),
}
impl AccountClient {
    pub fn admit_photo(&self, chat: &str, message: &str, bytes: usize) -> Result<PhotoAdmission> {
        self.admit_attachment(chat, message, bytes, AttachmentKind::Photo, None)
    }

    pub fn admit_file(
        &self,
        chat: &str,
        message: &str,
        bytes: usize,
        expected_epoch: u64,
    ) -> Result<AttachmentAdmission> {
        self.admit_attachment(
            chat,
            message,
            bytes,
            AttachmentKind::File,
            Some(expected_epoch),
        )
    }

    pub fn admit_voice(
        &self,
        chat: &str,
        message: &str,
        bytes: usize,
        expected_epoch: u64,
    ) -> Result<AttachmentAdmission> {
        self.admit_attachment(
            chat,
            message,
            bytes,
            AttachmentKind::Voice,
            Some(expected_epoch),
        )
    }

    fn admit_attachment(
        &self,
        chat: &str,
        message: &str,
        bytes: usize,
        kind: AttachmentKind,
        expected_epoch: Option<u64>,
    ) -> Result<AttachmentAdmission> {
        let chat = id(chat)?;
        let message = id(message)?;
        let max_bytes = match kind {
            AttachmentKind::Photo => MAX_INPUT_BYTES,
            AttachmentKind::File => MAX_PLAINTEXT_BYTES,
            AttachmentKind::Voice => MAX_MEDIA_BYTES,
            _ => return Err(AccountError::Invalid),
        };
        if bytes == 0
            || bytes > max_bytes
            || expected_epoch.is_some_and(|epoch| epoch > i64::MAX as u64)
        {
            return Err(AccountError::Invalid);
        }
        if self.stopping() {
            return Err(AccountError::Stopped);
        }
        if self.status().phase != AccountPhase::Running {
            return Err(AccountError::Initializing);
        }
        let cpu = INPUT_SLOT
            .clone()
            .try_acquire_owned()
            .map_err(|_| AccountError::Busy)?;
        Ok(AttachmentAdmission {
            owner: self.budget.clone(),
            chat,
            message,
            bytes,
            expected_epoch,
            kind,
            cpu,
        })
    }
    /// NEW photo input only, not Send/recipient ACK. Native code derives the
    /// private metadata flag. Occupied UUIDs conflict before any codec; retry
    /// via prepared_attachment/outgoing, never by reprocessing the source.
    pub async fn prepare_photo(
        &self,
        chat: &str,
        message: &str,
        photo: PhotoSource,
    ) -> Result<EpochAttachmentPreparation> {
        let admission = self.admit_photo(chat, message, photo.bytes.len())?;
        self.prepare_admitted_photo(admission, photo).await
    }

    pub async fn prepare_admitted_photo(
        &self,
        admission: PhotoAdmission,
        photo: PhotoSource,
    ) -> Result<EpochAttachmentPreparation> {
        self.prepare_admitted_attachment(admission, Source::Photo(photo))
            .await
    }

    /// Accept byte-identical file input in the requested cryptographic epoch.
    pub async fn prepare_admitted_file(
        &self,
        admission: AttachmentAdmission,
        source: FileSource,
    ) -> Result<EpochAttachmentPreparation> {
        self.prepare_admitted_attachment(admission, Source::File(source))
            .await
    }

    pub async fn prepare_admitted_voice(
        &self,
        admission: AttachmentAdmission,
        bytes: Zeroizing<Vec<u8>>,
        caption: Zeroizing<String>,
    ) -> Result<EpochAttachmentPreparation> {
        self.prepare_admitted_attachment(admission, Source::Voice(bytes, caption))
            .await
    }

    async fn prepare_admitted_attachment(
        &self,
        admission: AttachmentAdmission,
        source: Source,
    ) -> Result<EpochAttachmentPreparation> {
        if !Arc::ptr_eq(&admission.owner, &self.budget) {
            return Err(AccountError::Crypto(CryptoError::Conflict));
        }
        let (kind, bytes) = match &source {
            Source::Photo(source) => (AttachmentKind::Photo, source.bytes.len()),
            Source::File(source) => (AttachmentKind::File, source.bytes.len()),
            Source::Voice(bytes, _) => (AttachmentKind::Voice, bytes.len()),
        };
        if kind != admission.kind || bytes != admission.bytes {
            return Err(AccountError::Invalid);
        }
        if self.stopping() {
            return Err(AccountError::Stopped);
        }
        let input = Input {
            chat: admission.chat,
            message: admission.message,
            source,
            expected_epoch: admission.expected_epoch,
            cpu: admission.cpu,
        };
        match self.request(Request::Photo(Box::new(input))).await? {
            Response::Photo(value) => Ok(value),
            _ => Err(AccountError::Runtime),
        }
    }
}

type Prepared = std::result::Result<PreparedPhotoAttachment, PhotoAttachmentError>;

async fn blocking<H: Send + 'static, R: Send + 'static>(
    held: H,
    codec: impl FnOnce() -> R + Send + 'static,
) -> Result<(H, R)> {
    tokio::task::spawn_blocking(move || {
        let result = codec();
        (held, result)
    })
    .await
    .map_err(|_| AccountError::Runtime)
}

async fn process(
    worker: &CryptoCoordinator,
    input: Input,
    codec: impl FnOnce(&[u8], Zeroizing<String>, Zeroizing<String>, PhotoChoice) -> Prepared
    + Send
    + 'static,
) -> Result<Response> {
    let Some(ticket) = worker
        .new_epoch_attachment_input(input.chat, input.message, input.expected_epoch)
        .await?
    else {
        return Ok(Response::Photo(EpochAttachmentPreparation::CatalogRequired));
    };
    // The closure has NO worker/sender/DB owner. Dropping its waiter can
    // neither release admission early nor keep an account database open.
    let (ticket, slot, draft, bytes) = match input.source {
        Source::Photo(source) => {
            let ((ticket, slot), prepared) = blocking((ticket, input.cpu), move || {
                codec(
                    &source.bytes,
                    source.filename,
                    source.caption,
                    source.choice,
                )
            })
            .await?;
            let (draft, bytes) = prepared?.into_input();
            (ticket, slot, draft, bytes)
        }
        Source::File(source) => (
            ticket,
            input.cpu,
            AttachmentDraft::file(source.filename, source.caption),
            source.bytes,
        ),
        Source::Voice(bytes, caption) => {
            let format = VoiceFormat::detect(&bytes).ok_or(AccountError::VoiceFormat)?;
            let draft = AttachmentDraft {
                kind: AttachmentKind::Voice,
                filename: Zeroizing::new(format.filename().into()),
                mime_type: Zeroizing::new(format.mime().into()),
                caption,
                photo_metadata_stripped: None,
            };
            (ticket, input.cpu, draft, bytes)
        }
    };
    draft
        .validate(bytes.len())
        .map_err(|_| AccountError::Invalid)?;
    let result = worker
        .finish_epoch_attachment_input(ticket, draft, bytes)
        .await?;
    drop(slot);
    Ok(Response::Photo(result))
}

pub(in crate::encryption::account_owner) async fn serve(
    worker: &CryptoCoordinator,
    mut requests: mpsc::Receiver<Command>,
    mut stop: watch::Receiver<bool>,
    wake: &Notify,
) -> Result<()> {
    loop {
        let Some(mut command) = (tokio::select! {
            biased;
            _ = super::super::super::delivery_runner::cancelled(&mut stop) => return Ok(()),
            command = requests.recv() => command,
        }) else {
            return Ok(());
        };
        if command.reply.is_closed() {
            continue;
        }
        let Request::Photo(input) = command.request else {
            return Err(AccountError::Invalid);
        };
        let result = tokio::select! {
            biased;
            _ = super::super::super::delivery_runner::cancelled(&mut stop) => return Ok(()),
            _ = command.reply.closed() => continue,
            result = process(worker, *input, prepare_photo_attachment) => result,
        };
        if matches!(
            &result,
            Ok(Response::Photo(EpochAttachmentPreparation::Prepared(_)))
        ) {
            wake.notify_one();
        }
        // If cancellation raced an already started COMMIT, retained UUID lookup
        // resolves it. No user-visible successful reply after account shutdown.
        let _ = command.reply.send(result);
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod photo_input_tests {
    use super::*;
    use std::{sync::mpsc as sync, time::Duration};

    #[tokio::test]
    async fn cancelled_codec_waiter_keeps_both_slots_until_actual_exit() {
        let cpu = Arc::new(Semaphore::new(1));
        let files = Arc::new(Semaphore::new(1));
        let held = (
            cpu.clone().try_acquire_owned().unwrap(),
            files.clone().try_acquire_owned().unwrap(),
        );
        let (began, started) = oneshot::channel();
        let (release, wait) = sync::channel();
        let job = tokio::spawn(blocking(held, move || {
            let _ = began.send(());
            wait.recv_timeout(Duration::from_secs(10)).unwrap();
            Zeroizing::new(vec![42u8; 1024])
        }));
        started.await.unwrap();
        job.abort();
        assert!(job.await.unwrap_err().is_cancelled());
        assert_eq!(cpu.available_permits(), 0);
        assert_eq!(files.available_permits(), 0);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while cpu.available_permits() != 1 || files.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn codec_panic_releases_slots_and_has_no_successful_output() {
        let gate = Arc::new(Semaphore::new(1));
        let result = blocking(gate.clone().try_acquire_owned().unwrap(), || {
            panic!("injected codec panic")
        })
        .await;
        assert!(matches!(result, Err(AccountError::Runtime)));
        assert_eq!(gate.available_permits(), 1);
    }
}
