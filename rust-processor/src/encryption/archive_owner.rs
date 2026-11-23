//! Local read-only account lease. No transport or CryptoCoordinator is exposed;
//! requests are archive views and saved files. Cloned readers cannot keep the DB alive
//! after the owner stops. SQLite query_only also rejects accidental writes.
use super::*;
use crate::encryption::{epoch_rpc, provisioning::DeviceProvisioning};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
const CAPACITY: usize = 8;
struct Read {
    query: Query,
    _permit: OwnedSemaphorePermit,
}
enum Query {
    Views(ViewRequest, Reply<ViewPage>),
    File(
        String,
        String,
        String,
        Arc<OwnedSemaphorePermit>,
        Reply<EpochAttachmentFile>,
    ),
    #[cfg(test)]
    Barrier(mpsc::Receiver<()>, oneshot::Sender<()>),
}
struct Control {
    // Eight admitted reads plus one reserved stop wake-up. Never wait for a
    // request slot when retiring an account with cloned/abandoned readers.
    sender: mpsc::SyncSender<Option<Read>>,
    budget: Arc<Semaphore>,
    file_memory: Arc<Semaphore>,
    stopped: AtomicBool,
}
impl Control {
    fn stop(&self) {
        if !self.stopped.swap(true, Ordering::SeqCst) {
            self.budget.close();
            self.file_memory.close();
            let _ = self.sender.try_send(None);
        }
    }
}
#[derive(Clone)]
pub struct ArchiveClient(Arc<Control>);
impl ArchiveClient {
    #[cfg(test)]
    pub(in crate::encryption::signal_state) async fn hold_file_test_thread(
        &self,
        release: mpsc::Receiver<()>,
    ) -> CryptoResult<()> {
        let (entered, ready) = oneshot::channel();
        self.enqueue(Query::Barrier(release, entered), self.admit()?)?;
        ready.await.map_err(|_| CryptoError::Stopped)
    }
    pub fn is_running(&self) -> bool {
        !self.0.stopped.load(Ordering::SeqCst)
    }
    pub async fn read_views(&self, query: ViewRequest) -> CryptoResult<ViewPage> {
        if !self.is_running() {
            return Err(CryptoError::Stopped);
        }
        if !query.is_archive() {
            return Err(CryptoError::Invalid);
        }
        let permit = self.admit()?;
        let (reply, result) = oneshot::channel();
        self.enqueue(Query::Views(query, reply), permit)?;
        self.finish(result).await
    }
    /// Already saved incoming cache / promoted outgoing spool, bound to this account's
    /// known generation/chat/message. No transport or generic file-path input.
    pub async fn open_archived_attachment(
        &self,
        generation: &str,
        conversation: &str,
        message: &str,
    ) -> CryptoResult<EpochAttachmentFile> {
        if !epoch_rpc::uuid4(generation)
            || !epoch_rpc::uuid4(conversation)
            || !epoch_rpc::uuid4(message)
        {
            return Err(CryptoError::Invalid);
        }
        let permit = self.admit()?;
        let memory = self
            .0
            .file_memory
            .clone()
            .try_acquire_owned()
            .map_err(|_| {
                if self.is_running() {
                    CryptoError::Busy
                } else {
                    CryptoError::Stopped
                }
            })?;
        let (reply, result) = oneshot::channel();
        self.enqueue(
            Query::File(
                generation.into(),
                conversation.into(),
                message.into(),
                Arc::new(memory),
                reply,
            ),
            permit,
        )?;
        self.finish(result).await
    }
    fn admit(&self) -> CryptoResult<OwnedSemaphorePermit> {
        if !self.is_running() {
            return Err(CryptoError::Stopped);
        }
        let permit = self.0.budget.clone().try_acquire_owned().map_err(|_| {
            if self.is_running() {
                CryptoError::Busy
            } else {
                CryptoError::Stopped
            }
        })?;
        if !self.is_running() {
            return Err(CryptoError::Stopped);
        }
        Ok(permit)
    }
    fn enqueue(&self, query: Query, permit: OwnedSemaphorePermit) -> CryptoResult<()> {
        if !self.is_running() {
            return Err(CryptoError::Stopped);
        }
        self.0
            .sender
            .try_send(Some(Read {
                query,
                _permit: permit,
            }))
            .map_err(|e| match e {
                mpsc::TrySendError::Full(_) => CryptoError::Busy,
                mpsc::TrySendError::Disconnected(_) => CryptoError::Stopped,
            })
    }
    async fn finish<T>(&self, result: oneshot::Receiver<CryptoResult<T>>) -> CryptoResult<T> {
        let result = result.await.map_err(|_| CryptoError::Stopped)?;
        if !self.is_running() {
            return Err(CryptoError::Stopped);
        }
        result
    }
}
/// No Clone. Dropping/abandoning the lease independently requests cleanup;
/// explicit shutdown waits until the connection and key/owner lock are gone.
pub struct ArchiveOwner {
    control: Arc<Control>,
    stopped: Option<oneshot::Receiver<bool>>,
}
impl ArchiveOwner {
    /// The provisioning store was selected by authenticated native discovery.
    /// No account UUID/path/generation claim or credentials are accepted here.
    /// A store with no bound local device has no installation archive.
    pub async fn start(local: DeviceProvisioning) -> CryptoResult<Option<Self>> {
        let (sender, receiver) = mpsc::sync_channel(CAPACITY + 1);
        let control = Arc::new(Control {
            sender,
            budget: Arc::new(Semaphore::new(CAPACITY)),
            file_memory: Arc::new(Semaphore::new(1)),
            stopped: AtomicBool::new(false),
        });
        let (ready_tx, ready) = oneshot::channel();
        let (stopped_tx, stopped) = oneshot::channel();
        // Construct the stop lease BEFORE the first await. Cancelled startup
        // must wake the thread even if its ready reply was already queued.
        let owner = Self {
            control: control.clone(),
            stopped: Some(stopped),
        };
        std::thread::Builder::new()
            .name("zchat-archive".into())
            .spawn(move || {
                let _finished = Finished(control.clone(), Some(stopped_tx));
                {
                    let engine = (|| {
                        let Some((store, own)) = local.into_archive_store()? else {
                            return Ok(None);
                        };
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .build()
                            .map_err(|_| CryptoError::Stopped)?;
                        Ok::<_, CryptoError>(Some(Engine {
                            store,
                            own,
                            runtime,
                        }))
                    })();
                    match engine {
                        Err(error) => {
                            let _ = ready_tx.send(Err(error));
                        }
                        Ok(None) => {
                            let _ = ready_tx.send(Ok(false));
                        }
                        Ok(Some(mut engine)) => {
                            let instance = uuid::Uuid::new_v4();
                            if ready_tx.send(Ok(true)).is_ok() {
                                while !control.stopped.load(Ordering::SeqCst) {
                                    let Ok(Some(read)) = receiver.recv() else {
                                        break;
                                    };
                                    if !control.stopped.load(Ordering::SeqCst) {
                                        match read.query {
                                            #[cfg(test)]
                                            Query::Barrier(release, entered) => {
                                                let _ = entered.send(());
                                                let _ = release.recv();
                                            }
                                            Query::Views(query, reply) if !reply.is_closed() => {
                                                let _ =
                                                    reply.send(engine.read_views(instance, query));
                                            }
                                            Query::File(
                                                generation,
                                                chat,
                                                message,
                                                memory,
                                                reply,
                                            ) if !reply.is_closed() => {
                                                let _ = reply.send(engine.open_archive_attachment(
                                                    &generation,
                                                    &chat,
                                                    &message,
                                                    memory,
                                                ));
                                            }
                                            _ => (),
                                        }
                                    }
                                }
                            }
                        }
                    }
                } // Close SQLite/key material BEFORE releasing owner.lock/stop ACK.
                drop(receiver);
            })
            .map_err(StoreError::from)?;
        match ready.await {
            Ok(Ok(true)) => Ok(Some(owner)),
            Ok(Ok(false)) => {
                owner.shutdown().await?;
                Ok(None)
            }
            result => {
                let _ = owner.shutdown().await;
                Err(match result {
                    Ok(Err(error)) => error,
                    _ => CryptoError::Stopped,
                })
            }
        }
    }
    pub fn client(&self) -> ArchiveClient {
        ArchiveClient(self.control.clone())
    }
    pub async fn shutdown(mut self) -> CryptoResult<()> {
        self.control.stop();
        if self
            .stopped
            .take()
            .ok_or(CryptoError::Stopped)?
            .await
            .unwrap_or(false)
        {
            Ok(())
        } else {
            Err(CryptoError::Stopped)
        }
    }
}
impl Drop for ArchiveOwner {
    fn drop(&mut self) {
        self.control.stop();
    }
}
struct Finished(Arc<Control>, Option<oneshot::Sender<bool>>);
impl Drop for Finished {
    fn drop(&mut self) {
        self.0.stop();
        if let Some(ack) = self.1.take() {
            let _ = ack.send(!std::thread::panicking());
        }
    }
}

#[cfg(test)]
#[path = "archive_owner_tests.rs"]
mod tests;
