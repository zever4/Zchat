//! Real protected worker/journal/chunks, scripted native network boundaries.
use super::*;
use crate::encryption::{
    blob_upload::{BlobUploadError, BlobUploadStep, EpochBlobUploadTransport, upload_epoch_blob},
    native::{BlobHttpError, EpochBlobControlOutcome, RpcError},
};
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{Notify, watch};

enum Reply {
    Lease(v2::BlobLease),
    Missing,
    WrongMissing,
    Error(RpcError),
    Stall(Arc<Notify>),
}
enum Action {
    Observe(Reply),
    Reserve(Reply),
    Put(Result<u16, BlobHttpError>),
    StallPut(Arc<Notify>),
}
struct Network {
    actions: VecDeque<Action>,
    requests: Vec<v2::BlobLeaseRequest>,
    body: Option<EpochBlobUpload>,
    cipher_hashes: Vec<[u8; 32]>,
    join_gate: Option<(Arc<Notify>, Arc<Notify>)>,
    cancelled: bool,
}
impl Network {
    fn new(actions: impl IntoIterator<Item = Action>) -> Self {
        Self {
            actions: actions.into_iter().collect(),
            requests: vec![],
            body: None,
            cipher_hashes: vec![],
            join_gate: None,
            cancelled: false,
        }
    }
    async fn reply(
        &self,
        plan: &EpochBlobControlPlan,
        reply: Reply,
    ) -> Result<EpochBlobControlOutcome, RpcError> {
        Ok(match reply {
            Reply::Lease(g) => EpochBlobControlOutcome::Lease(Box::new(observed(plan, g))),
            Reply::Missing | Reply::WrongMissing => {
                let mut r = plan.request().clone();
                if matches!(reply, Reply::WrongMissing) {
                    r.operation_id = Uuid::new_v4().to_string();
                }
                EpochBlobControlOutcome::Rejected(Box::new(EpochObservation::test_observation(
                    StoreScope::new(SERVER, A).unwrap(),
                    plan.clone(),
                    v2::BlobFailure {
                        reason: v2::BlobFailureReason::OperationNotObserved as i32,
                        request: Some(r),
                    },
                )))
            }
            Reply::Error(e) => return Err(e),
            Reply::Stall(entered) => {
                entered.notify_one();
                return std::future::pending().await;
            }
        })
    }
}
#[async_trait::async_trait]
impl EpochBlobUploadTransport for Network {
    async fn observe(
        &mut self,
        p: &EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome, RpcError> {
        assert!(
            self.body.is_none(),
            "Observe cannot overlap retained file admission"
        );
        self.requests.push(p.request().clone());
        let Some(Action::Observe(reply)) = self.actions.pop_front() else {
            panic!("unexpected Observe")
        };
        self.reply(p, reply).await
    }
    async fn reserve(
        &mut self,
        p: &EpochBlobControlPlan,
    ) -> Result<EpochBlobControlOutcome, RpcError> {
        assert!(self.body.is_none());
        self.requests.push(p.request().clone());
        let Some(Action::Reserve(reply)) = self.actions.pop_front() else {
            panic!("unexpected Reserve")
        };
        self.reply(p, reply).await
    }
    async fn put(&mut self, source: EpochBlobUpload) -> Result<u16, BlobHttpError> {
        self.cipher_hashes
            .push(Sha256::digest(source.bytes()).into());
        self.requests.push(source.plan().request().clone());
        assert!(self.body.replace(source).is_none());
        match self.actions.pop_front() {
            Some(Action::Put(result)) => result,
            Some(Action::StallPut(entered)) => {
                entered.notify_one();
                std::future::pending().await
            }
            _ => panic!("unexpected PUT"),
        }
    }
    async fn join(&mut self) {
        if self.body.is_some()
            && let Some((entered, release)) = self.join_gate.take()
        {
            entered.notify_one();
            release.notified().await;
        }
        drop(self.body.take());
    }
    fn cancel(&mut self) {
        self.cancelled = true;
        self.join_gate = None;
    }
}
fn state(step: BlobUploadStep) -> (EpochBlobProgress, Option<Result<u16, BlobHttpError>>) {
    let BlobUploadStep::Observed { progress, http } = step else {
        panic!("unexpected renewal")
    };
    (*progress, http)
}
fn running() -> (watch::Sender<bool>, watch::Receiver<bool>) {
    watch::channel(false)
}

#[tokio::test]
async fn observe_reserve_put_observe_preserves_exact_operation_cipher_and_ready_replay() {
    let (f, own, id, p) = setup().await;
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    let mut ready = g.clone();
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    let mut net = Network::new([
        Action::Observe(Reply::Missing),
        Action::Reserve(Reply::Lease(g)),
        Action::Put(Ok(201)),
        Action::Observe(Reply::Lease(ready.clone())),
        Action::Observe(Reply::Lease(ready)),
    ]);
    let (_tx, mut stop) = running();
    let (progress, http) = state(
        upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop)
            .await
            .unwrap(),
    );
    assert_eq!(progress.lease().state, v2::BlobLeaseState::Ready as i32);
    assert_eq!(http, Some(Ok(201)));
    assert!(net.body.is_none());
    assert!(net.requests.iter().all(|r| r == p.request()));
    assert_eq!(
        net.cipher_hashes[0].as_slice(),
        p.request().blob.as_ref().unwrap().sha256
    );
    let saved = snapshot(f.root.path(), A);
    let (_, http) = state(
        upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop)
            .await
            .unwrap(),
    );
    assert!(http.is_none());
    assert_eq!(net.cipher_hashes.len(), 1);
    assert!(net.actions.is_empty());
    assert_eq!(snapshot(f.root.path(), A), saved);
    let plan = f
        .alice
        .prepared_epoch_attachment(id.clone())
        .await
        .unwrap()
        .unwrap();
    let blob = f.alice.prepared_epoch_attachment_blob(plan).await.unwrap();
    let bytes = blob.as_bytes().to_vec();
    drop(blob);
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
    verify_secret(f.root.path(), &own, &id, &bytes);
}

#[tokio::test]
async fn lost_reserve_and_put_replies_recover_same_saved_intent_without_reseal() {
    let (f, _, id, p) = setup().await;
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    let mut ready = g.clone();
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    let mut net = Network::new([
        Action::Observe(Reply::Missing),
        Action::Reserve(Reply::Error(RpcError::LocalDeadline)),
        Action::Observe(Reply::Lease(g)),
        Action::Put(Err(BlobHttpError::Transport)),
        Action::Observe(Reply::Lease(ready)),
    ]);
    let (_tx, mut stop) = running();
    let saved = snapshot(f.root.path(), A);
    assert!(matches!(
        upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop).await,
        Err(BlobUploadError::Rpc(RpcError::LocalDeadline))
    ));
    assert_eq!(snapshot(f.root.path(), A), saved);
    let (progress, http) = state(
        upload_epoch_blob(&f.alice, &mut net, id, &mut stop)
            .await
            .unwrap(),
    );
    assert_eq!(progress.lease().state, v2::BlobLeaseState::Ready as i32);
    assert_eq!(http, Some(Err(BlobHttpError::Transport)));
    assert!(net.requests.iter().all(|r| r == p.request()));
    assert_eq!(net.cipher_hashes.len(), 1);
    assert!(net.actions.is_empty());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_post_put_observation_preserves_uncertainty_then_ready_skips_second_put() {
    let (f, _, id, p) = setup().await;
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    let mut ready = g.clone();
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    let mut net = Network::new([
        Action::Observe(Reply::Lease(g)),
        Action::Put(Ok(201)),
        Action::Observe(Reply::Error(RpcError::Transport)),
        Action::Observe(Reply::Lease(ready)),
    ]);
    let (_tx, mut stop) = running();
    assert!(matches!(
        upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop).await,
        Err(BlobUploadError::Rpc(RpcError::Transport))
    ));
    assert!(net.body.is_none());
    assert_eq!(
        f.alice
            .epoch_attachment_control(id.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    let (progress, http) = state(
        upload_epoch_blob(&f.alice, &mut net, id, &mut stop)
            .await
            .unwrap(),
    );
    assert_eq!(progress.lease().state, v2::BlobLeaseState::Ready as i32);
    assert!(http.is_none());
    assert_eq!(net.cipher_hashes.len(), 1);
    assert!(net.actions.is_empty());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn fresh_observation_stages_do_not_upload_or_renew_implicitly() {
    for phase in [
        v2::BlobLeaseState::Writing,
        v2::BlobLeaseState::Ready,
        v2::BlobLeaseState::Linked,
    ] {
        let (f, _, id, p) = setup().await;
        let mut net = Network::new([Action::Observe(Reply::Lease(grant(&p, phase)))]);
        let (_tx, mut stop) = running();
        let (progress, http) = state(
            upload_epoch_blob(&f.alice, &mut net, id, &mut stop)
                .await
                .unwrap(),
        );
        assert_eq!(progress.lease().state, phase as i32);
        assert!(http.is_none());
        assert!(net.actions.is_empty());
        assert!(net.cipher_hashes.is_empty());
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn expired_or_reclaimed_observation_commits_only_one_successor_then_replays_it() {
    for phase in [
        v2::BlobLeaseState::Reserved,
        v2::BlobLeaseState::Deleting,
        v2::BlobLeaseState::Reclaimed,
    ] {
        let (f, _, id, p) = setup().await;
        let mut g = grant(&p, phase);
        if phase == v2::BlobLeaseState::Reserved {
            expire(&mut g);
        }
        let mut net = Network::new([Action::Observe(Reply::Lease(g.clone()))]);
        let (_tx, mut stop) = running();
        let BlobUploadStep::Renewed(next) =
            upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop)
                .await
                .unwrap()
        else {
            panic!("expected renewal")
        };
        assert_ne!(next.request().operation_id, p.request().operation_id);
        assert_eq!(next.request().expected_revision, g.revision);
        assert_eq!(next.request().expected_attempt_id, g.attempt_id);
        assert_eq!(next.request().blob, p.request().blob);
        assert_eq!(next.request().message_id, p.request().message_id);
        assert!(net.actions.is_empty());
        assert!(net.cipher_hashes.is_empty());
        // A retry observes the persisted successor; a failure cannot mint another.
        let saved = snapshot(f.root.path(), A);
        let mut retry = Network::new([Action::Observe(Reply::Error(RpcError::Transport))]);
        assert!(
            upload_epoch_blob(&f.alice, &mut retry, id.clone(), &mut stop)
                .await
                .is_err()
        );
        assert_eq!(retry.requests[0], *next.request());
        assert_eq!(snapshot(f.root.path(), A), saved);
        assert_eq!(
            f.alice
                .epoch_attachment_control(id)
                .await
                .unwrap()
                .request(),
            next.request()
        );
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn stale_reply_decisions_use_saved_progress_and_linked_stays_sticky_after_reclaim() {
    let (f, _, id, p) = setup().await;
    let old = grant(&p, v2::BlobLeaseState::Reserved);
    let mut ready = old.clone();
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    f.alice
        .accept_epoch_blob_lease(observed(&p, ready))
        .await
        .unwrap();
    let saved = snapshot(f.root.path(), A);
    let (_tx, mut stop) = running();
    let mut net = Network::new([Action::Observe(Reply::Lease(old.clone()))]);
    let (progress, http) = state(
        upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop)
            .await
            .unwrap(),
    );
    assert_eq!(progress.lease().state, v2::BlobLeaseState::Ready as i32);
    assert!(http.is_none());
    assert_eq!(snapshot(f.root.path(), A), saved);
    let mut linked = old;
    linked.state = v2::BlobLeaseState::Linked as i32;
    linked.observed_at_unix_ms += 2;
    f.alice
        .accept_epoch_blob_lease(observed(&p, linked.clone()))
        .await
        .unwrap();
    linked.state = v2::BlobLeaseState::Reclaimed as i32;
    expire(&mut linked);
    let mut net = Network::new([Action::Observe(Reply::Lease(linked))]);
    let (progress, http) = state(
        upload_epoch_blob(&f.alice, &mut net, id, &mut stop)
            .await
            .unwrap(),
    );
    assert!(progress.linked_seen());
    assert_eq!(progress.plan().request(), p.request());
    assert!(http.is_none());
    assert!(net.actions.is_empty());
    assert!(net.cipher_hashes.is_empty());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn generic_not_found_wrong_binding_and_post_put_absence_never_reserve_or_reset() {
    let (f, _, id, p) = setup().await;
    let (_tx, mut stop) = running();
    let saved = snapshot(f.root.path(), A);
    for reply in [
        Reply::Error(RpcError::Status(tonic::Code::NotFound)),
        Reply::WrongMissing,
    ] {
        let mut net = Network::new([Action::Observe(reply)]);
        assert!(matches!(
            upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop).await,
            Err(BlobUploadError::Rpc(_))
        ));
        assert!(net.actions.is_empty());
        assert_eq!(snapshot(f.root.path(), A), saved);
    }
    let mut net = Network::new([
        Action::Observe(Reply::Lease(grant(&p, v2::BlobLeaseState::Reserved))),
        Action::Put(Ok(201)),
        Action::Observe(Reply::Missing),
    ]);
    assert!(matches!(
        upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop).await,
        Err(BlobUploadError::Rejected(
            v2::BlobFailureReason::OperationNotObserved
        ))
    ));
    assert!(net.body.is_none());
    assert!(net.actions.is_empty());
    assert_eq!(
        f.alice
            .epoch_attachment_control(id)
            .await
            .unwrap()
            .request(),
        p.request()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn last_body_owner_is_joined_before_post_put_observe_or_metadata_acceptance() {
    let (f, _, id, p) = setup().await;
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    let mut ready = g.clone();
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut net = Network::new([
        Action::Observe(Reply::Lease(g)),
        Action::Put(Ok(201)),
        Action::Observe(Reply::Lease(ready)),
    ]);
    net.join_gate = Some((entered.clone(), release.clone()));
    let (_tx, mut stop) = running();
    let mut pending = Box::pin(upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop));
    tokio::select! { _=entered.notified()=>{},_=&mut pending=>panic!("join barrier skipped") }
    assert!(matches!(
        f.alice.epoch_attachment_control(id).await,
        Err(CryptoError::Busy)
    ));
    release.notify_one();
    let (progress, _) = state(
        tokio::time::timeout(Duration::from_secs(2), pending.as_mut())
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(progress.lease().state, v2::BlobLeaseState::Ready as i32);
    drop(pending);
    assert!(net.body.is_none());
    assert!(net.actions.is_empty());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn stop_during_each_network_stage_joins_without_followup_rpc_or_losing_intent() {
    for stage in 0..3 {
        let (f, _, id, p) = setup().await;
        let g = grant(&p, v2::BlobLeaseState::Reserved);
        let entered = Arc::new(Notify::new());
        let actions = match stage {
            0 => vec![Action::Observe(Reply::Stall(entered.clone()))],
            1 => vec![
                Action::Observe(Reply::Lease(g)),
                Action::StallPut(entered.clone()),
            ],
            _ => vec![
                Action::Observe(Reply::Lease(g)),
                Action::Put(Ok(201)),
                Action::Observe(Reply::Stall(entered.clone())),
            ],
        };
        let mut net = Network::new(actions);
        let (tx, mut stop) = running();
        let mut pending = Box::pin(upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop));
        tokio::select! { _=entered.notified()=>{},_=&mut pending=>panic!("network did not stall") }
        tx.send(true).unwrap();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(2), pending.as_mut())
                .await
                .unwrap(),
            Err(BlobUploadError::Stopped)
        ));
        drop(pending);
        assert!(net.cancelled && net.body.is_none() && net.actions.is_empty());
        assert_eq!(
            f.alice
                .epoch_attachment_control(id)
                .await
                .unwrap()
                .request(),
            p.request()
        );
        f.alice.shutdown().await.unwrap();
        f.bob.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn http_success_without_ready_is_not_promoted_and_step_never_repeats_put() {
    let (f, _, id, p) = setup().await;
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    let mut net = Network::new([
        Action::Observe(Reply::Lease(g.clone())),
        Action::Put(Ok(201)),
        Action::Observe(Reply::Lease(g)),
    ]);
    let (_tx, mut stop) = running();
    let (progress, http) = state(
        upload_epoch_blob(&f.alice, &mut net, id, &mut stop)
            .await
            .unwrap(),
    );
    assert_eq!(progress.lease().state, v2::BlobLeaseState::Reserved as i32);
    assert_eq!(http, Some(Ok(201)));
    assert!(
        f.alice
            .epoch_outbox_page(None)
            .await
            .unwrap()
            .messages
            .is_empty()
    );
    assert_eq!(net.cipher_hashes.len(), 1);
    assert!(net.actions.is_empty());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_http_result_stops_without_reconciliation_using_old_authentication() {
    let (f, _, id, p) = setup().await;
    let mut net = Network::new([
        Action::Observe(Reply::Lease(grant(&p, v2::BlobLeaseState::Reserved))),
        Action::Put(Err(BlobHttpError::Cancelled)),
    ]);
    let (_tx, mut stop) = running();
    assert!(matches!(
        upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop).await,
        Err(BlobUploadError::Stopped)
    ));
    assert!(net.cancelled && net.body.is_none() && net.actions.is_empty());
    assert_eq!(
        f.alice
            .epoch_attachment_control(id)
            .await
            .unwrap()
            .request(),
        p.request()
    );
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropped_step_waiter_retains_body_until_owner_join_then_ready_retry_is_read_only() {
    let (f, _, id, p) = setup().await;
    let g = grant(&p, v2::BlobLeaseState::Reserved);
    let entered = Arc::new(Notify::new());
    let mut net = Network::new([
        Action::Observe(Reply::Lease(g.clone())),
        Action::StallPut(entered.clone()),
    ]);
    let (_tx, mut stop) = running();
    let mut pending = Box::pin(upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop));
    tokio::select! { _=entered.notified()=>{},_=&mut pending=>panic!("PUT did not stall") }
    drop(pending);
    assert!(net.body.is_some());
    assert!(matches!(
        f.alice.epoch_attachment_control(id.clone()).await,
        Err(CryptoError::Busy)
    ));
    net.cancel();
    net.join().await;
    assert_eq!(
        f.alice
            .epoch_attachment_control(id.clone())
            .await
            .unwrap()
            .request(),
        p.request()
    );
    let mut ready = g;
    ready.state = v2::BlobLeaseState::Ready as i32;
    ready.observed_at_unix_ms += 1;
    let mut retry = Network::new([Action::Observe(Reply::Lease(ready))]);
    let (progress, http) = state(
        upload_epoch_blob(&f.alice, &mut retry, id, &mut stop)
            .await
            .unwrap(),
    );
    assert_eq!(progress.lease().state, v2::BlobLeaseState::Ready as i32);
    assert!(http.is_none() && retry.cipher_hashes.is_empty() && retry.actions.is_empty());
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}

#[tokio::test]
async fn signalled_or_disconnected_stop_never_starts_network_or_changes_store() {
    let (f, _, id, _) = setup().await;
    let saved = snapshot(f.root.path(), A);
    for disconnected in [false, true] {
        let (tx, mut stop) = running();
        if !disconnected {
            tx.send(true).unwrap();
        }
        drop(tx);
        let mut net = Network::new([]);
        assert!(matches!(
            upload_epoch_blob(&f.alice, &mut net, id.clone(), &mut stop).await,
            Err(BlobUploadError::Stopped)
        ));
        assert!(net.requests.is_empty() && net.body.is_none() && net.cancelled);
        assert_eq!(snapshot(f.root.path(), A), saved);
    }
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
