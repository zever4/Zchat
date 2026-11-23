//! Real protected owner + bounded fault transport. Actual TLS/PG in native tests.
use super::*;
use crate::encryption::{native::RpcError, prekey_maintenance::*};
use crate::proto::e2ee::PreKeyInventory;
use async_trait::async_trait;
use std::{collections::HashMap, time::Duration};
use tokio::{sync::watch, time::Instant};
use tonic::Code;

fn inventory(e: &Engine, ec: u32, pq: u32) -> VerifiedPreKeyInventory {
    VerifiedPreKeyInventory::from_rpc(
        e.store.scope().clone(),
        &e.own,
        PreKeyInventory {
            own_device: Some(e.own.clone()),
            publication_revision: 2,
            public_material_ready: true,
            available_ec: ec,
            available_pq: pq,
            retained_ec: 32,
            retained_pq: 33,
            retained_signed: 1,
            publication_batches: 2,
            max_retained_ec: 4096,
            max_retained_pq: 4096,
            max_retained_signed: 128,
            max_publication_batches: 8192,
            active_signed_id: Some(1),
            active_last_resort_id: Some(33),
            active_signed_created_at_unix_ms: Some(0),
        },
    )
    .unwrap()
}
fn proof(e: &Engine, v: PreKeyInventory) -> VerifiedPreKeyInventory {
    VerifiedPreKeyInventory::from_rpc(e.store.scope().clone(), &e.own, v).unwrap()
}
#[test]
fn inventory_rejects_malformed_counts_quotas_metadata_and_other_generation() {
    let root = tempfile::tempdir().unwrap();
    let e = installed(root.path(), A);
    let base = inventory(&e, 32, 32).value().clone();
    for mutate in [
        |v: &mut PreKeyInventory| v.available_ec = 33,
        |v: &mut PreKeyInventory| v.available_pq = 33,
        |v: &mut PreKeyInventory| v.retained_ec = 4097,
        |v: &mut PreKeyInventory| v.retained_pq = 4097,
        |v: &mut PreKeyInventory| v.retained_signed = 129,
        |v: &mut PreKeyInventory| v.publication_batches = 8193,
        |v: &mut PreKeyInventory| v.publication_revision = 3,
        |v: &mut PreKeyInventory| v.max_retained_ec = 0,
        |v: &mut PreKeyInventory| v.max_retained_pq = 4097,
        |v: &mut PreKeyInventory| v.max_retained_signed = 129,
        |v: &mut PreKeyInventory| v.max_publication_batches = 8193,
        |v: &mut PreKeyInventory| v.active_signed_id = None,
        |v: &mut PreKeyInventory| v.active_signed_created_at_unix_ms = None,
        |v: &mut PreKeyInventory| v.active_signed_created_at_unix_ms = Some(-1),
        |v: &mut PreKeyInventory| v.active_last_resort_id = None,
        |v: &mut PreKeyInventory| v.public_material_ready = false,
        |v: &mut PreKeyInventory| v.retained_signed = 0,
        |v: &mut PreKeyInventory| v.own_device.as_mut().unwrap().generation_id = BG.into(),
    ] {
        let mut v = base.clone();
        mutate(&mut v);
        assert!(matches!(
            VerifiedPreKeyInventory::from_rpc(e.store.scope().clone(), &e.own, v),
            Err(RpcError::Invalid)
        ));
    }
    let mut zero = base;
    zero.active_signed_id = Some(0);
    zero.active_last_resort_id = Some(0);
    assert!(VerifiedPreKeyInventory::from_rpc(e.store.scope().clone(), &e.own, zero).is_ok());
}
#[test]
fn inventory_policy_is_independent_bounded_by_retained_quota_and_last_batch() {
    let root = tempfile::tempdir().unwrap();
    let e = installed(root.path(), A);
    let mut v = inventory(&e, 8, 8).value().clone();
    let policy = RefillPolicy::default();
    assert_eq!(policy.amounts(&v), None);
    v.available_ec = 7;
    assert_eq!(policy.amounts(&v), Some((25, 0)));
    v.available_pq = 0;
    v.available_ec = 0;
    assert_eq!(policy.amounts(&v), Some((32, 32)));
    v.retained_ec = 4095;
    v.retained_pq = 4094;
    assert_eq!(policy.amounts(&v), Some((1, 2)));
    v.retained_ec = 4096;
    v.retained_pq = 4096;
    assert_eq!(policy.amounts(&v), Some((0, 0)));
    v.retained_ec = 32;
    v.retained_pq = 33;
    v.publication_batches = 8191;
    assert_eq!(policy.amounts(&v), Some((32, 28)));
    v.publication_batches = 8192;
    assert_eq!(policy.amounts(&v), Some((0, 0)));
    // Verify the conservative final-slot bound against the real wire planner
    // using maximum-width IDs/revision, not an estimate of encoded length.
    let context = ClientContext {
        protocol_version: 1,
        device_generation_id: AG.into(),
    };
    let ec = (0..32)
        .map(|i| crate::proto::e2ee::OneTimePreKey {
            key_id: u32::MAX - i,
            public_key: [&[5][..], &[0; 32]].concat(),
        })
        .collect::<Vec<_>>();
    let pq = (0..32)
        .map(|i| crate::proto::e2ee::KyberPreKey {
            key_id: u32::MAX - i,
            public_key: [&[8][..], &[0; 1568]].concat(),
            signature: vec![0; 64],
            is_last_resort: false,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        crate::encryption::publication::prepare_upload_batches(
            context.clone(),
            i64::MAX as u64 - 8,
            None,
            ec.clone(),
            pq[..28].to_vec()
        )
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        crate::encryption::publication::prepare_upload_batches(
            context,
            i64::MAX as u64 - 8,
            None,
            ec,
            pq
        )
        .unwrap()
        .len(),
        2
    );
}
#[test]
fn inventory_owner_checks_atomic_revision_pointer_scope_and_resumes_same_plan_after_reopen() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let observation = inventory(&e, 7, 20);
    let mut bad = observation.value().clone();
    bad.active_signed_id = Some(0);
    assert!(matches!(
        e.refill_from_inventory(proof(&e, bad), RefillPolicy::default()),
        Err(CryptoError::Conflict)
    ));
    assert!(e.current_prekey_publication().unwrap().is_none());
    let InventoryRefill::Publication(p) = e
        .refill_from_inventory(observation.clone(), RefillPolicy::default())
        .unwrap()
    else {
        panic!()
    };
    assert_eq!((p.ec_count, p.pq_count), (25, 0));
    let own = e.own.clone();
    drop(e);
    let mut e = reopen(root.path(), &own);
    assert_eq!(
        e.refill_from_inventory(observation.clone(), RefillPolicy::default())
            .unwrap(),
        InventoryRefill::Publication(p.clone())
    );
    e.materialize_prekey_refill(&p.operation_id).unwrap();
    finish(&mut e);
    assert!(matches!(
        e.refill_from_inventory(observation, RefillPolicy::default()),
        Err(CryptoError::Conflict)
    ));
    let other_root = tempfile::tempdir().unwrap();
    let other = installed(other_root.path(), B);
    assert!(
        e.refill_from_inventory(inventory(&other, 0, 0), RefillPolicy::default())
            .is_err()
    );
    let alternate =
        crate::encryption::protected::StoreScope::new("https://other.example.test", A).unwrap();
    let cross_origin =
        VerifiedPreKeyInventory::from_rpc(alternate, &own, inventory(&e, 0, 0).value().clone())
            .unwrap();
    assert!(
        e.refill_from_inventory(cross_origin, RefillPolicy::default())
            .is_err()
    );
}

struct Fake {
    observation: VerifiedPreKeyInventory,
    inventories: usize,
    uploads: Vec<UploadPreKeysRequest>,
    committed: HashMap<String, UploadPreKeysRequest>,
    lose_reply: bool,
    block: bool,
    error: Option<Code>,
}
#[async_trait]
impl PreKeyMaintenanceTransport for Fake {
    async fn inventory(&mut self) -> Result<VerifiedPreKeyInventory, RpcError> {
        self.inventories += 1;
        if self.block {
            std::future::pending::<()>().await;
        }
        if let Some(code) = self.error {
            return Err(RpcError::Status(code));
        }
        Ok(self.observation.clone())
    }
    async fn upload(
        &mut self,
        upload: &PreKeyRefillUpload,
    ) -> Result<VerifiedPreKeyRefillUpload, RpcError> {
        let req = upload.request();
        self.uploads.push(req.clone());
        if let Some(old) = self.committed.insert(req.batch_id.clone(), req.clone()) {
            assert_eq!(old, *req);
        }
        if self.lose_reply {
            self.lose_reply = false;
            return Err(RpcError::Transport);
        }
        Ok(VerifiedPreKeyRefillUpload::from_rpc(
            upload.clone(),
            ack(req),
        ))
    }
}
fn fake(e: &Engine) -> Fake {
    Fake {
        observation: inventory(e, 0, 0),
        inventories: 0,
        uploads: vec![],
        committed: HashMap::new(),
        lose_reply: false,
        block: false,
        error: None,
    }
}
fn due(s: &mut PreKeyMaintenance) {
    s.due_for_test();
}

fn parts(e: Engine) -> (ProtectedStore, DeviceRef) {
    let Engine {
        store,
        own,
        runtime,
    } = e;
    // Engine fixture owns an independent Tokio runtime; drop it off the async
    // test executor, just as the production crypto owner drops its own runtime.
    std::thread::spawn(move || drop(runtime)).join().unwrap();
    (store, own)
}

#[test]
fn inventory_capacity_does_not_reserve_or_replace_and_pending_rotation_wins() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let mut v = inventory(&e, 0, 0).value().clone();
    v.retained_ec = 4096;
    v.retained_pq = 4096;
    assert_eq!(
        e.refill_from_inventory(proof(&e, v), RefillPolicy::default())
            .unwrap(),
        InventoryRefill::CapacityExhausted
    );
    assert!(e.current_prekey_publication().unwrap().is_none());
    let p = e.prepare_prekey_rotation(true, true, None).unwrap();
    assert_eq!(
        e.refill_from_inventory(inventory(&e, 0, 0), RefillPolicy::default())
            .unwrap(),
        InventoryRefill::Publication(p.clone())
    );
    e.materialize_prekey_refill(&p.operation_id).unwrap();
    let requests = finish(&mut e);
    assert_eq!(requests[0].signed_pre_key.as_ref().unwrap().key_id, 2);
    assert_eq!(requests[0].kyber_pre_keys[0].key_id, 34);
}

#[tokio::test]
async fn maintenance_resumes_reserved_rotation_before_network_inventory() {
    let root = tempfile::tempdir().unwrap();
    let mut e = installed(root.path(), A);
    let p = e.prepare_prekey_rotation(true, true, None).unwrap();
    let mut net = fake(&e);
    net.error = Some(Code::Unauthenticated);
    let (store, own) = parts(e);
    let worker = CryptoCoordinator::start(store, own).await.unwrap();
    let (_stop, mut receiver) = watch::channel(false);
    let mut scheduler = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::Uploaded { complete: true }
    ));
    assert_eq!(net.inventories, 0);
    assert_eq!(net.uploads.len(), 1);
    assert!(net.uploads[0].kyber_pre_keys[0].is_last_resort);
    assert!(net.uploads[0].one_time_pre_keys.is_empty());
    assert_eq!(
        worker
            .current_prekey_publication()
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        p.operation_id
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn maintenance_lost_remote_reply_restarts_exact_shared_plan_and_two_bounded_batches() {
    let root = tempfile::tempdir().unwrap();
    let e = installed(root.path(), A);
    let own = e.own.clone();
    let mut net = fake(&e);
    net.lose_reply = true;
    let (store, _) = parts(e);
    let worker = CryptoCoordinator::start(store, own.clone()).await.unwrap();
    let (_stop, mut receiver) = watch::channel(false);
    let mut scheduler = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
    let MaintenanceStep::Planned(p) = scheduler
        .tick(&worker, &mut net, &mut receiver)
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!((p.ec_count, p.pq_count), (32, 32));
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::Waiting
    ));
    due(&mut scheduler);
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::Retry(_)
    ));
    let first = net.uploads[0].clone();
    worker.shutdown().await.unwrap();
    let e = reopen(root.path(), &own);
    let (store, _) = parts(e);
    let worker = CryptoCoordinator::start(store, own).await.unwrap();
    let mut scheduler = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::Uploaded { complete: false }
    ));
    assert_eq!(net.uploads[1], first);
    due(&mut scheduler);
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::Uploaded { complete: true }
    ));
    assert_eq!(net.inventories, 1);
    assert_eq!(net.committed.len(), 2);
    assert_eq!(
        worker
            .current_prekey_publication()
            .await
            .unwrap()
            .unwrap()
            .operation_id,
        p.operation_id
    );
    worker.shutdown().await.unwrap();
}
#[tokio::test]
async fn maintenance_cancellation_deadline_auth_halt_and_concurrent_policy_preserve_state() {
    let root = tempfile::tempdir().unwrap();
    let e = installed(root.path(), A);
    let mut net = fake(&e);
    net.block = true;
    let observation = net.observation.clone();
    let (store, own) = parts(e);
    let worker = CryptoCoordinator::start(store, own).await.unwrap();
    let (stop, mut receiver) = watch::channel(true);
    let mut scheduler = PreKeyMaintenance::new(MaintenanceConfig {
        step_timeout: Duration::from_millis(10),
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::Stopped
    ));
    assert_eq!(net.inventories, 0);
    stop.send(false).unwrap();
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::TimedOut
    ));
    assert!(worker.current_prekey_publication().await.unwrap().is_none());
    due(&mut scheduler);
    let cancel = async {
        tokio::time::sleep(Duration::from_millis(1)).await;
        stop.send(true).unwrap();
    };
    let (value, ()) = tokio::join!(scheduler.tick(&worker, &mut net, &mut receiver), cancel);
    assert!(matches!(value.unwrap(), MaintenanceStep::Stopped));
    stop.send(false).unwrap();
    net.block = false;
    net.error = Some(Code::Unauthenticated);
    let mut scheduler = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
    assert!(matches!(
        scheduler.tick(&worker, &mut net, &mut receiver).await,
        Err(MaintenanceError::Rpc(RpcError::Status(
            Code::Unauthenticated
        )))
    ));
    assert!(matches!(
        scheduler
            .tick(&worker, &mut net, &mut receiver)
            .await
            .unwrap(),
        MaintenanceStep::Halted
    ));
    assert!(worker.current_prekey_publication().await.unwrap().is_none());
    let (left, right) = tokio::join!(
        worker.refill_from_inventory(observation.clone(), RefillPolicy::default()),
        worker.refill_from_inventory(observation, RefillPolicy::default())
    );
    assert_eq!(left.unwrap(), right.unwrap());
    worker.shutdown().await.unwrap();
}
#[test]
fn maintenance_configuration_backoff_jitter_and_caps() {
    for config in [
        MaintenanceConfig {
            policy: RefillPolicy {
                low_water: 0,
                target: 32,
            },
            ..Default::default()
        },
        MaintenanceConfig {
            policy: RefillPolicy {
                low_water: 8,
                target: 33,
            },
            ..Default::default()
        },
        MaintenanceConfig {
            retry_initial: Duration::ZERO,
            ..Default::default()
        },
        MaintenanceConfig {
            retry_initial: Duration::from_secs(61),
            ..Default::default()
        },
    ] {
        assert!(PreKeyMaintenance::new(config).is_err());
    }
    let mut scheduler = PreKeyMaintenance::new(MaintenanceConfig::default()).unwrap();
    for failures in 1..=64 {
        scheduler.failures_for_test(failures);
        let d = scheduler.delay_for_test(true, false);
        let nominal = Duration::from_secs(1u64 << failures.saturating_sub(1).min(20))
            .min(Duration::from_secs(60));
        assert!(d >= nominal.mul_f64(0.8) && d <= nominal);
    }
    let interval = scheduler.delay_for_test(false, false);
    assert!((Duration::from_secs(240)..=Duration::from_secs(300)).contains(&interval));
    assert!(scheduler.next_due() <= Instant::now());
}
