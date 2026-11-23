//! Local timer provenance is distinct from remote gRPC codes and owner stop.
use super::*;
use crate::encryption::delivery_runner::{DeliveryIssue, Operation, RoundReport};

#[test]
fn local_transport_deadline_is_typed_even_when_tonic_uses_cancelled() {
    let status = tonic::Status::from_error(Box::new(tonic::TimeoutExpired(())));
    assert_eq!(status.code(), Code::Cancelled);
    assert_eq!(RpcError::from_status(status), RpcError::LocalDeadline);
}

#[test]
fn remote_codes_and_timeout_text_cannot_claim_a_local_timer() {
    for code in [Code::Cancelled, Code::DeadlineExceeded, Code::Unknown] {
        let status = tonic::Status::new(code, "Timeout expired");
        assert_eq!(RpcError::from_status(status), RpcError::Status(code));
    }
}

#[test]
fn wrapped_local_timeout_discards_source_diagnostics() {
    #[derive(Debug, thiserror::Error)]
    #[error("sensitive native connection detail")]
    struct Wrapped(#[source] tonic::TimeoutExpired);
    let status = tonic::Status::from_error(Box::new(Wrapped(tonic::TimeoutExpired(()))));
    let error = RpcError::from_status(status);
    assert_eq!(error, RpcError::LocalDeadline);
    assert!(!format!("{error:?} {error}").contains("sensitive"));
}

#[test]
fn proven_deadline_yields_the_lane_without_becoming_a_user_stop() {
    let mut report = RoundReport::default();
    assert!(
        report
            .rpc(Operation::Send, RpcError::LocalDeadline)
            .unwrap()
    );
    assert!(matches!(
        report.issues.as_slice(),
        [crate::encryption::delivery_runner::Issue {
            operation: Operation::Send,
            error: DeliveryIssue::Rpc(RpcError::LocalDeadline),
        }]
    ));
}

#[async_trait::async_trait]
trait Probe {
    async fn call(&mut self) -> Result<()>;
}
#[async_trait::async_trait]
impl Probe for NativeV1Client {
    async fn call(&mut self) -> Result<()> {
        self.capabilities().await.map(|_| ())
    }
}
#[async_trait::async_trait]
impl Probe for NativeEpochClient {
    async fn call(&mut self) -> Result<()> {
        self.capabilities().await.map(|_| ())
    }
}
async fn probe(api: &mut impl Probe) {
    for code in [Code::Cancelled, Code::DeadlineExceeded] {
        assert_eq!(api.call().await, Err(RpcError::Status(code)));
    }
    let started = std::time::Instant::now();
    assert_eq!(api.call().await, Err(RpcError::LocalDeadline));
    assert!(
        started.elapsed() >= Duration::from_secs(9) && started.elapsed() < Duration::from_secs(15)
    );
    // The same channel is usable after the timer drops the in-flight RPC.
    assert_eq!(api.call().await, Err(RpcError::Status(Code::Cancelled)));
}

/// Go owns a dedicated TLS fixture; no app server, account DB or Signal state.
#[test]
#[ignore = "requires the Go TLS deadline fixture"]
fn go_deadline_worker() {
    use std::io::Read;
    let mut input = String::new();
    std::io::stdin()
        .take(17000)
        .read_to_string(&mut input)
        .unwrap();
    let fields: Vec<_> = input.lines().collect();
    assert_eq!(fields.len(), 2);
    let ca = std::fs::read(fields[1]).unwrap();
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let account = "11111111-1111-4111-8111-111111111111";
            let mut registry = NativeV1Client::connect(
                NativeEndpoint::new(fields[0], Some(&ca)).unwrap(),
                account,
                Zeroizing::new("synthetic.jwt.token".into()),
                Zeroizing::new([11; 32]),
            )
            .await
            .unwrap();
            registry
                .bind_generation(&DeviceRef {
                    user_id: account.into(),
                    device_id: 1,
                    generation_id: "22222222-2222-4222-8222-222222222222".into(),
                })
                .unwrap();
            registry.bind_store_scope(StoreScope::new("https://deadline.test", account).unwrap());
            let mut epoch = registry.epoch_transport().unwrap();
            probe(&mut registry).await;
            probe(&mut epoch).await;
            println!("zchat-native-deadline-complete:v2");
        });
}
