//! Genuine protected Signal/outbox plans, synthetic trusted reply validation.
//! No raw plan constructor; actual TLS/Go/PG is exercised by owned-file fixture.
use super::incoming::tests::{payload, prepared};
use super::tests::records;
use super::*;
use crate::encryption::epoch_contract as data;
use crate::encryption::native::{NativeEpochClient, RpcError};
const A: &str = "11111111-1111-4111-8111-111111111111";

async fn accepted(
    w: &CryptoCoordinator,
    scope: &EpochScope,
    p: &EpochOutgoingPlan,
) -> EpochOutgoingPlan {
    let e = p.request().envelope.as_ref().unwrap();
    let receipt = v2::SendReceipt {
        message_id: e.message_id.clone(),
        envelope_digest: data::envelope_digest(e).unwrap().to_vec(),
        stored_at_unix_ms: 1700000000000,
        expires_at_unix_ms: 1700000000000 + crate::encryption::contract::MESSAGE_RETENTION_MS,
        crypto_epoch: e.crypto_epoch,
    };
    w.accept_epoch_send_receipt(EpochObservation::test_observation(
        scope.scope.clone(),
        p.clone(),
        receipt,
    ))
    .await
    .unwrap();
    w.epoch_outgoing_plan(e.message_id.clone()).await.unwrap()
}
fn reply(p: &EpochOutgoingPlan) -> v2::FileStatuses {
    let e = p.request().envelope.as_ref().unwrap();
    let receipt = p.receipt().unwrap().clone();
    v2::FileStatuses {
        files: vec![v2::FileStatus {
            format_version: 1,
            reference: Some(v2::FileStatusReference {
                binding: Some(data::send_binding(e).unwrap()),
                blob: e.blob.clone(),
            }),
            observed_at_unix_ms: receipt.expires_at_unix_ms + 100,
            expires_at_unix_ms: receipt.expires_at_unix_ms,
            acknowledged_at_unix_ms: Some(receipt.expires_at_unix_ms + 50),
            recipient_replaced_at_unix_ms: None,
            receipt: Some(receipt),
        }],
    }
}
#[tokio::test]
async fn file_status_transport_rejects_unknown_send_text_foreign_scope_and_duplicate_before_rpc() {
    let (f, a, b) = prepared().await;
    let (p, _) = super::attachment_tests::message(&a);
    let pending = f.alice.encrypt_epoch(a.clone(), p).await.unwrap();
    let mut client = NativeEpochClient::test_metadata_transport(a.scope.clone(), a.own.clone());
    assert!(matches!(
        client.file_statuses_prepared(&[]).await,
        Err(RpcError::Invalid)
    ));
    assert!(matches!(
        client
            .file_statuses_prepared(std::slice::from_ref(&pending))
            .await,
        Err(RpcError::Invalid)
    ));
    let plan = accepted(&f.alice, &a, &pending).await;
    let text = f
        .alice
        .encrypt_epoch(a.clone(), payload(&a, "text is not a file"))
        .await
        .unwrap();
    let text = accepted(&f.alice, &a, &text).await;
    assert!(matches!(
        client.file_statuses_prepared(&[text]).await,
        Err(RpcError::Invalid)
    ));
    assert!(matches!(
        client
            .file_statuses_prepared(&[plan.clone(), plan.clone()])
            .await,
        Err(RpcError::Invalid)
    ));
    assert!(matches!(
        client.file_statuses_prepared(&vec![plan.clone(); 33]).await,
        Err(RpcError::Invalid)
    ));
    let mut other = a.own.clone();
    other.generation_id = uuid::Uuid::new_v4().to_string();
    let before = records(f.root.path(), A);
    for (scope, own) in [
        (a.scope.clone(), b.own.clone()),
        (a.scope.clone(), other),
        (
            StoreScope::new("https://foreign.example.test", A).unwrap(),
            a.own.clone(),
        ),
    ] {
        let mut client = NativeEpochClient::test_metadata_transport(scope, own);
        assert!(matches!(
            client
                .file_statuses_prepared(std::slice::from_ref(&plan))
                .await,
            Err(RpcError::Invalid)
        ));
    }
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
#[tokio::test]
async fn file_status_transport_checks_exact_known_receipt_without_local_mutation() {
    let (f, a, _) = prepared().await;
    let (p, _) = super::attachment_tests::message(&a);
    let p = f.alice.encrypt_epoch(a.clone(), p).await.unwrap();
    let p = accepted(&f.alice, &a, &p).await;
    let client = NativeEpochClient::test_metadata_transport(a.scope.clone(), a.own.clone());
    let before = records(f.root.path(), A);
    let good = reply(&p);
    let observation = client
        .test_file_status_observation(std::slice::from_ref(&p), good.clone())
        .unwrap();
    assert_eq!(observation.response(), &good);
    assert_eq!(observation.request()[0].request(), p.request());
    for case in 0..6 {
        let mut bad = good.clone();
        let s = &mut bad.files[0];
        match case {
            0 => s.format_version = 2,
            1 => s.reference.as_mut().unwrap().blob.as_mut().unwrap().sha256[0] ^= 1,
            2 => s.receipt.as_mut().unwrap().crypto_epoch = None,
            3 => s.acknowledged_at_unix_ms = Some(0),
            4 => bad.files.clear(),
            _ => {
                let r = s.receipt.as_mut().unwrap();
                r.stored_at_unix_ms += 1;
                r.expires_at_unix_ms += 1;
                s.expires_at_unix_ms += 1;
            }
        }
        assert!(matches!(
            client.test_file_status_observation(std::slice::from_ref(&p), bad),
            Err(RpcError::Invalid)
        ));
    }
    assert_eq!(records(f.root.path(), A), before);
    f.alice.shutdown().await.unwrap();
    f.bob.shutdown().await.unwrap();
}
