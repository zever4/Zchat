use libsignal_protocol::{
    DeviceId, Direction, IdentityKey, IdentityKeyStore, KeyPair, ProtocolAddress,
    SignalProtocolError,
};
use rust_processor::encryption::{EncryptionError, Stores, grpc::GrpcMessenger};

#[tokio::test]
async fn legacy_reopen_and_exhaustion_do_not_generate_more_keys() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut stores = Stores::new(tmp.path().to_str().unwrap()).unwrap();
    stores.ensure_keys_generated().await.unwrap();
    let identity = stores
        .identity
        .get_identity_key_pair()
        .await
        .unwrap()
        .serialize();
    let conn = rusqlite::Connection::open(tmp.path()).unwrap();
    let counts = |c: &rusqlite::Connection| -> (i64, i64, i64) {
        c.query_row("SELECT (SELECT COUNT(*) FROM prekeys),(SELECT COUNT(*) FROM signed_prekeys),(SELECT COUNT(*) FROM kyber_pre_keys)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).unwrap()
    };
    let before = counts(&conn);
    assert_eq!(before, (100, 1, 101));
    drop(stores);
    for _ in 0..3 {
        let mut stores = Stores::new(tmp.path().to_str().unwrap()).unwrap();
        stores.ensure_keys_generated().await.unwrap();
        assert_eq!(
            stores
                .identity
                .get_identity_key_pair()
                .await
                .unwrap()
                .serialize(),
            identity
        );
        assert_eq!(counts(&conn), before);
    }
    conn.execute("UPDATE prekeys SET used=1", []).unwrap();
    let mut stores = Stores::new(tmp.path().to_str().unwrap()).unwrap();
    stores.ensure_keys_generated().await.unwrap();
    assert!(
        stores
            .prekey
            .get_unused_pre_keys(1)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(counts(&conn), before);
    conn.execute("DELETE FROM signed_prekeys", []).unwrap();
    assert!(stores.ensure_keys_generated().await.is_err());
    assert_eq!(counts(&conn), (100, 0, 101));
    conn.execute("DROP TABLE kyber_pre_keys", []).unwrap();
    assert!(stores.ensure_keys_generated().await.is_err());
}

#[tokio::test]
async fn missing_identity_is_none_and_pinned_identity_cannot_be_overwritten() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let mut stores = Stores::new(tmp.path().to_str().unwrap()).unwrap();
    let peer = ProtocolAddress::new("peer".into(), DeviceId::new(1).unwrap());
    assert!(stores.identity.get_identity(&peer).await.unwrap().is_none());
    let one = IdentityKey::from(KeyPair::generate(&mut rand::rng()).public_key);
    let two = IdentityKey::from(KeyPair::generate(&mut rand::rng()).public_key);
    stores.identity.save_identity(&peer, &one).await.unwrap();
    assert!(matches!(
        stores.identity.save_identity(&peer, &two).await,
        Err(SignalProtocolError::UntrustedIdentity(_))
    ));
    assert_eq!(
        stores.identity.get_identity(&peer).await.unwrap(),
        Some(one)
    );
    assert!(
        !stores
            .identity
            .is_trusted_identity(&peer, &two, Direction::Sending)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn legacy_grpc_connect_is_retired_before_any_network_access() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let stores = Stores::new(tmp.path().to_str().unwrap()).unwrap();
    assert!(matches!(
        GrpcMessenger::connect("this is not a URI", stores).await,
        Err(EncryptionError::LegacyTransportRetired)
    ));
}
