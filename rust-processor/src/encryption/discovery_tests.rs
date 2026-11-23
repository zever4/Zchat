use super::*;
use crate::encryption::{
    native::validate_own,
    protected::{ProtectedStore, test_open},
};
use crate::proto::e2ee::{Device, DeviceRef};

const ACCOUNT: &str = "11111111-1111-4111-8111-111111111111";
const OTHER: &str = "22222222-2222-4222-8222-222222222222";
const ORIGIN: &str = "https://chat.example.test";

#[test]
fn trusted_tuple_rejects_downgrade_and_unsafe_origins() {
    for (http, grpc) in [
        (ORIGIN, "http://localhost:50051"),
        ("http://example.test", "https://rpc.example.test"),
        (ORIGIN, "https://rpc.example.test/?token=secret"),
        (ORIGIN, "https://name:password@rpc.example.test"),
        ("https://chat.example.test/chat.html", ORIGIN),
    ] {
        assert!(TrustedServer::new(http, grpc, None).is_err());
    }
    assert!(TrustedServer::new(ORIGIN, "https://rpc.example.test:50051", None).is_ok());
    assert!(TrustedServer::new("http://localhost:18080", "http://127.0.0.1:50051", None).is_ok());
    assert_eq!(origin("https://CHAT.example.test:443/").unwrap(), ORIGIN);
    assert!(matches!(
        TrustedServer::new(ORIGIN, "https://rpc.example.test", Some(b"not a PEM CA")),
        Err(DiscoveryError::Scope)
    ));
}

#[tokio::test]
async fn stale_server_selection_and_bad_token_fail_before_network_access() {
    let config = TrustedServer::new(ORIGIN, "https://127.0.0.1:1", None).unwrap();
    assert!(matches!(
        config
            .discover(
                "https://other.example.test",
                Zeroizing::new("valid.jwt".into())
            )
            .await,
        Err(DiscoveryError::Scope)
    ));
    let config = TrustedServer::new(ORIGIN, "https://127.0.0.1:1", None).unwrap();
    assert!(matches!(
        config
            .discover(ORIGIN, Zeroizing::new("injected\r\n".into()))
            .await,
        Err(DiscoveryError::Rpc(RpcError::Invalid))
    ));
}

#[test]
fn discovery_response_requires_canonical_account_and_valid_device() {
    let empty = OwnDeviceState {
        user_id: ACCOUNT.into(),
        active_device: None,
    };
    assert!(validate_own(&empty).is_ok());
    for id in [
        "",
        "42",
        "../another-account",
        "11111111-1111-1111-8111-111111111111",
    ] {
        let mut bad = empty.clone();
        bad.user_id = id.into();
        assert!(validate_own(&bad).is_err());
    }
    let valid = OwnDeviceState {
        user_id: ACCOUNT.into(),
        active_device: Some(Device {
            reference: Some(DeviceRef {
                user_id: ACCOUNT.into(),
                device_id: 1,
                generation_id: OTHER.into(),
            }),
            registration_id: 123,
            identity_key: [vec![5], vec![1; 32]].concat(),
            public_material_ready: true,
            public_material_revision: 1,
        }),
    };
    assert!(validate_own(&valid).is_ok());
    for change in 0..7 {
        let mut bad = valid.clone();
        let d = bad.active_device.as_mut().unwrap();
        match change {
            0 => d.reference.as_mut().unwrap().user_id = OTHER.into(),
            1 => d.registration_id = 0,
            2 => d.registration_id = 16384,
            3 => d.public_material_revision = 0,
            4 => d.public_material_revision = u64::MAX,
            5 => d.identity_key[0] = 4,
            _ => d.reference.as_mut().unwrap().device_id = 2,
        }
        assert!(validate_own(&bad).is_err(), "mutation {change}");
    }
}

#[test]
fn authenticated_scope_separates_accounts_and_servers_and_never_opens_files() {
    let root = tempfile::tempdir().unwrap();
    let a = StoreScope::new(ORIGIN, ACCOUNT).unwrap();
    let b = StoreScope::new(ORIGIN, OTHER).unwrap();
    let other_server = StoreScope::new("https://other.example.test", ACCOUNT).unwrap();
    assert_ne!(a.database_path(root.path()), b.database_path(root.path()));
    assert_ne!(
        a.database_path(root.path()),
        other_server.database_path(root.path())
    );
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    let store: ProtectedStore = test_open(root.path(), a.clone(), true);
    let local = DeviceProvisioning::new(store);
    assert!(local.scope() == &a);
    assert!(local.scope() != &b);
    assert!(local.scope() != &other_server);
}
