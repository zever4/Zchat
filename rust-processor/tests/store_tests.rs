use std::time::SystemTime;

use libsignal_protocol::{
    CiphertextMessage, DeviceId, Direction, GenericSignedPreKey, IdentityChange, IdentityKey,
    IdentityKeyStore, KeyPair, KyberPreKeyId, KyberPreKeyStore, PreKeyBundle, PreKeyId,
    PreKeyRecord, PreKeyStore, ProtocolAddress, SessionRecord, SessionStore, SignedPreKeyId,
    SignedPreKeyStore, error::SignalProtocolError, kem, message_decrypt, message_encrypt,
    process_prekey_bundle,
};
use rust_processor::encryption::Stores;

fn setup_temp_stores() -> (Stores, tempfile::NamedTempFile) {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let stores = Stores::new(tmp.path().to_str().unwrap()).unwrap();
    (stores, tmp)
}

#[tokio::test]
async fn test_stores_new_creates_tables() {
    let (stores, _tmp) = setup_temp_stores();

    // Проверяем через публичные методы — если таблицы не созданы, упадёт с db error
    stores.identity.get_user_info().unwrap();

    match stores.identity.get_identity_key_pair().await {
        Err(SignalProtocolError::InvalidState(_, _)) => {}
        Err(e) => panic!("unexpected error: {:?}", e),
        Ok(_) => panic!("expected empty identity, but got one"),
    }

    stores
        .session
        .load_session(&libsignal_protocol::ProtocolAddress::new(
            "test".to_string(),
            libsignal_protocol::DeviceId::new(1).unwrap(),
        ))
        .await
        .unwrap();

    stores.prekey.get_unused_pre_keys(1).await.unwrap();

    stores
        .signed_prekey
        .get_active_signed_pre_key()
        .await
        .unwrap_err();

    stores
        .kyber_prekey
        .get_unused_kyber_pre_keys(1)
        .await
        .unwrap();

    stores
        .kyber_prekey
        .get_last_resort_kyber_pre_key()
        .await
        .unwrap();
}

#[tokio::test]
async fn test_ensure_keys_generates_all() {
    let (mut stores, _tmp) = setup_temp_stores();

    stores.ensure_keys_generated().await.unwrap();

    let identity = stores.identity.get_identity_key_pair().await.unwrap();
    assert!(!identity.public_key().serialize().is_empty());

    let ec = stores.prekey.get_unused_pre_keys(100).await.unwrap();
    assert_eq!(ec.len(), 100);

    let signed = stores
        .signed_prekey
        .get_active_signed_pre_key()
        .await
        .unwrap();
    assert!(signed.public_key().is_ok());

    let kyber = stores
        .kyber_prekey
        .get_unused_kyber_pre_keys(100)
        .await
        .unwrap();
    assert_eq!(kyber.len(), 100);

    let last_resort = stores
        .kyber_prekey
        .get_last_resort_kyber_pre_key()
        .await
        .unwrap();
    assert!(last_resort.is_some());
}

#[tokio::test]
async fn test_ensure_keys_idempotent() {
    let (mut stores, _tmp) = setup_temp_stores();

    stores.ensure_keys_generated().await.unwrap();
    let first_identity = stores.identity.get_identity_key_pair().await.unwrap();

    stores.ensure_keys_generated().await.unwrap();
    let second_identity = stores.identity.get_identity_key_pair().await.unwrap();

    // Identity не меняется
    assert_eq!(
        first_identity.public_key().serialize(),
        second_identity.public_key().serialize()
    );

    // Prekeys не дублируются
    let ec = stores.prekey.get_unused_pre_keys(200).await.unwrap();
    assert_eq!(ec.len(), 100);

    // Kyber тоже
    let kyber = stores
        .kyber_prekey
        .get_unused_kyber_pre_keys(200)
        .await
        .unwrap();
    assert_eq!(kyber.len(), 100);
}

#[tokio::test]
async fn test_prekey_save_and_get() {
    let (mut stores, _tmp) = setup_temp_stores();

    let mut csprng = rand::rng();
    let key_pair = KeyPair::generate(&mut csprng);
    let id = PreKeyId::from(42);
    let record = PreKeyRecord::new(id, &key_pair);

    stores.prekey.save_pre_key(id, &record).await.unwrap();

    let retrieved = stores.prekey.get_pre_key(id).await.unwrap();

    assert_eq!(u32::from(id), u32::from(retrieved.id().unwrap()));

    assert_eq!(
        key_pair.public_key.serialize(),
        retrieved.key_pair().unwrap().public_key.serialize()
    );
}

#[tokio::test]
async fn test_prekey_remove_and_filter() {
    let (mut stores, _tmp) = setup_temp_stores();

    // Генерируем 3 ключа
    let mut csprng = rand::rng();
    for i in 1..=3 {
        let key_pair = KeyPair::generate(&mut csprng);
        let id = PreKeyId::from(i);
        let record = PreKeyRecord::new(id, &key_pair);
        stores.prekey.save_pre_key(id, &record).await.unwrap();
    }

    // Удаляем (soft delete) ключ 2
    stores
        .prekey
        .remove_pre_key(PreKeyId::from(2))
        .await
        .unwrap();

    // Проверяем: used = 1, но запись в БД осталась
    // get_pre_key для used должен упасть
    let result = stores.prekey.get_pre_key(PreKeyId::from(2)).await;
    assert!(matches!(result, Err(SignalProtocolError::InvalidPreKeyId)));

    // get_unused — только 2 ключа (1 и 3)
    let unused = stores.prekey.get_unused_pre_keys(10).await.unwrap();
    assert_eq!(unused.len(), 2);
    let ids: Vec<u32> = unused.iter().map(|(id, _)| u32::from(*id)).collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&3));
    assert!(!ids.contains(&2));
}

#[tokio::test]
async fn test_signed_prekey_save_and_get() {
    let (mut stores, _tmp) = setup_temp_stores();
    stores.ensure_keys_generated().await.unwrap();

    let active = stores
        .signed_prekey
        .get_active_signed_pre_key()
        .await
        .unwrap();
    let id = active.id().unwrap();

    // Получаем по ID
    let by_id = stores.signed_prekey.get_signed_pre_key(id).await.unwrap();
    assert_eq!(
        active.public_key().unwrap().serialize(),
        by_id.public_key().unwrap().serialize()
    );
}

#[tokio::test]
async fn test_kyber_save_and_get() {
    let (mut stores, _tmp) = setup_temp_stores();
    stores.ensure_keys_generated().await.unwrap();

    // One-time kyber
    let one_time = stores
        .kyber_prekey
        .get_unused_kyber_pre_keys(1)
        .await
        .unwrap();

    assert_eq!(one_time.len(), 1);
    let (id, record, is_last_resort) = &one_time[0];
    assert!(!is_last_resort);
    assert_eq!(u32::from(*id), 1);

    // Получаем через трейт
    let retrieved = stores.kyber_prekey.get_kyber_pre_key(*id).await.unwrap();
    assert_eq!(
        record.public_key().unwrap().serialize(),
        retrieved.public_key().unwrap().serialize()
    );

    // Last-resort kyber
    let last_resort = stores
        .kyber_prekey
        .get_last_resort_kyber_pre_key()
        .await
        .unwrap();
    assert!(last_resort.is_some());
    let (lr_id, lr_record) = last_resort.unwrap();

    // Last-resort не в one-time выборке
    let one_time_ids: Vec<u32> = stores
        .kyber_prekey
        .get_unused_kyber_pre_keys(200)
        .await
        .unwrap()
        .iter()
        .map(|(id, _, _)| u32::from(*id))
        .collect();
    assert!(!one_time_ids.contains(&u32::from(lr_id)));

    // Last-resort можно получить через get_kyber_pre_key
    let lr_retrieved = stores.kyber_prekey.get_kyber_pre_key(lr_id).await.unwrap();
    assert_eq!(
        lr_record.public_key().unwrap().serialize(),
        lr_retrieved.public_key().unwrap().serialize()
    );
}

#[tokio::test]
async fn test_kyber_mark_used() {
    let (mut stores, _tmp) = setup_temp_stores();
    stores.ensure_keys_generated().await.unwrap();

    let mut csprng = rand::rng();

    // One-time: удаляется при использовании
    let one_time = stores
        .kyber_prekey
        .get_unused_kyber_pre_keys(1)
        .await
        .unwrap();
    let (ot_id, _, _) = one_time.into_iter().next().unwrap();

    let ec_id = SignedPreKeyId::from(1);
    let base_key = KeyPair::generate(&mut csprng).public_key;

    stores
        .kyber_prekey
        .mark_kyber_pre_key_used(ot_id, ec_id, &base_key)
        .await
        .unwrap();

    // Удалён — get_kyber_pre_key падает
    let result = stores.kyber_prekey.get_kyber_pre_key(ot_id).await;
    assert!(matches!(
        result,
        Err(SignalProtocolError::InvalidKyberPreKeyId)
    ));

    // Last-resort: не удаляется, логируется
    let (lr_id, _) = stores
        .kyber_prekey
        .get_last_resort_kyber_pre_key()
        .await
        .unwrap()
        .unwrap();

    stores
        .kyber_prekey
        .mark_kyber_pre_key_used(lr_id, ec_id, &base_key)
        .await
        .unwrap();

    // Всё ещё доступен
    let lr = stores.kyber_prekey.get_kyber_pre_key(lr_id).await.unwrap();
    assert!(lr.public_key().is_ok());

    // Дубль — ошибка
    let dup_result = stores
        .kyber_prekey
        .mark_kyber_pre_key_used(lr_id, ec_id, &base_key)
        .await;
    assert!(matches!(
        dup_result,
        Err(SignalProtocolError::DuplicatedMessage(_, _))
    ));
}

#[tokio::test]
async fn test_kyber_last_resort_required() {
    let (stores, _tmp) = setup_temp_stores();

    let lr = stores
        .kyber_prekey
        .get_last_resort_kyber_pre_key()
        .await
        .unwrap();
    assert!(lr.is_none());
}

#[tokio::test]
async fn test_encrypt_decrypt_roundtrip() {
    let (mut alice, _tmp1) = setup_temp_stores();
    let (mut bob, _tmp2) = setup_temp_stores();

    alice.ensure_keys_generated().await.unwrap();
    bob.ensure_keys_generated().await.unwrap();

    let alice_identity = alice.identity.get_identity_key_pair().await.unwrap();
    assert!(!alice_identity.serialize().is_empty());

    let mut rng = rand::rng();

    // Bob's keys for Alice to build PreKeyBundle
    let bob_identity = bob.identity.get_identity_key_pair().await.unwrap();
    let bob_reg_id = bob.identity.get_local_registration_id().await.unwrap();
    let bob_signed = bob.signed_prekey.get_active_signed_pre_key().await.unwrap();
    let bob_signed_id: u32 = bob_signed.id().unwrap().into();

    let bob_ot = bob.prekey.get_unused_pre_keys(1).await.unwrap();
    let (bob_ot_id, bob_ot_pair) = bob_ot.into_iter().next().unwrap();

    let bob_kyber = bob.kyber_prekey.get_unused_kyber_pre_keys(1).await.unwrap();
    let (bob_kyber_id, bob_kyber_record, _) = bob_kyber.into_iter().next().unwrap();

    // Alice builds PreKeyBundle from Bob's public keys
    let bob_addr = ProtocolAddress::new("bob".to_string(), DeviceId::new(1).unwrap());

    let bundle = PreKeyBundle::new(
        bob_reg_id,
        DeviceId::new(1).unwrap(),
        Some((bob_ot_id, bob_ot_pair.public_key)),
        SignedPreKeyId::from(bob_signed_id),
        bob_signed.public_key().unwrap(),
        bob_signed.signature().unwrap().to_vec(),
        KyberPreKeyId::from(u32::from(bob_kyber_id)),
        kem::PublicKey::try_from(
            bob_kyber_record
                .public_key()
                .unwrap()
                .serialize()
                .to_vec()
                .as_slice(),
        )
        .unwrap(),
        bob_kyber_record.signature().unwrap().to_vec(),
        IdentityKey::from(*bob_identity.public_key()),
    )
    .unwrap();

    let alice_addr = ProtocolAddress::new("alice".to_string(), DeviceId::new(1).unwrap());

    // Alice: establish session with Bob
    process_prekey_bundle(
        &bob_addr,
        &alice_addr,
        &mut alice.session,
        &mut alice.identity,
        &bundle,
        SystemTime::now(),
        &mut rng,
    )
    .await
    .unwrap(); // <- ВОТ 379 СТРОКА

    // Alice encrypts first message
    let ciphertext = message_encrypt(
        b"hello bob",
        &bob_addr,
        &alice_addr,
        &mut alice.session,
        &mut alice.identity,
        SystemTime::now(),
        &mut rng,
    )
    .await
    .unwrap();

    // Verify it's PreKeySignalMessage (first message)
    assert!(matches!(
        ciphertext,
        CiphertextMessage::PreKeySignalMessage(_)
    ));

    // Bob decrypts (first message, creates session automatically)
    let plaintext = message_decrypt(
        &ciphertext,
        &alice_addr, // sender address
        &bob_addr,   // local address
        &mut bob.session,
        &mut bob.identity,
        &mut bob.prekey,
        &bob.signed_prekey,
        &mut bob.kyber_prekey,
        &mut rng,
    )
    .await
    .unwrap();

    assert_eq!(plaintext, b"hello bob");

    // Bob encrypts reply (now session exists)
    let reply = message_encrypt(
        b"hi alice",
        &alice_addr,
        &bob_addr,
        &mut bob.session,
        &mut bob.identity,
        SystemTime::now(),
        &mut rng,
    )
    .await
    .unwrap();

    // Verify it's SignalMessage (not PreKey)
    assert!(matches!(reply, CiphertextMessage::SignalMessage(_)));

    // Alice decrypts reply
    let reply_plaintext = message_decrypt(
        &reply,
        &bob_addr,
        &alice_addr,
        &mut alice.session,
        &mut alice.identity,
        &mut alice.prekey,
        &alice.signed_prekey,
        &mut alice.kyber_prekey,
        &mut rng,
    )
    .await
    .unwrap();

    assert_eq!(reply_plaintext, b"hi alice");

    let msg3 = message_encrypt(
        b"how are you?",
        &bob_addr,
        &alice_addr,
        &mut alice.session,
        &mut alice.identity,
        SystemTime::now(),
        &mut rng,
    )
    .await
    .unwrap();
    assert!(matches!(msg3, CiphertextMessage::SignalMessage(_)));

    let pt3 = message_decrypt(
        &msg3,
        &alice_addr,
        &bob_addr,
        &mut bob.session,
        &mut bob.identity,
        &mut bob.prekey,
        &bob.signed_prekey,
        &mut bob.kyber_prekey,
        &mut rng,
    )
    .await
    .unwrap();
    assert_eq!(pt3, b"how are you?");

    let msg4 = message_encrypt(
        b"fine, thanks!",
        &alice_addr,
        &bob_addr,
        &mut bob.session,
        &mut bob.identity,
        SystemTime::now(),
        &mut rng,
    )
    .await
    .unwrap();
    assert!(matches!(msg4, CiphertextMessage::SignalMessage(_)));

    let pt4 = message_decrypt(
        &msg4,
        &bob_addr,
        &alice_addr,
        &mut alice.session,
        &mut alice.identity,
        &mut alice.prekey,
        &alice.signed_prekey,
        &mut alice.kyber_prekey,
        &mut rng,
    )
    .await
    .unwrap();
    assert_eq!(pt4, b"fine, thanks!");

    let msg5 = message_encrypt(
        b"see you later",
        &bob_addr,
        &alice_addr,
        &mut alice.session,
        &mut alice.identity,
        SystemTime::now(),
        &mut rng,
    )
    .await
    .unwrap();
    assert!(matches!(msg5, CiphertextMessage::SignalMessage(_)));

    let pt5 = message_decrypt(
        &msg5,
        &alice_addr,
        &bob_addr,
        &mut bob.session,
        &mut bob.identity,
        &mut bob.prekey,
        &bob.signed_prekey,
        &mut bob.kyber_prekey,
        &mut rng,
    )
    .await
    .unwrap();
    assert_eq!(pt5, b"see you later");
}

#[tokio::test]
async fn debug_process_prekey() {
    let (mut alice, _tmp) = setup_temp_stores();
    alice.ensure_keys_generated().await.unwrap();

    let bob_addr = ProtocolAddress::new("bob".to_string(), DeviceId::new(1).unwrap());

    // Проверяем каждый шаг
    let session = alice.session.load_session(&bob_addr).await.unwrap();
    println!("1. load_session: {:?}", session.is_none());

    let identity = alice.identity.get_identity_key_pair().await.unwrap();
    println!("2. identity: ok");

    let reg_id = alice.identity.get_local_registration_id().await.unwrap();
    println!("3. reg_id: {}", reg_id);

    let trusted = alice
        .identity
        .is_trusted_identity(
            &bob_addr,
            &IdentityKey::from(*identity.public_key()),
            Direction::Sending,
        )
        .await
        .unwrap();
    println!("4. trusted: {}", trusted);

    // save_identity
    let bob_identity = alice.identity.get_identity_key_pair().await.unwrap(); // фиктивный
    let change = alice
        .identity
        .save_identity(&bob_addr, &IdentityKey::from(*bob_identity.public_key()))
        .await
        .unwrap();
    println!("5. save_identity: {:?}", change);
}

#[tokio::test]
async fn test_session_save_and_load() {
    let (mut stores, _tmp) = setup_temp_stores();
    stores.ensure_keys_generated().await.unwrap();

    let addr = ProtocolAddress::new("test".to_string(), DeviceId::new(1).unwrap());
    let record = SessionRecord::new_fresh();

    stores.session.store_session(&addr, &record).await.unwrap();
    let loaded = stores.session.load_session(&addr).await.unwrap();

    assert!(loaded.is_some());
}

#[tokio::test]
async fn test_identity_save_and_trust() {
    let (mut stores, _tmp) = setup_temp_stores();
    stores.ensure_keys_generated().await.unwrap();

    let addr = ProtocolAddress::new("bob".to_string(), DeviceId::new(1).unwrap());
    let identity = stores.identity.get_identity_key_pair().await.unwrap();
    let id_key = IdentityKey::from(*identity.public_key());

    let change = stores.identity.save_identity(&addr, &id_key).await.unwrap();
    assert!(matches!(change, IdentityChange::NewOrUnchanged));

    let trusted = stores
        .identity
        .is_trusted_identity(&addr, &id_key, Direction::Sending)
        .await
        .unwrap();
    assert!(trusted);
}

#[tokio::test]
async fn test_user_info_set_and_get() {
    let (stores, _tmp) = setup_temp_stores();

    stores
        .identity
        .set_user_info("alice", 1, "token123")
        .unwrap();
    let (user_id, device_id, token) = stores.identity.get_user_info().unwrap().unwrap();

    assert_eq!(user_id, "alice");
    assert_eq!(device_id, 1);
    assert_eq!(token, Some("token123".to_string()));
}

#[test]
fn test_local_address_requires_registered_user_and_valid_device() {
    use rust_processor::encryption::EncryptionError;

    let (stores, tmp) = setup_temp_stores();
    assert!(matches!(
        stores.identity.local_address(),
        Err(EncryptionError::MissingUserInfo)
    ));
    for device_id in [0, 128, 256, u32::MAX] {
        stores
            .identity
            .set_user_info("alice", device_id, "test-only")
            .unwrap();
        assert!(matches!(
            stores.identity.local_address(),
            Err(EncryptionError::InvalidDeviceId(_))
        ));
    }
    stores.identity.set_user_info(" ", 1, "test-only").unwrap();
    assert!(matches!(
        stores.identity.local_address(),
        Err(EncryptionError::MissingUserInfo)
    ));
    stores
        .identity
        .set_user_info("alice", 2, "test-only")
        .unwrap();
    let address = stores.identity.local_address().unwrap();
    assert_eq!(address.name(), "alice");
    assert_eq!(u32::from(address.device_id()), 2);

    // Corrupt SQLite values must not wrap around to a valid device ID of 1.
    let conn = rusqlite::Connection::open(tmp.path()).unwrap();
    for device_id in [-4_294_967_295_i64, -1, 4_294_967_297] {
        conn.execute("UPDATE user_info SET device_id = ?1", [device_id])
            .unwrap();
        assert!(matches!(
            stores.identity.local_address(),
            Err(EncryptionError::Database(_))
        ));
    }
}
