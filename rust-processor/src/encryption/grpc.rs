//! Retired legacy wire implementation, retained for reference only.
use std::time::{SystemTime, UNIX_EPOCH};

use super::Result;
use crate::encryption::EncryptionError;
use crate::encryption::store::Stores;
use crate::proto::encryption::messenger_client::MessengerClient;
use libsignal_protocol::{
    CiphertextMessage, DeviceId, GenericSignedPreKey, IdentityKey, IdentityKeyStore, KyberPreKeyId,
    PreKeyBundle, PreKeyId, PreKeySignalMessage, ProtocolAddress, PublicKey, SessionStore,
    SignalMessage, SignalProtocolError, SignedPreKeyId, kem, message_decrypt, message_encrypt,
    process_prekey_bundle,
};
use tonic::transport::Channel;

use crate::proto::encryption::{
    EncryptedMessage, KyberPreKey, MessageType, OneTimePreKey, PreKeyBundle as ProtoPreKeyBundle,
    PreKeyBundleUpload, SignedPreKey, UserId,
};

pub struct GrpcMessenger {
    client: MessengerClient<Channel>,
    store: Stores,
}

impl GrpcMessenger {
    pub async fn connect<A: Into<String>>(_addr: A, _store: Stores) -> Result<Self> {
        Err(EncryptionError::LegacyTransportRetired)
    }

    // NOTE: when multi device is added, `send_text` have to be called in cycle for each
    // recipient's device id
    pub async fn send_text(
        &mut self,
        recipient_id: &str,
        recipient_device_id: u8,
        text: &str,
    ) -> Result<()> {
        let local_address = self.store.identity.local_address()?;
        let addr = ProtocolAddress::new(
            recipient_id.to_string(),
            DeviceId::new(recipient_device_id)
                .map_err(|e| EncryptionError::InvalidDeviceId(e.to_string()))?,
        );

        let mut csprng = rand::rng();

        let session_exists = self.store.session.load_session(&addr).await?.is_some();
        if !session_exists {
            self.establish_session(recipient_id, recipient_device_id)
                .await?;
        }

        let ciphertext = message_encrypt(
            text.as_bytes(),
            &addr,
            &local_address,
            &mut self.store.session,
            &mut self.store.identity,
            SystemTime::now(),
            &mut csprng,
        )
        .await?;

        let msg_type = match ciphertext {
            CiphertextMessage::PreKeySignalMessage(_) => MessageType::PrekeySignalMessage,
            CiphertextMessage::SignalMessage(_) => MessageType::SignalMessage,
            CiphertextMessage::SenderKeyMessage(_) => MessageType::SenderKeyMessage,
            _ => MessageType::Unknown,
        };

        self.send_message(
            recipient_id,
            recipient_device_id,
            ciphertext.serialize(),
            msg_type,
        )
        .await
    }

    pub async fn upload_prekeys(&mut self) -> Result<()> {
        let local_address = self.store.identity.local_address()?;

        let identity = self.store.identity.get_identity_key_pair().await?;
        let reg_id = self.store.identity.get_local_registration_id().await?;

        let signed_pre_key = self.store.signed_prekey.get_active_signed_pre_key().await?;
        let signed_pre_key_id: u32 = signed_pre_key.id()?.into();

        let one_time_pre_keys = self.store.prekey.get_unused_pre_keys(100).await?;

        // Get 100 unused ONE-TIME kyber prekeys. Any invalid prekeys is omitted.
        // If there is 0 valid one-time prekeys, then last resort is the only available one
        let mut kyber_pre_keys: Vec<KyberPreKey> = self
            .store
            .kyber_prekey
            .get_unused_kyber_pre_keys(100)
            .await?
            .into_iter()
            .filter_map(|(id, record, is_last_resort)| {
                let pub_key = record.public_key().ok()?.serialize().to_vec();
                let sig = record.signature().ok()?.to_vec();
                Some(KyberPreKey {
                    key_id: id.into(),
                    public_key: pub_key,
                    signature: sig,
                    is_last_resort,
                })
            })
            .collect();

        // Last resort kyber prekey MUST be valid
        let (last_resort_id, last_resort_record) = self
            .store
            .kyber_prekey
            .get_last_resort_kyber_pre_key()
            .await?
            .ok_or(EncryptionError::MissingLastResortKyberPreKey)?;

        kyber_pre_keys.push(KyberPreKey {
            key_id: last_resort_id.into(),
            public_key: last_resort_record.public_key()?.serialize().to_vec(),
            signature: last_resort_record.signature()?.to_vec(),
            is_last_resort: true,
        });

        let req = tonic::Request::new(PreKeyBundleUpload {
            user_id: local_address.name().to_string(),
            device_id: local_address.device_id().into(),
            registration_id: reg_id,
            identity_key: identity.public_key().serialize().to_vec(),
            signed_pre_key: Some(SignedPreKey {
                key_id: signed_pre_key_id,
                public_key: signed_pre_key.public_key()?.serialize().to_vec(),
                signature: signed_pre_key.signature()?.to_vec(),
            }),
            one_time_pre_keys: one_time_pre_keys
                .into_iter()
                .map(|(id, key_pair)| OneTimePreKey {
                    key_id: id.into(),
                    public_key: key_pair.public_key.serialize().to_vec(),
                })
                .collect(),
            kyber_pre_keys,
            signed_pre_key_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
                as u64,
        });

        self.client.upload_pre_keys(req).await?;
        Ok(())
    }

    pub async fn fetch_prekeys(
        &mut self,
        user_id: &str,
        device_id: u8,
    ) -> Result<ProtoPreKeyBundle> {
        let req = tonic::Request::new(UserId {
            user_id: user_id.to_string(),
            device_id: Some(device_id as u32),
        });

        let response = self.client.fetch_pre_keys(req).await?;
        let bundles = response.into_inner();

        // NOTE: server returns single bundle because we request specific device_id.
        // If device_id is `None` in `req`, need to iterate through all bundles and create sessions
        // for each device
        let bundle = bundles.bundles.into_iter().next().ok_or_else(|| {
            EncryptionError::MissingBundleField("no prekey bundles returned".to_string())
        })?;

        if bundle.user_id != user_id || bundle.device_id != u32::from(device_id) {
            return Err(
                SignalProtocolError::InvalidArgument("bundle owner mismatch".into()).into(),
            );
        }
        // No identity mutation here. Libsignal checks trust/signatures on bootstrap.

        Ok(bundle)
    }

    pub async fn send_message(
        &mut self,
        recipient_id: &str,
        recipient_device_id: u8,
        ciphertext: &[u8],
        message_type: MessageType,
    ) -> Result<()> {
        let local_address = self.store.identity.local_address()?;

        let req = tonic::Request::new(EncryptedMessage {
            message_id: uuid::Uuid::new_v4().to_string(),
            sender_id: local_address.name().to_string(),
            sender_device_id: local_address.device_id().into(),
            recipient_id: recipient_id.to_string(),
            recipient_device_id: recipient_device_id as u32,
            ciphertext: ciphertext.to_vec(),
            message_type: message_type as i32,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64,

            // ID ключей не нужны для шифрования
            // Могут быть полезны для отладки - добавить при необходимости.
            pre_key_id: None,
            signed_pre_key_id: None,
            kyber_pre_key_id: None,
        });

        self.client.send_message(req).await?;
        Ok(())
    }

    pub async fn receive_loop(&mut self) -> Result<()> {
        let local_address = self.store.identity.local_address()?;

        let req = tonic::Request::new(UserId {
            user_id: local_address.name().to_string(),
            device_id: Some(local_address.device_id().into()),
        });
        let mut stream = self.client.stream_messages(req).await?.into_inner();
        let mut rng = rand::rng();

        loop {
            match stream.message().await {
                Ok(Some(msg)) => {
                    // Конвертация device_id u32 → u8
                    let device_id_u8: u8 = msg.sender_device_id.try_into().map_err(|_| {
                        EncryptionError::InvalidDeviceId(format!(
                            "device_id {} too large",
                            msg.sender_device_id
                        ))
                    })?;

                    let sender_address = ProtocolAddress::new(
                        msg.sender_id.clone(),
                        DeviceId::new(device_id_u8)
                            .map_err(|e| EncryptionError::InvalidDeviceId(e.to_string()))?,
                    );

                    let ciphertext_msg = match msg.message_type {
                        1 => {
                            let prekey_msg =
                                PreKeySignalMessage::try_from(msg.ciphertext.as_slice())
                                    .map_err(EncryptionError::Signal)?;
                            CiphertextMessage::PreKeySignalMessage(prekey_msg)
                        }
                        2 => {
                            let signal_msg = SignalMessage::try_from(msg.ciphertext.as_slice())
                                .map_err(EncryptionError::Signal)?;
                            CiphertextMessage::SignalMessage(signal_msg)
                        }
                        3 => {
                            // SENDER_KEY_MESSAGE — пока не реализовано
                            println!("Sender key message not yet supported");
                            continue;
                        }
                        _ => {
                            println!("Unknown message type: {}", msg.message_type);
                            continue;
                        }
                    };

                    let _plaintext = zeroize::Zeroizing::new(
                        message_decrypt(
                            &ciphertext_msg,
                            &sender_address,
                            &local_address,
                            &mut self.store.session,
                            &mut self.store.identity,
                            &mut self.store.prekey,
                            &self.store.signed_prekey,
                            &mut self.store.kyber_prekey,
                            &mut rng,
                        )
                        .await?,
                    );
                }
                Ok(None) => {
                    println!("Stream closed.");
                    break;
                }
                Err(e) => {
                    // TODO: Reconnecting
                    println!("Stream error: {e}, reconnecting...");
                    break;
                }
            }
        }

        Ok(())
    }

    pub async fn establish_session(
        &mut self,
        recipient_id: &str,
        recipient_device_id: u8,
    ) -> Result<()> {
        let local_address = self.store.identity.local_address()?;
        // Fetch is read-only; libsignal checks trust and signatures before saving.
        let bundle: ProtoPreKeyBundle = self
            .fetch_prekeys(recipient_id, recipient_device_id)
            .await?;

        let addr = ProtocolAddress::new(
            recipient_id.to_string(),
            DeviceId::new(recipient_device_id)
                .map_err(|e| EncryptionError::InvalidDeviceId(e.to_string()))?,
        );

        let signed_pre_key = bundle
            .signed_pre_key
            .ok_or_else(|| EncryptionError::MissingBundleField("signed_pre_key".to_string()))?;

        let one_time_pre_key = bundle
            .one_time_pre_key
            .ok_or_else(|| EncryptionError::MissingBundleField("one_time_pre_key".to_string()))?;

        let kyber_pre_key = bundle
            .kyber_pre_key
            .ok_or_else(|| EncryptionError::MissingBundleField("kyber_pre_key".to_string()))?;

        let identity_key = IdentityKey::try_from(bundle.identity_key.as_slice()).map_err(|_| {
            EncryptionError::Signal(SignalProtocolError::InvalidArgument(
                "invalid identity key".into(),
            ))
        })?;

        let signal_bundle = PreKeyBundle::new(
            bundle.registration_id,
            DeviceId::new(recipient_device_id)
                .map_err(|e| EncryptionError::InvalidDeviceId(e.to_string()))?,
            Some((
                PreKeyId::from(one_time_pre_key.key_id),
                PublicKey::try_from(one_time_pre_key.public_key.as_slice()).map_err(|_| {
                    EncryptionError::Signal(SignalProtocolError::InvalidKeyAgreement)
                })?,
            )),
            SignedPreKeyId::from(signed_pre_key.key_id),
            PublicKey::try_from(signed_pre_key.public_key.as_slice())
                .map_err(|e| EncryptionError::SignalCurve(e.to_string()))?,
            signed_pre_key.signature,
            KyberPreKeyId::from(kyber_pre_key.key_id),
            kem::PublicKey::try_from(kyber_pre_key.public_key.as_slice())?,
            kyber_pre_key.signature,
            identity_key,
        )
        .map_err(EncryptionError::Signal)?;

        let mut csprng = rand::rng();

        process_prekey_bundle(
            &addr,
            &local_address,
            &mut self.store.session,
            &mut self.store.identity,
            &signal_bundle,
            SystemTime::now(),
            &mut csprng,
        )
        .await?;

        Ok(())
    }
}
