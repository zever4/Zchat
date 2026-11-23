-- +goose Up
-- libsignal/protobuf key IDs are uint32. INTEGER ends at 2^31-1;
-- BIGINT plus CHECK preserves the wire range and rejects negative IDs.
ALTER TABLE device_keys
    ALTER COLUMN registration_id TYPE BIGINT,
    ALTER COLUMN signed_pre_key_id TYPE BIGINT,
    ADD CONSTRAINT device_keys_registration_u32 CHECK (registration_id BETWEEN 0 AND 4294967295),
    ADD CONSTRAINT device_keys_signed_prekey_u32 CHECK (signed_pre_key_id BETWEEN 0 AND 4294967295);
ALTER TABLE one_time_prekeys
    ALTER COLUMN key_id TYPE BIGINT,
    ADD CONSTRAINT one_time_prekeys_id_u32 CHECK (key_id BETWEEN 0 AND 4294967295);
ALTER TABLE kyber_prekeys
    ALTER COLUMN key_id TYPE BIGINT,
    ADD CONSTRAINT kyber_prekeys_id_u32 CHECK (key_id BETWEEN 0 AND 4294967295);
ALTER TABLE encrypted_messages
    ALTER COLUMN pre_key_id TYPE BIGINT,
    ALTER COLUMN signed_pre_key_id TYPE BIGINT,
    ALTER COLUMN kyber_pre_key_id TYPE BIGINT,
    ADD CONSTRAINT encrypted_messages_prekey_u32 CHECK (pre_key_id BETWEEN 0 AND 4294967295),
    ADD CONSTRAINT encrypted_messages_signed_prekey_u32 CHECK (signed_pre_key_id BETWEEN 0 AND 4294967295),
    ADD CONSTRAINT encrypted_messages_kyber_prekey_u32 CHECK (kyber_pre_key_id BETWEEN 0 AND 4294967295);

-- Correct comments without changing an already applied migration.
COMMENT ON COLUMN device_keys.signed_pre_key_signature IS 'libsignal curve/XEdDSA signature over serialized signed public key; not Ed25519';
COMMENT ON COLUMN kyber_prekeys.public_key IS 'Serialized libsignal KEM public key with type tag; current client generates Kyber1024 (1569 bytes)';
COMMENT ON COLUMN kyber_prekeys.signature IS 'libsignal identity curve/XEdDSA signature over serialized KEM public key; not Ed25519';

-- +goose Down
-- Narrowing fails rather than truncating IDs above 2^31-1. Goose executes this
-- in a transaction, preserving schema/data on failure; no keys are deleted.
ALTER TABLE encrypted_messages
    DROP CONSTRAINT encrypted_messages_prekey_u32,
    DROP CONSTRAINT encrypted_messages_signed_prekey_u32,
    DROP CONSTRAINT encrypted_messages_kyber_prekey_u32,
    ALTER COLUMN pre_key_id TYPE INTEGER,
    ALTER COLUMN signed_pre_key_id TYPE INTEGER,
    ALTER COLUMN kyber_pre_key_id TYPE INTEGER;
ALTER TABLE kyber_prekeys
    DROP CONSTRAINT kyber_prekeys_id_u32,
    ALTER COLUMN key_id TYPE INTEGER;
ALTER TABLE one_time_prekeys
    DROP CONSTRAINT one_time_prekeys_id_u32,
    ALTER COLUMN key_id TYPE INTEGER;
ALTER TABLE device_keys
    DROP CONSTRAINT device_keys_registration_u32,
    DROP CONSTRAINT device_keys_signed_prekey_u32,
    ALTER COLUMN registration_id TYPE INTEGER,
    ALTER COLUMN signed_pre_key_id TYPE INTEGER;
COMMENT ON COLUMN device_keys.signed_pre_key_signature IS NULL;
COMMENT ON COLUMN kyber_prekeys.public_key IS NULL;
COMMENT ON COLUMN kyber_prekeys.signature IS NULL;
