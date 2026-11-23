-- +goose Up
-- Generation-scoped v1 public material. Do not import legacy device_keys: it
-- has no authenticated generation ownership or durable publication receipts.
CREATE TABLE e2ee_signed_prekeys (
    generation_id UUID NOT NULL REFERENCES e2ee_devices(generation_id) ON DELETE CASCADE,
    key_id BIGINT NOT NULL CHECK (key_id BETWEEN 0 AND 4294967295),
    public_key BYTEA NOT NULL CHECK (octet_length(public_key)=33 AND get_byte(public_key,0)=5),
    signature BYTEA NOT NULL CHECK (octet_length(signature)=64),
    created_at_unix_ms BIGINT NOT NULL CHECK (created_at_unix_ms>=0),
    PRIMARY KEY (generation_id,key_id)
);
CREATE TABLE e2ee_ec_prekeys (
    generation_id UUID NOT NULL REFERENCES e2ee_devices(generation_id) ON DELETE CASCADE,
    key_id BIGINT NOT NULL CHECK (key_id BETWEEN 0 AND 4294967295),
    public_key BYTEA NOT NULL CHECK (octet_length(public_key)=33 AND get_byte(public_key,0)=5),
    used BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (generation_id,key_id)
);
CREATE INDEX e2ee_unused_ec ON e2ee_ec_prekeys(generation_id,key_id) WHERE NOT used;
CREATE TABLE e2ee_kyber_prekeys (
    generation_id UUID NOT NULL REFERENCES e2ee_devices(generation_id) ON DELETE CASCADE,
    key_id BIGINT NOT NULL CHECK (key_id BETWEEN 0 AND 4294967295),
    public_key BYTEA NOT NULL CHECK (octet_length(public_key)=1569 AND get_byte(public_key,0)=8),
    signature BYTEA NOT NULL CHECK (octet_length(signature)=64),
    is_last_resort BOOLEAN NOT NULL,
    used BOOLEAN NOT NULL DEFAULT FALSE CHECK (NOT (is_last_resort AND used)),
    PRIMARY KEY (generation_id,key_id),
    UNIQUE (generation_id,key_id,is_last_resort)
);
CREATE INDEX e2ee_unused_kyber ON e2ee_kyber_prekeys(generation_id,key_id) WHERE NOT used AND NOT is_last_resort;

ALTER TABLE e2ee_devices
    ADD COLUMN public_material_revision BIGINT NOT NULL DEFAULT 0 CHECK (public_material_revision>=0),
    ADD COLUMN active_signed_prekey_id BIGINT,
    ADD COLUMN active_last_resort_id BIGINT,
    ADD COLUMN last_resort_marker BOOLEAN NOT NULL DEFAULT TRUE CHECK (last_resort_marker),
    ADD FOREIGN KEY (generation_id,active_signed_prekey_id) REFERENCES e2ee_signed_prekeys(generation_id,key_id),
    ADD FOREIGN KEY (generation_id,active_last_resort_id,last_resort_marker) REFERENCES e2ee_kyber_prekeys(generation_id,key_id,is_last_resort),
    ADD CHECK (NOT public_material_ready OR
        (retired_at IS NULL AND active_signed_prekey_id IS NOT NULL AND active_last_resort_id IS NOT NULL));

CREATE TABLE e2ee_publication_batches (
    generation_id UUID NOT NULL REFERENCES e2ee_devices(generation_id) ON DELETE CASCADE,
    batch_id UUID NOT NULL,
    digest BYTEA NOT NULL CHECK (octet_length(digest)=32),
    publication_revision BIGINT NOT NULL CHECK (publication_revision>0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (generation_id,batch_id),
    UNIQUE (generation_id,publication_revision),
    CHECK (substring(batch_id::text,15,1)='4' AND substring(batch_id::text,20,1) IN ('8','9','a','b'))
);

-- +goose Down
-- Protect even a registry-only installation: rolling back removes the revision
-- advertised by GetOwnDevice. No implicit key/receipt/state deletion.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM e2ee_devices) THEN
        RAISE EXCEPTION 'cannot roll back public-key schema with registered E2EE devices';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE e2ee_publication_batches;
ALTER TABLE e2ee_devices DROP COLUMN active_signed_prekey_id,
    DROP COLUMN active_last_resort_id, DROP COLUMN last_resort_marker,
    DROP COLUMN public_material_revision;
DROP TABLE e2ee_kyber_prekeys;
DROP TABLE e2ee_ec_prekeys;
DROP TABLE e2ee_signed_prekeys;
