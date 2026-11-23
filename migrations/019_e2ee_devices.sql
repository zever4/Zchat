-- +goose Up
-- V1 registration is separate from the legacy device_keys prototype. Existing
-- public keys cannot establish possession of a new device credential.
CREATE TABLE e2ee_devices (
    generation_id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    device_id SMALLINT NOT NULL DEFAULT 1 CHECK (device_id = 1),
    registration_id BIGINT NOT NULL CHECK (registration_id BETWEEN 0 AND 4294967295),
    identity_key BYTEA NOT NULL CHECK (octet_length(identity_key) = 33 AND get_byte(identity_key,0) = 5),
    credential_hash BYTEA NOT NULL CHECK (octet_length(credential_hash) = 32),
    public_material_ready BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    retired_at TIMESTAMPTZ,
    UNIQUE (user_id, generation_id),
    CHECK (substring(generation_id::text,15,1) = '4' AND substring(generation_id::text,20,1) IN ('8','9','a','b'))
);
CREATE UNIQUE INDEX e2ee_one_active_device ON e2ee_devices(user_id) WHERE retired_at IS NULL;

-- Keep the request/result association after retirement. No password or raw
-- device credential is persisted here; material comparison uses the result row.
CREATE TABLE e2ee_device_requests (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    request_id UUID NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('register','replace')),
    expected_generation_id UUID,
    result_generation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (user_id, request_id),
    FOREIGN KEY (user_id, result_generation_id) REFERENCES e2ee_devices(user_id,generation_id),
    FOREIGN KEY (user_id, expected_generation_id) REFERENCES e2ee_devices(user_id,generation_id),
    CHECK ((operation = 'register' AND expected_generation_id IS NULL) OR
           (operation = 'replace' AND expected_generation_id IS NOT NULL)),
    CHECK (substring(request_id::text,15,1) = '4' AND substring(request_id::text,20,1) IN ('8','9','a','b'))
);

-- +goose Down
-- A rollback must not silently erase device identities or replay records.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM e2ee_devices) OR EXISTS (SELECT 1 FROM e2ee_device_requests) THEN
        RAISE EXCEPTION 'cannot roll back nonempty E2EE device registry';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE e2ee_device_requests;
DROP TABLE e2ee_devices;
