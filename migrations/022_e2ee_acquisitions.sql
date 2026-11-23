-- +goose Up
-- Public-only reservations. Exact retry keeps the original keys and deadline;
-- expiry never returns consumed one-time keys to a pool. No GC is introduced.
CREATE TABLE e2ee_acquisitions (
    owner_user_id BIGINT NOT NULL,
    owner_generation_id UUID NOT NULL,
    acquisition_id UUID NOT NULL,
    conversation_id UUID NOT NULL,
    peer_user_id BIGINT NOT NULL,
    peer_generation_id UUID NOT NULL,
    low_user_id BIGINT NOT NULL,
    high_user_id BIGINT NOT NULL,
    low_generation_id UUID NOT NULL,
    high_generation_id UUID NOT NULL,
    signed_prekey_id BIGINT NOT NULL,
    ec_prekey_id BIGINT,
    kyber_prekey_id BIGINT NOT NULL,
    kyber_is_last_resort BOOLEAN NOT NULL,
    public_snapshot BYTEA NOT NULL CHECK (octet_length(public_snapshot) BETWEEN 1 AND 8192),
    created_at TIMESTAMPTZ NOT NULL,
    not_after TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (owner_generation_id,acquisition_id),
    FOREIGN KEY (conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id)
        REFERENCES e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id),
    FOREIGN KEY (peer_generation_id,signed_prekey_id) REFERENCES e2ee_signed_prekeys(generation_id,key_id),
    FOREIGN KEY (peer_generation_id,ec_prekey_id) REFERENCES e2ee_ec_prekeys(generation_id,key_id),
    FOREIGN KEY (peer_generation_id,kyber_prekey_id,kyber_is_last_resort) REFERENCES e2ee_kyber_prekeys(generation_id,key_id,is_last_resort),
    CHECK ((owner_user_id=low_user_id AND peer_user_id=high_user_id AND owner_generation_id=low_generation_id AND peer_generation_id=high_generation_id) OR
           (owner_user_id=high_user_id AND peer_user_id=low_user_id AND owner_generation_id=high_generation_id AND peer_generation_id=low_generation_id)),
    CHECK (not_after>created_at AND not_after<=created_at+INTERVAL '24 hours'),
    CHECK (substring(acquisition_id::text,15,1)='4' AND substring(acquisition_id::text,20,1) IN ('8','9','a','b'))
);
CREATE UNIQUE INDEX e2ee_acquisition_ec_once ON e2ee_acquisitions(peer_generation_id,ec_prekey_id) WHERE ec_prekey_id IS NOT NULL;
CREATE UNIQUE INDEX e2ee_acquisition_pq_once ON e2ee_acquisitions(peer_generation_id,kyber_prekey_id) WHERE NOT kyber_is_last_resort;
CREATE INDEX e2ee_acquisition_live ON e2ee_acquisitions(owner_generation_id,conversation_id,not_after);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM e2ee_acquisitions) THEN
        RAISE EXCEPTION 'cannot roll back nonempty E2EE acquisitions';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE e2ee_acquisitions;
