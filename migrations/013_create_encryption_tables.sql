-- +goose Up

-- ==========================================
-- 1. Device identity & signed prekey
-- ==========================================
CREATE TABLE IF NOT EXISTS device_keys (
    user_id TEXT NOT NULL REFERENCES users(user_uuid) ON DELETE CASCADE,
    device_id INTEGER NOT NULL,
    registration_id INTEGER NOT NULL,
    identity_key BYTEA NOT NULL,              -- 33 bytes (0x05 || X25519 pubkey)
    signed_pre_key_id INTEGER NOT NULL,
    signed_pre_key_public BYTEA NOT NULL,     -- 33 bytes
    signed_pre_key_signature BYTEA NOT NULL,  -- 64 bytes Ed25519 signature
    signed_pre_key_timestamp BIGINT NOT NULL, -- unix millis
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, device_id)
);

-- ==========================================
-- 2. One-time X25519 prekeys (X3DH)
-- ==========================================
CREATE TABLE IF NOT EXISTS one_time_prekeys (
    user_id TEXT NOT NULL,
    device_id INTEGER NOT NULL,
    key_id INTEGER NOT NULL,
    public_key BYTEA NOT NULL,                -- 33 bytes
    used BOOLEAN DEFAULT FALSE,
    used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, device_id, key_id),
    CONSTRAINT fk_otp_device
        FOREIGN KEY (user_id, device_id)
        REFERENCES device_keys(user_id, device_id)
        ON DELETE CASCADE
);

-- ==========================================
-- 3. Kyber prekeys (PQXDH, one-time + last-resort)
-- ==========================================
CREATE TABLE IF NOT EXISTS kyber_prekeys (
    user_id TEXT NOT NULL,
    device_id INTEGER NOT NULL,
    key_id INTEGER NOT NULL,
    public_key BYTEA NOT NULL,                -- Kyber-768 public key bytes
    signature BYTEA NOT NULL,                 -- Ed25519 signature over public_key
    is_last_resort BOOLEAN NOT NULL DEFAULT FALSE,
    used BOOLEAN DEFAULT FALSE,
    used_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, device_id, key_id),
    CONSTRAINT fk_kyber_device
        FOREIGN KEY (user_id, device_id)
        REFERENCES device_keys(user_id, device_id)
        ON DELETE CASCADE
);

-- ==========================================
-- 4. Encrypted messages (temporary blob storage)
-- ==========================================
CREATE TABLE IF NOT EXISTS encrypted_messages (
    message_id TEXT PRIMARY KEY,            -- UUID от клиента (Rust)
    sender_id TEXT NOT NULL,
    sender_device_id INTEGER NOT NULL,
    recipient_id TEXT NOT NULL,
    recipient_device_id INTEGER NOT NULL,
    ciphertext BYTEA NOT NULL,
    message_type INTEGER NOT NULL,            -- 1=PreKeySignal, 2=Signal, 3=SenderKey
    pre_key_id INTEGER,
    signed_pre_key_id INTEGER,
    kyber_pre_key_id INTEGER,
    timestamp BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW() + INTERVAL '30 days',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- ==========================================
-- 5. Indexes
-- ==========================================
CREATE INDEX IF NOT EXISTS idx_otp_available 
    ON one_time_prekeys(user_id, device_id, key_id)
    WHERE used = FALSE;

CREATE INDEX IF NOT EXISTS idx_kyber_available 
    ON kyber_prekeys(user_id, device_id, key_id, is_last_resort)
    WHERE used = FALSE OR is_last_resort = TRUE;

CREATE INDEX IF NOT EXISTS idx_messages_inbox 
    ON encrypted_messages(recipient_id, recipient_device_id, created_at);
-- Sender key групповые чаты в будущих версиях. (+ таблица sender keys)
-- 
-- CREATE TABLE groups (
--     id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
--     group_id TEXT UNIQUE NOT NULL,
--     created_at TIMESTAMPTZ DEFAULT NOW()
-- );
--
-- CREATE TABLE group_members (
--     group_id TEXT NOT NULL REFERENCES groups(group_id),
--     user_id TEXT NOT NULL REFERENCES users(user_id),
--     joined_at TIMESTAMPTZ DEFAULT NOW(),
--     PRIMARY KEY (group_id, user_id)
-- );
