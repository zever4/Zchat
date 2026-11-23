-- +goose Up
ALTER TABLE chats
    ADD COLUMN owner_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    ADD COLUMN is_public BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN invite_code VARCHAR(64),
    ADD COLUMN avatar_url TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS idx_chats_invite_code_unique
    ON chats(invite_code)
    WHERE invite_code IS NOT NULL;

CREATE TABLE group_join_requests (
    id SERIAL PRIMARY KEY,
    chat_id INTEGER NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    status VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'approved', 'rejected')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chat_id, user_id)
);

CREATE INDEX idx_group_join_requests_chat_status
    ON group_join_requests(chat_id, status);

-- +goose Down
DROP TABLE IF EXISTS group_join_requests;

DROP INDEX IF EXISTS idx_chats_invite_code_unique;

ALTER TABLE chats
    DROP COLUMN IF EXISTS owner_id,
    DROP COLUMN IF EXISTS is_public,
    DROP COLUMN IF EXISTS invite_code,
    DROP COLUMN IF EXISTS avatar_url;
