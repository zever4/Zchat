-- +goose Up
CREATE TABLE group_invite_links (
    id SERIAL PRIMARY KEY,
    chat_id INTEGER NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    code VARCHAR(64) NOT NULL UNIQUE,
    created_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deactivated_at TIMESTAMPTZ NULL
);

CREATE INDEX idx_group_invite_links_chat ON group_invite_links(chat_id);
CREATE INDEX idx_group_invite_links_chat_active ON group_invite_links(chat_id, is_active);

INSERT INTO group_invite_links (chat_id, code, created_by, is_active)
SELECT c.id, c.invite_code, c.owner_id, TRUE
FROM chats c
WHERE c.type = 'group'
  AND COALESCE(c.invite_code, '') <> ''
ON CONFLICT (code) DO NOTHING;

CREATE TABLE group_audit_logs (
    id SERIAL PRIMARY KEY,
    chat_id INTEGER NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    actor_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
    action VARCHAR(64) NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_group_audit_logs_chat_created ON group_audit_logs(chat_id, created_at DESC);

-- +goose Down
DROP TABLE IF EXISTS group_audit_logs;
DROP TABLE IF EXISTS group_invite_links;
