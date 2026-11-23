-- +goose Up
CREATE TABLE attachments (
    path TEXT PRIMARY KEY,
    chat_id BIGINT NOT NULL REFERENCES chats(id) ON DELETE CASCADE,
    owner_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    mime_type TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('file', 'media')),
    size BIGINT NOT NULL CHECK (size > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX attachments_chat_id_idx ON attachments(chat_id);

-- +goose Down
DROP TABLE attachments;
