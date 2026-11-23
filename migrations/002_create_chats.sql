-- +goose Up
CREATE TABLE chats (
    id SERIAL PRIMARY KEY,
    type VARCHAR(10) NOT NULL, -- 'group' или 'private'
    name VARCHAR(100) DEFAULT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE chat_members (
    chat_id INTEGER REFERENCES chats(id) ON DELETE CASCADE,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    PRIMARY KEY (chat_id, user_id)
);

CREATE INDEX idx_chat_members_user ON chat_members(user_id);
