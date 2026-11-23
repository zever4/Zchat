-- +goose Up
ALTER TABLE messages ADD COLUMN anonymous_sender BOOLEAN NOT NULL DEFAULT FALSE;

-- Older messages have no send-time marker. Preserve the mask on history that
-- is already anonymous when upgrading, without guessing in other groups.
UPDATE messages AS m
SET anonymous_sender = TRUE
FROM chats AS c
WHERE c.id = m.chat_id AND c.type = 'group' AND c.anonymous_mode IS TRUE;

-- +goose Down
ALTER TABLE messages DROP COLUMN anonymous_sender;
