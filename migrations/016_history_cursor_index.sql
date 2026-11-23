-- +goose Up
CREATE INDEX idx_messages_chat_id ON messages(chat_id, id DESC) WHERE is_deleted IS NOT TRUE;

-- +goose Down
DROP INDEX idx_messages_chat_id;
