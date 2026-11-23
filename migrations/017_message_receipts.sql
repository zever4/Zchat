-- +goose Up
-- NULL preserves legacy clients. Receipts live as long as the message row,
-- including soft deletion; the original digest survives message edits.
ALTER TABLE messages ADD COLUMN client_message_id UUID;
ALTER TABLE messages ADD COLUMN client_content_hash BYTEA;
ALTER TABLE messages ADD CONSTRAINT messages_client_receipt CHECK (
    (client_message_id IS NULL AND client_content_hash IS NULL) OR
    (client_message_id IS NOT NULL AND octet_length(client_content_hash) = 32 AND client_content_hash IS NOT NULL)
);
ALTER TABLE messages ADD CONSTRAINT messages_sender_client_id UNIQUE (sender_id, client_message_id);

-- +goose Down
ALTER TABLE messages DROP CONSTRAINT messages_sender_client_id;
ALTER TABLE messages DROP CONSTRAINT messages_client_receipt;
ALTER TABLE messages DROP COLUMN client_content_hash;
ALTER TABLE messages DROP COLUMN client_message_id;
