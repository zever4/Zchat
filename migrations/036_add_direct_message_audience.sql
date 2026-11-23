-- +goose Up
ALTER TABLE user_settings ADD COLUMN allow_direct_messages_from_non_friends BOOLEAN NOT NULL DEFAULT FALSE;

-- +goose Down
ALTER TABLE user_settings DROP COLUMN allow_direct_messages_from_non_friends;
