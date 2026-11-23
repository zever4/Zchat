-- +goose Up
ALTER TABLE chats ADD COLUMN hide_members_list BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE chats ADD COLUMN anonymous_mode BOOLEAN NOT NULL DEFAULT FALSE;

-- +goose Down
ALTER TABLE chats DROP COLUMN anonymous_mode;
ALTER TABLE chats DROP COLUMN hide_members_list;
