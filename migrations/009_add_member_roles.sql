-- +goose Up
ALTER TABLE chat_members ADD COLUMN role VARCHAR(32) NOT NULL DEFAULT 'member';
ALTER TABLE chats ADD COLUMN admins_only_write BOOLEAN NOT NULL DEFAULT FALSE;

-- Set owner role for existing group owners
UPDATE chat_members cm
SET role = 'owner'
FROM chats c
WHERE cm.chat_id = c.id AND cm.user_id = c.owner_id AND c.type = 'group';

-- +goose Down
ALTER TABLE chat_members DROP COLUMN role;
ALTER TABLE chats DROP COLUMN admins_only_write;
