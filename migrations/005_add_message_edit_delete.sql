-- +goose Up
ALTER TABLE messages ADD COLUMN updated_at TIMESTAMPTZ;
ALTER TABLE messages ADD COLUMN is_deleted BOOLEAN DEFAULT FALSE;

-- +goose Down
ALTER TABLE messages DROP COLUMN updated_at;
ALTER TABLE messages DROP COLUMN is_deleted;
