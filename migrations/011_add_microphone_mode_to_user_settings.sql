-- +goose Up

ALTER TABLE user_settings
    ADD COLUMN IF NOT EXISTS microphone_mode VARCHAR(16) NOT NULL DEFAULT 'toggle';

UPDATE user_settings
SET microphone_mode = 'toggle'
WHERE microphone_mode IS NULL OR microphone_mode = '';

-- +goose Down

ALTER TABLE user_settings
    DROP COLUMN IF EXISTS microphone_mode;
