-- +goose Up

ALTER TABLE user_settings
    ADD COLUMN IF NOT EXISTS microphone_ptt_key VARCHAR(32) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS screen_share_resolution VARCHAR(16) NOT NULL DEFAULT '1080p',
    ADD COLUMN IF NOT EXISTS screen_share_fps INTEGER NOT NULL DEFAULT 30;

UPDATE user_settings
SET
    microphone_ptt_key = COALESCE(microphone_ptt_key, ''),
    screen_share_resolution = CASE
        WHEN screen_share_resolution IN ('720p', '1080p', '1440p') THEN screen_share_resolution
        ELSE '1080p'
    END,
    screen_share_fps = CASE
        WHEN screen_share_fps IN (10, 15, 30, 60) THEN screen_share_fps
        ELSE 30
    END;

-- +goose Down

ALTER TABLE user_settings
    DROP COLUMN IF EXISTS microphone_ptt_key,
    DROP COLUMN IF EXISTS screen_share_resolution,
    DROP COLUMN IF EXISTS screen_share_fps;
