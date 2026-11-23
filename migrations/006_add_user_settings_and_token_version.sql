-- +goose Up

CREATE TABLE IF NOT EXISTS user_settings (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    notifications_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    notification_sound BOOLEAN NOT NULL DEFAULT TRUE,
    desktop_notifications BOOLEAN NOT NULL DEFAULT FALSE,
    dnd_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    dnd_from_hour SMALLINT NOT NULL DEFAULT 23,
    dnd_to_hour SMALLINT NOT NULL DEFAULT 8,
    allow_friend_requests BOOLEAN NOT NULL DEFAULT TRUE,
    allow_direct_messages BOOLEAN NOT NULL DEFAULT TRUE,
    show_online_status BOOLEAN NOT NULL DEFAULT TRUE,
    show_email BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO user_settings (user_id)
SELECT id FROM users
ON CONFLICT (user_id) DO NOTHING;

-- +goose Down

DROP TABLE IF EXISTS user_settings;
