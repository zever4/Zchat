-- +goose Up
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    user_uuid TEXT UNIQUE NOT NULL,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    bio TEXT DEFAULT '',
    avatar_url TEXT DEFAULT '',
    token_version INTEGER NOT NULL DEFAULT 1
);


-- +goose Down

DROP TABLE IF EXISTS users;
