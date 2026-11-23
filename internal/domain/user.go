package domain

import "time"

type User struct {
	ID           int64     `json:"id" db:"id"`
	UUID         string    `json:"user_uuid" db:"user_uuid"`
	Username     string    `json:"username" db:"username"`
	Email        string    `json:"email" db:"email"`
	PasswordHash string    `json:"-" db:"password_hash"`
	TokenVersion int       `json:"-" db:"token_version"`
	CreatedAt    time.Time `json:"created_at" db:"created_at"`
	Bio          string    `json:"bio" db:"bio"`
	AvatarURL    string    `json:"avatar_url" db:"avatar_url"`
}
