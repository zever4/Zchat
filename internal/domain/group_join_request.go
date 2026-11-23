package domain

import "time"

type GroupJoinRequest struct {
	ID            int64     `json:"id" db:"id"`
	ChatID        int64     `json:"chat_id" db:"chat_id"`
	UserID        int64     `json:"user_id" db:"user_id"`
	Status        string    `json:"status" db:"status"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time `json:"updated_at" db:"updated_at"`
	Username      string    `json:"username,omitempty"`
	UserAvatarURL string    `json:"user_avatar_url,omitempty"`
}
