package domain

import "time"

type GroupAuditLog struct {
	ID            int64     `json:"id" db:"id"`
	ChatID        int64     `json:"chat_id" db:"chat_id"`
	ActorUserID   int64     `json:"actor_user_id" db:"actor_user_id"`
	ActorUsername string    `json:"actor_username,omitempty"`
	Action        string    `json:"action" db:"action"`
	DetailsJSON   string    `json:"details_json" db:"details_json"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}
