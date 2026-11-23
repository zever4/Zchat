package domain

import "time"

type GroupInviteLink struct {
	ID            int64      `json:"id" db:"id"`
	ChatID        int64      `json:"chat_id" db:"chat_id"`
	Code          string     `json:"code" db:"code"`
	CreatedBy     int64      `json:"created_by" db:"created_by"`
	IsActive      bool       `json:"is_active" db:"is_active"`
	CreatedAt     time.Time  `json:"created_at" db:"created_at"`
	DeactivatedAt *time.Time `json:"deactivated_at,omitempty" db:"deactivated_at"`
}
