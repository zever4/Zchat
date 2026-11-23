package domain

import "time"

const (
	FriendRequestPending  = "pending"
	FriendRequestAccepted = "accepted"
	FriendRequestRejected = "rejected"
)

type FriendRequest struct {
	ID         int64     `json:"id" db:"id"`
	FromUserID int64     `json:"from_user_id" db:"from_user_id"`
	ToUserID   int64     `json:"to_user_id" db:"to_user_id"`
	Status     string    `json:"status" db:"status"`
	CreatedAt  time.Time `json:"created_at" db:"created_at"`

	// Populated by joins
	FromUsername string `json:"from_username,omitempty"`
	ToUsername   string `json:"to_username,omitempty"`
}
