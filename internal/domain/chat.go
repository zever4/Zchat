package domain

import "time"

type Chat struct {
	ID              int64     `json:"id" db:"id"`
	Type            string    `json:"type" db:"type"`
	Name            string    `json:"name" db:"name"`
	CreatedAt       time.Time `json:"created_at" db:"created_at"`
	OwnerID         int64     `json:"owner_id,omitempty" db:"owner_id"`
	IsPublic        bool      `json:"is_public" db:"is_public"`
	InviteCode      string    `json:"invite_code,omitempty" db:"invite_code"`
	AvatarURL       string    `json:"avatar_url,omitempty" db:"avatar_url"`
	AdminsOnlyWrite bool      `json:"admins_only_write" db:"admins_only_write"`
	HideMembersList bool      `json:"hide_members_list" db:"hide_members_list"`
	AnonymousMode   bool      `json:"anonymous_mode" db:"anonymous_mode"`

	// For private chats: the other participant
	OtherUsername  string `json:"other_username,omitempty"`
	OtherUserID    int64  `json:"other_user_id,omitempty"`
	OtherAvatarURL string `json:"other_avatar_url,omitempty"`
}

type ChatMemberWithRole struct {
	UserID    int64  `json:"user_id"`
	Username  string `json:"username"`
	AvatarURL string `json:"avatar_url"`
	Role      string `json:"role"`
}
