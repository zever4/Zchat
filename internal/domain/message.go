package domain

import "time"

type MessageType = string

const (
	MessageTypePrivate = "private" // direct message
	MessageTypeSystem  = "system"
)

type Message struct {
	ID      int64  `json:"id" db:"id"`
	Content string `json:"content" db:"content"`

	// Receipt metadata is private to the sending account, never group history.
	ClientMessageID string `json:"-" db:"client_message_id"`
	Replayed        bool   `json:"-" db:"-"`

	SenderID  int64     `json:"sender_id" db:"sender_id"`
	ChatID    int64     `json:"chat_id" db:"chat_id"`
	CreatedAt time.Time `json:"created_at" db:"created_at"`

	// Поля для фронтенда
	SenderName   string `json:"sender_name" db:"-"`
	SenderAvatar string `json:"sender_avatar" db:"-"`
	Type         string `json:"type" db:"-"`
}

type EncryptedMessage struct {
	MessageID         string    `json:"message_id"`
	SenderID          string    `json:"sender_id"`
	SenderDeviceID    int       `json:"sender_device_id"`
	RecipientID       string    `json:"recipient_id"`
	RecipientDeviceID int       `json:"recipient_device_id"`
	Ciphertext        []byte    `json:"ciphertext"`
	MessageType       int       `json:"message_type"`
	PreKeyID          *uint32   `json:"prekey_id"`
	SignedPreKeyID    *uint32   `json:"signed_prekey_id"`
	KyberPreKeyID     *uint32   `json:"kyber_prekey_id"`
	Timestamp         int64     `json:"timestamp"`
	CreatedAt         time.Time `json:"created_at"`
}
