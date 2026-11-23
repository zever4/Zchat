package domain

import (
	"time"
)

type OneTimePreKey struct {
	UserID    string
	DeviceID  int
	KeyID     uint32
	PublicKey []byte
}

type KyberPreKey struct {
	UserID       string
	DeviceID     int
	KeyID        uint32
	PublicKey    []byte
	Signature    []byte
	IsLastResort bool
}

type DeviceKeys struct {
	UserID                string
	DeviceID              int
	RegistrationID        uint32
	IdentityKey           []byte
	SignedPreKeyID        uint32
	SignedPreKeyPublic    []byte
	SignedPreKeySignature []byte
	SignedPreKeyTimestamp int64
	CreatedAt             time.Time
}
