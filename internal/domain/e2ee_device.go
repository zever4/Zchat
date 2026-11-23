package domain

import (
	"errors"
	"time"
)

var (
	ErrE2EEDeviceConflict    = errors.New("E2EE device already registered")
	ErrE2EERequestConflict   = errors.New("E2EE request id used for different material")
	ErrE2EEGenerationChanged = errors.New("E2EE device generation changed")
	ErrE2EEDeviceCredential  = errors.New("invalid E2EE device credential")
	ErrE2EEReauthLimited     = errors.New("E2EE replacement authentication rate limited")
)

type E2EEDevice struct {
	UserID                 int64
	UserUUID               string
	GenerationID           string
	DeviceID               uint32
	RegistrationID         uint32
	IdentityKey            []byte
	CredentialHash         []byte `json:"-"`
	PublicMaterialReady    bool
	PublicMaterialRevision uint64
	RetiredAt              *time.Time
}

type E2EEDeviceRegistration struct {
	RequestID            string
	ExpectedGenerationID string // Empty only for initial registration.
	RegistrationID       uint32
	IdentityKey          []byte
	CredentialHash       []byte `json:"-"`
}

// Credential snapshot verified by bcrypt; compare under the SQL user row lock.
// Do not persist a password or its hash in an idempotency request record.
type E2EEReauthentication struct {
	PasswordHash string `json:"-"`
	TokenVersion int
}

type E2EEDeviceChange struct {
	Device               *E2EEDevice
	Replayed             bool
	PreviousGenerationID string
}
