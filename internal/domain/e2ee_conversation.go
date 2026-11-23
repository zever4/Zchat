package domain

import (
	"errors"
	"time"
)

var (
	ErrE2EEConversationNotFound = errors.New("E2EE conversation unavailable")
	ErrE2EEPeerUnavailable      = errors.New("E2EE peer unavailable")
	ErrE2EEKeysNotReady         = errors.New("E2EE public keys are not ready")
	ErrE2EEConversationCapacity = errors.New("E2EE conversation capacity exhausted")
)

const (
	MaxE2EEConversationsPerAccount           = 4096 // Includes closed archives, no implicit GC.
	MaxE2EEConversationRequestsPerGeneration = 4096
)

type E2EEConversation struct {
	ID                 string
	Mode               string
	OwnDevice          *E2EEDevice
	PeerDevice         *E2EEDevice // Original bound generation, never silently the current replacement.
	State              string
	CloseReason        string
	ClosedGenerationID string
	ClosedAt           *time.Time
}
