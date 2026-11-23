package domain

import (
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
)

var (
	ErrE2EEMessageConflict    = errors.New("E2EE message identifier conflicts with saved data")
	ErrE2EEMessageCapacity    = errors.New("E2EE message storage capacity exhausted")
	ErrE2EEMessageState       = errors.New("invalid stored E2EE message state")
	ErrE2EEMessageNotFound    = errors.New("E2EE message unavailable for this generation")
	ErrE2EEEpochSealed        = errors.New("E2EE message epoch is sealed")
	ErrE2EEEpochInactive      = errors.New("E2EE epoch is not active")
	ErrE2EEBootstrapInitiator = errors.New("E2EE recovery bootstrap belongs to proposal creator")
)

// Bounded experimental storage; no automatic eviction of pending ciphertext or
// idempotency evidence. ACK/expiry cleanup must preserve the immutable receipt.
const (
	E2EEMessageRetention                = 30 * 24 * time.Hour
	MaxE2EESendReceiptsPerGeneration    = 100_000
	MaxE2EEPendingMessagesPerGeneration = 10_000
	MaxE2EEPendingBytesPerGeneration    = 64 << 20
)

type E2EESendReceipt struct {
	MessageID           string
	Digest              [32]byte
	StoredAt, ExpiresAt time.Time
	CryptoEpoch         *uint64 // nil only in the current prototype v1 path, never a v2 fallback.
}

// Bound server observation, NOT recipient ACK/read proof. Native must verify
// the accepted signatures/context before settling the exact retained outbox.
type E2EEEpochSealed struct {
	Fence    *pb.EpochSealedFence
	Recovery *pb.RecoveryOutcome
}

func (e *E2EEEpochSealed) Error() string { return ErrE2EEEpochSealed.Error() }
func (e *E2EEEpochSealed) Unwrap() error { return ErrE2EEEpochSealed }

type E2EEEpochDelivery struct {
	Sequence  int64
	Envelope  *v2.Envelope
	Digest    [32]byte
	ExpiresAt time.Time
}

type E2EEMessageStatus struct {
	Receipt    E2EESendReceipt
	State      pb.MessageState
	ObservedAt time.Time
}
