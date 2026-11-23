package domain

import (
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
)

var (
	ErrE2EERecoveryNotFound = errors.New("E2EE recovery unavailable")
	ErrE2EERecoveryConflict = errors.New("E2EE recovery epoch or pending proposal changed")
	ErrE2EERecoveryCapacity = errors.New("E2EE retained recovery capacity exhausted")
	ErrE2EERecoveryState    = errors.New("invalid stored E2EE recovery state")
)

// Technical defaults, not private-key TTL/GC. All terminal operation evidence is
// retained; quota exhaustion never resets identity, ratchets or pending outbox.
const (
	E2EERecoveryProposalLease               = 24 * time.Hour
	MaxE2EERecoveryProposalsPerConversation = 256
	MaxE2EERecoveryOperationsPerGeneration  = 4096
	MaxE2EERecoveryPage                     = 32
)

type E2EERecovery struct {
	Outcome              *pb.RecoveryOutcome
	CreatedAt, ExpiresAt time.Time
	ResolvedAt           *time.Time
}

type E2EEEpoch struct {
	Epoch, Revision                         uint64
	Sealed                                  bool
	CreatedByProposalID, SealedByProposalID string
	BootstrapUserID                         int64 // Zero only for the initial epoch, which has no recovery creator.
	CreatedAt                               time.Time
	SealedAt                                *time.Time
}

type E2EEEpochCatalog struct {
	Conversation          *E2EEConversation
	ActiveEpoch, Revision uint64
	Epochs                []E2EEEpoch
	NextAfterEpoch        *uint64
}

// Advisory discovery, never an acceptance/expiry/fence proof.
type E2EEPendingRecoveryReference struct {
	ConversationID, ProposalID      string
	ExpectedEpoch, ExpectedRevision uint64
	InitiatedByOwn                  bool
	CreatedAt, ExpiresAt            time.Time
}
type E2EEPendingRecoveries struct {
	Proposals          []E2EEPendingRecoveryReference
	NextConversationID string
}

// An observation for an authorized participant, not a terminal result/fence.
type E2EERecoveryConflict struct {
	Epoch, Revision   uint64
	PendingProposalID string
}

func (e *E2EERecoveryConflict) Error() string { return ErrE2EERecoveryConflict.Error() }
func (e *E2EERecoveryConflict) Unwrap() error { return ErrE2EERecoveryConflict }
