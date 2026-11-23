package domain

import (
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
)

var (
	ErrE2EEBlobCapacity          = errors.New("E2EE blob reservation capacity exhausted")
	ErrE2EEBlobCAS               = errors.New("E2EE blob reservation revision changed")
	ErrE2EEBlobLeaseActive       = errors.New("E2EE blob reservation lease is still active")
	ErrE2EEBlobConfiguration     = errors.New("E2EE blob storage limits unavailable or different")
	ErrE2EEBlobLeaseExpired      = errors.New("E2EE blob attempt lease expired")
	ErrE2EEBlobWriteState        = errors.New("E2EE blob attempt cannot be written in its current state")
	ErrE2EEBlobStorage           = errors.New("E2EE blob storage identity differs or is unavailable")
	ErrE2EEBlobCleanup           = errors.New("E2EE blob attempt is not eligible for staging cleanup")
	ErrE2EEBlobNotReady          = errors.New("E2EE blob storage startup reconciliation is incomplete")
	ErrE2EEBlobMismatch          = errors.New("E2EE blob filesystem and metadata do not match")
	ErrE2EEBlobOperationNotFound = errors.New("E2EE blob operation not observed after committed read")
	ErrE2EEBlobNotReadyForSend   = errors.New("E2EE message requires its live Ready attachment")
	ErrE2EEFileAckUnavailable    = errors.New("accepted attachment unavailable for file acknowledgement")
)

const E2EEBlobStagingLease = time.Hour

// Produced only by an authorized attachment Send after its Ready gate failed.
// This compact immutable binding is not a receipt/unsent proof or renewal grant.
type E2EEBlobSendFailure struct {
	Cause   error
	Binding *pb.EpochSendBinding
}

func (e *E2EEBlobSendFailure) Error() string { return "E2EE attachment unavailable for exact Send" }
func (e *E2EEBlobSendFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Validated public control input. Reserve precedes Signal encryption:
// it deliberately has no envelope digest, private descriptor, key or filename.
// The data framing is always attachment3; Context remains control protocol2.
type E2EEBlobReservationRequest struct {
	Context                   *pb.ClientContext
	MessageID, ConversationID string
	CryptoEpoch               *uint64
	Sender, Recipient         *pb.DeviceRef
	Blob                      *v2.BlobRef
	OperationID               string
	ExpectedRevision          uint64
	ExpectedAttemptID         string
}

// All counts bound retained metadata, including expired/deleting attempts.
// Bytes reserve declared ciphertext content, including old attempts awaiting
// unlink; shared FS block/inode/temp accounting is a separate physical adapter.
// Per-account counters span device generations. No defaults enable storage.
type E2EEBlobLimits struct {
	GlobalBytes, AccountBytes       int64
	GlobalClaims, AccountClaims     int64
	GlobalAttempts, AccountAttempts int64
}

func (l E2EEBlobLimits) Valid() bool {
	return l.AccountBytes > 0 && l.AccountBytes <= l.GlobalBytes &&
		l.AccountClaims > 0 && l.AccountClaims <= l.GlobalClaims &&
		l.AccountAttempts > 0 && l.AccountAttempts <= l.GlobalAttempts
}

// A metadata observation, NOT a bearer upload token or proof of Ready/Send.
// Expired is evaluated using the database clock AFTER acquiring locks. Replays
// preserve the exact attempt, operation, revision and exclusive deadline.
type E2EEBlobReservation struct {
	MessageID, AttemptID, OperationID string
	Revision                          uint64
	ReservedAt, ExpiresAt, ObservedAt time.Time
	State                             string
	Expired                           bool
}

// Current grant selection is separate from the original Reserve/renew CAS input.
// WriterID is assigned by the SERVER per upload, not by the native client.
type E2EEBlobAttemptRequest struct {
	Reservation *E2EEBlobReservationRequest
	Revision    uint64
	AttemptID   string
}
type E2EEBlobWriteRequest struct {
	Attempt   *E2EEBlobAttemptRequest
	WriterID  string
	StorageID string
}
type E2EEBlobWriteLease struct {
	Grant     E2EEBlobReservation
	WriterID  string
	StorageID string
}

type E2EEBlobCleanupReason string

const (
	E2EEBlobCleanupExpired       E2EEBlobCleanupReason = "expired"
	E2EEBlobCleanupFailedWriter  E2EEBlobCleanupReason = "writer_failed"
	E2EEBlobCleanupInactiveScope E2EEBlobCleanupReason = "scope_inactive"
)

// Internal system operation, never an authenticated user's arbitrary delete API.
// Size/account are discovered from immutable SQL data, not supplied by a caller.
type E2EEBlobCleanupRequest struct {
	StorageID, MessageID, AttemptID string
	Revision                        uint64
	Reason                          E2EEBlobCleanupReason
	ExpectedWriterID                string
}

// Internal accepted-file cleanup has a separate entry point from staging.
// Eligibility/reason, ownership, bytes and deadline come from immutable SQL
// facts, never the caller. This is not a network deletion capability.
type E2EEAcceptedBlobCleanupRequest struct {
	StorageID, MessageID, AttemptID string
	Revision                        uint64
}

// Persisted accepted-deletion causes chosen by SQL, not staging request reasons.
const (
	E2EEBlobReclaimFileAcknowledged = "file_acknowledged"
	E2EEBlobReclaimAcceptedExpired  = "accepted_expired"
)

type E2EEBlobCleanup struct {
	StorageID, MessageID, AttemptID, State, Reason string
	Revision                                       uint64
	ReclaimedAt                                    *time.Time
}

type E2EEBlobInventoryEntry struct {
	Cleanup                  E2EEBlobCleanupRequest
	State                    string
	WriterID, BoundStorageID string
	Bytes                    int64
	SenderUserID             int64
	SHA256                   [32]byte
	Expired                  bool
	Accepted                 bool // This exact selected attempt, not any receipt for the same UUID.
	DeletionReason           string
	ReclaimedAt              *time.Time
}

type E2EEBlobCleanupLane uint8

const (
	E2EEBlobCleanupUnfinished E2EEBlobCleanupLane = iota
	E2EEBlobCleanupExpiredStaging
	E2EEBlobCleanupAccepted
	E2EEBlobCleanupLaneCount
)

// Scheduling hints only. Expired uses the immutable (deadline, UUID) tuple;
// Accepted uses the active queue's (due_at, UUID) tuple in ExpiresAt. Unfinished
// uses only UUID. Neither cursor nor a candidate grants unlink rights.
type E2EEBlobCleanupPosition struct {
	AttemptID string
	ExpiresAt time.Time
}
type E2EEBlobCleanupCandidate struct {
	Request         E2EEBlobCleanupRequest // Staging lanes only; zero in accepted lane.
	AcceptedRequest *E2EEAcceptedBlobCleanupRequest
	Position        E2EEBlobCleanupPosition
}
