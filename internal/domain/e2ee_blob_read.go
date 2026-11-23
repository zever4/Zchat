package domain

import (
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
)

// Deliberately indistinguishable: unknown, unaccepted, mismatched and expired
// objects do not disclose attachment existence to a caller.
var ErrE2EEBlobReadUnavailable = errors.New("E2EE accepted attachment unavailable")

// Independent of the staging lease and descriptor/inbox retention policy.
// The fixed file deadline starts at the original accepted Send timestamp.
const E2EEBlobAcceptedRetention = 720 * time.Hour

// Public identity from the authenticated descriptor/envelope, never a path,
// upload operation, attempt selector, filename, file key or download bearer.
// Context remains2; Binding must identify an attachment envelope3.
type E2EEBlobReadRequest struct {
	Context *pb.ClientContext
	Binding *pb.EpochSendBinding
	Blob    *v2.BlobRef
}

// A committed metadata observation only. It is NOT an open-file/read lease,
// transferable capability or proof the bytes still exist. A coordinator must
// pin its own root/attempt, recheck authorization and join readers before GC.
// Do not export StorageID/AttemptID or this internal struct as an HTTP DTO.
type E2EEBlobReadTarget struct {
	Request                         *E2EEBlobReadRequest
	StorageID, AttemptID            string
	Revision                        uint64
	StoredAt, ExpiresAt, ObservedAt time.Time
}
