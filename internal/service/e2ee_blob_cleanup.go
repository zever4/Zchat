package service

import (
	"context"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type E2EEBlobReclaimer interface {
	ClaimCleanup(context.Context, *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error)
	CompleteReclaim(context.Context, *domain.E2EEBlobCleanupRequest, *blobstore.Removed) (*domain.E2EEBlobCleanup, error)
}

type E2EEAcceptedBlobReclaimer interface {
	ClaimAcceptedCleanup(context.Context, *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error)
	CompleteAcceptedReclaim(context.Context, *domain.E2EEAcceptedBlobCleanupRequest, *blobstore.Removed) (*domain.E2EEBlobCleanup, error)
}

type blobCleanupWork struct {
	binding  domain.E2EEBlobCleanupRequest // Shape/binding only, not accepted eligibility.
	accepted bool
	claim    func(context.Context) (*domain.E2EEBlobCleanup, error)
	complete func(context.Context, *blobstore.Removed) (*domain.E2EEBlobCleanup, error)
}

// Cleanup is an INTERNAL staging collector, not a user-facing delete action.
// The service's owned root is authoritative; a caller cannot select another one.
// One cleanup slot is separate from upload slots, and owns shutdown until even
// its final DB commit and the removal lease have finished.
func (s *E2EEBlobUploadService) Cleanup(parent context.Context, input *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	return s.cleanup(parent, input, false, false)
}
func (s *E2EEBlobUploadService) cleanup(parent context.Context, input *domain.E2EEBlobCleanupRequest, startup, idleOnly bool) (*domain.E2EEBlobCleanup, error) {
	if input == nil {
		return nil, domain.ErrInvalidInput
	}
	req := *input
	req.StorageID = s.files.StorageID()
	if err := boundary.ValidateBlobCleanup(&req); err != nil {
		return nil, err
	}
	repo, ok := s.repo.(E2EEBlobReclaimer)
	if !ok {
		return nil, domain.ErrInvalidInput
	}
	return s.runBlobCleanup(parent, blobCleanupWork{binding: req,
		claim: func(ctx context.Context) (*domain.E2EEBlobCleanup, error) { return repo.ClaimCleanup(ctx, &req) },
		complete: func(ctx context.Context, proof *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
			return repo.CompleteReclaim(ctx, &req, proof)
		},
	}, startup, idleOnly)
}

// CleanupAccepted is internal owned coordination, never an arbitrary user delete
// endpoint. Eligibility/reason is chosen by SQL; a successful read COMMIT or a
// caller UUID alone cannot authorize unlink. Uses the SAME single cleanup slot.
func (s *E2EEBlobUploadService) CleanupAccepted(parent context.Context, input *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	return s.cleanupAccepted(parent, input, false)
}
func (s *E2EEBlobUploadService) cleanupAccepted(parent context.Context, input *domain.E2EEAcceptedBlobCleanupRequest, startup bool) (*domain.E2EEBlobCleanup, error) {
	if input == nil {
		return nil, domain.ErrInvalidInput
	}
	req := *input
	if req.StorageID != "" && req.StorageID != s.files.StorageID() {
		return nil, domain.ErrE2EEBlobStorage
	}
	req.StorageID = s.files.StorageID()
	binding := domain.E2EEBlobCleanupRequest{StorageID: req.StorageID, MessageID: req.MessageID, AttemptID: req.AttemptID, Revision: req.Revision, Reason: domain.E2EEBlobCleanupExpired}
	if err := boundary.ValidateBlobCleanup(&binding); err != nil {
		return nil, err
	}
	repo, ok := s.repo.(E2EEAcceptedBlobReclaimer)
	if !ok {
		return nil, domain.ErrInvalidInput
	}
	return s.runBlobCleanup(parent, blobCleanupWork{binding: binding, accepted: true,
		claim: func(ctx context.Context) (*domain.E2EEBlobCleanup, error) {
			return repo.ClaimAcceptedCleanup(ctx, &req)
		},
		complete: func(ctx context.Context, proof *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
			return repo.CompleteAcceptedReclaim(ctx, &req, proof)
		},
	}, startup, false)
}

func (s *E2EEBlobUploadService) runBlobCleanup(parent context.Context, work blobCleanupWork, startup, idleOnly bool) (*domain.E2EEBlobCleanup, error) {
	req := &work.binding
	base, timeoutCancel := context.WithTimeout(parent, s.config.Timeout)
	defer timeoutCancel()
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	op := &blobUploadOperation{cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, blobstore.ErrClosed
	}
	if !startup && (!s.ready || s.physical == nil || s.initializing != nil) || startup && s.initializing == nil {
		s.mu.Unlock()
		return nil, domain.ErrE2EEBlobNotReady
	}
	if len(s.cleanups) > 0 {
		s.mu.Unlock()
		return nil, blobstore.ErrBusy
	}
	// Runtime discovery of an unexpired Writing record is only a hint. A live
	// producer may still be checking auth or awaiting Ready COMMIT. Inspect and
	// reserve cleanup atomically with upload admission, never cancel that writer.
	if idleOnly && s.active[req.AttemptID] != nil {
		s.mu.Unlock()
		return nil, blobstore.ErrBusy
	}
	s.cleanups[req.AttemptID] = op
	writer := s.active[req.AttemptID]
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.cleanups, req.AttemptID); close(op.done); s.mu.Unlock() }()
	claimed, err := work.claim(ctx)
	if err != nil {
		return nil, err
	} // Unknown COMMIT is NOT unlink permission.
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	if !sameCleanup(claimed, req) || work.accepted && !validAcceptedCleanupState(claimed) {
		return nil, domain.ErrE2EEBlobCleanup
	}
	if work.accepted {
		// Admission is committed BEFORE cancellation. Registration precedes
		// ResolveRead, so an earlier snapshot cannot escape this join; later
		// registrations see Deleting/Reclaimed and cannot acquire a read lease.
		if err = s.CancelDownloads(ctx, req.MessageID); err != nil {
			return nil, err
		}
	}
	if claimed.State == "reclaimed" {
		return claimed, nil
	}
	if claimed.State != "deleting" {
		return nil, domain.ErrE2EEBlobCleanup
	}
	if writer != nil {
		writer.cancel(domain.ErrE2EEBlobWriteState)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-writer.done:
		}
	}
	if cause := context.Cause(ctx); cause != nil {
		return nil, cause
	}
	proof, err := s.files.RemoveVerified(ctx, req.AttemptID)
	if err != nil {
		return nil, err
	}
	defer proof.Close() // Hold root/attempt ownership through final SQL COMMIT.
	if s.physical != nil {
		if err = s.physical.Forget(req.AttemptID, proof); err != nil {
			return nil, err
		}
	}
	result, err := work.complete(ctx, proof)
	if err != nil {
		return nil, err
	}
	if !sameCleanup(result, req) || result.State != "reclaimed" || work.accepted && (!validAcceptedCleanupState(result) || result.Reason != claimed.Reason) {
		return nil, domain.ErrE2EEBlobCleanup
	}
	return result, nil
}
func acceptedCleanupReason(reason string) bool {
	return reason == domain.E2EEBlobReclaimFileAcknowledged || reason == domain.E2EEBlobReclaimAcceptedExpired
}
func validAcceptedCleanupState(c *domain.E2EEBlobCleanup) bool {
	return c != nil && acceptedCleanupReason(c.Reason) &&
		(c.State == "deleting" && c.ReclaimedAt == nil || c.State == "reclaimed" && c.ReclaimedAt != nil && c.ReclaimedAt.UnixMilli() > 0)
}
func sameCleanup(c *domain.E2EEBlobCleanup, r *domain.E2EEBlobCleanupRequest) bool {
	return c != nil && c.StorageID == r.StorageID && c.MessageID == r.MessageID && c.AttemptID == r.AttemptID && c.Revision == r.Revision
}
