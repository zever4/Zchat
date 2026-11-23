package service

import (
	"context"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type E2EEBlobInventory interface {
	BindStorage(context.Context, string) error
	Inventory(context.Context, string, string, int) ([]domain.E2EEBlobInventoryEntry, error)
}
type E2EEBlobStartupConfig struct {
	MaxAttempts, MaxArtifacts int
	MaxVerifyBytes            int64
	Timeout                   time.Duration
}
type E2EEBlobStartupResult struct {
	Attempts, Artifacts, Verified, Reclaimed int
	VerifiedBytes                            int64
}
type blobStartupCleanup struct {
	request  domain.E2EEBlobCleanupRequest
	accepted bool
}

func (s *E2EEBlobUploadService) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready && !s.closed
}

// Reconcile is a bounded, quiescent startup operation. Readiness is granted only
// after ALL records/artifacts are accounted for and persisted Ready/Linked bytes
// verified. No orphan is promoted. No private cryptographic state is reset.
func (s *E2EEBlobUploadService) Reconcile(parent context.Context, c E2EEBlobStartupConfig) (result *E2EEBlobStartupResult, err error) {
	if c.MaxAttempts < 1 || c.MaxAttempts > 100000 || c.MaxArtifacts < 1 || c.MaxArtifacts > 20000 || c.MaxVerifyBytes < 1 || c.Timeout <= 0 || c.Timeout > 10*time.Minute {
		return nil, domain.ErrInvalidInput
	}
	repo, ok := s.repo.(E2EEBlobInventory)
	if !ok {
		return nil, domain.ErrInvalidInput
	}
	base, timeoutCancel := context.WithTimeout(parent, c.Timeout)
	defer timeoutCancel()
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	op := &blobUploadOperation{cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, blobstore.ErrClosed
	}
	if s.ready || s.initializing != nil || len(s.active) > 0 || len(s.cleanups) > 0 {
		s.mu.Unlock()
		return nil, blobstore.ErrBusy
	}
	s.initializing = op
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		if err == nil && !s.closed {
			s.ready = true
		}
		s.initializing = nil
		close(op.done)
		s.mu.Unlock()
	}()
	storage := s.files.StorageID()
	if err = repo.BindStorage(ctx, storage); err != nil {
		return nil, err
	}
	artifacts, err := s.files.Inventory(ctx, c.MaxArtifacts)
	if err != nil {
		return nil, err
	}
	byID := make(map[string][]blobstore.Artifact, len(artifacts))
	for _, a := range artifacts {
		byID[a.AttemptID] = append(byID[a.AttemptID], a)
	}
	records := make([]domain.E2EEBlobInventoryEntry, 0, min(c.MaxAttempts, 128))
	known := make(map[string]bool)
	after := ""
	for {
		page, e := repo.Inventory(ctx, storage, after, 128)
		if e != nil {
			return nil, e
		}
		for _, m := range page {
			if len(records) >= c.MaxAttempts {
				return nil, domain.ErrE2EEBlobCapacity
			}
			if m.Cleanup.StorageID != storage || m.Cleanup.AttemptID <= after || known[m.Cleanup.AttemptID] || (m.BoundStorageID != "" && m.BoundStorageID != storage) {
				return nil, domain.ErrE2EEBlobMismatch
			}
			known[m.Cleanup.AttemptID] = true
			records = append(records, m)
			after = m.Cleanup.AttemptID
		}
		if len(page) < 128 {
			break
		}
	}
	for id := range byID {
		if !known[id] {
			return nil, domain.ErrE2EEBlobMismatch
		}
	}
	result = &E2EEBlobStartupResult{Attempts: len(records), Artifacts: len(artifacts)}
	verify := make([]domain.E2EEBlobInventoryEntry, 0)
	cleanup := make([]blobStartupCleanup, 0)
	// Construct/validate the ENTIRE plan before changing any staging records.
	for _, m := range records {
		files := byID[m.Cleanup.AttemptID]
		if m.Bytes <= boundary.AttachmentBlobOverhead || m.Bytes > boundary.MaxAttachmentBlobBytes {
			return nil, domain.ErrE2EEBlobMismatch
		}
		if m.Accepted {
			binding := m.Cleanup
			binding.Reason = domain.E2EEBlobCleanupExpired // Validate binding, not eligibility.
			if boundary.ValidateBlobCleanup(&binding) != nil || boundary.ValidateBlobStorage(m.WriterID) != nil || m.BoundStorageID != storage {
				return nil, domain.ErrE2EEBlobMismatch
			}
			if m.State == "linked" {
				if m.DeletionReason != "" || m.ReclaimedAt != nil {
					return nil, domain.ErrE2EEBlobMismatch
				}
			} else if !validAcceptedCleanupState(&domain.E2EEBlobCleanup{State: m.State, Reason: m.DeletionReason, ReclaimedAt: m.ReclaimedAt}) {
				return nil, domain.ErrE2EEBlobMismatch
			}
		} else if m.State == "linked" || acceptedCleanupReason(m.DeletionReason) {
			return nil, domain.ErrE2EEBlobMismatch
		}
		for _, a := range files {
			if a.Bytes < 0 || a.Bytes > m.Bytes {
				return nil, domain.ErrE2EEBlobMismatch
			}
		}
		r := m.Cleanup
		switch m.State {
		case "reserved", "reclaimed":
			if len(files) > 0 {
				return nil, domain.ErrE2EEBlobMismatch
			}
			if m.State == "reserved" && m.Expired {
				r.Reason = domain.E2EEBlobCleanupExpired
				cleanup = append(cleanup, blobStartupCleanup{r, m.Accepted})
			}
		case "writing", "deleting":
			if m.State == "writing" {
				if m.WriterID == "" || m.BoundStorageID != storage {
					return nil, domain.ErrE2EEBlobMismatch
				}
				r.Reason = domain.E2EEBlobCleanupFailedWriter
				r.ExpectedWriterID = m.WriterID
			} else {
				r.Reason = domain.E2EEBlobCleanupExpired
			}
			cleanup = append(cleanup, blobStartupCleanup{r, m.Accepted})
		case "ready", "linked":
			if m.WriterID == "" || m.BoundStorageID != storage || len(files) != 1 || files[0].Part || files[0].Bytes != m.Bytes {
				return nil, domain.ErrE2EEBlobMismatch
			}
			if m.Bytes > c.MaxVerifyBytes-result.VerifiedBytes {
				return nil, domain.ErrE2EEBlobCapacity
			}
			result.VerifiedBytes += m.Bytes
			verify = append(verify, m)
			if m.State == "ready" && m.Expired {
				r.Reason = domain.E2EEBlobCleanupExpired
				cleanup = append(cleanup, blobStartupCleanup{r, m.Accepted})
			}
		default:
			return nil, domain.ErrE2EEBlobMismatch
		}
	}
	for _, m := range verify {
		if err = s.files.Verify(ctx, blobstore.Attempt{ID: m.Cleanup.AttemptID, Bytes: m.Bytes, SHA256: m.SHA256}); err != nil {
			return nil, err
		}
		result.Verified++
	}
	for i := range cleanup {
		item := &cleanup[i]
		if item.accepted {
			r := item.request
			_, err = s.cleanupAccepted(ctx, &domain.E2EEAcceptedBlobCleanupRequest{StorageID: r.StorageID, MessageID: r.MessageID, AttemptID: r.AttemptID, Revision: r.Revision}, true)
		} else {
			_, err = s.cleanup(ctx, &item.request, true, false)
		}
		if err != nil {
			return nil, err
		}
		result.Reclaimed++
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
