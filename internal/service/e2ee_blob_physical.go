package service

import (
	"context"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/storagebudget"
)

// AttachBudget connects the reconciled root to the ordinary manager's budget.
// The opt-in runtime calls this before mounting HTTP. Ready is reconciliation;
// upload/collector admission additionally requires this physical accounting.
func (s *E2EEBlobUploadService) AttachBudget(parent context.Context, b *storagebudget.Budget, c E2EEBlobStartupConfig) (err error) {
	if b == nil || c.MaxAttempts < 1 || c.MaxAttempts > 100000 || c.MaxArtifacts < 1 || c.MaxArtifacts > 20000 || c.MaxVerifyBytes < 1 || c.Timeout <= 0 || c.Timeout > 10*time.Minute {
		return domain.ErrInvalidInput
	}
	repo, ok := s.repo.(E2EEBlobInventory)
	if !ok {
		return domain.ErrInvalidInput
	}
	base, timeoutCancel := context.WithTimeout(parent, c.Timeout)
	defer timeoutCancel()
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	op := &blobUploadOperation{cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return blobstore.ErrClosed
	}
	if !s.ready {
		s.mu.Unlock()
		return domain.ErrE2EEBlobNotReady
	}
	if s.physical != nil || s.initializing != nil || s.collector != nil || len(s.active) != 0 || len(s.cleanups) != 0 {
		s.mu.Unlock()
		return blobstore.ErrBusy
	}
	s.initializing = op
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.initializing = nil; close(op.done); s.mu.Unlock() }()
	owners := make(map[string]int64)
	after := ""
	count := 0
	var verified int64
	for {
		page, e := repo.Inventory(ctx, s.files.StorageID(), after, 128)
		if e != nil {
			return e
		}
		for _, m := range page {
			count++
			if count > c.MaxAttempts {
				return domain.ErrE2EEBlobCapacity
			}
			if m.Cleanup.StorageID != s.files.StorageID() || m.Cleanup.AttemptID <= after || m.SenderUserID <= 0 {
				return domain.ErrE2EEBlobMismatch
			}
			after = m.Cleanup.AttemptID
			switch m.State {
			case "reserved", "reclaimed":
			case "ready", "linked":
				if m.BoundStorageID != s.files.StorageID() || m.Bytes <= 0 || m.Bytes > c.MaxVerifyBytes-verified {
					return domain.ErrE2EEBlobMismatch
				}
				if e = s.files.Verify(ctx, blobstore.Attempt{ID: m.Cleanup.AttemptID, Bytes: m.Bytes, SHA256: m.SHA256}); e != nil {
					return e
				}
				verified += m.Bytes
				owners[m.Cleanup.AttemptID] = m.SenderUserID
			default:
				return domain.ErrE2EEBlobMismatch
			}
		}
		if len(page) < 128 {
			break
		}
	}
	physical, err := s.files.AttachAccounting(ctx, b, owners, c.MaxArtifacts)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.physical = physical
	s.downloadLimit = b.DownloadCapacity()
	s.mu.Unlock()
	return nil
}
