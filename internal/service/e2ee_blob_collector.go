package service

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type E2EEBlobCleanupScanner interface {
	CleanupCandidates(context.Context, string, domain.E2EEBlobCleanupLane, *domain.E2EEBlobCleanupPosition, int) ([]domain.E2EEBlobCleanupCandidate, error)
}

// No defaults enable storage. Retry entries are bounded scheduling hints only;
// eviction/restart may retry sooner, but cannot lose the persisted DB work.
type E2EEBlobCollectorConfig struct {
	Batch, MaxRetryEntries                     int
	Interval, StepTimeout, RetryBase, RetryMax time.Duration
}

func (c E2EEBlobCollectorConfig) Validate() error {
	if c.Batch < 1 || c.Batch > 128 || c.MaxRetryEntries < 1 || c.MaxRetryEntries > 4096 || c.Interval < 10*time.Millisecond || c.Interval > time.Minute || c.StepTimeout <= 0 || c.StepTimeout > time.Minute || c.RetryBase < c.Interval || c.RetryMax < c.RetryBase || c.RetryMax > 5*time.Minute {
		return domain.ErrInvalidInput
	}
	return nil
}

type E2EEBlobCleanupSweep struct {
	Lane                                                 domain.E2EEBlobCleanupLane
	Scanned, Reclaimed, Deferred, Failed, RetryEvictions int
	QueryDeferred                                        bool
}
type blobCleanupRetry struct {
	until    time.Time
	failures uint8
}
type blobCollector struct {
	s          *E2EEBlobUploadService
	repo       E2EEBlobCleanupScanner
	config     E2EEBlobCollectorConfig
	after      [domain.E2EEBlobCleanupLaneCount]*domain.E2EEBlobCleanupPosition
	queryRetry [domain.E2EEBlobCleanupLaneCount]blobCleanupRetry
	retry      map[string]blobCleanupRetry
}

// RunCleanup runs ONE serial collector across accounts under the owned root.
// The caller supplies the goroutine. Close cancels/joins it, even if a DB call
// ignores cancellation. observe must return promptly; it runs inline per sweep.
func (s *E2EEBlobUploadService) RunCleanup(parent context.Context, c E2EEBlobCollectorConfig, observe func(E2EEBlobCleanupSweep, error)) error {
	if err := c.Validate(); err != nil {
		return err
	}
	repo, ok := s.repo.(E2EEBlobCleanupScanner)
	if !ok {
		return domain.ErrInvalidInput
	}
	if _, ok = s.repo.(E2EEBlobReclaimer); !ok {
		return domain.ErrInvalidInput
	}
	if _, ok = s.repo.(E2EEAcceptedBlobReclaimer); !ok {
		return domain.ErrInvalidInput
	}
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	op := &blobUploadOperation{cancel: cancel, done: make(chan struct{})}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return blobstore.ErrClosed
	}
	if !s.ready || s.physical == nil || s.initializing != nil {
		s.mu.Unlock()
		return domain.ErrE2EEBlobNotReady
	}
	if s.collector != nil {
		s.mu.Unlock()
		return blobstore.ErrBusy
	}
	s.collector = op
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.collector = nil; close(op.done); s.mu.Unlock() }()
	w := &blobCollector{s: s, repo: repo, config: c, retry: make(map[string]blobCleanupRetry)}
	wait := func(d time.Duration) bool {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	lane := domain.E2EEBlobCleanupUnfinished
	if !wait(time.Duration(rand.Int64N(int64(c.Interval) + 1))) {
		return context.Cause(ctx)
	}
	for ctx.Err() == nil {
		result, err := w.step(ctx, lane, time.Now())
		if observe != nil && ctx.Err() == nil {
			observe(result, err)
		}
		lane = (lane + 1) % domain.E2EEBlobCleanupLaneCount // Failures cannot monopolize other lanes.
		if !wait(c.Interval + time.Duration(rand.Int64N(int64(c.Interval/5)+1))) {
			break
		}
	}
	return context.Cause(ctx)
}

func (w *blobCollector) step(parent context.Context, lane domain.E2EEBlobCleanupLane, now time.Time) (result E2EEBlobCleanupSweep, err error) {
	started := time.Now()
	current := func() time.Time { return now.Add(time.Since(started)) }
	result.Lane = lane
	if lane >= domain.E2EEBlobCleanupLaneCount {
		return result, domain.ErrInvalidInput
	}
	if now.Before(w.queryRetry[lane].until) {
		result.QueryDeferred = true
		return result, nil
	}
	ctx, cancel := context.WithTimeout(parent, w.config.StepTimeout)
	defer cancel()
	page, err := w.repo.CleanupCandidates(ctx, w.s.files.StorageID(), lane, w.after[lane], w.config.Batch)
	if err == nil {
		err = w.validatePage(lane, page)
	}
	if err != nil {
		w.queryRetry[lane] = w.backoff(w.queryRetry[lane], current())
		return result, err
	}
	w.queryRetry[lane] = blobCleanupRetry{}
	for id, r := range w.retry {
		if !now.Before(r.until.Add(w.config.RetryMax)) {
			delete(w.retry, id)
		}
	}
	for _, candidate := range page {
		if ctx.Err() != nil {
			return result, errors.Join(err, ctx.Err())
		}
		result.Scanned++
		position := candidate.Position
		w.after[lane] = &position // Even failed/deferred rows advance.
		id := candidate.Position.AttemptID
		if current().Before(w.retry[id].until) {
			result.Deferred++
			continue
		}
		var e error
		if lane == domain.E2EEBlobCleanupAccepted {
			_, e = w.s.CleanupAccepted(ctx, candidate.AcceptedRequest)
		} else {
			_, e = w.s.cleanup(ctx, &candidate.Request, false, candidate.Request.Reason == domain.E2EEBlobCleanupFailedWriter)
		}
		if e == nil {
			result.Reclaimed++
			delete(w.retry, id)
			continue
		}
		if errors.Is(e, blobstore.ErrBusy) {
			result.Deferred++
			continue
		}
		result.Failed++
		err = errors.Join(err, e)
		if _, exists := w.retry[id]; !exists && len(w.retry) >= w.config.MaxRetryEntries {
			var oldest string
			for key, r := range w.retry {
				if oldest == "" || r.until.Before(w.retry[oldest].until) {
					oldest = key
				}
			}
			delete(w.retry, oldest)
			result.RetryEvictions++
		}
		w.retry[id] = w.backoff(w.retry[id], current())
	}
	if len(page) < w.config.Batch {
		w.after[lane] = nil
	}
	return result, err
}

func (w *blobCollector) backoff(r blobCleanupRetry, now time.Time) blobCleanupRetry {
	if r.failures < 32 {
		r.failures++
	}
	d := w.config.RetryBase
	for i := uint8(1); i < r.failures && d < w.config.RetryMax; i++ {
		if d > w.config.RetryMax/2 {
			d = w.config.RetryMax
		} else {
			d *= 2
		}
	}
	d = min(d, w.config.RetryMax)
	// Equal jitter:50..100% of the exponential ceiling, bounded by RetryMax.
	r.until = now.Add(d/2 + time.Duration(rand.Int64N(int64(d-d/2)+1)))
	return r
}

func (w *blobCollector) validatePage(lane domain.E2EEBlobCleanupLane, page []domain.E2EEBlobCleanupCandidate) error {
	if len(page) > w.config.Batch {
		return domain.ErrE2EEBlobMismatch
	}
	after := w.after[lane]
	for _, e := range page {
		r := e.Request
		if lane == domain.E2EEBlobCleanupAccepted {
			if e.AcceptedRequest == nil || r != (domain.E2EEBlobCleanupRequest{}) {
				return domain.ErrE2EEBlobMismatch
			}
			a := e.AcceptedRequest
			r = domain.E2EEBlobCleanupRequest{StorageID: a.StorageID, MessageID: a.MessageID, AttemptID: a.AttemptID, Revision: a.Revision, Reason: domain.E2EEBlobCleanupExpired}
		} else if e.AcceptedRequest != nil {
			return domain.ErrE2EEBlobMismatch
		}
		if r.StorageID != w.s.files.StorageID() || e.Position.AttemptID != r.AttemptID {
			return domain.ErrE2EEBlobMismatch
		}
		if err := boundary.ValidateBlobCleanup(&r); err != nil {
			return err
		}
		if lane == domain.E2EEBlobCleanupUnfinished {
			if !e.Position.ExpiresAt.IsZero() || r.Reason != domain.E2EEBlobCleanupExpired && r.Reason != domain.E2EEBlobCleanupFailedWriter || after != nil && e.Position.AttemptID <= after.AttemptID {
				return domain.ErrE2EEBlobMismatch
			}
		} else {
			if e.Position.ExpiresAt.IsZero() || r.Reason != domain.E2EEBlobCleanupExpired || after != nil && (e.Position.ExpiresAt.Before(after.ExpiresAt) || e.Position.ExpiresAt.Equal(after.ExpiresAt) && e.Position.AttemptID <= after.AttemptID) {
				return domain.ErrE2EEBlobMismatch
			}
		}
		position := e.Position
		after = &position
	}
	return nil
}
