package service

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

type E2EECleanupRepository interface {
	Cleanup(context.Context, domain.E2EECleanupLane, *domain.E2EECleanupPosition) (domain.E2EECleanupResult, error)
}

// One serial worker across all accounts. Its cursors are scheduling hints only;
// persisted receipts/removals/counters make every pass/restart idempotent.
type E2EECleanupWorker struct {
	repo     E2EECleanupRepository
	observe  func(domain.E2EECleanupLane, domain.E2EECleanupResult, error)
	interval time.Duration
}

func NewE2EECleanupWorker(repo E2EECleanupRepository, observe func(domain.E2EECleanupLane, domain.E2EECleanupResult, error)) *E2EECleanupWorker {
	return &E2EECleanupWorker{repo: repo, observe: observe, interval: 5 * time.Second}
}

func (w *E2EECleanupWorker) Run(ctx context.Context) {
	wait := func(delay time.Duration) bool {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return true
		}
	}
	if !wait(time.Duration(rand.Int64N(int64(w.interval) + 1))) {
		return
	}
	var after [2]*domain.E2EECleanupPosition
	lane := domain.E2EECleanupExpired
	for ctx.Err() == nil {
		bounded, cancel := context.WithTimeout(ctx, 2*time.Second)
		result, err := w.repo.Cleanup(bounded, lane, after[lane])
		cancel()
		after[lane] = result.After
		if w.observe != nil && ctx.Err() == nil {
			w.observe(lane, result, err)
		}
		lane = 1 - lane // A failing/busy lane cannot monopolize the next pass.
		if !wait(w.interval + time.Duration(rand.Int64N(int64(w.interval/5)+1))) {
			return
		}
	}
}
