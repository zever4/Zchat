package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

type cleanupRepositoryFunc func(context.Context, domain.E2EECleanupLane, *domain.E2EECleanupPosition) (domain.E2EECleanupResult, error)

func (f cleanupRepositoryFunc) Cleanup(ctx context.Context, lane domain.E2EECleanupLane, after *domain.E2EECleanupPosition) (domain.E2EECleanupResult, error) {
	return f(ctx, lane, after)
}

func TestE2EECleanupWorkerAlternatesAndKeepsIndependentRecoveryCursors(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	positions := [2]*domain.E2EECleanupPosition{{Sequence: 7}, {Sequence: 11}}
	calls, observed := 0, 0
	repo := cleanupRepositoryFunc(func(ctx context.Context, lane domain.E2EECleanupLane, after *domain.E2EECleanupPosition) (domain.E2EECleanupResult, error) {
		if lane != domain.E2EECleanupLane(calls%2) {
			t.Fatal("lane starved", lane, calls)
		}
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 2*time.Second || time.Until(deadline) <= 0 {
			t.Fatal("missing pass deadline")
		}
		if calls < 2 && after != nil || calls >= 2 && calls < 4 && after != positions[lane] || calls == 4 && after != nil {
			t.Fatal("cursor lost, mixed or not wrapped", lane, calls)
		}
		calls++
		if calls == 5 {
			cancel()
		}
		result := domain.E2EECleanupResult{After: positions[lane], Busy: 1, Visited: 1}
		if calls > 2 {
			result.After = nil
		}
		if lane == domain.E2EECleanupExpired {
			return result, errors.New("transient storage failure after scheduling progress")
		}
		return result, nil
	})
	worker := NewE2EECleanupWorker(repo, func(_ domain.E2EECleanupLane, r domain.E2EECleanupResult, _ error) {
		observed++
		if r.Visited != 1 {
			t.Error("observer lost progress")
		}
	})
	worker.interval = time.Millisecond
	worker.Run(ctx)
	if calls != 5 || observed != 4 {
		t.Fatal("loop/cancellation/observer drift", calls, observed)
	}
}

func TestE2EECleanupWorkerCancellationJoinsInFlightPass(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started := make(chan struct{})
	done := make(chan struct{})
	repo := cleanupRepositoryFunc(func(ctx context.Context, _ domain.E2EECleanupLane, _ *domain.E2EECleanupPosition) (domain.E2EECleanupResult, error) {
		close(started)
		<-ctx.Done()
		return domain.E2EECleanupResult{}, ctx.Err()
	})
	worker := NewE2EECleanupWorker(repo, func(domain.E2EECleanupLane, domain.E2EECleanupResult, error) {
		t.Error("shutdown reported as a cleanup error")
	})
	worker.interval = time.Millisecond
	go func() { defer close(done); worker.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker failed to join")
	}
}

func TestE2EECleanupWorkerCancelledStartupDoesNoWork(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	worker := NewE2EECleanupWorker(cleanupRepositoryFunc(func(context.Context, domain.E2EECleanupLane, *domain.E2EECleanupPosition) (domain.E2EECleanupResult, error) {
		t.Fatal("work after cancellation")
		return domain.E2EECleanupResult{}, nil
	}), nil)
	worker.Run(ctx)
}
