//go:build linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
)

type collectorRepositoryModel struct {
	E2EEBlobWriterRepository
	page   func(context.Context, domain.E2EEBlobCleanupLane, *domain.E2EEBlobCleanupPosition, int) ([]domain.E2EEBlobCleanupCandidate, error)
	claim  func(context.Context, *domain.E2EEBlobCleanupRequest) error
	claims []string
}

func (*collectorRepositoryModel) ClaimAcceptedCleanup(context.Context, *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	return nil, domain.ErrE2EEBlobCleanup
}
func (*collectorRepositoryModel) CompleteAcceptedReclaim(context.Context, *domain.E2EEAcceptedBlobCleanupRequest, *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	return nil, domain.ErrE2EEBlobCleanup
}

func (m *collectorRepositoryModel) CleanupCandidates(ctx context.Context, _ string, lane domain.E2EEBlobCleanupLane, after *domain.E2EEBlobCleanupPosition, limit int) ([]domain.E2EEBlobCleanupCandidate, error) {
	return m.page(ctx, lane, after, limit)
}
func (m *collectorRepositoryModel) ClaimCleanup(ctx context.Context, r *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	m.claims = append(m.claims, r.AttemptID)
	if m.claim != nil {
		if err := m.claim(ctx, r); err != nil {
			return nil, err
		}
	}
	return &domain.E2EEBlobCleanup{StorageID: r.StorageID, MessageID: r.MessageID, AttemptID: r.AttemptID, Revision: r.Revision, State: "deleting"}, nil
}
func (m *collectorRepositoryModel) CompleteReclaim(_ context.Context, r *domain.E2EEBlobCleanupRequest, _ *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	return &domain.E2EEBlobCleanup{StorageID: r.StorageID, MessageID: r.MessageID, AttemptID: r.AttemptID, Revision: r.Revision, State: "reclaimed"}, nil
}
func collectorTestConfig() E2EEBlobCollectorConfig {
	return E2EEBlobCollectorConfig{Batch: 2, MaxRetryEntries: 8, Interval: 10 * time.Millisecond, StepTimeout: time.Second, RetryBase: 20 * time.Millisecond, RetryMax: 80 * time.Millisecond}
}
func collectorModel(t *testing.T) (*uploadCoreFixture, *collectorRepositoryModel, *blobCollector) {
	f := uploadCore(t)
	m := &collectorRepositoryModel{E2EEBlobWriterRepository: f.m}
	f.s.repo = m
	w := &blobCollector{s: f.s, repo: m, config: collectorTestConfig(), retry: make(map[string]blobCleanupRetry)}
	return f, m, w
}
func collectorRows(root string, n int) []domain.E2EEBlobCleanupCandidate {
	rows := make([]domain.E2EEBlobCleanupCandidate, n)
	for i := range rows {
		id := uuid.NewString()
		rows[i] = domain.E2EEBlobCleanupCandidate{Request: domain.E2EEBlobCleanupRequest{StorageID: root, MessageID: uuid.NewString(), AttemptID: id, Revision: 1, Reason: domain.E2EEBlobCleanupFailedWriter, ExpectedWriterID: uuid.NewString()}, Position: domain.E2EEBlobCleanupPosition{AttemptID: id}}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Position.AttemptID < rows[j].Position.AttemptID })
	return rows
}
func collectorPage(rows []domain.E2EEBlobCleanupCandidate) func(context.Context, domain.E2EEBlobCleanupLane, *domain.E2EEBlobCleanupPosition, int) ([]domain.E2EEBlobCleanupCandidate, error) {
	return func(_ context.Context, _ domain.E2EEBlobCleanupLane, after *domain.E2EEBlobCleanupPosition, limit int) ([]domain.E2EEBlobCleanupCandidate, error) {
		var page []domain.E2EEBlobCleanupCandidate
		for _, r := range rows {
			if after == nil || r.Position.AttemptID > after.AttemptID {
				page = append(page, r)
				if len(page) == limit {
					break
				}
			}
		}
		return page, nil
	}
}
func TestBlobCollectorFairCursorRetryAndBoundedMemory(t *testing.T) {
	f, m, w := collectorModel(t)
	w.config.Batch = 1
	w.config.MaxRetryEntries = 1
	rows := collectorRows(f.s.files.StorageID(), 3)
	m.page = collectorPage(rows)
	m.claim = func(_ context.Context, r *domain.E2EEBlobCleanupRequest) error {
		if r.AttemptID != rows[2].Request.AttemptID {
			return errBlobSynthetic
		}
		return nil
	}
	now := time.Now()
	for i := range 3 {
		r, err := w.step(t.Context(), domain.E2EEBlobCleanupUnfinished, now)
		if i < 2 && (r.Failed != 1 || !errors.Is(err, errBlobSynthetic)) || i == 2 && (r.Reclaimed != 1 || err != nil) {
			t.Fatal(i, r, err)
		}
		if len(w.retry) > 1 {
			t.Fatal("unbounded retry cache")
		}
		if i == 1 && r.RetryEvictions != 1 {
			t.Fatal(r)
		}
	}
	if len(m.claims) != 3 || m.claims[2] != rows[2].Request.AttemptID {
		t.Fatal("poisoned head starved healthy row", m.claims)
	}
	if _, err := w.step(t.Context(), domain.E2EEBlobCleanupUnfinished, now); err != nil || w.after[0] != nil {
		t.Fatal("cursor did not wrap", err)
	}
	m.claim = nil
	r, err := w.step(t.Context(), domain.E2EEBlobCleanupUnfinished, now.Add(time.Second))
	if err != nil || r.Reclaimed != 1 || m.claims[3] != rows[0].Request.AttemptID {
		t.Fatal("failed row lost after wrap/eviction", r, err)
	}
}
func TestBlobCollectorQueryBackoffDoesNotBlockOtherLane(t *testing.T) {
	f, m, w := collectorModel(t)
	now := time.Now()
	rows := collectorRows(f.s.files.StorageID(), 1)
	rows[0].Request.Reason = domain.E2EEBlobCleanupExpired
	rows[0].Position.ExpiresAt = now.Add(-time.Hour)
	queries := 0
	m.page = func(_ context.Context, lane domain.E2EEBlobCleanupLane, _ *domain.E2EEBlobCleanupPosition, _ int) ([]domain.E2EEBlobCleanupCandidate, error) {
		queries++
		if lane == domain.E2EEBlobCleanupUnfinished {
			return nil, errBlobSynthetic
		}
		return rows, nil
	}
	if _, err := w.step(t.Context(), domain.E2EEBlobCleanupUnfinished, now); !errors.Is(err, errBlobSynthetic) {
		t.Fatal(err)
	}
	r, err := w.step(t.Context(), domain.E2EEBlobCleanupUnfinished, now)
	if err != nil || !r.QueryDeferred || queries != 1 {
		t.Fatal(r, queries, err)
	}
	r, err = w.step(t.Context(), domain.E2EEBlobCleanupExpiredStaging, now)
	if err != nil || r.Reclaimed != 1 || queries != 2 {
		t.Fatal("query failure starved other lane", r, err)
	}
}
func TestBlobCollectorValidatesEntirePageBeforeUnlink(t *testing.T) {
	f, m, w := collectorModel(t)
	rows := collectorRows(f.s.files.StorageID(), 2)
	rows[1].Request.StorageID = uuid.NewString()
	m.page = collectorPage(rows)
	name := filepath.Join(f.path, rows[0].Request.AttemptID+".blob")
	if err := os.WriteFile(name, []byte("retain"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := w.step(t.Context(), domain.E2EEBlobCleanupUnfinished, time.Now()); !errors.Is(err, domain.ErrE2EEBlobMismatch) {
		t.Fatal(err)
	}
	if len(m.claims) != 0 {
		t.Fatal("mutated before validating whole page")
	}
	if _, err := os.Stat(name); err != nil {
		t.Fatal("deleted valid prefix of malformed page", err)
	}
}
func TestBlobCollectorCancellationKeepsUnprocessedCursor(t *testing.T) {
	f, m, w := collectorModel(t)
	rows := collectorRows(f.s.files.StorageID(), 2)
	m.page = collectorPage(rows)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	m.claim = func(context.Context, *domain.E2EEBlobCleanupRequest) error { cancel(); return context.Canceled }
	if _, err := w.step(ctx, domain.E2EEBlobCleanupUnfinished, time.Now()); !errors.Is(err, context.Canceled) || w.after[0].AttemptID != rows[0].Request.AttemptID {
		t.Fatal("skipped unprocessed rows", err)
	}
	m.claim = nil
	r, err := w.step(t.Context(), domain.E2EEBlobCleanupUnfinished, time.Now().Add(time.Second))
	if err != nil || r.Reclaimed != 1 || m.claims[1] != rows[1].Request.AttemptID {
		t.Fatal(r, err)
	}
}
func TestBlobCollectorBackoffJitterAndCeiling(t *testing.T) {
	w := &blobCollector{config: collectorTestConfig()}
	now := time.Now()
	r := blobCleanupRetry{}
	for i := range 40 {
		r = w.backoff(r, now)
		ceiling := w.config.RetryMax
		if i == 0 {
			ceiling = w.config.RetryBase
		} else if i == 1 {
			ceiling = 2 * w.config.RetryBase
		}
		delay := r.until.Sub(now)
		if delay < ceiling/2 || delay > ceiling || r.failures > 32 {
			t.Fatal("invalid bounded backoff", i, r, delay)
		}
	}
}
func TestBlobCollectorSingletonClosePinsRootAndStopsQueries(t *testing.T) {
	f, m, _ := collectorModel(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	m.page = func(ctx context.Context, _ domain.E2EEBlobCleanupLane, _ *domain.E2EEBlobCleanupPosition, _ int) ([]domain.E2EEBlobCleanupCandidate, error) {
		close(entered)
		<-release
		return nil, ctx.Err()
	}
	done := make(chan error, 1)
	go func() { done <- f.s.RunCleanup(t.Context(), collectorTestConfig(), nil) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not start")
	}
	if err := f.s.RunCleanup(t.Context(), collectorTestConfig(), nil); !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal("second collector admitted", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := f.s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	other, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("dropped owner while query pending", err)
	}
	once.Do(func() { close(release) })
	if err = <-done; !errors.Is(err, blobstore.ErrClosed) {
		t.Fatal(err)
	}
	if err = f.s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestBlobCollectorStartupGateAndInvalidConfiguration(t *testing.T) {
	f, _, _ := collectorModel(t)
	f.s.ready = false
	if err := f.s.RunCleanup(t.Context(), collectorTestConfig(), nil); !errors.Is(err, domain.ErrE2EEBlobNotReady) {
		t.Fatal(err)
	}
	c := collectorTestConfig()
	c.Batch = 129
	if err := f.s.RunCleanup(t.Context(), c, nil); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal(err)
	}
}
