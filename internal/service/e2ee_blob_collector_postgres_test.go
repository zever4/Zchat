//go:build linux

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

type collectorObservation struct {
	sweep E2EEBlobCleanupSweep
	err   error
}

func startCollector(t *testing.T, s *E2EEBlobUploadService) <-chan collectorObservation {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	events := make(chan collectorObservation, 128)
	done := make(chan error, 1)
	go func() {
		done <- s.RunCleanup(ctx, collectorTestConfig(), func(r E2EEBlobCleanupSweep, err error) {
			select {
			case events <- collectorObservation{r, err}:
			default:
			}
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) && !errors.Is(err, blobstore.ErrClosed) {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("collector did not join")
		}
	})
	return events
}
func awaitCollector(t *testing.T, events <-chan collectorObservation, predicate func(collectorObservation) bool) collectorObservation {
	t.Helper()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if predicate(event) {
				return event
			}
		case <-timer.C:
			t.Fatal("collector observation not reached")
			return collectorObservation{}
		}
	}
}
func TestBlobCollectorKeepsLiveWriterThenReclaimsFailurePostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	src := blockedUploadSource()
	defer src.Close()
	done := make(chan error, 1)
	go func() { _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, src); done <- err }()
	awaitUploadRead(t, src)
	events := startCollector(t, f.upload)
	awaitCollector(t, events, func(e collectorObservation) bool {
		return e.sweep.Lane == domain.E2EEBlobCleanupUnfinished && e.sweep.Deferred == 1
	})
	f.assertState(t, "writing")
	select {
	case <-src.closed:
		t.Fatal("collector cancelled unexpired live producer")
	default:
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("failed source returned Ready")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("producer did not join")
	}
	awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Reclaimed == 1 })
	assertReclaimed(t, f) // No hour-long expiry or process restart needed.
}

func TestBlobCollectorExpiresAndJoinsLiveProducerPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	// Delay the existing watcher in this fixture so the collector, not the
	// periodic auth check, proves cancellation/join of the expired producer.
	f.upload.config.RecheckInterval = 2 * time.Second
	src := blockedUploadSource()
	defer src.Close()
	done := make(chan error, 1)
	go func() { _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, src); done <- err }()
	awaitUploadRead(t, src)
	expireServiceAttempt(t, f)
	events := startCollector(t, f.upload)
	awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Reclaimed == 1 })
	select {
	case err := <-done:
		if !errors.Is(err, domain.ErrE2EEBlobWriteState) {
			t.Fatal("collector did not cancel expired producer", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("expired producer not joined")
	}
	assertReclaimed(t, f)
}

type lostCleanupCollectorRepository struct {
	*repository.PgE2EEBlobRepository
	lane      string
	lost      atomic.Bool
	completed atomic.Int32
}

func (m *lostCleanupCollectorRepository) ClaimCleanup(ctx context.Context, r *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	x, err := m.PgE2EEBlobRepository.ClaimCleanup(ctx, r)
	if err == nil && m.lane == "claim" && m.lost.CompareAndSwap(false, true) {
		return nil, errBlobSynthetic
	}
	return x, err
}
func (m *lostCleanupCollectorRepository) CompleteReclaim(ctx context.Context, r *domain.E2EEBlobCleanupRequest, p *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	x, err := m.PgE2EEBlobRepository.CompleteReclaim(ctx, r, p)
	m.completed.Add(1)
	if err == nil && m.lane == "reclaim" && m.lost.CompareAndSwap(false, true) {
		return nil, errBlobSynthetic
	}
	return x, err
}
func TestBlobCollectorRetriesUnknownCleanupCommitPostgres(t *testing.T) {
	for _, lane := range []string{"claim", "reclaim"} {
		t.Run(lane, func(t *testing.T) {
			f := blobUploadPostgres(t)
			if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); err != nil {
				t.Fatal(err)
			}
			expireServiceAttempt(t, f)
			model := &lostCleanupCollectorRepository{PgE2EEBlobRepository: f.repo, lane: lane}
			f.upload.repo = model
			events := startCollector(t, f.upload)
			event := awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Failed == 1 })
			if !errors.Is(event.err, errBlobSynthetic) {
				t.Fatal(event.err)
			}
			if lane == "claim" {
				awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Reclaimed == 1 })
			} else {
				for _, next := range []domain.E2EEBlobCleanupLane{domain.E2EEBlobCleanupUnfinished, domain.E2EEBlobCleanupExpiredStaging} {
					awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Lane == next })
				}
			}
			assertReclaimed(t, f)
			if model.completed.Load() != 1 {
				t.Fatal("repeated physical/quota completion", model.completed.Load())
			}
		})
	}
}

type lostReadyCollectorRepository struct {
	*repository.PgE2EEBlobRepository
}

func (m *lostReadyCollectorRepository) CompleteWrite(ctx context.Context, uid int64, r *domain.E2EEBlobWriteRequest, p *blobstore.Written) (*domain.E2EEBlobWriteLease, error) {
	x, err := m.PgE2EEBlobRepository.CompleteWrite(ctx, uid, r, p)
	if err == nil && x != nil {
		return nil, errBlobSynthetic
	}
	return x, err
}
func TestBlobCollectorProtectsLostReadyCommitThenExpiresPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	f.upload.repo = &lostReadyCollectorRepository{f.repo}
	if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); !errors.Is(err, errBlobSynthetic) {
		t.Fatal(err)
	}
	events := startCollector(t, f.upload)
	for _, lane := range []domain.E2EEBlobCleanupLane{domain.E2EEBlobCleanupUnfinished, domain.E2EEBlobCleanupExpiredStaging} {
		awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Lane == lane })
	}
	f.assertState(t, "ready")
	got, err := os.ReadFile(filepath.Join(f.path, f.requests.AttemptID+".blob"))
	if err != nil || !bytes.Equal(got, f.data) {
		t.Fatal("deleted uncertain Ready commit", err)
	}
	expireServiceAttempt(t, f)
	awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Reclaimed == 1 })
	assertReclaimed(t, f)
}
func TestBlobCollectorFilesystemErrorRetainsQuotaAndOtherLaneProgressesPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	if _, err := f.repo.BeginWrite(t.Context(), f.uid, &domain.E2EEBlobWriteRequest{Attempt: f.requests, WriterID: uuid.NewString(), StorageID: f.upload.files.StorageID()}); err != nil {
		t.Fatal(err)
	}
	// Inject an explicit operator/filesystem fault. GC must refuse to remove a
	// directory masquerading as a blob, retain the quota, and report the failure.
	name := filepath.Join(f.path, f.requests.AttemptID+".blob")
	if err := os.Mkdir(name, 0700); err != nil {
		t.Fatal(err)
	}
	second := copyBlobAttempt(f.requests).Reservation
	second.MessageID = uuid.NewString()
	second.OperationID = uuid.NewString()
	second.Blob.BlobId = uuid.NewString()
	if _, err := f.repo.Reserve(t.Context(), f.uid, second); err != nil {
		t.Fatal(err)
	}
	expireServiceAttempt(t, f)
	events := startCollector(t, f.upload)
	event := awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Failed > 0 })
	if !errors.Is(event.err, blobstore.ErrRoot) {
		t.Fatal("missing filesystem failure", event.err)
	}
	awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Reclaimed == 1 })
	f.assertState(t, "deleting") // Exactly the faulty object's 64 bytes remain charged.
	if err := os.Remove(name); err != nil {
		t.Fatal(err)
	} // Explicit fixture repair, not production auto-repair.
	awaitCollector(t, events, func(e collectorObservation) bool { return e.sweep.Reclaimed == 1 })
	var state string
	var quota int64
	if err := f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, f.requests.AttemptID).Scan(&state); err != nil || state != "reclaimed" {
		t.Fatal(state, err)
	}
	if err := f.db.QueryRowContext(t.Context(), `SELECT reserved_bytes FROM e2ee_blob_global_quota`).Scan(&quota); err != nil || quota != 0 {
		t.Fatal(quota, err)
	}
}
