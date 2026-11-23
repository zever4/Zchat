//go:build linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/domain"
)

type acceptedCollectorModel struct {
	*acceptedCleanupModel
	rows []domain.E2EEBlobCleanupCandidate
}

func (m *acceptedCollectorModel) CleanupCandidates(_ context.Context, _ string, _ domain.E2EEBlobCleanupLane, after *domain.E2EEBlobCleanupPosition, limit int) ([]domain.E2EEBlobCleanupCandidate, error) {
	var page []domain.E2EEBlobCleanupCandidate
	for _, row := range m.rows {
		if after == nil || row.Position.AttemptID > after.AttemptID {
			page = append(page, row)
			if len(page) == limit {
				break
			}
		}
	}
	return page, nil
}
func acceptedCandidate(req *domain.E2EEAcceptedBlobCleanupRequest, at time.Time) domain.E2EEBlobCleanupCandidate {
	return domain.E2EEBlobCleanupCandidate{AcceptedRequest: req, Position: domain.E2EEBlobCleanupPosition{AttemptID: req.AttemptID, ExpiresAt: at}}
}
func TestAcceptedBlobCollectorPoisonedCandidateCannotStarveNext(t *testing.T) {
	f, m, req := acceptedCleanupCore(t)
	poison := *req
	poison.MessageID, poison.AttemptID = uuid.NewString(), "00000000-0000-4000-8000-000000000001"
	now := time.Now()
	scanner := &acceptedCollectorModel{acceptedCleanupModel: m, rows: []domain.E2EEBlobCleanupCandidate{acceptedCandidate(&poison, now), acceptedCandidate(req, now)}}
	m.claimHook = func(_ context.Context, x *domain.E2EEBlobCleanup) (*domain.E2EEBlobCleanup, error) {
		if x.AttemptID == poison.AttemptID {
			return nil, errBlobSynthetic
		}
		return x, nil
	}
	f.s.repo = scanner
	c := collectorTestConfig()
	c.Batch, c.MaxRetryEntries = 1, 1
	w := &blobCollector{s: f.s, repo: scanner, config: c, retry: make(map[string]blobCleanupRetry)}
	if r, err := w.step(t.Context(), domain.E2EEBlobCleanupAccepted, now); !errors.Is(err, errBlobSynthetic) || r.Failed != 1 {
		t.Fatal(r, err)
	}
	if r, err := w.step(t.Context(), domain.E2EEBlobCleanupAccepted, now); err != nil || r.Reclaimed != 1 || len(w.retry) != 1 {
		t.Fatal("poisoned row starved healthy successor", r, err)
	}
	if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("accepted lane never used typed coordinator", err)
	}
}
func TestAcceptedBlobCollectorRejectsWholeMalformedPage(t *testing.T) {
	for _, kind := range []string{"ambiguous", "foreign root", "missing binding", "duplicate cursor"} {
		t.Run(kind, func(t *testing.T) {
			f, m, req := acceptedCleanupCore(t)
			before, _ := f.budget.Usage()
			now := time.Now()
			other := *req
			other.AttemptID, other.MessageID = uuid.NewString(), uuid.NewString()
			bad := acceptedCandidate(&other, now.Add(time.Second))
			switch kind {
			case "ambiguous":
				bad.Request = domain.E2EEBlobCleanupRequest{MessageID: req.MessageID}
			case "foreign root":
				other.StorageID = uuid.NewString()
			case "missing binding":
				bad.AcceptedRequest = nil
			case "duplicate cursor":
				bad = acceptedCandidate(req, now)
			}
			scanner := &acceptedCollectorModel{acceptedCleanupModel: m, rows: []domain.E2EEBlobCleanupCandidate{acceptedCandidate(req, now), bad}}
			f.s.repo = scanner
			w := &blobCollector{s: f.s, repo: scanner, config: collectorTestConfig(), retry: make(map[string]blobCleanupRetry)}
			if _, err := w.step(t.Context(), domain.E2EEBlobCleanupAccepted, now); err == nil || m.claims.Load() != 0 {
				t.Fatal("mutated valid prefix of malformed accepted page", err)
			}
			if u, _ := f.budget.Usage(); u != before {
				t.Fatal("malformed page freed physical charge", u, before)
			}
		})
	}
}
func TestBlobCollectorRotatesAllThreeLanesAfterQueryFailure(t *testing.T) {
	f, m, _ := collectorModel(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var lanes []domain.E2EEBlobCleanupLane
	m.page = func(_ context.Context, lane domain.E2EEBlobCleanupLane, _ *domain.E2EEBlobCleanupPosition, _ int) ([]domain.E2EEBlobCleanupCandidate, error) {
		lanes = append(lanes, lane)
		if len(lanes) == int(domain.E2EEBlobCleanupLaneCount) {
			cancel()
		}
		if lane == domain.E2EEBlobCleanupUnfinished {
			return nil, errBlobSynthetic
		}
		return nil, nil
	}
	if err := f.s.RunCleanup(ctx, collectorTestConfig(), nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(lanes) != 3 || lanes[0] != domain.E2EEBlobCleanupUnfinished || lanes[1] != domain.E2EEBlobCleanupExpiredStaging || lanes[2] != domain.E2EEBlobCleanupAccepted {
		t.Fatal("one failed lane starved another", lanes)
	}
}
