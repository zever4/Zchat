//go:build linux

package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
)

// Models only committed metadata outcomes; admission/receipt eligibility is
// tested separately against PostgreSQL. Files, pins and accounting are real.
type acceptedCleanupModel struct {
	*downloadRepositoryModel
	deleting, reclaimed atomic.Bool
	claims, completes   atomic.Int32
	claimHook           func(context.Context, *domain.E2EEBlobCleanup) (*domain.E2EEBlobCleanup, error)
	completeHook        func(context.Context) error
	at                  time.Time
}

func acceptedCleanupCore(t *testing.T) (*downloadFixture, *acceptedCleanupModel, *domain.E2EEAcceptedBlobCleanupRequest) {
	t.Helper()
	f := downloadCore(t)
	m := &acceptedCleanupModel{downloadRepositoryModel: f.m, at: time.Now()}
	f.s.repo = m
	r := &domain.E2EEAcceptedBlobCleanupRequest{StorageID: f.s.files.StorageID(), MessageID: f.r.Reservation.MessageID, AttemptID: f.r.AttemptID, Revision: f.r.Revision}
	return f, m, r
}
func (m *acceptedCleanupModel) ResolveRead(ctx context.Context, uid int64, storage string, req *domain.E2EEBlobReadRequest) (*domain.E2EEBlobReadTarget, error) {
	if m.deleting.Load() || m.reclaimed.Load() {
		return nil, domain.ErrE2EEBlobReadUnavailable
	}
	return m.downloadRepositoryModel.ResolveRead(ctx, uid, storage, req)
}
func (m *acceptedCleanupModel) result(r *domain.E2EEAcceptedBlobCleanupRequest) *domain.E2EEBlobCleanup {
	x := &domain.E2EEBlobCleanup{StorageID: r.StorageID, MessageID: r.MessageID, AttemptID: r.AttemptID, Revision: r.Revision, State: "deleting", Reason: domain.E2EEBlobReclaimFileAcknowledged}
	if m.reclaimed.Load() {
		x.State, x.ReclaimedAt = "reclaimed", &m.at
	}
	return x
}
func (m *acceptedCleanupModel) ClaimAcceptedCleanup(ctx context.Context, r *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	m.claims.Add(1)
	m.deleting.Store(true)
	x := m.result(r)
	if m.claimHook != nil {
		return m.claimHook(ctx, x)
	}
	return x, nil
}
func (m *acceptedCleanupModel) CompleteAcceptedReclaim(ctx context.Context, r *domain.E2EEAcceptedBlobCleanupRequest, proof *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	m.completes.Add(1)
	if m.completeHook != nil {
		if err := m.completeHook(ctx); err != nil {
			return nil, err
		}
	}
	if err := proof.Guard(r.StorageID, r.AttemptID, func() error { m.reclaimed.Store(true); return nil }); err != nil {
		return nil, err
	}
	return m.result(r), nil
}
func (*acceptedCleanupModel) ClaimCleanup(context.Context, *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	return nil, errBlobSynthetic
}
func (*acceptedCleanupModel) CompleteReclaim(context.Context, *domain.E2EEBlobCleanupRequest, *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	return nil, errBlobSynthetic
}

func TestBlobAcceptedCleanupJoinsAllReadersAndCancelledRetry(t *testing.T) {
	f, m, req := acceptedCleanupCore(t)
	before, _ := f.budget.Usage()
	a, b := heldSink(true), heldSink(false)
	defer b.unblock()
	doneA, doneB := make(chan error, 1), make(chan error, 1)
	go func() { doneA <- f.download(t.Context(), a) }()
	go func() { doneB <- f.download(t.Context(), b) }()
	awaitDownload(t, a.entered)
	awaitDownload(t, b.entered)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cleaned := make(chan error, 1)
	go func() { _, err := f.s.CleanupAccepted(ctx, req); cleaned <- err }()
	awaitDownload(t, a.closed)
	awaitDownload(t, b.closed)
	if err := awaitDownloadResult(t, doneA); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal(err)
	}
	// One sink ignores Close: cancelling it does not join its in-progress Write.
	if u, _ := f.budget.Usage(); u != before || !f.budget.Active() || m.completes.Load() != 0 {
		t.Fatal("released before all readers joined", u, before)
	}
	if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); err != nil {
		t.Fatal("unlinked a pinned file", err)
	}
	if _, err := f.s.CleanupAccepted(t.Context(), req); !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal("accepted cleanup did not own shared slot", err)
	}
	if _, err := f.s.Cleanup(t.Context(), &domain.E2EEBlobCleanupRequest{MessageID: req.MessageID, AttemptID: req.AttemptID, Revision: req.Revision, Reason: domain.E2EEBlobCleanupExpired}); !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal("staging cleanup escaped shared slot", err)
	}
	for _, head := range []bool{false, true} {
		sink := &downloadSink{}
		var err error
		if head {
			err = f.s.Head(t.Context(), "model-token", make([]byte, 32), f.req, sink)
		} else {
			err = f.download(t.Context(), sink)
		}
		if !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) || sink.writes.Load() != 0 {
			t.Fatal("new GET/HEAD opened after committed admission", head, err)
		}
	}
	cancel()
	if err := awaitDownloadResult(t, cleaned); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if u, _ := f.budget.Usage(); u != before || m.completes.Load() != 0 {
		t.Fatal("cancelled join released bytes", u)
	}
	go func() { _, err := f.s.CleanupAccepted(t.Context(), req); cleaned <- err }()
	b.unblock()
	if err := awaitDownloadResult(t, doneB); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal(err)
	}
	if err := awaitDownloadResult(t, cleaned); err != nil {
		t.Fatal(err)
	}
	after, _ := f.budget.Usage()
	if after.Bytes >= before.Bytes || after.Entries >= before.Entries || f.budget.Active() || f.guards.Load() != 0 || m.completes.Load() != 1 {
		t.Fatal("reclaim did not release exactly after join", before, after)
	}
	if _, err := f.s.CleanupAccepted(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if u, _ := f.budget.Usage(); u != after || m.completes.Load() != 1 {
		t.Fatal("replay released/finalized twice", u, after)
	}
}

func TestBlobAcceptedCleanupShutdownOwnsBothMetadataPhases(t *testing.T) {
	for _, phase := range []string{"claim", "complete"} {
		t.Run(phase, func(t *testing.T) {
			f, m, req := acceptedCleanupCore(t)
			before, _ := f.budget.Usage()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			block := func(ctx context.Context) error { close(entered); <-release; return ctx.Err() }
			if phase == "claim" {
				m.claimHook = func(ctx context.Context, x *domain.E2EEBlobCleanup) (*domain.E2EEBlobCleanup, error) {
					_ = block(ctx)
					return x, nil
				}
			} else {
				m.completeHook = block
			}
			done := make(chan error, 1)
			go func() { _, err := f.s.CleanupAccepted(t.Context(), req); done <- err }()
			awaitDownload(t, entered)
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			if err := f.s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("shutdown failed to join metadata phase", err)
			}
			other, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
			if other != nil {
				other.Close()
			}
			if !errors.Is(err, blobstore.ErrBusy) {
				t.Fatal("released owned root while callback still running", err)
			}
			u, _ := f.budget.Usage()
			if phase == "claim" && u != before || phase == "complete" && u.Bytes >= before.Bytes {
				t.Fatal("wrong unlink/accounting boundary", phase, u, before)
			}
			once.Do(func() { close(release) })
			if err = awaitDownloadResult(t, done); err == nil {
				t.Fatal("shutdown cancellation ignored")
			}
			if err = f.s.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBlobAcceptedCleanupJoinsReadResolvedBeforeAdmission(t *testing.T) {
	for _, head := range []bool{false, true} {
		name := "GET"
		if head {
			name = "HEAD"
		}
		t.Run(name, func(t *testing.T) {
			f, m, req := acceptedCleanupCore(t)
			before, _ := f.budget.Usage()
			entered, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			f.m.resolve = func(ctx context.Context, _ int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
				close(entered)
				<-ctx.Done()
				close(cancelled)
				<-release // An old metadata reply arrives after Deleting committed.
				return target, nil
			}
			sink := &downloadSink{}
			read := make(chan error, 1)
			go func() {
				if head {
					read <- f.s.Head(t.Context(), "model-token", make([]byte, 32), f.req, sink)
				} else {
					read <- f.download(t.Context(), sink)
				}
			}()
			awaitDownload(t, entered)
			cleaned := make(chan error, 1)
			go func() { _, err := f.s.CleanupAccepted(t.Context(), req); cleaned <- err }()
			awaitDownload(t, cancelled)
			if u, _ := f.budget.Usage(); u != before || m.completes.Load() != 0 {
				t.Fatal("cleanup escaped pending read registration", u, before)
			}
			if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); err != nil {
				t.Fatal("unlinked before pending read joined", err)
			}
			once.Do(func() { close(release) })
			if err := awaitDownloadResult(t, read); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) || sink.writes.Load() != 0 {
				t.Fatal("stale metadata reply opened file/sink", err)
			}
			if err := awaitDownloadResult(t, cleaned); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBlobAcceptedCleanupRejectsUnknownOrMalformedAdmission(t *testing.T) {
	for _, name := range []string{"unknown commit", "wrong binding", "staging cause", "missing reclaimed time", "foreign root"} {
		t.Run(name, func(t *testing.T) {
			f, m, req := acceptedCleanupCore(t)
			before, _ := f.budget.Usage()
			m.claimHook = func(_ context.Context, x *domain.E2EEBlobCleanup) (*domain.E2EEBlobCleanup, error) {
				switch name {
				case "unknown commit":
					return nil, errBlobSynthetic
				case "wrong binding":
					x.MessageID = uuid.NewString()
				case "staging cause":
					x.Reason = string(domain.E2EEBlobCleanupExpired)
				case "missing reclaimed time":
					x.State = "reclaimed"
				}
				return x, nil
			}
			if name == "foreign root" {
				req.StorageID = uuid.NewString()
			}
			if result, err := f.s.CleanupAccepted(t.Context(), req); result != nil || err == nil {
				t.Fatal("invalid admission accepted", result, err)
			}
			if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); err != nil {
				t.Fatal("unlinked without authority", err)
			}
			if u, _ := f.budget.Usage(); u != before || m.completes.Load() != 0 || name == "foreign root" && m.claims.Load() != 0 {
				t.Fatal("invalid admission changed accounting", u, before)
			}
		})
	}
}

type acceptedStartupModel struct {
	*startupRepositoryModel
	E2EEBlobReclaimer
	E2EEAcceptedBlobReclaimer
	mutations int
}

func (m *acceptedStartupModel) ClaimCleanup(context.Context, *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	m.mutations++
	return nil, errBlobSynthetic
}
func (m *acceptedStartupModel) ClaimAcceptedCleanup(context.Context, *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	m.mutations++
	return nil, errBlobSynthetic
}

func TestBlobAcceptedStartupValidatesWholePlanBeforeAnyMutation(t *testing.T) {
	for _, cause := range []string{"", "expired", domain.E2EEBlobReclaimFileAcknowledged, domain.E2EEBlobReclaimAcceptedExpired} {
		t.Run("cause="+cause, func(t *testing.T) {
			f := uploadCoreUnattached(t)
			f.s.ready = false
			root := f.s.files.StorageID()
			rows := []domain.E2EEBlobInventoryEntry{
				{Cleanup: domain.E2EEBlobCleanupRequest{StorageID: root, MessageID: uuid.NewString(), AttemptID: "00000000-0000-4000-8000-000000000001", Revision: 1}, State: "reserved", Bytes: 64, Expired: true},
				{Cleanup: domain.E2EEBlobCleanupRequest{StorageID: root, MessageID: uuid.NewString(), AttemptID: "00000000-0000-4000-8000-000000000002", Revision: 1}, State: "reclaimed", Bytes: 64, Accepted: true, WriterID: uuid.NewString(), BoundStorageID: root, DeletionReason: cause},
			}
			// Even a recognized accepted cause is invalid without reclaimed_at.
			m := &acceptedStartupModel{startupRepositoryModel: &startupRepositoryModel{E2EEBlobWriterRepository: f.m, rows: rows}}
			f.s.repo = m
			if _, err := f.s.Reconcile(t.Context(), startupTestConfig()); !errors.Is(err, domain.ErrE2EEBlobMismatch) || f.s.Ready() || m.mutations != 0 {
				t.Fatal("invalid suffix mutated an otherwise valid cleanup prefix", cause, err, m.mutations)
			}
		})
	}
}
