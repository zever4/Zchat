//go:build linux

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
)

func serviceCleanupRequest(f *postgresBlobUpload, reason domain.E2EEBlobCleanupReason) *domain.E2EEBlobCleanupRequest {
	return &domain.E2EEBlobCleanupRequest{MessageID: f.requests.Reservation.MessageID, AttemptID: f.requests.AttemptID, Revision: f.requests.Revision, Reason: reason}
}
func assertReclaimed(t *testing.T, f *postgresBlobUpload) {
	t.Helper()
	var state string
	var bytes, claims, attempts int64
	if err := f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, f.requests.AttemptID).Scan(&state); err != nil || state != "reclaimed" {
		t.Fatal(state, err)
	}
	if err := f.db.QueryRowContext(t.Context(), `SELECT reserved_bytes,retained_claims,retained_attempts FROM e2ee_blob_global_quota`).Scan(&bytes, &claims, &attempts); err != nil || bytes != 0 || claims != 1 || attempts != 1 {
		t.Fatal(bytes, claims, attempts, err)
	}
	for _, suffix := range []string{".part", ".blob"} {
		if _, err := os.Lstat(filepath.Join(f.path, f.requests.AttemptID+suffix)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("artifact not removed", err)
		}
	}
}
func TestBlobCleanupCancelsWriterBeforeQuotaReleasePostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	src := blockedUploadSource()
	done := make(chan error, 1)
	go func() { _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, src); done <- err }()
	awaitUploadRead(t, src)
	req := serviceCleanupRequest(f, domain.E2EEBlobCleanupFailedWriter)
	if err := f.db.QueryRowContext(t.Context(), `SELECT writer_id::text FROM e2ee_blob_attempts WHERE attempt_id=$1`, req.AttemptID).Scan(&req.ExpectedWriterID); err != nil {
		t.Fatal(err)
	}
	result, err := f.upload.Cleanup(t.Context(), req)
	if err != nil || result == nil || result.State != "reclaimed" {
		t.Fatal(result, err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, domain.ErrE2EEBlobWriteState) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not join producer")
	}
	assertReclaimed(t, f)
	again, err := f.upload.Cleanup(t.Context(), req)
	if err != nil || again.ReclaimedAt == nil || !again.ReclaimedAt.Equal(*result.ReclaimedAt) {
		t.Fatal("repeated cleanup changed fact", again, err)
	}
	assertReclaimed(t, f)
}

type cleanupOutcomeModel struct {
	E2EEBlobWriterRepository
	E2EEBlobReclaimer
	claimLost, reclaimLost bool
	claimHook, reclaimHook func(context.Context)
}

func (m *cleanupOutcomeModel) ClaimCleanup(ctx context.Context, r *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	if m.claimHook != nil {
		m.claimHook(ctx)
	}
	x, err := m.E2EEBlobReclaimer.ClaimCleanup(ctx, r)
	if err == nil && m.claimLost {
		return nil, errBlobSynthetic
	}
	return x, err
}
func (m *cleanupOutcomeModel) CompleteReclaim(ctx context.Context, r *domain.E2EEBlobCleanupRequest, p *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	if m.reclaimHook != nil {
		m.reclaimHook(ctx)
	}
	x, err := m.E2EEBlobReclaimer.CompleteReclaim(ctx, r, p)
	if err == nil && m.reclaimLost {
		return nil, errBlobSynthetic
	}
	return x, err
}
func expireServiceAttempt(t *testing.T, f *postgresBlobUpload) {
	t.Helper()
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt;
        UPDATE e2ee_blob_attempts SET reserved_at=reserved_at-INTERVAL '2 hours',expires_at=expires_at-INTERVAL '2 hours';
        ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
}
func TestBlobCleanupUnknownCommitsAreConservativePostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	baseline, _ := f.budget.Usage()
	if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); err != nil {
		t.Fatal(err)
	}
	retained, _ := f.budget.Usage()
	if retained.Bytes <= baseline.Bytes {
		t.Fatal("private file not charged", retained, baseline)
	}
	expireServiceAttempt(t, f)
	req := serviceCleanupRequest(f, domain.E2EEBlobCleanupExpired)
	model := &cleanupOutcomeModel{E2EEBlobWriterRepository: f.repo, E2EEBlobReclaimer: f.repo, claimLost: true}
	f.upload.repo = model
	if result, err := f.upload.Cleanup(t.Context(), req); result != nil || !errors.Is(err, errBlobSynthetic) {
		t.Fatal(result, err)
	}
	if _, err := os.Stat(filepath.Join(f.path, req.AttemptID+".blob")); err != nil {
		t.Fatal("unlinked after unknown Claim commit", err)
	}
	f.assertState(t, "deleting")
	if u, _ := f.budget.Usage(); u != retained {
		t.Fatal("freed bytes after unknown Claim COMMIT", u, retained)
	}
	model.claimLost = false
	model.reclaimLost = true
	if result, err := f.upload.Cleanup(t.Context(), req); result != nil || !errors.Is(err, errBlobSynthetic) {
		t.Fatal(result, err)
	}
	assertReclaimed(t, f)
	if u, _ := f.budget.Usage(); u != baseline {
		t.Fatal("physical bytes not freed after proven unlink", u, baseline)
	}
	model.reclaimLost = false
	if _, err := f.upload.Cleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	assertReclaimed(t, f)
	if u, _ := f.budget.Usage(); u != baseline {
		t.Fatal("repeated cleanup changed physical accounting", u, baseline)
	}
}

func TestBlobCleanupShutdownRetainsRootWhileReclaimingPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	expireServiceAttempt(t, f)
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	model := &cleanupOutcomeModel{E2EEBlobWriterRepository: f.repo, E2EEBlobReclaimer: f.repo, reclaimHook: func(ctx context.Context) {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
		}
	}}
	f.upload.repo = model
	done := make(chan error, 1)
	go func() {
		_, err := f.upload.Cleanup(t.Context(), serviceCleanupRequest(f, domain.E2EEBlobCleanupExpired))
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reclaim not reached")
	}
	other, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("root ownership dropped during reclaim", err)
	}
	if err = f.upload.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("cancelled cleanup returned success")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup failed to join")
	}
	f.assertState(t, "deleting")
}
