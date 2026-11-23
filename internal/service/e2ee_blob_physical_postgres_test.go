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

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type physicalSpySource struct {
	reads  int
	closed bool
}

func (s *physicalSpySource) Read([]byte) (int, error) { s.reads++; return 0, io.EOF }
func (s *physicalSpySource) Close() error             { s.closed = true; return nil }

func TestBlobPhysicalRestartImportsExactSenderOncePostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); err != nil {
		t.Fatal(err)
	}
	s := restartBlobOwner(t, f)
	if _, err := s.Reconcile(t.Context(), startupTestConfig()); err != nil {
		t.Fatal(err)
	}
	src := &physicalSpySource{}
	if _, err := s.Upload(t.Context(), f.token, f.credential, f.requests, src); !errors.Is(err, domain.ErrE2EEBlobNotReady) || src.reads != 0 || !src.closed {
		t.Fatal("FS Ready bypassed physical admission", src, err)
	}
	b := testPhysicalBudget(t)
	if err := s.AttachBudget(t.Context(), b, startupTestConfig()); err != nil {
		t.Fatal(err)
	}
	before, _ := b.Usage()
	if before.Bytes == 0 || before.Entries != 4 {
		t.Fatal("root metadata/file census", before)
	}
	if _, err := b.Reserve(t.Context(), f.uid, storagebudget.Usage{Bytes: 10_000_000_000, Entries: 1}); !errors.Is(err, storagebudget.ErrQuota) {
		t.Fatal("restart lost sender account charge", err)
	}
	other, err := b.Reserve(t.Context(), f.uid+100, storagebudget.Usage{Bytes: 10_000_000_000, Entries: 1})
	if err != nil {
		t.Fatal("other account inherited sender charge", err)
	}
	other.Settle(storagebudget.Usage{})
	if err = s.AttachBudget(t.Context(), b, startupTestConfig()); !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal("duplicate attach", err)
	}
	if u, _ := b.Usage(); u != before {
		t.Fatal("duplicate attach changed quota", u, before)
	}
	expireServiceAttempt(t, f)
	if _, err = s.Cleanup(t.Context(), serviceCleanupRequest(f, domain.E2EEBlobCleanupExpired)); err != nil {
		t.Fatal(err)
	}
	if u, _ := b.Usage(); u.Entries != 3 || u.Bytes >= before.Bytes {
		t.Fatal("restart cleanup not reflected in budget", u, before)
	}
}

func TestBlobPhysicalUnknownReadyCommitKeepsFileAndChargePostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	before, _ := f.budget.Usage()
	f.upload.repo = &lostReadyCollectorRepository{f.repo}
	if result, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); result != nil || !errors.Is(err, errBlobSynthetic) {
		t.Fatal(result, err)
	}
	f.assertState(t, "ready")
	if u, n := f.budget.Usage(); u.Bytes <= before.Bytes || u.Entries != before.Entries+1 || n != 0 {
		t.Fatal("unknown Ready lost retained charge/slot", u, n, before)
	}
	got, err := os.ReadFile(filepath.Join(f.path, f.requests.AttemptID+".blob"))
	if err != nil || !bytes.Equal(got, f.data) {
		t.Fatal("unknown Ready removed ciphertext", err)
	}
}

func TestBlobPhysicalQuotaRefusesBodyAndCleanupCannotFreeOtherReservationPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	held, err := f.budget.Reserve(t.Context(), f.uid, storagebudget.Usage{Bytes: 10_000_000_000, Entries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Settle(storagebudget.Usage{})
	before, _ := f.budget.Usage()
	src := &physicalSpySource{}
	if result, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, src); result != nil || !errors.Is(err, storagebudget.ErrQuota) || src.reads != 0 || !src.closed {
		t.Fatal("body consumed before shared admission", result, src, err)
	}
	f.assertState(t, "writing")
	if _, err = os.Stat(filepath.Join(f.path, f.requests.AttemptID+".part")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("created file without reservation", err)
	}
	req := serviceCleanupRequest(f, domain.E2EEBlobCleanupFailedWriter)
	if err = f.db.QueryRowContext(context.Background(), `SELECT writer_id::text FROM e2ee_blob_attempts WHERE attempt_id=$1`, req.AttemptID).Scan(&req.ExpectedWriterID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.upload.Cleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if u, n := f.budget.Usage(); u != before || n != 1 {
		t.Fatal("cleanup freed another writer's reservation", u, n, before)
	}
	assertReclaimed(t, f)
}
