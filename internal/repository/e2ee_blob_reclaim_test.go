//go:build linux

package repository

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
)

func cleanupRequest(r *domain.E2EEBlobWriteRequest, reason domain.E2EEBlobCleanupReason) *domain.E2EEBlobCleanupRequest {
	c := &domain.E2EEBlobCleanupRequest{StorageID: r.StorageID, MessageID: r.Attempt.Reservation.MessageID, AttemptID: r.Attempt.AttemptID, Revision: r.Attempt.Revision, Reason: reason}
	if reason == domain.E2EEBlobCleanupFailedWriter {
		c.ExpectedWriterID = r.WriterID
	}
	return c
}
func TestBlobReclaimLifecycleIdempotencyAndRenewedQuotaPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	w := writeBlobProof(t, files, r, data)
	req := cleanupRequest(r, domain.E2EEBlobCleanupFailedWriter)
	claimed, err := f.blobs.ClaimCleanup(t.Context(), req)
	if err != nil || claimed.State != "deleting" {
		t.Fatal(claimed, err)
	}
	f.quota(t, 64, 1, 1)
	if _, err = f.blobs.CompleteWrite(t.Context(), 1, r, w); !errors.Is(err, domain.ErrE2EEBlobWriteState) {
		t.Fatal("deleting file became Ready", err)
	}
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, nil); err == nil {
		t.Fatal("released without unlink proof")
	}
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	done, err := f.blobs.CompleteReclaim(t.Context(), req, proof)
	if err != nil || done.State != "reclaimed" || done.ReclaimedAt == nil {
		t.Fatal(done, err)
	}
	f.quota(t, 0, 1, 1)
	for range 3 {
		again, err := f.blobs.CompleteReclaim(t.Context(), req, proof)
		if err != nil || !again.ReclaimedAt.Equal(*done.ReclaimedAt) {
			t.Fatal("reclaim replay mutated fact", again, err)
		}
		f.quota(t, 0, 1, 1)
	}
	renew := cloneBlobReservation(r.Attempt.Reservation)
	renew.OperationID = uuid.NewString()
	renew.ExpectedRevision = r.Attempt.Revision
	renew.ExpectedAttemptID = r.Attempt.AttemptID
	f.reserve(t, 1, renew)
	f.quota(t, 64, 1, 2)
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); err != nil {
		t.Fatal(err)
	}
	f.quota(t, 64, 1, 2)
	if err = proof.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); err == nil {
		t.Fatal("accepted closed removal proof")
	}
	again, err := f.blobs.ClaimCleanup(t.Context(), req)
	if err != nil || again.State != "reclaimed" {
		t.Fatal(again, err)
	}
	f.quota(t, 64, 1, 2)
}
func TestBlobReclaimRootBindingAndLiveReadyProtectionPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	if err := f.blobs.BindStorage(t.Context(), r.StorageID); err != nil {
		t.Fatal(err)
	}
	if err := f.blobs.BindStorage(t.Context(), uuid.NewString()); !errors.Is(err, domain.ErrE2EEBlobStorage) {
		t.Fatal("rebound physical root", err)
	}
	lease, err := f.blobs.BeginWrite(t.Context(), 1, r)
	if err != nil {
		t.Fatal(err)
	}
	w := writeBlobProof(t, files, r, data)
	if _, err = f.blobs.CompleteWrite(t.Context(), 1, r, w); err != nil {
		t.Fatal(err)
	}
	for _, reason := range []domain.E2EEBlobCleanupReason{domain.E2EEBlobCleanupFailedWriter, domain.E2EEBlobCleanupExpired, domain.E2EEBlobCleanupInactiveScope} {
		if _, err = f.blobs.ClaimCleanup(t.Context(), cleanupRequest(r, reason)); !errors.Is(err, domain.ErrE2EEBlobCleanup) {
			t.Fatal("deleted live Ready/unknown-COMMIT file", reason, err)
		}
	}
	f.age(t, &lease.Grant)
	req := cleanupRequest(r, domain.E2EEBlobCleanupExpired)
	if _, err = f.blobs.ClaimCleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir()
	if err = os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	other, err := blobstore.Open(path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	proof, err := other.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); err == nil {
		t.Fatal("empty wrong directory freed quota")
	}
	f.quota(t, 64, 1, 1)
	bad := *req
	bad.StorageID = other.StorageID()
	if _, err = f.blobs.ClaimCleanup(t.Context(), &bad); !errors.Is(err, domain.ErrE2EEBlobStorage) {
		t.Fatal(err)
	}
	_ = files
}
func TestBlobReclaimLinkedAndOldScopesPostgres(t *testing.T) {
	for _, name := range []string{"linked", "closed", "sealed", "reserved"} {
		t.Run(name, func(t *testing.T) {
			f, r, files, data := blobWriterFixture(t)
			if name != "reserved" {
				if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
					t.Fatal(err)
				}
				w := writeBlobProof(t, files, r, data)
				if _, err := f.blobs.CompleteWrite(t.Context(), 1, r, w); err != nil {
					t.Fatal(err)
				}
			}
			reason := domain.E2EEBlobCleanupInactiveScope
			switch name {
			case "linked":
				acceptAttachment(t, f, r.Attempt.Reservation)
				f.f.replace(t, 2, f.gen(2))
				if _, err := f.blobs.ClaimCleanup(t.Context(), cleanupRequest(r, reason)); !errors.Is(err, domain.ErrE2EEBlobCleanup) {
					t.Fatal("linked file erased", err)
				}
				f.quota(t, 64, 1, 1)
				return
			case "closed":
				f.f.replace(t, 2, f.gen(2))
			case "sealed":
				f.transition(t)
			case "reserved":
				g := f.reserve(t, 1, r.Attempt.Reservation)
				f.age(t, g)
				reason = domain.E2EEBlobCleanupExpired
			}
			req := cleanupRequest(r, reason)
			if _, err := f.blobs.ClaimCleanup(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
			if err != nil {
				t.Fatal(err)
			}
			defer proof.Close()
			if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); err != nil {
				t.Fatal(err)
			}
			f.quota(t, 0, 1, 1)
		})
	}
}
func TestBlobReclaimDeferredCommitFaultsPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	writeBlobProof(t, files, r, data)
	req := cleanupRequest(r, domain.E2EEBlobCleanupFailedWriter)
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION reclaim_fault()RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic reclaim commit fault';END $$;
        CREATE CONSTRAINT TRIGGER reclaim_fault AFTER UPDATE ON e2ee_blob_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reclaim_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.blobs.ClaimCleanup(t.Context(), req); got != nil || err == nil {
		t.Fatal("failed Claim commit granted unlink", got, err)
	}
	var state string
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts`).Scan(&state); err != nil || state != "writing" {
		t.Fatal(state, err)
	}
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER reclaim_fault ON e2ee_blob_attempts`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.ClaimCleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if _, err = f.f.db.ExecContext(t.Context(), `CREATE CONSTRAINT TRIGGER reclaim_fault AFTER UPDATE ON e2ee_blob_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reclaim_fault()`); err != nil {
		t.Fatal(err)
	}
	if got, err := f.blobs.CompleteReclaim(t.Context(), req, proof); got != nil || err == nil {
		t.Fatal("failed reclaim commit released quota", got, err)
	}
	f.quota(t, 64, 1, 1)
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER reclaim_fault ON e2ee_blob_attempts;DROP FUNCTION reclaim_fault()`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); err != nil {
		t.Fatal(err)
	}
	f.quota(t, 0, 1, 1)
}
func TestBlobReclaimConcurrentRenewalAndCounterCorruptionPostgres(t *testing.T) {
	f, r, files, _ := blobWriterFixture(t)
	g := f.reserve(t, 1, r.Attempt.Reservation)
	f.age(t, g)
	req := cleanupRequest(r, domain.E2EEBlobCleanupExpired)
	renew := cloneBlobReservation(r.Attempt.Reservation)
	renew.OperationID = uuid.NewString()
	renew.ExpectedRevision = g.Revision
	renew.ExpectedAttemptID = g.AttemptID
	var wg sync.WaitGroup
	var e1, e2 error
	wg.Go(func() { _, e1 = f.blobs.ClaimCleanup(t.Context(), req) })
	wg.Go(func() { _, e2 = f.blobs.Reserve(t.Context(), 1, renew) })
	wg.Wait()
	if e1 != nil || e2 != nil {
		t.Fatal(e1, e2)
	}
	f.quota(t, 128, 1, 2)
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if _, err = f.f.db.ExecContext(t.Context(), `UPDATE e2ee_blob_account_quota SET reserved_bytes=0`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); !errors.Is(err, domain.ErrE2EEMessageState) {
		t.Fatal("counter underflow accepted", err)
	}
	if _, err = f.f.db.ExecContext(t.Context(), `UPDATE e2ee_blob_account_quota SET reserved_bytes=128`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.CompleteReclaim(t.Context(), req, proof); err != nil {
		t.Fatal(err)
	}
	f.quota(t, 64, 1, 2)
}
func TestBlobReclaimMigrationGuardsPostgres(t *testing.T) {
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 30); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 30); err != nil {
		t.Fatal(err)
	}
	repo := NewPgE2EEBlobRepository(db)
	if err := repo.BindStorage(t.Context(), uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("Down lost root binding")
	}
	f, r, _, _ := blobWriterFixture(t)
	if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`UPDATE e2ee_blob_storage SET storage_id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'`, `DELETE FROM e2ee_blob_storage`, `UPDATE e2ee_blob_attempts SET storage_id=NULL`} {
		if _, err := f.f.db.ExecContext(t.Context(), q); err == nil {
			t.Fatal("rewrote root binding", q)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.blobs.ClaimCleanup(ctx, cleanupRequest(r, domain.E2EEBlobCleanupFailedWriter)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	file, err := os.ReadFile("../../migrations/030_e2ee_blob_reclamation.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.f.db.ExecContext(t.Context(), strings.SplitN(string(file), "-- +goose Down", 2)[1]); err == nil {
		t.Fatal("discarded bound writer evidence")
	}
}
