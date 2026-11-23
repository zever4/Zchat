//go:build linux

package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

func acceptedReclaimRequest(w *domain.E2EEBlobWriteRequest) *domain.E2EEAcceptedBlobCleanupRequest {
	return &domain.E2EEAcceptedBlobCleanupRequest{StorageID: w.StorageID, MessageID: w.Attempt.Reservation.MessageID, AttemptID: w.Attempt.AttemptID, Revision: w.Attempt.Revision}
}
func assertAcceptedReclaimState(t *testing.T, f *blobReservationFixture, id, want string) {
	t.Helper()
	var state string
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, id).Scan(&state); err != nil || state != want {
		t.Fatal("accepted reclaim state", state, want, err)
	}
}

func TestAcceptedBlobReclaimAckAdmissionProofReplayPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	req := acceptedReclaimRequest(w)
	denied := func() {
		t.Helper()
		if got, err := f.blobs.ClaimAcceptedCleanup(t.Context(), req); got != nil || !errors.Is(err, domain.ErrE2EEBlobCleanup) {
			t.Fatal("unaccepted/live file deletion admitted", got, err)
		}
	}
	denied() // Reserved is not accepted.
	readyAttachment(t, f, w, files, data)
	denied() // Ready is not accepted.
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	denied() // Receipt alone starts retention, not immediate deletion.
	for _, reason := range []string{"expired", blobFileAcknowledged, blobAcceptedExpired} {
		if _, err := f.f.db.ExecContext(t.Context(), `UPDATE e2ee_blob_attempts SET state='deleting',deletion_reason=$2 WHERE attempt_id=$1`, req.AttemptID, reason); err == nil {
			t.Fatal("SQL guard admitted live accepted deletion", reason)
		}
	}
	read := readAttachmentRequest(t, w.Attempt.Reservation)
	if err := f.delivery.AcknowledgeV2(t.Context(), 2, &pb.AcknowledgeMessagesRequest{Context: read.Context, Messages: []*pb.Acknowledgement{{MessageId: req.MessageID, EnvelopeDigest: read.Binding.EnvelopeDigest, Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}); err != nil {
		t.Fatal(err)
	}
	denied() // Descriptor ACK is not FILEACK.
	ack, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation))
	if err != nil {
		t.Fatal(err)
	}
	// Accepted old-epoch data remains collectible after sealing/replacement.
	f.transition(t)
	f.f.replace(t, 2, f.gen(2))
	bad := *req
	bad.StorageID = uuid.NewString()
	if got, err := f.blobs.ClaimAcceptedCleanup(t.Context(), &bad); got != nil || !errors.Is(err, domain.ErrE2EEBlobStorage) {
		t.Fatal("foreign root admitted", got, err)
	}
	bad = *req
	bad.Revision++
	if got, err := f.blobs.ClaimAcceptedCleanup(t.Context(), &bad); got != nil || err == nil {
		t.Fatal("wrong accepted revision admitted", got, err)
	}
	for _, reason := range []domain.E2EEBlobCleanupReason{domain.E2EEBlobCleanupExpired, domain.E2EEBlobCleanupFailedWriter, domain.E2EEBlobCleanupInactiveScope} {
		if got, err := f.blobs.ClaimCleanup(t.Context(), cleanupRequest(w, reason)); got != nil || !errors.Is(err, domain.ErrE2EEBlobCleanup) {
			t.Fatal("staging API admitted accepted deletion", got, err)
		}
	}
	claim, err := f.blobs.ClaimAcceptedCleanup(t.Context(), req)
	if err != nil || claim.State != "deleting" || claim.Reason != blobFileAcknowledged {
		t.Fatal(claim, err)
	}
	f.quota(t, 64, 1, 1) // Admission does not release bytes or unlink.
	if err = files.Verify(t.Context(), blobstore.Attempt{ID: req.AttemptID, Bytes: int64(len(data)), SHA256: [32]byte(w.Attempt.Reservation.Blob.Sha256)}); err != nil {
		t.Fatal("metadata admission removed ciphertext", err)
	}
	if got, err := f.blobs.CompleteAcceptedReclaim(t.Context(), req, nil); got != nil || err == nil {
		t.Fatal("quota released without physical proof", got, err)
	}
	otherRoot := t.TempDir()
	if err = os.Chmod(otherRoot, 0700); err != nil {
		t.Fatal(err)
	}
	other, err := blobstore.Open(otherRoot, blobstore.Config{MaxConcurrent: 1, HeadroomBytes: 1, WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	foreign, err := other.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer foreign.Close()
	if got, err := f.blobs.CompleteAcceptedReclaim(t.Context(), req, foreign); got != nil || err == nil {
		t.Fatal("foreign absence proof released quota", got, err)
	}
	f.quota(t, 64, 1, 1)
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if got, err := f.blobs.CompleteReclaim(t.Context(), cleanupRequest(w, domain.E2EEBlobCleanupExpired), proof); got != nil || !errors.Is(err, domain.ErrE2EEBlobCleanup) {
		t.Fatal("staging completion accepted a selected file", got, err)
	}
	finished, err := f.blobs.CompleteAcceptedReclaim(t.Context(), req, proof)
	if err != nil || finished.State != "reclaimed" || finished.ReclaimedAt == nil {
		t.Fatal(finished, err)
	}
	for range 3 {
		again, err := f.blobs.CompleteAcceptedReclaim(t.Context(), req, proof)
		if err != nil || !again.ReclaimedAt.Equal(*finished.ReclaimedAt) || again.Reason != blobFileAcknowledged {
			t.Fatal(again, err)
		}
		f.quota(t, 0, 1, 1)
	}
	if saved, err := f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(w.Attempt.Reservation)); err != nil || !proto.Equal(saved, receipt) {
		t.Fatal("reclaim changed receipt/re-enqueued Send", saved, err)
	}
	status, err := f.blobs.FileStatuses(t.Context(), 1, fileStatusRequest(t, w.Attempt.Reservation))
	if err != nil || status.Files[0].GetAcknowledgedAtUnixMs() != ack.Files[0].AcknowledgedAtUnixMs || !proto.Equal(status.Files[0].Receipt, receipt) {
		t.Fatal(status, err)
	}
	registryCount(t, f.f.db, "e2ee_message_blobs", 1)
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 1)
	registryCount(t, f.f.db, "e2ee_message_inbox", 0)
}

// Disposable SQL clock, including triggers, on one owned connection. No
// immutable receipt/lease date or production pool/configuration is modified.
func acceptedReclaimClock(t *testing.T, db *sql.DB, at time.Time) func(time.Time) {
	t.Helper()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	var schema string
	if err := db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `SET search_path=`+pq.QuoteIdentifier(schema)+`,pg_catalog`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `CREATE TABLE accepted_reclaim_clock(at timestamptz NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO accepted_reclaim_clock VALUES($1)`, at); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `CREATE FUNCTION clock_timestamp() RETURNS timestamptz LANGUAGE sql AS 'SELECT at FROM accepted_reclaim_clock'; ALTER TABLE e2ee_file_acknowledgements ALTER COLUMN acknowledged_at SET DEFAULT date_trunc('milliseconds',clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	return func(at time.Time) {
		t.Helper()
		if _, err := db.ExecContext(t.Context(), `UPDATE accepted_reclaim_clock SET at=$1`, at); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAcceptedBlobReclaimExpiryBoundaryAndLateAckPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	req := acceptedReclaimRequest(w)
	deadline := time.UnixMilli(receipt.ExpiresAtUnixMs)
	clock := acceptedReclaimClock(t, f.f.db, deadline.Add(-time.Millisecond))
	if got, err := f.blobs.ClaimAcceptedCleanup(t.Context(), req); got != nil || !errors.Is(err, domain.ErrE2EEBlobCleanup) {
		t.Fatal("early accepted expiry", got, err)
	}
	if _, err := f.f.db.ExecContext(t.Context(), `UPDATE e2ee_blob_attempts SET state='deleting',deletion_reason='accepted_expired' WHERE attempt_id=$1`, req.AttemptID); err == nil {
		t.Fatal("SQL accepted early expiry")
	}
	clock(deadline)
	claim, err := f.blobs.ClaimAcceptedCleanup(t.Context(), req)
	if err != nil || claim.State != "deleting" || claim.Reason != blobAcceptedExpired {
		t.Fatal(claim, err)
	}
	f.quota(t, 64, 1, 1)
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if _, err = f.blobs.CompleteAcceptedReclaim(t.Context(), req, proof); err != nil {
		t.Fatal(err)
	}
	clock(deadline.Add(time.Millisecond))
	ack, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation))
	if err != nil || ack.Files[0].AcknowledgedAtUnixMs <= receipt.ExpiresAtUnixMs {
		t.Fatal("late ACK after physical deletion", ack, err)
	}
	status, err := f.blobs.FileStatuses(t.Context(), 1, fileStatusRequest(t, w.Attempt.Reservation))
	if err != nil || status.Files[0].ObservedAtUnixMs <= receipt.ExpiresAtUnixMs || status.Files[0].GetAcknowledgedAtUnixMs() != ack.Files[0].AcknowledgedAtUnixMs || !proto.Equal(status.Files[0].Receipt, receipt) {
		t.Fatal(status, err)
	}
	again, err := f.blobs.ClaimAcceptedCleanup(t.Context(), req)
	if err != nil || again.Reason != blobAcceptedExpired || again.State != "reclaimed" {
		t.Fatal("late ACK changed removal fact", again, err)
	}
	f.quota(t, 0, 1, 1)
}

func TestAcceptedBlobReclaimCommitFaultsAndLostRepliesPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	if _, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation)); err != nil {
		t.Fatal(err)
	}
	req := acceptedReclaimRequest(w)
	if _, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION accepted_reclaim_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic accepted COMMIT fault'; END $$`); err != nil {
		t.Fatal(err)
	}
	setFault := func(on bool) {
		t.Helper()
		q := `DROP TRIGGER accepted_reclaim_fault ON e2ee_blob_attempts`
		if on {
			q = `CREATE CONSTRAINT TRIGGER accepted_reclaim_fault AFTER UPDATE ON e2ee_blob_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION accepted_reclaim_fault()`
		}
		if _, err := f.f.db.ExecContext(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
	setFault(true)
	if got, err := f.blobs.ClaimAcceptedCleanup(t.Context(), req); got != nil || err == nil {
		t.Fatal("failed admission COMMIT granted unlink", got, err)
	}
	assertAcceptedReclaimState(t, f, req.AttemptID, "linked")
	f.quota(t, 64, 1, 1)
	setFault(false)
	fault := new(blobReadFault)
	lost := NewPgE2EEBlobRepository(blobReadPool(t, f.f.db, fault))
	fault.failCommit.Store(true)
	if got, err := lost.ClaimAcceptedCleanup(t.Context(), req); got != nil || !errors.Is(err, errBlobReadCommit) {
		t.Fatal("unknown admission COMMIT granted unlink", got, err)
	}
	assertAcceptedReclaimState(t, f, req.AttemptID, "deleting")
	if got, err := f.blobs.ClaimAcceptedCleanup(t.Context(), req); err != nil || got.State != "deleting" {
		t.Fatal(got, err)
	}
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	setFault(true)
	if got, err := f.blobs.CompleteAcceptedReclaim(t.Context(), req, proof); got != nil || err == nil {
		t.Fatal("failed final COMMIT released quota", got, err)
	}
	assertAcceptedReclaimState(t, f, req.AttemptID, "deleting")
	f.quota(t, 64, 1, 1)
	setFault(false)
	fault.failCommit.Store(true)
	if got, err := lost.CompleteAcceptedReclaim(t.Context(), req, proof); got != nil || !errors.Is(err, errBlobReadCommit) {
		t.Fatal("unknown final COMMIT returned receipt", got, err)
	}
	assertAcceptedReclaimState(t, f, req.AttemptID, "reclaimed")
	for range 2 {
		if got, err := f.blobs.CompleteAcceptedReclaim(t.Context(), req, proof); err != nil || got.State != "reclaimed" {
			t.Fatal(got, err)
		}
		f.quota(t, 0, 1, 1)
	}
}

func TestAcceptedBlobReclaimMigrationPreservesFactsAndGuardsDownPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 33); err != nil {
		t.Fatal(err)
	}
	f, w, files, data := blobWriterOnFixture(t, blobReservationsOnFixture(t, epochPersistenceOnDB(t, db), nil))
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	ack, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation))
	if err != nil {
		t.Fatal(err)
	}
	if err = goose.UpTo(db, "../../migrations", 34); err != nil {
		t.Fatal(err)
	}
	if err = goose.DownTo(db, "../../migrations", 33); err != nil {
		t.Fatal(err)
	}
	if err = goose.UpTo(db, "../../migrations", 34); err != nil {
		t.Fatal(err)
	}
	req := acceptedReclaimRequest(w)
	if _, err = f.blobs.ClaimAcceptedCleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	if err = goose.DownTo(db, "../../migrations", 33); err == nil || !strings.Contains(err.Error(), "cannot discard accepted blob deletion evidence") {
		t.Fatal("rollback discarded admission", err)
	}
	version, err := goose.GetDBVersion(db)
	if err != nil || version != 34 {
		t.Fatal(version, err)
	}
	status, err := f.blobs.FileStatuses(t.Context(), 1, fileStatusRequest(t, w.Attempt.Reservation))
	if err != nil || !proto.Equal(status.Files[0].Receipt, receipt) || status.Files[0].GetAcknowledgedAtUnixMs() != ack.Files[0].AcknowledgedAtUnixMs {
		t.Fatal("migration changed facts", status, err)
	}
	f.quota(t, 64, 1, 1)
}

func TestAcceptedBlobReclaimExactAttemptAndConcurrentReplayPostgres(t *testing.T) {
	f, old, files, data := blobWriterFixture(t)
	readyAttachment(t, f, old, files, data)
	f.age(t, f.reserve(t, 1, old.Attempt.Reservation))
	next := cloneBlobReservation(old.Attempt.Reservation)
	next.OperationID = uuid.NewString()
	next.ExpectedRevision, next.ExpectedAttemptID = old.Attempt.Revision, old.Attempt.AttemptID
	g := f.reserve(t, 1, next)
	w := &domain.E2EEBlobWriteRequest{Attempt: &domain.E2EEBlobAttemptRequest{Reservation: next, Revision: g.Revision, AttemptID: g.AttemptID}, StorageID: old.StorageID, WriterID: uuid.NewString()}
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, next)
	if _, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, next)); err != nil {
		t.Fatal(err)
	}
	if got, err := f.blobs.ClaimAcceptedCleanup(t.Context(), acceptedReclaimRequest(old)); got != nil || !errors.Is(err, domain.ErrE2EEBlobCleanup) {
		t.Fatal("selected an old unaccepted attempt", got, err)
	}
	req := acceptedReclaimRequest(w)
	var first *domain.E2EEBlobCleanup
	parallel := func(fn func(context.Context) (*domain.E2EEBlobCleanup, error), state string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		errs := make(chan error, 6)
		facts := make(chan *domain.E2EEBlobCleanup, 6)
		for range 6 {
			wg.Go(func() {
				got, err := fn(ctx)
				if err == nil && (got == nil || got.State != state || got.Reason != blobFileAcknowledged) {
					err = fmt.Errorf("unexpected concurrent state: %+v", got)
				}
				errs <- err
				facts <- got
			})
		}
		wg.Wait()
		close(errs)
		close(facts)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		for fact := range facts {
			if state == "reclaimed" {
				if first == nil {
					first = fact
				}
				if fact.ReclaimedAt == nil || !fact.ReclaimedAt.Equal(*first.ReclaimedAt) {
					t.Fatal("concurrent replay changed reclaim fact")
				}
			}
		}
	}
	parallel(func(ctx context.Context) (*domain.E2EEBlobCleanup, error) {
		return f.blobs.ClaimAcceptedCleanup(ctx, req)
	}, "deleting")
	f.quota(t, 128, 1, 2)
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	parallel(func(ctx context.Context) (*domain.E2EEBlobCleanup, error) {
		return f.blobs.CompleteAcceptedReclaim(ctx, req, proof)
	}, "reclaimed")
	f.quota(t, 64, 1, 2) // Only the accepted attempt's bytes, never its predecessor.
	if err = files.Verify(t.Context(), blobstore.Attempt{ID: old.Attempt.AttemptID, Bytes: int64(len(data)), SHA256: [32]byte(old.Attempt.Reservation.Blob.Sha256)}); err != nil {
		t.Fatal("reclaim erased the predecessor", err)
	}
	page, err := f.blobs.Inventory(t.Context(), w.StorageID, "", 128)
	if err != nil || len(page) != 2 {
		t.Fatal(page, err)
	}
	for _, item := range page {
		if item.Accepted != (item.Cleanup.AttemptID == req.AttemptID) {
			t.Fatal("inventory selected the message instead of exact attempt", item)
		}
	}
	// The accepted receipt must not strand its earlier unselected ciphertext.
	staging := cleanupRequest(old, domain.E2EEBlobCleanupExpired)
	if got, err := f.blobs.ClaimCleanup(t.Context(), staging); err != nil || got.State != "deleting" {
		t.Fatal(got, err)
	}
	oldProof, err := files.RemoveVerified(t.Context(), old.Attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer oldProof.Close()
	if _, err = f.blobs.CompleteReclaim(t.Context(), staging, oldProof); err != nil {
		t.Fatal(err)
	}
	f.quota(t, 0, 1, 2)
}
