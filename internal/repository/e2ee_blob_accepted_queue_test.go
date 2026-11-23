//go:build linux

package repository

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
)

func acceptedQueueCount(t *testing.T, f *blobReservationFixture, want int) {
	t.Helper()
	var n int
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_accepted_blob_queue`).Scan(&n); err != nil || n != want {
		t.Fatal("active accepted queue", n, want, err)
	}
}

func TestAcceptedBlobQueueProjectionGuardsAndRollbackPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptedQueueCount(t, f, 0)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	acceptedQueueCount(t, f, 1)
	var due time.Time
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT due_at FROM e2ee_accepted_blob_queue`).Scan(&due); err != nil || due.UnixMilli() != receipt.ExpiresAtUnixMs {
		t.Fatal("queue used staging TTL", due, err)
	}
	for _, query := range []string{
		`DELETE FROM e2ee_accepted_blob_queue`,
		`UPDATE e2ee_accepted_blob_queue SET due_at=due_at-INTERVAL '1 second'`,
		`UPDATE e2ee_accepted_blob_queue SET storage_id='00000000-0000-4000-8000-000000000001'`,
	} {
		if _, err := f.f.db.ExecContext(t.Context(), query); err == nil {
			t.Fatal("queue guard accepted mutation", query)
		}
	}
	// A failure at FILEACK COMMIT must roll back BOTH the fact and due projection.
	if _, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_queue_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic queue ACK COMMIT failure'; END $$;
        CREATE CONSTRAINT TRIGGER reject_queue_ack AFTER INSERT ON e2ee_file_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_queue_ack()`); err != nil {
		t.Fatal(err)
	}
	if ack, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation)); ack != nil || err == nil {
		t.Fatal("failed FILEACK commit returned receipt", ack, err)
	}
	var unchanged time.Time
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT due_at FROM e2ee_accepted_blob_queue`).Scan(&unchanged); err != nil || !unchanged.Equal(due) {
		t.Fatal("rolled back ACK leaked queue change", unchanged, due, err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 0)
	if _, err := f.f.db.ExecContext(t.Context(), `DROP TRIGGER reject_queue_ack ON e2ee_file_acknowledgements`); err != nil {
		t.Fatal(err)
	}
	ack, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation))
	if err != nil {
		t.Fatal(err)
	}
	page, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), domain.E2EEBlobCleanupAccepted, nil, 1)
	if err != nil || len(page) != 1 || page[0].AcceptedRequest == nil || page[0].Request != (domain.E2EEBlobCleanupRequest{}) || page[0].Position.ExpiresAt.UnixMilli() != ack.Files[0].AcknowledgedAtUnixMs {
		t.Fatal("FILEACK did not schedule exact typed accepted candidate", page, err)
	}
	req := acceptedReclaimRequest(w)
	if _, err = f.blobs.ClaimAcceptedCleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
	acceptedQueueCount(t, f, 1)
	staging, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), domain.E2EEBlobCleanupUnfinished, nil, 128)
	if err != nil || len(staging) != 0 {
		t.Fatal("accepted Deleting leaked into staging lane", staging, err)
	}
	proof, err := files.RemoveVerified(t.Context(), req.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if _, err = f.f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_queue_reclaim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic queue reclaim COMMIT failure'; END $$;
        CREATE CONSTRAINT TRIGGER reject_queue_reclaim AFTER UPDATE ON e2ee_blob_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN(NEW.state='reclaimed') EXECUTE FUNCTION reject_queue_reclaim()`); err != nil {
		t.Fatal(err)
	}
	if got, err := f.blobs.CompleteAcceptedReclaim(t.Context(), req, proof); got != nil || err == nil {
		t.Fatal("failed finalization returned receipt", got, err)
	}
	acceptedQueueCount(t, f, 1)
	f.quota(t, 64, 1, 1)
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER reject_queue_reclaim ON e2ee_blob_attempts`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.CompleteAcceptedReclaim(t.Context(), req, proof); err != nil {
		t.Fatal(err)
	}
	acceptedQueueCount(t, f, 0)
	f.quota(t, 0, 1, 1)
	if _, err = f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation)); err != nil {
		t.Fatal("ACK replay after GC", err)
	}
	if _, err = f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(w.Attempt.Reservation)); err != nil {
		t.Fatal("Send replay after GC", err)
	}
	acceptedQueueCount(t, f, 0)
	registryCount(t, f.f.db, "e2ee_send_receipts", 1)
	registryCount(t, f.f.db, "e2ee_message_blobs", 1)
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 1)
}

func TestAcceptedBlobQueueExpiryCursorAndIndexPostgres(t *testing.T) {
	f, first, files, data := blobWriterFixture(t)
	readyAttachment(t, f, first, files, data)
	receipt := acceptAttachment(t, f, first.Attempt.Reservation)
	deadline := time.UnixMilli(receipt.ExpiresAtUnixMs)
	clock := acceptedReclaimClock(t, f.f.db, deadline.Add(-time.Millisecond))
	if _, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION statement_timestamp() RETURNS timestamptz LANGUAGE sql STABLE AS 'SELECT at FROM accepted_reclaim_clock'`); err != nil {
		t.Fatal(err)
	}
	if page, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), domain.E2EEBlobCleanupAccepted, nil, 1); err != nil || len(page) != 0 {
		t.Fatal("early expiry discovery", page, err)
	}
	clock(deadline)
	page, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), domain.E2EEBlobCleanupAccepted, nil, 1)
	if err != nil || len(page) != 1 || page[0].AcceptedRequest.AttemptID != first.Attempt.AttemptID || !page[0].Position.ExpiresAt.Equal(deadline) {
		t.Fatal("exclusive receipt deadline", page, err)
	}
	if page, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), domain.E2EEBlobCleanupAccepted, &page[0].Position, 1); err != nil || len(page) != 0 {
		t.Fatal("cursor repeated candidate", page, err)
	}
	// Actual production query; forcing seqscan off demonstrates index eligibility
	// on this tiny fixture, not capacity or the optimizer choice on a large VPS.
	tx, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(t.Context(), `SET LOCAL enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	var plan string
	if err = tx.QueryRowContext(t.Context(), "EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) "+blobAcceptedCandidates, files.StorageID(), deadline.Add(-time.Hour), "00000000-0000-0000-0000-000000000000", 1).Scan(&plan); err != nil || !strings.Contains(plan, "e2ee_accepted_blob_due") || strings.Contains(plan, "Seq Scan") {
		t.Fatal("accepted cursor cannot use bounded index", plan, err)
	}
	t.Log("accepted candidate EXPLAIN ANALYZE:", plan)
}

func TestAcceptedBlobQueueMigrationBackfillRoundTripPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 34); err != nil {
		t.Fatal(err)
	}
	f, w, files, data := blobWriterOnFixture(t, blobReservationsOnFixture(t, epochPersistenceOnDB(t, db), nil))
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	if _, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.blobs.ClaimAcceptedCleanup(t.Context(), acceptedReclaimRequest(w)); err != nil {
		t.Fatal(err)
	}
	// A second account's live accepted file is backfilled but not yet due.
	r := f.request(2)
	r.Blob.Sha256 = w.Attempt.Reservation.Blob.Sha256
	g := f.reserve(t, 2, r)
	other := &domain.E2EEBlobWriteRequest{Attempt: &domain.E2EEBlobAttemptRequest{Reservation: r, AttemptID: g.AttemptID, Revision: g.Revision}, StorageID: files.StorageID(), WriterID: uuid.NewString()}
	if _, err := f.blobs.BeginWrite(t.Context(), 2, other); err != nil {
		t.Fatal(err)
	}
	if _, err := f.blobs.CompleteWrite(t.Context(), 2, other, writeBlobProof(t, files, other, data)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.delivery.SendV2(t.Context(), 2, attachmentSendRequest(r)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := goose.UpTo(db, "../../migrations", 35); err != nil {
			t.Fatal(err)
		}
		acceptedQueueCount(t, f, 2)
		page, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), domain.E2EEBlobCleanupAccepted, nil, 128)
		if err != nil || len(page) != 1 || page[0].AcceptedRequest.AttemptID != w.Attempt.AttemptID {
			t.Fatal("backfill lost Deleting or admitted live copy", page, err)
		}
		if err := goose.DownTo(db, "../../migrations", 34); err != nil {
			t.Fatal(err)
		}
		registryCount(t, db, "e2ee_send_receipts", 2)
		registryCount(t, db, "e2ee_file_acknowledgements", 1)
	}
}
