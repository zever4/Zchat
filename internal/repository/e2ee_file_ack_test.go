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
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

// Repository/FS component checks with opaque synthetic Signal descriptors. A
// request here is NOT evidence of a native cache COMMIT or an implemented RPC.
func fileAckRequest(t *testing.T, r *domain.E2EEBlobReservationRequest) *v2.AcknowledgeFilesRequest {
	t.Helper()
	read := readAttachmentRequest(t, r)
	return &v2.AcknowledgeFilesRequest{Context: read.Context, Files: []*v2.FileAcknowledgement{{Binding: read.Binding, Blob: read.Blob}}}
}

func TestFileAckRequiresAcceptedExactIdentityPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	r := fileAckRequest(t, w.Attempt.Reservation)
	unavailable := func(uid int64, req *v2.AcknowledgeFilesRequest) {
		t.Helper()
		if got, err := f.blobs.AcknowledgeFiles(t.Context(), uid, req); got != nil || !errors.Is(err, domain.ErrE2EEFileAckUnavailable) {
			t.Fatal("unexpected FILEACK identity", got, err)
		}
	}
	unavailable(2, r) // Reservation alone is not acceptance.
	readyAttachment(t, f, w, files, data)
	unavailable(2, r) // Neither is successful upload/Ready.
	acceptAttachment(t, f, w.Attempt.Reservation)
	for name, mutate := range map[string]func(*v2.AcknowledgeFilesRequest){
		"message":           func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.MessageId = uuid.NewString() },
		"conversation":      func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.ConversationId = uuid.NewString() },
		"epoch":             func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.Epoch++ },
		"sender":            func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.Sender.UserId = uuid.NewString() },
		"sender generation": func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.Sender.GenerationId = uuid.NewString() },
		"recipient":         func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.Recipient.UserId = uuid.NewString() },
		"envelope digest":   func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.EnvelopeDigest[0] ^= 1 },
		"blob id":           func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob.BlobId = uuid.NewString() },
		"blob bytes":        func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob.CiphertextBytes++ },
		"blob hash":         func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob.Sha256[0] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(r).(*v2.AcknowledgeFilesRequest)
			mutate(bad)
			unavailable(2, bad)
		})
	}
	wrongOwner := proto.Clone(r).(*v2.AcknowledgeFilesRequest)
	wrongOwner.Context = f.context(1)
	wrongOwner.Files[0].Binding.Recipient.GenerationId = f.gen(1)
	wrongOwner.Files[0].Binding.Sender.GenerationId = uuid.NewString()
	unavailable(1, wrongOwner)
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 0)
	if got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r); err != nil || got == nil {
		t.Fatal("exact accepted FILEACK", got, err)
	}
}

func TestFileAckReplaySeparateFromDescriptorAndPhysicalCleanupPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	originalReceipt := acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileAckRequest(t, w.Attempt.Reservation)
	first, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r)
	if err != nil || boundary.ValidateFileAcknowledgementReceipts(r, first) != nil {
		t.Fatal("first FILEACK", first, err)
	}
	registryCount(t, f.f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.f.db, "e2ee_message_acknowledgements", 0)
	if target, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, readAttachmentRequest(t, w.Attempt.Reservation)); target != nil || !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal("new read authorized after FILEACK", target, err)
	}
	if err := f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(attachmentSendRequest(w.Attempt.Reservation))); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r)
		if err != nil || !proto.Equal(got, first) {
			t.Fatal("replay changed first ACK fact", got, err)
		}
	}
	if got, err := f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(w.Attempt.Reservation)); err != nil || !proto.Equal(got, originalReceipt) {
		t.Fatal("FILEACK changed Send receipt", got, err)
	}
	if err := files.Verify(t.Context(), blobstore.Attempt{ID: w.Attempt.AttemptID, Bytes: int64(len(data)), SHA256: [32]byte(w.Attempt.Reservation.Blob.Sha256)}); err != nil {
		t.Fatal("metadata FILEACK physically deleted bytes", err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 1)
	registryCount(t, f.f.db, "e2ee_message_blobs", 1)
	f.quota(t, 64, 1, 1)
}

func TestFileAckLateAfterReadDeadlineSealAndSenderReplacementPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileAckRequest(t, w.Attempt.Reservation)
	f.transition(t)
	f.f.replace(t, 1, f.gen(1))
	fault := new(blobReadFault)
	late := time.UnixMilli(receipt.ExpiresAtUnixMs).Add(time.Hour)
	fault.now.Store(&late)
	reader := NewPgE2EEBlobRepository(blobReadPool(t, f.f.db, fault))
	if got, err := reader.ResolveRead(t.Context(), 2, w.StorageID, readAttachmentRequest(t, w.Attempt.Reservation)); got != nil || !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) || fault.clockCalls.Load() != 1 {
		t.Fatal("controlled expired read", got, err)
	}
	// Trusted primitive simulates external file loss, NOT accepted-file GC.
	// No metadata rewrite, trigger bypass or wall-clock soak is involved.
	proof, err := files.RemoveVerified(t.Context(), w.Attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if err = proof.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r); err != nil || got == nil {
		t.Fatal("late old-epoch FILEACK blocked by present state", got, err)
	}
	if err := files.Verify(t.Context(), blobstore.Attempt{ID: w.Attempt.AttemptID, Bytes: int64(len(data)), SHA256: [32]byte(w.Attempt.Reservation.Blob.Sha256)}); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("late FILEACK resurrected lost bytes", err)
	}
	registryCount(t, f.f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.f.db, "e2ee_message_acknowledgements", 0)
	f.quota(t, 64, 1, 1)
}

func TestFileAckRecipientReplacementNeverReusesOldAuthorityPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileAckRequest(t, w.Attempt.Reservation)
	next := f.f.replace(t, 2, f.gen(2))
	if got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r); got != nil || !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired recipient FILEACK", got, err)
	}
	r.Context.DeviceGenerationId = next.GenerationID
	r.Files[0].Binding.Recipient.GenerationId = next.GenerationID
	if got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r); got != nil || !errors.Is(err, domain.ErrE2EEFileAckUnavailable) {
		t.Fatal("new generation claimed old recipient file", got, err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 0)
}

func TestFileAckBatchRollbackCommitFailureAndLostReplyPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileAckRequest(t, w.Attempt.Reservation)
	bad := proto.Clone(r).(*v2.AcknowledgeFilesRequest)
	other := proto.Clone(r.Files[0]).(*v2.FileAcknowledgement)
	// Sort strictly after the valid UUID, so failure occurs after its INSERT.
	other.Binding.MessageId = "ffffffff-ffff-4fff-bfff-ffffffffffff"
	bad.Files = append(bad.Files, other)
	if got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, bad); got != nil || !errors.Is(err, domain.ErrE2EEFileAckUnavailable) {
		t.Fatal("partial batch", got, err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 0)
	if _, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION file_ack_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic FILEACK commit fault'; END $$;
		CREATE CONSTRAINT TRIGGER file_ack_fault AFTER INSERT ON e2ee_file_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION file_ack_fault()`); err != nil {
		t.Fatal(err)
	}
	if got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r); got != nil || err == nil {
		t.Fatal("failed COMMIT returned fact", got, err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 0)
	if _, err := f.f.db.ExecContext(t.Context(), `DROP TRIGGER file_ack_fault ON e2ee_file_acknowledgements; DROP FUNCTION file_ack_fault()`); err != nil {
		t.Fatal(err)
	}
	fault := new(blobReadFault)
	repo := NewPgE2EEBlobRepository(blobReadPool(t, f.f.db, fault))
	fault.failCommit.Store(true) // Actual COMMIT succeeds; only its reply is lost.
	if got, err := repo.AcknowledgeFiles(t.Context(), 2, r); got != nil || !errors.Is(err, errBlobReadCommit) || fault.failCommit.Load() {
		t.Fatal("uncertain COMMIT reported success", got, err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 1)
	var original time.Time
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT acknowledged_at FROM e2ee_file_acknowledgements`).Scan(&original); err != nil {
		t.Fatal(err)
	}
	got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, r)
	if err != nil || got.Files[0].AcknowledgedAtUnixMs != original.UnixMilli() {
		t.Fatal("lost reply retry changed ACK", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := f.blobs.AcknowledgeFiles(ctx, 2, r); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled request returned a fact", got, err)
	}
}

func TestFileAckConcurrentOppositeBatchOrderPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileAckRequest(t, w.Attempt.Reservation)
	second := cloneBlobReservation(w.Attempt.Reservation)
	second.MessageID, second.Blob.BlobId, second.OperationID = uuid.NewString(), uuid.NewString(), uuid.NewString()
	g := f.reserve(t, 1, second)
	w2 := &domain.E2EEBlobWriteRequest{StorageID: w.StorageID, WriterID: uuid.NewString(), Attempt: &domain.E2EEBlobAttemptRequest{Reservation: second, Revision: g.Revision, AttemptID: g.AttemptID}}
	readyAttachment(t, f, w2, files, data)
	acceptAttachment(t, f, second)
	r.Files = append(r.Files, fileAckRequest(t, second).Files[0])
	const n = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			req := proto.Clone(r).(*v2.AcknowledgeFilesRequest)
			if i%2 != 0 {
				req.Files[0], req.Files[1] = req.Files[1], req.Files[0]
			}
			<-start
			got, err := f.blobs.AcknowledgeFiles(t.Context(), 2, req)
			if err != nil || boundary.ValidateFileAcknowledgementReceipts(req, got) != nil {
				t.Error("concurrent FILEACK/order", got, err)
			}
		})
	}
	close(start)
	wg.Wait()
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 2)
	f.quota(t, 128, 2, 2)
}

func TestFileAckSQLConstraintsAndMigrationPostgres(t *testing.T) {
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 33); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 33); err != nil {
		t.Fatal(err)
	}
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	id := w.Attempt.Reservation.MessageID
	file, err := os.ReadFile("../../migrations/033_e2ee_file_acknowledgements.sql")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(file), "-- +goose Down", 2)
	// Apply033 over existing accepted032 data, rather than testing only an
	// empty-schema migration. Empty Down must preserve the receipt/link/file.
	if _, err := f.f.db.ExecContext(t.Context(), parts[1]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.f.db.ExecContext(t.Context(), parts[0]); err != nil {
		t.Fatal(err)
	}
	registryCount(t, f.f.db, "e2ee_message_blobs", 1)
	registryCount(t, f.f.db, "e2ee_send_receipts", 1)
	for _, query := range []string{
		`INSERT INTO e2ee_file_acknowledgements(message_id) VALUES('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')`,
		`INSERT INTO e2ee_file_acknowledgements(message_id,acknowledged_at) VALUES($1,'infinity')`,
		`INSERT INTO e2ee_file_acknowledgements(message_id,acknowledged_at) VALUES($1,'epoch')`,
		`INSERT INTO e2ee_file_acknowledgements(message_id,acknowledged_at) VALUES($1,'2026-01-01 00:00:00.000001+00')`,
	} {
		args := []any{id}
		if !strings.Contains(query, "$1") {
			args = nil
		}
		if _, err := f.f.db.ExecContext(t.Context(), query, args...); err == nil {
			t.Fatal("invalid SQL ACK evidence accepted", query)
		}
	}
	if _, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation)); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`DELETE FROM e2ee_file_acknowledgements`, `UPDATE e2ee_file_acknowledgements SET acknowledged_at=acknowledged_at+INTERVAL '1 second'`, `UPDATE e2ee_file_acknowledgements SET message_id=gen_random_uuid()`} {
		if _, err := f.f.db.ExecContext(t.Context(), query); err == nil {
			t.Fatal("changed immutable ACK evidence", query)
		}
	}
	if _, err = f.f.db.ExecContext(t.Context(), parts[1]); err == nil || !strings.Contains(err.Error(), "cannot discard file acknowledgement") {
		t.Fatal("Down discarded retained ACK", err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 1)
}

func TestFileAckInvalidInputNeverOpensDatabase(t *testing.T) {
	repo := NewPgE2EEBlobRepository(nil)
	for _, uid := range []int64{0, -1, 1} {
		if got, err := repo.AcknowledgeFiles(context.Background(), uid, nil); got != nil || err == nil {
			t.Fatal("invalid FILEACK opened database", got, err)
		}
	}
	// Shape failure, not permission to ACK under a sender context.
	r := &v2.AcknowledgeFilesRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: uuid.NewString()}}
	if _, err := repo.AcknowledgeFiles(context.Background(), 1, r); err == nil {
		t.Fatal("empty FILEACK batch")
	}
}
