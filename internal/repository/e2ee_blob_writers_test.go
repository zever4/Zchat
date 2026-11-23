//go:build linux

package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
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
	"github.com/zever4/Zchat/internal/testutil"
)

func blobWriterFixture(t *testing.T) (*blobReservationFixture, *domain.E2EEBlobWriteRequest, *blobstore.Store, []byte) {
	t.Helper()
	return blobWriterOnFixture(t, blobReservations(t, nil))
}
func blobWriterOnFixture(t *testing.T, f *blobReservationFixture) (*blobReservationFixture, *domain.E2EEBlobWriteRequest, *blobstore.Store, []byte) {
	t.Helper()
	data := bytes.Repeat([]byte{0x42}, 64)
	r := f.request(1)
	h := sha256.Sum256(data)
	r.Blob.Sha256 = h[:]
	g := f.reserve(t, 1, r)
	req := &domain.E2EEBlobWriteRequest{Attempt: &domain.E2EEBlobAttemptRequest{Reservation: r, Revision: g.Revision, AttemptID: g.AttemptID}, WriterID: uuid.NewString()}
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	files, err := blobstore.Open(path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	req.StorageID = files.StorageID()
	if err = f.blobs.BindStorage(t.Context(), req.StorageID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	})
	return f, req, files, data
}
func writeBlobProof(t *testing.T, files *blobstore.Store, r *domain.E2EEBlobWriteRequest, data []byte) *blobstore.Written {
	t.Helper()
	w, err := files.Write(t.Context(), blobstore.Attempt{ID: r.Attempt.AttemptID, Bytes: int64(len(data)), SHA256: sha256.Sum256(data)}, io.NopCloser(bytes.NewReader(data)))
	if err != nil {
		t.Fatal(err)
	}
	return w
}
func TestE2EEBlobWriterLifecycleProofAndReplayPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	for _, proof := range []*blobstore.Written{nil, {}} {
		if g, err := f.blobs.CompleteWrite(t.Context(), 1, r, proof); g != nil || !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatal("forged filesystem proof", g, err)
		}
	}
	if _, err := f.blobs.CheckWrite(t.Context(), 1, r); !errors.Is(err, domain.ErrE2EEBlobWriteState) {
		t.Fatal("unstarted writer", err)
	}
	a, err := f.blobs.BeginWrite(t.Context(), 1, r)
	if err != nil || a.Grant.State != "writing" || a.WriterID != r.WriterID {
		t.Fatal(a, err)
	}
	for range 2 {
		b, err := f.blobs.BeginWrite(t.Context(), 1, r)
		if err != nil || b.Grant.AttemptID != a.Grant.AttemptID || !b.Grant.ExpiresAt.Equal(a.Grant.ExpiresAt) {
			t.Fatal("writer replay extended lease", b, err)
		}
		if _, err = f.blobs.CheckWrite(t.Context(), 1, r); err != nil {
			t.Fatal(err)
		}
	}
	wrong := *r
	wrong.WriterID = uuid.NewString()
	if _, err = f.blobs.BeginWrite(t.Context(), 1, &wrong); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("another writer stole attempt", err)
	}
	w := writeBlobProof(t, files, r, data)
	ready, err := f.blobs.CompleteWrite(t.Context(), 1, r, w)
	if err != nil || ready.Grant.State != "ready" {
		t.Fatal(ready, err)
	}
	again, err := f.blobs.CompleteWrite(t.Context(), 1, r, w)
	if err != nil || again.Grant.State != "ready" || !again.Grant.ExpiresAt.Equal(ready.Grant.ExpiresAt) {
		t.Fatal("Ready replay", again, err)
	}
	if _, err = f.blobs.BeginWrite(t.Context(), 1, r); !errors.Is(err, domain.ErrE2EEBlobWriteState) {
		t.Fatal("reopened Ready file", err)
	}
	f.quota(t, 64, 1, 1)
	registryCount(t, f.f.db, "e2ee_send_receipts", 0)
}
func TestE2EEBlobWriterConcurrentBindingAndNoFirstUsePostgres(t *testing.T) {
	f, r, _, _ := blobWriterFixture(t)
	const n = 10
	start := make(chan struct{})
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for range n {
		q := *r
		q.WriterID = uuid.NewString()
		wg.Go(func() { <-start; _, err := f.blobs.BeginWrite(t.Context(), 1, &q); errs <- err })
	}
	close(start)
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, domain.ErrE2EEBlobCAS) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal("writer winners", wins)
	}
	bad := *r
	attempt := *r.Attempt
	reservation := *attempt.Reservation
	reservation.MessageID = uuid.NewString()
	attempt.Reservation = &reservation
	bad.Attempt = &attempt
	if _, err := f.blobs.BeginWrite(t.Context(), 1, &bad); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("upload created first-use claim", err)
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 1)
	f.quota(t, 64, 1, 1)
	for _, field := range []string{"revision", "attempt", "operation", "hash", "length"} {
		q := *r
		a := *r.Attempt
		a.Reservation = cloneBlobReservation(a.Reservation)
		q.Attempt = &a
		want := domain.ErrE2EEBlobCAS
		switch field {
		case "revision":
			a.Revision++
		case "attempt":
			a.AttemptID = uuid.NewString()
		case "operation":
			a.Reservation.OperationID = uuid.NewString()
		case "hash":
			a.Reservation.Blob.Sha256[0]++
			want = domain.ErrE2EEMessageConflict
		case "length":
			a.Reservation.Blob.CiphertextBytes++
			want = domain.ErrE2EEMessageConflict
		}
		if _, err := f.blobs.CheckWrite(t.Context(), 1, &q); !errors.Is(err, want) {
			t.Fatal("mismatched writer binding", field, err)
		}
	}
}
func TestE2EEBlobWriterExpirySealReplacementAndRenewalPostgres(t *testing.T) {
	for _, name := range []string{"expiry", "seal", "replacement", "renewal"} {
		t.Run(name, func(t *testing.T) {
			f, r, files, data := blobWriterFixture(t)
			lease, err := f.blobs.BeginWrite(t.Context(), 1, r)
			if err != nil {
				t.Fatal(err)
			}
			w := writeBlobProof(t, files, r, data)
			want := domain.ErrE2EEBlobLeaseExpired
			switch name {
			case "expiry":
				f.age(t, &lease.Grant)
			case "seal":
				f.transition(t)
				want = domain.ErrE2EEEpochInactive
			case "replacement":
				f.f.replace(t, 2, f.gen(2))
				want = domain.ErrE2EEConversationClosed
			case "renewal":
				f.age(t, &lease.Grant)
				renew := cloneBlobReservation(r.Attempt.Reservation)
				renew.OperationID = uuid.NewString()
				renew.ExpectedRevision = lease.Grant.Revision
				renew.ExpectedAttemptID = lease.Grant.AttemptID
				f.reserve(t, 1, renew)
				want = domain.ErrE2EEBlobCAS
			}
			if g, err := f.blobs.CompleteWrite(t.Context(), 1, r, w); g != nil || !errors.Is(err, want) {
				t.Fatal("stale durable proof became Ready", g, err)
			}
			if _, err := f.blobs.CheckWrite(t.Context(), 1, r); !errors.Is(err, want) {
				t.Fatal("stale ongoing writer", err)
			}
		})
	}
}
func TestE2EEBlobWriterProofBindingAndCommitFaultPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION writer_commit_fault()RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic writer COMMIT fault'; END $$;
        CREATE CONSTRAINT TRIGGER writer_commit_fault AFTER UPDATE ON e2ee_blob_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION writer_commit_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	if g, err := f.blobs.BeginWrite(t.Context(), 1, r); g != nil || err == nil {
		t.Fatal("failed Begin commit returned lease", g, err)
	}
	var state string
	var writer bool
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT state,writer_id IS NOT NULL FROM e2ee_blob_attempts`).Scan(&state, &writer); err != nil || state != "reserved" || writer {
		t.Fatal(state, writer, err)
	}
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER writer_commit_fault ON e2ee_blob_attempts`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	wrong := *r
	attempt := *r.Attempt
	attempt.AttemptID = uuid.NewString()
	wrong.Attempt = &attempt
	proof := writeBlobProof(t, files, &wrong, data)
	if _, err = f.blobs.CompleteWrite(t.Context(), 1, r, proof); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal("accepted another attempt's proof", err)
	}
	if err = files.Remove(t.Context(), attempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	w := writeBlobProof(t, files, r, data)
	if _, err = f.f.db.ExecContext(t.Context(), `CREATE CONSTRAINT TRIGGER writer_commit_fault AFTER UPDATE ON e2ee_blob_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION writer_commit_fault()`); err != nil {
		t.Fatal(err)
	}
	if g, err := f.blobs.CompleteWrite(t.Context(), 1, r, w); g != nil || err == nil {
		t.Fatal("failed Ready commit returned proof", g, err)
	}
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts`).Scan(&state); err != nil || state != "writing" {
		t.Fatal(state, err)
	}
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER writer_commit_fault ON e2ee_blob_attempts;DROP FUNCTION writer_commit_fault()`); err != nil {
		t.Fatal(err)
	}
	if _, err = f.blobs.CompleteWrite(t.Context(), 1, r, w); err != nil {
		t.Fatal(err)
	}
	f.quota(t, 64, 1, 1)
}
func TestE2EEBlobWriterSQLAndMigrationPostgres(t *testing.T) {
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 29); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty Down", err)
	}
	seed := seedConversations(t, db)
	chat := seed.create(t)
	beforeRepo := NewPgE2EEBlobRepository(db)
	if err := beforeRepo.Configure(t.Context(), domain.E2EEBlobLimits{GlobalBytes: 4096, AccountBytes: 2048, GlobalClaims: 16, AccountClaims: 8, GlobalAttempts: 32, AccountAttempts: 16}); err != nil {
		t.Fatal(err)
	}
	// Reserve using actual migration028, then upgrade without relabeling the
	// immutable attempt or extending the original one-hour deadline.
	rsv := &domain.E2EEBlobReservationRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: seed.low.GenerationID}, MessageID: uuid.NewString(), ConversationID: chat.ID, CryptoEpoch: new(uint64),
		Sender: &pb.DeviceRef{UserId: seed.low.UserUUID, DeviceId: 1, GenerationId: seed.low.GenerationID}, Recipient: &pb.DeviceRef{UserId: seed.high.UserUUID, DeviceId: 1, GenerationId: seed.high.GenerationID},
		OperationID: uuid.NewString(), Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: 64, Sha256: make([]byte, 32)}}
	before, err := beforeRepo.Reserve(t.Context(), 1, rsv)
	if err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 29); err != nil {
		t.Fatal(err)
	}
	after, err := beforeRepo.Reserve(t.Context(), 1, rsv)
	if err != nil || before.AttemptID != after.AttemptID || !before.ExpiresAt.Equal(after.ExpiresAt) {
		t.Fatal("writer migration changed grant", before, after, err)
	}
	// Historical writer fixture for schema029: current runtime requires030 and
	// never falls back to an unbound storage root.
	if _, err = db.ExecContext(t.Context(), `UPDATE e2ee_blob_attempts SET state='writing',writer_id=$1 WHERE attempt_id=$2`, uuid.NewString(), after.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err = goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("goose Down lost retained writer")
	}
	f, r, _, _ := blobWriterFixture(t)
	for _, q := range []string{`UPDATE e2ee_blob_attempts SET writer_id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'`, `UPDATE e2ee_blob_attempts SET state='writing'`} {
		if _, err := f.f.db.ExecContext(t.Context(), q); err == nil {
			t.Fatal("SQL invalid writer", q)
		}
	}
	if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{`UPDATE e2ee_blob_attempts SET writer_id=NULL`, `UPDATE e2ee_blob_attempts SET writer_id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'`} {
		if _, err := f.f.db.ExecContext(t.Context(), q); err == nil {
			t.Fatal("SQL stole writer", q)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := f.blobs.CheckWrite(ctx, 1, r); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// The SQL Down guard is tested on retained actual writer state as well as
	// goose's empty Down/Up above. No attempt to rewrite/erase the old writer.
	file, err := os.ReadFile("../../migrations/029_e2ee_blob_writers.sql")
	if err != nil {
		t.Fatal(err)
	}
	down := strings.SplitN(string(file), "-- +goose Down", 2)[1]
	if _, err = f.f.db.ExecContext(t.Context(), down); err == nil {
		t.Fatal("Down discarded retained writer")
	}
	var writer string
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT writer_id::text FROM e2ee_blob_attempts`).Scan(&writer); err != nil || writer != r.WriterID {
		t.Fatal("failed Down changed writer", writer, err)
	}
}
