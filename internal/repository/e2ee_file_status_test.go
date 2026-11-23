//go:build linux

package repository

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

func fileStatusRequest(t *testing.T, r *domain.E2EEBlobReservationRequest) *v2.GetFileStatusesRequest {
	t.Helper()
	read := readAttachmentRequest(t, r)
	return &v2.GetFileStatusesRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Sender.GenerationId},
		Files: []*v2.FileStatusReference{{Binding: read.Binding, Blob: read.Blob}}}
}
func TestFileStatusAcceptedAndDescriptorOutcomesAreIndependentPostgres(t *testing.T) {
	for _, disposition := range []pb.AckDisposition{pb.AckDisposition_ACK_DISPOSITION_STORED, pb.AckDisposition_ACK_DISPOSITION_REJECTED} {
		t.Run(disposition.String(), func(t *testing.T) {
			f, w, files, data := blobWriterFixture(t)
			r := fileStatusRequest(t, w.Attempt.Reservation)
			unavailable := func() {
				t.Helper()
				if got, err := f.blobs.FileStatuses(t.Context(), 1, r); got != nil || !errors.Is(err, domain.ErrE2EEFileStatusUnavailable) {
					t.Fatal("unaccepted file status", got, err)
				}
			}
			unavailable()
			readyAttachment(t, f, w, files, data)
			unavailable() // Ready/upload is not a Send receipt.
			receipt := acceptAttachment(t, f, w.Attempt.Reservation)
			got, err := f.blobs.FileStatuses(t.Context(), 1, r)
			if err != nil || boundary.ValidateFileStatuses(r, got) != nil || !proto.Equal(got.Files[0].Receipt, receipt) || got.Files[0].AcknowledgedAtUnixMs != nil {
				t.Fatal("initial file facts", got, err)
			}
			ack := f.ack(attachmentSendRequest(w.Attempt.Reservation))
			ack.Messages[0].Disposition = disposition
			if err = f.delivery.AcknowledgeV2(t.Context(), 2, ack); err != nil {
				t.Fatal(err)
			}
			got, err = f.blobs.FileStatuses(t.Context(), 1, r)
			if err != nil || got.Files[0].AcknowledgedAtUnixMs != nil || !proto.Equal(got.Files[0].Receipt, receipt) {
				t.Fatal("descriptor outcome implied saved file", got, err)
			}
			registryCount(t, f.f.db, "e2ee_message_inbox", 0)
			fileAck, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation))
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				got, err = f.blobs.FileStatuses(t.Context(), 1, r)
				if err != nil || boundary.ValidateFileStatuses(r, got) != nil || got.Files[0].GetAcknowledgedAtUnixMs() != fileAck.Files[0].AcknowledgedAtUnixMs || !proto.Equal(got.Files[0].Receipt, receipt) {
					t.Fatal("lost independent first file ACK", got, err)
				}
			}
			registryCount(t, f.f.db, "e2ee_file_acknowledgements", 1)
			f.quota(t, 64, 1, 1) // Metadata observation/ACK releases no bytes.
		})
	}
}

func TestFileStatusExactSenderBindingBatchAndGenerationPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	original := fileStatusRequest(t, w.Attempt.Reservation)
	for name, mutate := range map[string]func(*v2.GetFileStatusesRequest){
		"uuid":                 func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.MessageId = uuid.NewString() },
		"chat":                 func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.ConversationId = uuid.NewString() },
		"epoch":                func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.Epoch++ },
		"sender":               func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.Sender.UserId = uuid.NewString() },
		"recipient":            func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.Recipient.UserId = uuid.NewString() },
		"recipient generation": func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.Recipient.GenerationId = uuid.NewString() },
		"digest":               func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.EnvelopeDigest[0] ^= 1 },
		"blob":                 func(r *v2.GetFileStatusesRequest) { r.Files[0].Blob.BlobId = uuid.NewString() },
		"size":                 func(r *v2.GetFileStatusesRequest) { r.Files[0].Blob.CiphertextBytes++ },
		"sha":                  func(r *v2.GetFileStatusesRequest) { r.Files[0].Blob.Sha256[0] ^= 1 },
		"partial batch": func(r *v2.GetFileStatusesRequest) {
			ref := proto.Clone(r.Files[0]).(*v2.FileStatusReference)
			ref.Binding.MessageId = uuid.NewString()
			r.Files = append(r.Files, ref)
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := proto.Clone(original).(*v2.GetFileStatusesRequest)
			mutate(r)
			if got, err := f.blobs.FileStatuses(t.Context(), 1, r); got != nil || !errors.Is(err, domain.ErrE2EEFileStatusUnavailable) {
				t.Fatal("mismatched sender file query", got, err)
			}
		})
	}
	// Recipient may not turn sender-only metadata into its own status oracle.
	r := proto.Clone(original).(*v2.GetFileStatusesRequest)
	r.Context = f.context(2)
	r.Files[0].Binding.Sender, r.Files[0].Binding.Recipient = r.Files[0].Binding.Recipient, r.Files[0].Binding.Sender
	if got, err := f.blobs.FileStatuses(t.Context(), 2, r); got != nil || !errors.Is(err, domain.ErrE2EEFileStatusUnavailable) {
		t.Fatal("recipient queried sender file", got, err)
	}
	next := f.f.replace(t, 1, f.gen(1))
	if got, err := f.blobs.FileStatuses(t.Context(), 1, original); got != nil || !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired sender queried live metadata", got, err)
	}
	r = proto.Clone(original).(*v2.GetFileStatusesRequest)
	r.Context.DeviceGenerationId = next.GenerationID
	r.Files[0].Binding.Sender.GenerationId = next.GenerationID
	if got, err := f.blobs.FileStatuses(t.Context(), 1, r); got != nil || !errors.Is(err, domain.ErrE2EEFileStatusUnavailable) {
		t.Fatal("new sender adopted old facts", got, err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 0)
}

func TestFileStatusExpiryLateAckOldEpochAndLostBytesPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileStatusRequest(t, w.Attempt.Reservation)
	f.transition(t)
	fault := new(blobReadFault)
	repo := NewPgE2EEBlobRepository(blobReadPool(t, f.f.db, fault))
	deadline := time.UnixMilli(receipt.StoredAtUnixMs).Add(domain.E2EEBlobAcceptedRetention)
	if deadline.Sub(time.UnixMilli(receipt.StoredAtUnixMs)).Milliseconds() != boundary.FileRetentionMilliseconds {
		t.Fatal("wire/domain TTL drift")
	}
	for _, at := range []time.Time{deadline.Add(-time.Millisecond), deadline, deadline.Add(time.Millisecond)} {
		fault.now.Store(&at)
		got, err := repo.FileStatuses(t.Context(), 1, r)
		if err != nil || got.Files[0].ObservedAtUnixMs != at.UnixMilli() || got.Files[0].ExpiresAtUnixMs != deadline.UnixMilli() || got.Files[0].AcknowledgedAtUnixMs != nil {
			t.Fatal("exclusive deadline/old epoch", got, err)
		}
	}
	// Controlled observation order, not a real30day soak. Message inbox is
	// deliberately still present: status lookup never invokes message cleanup.
	registryCount(t, f.f.db, "e2ee_message_inbox", 1)
	ack, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation))
	if err != nil {
		t.Fatal(err)
	}
	proof, err := files.RemoveVerified(t.Context(), w.Attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if err = proof.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FileStatuses(t.Context(), 1, r)
	if err != nil || boundary.ValidateFileStatuses(r, got) != nil || got.Files[0].GetAcknowledgedAtUnixMs() != ack.Files[0].AcknowledgedAtUnixMs || got.Files[0].ObservedAtUnixMs < got.Files[0].ExpiresAtUnixMs || !proto.Equal(got.Files[0].Receipt, receipt) {
		t.Fatal("ACK erased expiry/receipt", got, err)
	}
	if err = files.Verify(t.Context(), blobstore.Attempt{ID: w.Attempt.AttemptID, Bytes: int64(len(data)), SHA256: [32]byte(w.Attempt.Reservation.Blob.Sha256)}); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("status recreated lost file", err)
	}
	f.quota(t, 64, 1, 1)
}

func TestFileStatusRecipientReplacementRetainsSavedFactPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileStatusRequest(t, w.Attempt.Reservation)
	ack, err := f.blobs.AcknowledgeFiles(t.Context(), 2, fileAckRequest(t, w.Attempt.Reservation))
	if err != nil {
		t.Fatal(err)
	}
	f.f.replace(t, 2, f.gen(2))
	got, err := f.blobs.FileStatuses(t.Context(), 1, r)
	if err != nil || boundary.ValidateFileStatuses(r, got) != nil || got.Files[0].RecipientReplacedAtUnixMs == nil || got.Files[0].GetAcknowledgedAtUnixMs() != ack.Files[0].AcknowledgedAtUnixMs || !proto.Equal(got.Files[0].Receipt, receipt) {
		t.Fatal("replacement erased saved fact", got, err)
	}
	registryCount(t, f.f.db, "e2ee_message_inbox", 1)
}

func TestFileStatusCommitCancellationAndClockAfterLockPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileStatusRequest(t, w.Attempt.Reservation)
	fault := new(blobReadFault)
	repo := NewPgE2EEBlobRepository(blobReadPool(t, f.f.db, fault))
	fault.failCommit.Store(true)
	if got, err := repo.FileStatuses(t.Context(), 1, r); got != nil || !errors.Is(err, errBlobReadCommit) || fault.failCommit.Load() {
		t.Fatal("uncertain read COMMIT returned fact", got, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := repo.FileStatuses(ctx, 1, r); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read returned fact", got, err)
	}
	lock, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.ExecContext(t.Context(), `SELECT message_id FROM e2ee_message_claims WHERE message_id=$1 FOR UPDATE`, r.Files[0].Binding.MessageId); err != nil {
		t.Fatal(err)
	}
	before := time.UnixMilli(receipt.ExpiresAtUnixMs).Add(-time.Millisecond)
	fault.now.Store(&before)
	result := make(chan *v2.FileStatuses, 1)
	failure := make(chan error, 1)
	go func() { got, err := repo.FileStatuses(t.Context(), 1, r); result <- got; failure <- err }()
	barrier, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	for {
		var blocked bool
		if err = f.f.db.QueryRowContext(barrier, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='blob_read_target' AND wait_event_type='Lock')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-barrier.Done():
			t.Fatal("status did not reach claim lock")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if fault.clockCalls.Load() != 0 {
		t.Fatal("used pre-lock clock")
	}
	after := before.Add(time.Millisecond)
	fault.now.Store(&after)
	if err = lock.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := <-result, <-failure
	if err != nil || got.Files[0].ObservedAtUnixMs != after.UnixMilli() || got.Files[0].ObservedAtUnixMs < got.Files[0].ExpiresAtUnixMs || fault.clockCalls.Load() != 1 {
		t.Fatal("stale pre-lock clock", got, err)
	}
}

func TestFileStatusConcurrentOppositeBatchesAndFileAckPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	r := fileStatusRequest(t, w.Attempt.Reservation)
	ack := fileAckRequest(t, w.Attempt.Reservation)
	second := cloneBlobReservation(w.Attempt.Reservation)
	second.MessageID, second.Blob.BlobId, second.OperationID = uuid.NewString(), uuid.NewString(), uuid.NewString()
	g := f.reserve(t, 1, second)
	w2 := &domain.E2EEBlobWriteRequest{StorageID: w.StorageID, WriterID: uuid.NewString(), Attempt: &domain.E2EEBlobAttemptRequest{Reservation: second, Revision: g.Revision, AttemptID: g.AttemptID}}
	readyAttachment(t, f, w2, files, data)
	acceptAttachment(t, f, second)
	r.Files = append(r.Files, fileStatusRequest(t, second).Files[0])
	ack.Files = append(ack.Files, fileAckRequest(t, second).Files[0])
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			<-start
			if i%3 == 0 {
				req := proto.Clone(ack).(*v2.AcknowledgeFilesRequest)
				if i%2 == 0 {
					req.Files[0], req.Files[1] = req.Files[1], req.Files[0]
				}
				if _, err := f.blobs.AcknowledgeFiles(t.Context(), 2, req); err != nil {
					t.Error(err)
				}
				return
			}
			req := proto.Clone(r).(*v2.GetFileStatusesRequest)
			if i%2 == 0 {
				req.Files[0], req.Files[1] = req.Files[1], req.Files[0]
			}
			got, err := f.blobs.FileStatuses(t.Context(), 1, req)
			if err != nil || boundary.ValidateFileStatuses(req, got) != nil {
				t.Error("concurrent ordered status", got, err)
			}
		})
	}
	close(start)
	wg.Wait()
	got, err := f.blobs.FileStatuses(t.Context(), 1, r)
	if err != nil || got.Files[0].AcknowledgedAtUnixMs == nil || got.Files[1].AcknowledgedAtUnixMs == nil {
		t.Fatal("missing final ACK facts", got, err)
	}
	registryCount(t, f.f.db, "e2ee_file_acknowledgements", 2)
	f.quota(t, 128, 2, 2)
}

func TestFileStatusInvalidInputNeverOpensDatabase(t *testing.T) {
	repo := NewPgE2EEBlobRepository(nil)
	for _, uid := range []int64{0, -1, 1} {
		if got, err := repo.FileStatuses(context.Background(), uid, nil); got != nil || err == nil {
			t.Fatal("invalid status opened database", got, err)
		}
	}
}

func TestFileStatusBatchAcrossPeersPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	if _, err := f.f.db.ExecContext(t.Context(), `INSERT INTO users(id,user_uuid,username,email,password_hash)
        VALUES(3,'77777777-7777-4777-8777-777777777777','third','third@file-status.invalid','synthetic-hash');
        INSERT INTO user_settings(user_id) VALUES(3);
        INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES(1,3,'accepted')`); err != nil {
		t.Fatal(err)
	}
	registered, err := f.f.devices.Register(t.Context(), 3, registrationMaterial())
	if err != nil {
		t.Fatal(err)
	}
	gen := registered.Device.GenerationID
	if _, err = f.keys.Publish(t.Context(), 3, testutil.PublicPreKeyUpload(t, gen)); err != nil {
		t.Fatal(err)
	}
	creation := proto.Clone(f.f.req).(*pb.CreateConversationRequest)
	creation.CreationRequestId, creation.PeerUserId = uuid.NewString(), 3
	chat, err := f.f.repo.Create(t.Context(), 1, creation)
	if err != nil {
		t.Fatal(err)
	}
	second := cloneBlobReservation(w.Attempt.Reservation)
	second.MessageID, second.Blob.BlobId, second.OperationID, second.ConversationID = uuid.NewString(), uuid.NewString(), uuid.NewString(), chat.ID
	second.Recipient = &pb.DeviceRef{UserId: "77777777-7777-4777-8777-777777777777", DeviceId: 1, GenerationId: gen}
	lease := f.reserve(t, 1, second)
	w2 := &domain.E2EEBlobWriteRequest{StorageID: w.StorageID, WriterID: uuid.NewString(), Attempt: &domain.E2EEBlobAttemptRequest{Reservation: second, Revision: lease.Revision, AttemptID: lease.AttemptID}}
	readyAttachment(t, f, w2, files, data)
	acceptAttachment(t, f, second)
	fileAck, err := f.blobs.AcknowledgeFiles(t.Context(), 3, fileAckRequest(t, second))
	if err != nil {
		t.Fatal(err)
	}
	r := fileStatusRequest(t, w.Attempt.Reservation)
	r.Files = append(r.Files, fileStatusRequest(t, second).Files[0])
	for range 2 {
		r.Files[0], r.Files[1] = r.Files[1], r.Files[0]
		got, err := f.blobs.FileStatuses(t.Context(), 1, r)
		if err != nil || boundary.ValidateFileStatuses(r, got) != nil {
			t.Fatal("cross-peer batch", got, err)
		}
		for _, status := range got.Files {
			if status.Reference.Binding.Recipient.UserId == second.Recipient.UserId {
				if status.GetAcknowledgedAtUnixMs() != fileAck.Files[0].AcknowledgedAtUnixMs {
					t.Fatal("lost other peer ACK")
				}
			} else if status.AcknowledgedAtUnixMs != nil {
				t.Fatal("ACK assigned to wrong peer")
			}
		}
	}
}
