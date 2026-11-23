//go:build linux

package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

func readAttachmentRequest(t *testing.T, r *domain.E2EEBlobReservationRequest) *domain.E2EEBlobReadRequest {
	t.Helper()
	binding, err := boundary.EpochSendBinding(attachmentSendRequest(r).Envelope)
	if err != nil {
		t.Fatal(err)
	}
	return &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Recipient.GenerationId}, Binding: binding, Blob: proto.Clone(r.Blob).(*v2.BlobRef)}
}
func cloneBlobRead(r *domain.E2EEBlobReadRequest) *domain.E2EEBlobReadRequest {
	return &domain.E2EEBlobReadRequest{Context: proto.Clone(r.Context).(*pb.ClientContext), Binding: proto.Clone(r.Binding).(*pb.EpochSendBinding), Blob: proto.Clone(r.Blob).(*v2.BlobRef)}
}
func TestBlobReadRequiresAcceptedExactImmutableLinkPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	req := readAttachmentRequest(t, w.Attempt.Reservation)
	checkUnavailable := func(r *domain.E2EEBlobReadRequest, uid int64) {
		t.Helper()
		target, err := f.blobs.ResolveRead(t.Context(), uid, w.StorageID, r)
		if target != nil || !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
			t.Fatal("unexpected download identity", target, err)
		}
	}
	checkUnavailable(req, 2) // A reservation is not accepted.
	readyAttachment(t, f, w, files, data)
	checkUnavailable(req, 2) // Ready is still not accepted.
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	for name, mutate := range map[string]func(*domain.E2EEBlobReadRequest){
		"unknown message":   func(r *domain.E2EEBlobReadRequest) { r.Binding.MessageId = uuid.NewString() },
		"chat":              func(r *domain.E2EEBlobReadRequest) { r.Binding.ConversationId = uuid.NewString() },
		"epoch":             func(r *domain.E2EEBlobReadRequest) { r.Binding.Epoch++ },
		"sender":            func(r *domain.E2EEBlobReadRequest) { r.Binding.Sender.UserId = uuid.NewString() },
		"sender generation": func(r *domain.E2EEBlobReadRequest) { r.Binding.Sender.GenerationId = uuid.NewString() },
		"recipient":         func(r *domain.E2EEBlobReadRequest) { r.Binding.Recipient.UserId = uuid.NewString() },
		"digest":            func(r *domain.E2EEBlobReadRequest) { r.Binding.EnvelopeDigest[0] ^= 1 },
		"blob":              func(r *domain.E2EEBlobReadRequest) { r.Blob.BlobId = uuid.NewString() },
		"size":              func(r *domain.E2EEBlobReadRequest) { r.Blob.CiphertextBytes++ },
		"hash":              func(r *domain.E2EEBlobReadRequest) { r.Blob.Sha256[0] ^= 1 },
	} {
		t.Run(name, func(t *testing.T) { r := cloneBlobRead(req); mutate(r); checkUnavailable(r, 2) })
	}
	sender := cloneBlobRead(req)
	sender.Binding.Sender, sender.Binding.Recipient = sender.Binding.Recipient, sender.Binding.Sender
	sender.Context = f.context(1)
	checkUnavailable(sender, 1)
	if got, err := f.blobs.ResolveRead(t.Context(), 2, uuid.NewString(), req); got != nil || !errors.Is(err, domain.ErrE2EEBlobStorage) {
		t.Fatal("wrong owned storage", got, err)
	}
	target, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, req)
	if err != nil || target == nil {
		t.Fatal(target, err)
	}
	if target.AttemptID != w.Attempt.AttemptID || target.Revision != w.Attempt.Revision || target.StorageID != files.StorageID() || target.StoredAt.UnixMilli() != receipt.StoredAtUnixMs || target.ExpiresAt.UnixMilli() != receipt.ExpiresAtUnixMs || !target.ObservedAt.Before(target.ExpiresAt) {
		t.Fatal("wrong accepted target")
	}
	old := cloneBlobRead(req)
	req.Blob.Sha256[0] ^= 1
	req.Binding.EnvelopeDigest[0] ^= 1
	req.Context.DeviceGenerationId = uuid.NewString()
	if !proto.Equal(target.Request.Context, old.Context) || !proto.Equal(target.Request.Binding, old.Binding) || !proto.Equal(target.Request.Blob, old.Blob) {
		t.Fatal("input aliased committed result")
	}
	f.quota(t, 64, 1, 1)
}

func TestBlobReadSurvivesDescriptorAckRecoveryAndSenderReplacementPostgres(t *testing.T) {
	for _, disposition := range []pb.AckDisposition{pb.AckDisposition_ACK_DISPOSITION_STORED, pb.AckDisposition_ACK_DISPOSITION_REJECTED} {
		t.Run(disposition.String(), func(t *testing.T) {
			f, w, files, data := blobWriterFixture(t)
			readyAttachment(t, f, w, files, data)
			receipt := acceptAttachment(t, f, w.Attempt.Reservation)
			req := readAttachmentRequest(t, w.Attempt.Reservation)
			ack := f.ack(attachmentSendRequest(w.Attempt.Reservation))
			ack.Messages[0].Disposition = disposition
			if err := f.delivery.AcknowledgeV2(t.Context(), 2, ack); err != nil {
				t.Fatal(err)
			}
			registryCount(t, f.f.db, "e2ee_message_inbox", 0)
			f.transition(t)
			f.f.replace(t, 1, f.gen(1))
			got, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, req)
			if err != nil || got == nil || got.AttemptID != w.Attempt.AttemptID || got.ExpiresAt.UnixMilli() != receipt.ExpiresAtUnixMs {
				t.Fatal("accepted old scope lost download identity", err)
			}
			registryCount(t, f.f.db, "e2ee_blob_attempts", 1)
			f.quota(t, 64, 1, 1)
		})
	}
}

func TestBlobReadRecipientReplacementNeverTransfersOldFileAccessPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	req := readAttachmentRequest(t, w.Attempt.Reservation)
	d := f.f.replace(t, 2, f.gen(2))
	if got, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, req); got != nil || !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired recipient kept access", err)
	}
	for _, changeBinding := range []bool{false, true} {
		r := cloneBlobRead(req)
		r.Context.DeviceGenerationId = d.GenerationID
		if changeBinding {
			r.Binding.Recipient.GenerationId = d.GenerationID
		}
		got, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, r)
		if got != nil || err == nil {
			t.Fatal("replacement installation inherited old file", err)
		}
	}
}

func TestBlobReadSelectsAcceptedSuccessorWithoutRenewingPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	f.age(t, &domain.E2EEBlobReservation{})
	r := cloneBlobReservation(w.Attempt.Reservation)
	r.OperationID = uuid.NewString()
	r.ExpectedRevision = w.Attempt.Revision
	r.ExpectedAttemptID = w.Attempt.AttemptID
	g := f.reserve(t, 1, r)
	next := &domain.E2EEBlobWriteRequest{StorageID: w.StorageID, WriterID: uuid.NewString(), Attempt: &domain.E2EEBlobAttemptRequest{Reservation: r, Revision: g.Revision, AttemptID: g.AttemptID}}
	readyAttachment(t, f, next, files, data)
	acceptAttachment(t, f, r)
	for range 2 {
		got, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, readAttachmentRequest(t, r))
		if err != nil || got == nil || got.AttemptID != g.AttemptID || got.Revision != 2 {
			t.Fatal("selected staging predecessor", got, err)
		}
	}
	f.quota(t, 128, 1, 2)
}

func TestBlobReadInvalidInputNeverOpensDatabase(t *testing.T) {
	repo := NewPgE2EEBlobRepository(nil)
	for _, uid := range []int64{0, -1, 1} {
		if v, e := repo.ResolveRead(context.Background(), uid, uuid.NewString(), nil); v != nil || e == nil {
			t.Fatal("invalid input admitted")
		}
	}
}
