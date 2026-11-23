package e2ee

import (
	"math"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
)

func TestBlobReservationBoundary(t *testing.T) {
	fresh := func() *domain.E2EEBlobReservationRequest {
		epoch := uint64(0)
		g := uuid.NewString()
		return &domain.E2EEBlobReservationRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: g}, MessageID: uuid.NewString(), ConversationID: uuid.NewString(), CryptoEpoch: &epoch,
			Sender: &pb.DeviceRef{UserId: uuid.NewString(), DeviceId: 1, GenerationId: g}, Recipient: &pb.DeviceRef{UserId: uuid.NewString(), DeviceId: 1, GenerationId: uuid.NewString()},
			Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: 37, Sha256: make([]byte, 32)}, OperationID: uuid.NewString()}
	}
	if err := ValidateBlobReservation(fresh()); err != nil {
		t.Fatal(err)
	}
	r := fresh()
	r.ExpectedRevision = 42
	r.ExpectedAttemptID = uuid.NewString()
	if err := ValidateBlobReservation(r); err != nil {
		t.Fatal(err)
	}
	if ValidateBlobReservation(nil) == nil {
		t.Fatal("nil request")
	}
	for name, mutate := range map[string]func(*domain.E2EEBlobReservationRequest){
		"context missing":         func(r *domain.E2EEBlobReservationRequest) { r.Context = nil },
		"control3":                func(r *domain.E2EEBlobReservationRequest) { r.Context.ProtocolVersion = 3 },
		"context unknown":         func(r *domain.E2EEBlobReservationRequest) { r.Context.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"sender context":          func(r *domain.E2EEBlobReservationRequest) { r.Context.DeviceGenerationId = uuid.NewString() },
		"epoch absent":            func(r *domain.E2EEBlobReservationRequest) { r.CryptoEpoch = nil },
		"epoch overflow":          func(r *domain.E2EEBlobReservationRequest) { *r.CryptoEpoch = math.MaxUint64 },
		"message UUID":            func(r *domain.E2EEBlobReservationRequest) { r.MessageID = "not-a-uuid" },
		"chat UUID":               func(r *domain.E2EEBlobReservationRequest) { r.ConversationID = "not-a-uuid" },
		"operation UUID":          func(r *domain.E2EEBlobReservationRequest) { r.OperationID = "not-a-uuid" },
		"revision overflow":       func(r *domain.E2EEBlobReservationRequest) { r.ExpectedRevision = math.MaxInt64 },
		"initial attempt":         func(r *domain.E2EEBlobReservationRequest) { r.ExpectedAttemptID = uuid.NewString() },
		"renewal attempt missing": func(r *domain.E2EEBlobReservationRequest) { r.ExpectedRevision = 1 },
		"sender missing":          func(r *domain.E2EEBlobReservationRequest) { r.Sender = nil },
		"same account":            func(r *domain.E2EEBlobReservationRequest) { r.Recipient.UserId = r.Sender.UserId },
		"device2":                 func(r *domain.E2EEBlobReservationRequest) { r.Recipient.DeviceId = 2 },
		"blob missing":            func(r *domain.E2EEBlobReservationRequest) { r.Blob = nil },
		"blob format":             func(r *domain.E2EEBlobReservationRequest) { r.Blob.FormatVersion = 2 },
		"blob too large":          func(r *domain.E2EEBlobReservationRequest) { r.Blob.CiphertextBytes = MaxAttachmentBlobBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) {
			r := fresh()
			mutate(r)
			if ValidateBlobReservation(r) == nil {
				t.Fatal("accepted invalid request")
			}
		})
	}
}

func TestBlobLimitsRequireFiniteOrderedBudgets(t *testing.T) {
	valid := domain.E2EEBlobLimits{GlobalBytes: math.MaxInt64, AccountBytes: 37, GlobalClaims: 2, AccountClaims: 1, GlobalAttempts: 3, AccountAttempts: 2}
	if !valid.Valid() {
		t.Fatal("valid limits rejected")
	}
	for _, l := range []domain.E2EEBlobLimits{{}, {GlobalBytes: -1, AccountBytes: 37, GlobalClaims: 2, AccountClaims: 1, GlobalAttempts: 3, AccountAttempts: 2},
		{GlobalBytes: 36, AccountBytes: 37, GlobalClaims: 2, AccountClaims: 1, GlobalAttempts: 3, AccountAttempts: 2},
		{GlobalBytes: 100, AccountBytes: 37, GlobalClaims: 1, AccountClaims: 2, GlobalAttempts: 3, AccountAttempts: 2},
		{GlobalBytes: 100, AccountBytes: 37, GlobalClaims: 2, AccountClaims: 1, GlobalAttempts: 1, AccountAttempts: 2}} {
		if l.Valid() {
			t.Fatal("invalid limits", l)
		}
	}
}

func TestBlobWriterBoundary(t *testing.T) {
	epoch := uint64(0)
	g := uuid.NewString()
	r := &domain.E2EEBlobWriteRequest{Attempt: &domain.E2EEBlobAttemptRequest{Revision: 1, AttemptID: uuid.NewString(), Reservation: &domain.E2EEBlobReservationRequest{
		Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: g}, MessageID: uuid.NewString(), ConversationID: uuid.NewString(), CryptoEpoch: &epoch,
		Sender: &pb.DeviceRef{UserId: uuid.NewString(), DeviceId: 1, GenerationId: g}, Recipient: &pb.DeviceRef{UserId: uuid.NewString(), DeviceId: 1, GenerationId: uuid.NewString()},
		Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: 64, Sha256: make([]byte, 32)}, OperationID: uuid.NewString()}}, WriterID: uuid.NewString(), StorageID: uuid.NewString()}
	if err := ValidateBlobWrite(r); err != nil {
		t.Fatal(err)
	}
	if ValidateBlobWrite(nil) == nil || ValidateBlobAttempt(nil) == nil {
		t.Fatal("nil writer/attempt")
	}
	for _, bad := range []domain.E2EEBlobAttemptRequest{{}, {Reservation: r.Attempt.Reservation, AttemptID: r.Attempt.AttemptID}, {Reservation: r.Attempt.Reservation, Revision: 1, AttemptID: "../bad"}, {Reservation: r.Attempt.Reservation, Revision: math.MaxUint64, AttemptID: r.Attempt.AttemptID}} {
		if ValidateBlobAttempt(&bad) == nil {
			t.Fatal("invalid attempt")
		}
	}
	r.WriterID = "invalid"
	if ValidateBlobWrite(r) == nil {
		t.Fatal("invalid server writer")
	}
}

func TestBlobCleanupBoundary(t *testing.T) {
	good := domain.E2EEBlobCleanupRequest{StorageID: uuid.NewString(), MessageID: uuid.NewString(), AttemptID: uuid.NewString(), Revision: 1, Reason: domain.E2EEBlobCleanupExpired}
	if err := ValidateBlobCleanup(&good); err != nil {
		t.Fatal(err)
	}
	if ValidateBlobCleanup(nil) == nil || ValidateBlobStorage("../root") == nil {
		t.Fatal("nil/invalid root")
	}
	for _, change := range []func(*domain.E2EEBlobCleanupRequest){
		func(r *domain.E2EEBlobCleanupRequest) { r.StorageID = "" }, func(r *domain.E2EEBlobCleanupRequest) { r.MessageID = "invalid" }, func(r *domain.E2EEBlobCleanupRequest) { r.AttemptID = "../escape" },
		func(r *domain.E2EEBlobCleanupRequest) { r.Revision = 0 }, func(r *domain.E2EEBlobCleanupRequest) { r.Revision = math.MaxUint64 }, func(r *domain.E2EEBlobCleanupRequest) { r.Reason = "arbitrary" },
		func(r *domain.E2EEBlobCleanupRequest) { r.Reason = domain.E2EEBlobCleanupFailedWriter }, func(r *domain.E2EEBlobCleanupRequest) { r.ExpectedWriterID = "invalid" },
	} {
		r := good
		change(&r)
		if ValidateBlobCleanup(&r) == nil {
			t.Fatal("invalid cleanup input accepted")
		}
	}
	good.Reason = domain.E2EEBlobCleanupFailedWriter
	good.ExpectedWriterID = uuid.NewString()
	if err := ValidateBlobCleanup(&good); err != nil {
		t.Fatal(err)
	}
}
