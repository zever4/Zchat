package e2ee

import (
	"math"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	"google.golang.org/protobuf/proto"
)

func blobReadRequest(t *testing.T) *domain.E2EEBlobReadRequest {
	t.Helper()
	e := blobEnvelope(t)
	b, err := EpochSendBinding(e)
	if err != nil {
		t.Fatal(err)
	}
	return &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: b.Recipient.GenerationId}, Binding: b, Blob: e.Blob}
}
func TestBlobReadRequestIsBoundedRecipientOnlyAttachmentIdentity(t *testing.T) {
	if err := ValidateBlobReadRequest(blobReadRequest(t)); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*domain.E2EEBlobReadRequest){
		"missing context":         func(r *domain.E2EEBlobReadRequest) { r.Context = nil },
		"old context":             func(r *domain.E2EEBlobReadRequest) { r.Context.ProtocolVersion = 1 },
		"sender context":          func(r *domain.E2EEBlobReadRequest) { r.Context.DeviceGenerationId = r.Binding.Sender.GenerationId },
		"foreign context":         func(r *domain.E2EEBlobReadRequest) { r.Context.DeviceGenerationId = uuid.NewString() },
		"missing binding":         func(r *domain.E2EEBlobReadRequest) { r.Binding = nil },
		"text binding":            func(r *domain.E2EEBlobReadRequest) { r.Binding.ProtocolVersion = 2 },
		"unknown binding version": func(r *domain.E2EEBlobReadRequest) { r.Binding.ProtocolVersion = 4 },
		"bad message":             func(r *domain.E2EEBlobReadRequest) { r.Binding.MessageId = "../file" },
		"bad chat":                func(r *domain.E2EEBlobReadRequest) { r.Binding.ConversationId = "invalid" },
		"epoch overflow":          func(r *domain.E2EEBlobReadRequest) { r.Binding.Epoch = math.MaxUint64 },
		"short digest":            func(r *domain.E2EEBlobReadRequest) { r.Binding.EnvelopeDigest = r.Binding.EnvelopeDigest[:31] },
		"long digest":             func(r *domain.E2EEBlobReadRequest) { r.Binding.EnvelopeDigest = make([]byte, 33) },
		"same account":            func(r *domain.E2EEBlobReadRequest) { r.Binding.Sender.UserId = r.Binding.Recipient.UserId },
		"same installation":       func(r *domain.E2EEBlobReadRequest) { r.Binding.Sender.GenerationId = r.Binding.Recipient.GenerationId },
		"missing recipient":       func(r *domain.E2EEBlobReadRequest) { r.Binding.Recipient = nil },
		"second device":           func(r *domain.E2EEBlobReadRequest) { r.Binding.Recipient.DeviceId = 2 },
		"missing ref":             func(r *domain.E2EEBlobReadRequest) { r.Blob = nil },
		"unknown format":          func(r *domain.E2EEBlobReadRequest) { r.Blob.FormatVersion = 2 },
		"empty ciphertext":        func(r *domain.E2EEBlobReadRequest) { r.Blob.CiphertextBytes = 36 },
		"oversized ciphertext":    func(r *domain.E2EEBlobReadRequest) { r.Blob.CiphertextBytes = MaxAttachmentBlobBytes + 1 },
		"path as blob":            func(r *domain.E2EEBlobReadRequest) { r.Blob.BlobId = "../../key" },
		"short hash":              func(r *domain.E2EEBlobReadRequest) { r.Blob.Sha256 = r.Blob.Sha256[:31] },
		"unknown context fields":  func(r *domain.E2EEBlobReadRequest) { r.Context.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown binding fields":  func(r *domain.E2EEBlobReadRequest) { r.Binding.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown ref fields":      func(r *domain.E2EEBlobReadRequest) { r.Blob.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			r := blobReadRequest(t)
			mutate(r)
			if ValidateBlobReadRequest(r) == nil {
				t.Fatal("invalid read request accepted")
			}
		})
	}
	if ValidateBlobReadRequest(nil) == nil {
		t.Fatal("nil request accepted")
	}
	// Exactly the maximum public ciphertext shape remains valid.
	r := blobReadRequest(t)
	r.Blob.CiphertextBytes = MaxAttachmentBlobBytes
	if err := ValidateBlobReadRequest(r); err != nil {
		t.Fatal(err)
	}
	// The input contains public fields only, with no raw Signal ciphertext.
	if proto.Size(r.Context)+proto.Size(r.Binding)+proto.Size(r.Blob) > 1024 {
		t.Fatal("unbounded public identity")
	}
}
