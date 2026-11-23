package e2ee

import (
	"bytes"
	"math"
	"os"
	"testing"

	"github.com/google/uuid"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

func fileAckVector(t *testing.T) (*v2.AcknowledgeFilesRequest, *v2.FileAcknowledgements) {
	t.Helper()
	r := &v2.AcknowledgeFilesRequest{}
	s := &v2.FileAcknowledgements{}
	for name, m := range map[string]proto.Message{"request": r, "response": s} {
		wire, err := os.ReadFile("../../proto/fixtures/file-ack-v1/" + name + ".bin")
		if err != nil {
			t.Fatal(err)
		}
		if err = proto.Unmarshal(wire, m); err != nil {
			t.Fatal(err)
		}
		encoded, err := proto.Marshal(m)
		if err != nil || !bytes.Equal(wire, encoded) {
			t.Fatal("cross-generator wire drift", name, err)
		}
	}
	return r, s
}
func TestFileAckPublicVectorAndExactReceipt(t *testing.T) {
	r, s := fileAckVector(t)
	if err := ValidateFileAcknowledgementReceipts(r, s); err != nil {
		t.Fatal(err)
	}
	if r.Files[0].Binding.Epoch != (1<<53)+17 {
		t.Fatal("epoch lost integer precision")
	}
	// These are shape validators, not a COMMIT, current scope or file proof.
	r.Files[0].Binding.Epoch = 0
	s.Files[0].Acknowledgement.Binding.Epoch = 0
	if err := ValidateFileAcknowledgementReceipts(r, s); err != nil {
		t.Fatal("initial epoch", err)
	}
}
func TestFileAckRejectsInvalidAndUnknownFields(t *testing.T) {
	if ValidateFileAcknowledgements(nil) == nil {
		t.Fatal("nil batch")
	}
	for name, mutate := range map[string]func(*v2.AcknowledgeFilesRequest){
		"context":                 func(r *v2.AcknowledgeFilesRequest) { r.Context = nil },
		"control version":         func(r *v2.AcknowledgeFilesRequest) { r.Context.ProtocolVersion = 3 },
		"no files":                func(r *v2.AcknowledgeFilesRequest) { r.Files = nil },
		"nil file":                func(r *v2.AcknowledgeFilesRequest) { r.Files[0] = nil },
		"nil binding":             func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding = nil },
		"nil ref":                 func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob = nil },
		"text binding":            func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.ProtocolVersion = 2 },
		"epoch overflow":          func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.Epoch = math.MaxUint64 },
		"wrong current recipient": func(r *v2.AcknowledgeFilesRequest) { r.Context.DeviceGenerationId = uuid.NewString() },
		"digest":                  func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.EnvelopeDigest = nil },
		"hash":                    func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob.Sha256 = nil },
		"device":                  func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Binding.Recipient.DeviceId = 2 },
		"ref version":             func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob.FormatVersion = 2 },
		"ref size":                func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob.CiphertextBytes = 0 },
		"duplicate": func(r *v2.AcknowledgeFilesRequest) {
			r.Files = append(r.Files, proto.Clone(r.Files[0]).(*v2.FileAcknowledgement))
		},
		"mixed accounts": func(r *v2.AcknowledgeFilesRequest) {
			a := proto.Clone(r.Files[0]).(*v2.FileAcknowledgement)
			a.Binding.MessageId = uuid.NewString()
			a.Binding.Recipient.UserId = uuid.NewString()
			r.Files = append(r.Files, a)
		},
		"unknown batch": func(r *v2.AcknowledgeFilesRequest) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown item":  func(r *v2.AcknowledgeFilesRequest) { r.Files[0].ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown ref":   func(r *v2.AcknowledgeFilesRequest) { r.Files[0].Blob.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			r, _ := fileAckVector(t)
			mutate(r)
			if ValidateFileAcknowledgements(r) == nil {
				t.Fatal("invalid batch accepted")
			}
		})
	}
}
func TestFileAckBatchBoundAndResponseOrder(t *testing.T) {
	r, s := fileAckVector(t)
	item := r.Files[0]
	r.Files = nil
	s.Files = nil
	for i := 0; i < MaxFileAcknowledgements; i++ {
		a := proto.Clone(item).(*v2.FileAcknowledgement)
		a.Binding.MessageId = uuid.NewString()
		r.Files = append(r.Files, a)
		s.Files = append(s.Files, &v2.FileAcknowledgementReceipt{Acknowledgement: proto.Clone(a).(*v2.FileAcknowledgement), AcknowledgedAtUnixMs: 1700000000123})
	}
	if err := ValidateFileAcknowledgementReceipts(r, s); err != nil {
		t.Fatal(err)
	}
	r.Files = append(r.Files, proto.Clone(item).(*v2.FileAcknowledgement))
	if ValidateFileAcknowledgements(r) == nil {
		t.Fatal("unbounded batch")
	}
	r.Files = r.Files[:MaxFileAcknowledgements]
	s.Files[0], s.Files[1] = s.Files[1], s.Files[0]
	if ValidateFileAcknowledgementReceipts(r, s) == nil {
		t.Fatal("reordered receipts")
	}
	s.Files[0], s.Files[1] = s.Files[1], s.Files[0]
	s.Files[0].AcknowledgedAtUnixMs = 0
	if ValidateFileAcknowledgementReceipts(r, s) == nil {
		t.Fatal("missing timestamp")
	}
	s.Files[0].AcknowledgedAtUnixMs = 1700000000123
	s.Files[0].Acknowledgement.Blob.Sha256[0] ^= 1
	if ValidateFileAcknowledgementReceipts(r, s) == nil {
		t.Fatal("changed ref")
	}
	if ValidateFileAcknowledgementReceipts(r, nil) == nil {
		t.Fatal("nil response")
	}
}
