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

func fileStatusVector(t *testing.T) (*v2.GetFileStatusesRequest, *v2.FileStatuses) {
	t.Helper()
	r, s := &v2.GetFileStatusesRequest{}, &v2.FileStatuses{}
	for name, m := range map[string]proto.Message{"request": r, "response": s} {
		wire, err := os.ReadFile("../../proto/fixtures/file-status-v1/" + name + ".bin")
		if err != nil {
			t.Fatal(err)
		}
		if err = proto.Unmarshal(wire, m); err != nil {
			t.Fatal(err)
		}
		encoded, err := proto.Marshal(m)
		if err != nil || !bytes.Equal(encoded, wire) {
			t.Fatal("cross-generator wire drift", name, err)
		}
	}
	return r, s
}
func TestFileStatusPublicVectorKeepsIndependentLateFacts(t *testing.T) {
	r, s := fileStatusVector(t)
	if err := ValidateFileStatuses(r, s); err != nil {
		t.Fatal(err)
	}
	f := s.Files[0]
	if f.Reference.Binding.Epoch != (1<<53)+17 || f.ObservedAtUnixMs < f.ExpiresAtUnixMs || *f.AcknowledgedAtUnixMs <= f.ExpiresAtUnixMs || f.RecipientReplacedAtUnixMs == nil {
		t.Fatal("lost late facts/precision")
	}
	r.Files[0].Binding.Epoch, f.Reference.Binding.Epoch, *f.Receipt.CryptoEpoch = 0, 0, 0
	f.AcknowledgedAtUnixMs, f.RecipientReplacedAtUnixMs = nil, nil
	if err := ValidateFileStatuses(r, s); err != nil {
		t.Fatal("missing facts are not absence proofs", err)
	}
}
func TestFileStatusRequestRejectsInvalidScopeAndUnknownFields(t *testing.T) {
	if ValidateFileStatusRequest(nil) == nil {
		t.Fatal("nil request")
	}
	for name, mutate := range map[string]func(*v2.GetFileStatusesRequest){
		"context":         func(r *v2.GetFileStatusesRequest) { r.Context = nil },
		"context version": func(r *v2.GetFileStatusesRequest) { r.Context.ProtocolVersion = 1 },
		"recipient context": func(r *v2.GetFileStatusesRequest) {
			r.Context.DeviceGenerationId = r.Files[0].Binding.Recipient.GenerationId
		},
		"empty":         func(r *v2.GetFileStatusesRequest) { r.Files = nil },
		"nil reference": func(r *v2.GetFileStatusesRequest) { r.Files[0] = nil },
		"nil binding":   func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding = nil },
		"nil blob":      func(r *v2.GetFileStatusesRequest) { r.Files[0].Blob = nil },
		"text":          func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.ProtocolVersion = 2 },
		"epoch":         func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.Epoch = math.MaxUint64 },
		"device":        func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.Sender.DeviceId = 2 },
		"digest":        func(r *v2.GetFileStatusesRequest) { r.Files[0].Binding.EnvelopeDigest = nil },
		"ref format":    func(r *v2.GetFileStatusesRequest) { r.Files[0].Blob.FormatVersion = 2 },
		"hash":          func(r *v2.GetFileStatusesRequest) { r.Files[0].Blob.Sha256 = nil },
		"duplicate": func(r *v2.GetFileStatusesRequest) {
			r.Files = append(r.Files, proto.Clone(r.Files[0]).(*v2.FileStatusReference))
		},
		"different sender": func(r *v2.GetFileStatusesRequest) {
			ref := proto.Clone(r.Files[0]).(*v2.FileStatusReference)
			ref.Binding.MessageId = uuid.NewString()
			ref.Binding.Sender.UserId = uuid.NewString()
			r.Files = append(r.Files, ref)
		},
		"unknown batch": func(r *v2.GetFileStatusesRequest) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown ref":   func(r *v2.GetFileStatusesRequest) { r.Files[0].ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			r, _ := fileStatusVector(t)
			mutate(r)
			if ValidateFileStatusRequest(r) == nil {
				t.Fatal("invalid request accepted")
			}
		})
	}
}
func TestFileStatusResponseRejectsBindingTimeVersionAndUnknownFields(t *testing.T) {
	for name, mutate := range map[string]func(*v2.FileStatuses){
		"empty":         func(s *v2.FileStatuses) { s.Files = nil },
		"nil status":    func(s *v2.FileStatuses) { s.Files[0] = nil },
		"format0":       func(s *v2.FileStatuses) { s.Files[0].FormatVersion = 0 },
		"future format": func(s *v2.FileStatuses) { s.Files[0].FormatVersion = 2 },
		"reference":     func(s *v2.FileStatuses) { s.Files[0].Reference = nil },
		"changed ref":   func(s *v2.FileStatuses) { s.Files[0].Reference.Blob.Sha256[0] ^= 1 },
		"receipt":       func(s *v2.FileStatuses) { s.Files[0].Receipt = nil },
		"epoch missing": func(s *v2.FileStatuses) { s.Files[0].Receipt.CryptoEpoch = nil },
		"epoch changed": func(s *v2.FileStatuses) { *s.Files[0].Receipt.CryptoEpoch++ },
		"message":       func(s *v2.FileStatuses) { s.Files[0].Receipt.MessageId = uuid.NewString() },
		"digest":        func(s *v2.FileStatuses) { s.Files[0].Receipt.EnvelopeDigest[0] ^= 1 },
		"stored0":       func(s *v2.FileStatuses) { s.Files[0].Receipt.StoredAtUnixMs = 0 },
		"overflow":      func(s *v2.FileStatuses) { s.Files[0].Receipt.StoredAtUnixMs = math.MaxInt64 },
		"message ttl":   func(s *v2.FileStatuses) { s.Files[0].Receipt.ExpiresAtUnixMs++ },
		"file ttl":      func(s *v2.FileStatuses) { s.Files[0].ExpiresAtUnixMs++ },
		"clock":         func(s *v2.FileStatuses) { s.Files[0].ObservedAtUnixMs = s.Files[0].Receipt.StoredAtUnixMs - 1 },
		"ack0":          func(s *v2.FileStatuses) { zero := int64(0); s.Files[0].AcknowledgedAtUnixMs = &zero },
		"future replacement": func(s *v2.FileStatuses) {
			at := s.Files[0].ObservedAtUnixMs + 1
			s.Files[0].RecipientReplacedAtUnixMs = &at
		},
		"unknown batch":   func(s *v2.FileStatuses) { s.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown receipt": func(s *v2.FileStatuses) { s.Files[0].Receipt.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			r, s := fileStatusVector(t)
			mutate(s)
			if ValidateFileStatuses(r, s) == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
	r, _ := fileStatusVector(t)
	if ValidateFileStatuses(r, nil) == nil {
		t.Fatal("nil response")
	}
}
func TestFileStatusBoundedBatchOrderAndExclusiveDeadline(t *testing.T) {
	r, s := fileStatusVector(t)
	ref, original := r.Files[0], s.Files[0]
	r.Files = nil
	s.Files = nil
	for range MaxFileStatuses {
		ref := proto.Clone(ref).(*v2.FileStatusReference)
		ref.Binding.MessageId = uuid.NewString()
		ref.Binding.Recipient.UserId = uuid.NewString()
		f := proto.Clone(original).(*v2.FileStatus)
		f.Reference = proto.Clone(ref).(*v2.FileStatusReference)
		f.Receipt.MessageId = ref.Binding.MessageId
		r.Files = append(r.Files, ref)
		s.Files = append(s.Files, f)
	}
	if err := ValidateFileStatuses(r, s); err != nil {
		t.Fatal("mixed peers", err)
	}
	r.Files = append(r.Files, ref)
	if ValidateFileStatusRequest(r) == nil {
		t.Fatal("unbounded request")
	}
	r.Files = r.Files[:MaxFileStatuses]
	s.Files[0], s.Files[1] = s.Files[1], s.Files[0]
	if ValidateFileStatuses(r, s) == nil {
		t.Fatal("reordered response")
	}
	s.Files[0], s.Files[1] = s.Files[1], s.Files[0]
	for _, f := range s.Files {
		f.AcknowledgedAtUnixMs = nil
		f.RecipientReplacedAtUnixMs = nil
		f.ObservedAtUnixMs = f.ExpiresAtUnixMs - 1
	}
	if err := ValidateFileStatuses(r, s); err != nil {
		t.Fatal(err)
	}
	for _, f := range s.Files {
		f.ObservedAtUnixMs++
	}
	if err := ValidateFileStatuses(r, s); err != nil {
		t.Fatal(err)
	}
	s.Files[1].ObservedAtUnixMs++
	if ValidateFileStatuses(r, s) == nil {
		t.Fatal("mixed snapshot clocks")
	}
}
