package e2ee

import (
	"math"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

func blobControlRequest() *v2.BlobLeaseRequest {
	g := uuid.NewString()
	return &v2.BlobLeaseRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: g},
		MessageId: uuid.NewString(), ConversationId: uuid.NewString(), CryptoEpoch: proto.Uint64(0),
		Sender:    &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: g, DeviceId: 1},
		Recipient: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: uuid.NewString(), DeviceId: 1},
		Blob:      &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: 64, Sha256: make([]byte, 32)}, OperationId: uuid.NewString()}
}
func TestBlobControlWireOwnsValidatedMetadata(t *testing.T) {
	r := blobControlRequest()
	d, err := BlobReservationFromWire(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Context.DeviceGenerationId = uuid.NewString()
	r.Sender.UserId = uuid.NewString()
	r.Recipient.UserId = uuid.NewString()
	r.Blob.Sha256[0]++
	*r.CryptoEpoch = 7
	if d.Context.DeviceGenerationId == r.Context.DeviceGenerationId || d.Sender.UserId == r.Sender.UserId ||
		d.Recipient.UserId == r.Recipient.UserId || d.Blob.Sha256[0] == r.Blob.Sha256[0] || *d.CryptoEpoch != 0 {
		t.Fatal("public request retained mutable aliases")
	}
	if _, err := BlobReservationFromWire(nil); err == nil {
		t.Fatal("nil request")
	}
	for name, mutate := range map[string]func(*v2.BlobLeaseRequest){
		"unknown":        func(r *v2.BlobLeaseRequest) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown nested": func(r *v2.BlobLeaseRequest) { r.Blob.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"context":        func(r *v2.BlobLeaseRequest) { r.Context = nil },
		"control3":       func(r *v2.BlobLeaseRequest) { r.Context.ProtocolVersion = 3 },
		"sender":         func(r *v2.BlobLeaseRequest) { r.Sender = nil },
		"recipient":      func(r *v2.BlobLeaseRequest) { r.Recipient = nil },
		"epoch":          func(r *v2.BlobLeaseRequest) { r.CryptoEpoch = nil },
		"ref":            func(r *v2.BlobLeaseRequest) { r.Blob = nil },
		"overflow":       func(r *v2.BlobLeaseRequest) { r.ExpectedRevision = math.MaxUint64 },
		"CAS presence":   func(r *v2.BlobLeaseRequest) { r.ExpectedRevision = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			r := blobControlRequest()
			mutate(r)
			if _, err := BlobReservationFromWire(r); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
}
func TestBlobLeaseObservationBoundary(t *testing.T) {
	r := blobControlRequest()
	now := time.Now().UnixMilli()
	g := &v2.BlobLease{MessageId: r.MessageId, OperationId: r.OperationId, AttemptId: uuid.NewString(), Revision: 1,
		ReservedAtUnixMs: now, ExpiresAtUnixMs: now + time.Hour.Milliseconds(), ObservedAtUnixMs: now, State: v2.BlobLeaseState_BLOB_LEASE_STATE_RESERVED}
	for _, state := range []v2.BlobLeaseState{1, 2, 3, 4, 5, 6} {
		g.State = state
		if err := ValidateBlobLease(r, g); err != nil {
			t.Fatal(state, err)
		}
	}
	if ValidateBlobLease(r, nil) == nil || ValidateBlobLease(nil, g) == nil {
		t.Fatal("nil observation/request")
	}
	for name, mutate := range map[string]func(*v2.BlobLease){
		"message":           func(g *v2.BlobLease) { g.MessageId = uuid.NewString() },
		"operation":         func(g *v2.BlobLease) { g.OperationId = uuid.NewString() },
		"attempt":           func(g *v2.BlobLease) { g.AttemptId = "../bad" },
		"revision":          func(g *v2.BlobLease) { g.Revision++ },
		"deadline":          func(g *v2.BlobLease) { g.ExpiresAtUnixMs++ },
		"missing time":      func(g *v2.BlobLease) { g.ReservedAtUnixMs = 0 },
		"clock":             func(g *v2.BlobLease) { g.ObservedAtUnixMs-- },
		"false expiry":      func(g *v2.BlobLease) { g.Expired = true },
		"unknown state":     func(g *v2.BlobLease) { g.State = 77 },
		"unspecified state": func(g *v2.BlobLease) { g.State = 0 },
		"unknown":           func(g *v2.BlobLease) { g.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(g).(*v2.BlobLease)
			mutate(bad)
			if ValidateBlobLease(r, bad) == nil {
				t.Fatal("invalid observation")
			}
		})
	}
	g.ObservedAtUnixMs = g.ExpiresAtUnixMs
	if ValidateBlobLease(r, g) == nil {
		t.Fatal("exclusive deadline")
	}
	g.Expired = true
	if err := ValidateBlobLease(r, g); err != nil {
		t.Fatal(err)
	}
	r.ExpectedRevision = 1
	r.ExpectedAttemptId = uuid.NewString()
	g.Revision = 2
	if err := ValidateBlobLease(r, g); err != nil {
		t.Fatal("renewal observation", err)
	}
}
