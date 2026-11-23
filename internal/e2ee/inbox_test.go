package e2ee

import (
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/proto"
)

func TestE2EEInboxAndOutcomeBoundaries(t *testing.T) {
	ctx := deviceContext(envelopeFixture(t))
	unknown := []byte{0x98, 0x06, 0x01}
	pending := &pb.GetPendingMessagesRequest{Context: ctx}
	if err := ValidatePendingMessages(pending); err != nil {
		t.Fatal(err)
	}
	for _, where := range []string{"request", "context", "missing", "version"} {
		bad := proto.Clone(pending).(*pb.GetPendingMessagesRequest)
		switch where {
		case "request":
			bad.ProtoReflect().SetUnknown(unknown)
		case "context":
			bad.Context.ProtoReflect().SetUnknown(unknown)
		case "missing":
			bad.Context = nil
		case "version":
			bad.Context.ProtocolVersion = 2
		}
		if err := ValidatePendingMessages(bad); err == nil {
			t.Fatal("pending", where)
		}
	}
	refs := &pb.GetMessageStatusesRequest{Context: ctx, Messages: []*pb.MessageReference{{MessageId: requestID, EnvelopeDigest: make([]byte, 32)}}}
	if err := ValidateMessageStatuses(refs); err != nil {
		t.Fatal(err)
	}
	for _, where := range []string{"request", "context", "reference", "duplicate", "missing", "digest", "id", "unknown context"} {
		bad := proto.Clone(refs).(*pb.GetMessageStatusesRequest)
		switch where {
		case "request":
			bad.ProtoReflect().SetUnknown(unknown)
		case "context":
			bad.Context = nil
		case "reference":
			bad.Messages[0].ProtoReflect().SetUnknown(unknown)
		case "duplicate":
			bad.Messages = append(bad.Messages, bad.Messages[0])
		case "missing":
			bad.Messages[0] = nil
		case "digest":
			bad.Messages[0].EnvelopeDigest = nil
		case "id":
			bad.Messages[0].MessageId = ""
		case "unknown context":
			bad.Context.ProtoReflect().SetUnknown(unknown)
		}
		if err := ValidateMessageStatuses(bad); err == nil {
			t.Fatal("status", where)
		}
	}
	ack := &pb.AcknowledgeMessagesRequest{Context: ctx, Messages: []*pb.Acknowledgement{{MessageId: requestID, EnvelopeDigest: make([]byte, 32), Disposition: 1}}}
	for _, where := range []string{"request", "context", "reference"} {
		bad := proto.Clone(ack).(*pb.AcknowledgeMessagesRequest)
		switch where {
		case "request":
			bad.ProtoReflect().SetUnknown(unknown)
		case "context":
			bad.Context.ProtoReflect().SetUnknown(unknown)
		case "reference":
			bad.Messages[0].ProtoReflect().SetUnknown(unknown)
		}
		if err := ValidateAcknowledgements(bad); err == nil {
			t.Fatal("unknown ACK field", where)
		}
	}
	for refs.Messages = nil; len(refs.Messages) < MaxAcknowledgements+1; {
		refs.Messages = append(refs.Messages, &pb.MessageReference{MessageId: uuid.NewString(), EnvelopeDigest: make([]byte, 32)})
		err := ValidateMessageStatuses(refs)
		if len(refs.Messages) <= MaxAcknowledgements && err != nil || len(refs.Messages) > MaxAcknowledgements && err == nil {
			t.Fatal("status bound", len(refs.Messages), err)
		}
	}
}
