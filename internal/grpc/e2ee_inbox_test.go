package grpc

import (
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestE2EERPCInboxAckStatusAndIsolationPostgres(t *testing.T) {
	f, send, bobRaw := messageRPCFixture(t)
	alice := f.ctx(t, f.aliceToken, f.aliceRaw)
	bob := f.ctx(t, f.bobToken, bobRaw)
	receipt, err := f.client.SendMessage(alice, send)
	if err != nil {
		t.Fatal(err)
	}
	inbox := &pb.GetPendingMessagesRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: send.Envelope.Recipient.GenerationId}}
	_, err = f.client.GetPendingMessages(context.Background(), inbox)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.GetPendingMessages(alice, inbox)
	rpcCode(t, err, codes.Unauthenticated)
	page, err := f.client.GetPendingMessages(bob, inbox)
	if err != nil || len(page.GetMessages()) != 1 || !proto.Equal(page.Messages[0].Envelope, send.Envelope) {
		t.Fatal("wire inbox", err)
	}
	statusReq := &pb.GetMessageStatusesRequest{Context: send.Context, Messages: []*pb.MessageReference{{MessageId: receipt.MessageId, EnvelopeDigest: receipt.EnvelopeDigest}}}
	got, err := f.client.GetMessageStatuses(alice, statusReq)
	if err != nil || got.Messages[0].State != pb.MessageState_MESSAGE_STATE_PENDING {
		t.Fatal("pending status", err)
	}
	ack := &pb.AcknowledgeMessagesRequest{Context: inbox.Context, Messages: []*pb.Acknowledgement{{MessageId: receipt.MessageId, EnvelopeDigest: receipt.EnvelopeDigest, Disposition: 1}}}
	bad := proto.Clone(ack).(*pb.AcknowledgeMessagesRequest)
	bad.Messages[0].EnvelopeDigest[0] ^= 1
	_, err = f.client.AcknowledgeMessages(bob, bad)
	rpcCode(t, err, codes.AlreadyExists)
	bad.Messages[0].MessageId = uuid.NewString()
	_, err = f.client.AcknowledgeMessages(bob, bad)
	rpcCode(t, err, codes.NotFound)
	if _, err = f.client.AcknowledgeMessages(bob, ack); err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.AcknowledgeMessages(bob, ack); err != nil {
		t.Fatal("ACK repeat", err)
	}
	page, err = f.client.GetPendingMessages(bob, inbox)
	if err != nil || len(page.Messages) != 0 {
		t.Fatal("ACK didn't remove ciphertext", err)
	}
	got, err = f.client.GetMessageStatuses(alice, statusReq)
	if err != nil || got.Messages[0].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED || !proto.Equal(got.Messages[0].Receipt, receipt) {
		t.Fatal("recipient stored status", err)
	}
	if repeat, err := f.client.SendMessage(alice, send); err != nil || !proto.Equal(repeat, receipt) {
		t.Fatal("removed ciphertext replay", err)
	}
	foreign := proto.Clone(statusReq).(*pb.GetMessageStatusesRequest)
	foreign.Context = inbox.Context
	_, err = f.client.GetMessageStatuses(bob, foreign)
	rpcCode(t, err, codes.NotFound)
	for _, where := range []string{"request", "context", "reference"} {
		bad := proto.Clone(ack).(*pb.AcknowledgeMessagesRequest)
		unknown := []byte{0x98, 0x06, 0x01}
		switch where {
		case "request":
			bad.ProtoReflect().SetUnknown(unknown)
		case "context":
			bad.Context.ProtoReflect().SetUnknown(unknown)
		case "reference":
			bad.Messages[0].ProtoReflect().SetUnknown(unknown)
		}
		_, err := f.client.AcknowledgeMessages(bob, bad)
		rpcCode(t, err, codes.InvalidArgument)
	}
	caps, err := f.client.GetCapabilities(alice, &pb.Empty{})
	if err != nil || caps.MessagingAvailable || caps.Limits.MaxInboxMessages != 32 || caps.Limits.MaxResponseBytes != 65536 {
		t.Fatal("readiness/limits", err)
	}
}
