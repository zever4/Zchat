package grpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func messageRPCFixture(t *testing.T) (*deviceRPCFixture, *pb.SendMessageRequest, []byte) {
	t.Helper()
	f := newDeviceRPCFixture(t, false)
	bobRaw := bytes.Repeat([]byte{86}, 32)
	hash := service.E2EEDeviceCredentialHash(bobRaw)
	bob, err := f.client.RegisterDevice(f.ctx(t, f.bobToken, bobRaw), &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 37, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:]})
	if err != nil {
		t.Fatal(err)
	}
	for _, party := range []struct {
		token string
		raw   []byte
		gen   string
	}{{f.aliceToken, f.aliceRaw, f.device.ActiveDevice.Reference.GenerationId}, {f.bobToken, bobRaw, bob.ActiveDevice.Reference.GenerationId}} {
		if _, err := f.client.UploadPreKeys(f.ctx(t, party.token, party.raw), testutil.PublicPreKeyUpload(t, party.gen)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')`, f.aliceID, f.bobID); err != nil {
		t.Fatal(err)
	}
	contextual := &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.device.ActiveDevice.Reference.GenerationId}
	c, err := f.client.CreateConversation(f.ctx(t, f.aliceToken, f.aliceRaw), &pb.CreateConversationRequest{Context: contextual, CreationRequestId: uuid.NewString(), PeerUserId: f.bobID})
	if err != nil {
		t.Fatal(err)
	}
	return f, &pb.SendMessageRequest{Context: contextual, Envelope: &pb.Envelope{ProtocolVersion: 1, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: c.ConversationId, Sender: c.OwnDevice, Recipient: c.PeerDevice.Reference, MessageType: 1, Ciphertext: []byte("synthetic RPC ciphertext")}}, bobRaw
}

func TestE2EERPCSendPersistencePostgres(t *testing.T) {
	f, req, _ := messageRPCFixture(t)
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	_, err := f.client.SendMessage(context.Background(), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.SendMessage(f.ctx(t, f.aliceToken, nil), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.SendMessage(f.ctx(t, f.aliceToken, bytes.Repeat([]byte{1}, 32)), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.SendMessage(f.ctx(t, f.bobToken, f.aliceRaw), req)
	rpcCode(t, err, codes.Unauthenticated)
	for _, where := range []string{"request", "context", "sender"} {
		bad := proto.Clone(req).(*pb.SendMessageRequest)
		unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
		switch where {
		case "request":
			bad.ProtoReflect().SetUnknown(unknown)
		case "context":
			bad.Context.ProtoReflect().SetUnknown(unknown)
		default:
			bad.Envelope.Sender.ProtoReflect().SetUnknown(unknown)
		}
		_, err := f.client.SendMessage(ctx, bad)
		rpcCode(t, err, codes.InvalidArgument)
	}
	first, err := f.client.SendMessage(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	replay, err := f.client.SendMessage(ctx, req)
	if err != nil || !proto.Equal(first, replay) {
		t.Fatal("persistence receipt retry drift", err)
	}
	var inbox, receipts int
	if err := f.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_message_inbox),(SELECT count(*) FROM e2ee_send_receipts)`).Scan(&inbox, &receipts); err != nil || inbox != 1 || receipts != 1 {
		t.Fatal("missing durable message or duplicate retry", err)
	}
	bad := proto.Clone(req).(*pb.SendMessageRequest)
	bad.Envelope.Ciphertext = []byte("different")
	_, err = f.client.SendMessage(ctx, bad)
	rpcCode(t, err, codes.AlreadyExists)
	bad = proto.Clone(req).(*pb.SendMessageRequest)
	bad.Envelope.MessageId = uuid.NewString()
	bad.Envelope.Sender.UserId = uuid.NewString()
	_, err = f.client.SendMessage(ctx, bad)
	rpcCode(t, err, codes.InvalidArgument)
	caps, err := f.client.GetCapabilities(ctx, &pb.Empty{})
	if err != nil || caps.MessagingAvailable {
		t.Fatal("Send alone advertised delivery", err)
	}
	_, err = f.client.AcknowledgeMessages(ctx, &pb.AcknowledgeMessagesRequest{Context: req.Context, Messages: []*pb.Acknowledgement{{MessageId: first.MessageId, EnvelopeDigest: first.EnvelopeDigest, Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}})
	rpcCode(t, err, codes.NotFound)
}

func TestE2EERPCSendFailedCommitDoesNotAcknowledgePostgres(t *testing.T) {
	f, req, _ := messageRPCFixture(t)
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	if _, err := f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_rpc_send() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'private SQL diagnostic must not leak'; END $$;
        CREATE CONSTRAINT TRIGGER reject_rpc_send AFTER INSERT ON e2ee_send_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_rpc_send()`); err != nil {
		t.Fatal(err)
	}
	got, err := f.client.SendMessage(ctx, req)
	rpcCode(t, err, codes.Unavailable)
	if got != nil {
		t.Fatal("failed COMMIT acknowledged")
	}
	var count int
	if err := f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_send_receipts`).Scan(&count); err != nil || count != 0 {
		t.Fatal("failed COMMIT persisted receipt", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_rpc_send ON e2ee_send_receipts; DROP FUNCTION reject_rpc_send()`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.SendMessage(ctx, req); err != nil {
		t.Fatal("same UUID failed after rollback", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_delivery_state SET pending_messages=10000 WHERE generation_id=$1`, req.Envelope.Recipient.GenerationId); err != nil {
		t.Fatal(err)
	}
	newReq := proto.Clone(req).(*pb.SendMessageRequest)
	newReq.Envelope.MessageId = uuid.NewString()
	_, err = f.client.SendMessage(ctx, newReq)
	rpcCode(t, err, codes.ResourceExhausted)
	if _, err := f.client.SendMessage(ctx, req); err != nil {
		t.Fatal("quota blocked saved receipt", err)
	}
}

func TestE2EERPCSendRetryAfterPeerReplacementPostgres(t *testing.T) {
	f, req, bobRaw := messageRPCFixture(t)
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	first, err := f.client.SendMessage(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	newRaw := bytes.Repeat([]byte{87}, 32)
	hash := service.E2EEDeviceCredentialHash(newRaw)
	_, err = f.client.ReplaceOwnDevice(f.ctx(t, f.bobToken, newRaw), &pb.ReplaceOwnDeviceRequest{ProtocolVersion: 1, ReplacementRequestId: uuid.NewString(), ExpectedGenerationId: req.Envelope.Recipient.GenerationId, RegistrationId: 38, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:], CurrentPassword: deviceTestPassword})
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.client.SendMessage(ctx, req)
	if err != nil || !proto.Equal(first, again) {
		t.Fatal("peer replacement lost past receipt", err)
	}
	changed := proto.Clone(req).(*pb.SendMessageRequest)
	changed.Envelope.MessageId = uuid.NewString()
	_, err = f.client.SendMessage(ctx, changed)
	rpcCode(t, err, codes.FailedPrecondition)
	changed.Context.DeviceGenerationId = req.Envelope.Recipient.GenerationId
	changed.Envelope.Sender, changed.Envelope.Recipient = changed.Envelope.Recipient, changed.Envelope.Sender
	_, err = f.client.SendMessage(f.ctx(t, f.bobToken, bobRaw), changed)
	rpcCode(t, err, codes.Unauthenticated)
}
