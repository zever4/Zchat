package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/e2ee/wakeup"
	"github.com/zever4/Zchat/internal/redis"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

func TestE2EERPCWatchInboxPersistencePostgres(t *testing.T) {
	f, send, bobRaw := messageRPCFixture(t)
	hints := wakeup.New(redis.NewClient("127.0.0.1:1"))
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() { defer close(done); hints.Run(ctx) }()
	t.Cleanup(func() {
		stop()
		select {
		case <-done:
		case <-time.After(4 * time.Second):
			t.Error("hub leaked")
		}
	})
	f.server.WithWakeups(hints)
	bob := f.ctx(t, f.bobToken, bobRaw)
	request := &pb.StreamMessagesRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: send.Envelope.Recipient.GenerationId}}
	stream, err := f.client.WatchInbox(bob, request)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := stream.Recv()
	if err != nil || initial.GetProtocolVersion() != 1 {
		t.Fatal("initial reconciliation hint", err)
	}
	if _, err = f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_hint_send() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'reject'; END $$;
CREATE CONSTRAINT TRIGGER reject_hint_send AFTER INSERT ON e2ee_send_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_hint_send()`); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.SendMessage(f.ctx(t, f.aliceToken, f.aliceRaw), send)
	rpcCode(t, err, codes.Unavailable)
	received := make(chan error, 1)
	go func() {
		hint, err := stream.Recv()
		if err == nil && hint.ProtocolVersion != 1 {
			err = context.Canceled
		}
		received <- err
	}()
	select {
	case err := <-received:
		t.Fatal("hint before durable commit", err)
	case <-time.After(350 * time.Millisecond):
	}
	if _, err = f.db.ExecContext(t.Context(), `DROP TRIGGER reject_hint_send ON e2ee_send_receipts; DROP FUNCTION reject_hint_send()`); err != nil {
		t.Fatal(err)
	}
	receipt, err := f.client.SendMessage(f.ctx(t, f.aliceToken, f.aliceRaw), send)
	if err != nil {
		t.Fatal("unavailable Redis changed persisted send", err)
	}
	select {
	case err := <-received:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("post-commit hint missing")
	}
	for range 8 {
		if _, err := f.client.SendMessage(f.ctx(t, f.aliceToken, f.aliceRaw), send); err != nil {
			t.Fatal(err)
		}
	}
	page, err := f.client.GetPendingMessages(bob, &pb.GetPendingMessagesRequest{Context: request.Context})
	if err != nil || len(page.GetMessages()) != 1 || !proto.Equal(page.Messages[0].Envelope, send.Envelope) {
		t.Fatal("hints changed inbox", err)
	}
	if _, err = f.client.AcknowledgeMessages(bob, &pb.AcknowledgeMessagesRequest{Context: request.Context, Messages: []*pb.Acknowledgement{{MessageId: receipt.MessageId, EnvelopeDigest: receipt.EnvelopeDigest, Disposition: 1}}}); err != nil {
		t.Fatal(err)
	}
	if _, err = f.client.SendMessage(f.ctx(t, f.aliceToken, f.aliceRaw), send); err != nil {
		t.Fatal(err)
	}
	page, err = f.client.GetPendingMessages(bob, &pb.GetPendingMessagesRequest{Context: request.Context})
	if err != nil || len(page.GetMessages()) != 0 {
		t.Fatal("retry hint restored ACKed body", err)
	}
}

func TestE2EERPCWatchInboxAuthAndLifetimePostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, false)
	f.server.WithWakeups(wakeup.New(nil))
	recvCode := func(ctx context.Context, request *pb.StreamMessagesRequest, want codes.Code) {
		t.Helper()
		stream, err := f.client.WatchInbox(ctx, request)
		if err == nil {
			_, err = stream.Recv()
		}
		rpcCode(t, err, want)
	}
	recvCode(t.Context(), f.streamRequest(), codes.Unauthenticated)
	bad := proto.Clone(f.streamRequest()).(*pb.StreamMessagesRequest)
	bad.Context.DeviceGenerationId = uuid.NewString()
	recvCode(f.ctx(t, f.aliceToken, f.aliceRaw), bad, codes.Unauthenticated)
	bad = proto.Clone(f.streamRequest()).(*pb.StreamMessagesRequest)
	bad.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 1})
	recvCode(f.ctx(t, f.aliceToken, f.aliceRaw), bad, codes.InvalidArgument)
	f.guard.watchLifetime = 400 * time.Millisecond
	stream, err := f.client.WatchInbox(f.ctx(t, f.aliceToken, f.aliceRaw), f.streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = stream.Recv(); err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	rpcCode(t, err, codes.DeadlineExceeded)
}

func TestE2EERPCWatchInboxRevocationPostgres(t *testing.T) {
	for _, mode := range []string{"logout", "replace", "cancel", "root"} {
		t.Run(mode, func(t *testing.T) {
			f := newDeviceRPCFixture(t, false)
			f.server.WithWakeups(wakeup.New(nil))
			ctx, cancel := context.WithCancel(f.ctx(t, f.aliceToken, f.aliceRaw))
			defer cancel()
			stream, err := f.client.WatchInbox(ctx, f.streamRequest())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = stream.Recv(); err != nil {
				t.Fatal(err)
			}
			want := codes.Unauthenticated
			switch mode {
			case "logout":
				err = f.auth.Logout(t.Context(), f.aliceToken, false, func(int64, string) {})
			case "replace":
				_, err = f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET retired_at=clock_timestamp() WHERE generation_id=$1`, f.device.ActiveDevice.Reference.GenerationId)
			case "cancel":
				cancel()
				want = codes.Canceled
			case "root":
				f.rootCancel()
				want = codes.Canceled
			}
			if err != nil {
				t.Fatal(err)
			}
			_, err = stream.Recv()
			rpcCode(t, err, want)
		})
	}
}
