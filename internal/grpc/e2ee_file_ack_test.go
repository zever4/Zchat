package grpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fileAckFake struct {
	service.E2EEBlobControls
	ack func(context.Context, int64, *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error)
}

func (f fileAckFake) AcknowledgeFiles(c context.Context, uid int64, r *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
	return f.ack(c, uid, r)
}
func fileAckRPCRequest(t *testing.T) *v2.AcknowledgeFilesRequest {
	t.Helper()
	r := blobSendRPCRequest()
	b, err := boundary.EpochSendBinding(r.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	r.Context.DeviceGenerationId = b.Recipient.GenerationId
	return &v2.AcknowledgeFilesRequest{Context: r.Context, Files: []*v2.FileAcknowledgement{{Binding: b, Blob: r.Envelope.Blob}}}
}
func fileAckRPCReply(r *v2.AcknowledgeFilesRequest) *v2.FileAcknowledgements {
	s := &v2.FileAcknowledgements{}
	for _, a := range r.Files {
		s.Files = append(s.Files, &v2.FileAcknowledgementReceipt{Acknowledgement: proto.Clone(a).(*v2.FileAcknowledgement), AcknowledgedAtUnixMs: 123})
	}
	return s
}
func TestFileAckRPCRejectsInvalidAndMalformedSuccessAndSanitizesErrors(t *testing.T) {
	r := fileAckRPCRequest(t)
	ctx := context.WithValue(t.Context(), e2eePrincipalKey{}, e2eePrincipal{userID: 42, device: &domain.E2EEDevice{UserID: 42, GenerationID: r.Context.DeviceGenerationId}})
	var calls int
	s := NewE2EEEpochServer(nil).WithBlobControls(fileAckFake{ack: func(_ context.Context, uid int64, req *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
		calls++
		if uid != 42 {
			t.Fatal(uid)
		}
		return fileAckRPCReply(req), nil
	}})
	if got, err := s.AcknowledgeFiles(t.Context(), r); got != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatal(got, err)
	}
	for _, bad := range []*v2.AcknowledgeFilesRequest{nil, {}, proto.Clone(r).(*v2.AcknowledgeFilesRequest)} {
		if bad != nil && bad.Context != nil {
			bad.Files[0].Blob.Sha256 = nil
		}
		if got, err := s.AcknowledgeFiles(ctx, bad); got != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatal(got, err)
		}
	}
	if calls != 0 {
		t.Fatal("invalid reached storage")
	}
	if got, err := s.AcknowledgeFiles(ctx, r); err != nil || boundary.ValidateFileAcknowledgementReceipts(r, got) != nil {
		t.Fatal(got, err)
	}
	for _, mutation := range []func(*v2.FileAcknowledgements){
		func(s *v2.FileAcknowledgements) { s.Files = nil },
		func(s *v2.FileAcknowledgements) { s.Files[0].AcknowledgedAtUnixMs = 0 },
		func(s *v2.FileAcknowledgements) { s.Files[0].Acknowledgement.Blob.Sha256[0] ^= 1 },
		func(s *v2.FileAcknowledgements) { s.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		s.WithBlobControls(fileAckFake{ack: func(_ context.Context, _ int64, r *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
			reply := fileAckRPCReply(r)
			mutation(reply)
			return reply, nil
		}})
		if got, err := s.AcknowledgeFiles(ctx, r); got != nil || status.Code(err) != codes.Unavailable {
			t.Fatal("malformed success", got, err)
		}
	}
	for _, tc := range []struct {
		cause error
		code  codes.Code
	}{{domain.ErrE2EEFileAckUnavailable, codes.NotFound}, {domain.ErrE2EEBlobNotReady, codes.FailedPrecondition}, {context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded}, {errors.New("private SQL/path"), codes.Unavailable}} {
		s.WithBlobControls(fileAckFake{ack: func(context.Context, int64, *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
			return nil, tc.cause
		}})
		got, err := s.AcknowledgeFiles(ctx, r)
		st := status.Convert(err)
		if got != nil || st.Code() != tc.code || len(st.Details()) != 0 || st.Message() == "private SQL/path" {
			t.Fatal(got, err)
		}
	}
	s.blobs = nil
	if _, err := s.AcknowledgeFiles(ctx, r); status.Code(err) != codes.FailedPrecondition {
		t.Fatal(err)
	}
}

// Auth/guard fixture with synthetic accepted replies, NOT file-storage proof.
// Actual accepted receipts/files and native Signal are covered by owned interop.
func TestFileAckRPCTLSCurrentDeviceAndRevokedSessionPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	r := fileAckRPCRequest(t)
	r.Context = f.wire(2)
	r.Files[0].Binding.Sender = f.device.ActiveDevice.Reference
	r.Files[0].Binding.Recipient = f.bob.ActiveDevice.Reference
	r.Files[0].Binding.ConversationId = f.chat.ConversationId
	var calls atomic.Int32
	f.epoch.WithBlobControls(fileAckFake{ack: func(_ context.Context, uid int64, req *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
		calls.Add(1)
		if uid != f.bobID {
			return nil, domain.ErrE2EEFileAckUnavailable
		}
		return fileAckRPCReply(req), nil
	}})
	for name, ctx := range map[string]context.Context{"no JWT": t.Context(), "no credential": f.ctx(t, f.bobToken, nil), "wrong credential": f.ctx(t, f.bobToken, f.aliceRaw), "wrong generation owner": f.ownerContext(t, 1)} {
		t.Run(name, func(t *testing.T) {
			if got, err := f.v2.AcknowledgeFiles(ctx, r); got != nil || status.Code(err) != codes.Unauthenticated {
				t.Fatal(got, err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("auth bypass")
	}
	bad := proto.Clone(r).(*v2.AcknowledgeFilesRequest)
	bad.Context.DeviceGenerationId = uuid.NewString()
	if got, err := f.v2.AcknowledgeFiles(f.ownerContext(t, 2), bad); got != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatal(got, err)
	}
	if got, err := f.v2.AcknowledgeFiles(f.ownerContext(t, 2), r); err != nil || boundary.ValidateFileAcknowledgementReceipts(r, got) != nil {
		t.Fatal(got, err)
	}
	if err := f.auth.Logout(t.Context(), f.bobToken, false, f.guard.DisconnectSession); err != nil {
		t.Fatal(err)
	}
	if got, err := f.v2.AcknowledgeFiles(f.ownerContext(t, 2), r); got != nil || status.Code(err) != codes.Unauthenticated || calls.Load() != 1 {
		t.Fatal(got, err, calls.Load())
	}
}

func TestFileAckRPCHoldsSessionGuardUntilMetadataCompletionPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	r := fileAckRPCRequest(t)
	r.Context = f.wire(2)
	r.Files[0].Binding.Recipient = f.bob.ActiveDevice.Reference
	r.Files[0].Binding.Sender = f.device.ActiveDevice.Reference
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.epoch.WithBlobControls(fileAckFake{ack: func(ctx context.Context, _ int64, r *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return fileAckRPCReply(r), nil
		}
	}})
	ctx, cancel := context.WithTimeout(f.ownerContext(t, 2), 5*time.Second)
	defer cancel()
	ack := make(chan error, 1)
	go func() { _, err := f.v2.AcknowledgeFiles(ctx, r); ack <- err }()
	select {
	case <-entered:
	case err := <-ack:
		t.Fatal("FILEACK did not enter metadata boundary", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	logout := make(chan error, 1)
	go func() { logout <- f.auth.Logout(ctx, f.bobToken, false, f.guard.DisconnectSession) }()
	select {
	case err := <-logout:
		t.Fatal("logout crossed in-flight metadata guard", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-ack; err != nil {
		t.Fatal(err)
	}
	if err := <-logout; err != nil {
		t.Fatal(err)
	}
	if _, err := f.v2.AcknowledgeFiles(f.ownerContext(t, 2), r); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
}
