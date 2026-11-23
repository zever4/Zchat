package grpc

import (
	"context"
	"errors"
	"strings"
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

type fileStatusFake struct {
	service.E2EEBlobControls
	read func(context.Context, int64, *v2.GetFileStatusesRequest) (*v2.FileStatuses, error)
}

func (f fileStatusFake) FileStatuses(c context.Context, uid int64, r *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
	return f.read(c, uid, r)
}
func fileStatusRPCRequest(t *testing.T) *v2.GetFileStatusesRequest {
	t.Helper()
	r := blobSendRPCRequest()
	b, err := boundary.EpochSendBinding(r.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	return &v2.GetFileStatusesRequest{Context: r.Context, Files: []*v2.FileStatusReference{{Binding: b, Blob: r.Envelope.Blob}}}
}
func fileStatusRPCReply(r *v2.GetFileStatusesRequest) *v2.FileStatuses {
	s := &v2.FileStatuses{}
	for _, ref := range r.Files {
		s.Files = append(s.Files, &v2.FileStatus{FormatVersion: 1, Reference: proto.Clone(ref).(*v2.FileStatusReference),
			Receipt:          &v2.SendReceipt{MessageId: ref.Binding.MessageId, EnvelopeDigest: append([]byte(nil), ref.Binding.EnvelopeDigest...), StoredAtUnixMs: 1700000000000, ExpiresAtUnixMs: 1700000000000 + boundary.FileRetentionMilliseconds, CryptoEpoch: proto.Uint64(ref.Binding.Epoch)},
			ObservedAtUnixMs: 1700000000001, ExpiresAtUnixMs: 1700000000000 + boundary.FileRetentionMilliseconds})
	}
	return s
}
func TestFileStatusRPCRejectsInvalidAndMalformedSuccessAndSanitizesErrors(t *testing.T) {
	r := fileStatusRPCRequest(t)
	ctx := context.WithValue(t.Context(), e2eePrincipalKey{}, e2eePrincipal{userID: 42, device: &domain.E2EEDevice{UserID: 42, GenerationID: r.Context.DeviceGenerationId}})
	calls := 0
	s := NewE2EEEpochServer(nil).WithBlobControls(fileStatusFake{read: func(_ context.Context, uid int64, req *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
		calls++
		if uid != 42 {
			t.Fatal(uid)
		}
		return fileStatusRPCReply(req), nil
	}})
	if got, err := s.GetFileStatuses(t.Context(), r); got != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatal(got, err)
	}
	for _, bad := range []*v2.GetFileStatusesRequest{nil, {}, proto.Clone(r).(*v2.GetFileStatusesRequest)} {
		if bad != nil && bad.Context != nil {
			bad.Files[0].Blob.Sha256 = nil
		}
		if got, err := s.GetFileStatuses(ctx, bad); got != nil || status.Code(err) != codes.InvalidArgument {
			t.Fatal(got, err)
		}
	}
	if calls != 0 {
		t.Fatal("invalid reached storage")
	}
	if got, err := s.GetFileStatuses(ctx, r); err != nil || boundary.ValidateFileStatuses(r, got) != nil {
		t.Fatal(got, err)
	}
	for _, mutate := range []func(*v2.FileStatuses){
		func(s *v2.FileStatuses) { s.Files = nil },
		func(s *v2.FileStatuses) { s.Files[0].FormatVersion = 2 },
		func(s *v2.FileStatuses) { s.Files[0].Reference.Blob.Sha256[0] ^= 1 },
		func(s *v2.FileStatuses) { s.Files[0].Receipt.CryptoEpoch = nil },
		func(s *v2.FileStatuses) { s.Files[0].ExpiresAtUnixMs++ },
		func(s *v2.FileStatuses) { s.Files[0].AcknowledgedAtUnixMs = proto.Int64(0) },
		func(s *v2.FileStatuses) { s.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		s.WithBlobControls(fileStatusFake{read: func(_ context.Context, _ int64, r *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
			reply := fileStatusRPCReply(r)
			mutate(reply)
			return reply, nil
		}})
		if got, err := s.GetFileStatuses(ctx, r); got != nil || status.Code(err) != codes.Unavailable {
			t.Fatal("malformed success", got, err)
		}
	}
	for _, tc := range []struct {
		cause error
		code  codes.Code
	}{
		{domain.ErrE2EEFileStatusUnavailable, codes.NotFound}, {domain.ErrE2EEBlobNotReady, codes.FailedPrecondition},
		{context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded},
		{errors.New("private SQL/path"), codes.Unavailable}, {domain.ErrE2EEBlobOperationNotFound, codes.Unavailable},
	} {
		s.WithBlobControls(fileStatusFake{read: func(context.Context, int64, *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
			return nil, tc.cause
		}})
		got, err := s.GetFileStatuses(ctx, r)
		st := status.Convert(err)
		if got != nil || st.Code() != tc.code || len(st.Details()) != 0 || strings.Contains(st.Message(), "private") {
			t.Fatal(got, err)
		}
	}
	s.blobs = nil
	if got, err := s.GetFileStatuses(ctx, r); got != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatal(got, err)
	}
}

// Auth/session boundary only: storage replies below are synthetic, not receipt
// or Signal proof. Real accepted metadata/native interop is exercised separately.
func TestFileStatusRPCTLSCurrentSenderAndRevokedSessionPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	r := fileStatusRPCRequest(t)
	r.Context = f.wire(1)
	r.Files[0].Binding.Sender = f.device.ActiveDevice.Reference
	r.Files[0].Binding.Recipient = f.bob.ActiveDevice.Reference
	r.Files[0].Binding.ConversationId = f.chat.ConversationId
	var calls atomic.Int32
	f.epoch.WithBlobControls(fileStatusFake{read: func(_ context.Context, uid int64, r *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
		calls.Add(1)
		if uid != f.aliceID {
			return nil, domain.ErrE2EEFileStatusUnavailable
		}
		return fileStatusRPCReply(r), nil
	}})
	for name, ctx := range map[string]context.Context{"no JWT": t.Context(), "no credential": f.ctx(t, f.aliceToken, nil), "wrong credential": f.ctx(t, f.aliceToken, f.bobRaw), "recipient owner": f.ownerContext(t, 2)} {
		t.Run(name, func(t *testing.T) {
			if got, err := f.v2.GetFileStatuses(ctx, r); got != nil || status.Code(err) != codes.Unauthenticated {
				t.Fatal(got, err)
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("auth bypass")
	}
	bad := proto.Clone(r).(*v2.GetFileStatusesRequest)
	bad.Context.DeviceGenerationId = uuid.NewString()
	if got, err := f.v2.GetFileStatuses(f.ownerContext(t, 1), bad); got != nil || status.Code(err) != codes.Unauthenticated {
		t.Fatal(got, err)
	}
	if got, err := f.v2.GetFileStatuses(f.ownerContext(t, 1), r); err != nil || boundary.ValidateFileStatuses(r, got) != nil {
		t.Fatal(got, err)
	}
	if err := f.auth.Logout(t.Context(), f.aliceToken, false, f.guard.DisconnectSession); err != nil {
		t.Fatal(err)
	}
	if got, err := f.v2.GetFileStatuses(f.ownerContext(t, 1), r); got != nil || status.Code(err) != codes.Unauthenticated || calls.Load() != 1 {
		t.Fatal(got, err, calls.Load())
	}
}
func TestFileStatusRPCHoldsSessionGuardUntilMetadataCompletionPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	r := fileStatusRPCRequest(t)
	r.Context = f.wire(1)
	r.Files[0].Binding.Sender = f.device.ActiveDevice.Reference
	r.Files[0].Binding.Recipient = f.bob.ActiveDevice.Reference
	entered, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	f.epoch.WithBlobControls(fileStatusFake{read: func(ctx context.Context, _ int64, r *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return fileStatusRPCReply(r), nil
		}
	}})
	ctx, cancel := context.WithTimeout(f.ownerContext(t, 1), 5*time.Second)
	defer cancel()
	read := make(chan error, 1)
	go func() { _, err := f.v2.GetFileStatuses(ctx, r); read <- err }()
	select {
	case <-entered:
	case err := <-read:
		t.Fatal("status did not enter metadata", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	logout := make(chan error, 1)
	go func() { logout <- f.auth.Logout(ctx, f.aliceToken, false, f.guard.DisconnectSession) }()
	select {
	case err := <-logout:
		t.Fatal("logout crossed metadata guard", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if err := <-logout; err != nil {
		t.Fatal(err)
	}
	if _, err := f.v2.GetFileStatuses(f.ownerContext(t, 1), r); status.Code(err) != codes.Unauthenticated {
		t.Fatal(err)
	}
}
