package grpc

import (
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func blobSendRPCRequest() *v2.SendMessageRequest {
	b := blobRPCRequest()
	return &v2.SendMessageRequest{Context: b.Context, Envelope: &v2.Envelope{ProtocolVersion: 3, CryptoSuite: 1, MessageId: b.MessageId, ConversationId: b.ConversationId, CryptoEpoch: b.CryptoEpoch, Sender: b.Sender, Recipient: b.Recipient, Blob: b.Blob, MessageType: 1, Ciphertext: []byte("synthetic private ciphertext must not be echoed")}}
}
func TestBlobSendFailureDetailsAreExactAndNeverGenericOrCrossMethod(t *testing.T) {
	r := blobSendRPCRequest()
	b, err := boundary.EpochSendBinding(r.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		cause  error
		reason v2.BlobSendFailureReason
	}{{domain.ErrE2EEBlobNotReadyForSend, 1}, {domain.ErrE2EEBlobLeaseExpired, 2}} {
		err := epochSendStatus(&domain.E2EEBlobSendFailure{Cause: tc.cause, Binding: b}, r)
		s := status.Convert(err)
		details := s.Details()
		if s.Code() != codes.FailedPrecondition || len(details) != 1 {
			t.Fatal("missing bound failure", err)
		}
		f, ok := details[0].(*v2.BlobSendFailure)
		if !ok || f.Reason != tc.reason || boundary.ValidateBlobSendFailure(r, f) != nil {
			t.Fatal("wrong binding", err)
		}
		if proto.Size(s.Proto()) > 1024 || strings.Contains(s.String(), "synthetic private") {
			t.Fatal("ciphertext leaked/unbounded", err)
		}
		wrong := proto.Clone(r).(*v2.SendMessageRequest)
		wrong.Envelope.Blob.Sha256[0]++
		if len(status.Convert(epochSendStatus(&domain.E2EEBlobSendFailure{Cause: tc.cause, Binding: b}, wrong)).Details()) != 0 {
			t.Fatal("cross-Send error")
		}
		if len(status.Convert(epochStatus(&domain.E2EEBlobSendFailure{Cause: tc.cause, Binding: b})).Details()) != 0 {
			t.Fatal("cross-method error")
		}
	}
	for _, cause := range []error{nil, context.Canceled, context.DeadlineExceeded, domain.ErrE2EEGenerationChanged, domain.ErrE2EEEpochInactive, errors.New("private SQL diagnostic")} {
		err := epochSendStatus(&domain.E2EEBlobSendFailure{Cause: cause, Binding: b}, r)
		if status.Code(err) == codes.OK || len(status.Convert(err).Details()) != 0 || strings.Contains(err.Error(), "private SQL") {
			t.Fatal("generic failure minted detail", err)
		}
	}
	for _, err := range []error{domain.ErrE2EEBlobNotReadyForSend, domain.ErrE2EEBlobLeaseExpired, (*domain.E2EEBlobSendFailure)(nil)} {
		if len(status.Convert(epochSendStatus(err, r)).Details()) != 0 {
			t.Fatal("bare/nil error minted detail")
		}
	}
	for _, req := range []*v2.SendMessageRequest{nil, {}, {Context: &pb.ClientContext{ProtocolVersion: 1}}} {
		if len(status.Convert(epochSendStatus(&domain.E2EEBlobSendFailure{Cause: domain.ErrE2EEBlobLeaseExpired, Binding: b}, req)).Details()) != 0 {
			t.Fatal("invalidrequest minted detail")
		}
	}
}
