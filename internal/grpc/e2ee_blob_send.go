package grpc

import (
	"errors"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// A bare sentinel, generic SQL/auth/commit failure or another RPC must never
// mint this bound observation. It is not a signed fence or nonacceptance proof.
func epochBlobSendStatus(f *domain.E2EEBlobSendFailure, req *v2.SendMessageRequest) error {
	failed := func() error { return status.Error(codes.Unavailable, "E2EE attachment Send detail unavailable") }
	if f == nil {
		return failed()
	}
	var reason v2.BlobSendFailureReason
	switch {
	case errors.Is(f.Cause, domain.ErrE2EEBlobNotReadyForSend):
		reason = v2.BlobSendFailureReason_BLOB_SEND_FAILURE_REASON_NOT_READY
	case errors.Is(f.Cause, domain.ErrE2EEBlobLeaseExpired):
		reason = v2.BlobSendFailureReason_BLOB_SEND_FAILURE_REASON_LEASE_EXPIRED
	default:
		return failed()
	}
	detail := &v2.BlobSendFailure{Reason: reason, Binding: f.Binding}
	if boundary.ValidateBlobSendFailure(req, detail) != nil {
		return failed()
	}
	detail = proto.Clone(detail).(*v2.BlobSendFailure)
	s, err := status.New(codes.FailedPrecondition, "E2EE attachment is not available for this exact Send").WithDetails(detail)
	if err != nil {
		return failed()
	}
	return s.Err()
}
