package grpc

import (
	"context"
	"errors"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Live JWT/session/device guards cover the metadata read COMMIT. No nested auth,
// ResolveRead/FS/TTL/active-epoch gate: an expired old file may acquire a late ACK.
func (s *E2EEEpochServer) GetFileStatuses(ctx context.Context, input *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
	uid, err := epochOwner(ctx, input.GetContext())
	if err != nil {
		return nil, err
	}
	if err = boundary.ValidateFileStatusRequest(input); err != nil {
		return nil, epochStatus(err)
	}
	if s.blobs == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE blob storage is not available")
	}
	result, err := s.blobs.FileStatuses(ctx, uid, input)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrE2EEFileStatusUnavailable):
			return nil, status.Error(codes.NotFound, "E2EE attachment status unavailable")
		case errors.Is(err, domain.ErrE2EEBlobNotReady):
			return nil, status.Error(codes.FailedPrecondition, "E2EE blob storage is not available")
		default:
			return nil, epochStatus(err)
		}
	}
	if boundary.ValidateFileStatuses(input, result) != nil {
		return nil, epochStatus(domain.ErrE2EEMessageState)
	}
	return epochReply(result, nil)
}
