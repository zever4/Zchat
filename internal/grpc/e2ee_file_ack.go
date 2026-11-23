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

// The native interceptor holds live session/device guards through this short
// metadata COMMIT. No ResolveRead/FS/TTL gate: late ACK retains only a statement.
func (s *E2EEEpochServer) AcknowledgeFiles(ctx context.Context, input *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
	uid, err := epochOwner(ctx, input.GetContext())
	if err != nil {
		return nil, err
	}
	if err = boundary.ValidateFileAcknowledgements(input); err != nil {
		return nil, epochStatus(err)
	}
	if s.blobs == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE blob storage is not available")
	}
	result, err := s.blobs.AcknowledgeFiles(ctx, uid, input)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrE2EEFileAckUnavailable):
			return nil, status.Error(codes.NotFound, "E2EE attachment acknowledgement unavailable")
		case errors.Is(err, domain.ErrE2EEBlobNotReady):
			return nil, status.Error(codes.FailedPrecondition, "E2EE blob storage is not available")
		default:
			return nil, epochStatus(err)
		}
	}
	if boundary.ValidateFileAcknowledgementReceipts(input, result) != nil {
		return nil, epochStatus(domain.ErrE2EEMessageState)
	}
	return epochReply(result, nil)
}
