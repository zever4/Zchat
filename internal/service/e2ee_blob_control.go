package service

import (
	"context"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
)

// Auth/session/device guards belong to the native RPC boundary. Implementations
// must remain short metadata-only operations, without body/FS I/O under guards.
type E2EEBlobControls interface {
	Reserve(context.Context, int64, *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error)
	Observe(context.Context, int64, *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error)
	AcknowledgeFiles(context.Context, int64, *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error)
	FileStatuses(context.Context, int64, *v2.GetFileStatusesRequest) (*v2.FileStatuses, error)
}
