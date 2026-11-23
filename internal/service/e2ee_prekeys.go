package service

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type E2EEPreKeyRepository interface {
	Inventory(context.Context, int64, *pb.GetPreKeyInventoryRequest) (*pb.PreKeyInventory, error)
	Publish(context.Context, int64, *pb.UploadPreKeysRequest) (*domain.E2EEPublication, error)
	Acquire(context.Context, int64, *pb.AcquireBundleRequest) (*pb.AcquiredBundle, error)
}

func (s *E2EEPreKeyService) Inventory(ctx context.Context, userID int64, req *pb.GetPreKeyInventoryRequest) (*pb.PreKeyInventory, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateInventory(req); err != nil {
		return nil, err
	}
	return s.repo.Inventory(ctx, userID, req)
}

func (s *E2EEPreKeyService) Acquire(ctx context.Context, userID int64, req *pb.AcquireBundleRequest) (*pb.AcquiredBundle, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateAcquisition(req); err != nil {
		return nil, err
	}
	return s.repo.Acquire(ctx, userID, req)
}

type E2EEPreKeyService struct{ repo E2EEPreKeyRepository }

func NewE2EEPreKeyService(repo E2EEPreKeyRepository) *E2EEPreKeyService {
	return &E2EEPreKeyService{repo: repo}
}

// The authenticated adapter holds short login/device barriers. The repository
// rechecks account/generation in its SQL transaction; no network I/O under SQL locks.
func (s *E2EEPreKeyService) Publish(ctx context.Context, userID int64, req *pb.UploadPreKeysRequest) (*domain.E2EEPublication, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateUpload(req); err != nil {
		return nil, err
	}
	return s.repo.Publish(ctx, userID, req)
}
