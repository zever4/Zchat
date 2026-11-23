package service

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type E2EEConversationRepository interface {
	Create(context.Context, int64, *pb.CreateConversationRequest) (*domain.E2EEConversation, error)
	Get(context.Context, int64, *pb.GetConversationRequest) (*domain.E2EEConversation, error)
}

type E2EEConversationService struct{ repo E2EEConversationRepository }

func NewE2EEConversationService(repo E2EEConversationRepository) *E2EEConversationService {
	return &E2EEConversationService{repo: repo}
}

func (s *E2EEConversationService) Create(ctx context.Context, userID int64, req *pb.CreateConversationRequest) (*domain.E2EEConversation, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateCreateConversation(req); err != nil {
		return nil, err
	}
	if userID == req.PeerUserId {
		return nil, domain.ErrInvalidInput
	}
	return s.repo.Create(ctx, userID, req)
}

func (s *E2EEConversationService) Get(ctx context.Context, userID int64, req *pb.GetConversationRequest) (*domain.E2EEConversation, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateGetConversation(req); err != nil {
		return nil, err
	}
	return s.repo.Get(ctx, userID, req)
}
