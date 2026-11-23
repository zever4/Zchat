package service

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type E2EEMessageRepository interface {
	Send(context.Context, int64, *pb.SendMessageRequest) (*domain.E2EESendReceipt, error)
	Pending(context.Context, int64, *pb.GetPendingMessagesRequest) (*pb.PendingMessages, error)
	Acknowledge(context.Context, int64, *pb.AcknowledgeMessagesRequest) error
	Statuses(context.Context, int64, *pb.GetMessageStatusesRequest) (*pb.MessageStatuses, error)
}

func (s *E2EEMessageService) Pending(ctx context.Context, userID int64, req *pb.GetPendingMessagesRequest) (*pb.PendingMessages, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidatePendingMessages(req); err != nil {
		return nil, err
	}
	return s.repo.Pending(ctx, userID, req)
}

func (s *E2EEMessageService) Acknowledge(ctx context.Context, userID int64, req *pb.AcknowledgeMessagesRequest) error {
	if userID <= 0 {
		return domain.ErrInvalidInput
	}
	if err := boundary.ValidateAcknowledgements(req); err != nil {
		return err
	}
	return s.repo.Acknowledge(ctx, userID, req)
}

func (s *E2EEMessageService) Statuses(ctx context.Context, userID int64, req *pb.GetMessageStatusesRequest) (*pb.MessageStatuses, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateMessageStatuses(req); err != nil {
		return nil, err
	}
	return s.repo.Statuses(ctx, userID, req)
}

type E2EEMessageService struct{ repo E2EEMessageRepository }

func NewE2EEMessageService(repo E2EEMessageRepository) *E2EEMessageService {
	return &E2EEMessageService{repo: repo}
}

func (s *E2EEMessageService) Send(ctx context.Context, userID int64, req *pb.SendMessageRequest) (*domain.E2EESendReceipt, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateSend(req); err != nil {
		return nil, err
	}
	return s.repo.Send(ctx, userID, req)
}
