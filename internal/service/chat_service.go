package service

import (
	"context"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

type ChatService struct {
	chatRepo    repository.ChatRepository
	messageRepo repository.MessageRepository
}

func NewChatService(cr repository.ChatRepository, mr repository.MessageRepository) *ChatService {
	return &ChatService{chatRepo: cr, messageRepo: mr}
}

func (s *ChatService) GetUserChats(ctx context.Context, userID int64) ([]*domain.Chat, error) {
	return s.chatRepo.GetUserChats(ctx, userID)
}

func (s *ChatService) GetChatHistory(ctx context.Context, chatID, lastID, userID int64, limit int) ([]*domain.Message, bool, error) {
	limit, err := domain.NormalizeHistoryLimit(limit)
	if err != nil || chatID <= 0 || lastID < 0 {
		return nil, false, domain.ErrInvalidInput
	}
	isMember, err := s.chatRepo.IsMember(ctx, chatID, userID)
	if err != nil {
		return nil, false, err
	}
	if !isMember {
		return nil, false, domain.ErrNotChatMember
	}

	// The repository returns ascending IDs; the extra oldest row is only a probe.
	history, err := s.messageRepo.GetHistory(ctx, chatID, lastID, limit+1)
	if err != nil {
		return nil, false, err
	}
	hasMore := len(history) > limit
	if hasMore {
		history = history[len(history)-limit:]
	}
	return history, hasMore, nil
}
