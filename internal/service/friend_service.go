package service

import (
	"context"
	"errors"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

type FriendService struct {
	friendRepo   repository.FriendRequestRepository
	chatRepo     repository.ChatRepository
	userRepo     repository.UserRepository
	transactions repository.FriendTransactor
}

func NewFriendService(fr repository.FriendRequestRepository, cr repository.ChatRepository, ur repository.UserRepository, transactions repository.FriendTransactor) *FriendService {
	return &FriendService{friendRepo: fr, chatRepo: cr, userRepo: ur, transactions: transactions}
}

// The transaction owns every repository call in a state transition. There is no
// nontransactional production fallback. Pair locks also serialize crossed requests.
func (s *FriendService) withPair(ctx context.Context, first, second int64, work func(context.Context, *FriendService) error) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return s.transactions.WithPair(ctx, first, second, func(fr repository.FriendRequestRepository, cr repository.ChatRepository, ur repository.UserRepository) error {
		bound := &FriendService{friendRepo: fr, chatRepo: cr, userRepo: ur}
		return work(ctx, bound)
	})
}

func (s *FriendService) SendRequest(ctx context.Context, from, to int64) (*domain.FriendRequest, error) {
	if from == to {
		return nil, domain.ErrSelfFriendRequest
	}
	var req *domain.FriendRequest
	err := s.withPair(ctx, from, to, func(ctx context.Context, bound *FriendService) error {
		var err error
		req, err = bound.sendRequest(ctx, from, to)
		return err
	})
	if err != nil {
		return nil, err
	}
	return req, nil
}

func (s *FriendService) AcceptRequest(ctx context.Context, requestID, userID int64) (*domain.Chat, error) {
	req, err := s.friendRepo.GetByID(ctx, requestID)
	if err != nil {
		return nil, err
	}
	if req.ToUserID != userID {
		return nil, domain.ErrFriendRequestNotFound
	}
	var chat *domain.Chat
	err = s.withPair(ctx, req.FromUserID, req.ToUserID, func(ctx context.Context, bound *FriendService) error {
		var err error
		chat, err = bound.acceptRequest(ctx, requestID, userID)
		return err
	})
	if err != nil {
		return nil, err
	}
	return chat, nil
}

// OpenPrivateChat serializes explicit creation with friendship acceptance and
// settings writes. The recipient controls incoming starts; a sender can
// choose to initiate a conversation even with incoming starts disabled.
func (s *FriendService) OpenPrivateChat(ctx context.Context, senderID, peerID int64) (*domain.Chat, error) {
	if peerID <= 0 || senderID == peerID {
		return nil, domain.ErrInvalidInput
	}
	var chat *domain.Chat
	err := s.withPair(ctx, senderID, peerID, func(ctx context.Context, bound *FriendService) error {
		var err error
		chat, err = bound.chatRepo.GetPrivateChatBetween(ctx, senderID, peerID)
		if err != nil {
			return err
		}
		if chat == nil {
			settings, err := bound.userRepo.GetSettings(ctx, peerID)
			if err != nil {
				return err
			}
			if settings == nil || !settings.AllowDirectMessages {
				return domain.ErrDirectMessagesDisabled
			}
			if settings.AllowDirectMessagesFromNonFriends == nil || !*settings.AllowDirectMessagesFromNonFriends {
				friends, err := bound.friendRepo.AreFriends(ctx, senderID, peerID)
				if err != nil {
					return err
				}
				if !friends {
					return domain.ErrDirectMessagesDisabled
				}
			}
			chat, err = bound.chatRepo.CreateWithMembers(ctx, "private", senderID, peerID)
			if err != nil {
				return err
			}
		}
		return bound.setPrivatePeer(ctx, chat, peerID)
	})
	if err != nil {
		return nil, err
	}
	return chat, nil
}

func (s *FriendService) RejectRequest(ctx context.Context, requestID, userID int64) error {
	req, err := s.friendRepo.GetByID(ctx, requestID)
	if err != nil {
		return err
	}
	if req.ToUserID != userID {
		return domain.ErrFriendRequestNotFound
	}
	return s.withPair(ctx, req.FromUserID, req.ToUserID, func(ctx context.Context, bound *FriendService) error {
		return bound.rejectRequest(ctx, requestID, userID)
	})
}

func (s *FriendService) sendRequest(ctx context.Context, fromUserID, toUserID int64) (*domain.FriendRequest, error) {
	if fromUserID == toUserID {
		return nil, domain.ErrSelfFriendRequest
	}

	settings, err := s.userRepo.GetSettings(ctx, toUserID)
	if err != nil {
		return nil, err
	}
	if settings == nil || !settings.AllowFriendRequests {
		return nil, domain.ErrFriendRequestsDisabled
	}

	friends, err := s.friendRepo.AreFriends(ctx, fromUserID, toUserID)
	if err != nil {
		return nil, err
	}
	if friends {
		return nil, domain.ErrAlreadyFriends
	}

	existing, err := s.friendRepo.GetBetweenUsers(ctx, fromUserID, toUserID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Status == domain.FriendRequestPending {
			// If the other user already sent us a request, auto-accept
			if existing.FromUserID == toUserID {
				if _, err := s.acceptRequest(ctx, existing.ID, fromUserID); err != nil {
					return nil, err
				}
				existing.Status = domain.FriendRequestAccepted
				return existing, nil
			}
			return nil, domain.ErrFriendRequestExists
		}
		if existing.Status == domain.FriendRequestAccepted {
			return nil, domain.ErrAlreadyFriends
		}
		if existing.Status == domain.FriendRequestRejected {
			if err := s.friendRepo.ReopenRequest(ctx, existing.ID, fromUserID, toUserID); err != nil {
				return nil, err
			}
			return s.friendRepo.GetByID(ctx, existing.ID)
		}
		return nil, domain.ErrFriendRequestNotPending
	}

	req := &domain.FriendRequest{
		FromUserID: fromUserID,
		ToUserID:   toUserID,
		Status:     domain.FriendRequestPending,
	}

	_, err = s.friendRepo.Create(ctx, req)
	if err != nil {
		return nil, err
	}

	return req, nil
}

func (s *FriendService) acceptRequest(ctx context.Context, requestID, userID int64) (*domain.Chat, error) {
	req, err := s.friendRepo.GetByID(ctx, requestID)
	if err != nil {
		return nil, err
	}

	if req.ToUserID != userID {
		return nil, domain.ErrFriendRequestNotFound
	}

	if req.Status != domain.FriendRequestPending && req.Status != domain.FriendRequestAccepted {
		return nil, domain.ErrFriendRequestNotPending
	}

	existing, err := s.checkNewPrivateChat(ctx, req.FromUserID, req.ToUserID)
	if err != nil && !errors.Is(err, domain.ErrDirectMessagesDisabled) {
		return nil, err
	}
	if req.Status == domain.FriendRequestPending {
		if err := s.friendRepo.UpdateStatus(ctx, requestID, domain.FriendRequestAccepted); err != nil {
			return nil, err
		}
	}
	if errors.Is(err, domain.ErrDirectMessagesDisabled) {
		// Joining the friends list does not authorize a new chat.
		return nil, nil
	}

	chat := existing
	if chat == nil {
		chat, err = s.chatRepo.CreateWithMembers(ctx, "private", req.FromUserID, req.ToUserID)
		if err != nil {
			return nil, err
		}
	}

	_ = s.setPrivatePeer(ctx, chat, req.FromUserID)
	return chat, nil
}

func (s *FriendService) setPrivatePeer(ctx context.Context, chat *domain.Chat, peerID int64) error {
	peer, err := s.userRepo.GetByID(ctx, peerID)
	if err != nil {
		return err
	}
	chat.OtherUsername = peer.Username
	chat.OtherUserID = peer.ID
	chat.OtherAvatarURL = peer.AvatarURL
	return nil
}

func (s *FriendService) rejectRequest(ctx context.Context, requestID, userID int64) error {
	req, err := s.friendRepo.GetByID(ctx, requestID)
	if err != nil {
		return err
	}

	if req.ToUserID != userID {
		return domain.ErrFriendRequestNotFound
	}

	if req.Status == domain.FriendRequestRejected {
		return nil
	}
	if req.Status != domain.FriendRequestPending {
		return domain.ErrFriendRequestNotPending
	}

	return s.friendRepo.UpdateStatus(ctx, requestID, domain.FriendRequestRejected)
}

func (s *FriendService) GetPendingRequests(ctx context.Context, userID int64) ([]*domain.FriendRequest, error) {
	return s.friendRepo.GetPendingForUser(ctx, userID)
}

func (s *FriendService) GetFriends(ctx context.Context, userID int64) ([]*domain.User, error) {
	return s.friendRepo.GetFriends(ctx, userID)
}

// Existing private chats remain usable when a participant disables new dialogs.
// Check both participants before any acceptance mutation creates a new chat.
func (s *FriendService) checkNewPrivateChat(ctx context.Context, first, second int64) (*domain.Chat, error) {
	existing, err := s.chatRepo.GetPrivateChatBetween(ctx, first, second)
	if err != nil || existing != nil {
		return existing, err
	}
	for _, id := range []int64{first, second} {
		settings, err := s.userRepo.GetSettings(ctx, id)
		if err != nil {
			return nil, err
		}
		if settings == nil || !settings.AllowDirectMessages {
			return nil, domain.ErrDirectMessagesDisabled
		}
	}
	return nil, nil
}
