package grpc

import (
	"context"
	"errors"

	pb "github.com/zever4/Zchat/gen/chat"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ChatGRPCServer struct {
	pb.UnimplementedChatServiceServer
	messageRepo repository.MessageRepository
	chatService service.ChatService
}

func NewChatGRPCServer(mRepo repository.MessageRepository, cService service.ChatService) *ChatGRPCServer {
	return &ChatGRPCServer{
		messageRepo: mRepo,
		chatService: cService,
	}
}

func (s *ChatGRPCServer) GetChatHistory(ctx context.Context, req *pb.GetChatHistoryRequest) (*pb.GetChatHistoryResponse, error) {
	userID, ok := ctx.Value(userIDKey).(int64)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing user id")
	}

	history, hasMore, err := s.chatService.GetChatHistory(ctx, req.ChatId, req.LastMessageId, userID, int(req.Limit))
	if err != nil {
		if errors.Is(err, domain.ErrInvalidInput) {
			return nil, status.Error(codes.InvalidArgument, "chat_id must be positive, last_message_id nonnegative, and limit between 1 and 100 (0 defaults to 50)")
		}
		if errors.Is(err, domain.ErrNotChatMember) {
			return nil, status.Error(codes.PermissionDenied, err.Error())
		}
		return nil, status.Error(codes.Internal, "failed to get chat history")
	}

	var pbHistory []*pb.Message
	for _, message := range history {
		pbHistory = append(pbHistory, &pb.Message{
			Id:           message.ID,
			Content:      message.Content,
			SenderId:     message.SenderID,
			SenderName:   message.SenderName,
			SenderAvatar: message.SenderAvatar,
			CreatedAt:    timestamppb.New(message.CreatedAt),
		})

	}

	return &pb.GetChatHistoryResponse{
		Messages: pbHistory,
		HasMore:  hasMore,
	}, nil
}

func (s *ChatGRPCServer) GetChats(ctx context.Context, req *pb.GetChatsRequest) (*pb.GetChatsResponse, error) {
	userID, ok := ctx.Value(userIDKey).(int64)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing user id")
	}

	dbChats, err := s.chatService.GetUserChats(ctx, userID)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to fetch chats")
	}

	var pbChats []*pb.Chat
	for _, c := range dbChats {
		pbChats = append(pbChats, &pb.Chat{
			Id:             c.ID,
			Type:           c.Type,
			Name:           c.Name,
			OtherUsername:  c.OtherUsername,
			OtherUserId:    c.OtherUserID,
			OtherAvatarUrl: c.OtherAvatarURL,
			CreatedAt:      timestamppb.New(c.CreatedAt),
		})
	}

	return &pb.GetChatsResponse{Chats: pbChats}, nil
}
