package grpc

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	grpcCodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func conversationProto(c *domain.E2EEConversation) *pb.Conversation {
	result := &pb.Conversation{ConversationId: c.ID, Mode: pb.ConversationMode_CONVERSATION_MODE_SIGNAL_PQ_1, OwnDevice: deviceProto(c.OwnDevice).Reference, PeerDevice: deviceProto(c.PeerDevice)}
	if c.State == "active" {
		result.State = pb.ConversationState_CONVERSATION_STATE_ACTIVE
	} else if c.State == "closed" {
		result.State = pb.ConversationState_CONVERSATION_STATE_CLOSED
	}
	if c.CloseReason == "device_replaced" {
		result.CloseReason = pb.ConversationCloseReason_CONVERSATION_CLOSE_REASON_DEVICE_REPLACED
	}
	result.ClosedGenerationId = c.ClosedGenerationID
	if c.ClosedAt != nil {
		result.ClosedAtUnixMs = c.ClosedAt.UnixMilli()
	}
	return result
}

func (s *E2EEDeviceServer) CreateConversation(ctx context.Context, req *pb.CreateConversationRequest) (*pb.Conversation, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if s.conversations == nil {
		return nil, status.Error(grpcCodes.FailedPrecondition, "E2EE conversations are not ready")
	}
	result, err := s.conversations.Create(ctx, uid, req)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return conversationProto(result), nil
}

func (s *E2EEDeviceServer) GetConversation(ctx context.Context, req *pb.GetConversationRequest) (*pb.Conversation, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if s.conversations == nil {
		return nil, status.Error(grpcCodes.FailedPrecondition, "E2EE conversations are not ready")
	}
	result, err := s.conversations.Get(ctx, uid, req)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return conversationProto(result), nil
}
