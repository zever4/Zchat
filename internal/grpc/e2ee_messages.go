package grpc

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *E2EEDeviceServer) SendMessage(ctx context.Context, req *pb.SendMessageRequest) (*pb.SendReceipt, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if !s.prototypeData {
		return nil, retiredE2EEData()
	}
	if s.messages == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE persistence is not ready")
	}
	result, err := s.messages.Send(ctx, uid, req)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	// The repository has committed. Notify never performs network I/O or changes
	// the receipt on failure; duplicate/lost hints are recovered by polling.
	if s.wakeups != nil {
		s.wakeups.Notify(req.Envelope.Recipient.GenerationId)
	}
	return &pb.SendReceipt{MessageId: result.MessageID, EnvelopeDigest: result.Digest[:], StoredAtUnixMs: result.StoredAt.UnixMilli(), ExpiresAtUnixMs: result.ExpiresAt.UnixMilli()}, nil
}

func (s *E2EEDeviceServer) GetPendingMessages(ctx context.Context, req *pb.GetPendingMessagesRequest) (*pb.PendingMessages, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if !s.prototypeData {
		return nil, retiredE2EEData()
	}
	if s.messages == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE persistence is not ready")
	}
	result, err := s.messages.Pending(ctx, uid, req)
	return result, e2eeStatus(err)
}

func (s *E2EEDeviceServer) AcknowledgeMessages(ctx context.Context, req *pb.AcknowledgeMessagesRequest) (*pb.Empty, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if !s.prototypeData {
		return nil, retiredE2EEData()
	}
	if s.messages == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE persistence is not ready")
	}
	if err := s.messages.Acknowledge(ctx, uid, req); err != nil {
		return nil, e2eeStatus(err)
	}
	return &pb.Empty{}, nil
}

func (s *E2EEDeviceServer) GetMessageStatuses(ctx context.Context, req *pb.GetMessageStatusesRequest) (*pb.MessageStatuses, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if !s.prototypeData {
		return nil, retiredE2EEData()
	}
	if s.messages == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE persistence is not ready")
	}
	result, err := s.messages.Statuses(ctx, uid, req)
	return result, e2eeStatus(err)
}
