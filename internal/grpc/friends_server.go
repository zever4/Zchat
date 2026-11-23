package grpc

import (
	"context"
	"log"

	pb "github.com/zever4/Zchat/gen/friends"
	"github.com/zever4/Zchat/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type FriendGRPCServer struct {
	pb.UnimplementedFriendServiceServer
	friendService *service.FriendService
}

func NewFriendGRPCServer(fs *service.FriendService) *FriendGRPCServer {
	return &FriendGRPCServer{friendService: fs}
}

func (s *FriendGRPCServer) GetPendingRequests(ctx context.Context, req *pb.GetPendingRequestsRequest) (*pb.GetPendingRequestsResponse, error) {
	userID, ok := ctx.Value(userIDKey).(int64)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing user id")
	}
	

	requests, err := s.friendService.GetPendingRequests(ctx, userID)
	if err != nil {
		log.Printf("Get pending requests error: %v", err)
		return nil, status.Error(codes.Internal, "failed to get friend requests")
	}

	var pbRequests []*pb.FriendRequest
	for _, pbRequest := range requests {
		pbRequests = append(pbRequests, &pb.FriendRequest{
			Id: pbRequest.ID,
			FromUserId: pbRequest.FromUserID,
			FromUsername: pbRequest.FromUsername,
			Status: pbRequest.Status,
			CreatedAt: timestamppb.New(pbRequest.CreatedAt),
		})
	}

	return &pb.GetPendingRequestsResponse{Requests: pbRequests}, nil
}