package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/zever4/Zchat/gen/user"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

type contextKey string

const userIDKey contextKey = "user_id"

type UserGRPCServer struct {
	user.UnimplementedUserServiceServer
	repo        repository.UserRepository
	authService *service.AuthService
}

func NewUserGRPCServer(repo repository.UserRepository, au *service.AuthService) *UserGRPCServer {
	return &UserGRPCServer{repo: repo, authService: au}
}

func (s *UserGRPCServer) GetMyProfile(ctx context.Context, req *user.GetMyProfileRequest) (*user.UserResponse, error) {
	userID, ok := ctx.Value(userIDKey).(int64)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing user id")
	}

	dbUser, err := s.repo.GetByID(ctx, userID)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "user not found: %v", err)
	}

	return &user.UserResponse{
		Id:        dbUser.ID,
		Username:  dbUser.Username,
		Email:     dbUser.Email,
		Bio:       dbUser.Bio,
		AvatarUrl: dbUser.AvatarURL,
		CreatedAt: timestamppb.New(dbUser.CreatedAt),
	}, nil
}

func (s *UserGRPCServer) GetUserProfile(ctx context.Context, req *user.GetUserProfileRequest) (*user.UserResponse, error) {
	_, ok := ctx.Value(userIDKey).(int64)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing user id")
	}

	dbUser, err := s.repo.GetByID(ctx, req.TargetId)
	if err != nil {
		return nil, status.Errorf(codes.NotFound, "user not found")
	}

	return &user.UserResponse{
		Id:        dbUser.ID,
		Username:  dbUser.Username,
		Bio:       dbUser.Bio,
		AvatarUrl: dbUser.AvatarURL,
		CreatedAt: timestamppb.New(dbUser.CreatedAt),
	}, nil
}

func (s *UserGRPCServer) UpdateProfile(ctx context.Context, req *user.UpdateProfileRequest) (*user.UpdateProfileResponse, error) {
	userID, ok := ctx.Value(userIDKey).(int64)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing user id")
	}

	if len([]rune(req.Bio)) > 500 {
		return nil, status.Error(codes.InvalidArgument, "bio is too long (max 500 charachters)")
	}

	if err := s.repo.UpdateProfileBio(ctx, userID, req.Bio); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update profile")
	}

	return &user.UpdateProfileResponse{
		Ok: true,
	}, nil
}
