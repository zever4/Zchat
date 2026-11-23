package grpc

import (
	"context"
	"errors"
	"github.com/zever4/Zchat/internal/admission"
	"time"

	"github.com/zever4/Zchat/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func (s *UserGRPCServer) AuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	values := md.Get("authorization")
	// Avoid ambiguous credentials, including the gateway metadata alias.
	if len(values) != 1 || len(md.Get("grpc-metadata-authorization")) != 0 {
		return nil, status.Error(codes.Unauthenticated, "token is missing or ambiguous")
	}
	token, err := service.BearerToken(values[0])
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	claims, unlock, err := s.authService.AuthorizeContext(ctx, token)
	if err != nil {
		if errors.Is(err, admission.ErrBusy) {
			_ = grpc.SetHeader(ctx, metadata.Pairs("retry-after", "1"))
			return nil, status.Error(codes.ResourceExhausted, "server busy; retry later")
		}
		if errors.Is(err, service.ErrAuthUnavailable) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.Error(codes.Unavailable, "authentication temporarily unavailable")
		}
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}
	defer unlock()
	return handler(context.WithValue(ctx, userIDKey, claims.UserID), req)
}
