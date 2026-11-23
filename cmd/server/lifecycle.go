package main

import (
	"context"
	"net/http"

	"github.com/zever4/Zchat/internal/chat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func serverContextInterceptor(server context.Context) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		stop := context.AfterFunc(server, cancel)
		defer stop()
		if server.Err() != nil {
			return nil, status.Error(codes.Unavailable, "server stopping")
		}
		return handler(ctx, req)
	}
}

func shutdownServers(ctx context.Context, httpServer *http.Server, grpcServer *grpc.Server, hub *chat.Hub) error {
	hub.Stop()
	finished := make(chan struct{}, 3)
	go func() { grpcServer.GracefulStop(); finished <- struct{}{} }()
	go func() {
		if err := httpServer.Shutdown(ctx); err != nil {
			_ = httpServer.Close()
		}
		finished <- struct{}{}
	}()
	go func() { _ = hub.Wait(ctx); finished <- struct{}{} }()
	for range 3 {
		select {
		case <-finished:
		case <-ctx.Done():
			// Stop may contend with GracefulStop while a handler ignores cancellation.
			// Request it without extending the caller's shutdown deadline.
			go grpcServer.Stop()
			_ = httpServer.Close()
			return ctx.Err()
		}
	}
	return ctx.Err()
}
