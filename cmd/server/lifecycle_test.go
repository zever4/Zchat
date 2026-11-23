package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/chat"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"
)

type waitingHealth struct {
	pb.UnimplementedHealthServer
	entered        chan struct{}
	release        chan struct{}
	respectContext bool
}

func (h *waitingHealth) Check(ctx context.Context, _ *pb.HealthCheckRequest) (*pb.HealthCheckResponse, error) {
	close(h.entered)
	if h.respectContext {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	<-h.release
	return &pb.HealthCheckResponse{}, nil
}
func healthClient(t *testing.T, server *grpc.Server) pb.HealthClient {
	t.Helper()
	l := bufconn.Listen(1024 * 1024)
	go server.Serve(l)
	t.Cleanup(func() { server.Stop(); l.Close() })
	c, err := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return l.DialContext(ctx) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return pb.NewHealthClient(c)
}
func TestGRPCRequestCancelledOnServerStop(t *testing.T) {
	root, stop := context.WithCancel(context.Background())
	defer stop()
	h := &waitingHealth{entered: make(chan struct{}), respectContext: true}
	s := grpc.NewServer(grpc.UnaryInterceptor(serverContextInterceptor(root)))
	pb.RegisterHealthServer(s, h)
	client := healthClient(t, s)
	returned := make(chan error, 1)
	go func() { _, err := client.Check(context.Background(), &pb.HealthCheckRequest{}); returned <- err }()
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("RPC did not start")
	}
	stop()
	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("cancelled RPC succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("RPC ignored server cancellation")
	}
}
func TestShutdownForcesBlockedHTTPAndGRPCWithinBudget(t *testing.T) {
	release := make(chan struct{})
	httpEntered := make(chan struct{})
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(httpEntered); <-release }))
	h := &waitingHealth{entered: make(chan struct{}), release: release}
	s := grpc.NewServer()
	pb.RegisterHealthServer(s, h)
	client := healthClient(t, s)
	t.Cleanup(func() { close(release); httpServer.Close() })
	go func() { _, _ = client.Check(context.Background(), &pb.HealthCheckRequest{}) }()
	go func() {
		r, err := http.Get(httpServer.URL)
		if err == nil {
			r.Body.Close()
		}
	}()
	for _, started := range []chan struct{}{httpEntered, h.entered} {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("request did not start")
		}
	}
	hub := chat.NewHub()
	go hub.Run(context.Background())
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := shutdownServers(ctx, httpServer.Config, s, hub); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("missing forced-stop result", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("shutdown exceeded budget")
	}
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := hub.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}
