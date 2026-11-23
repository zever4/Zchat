package grpc

import (
	"context"
	"errors"
	"net"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	pb "github.com/zever4/Zchat/gen/user"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type rpcSessionUsers struct {
	repository.UserRepository
	version atomic.Int64
	offline atomic.Bool
}

func (u *rpcSessionUsers) GetSessionVersion(ctx context.Context, _ int64, _ string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if u.offline.Load() {
		return 0, errors.New("DB unavailable")
	}
	return int(u.version.Load()), nil
}
func (u *rpcSessionUsers) GetByID(ctx context.Context, id int64) (*domain.User, error) {
	return &domain.User{ID: id, Username: "RPC user"}, nil
}

func TestGRPCSessionValidation(t *testing.T) {
	users := &rpcSessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "grpc-session-test-secret-at-least-32")
	userServer := NewUserGRPCServer(users, auth)
	listener := bufconn.Listen(1 << 20)
	server := grpcgo.NewServer(grpcgo.UnaryInterceptor(userServer.AuthInterceptor))
	pb.RegisterUserServiceServer(server, userServer)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	conn, err := grpcgo.NewClient("passthrough:///sessions", grpcgo.WithTransportCredentials(insecure.NewCredentials()), grpcgo.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewUserServiceClient(conn)
	token, _ := auth.GenerateToken(1, 1)
	t.Run("HTTP gateway forwards exactly one credential", func(t *testing.T) {
		mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(CustomMatcher))
		if err := pb.RegisterUserServiceHandler(context.Background(), mux, conn); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct {
			name  string
			auth  []string
			alias string
			want  int
		}{
			{"valid", []string{"Bearer " + token}, "", 200},
			{"alias only", nil, "Bearer " + token, 401},
			{"duplicate", []string{"Bearer " + token, "Bearer " + token}, "", 401},
			{"alias cannot override", []string{"Bearer " + token}, "Bearer invalid", 200},
		} {
			t.Run(tc.name, func(t *testing.T) {
				r := httptest.NewRequest("GET", "/api/profile", nil)
				for _, value := range tc.auth {
					r.Header.Add("Authorization", value)
				}
				if tc.alias != "" {
					r.Header.Set("Grpc-Metadata-Authorization", tc.alias)
				}
				w := httptest.NewRecorder()
				mux.ServeHTTP(w, r)
				if w.Code != tc.want {
					t.Fatalf("status %d expected %d: %s", w.Code, tc.want, w.Body.String())
				}
			})
		}
	})
	for _, tc := range []struct {
		name string
		md   metadata.MD
		want codes.Code
	}{
		{"valid", metadata.Pairs("authorization", "Bearer "+token), codes.OK},
		{"missing", metadata.MD{}, codes.Unauthenticated},
		{"raw token", metadata.Pairs("authorization", token), codes.Unauthenticated},
		{"alias only", metadata.Pairs("grpc-metadata-authorization", "Bearer "+token), codes.Unauthenticated},
		{"duplicate", metadata.Pairs("authorization", "Bearer "+token, "authorization", "Bearer "+token), codes.Unauthenticated},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), tc.md), time.Second)
			defer cancel()
			_, err := client.GetMyProfile(ctx, &pb.GetMyProfileRequest{})
			if status.Code(err) != tc.want {
				t.Fatalf("status %s expected %s", status.Code(err), tc.want)
			}
		})
	}
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	users.offline.Store(true)
	if _, err = client.GetMyProfile(ctx, &pb.GetMyProfileRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatal("DB outage treated as logout", err)
	}
	users.offline.Store(false)
	users.version.Add(1)
	if _, err = client.GetMyProfile(ctx, &pb.GetMyProfileRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatal("revoked token accepted", err)
	}
}

func TestGRPCRevocationWaitsForInFlightOperation(t *testing.T) {
	users := &rpcSessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "grpc-session-test-secret-at-least-32")
	server := NewUserGRPCServer(users, auth)
	token, _ := auth.GenerateToken(1, 1)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(finished)
		_, err := server.AuthInterceptor(ctx, nil, &grpcgo.UnaryServerInfo{}, func(context.Context, any) (any, error) { close(entered); <-release; return nil, nil })
		if err != nil {
			t.Error(err)
		}
	}()
	<-entered
	revoked := make(chan error, 1)
	go func() {
		revoked <- auth.RevokeSessions(1, func() error { users.version.Add(1); return nil }, func() {})
	}()
	select {
	case <-revoked:
		t.Fatal("revoke skipped in-flight RPC")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-finished
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	executed := false
	_, err := server.AuthInterceptor(ctx, nil, &grpcgo.UnaryServerInfo{}, func(context.Context, any) (any, error) { executed = true; return nil, nil })
	if status.Code(err) != codes.Unauthenticated || executed {
		t.Fatal("revoked RPC reached handler", err)
	}
}
