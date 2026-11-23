package grpc

import (
	"context"
	"encoding/json"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	chatpb "github.com/zever4/Zchat/gen/chat"
	userpb "github.com/zever4/Zchat/gen/user"
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

type historyChats struct{ repository.ChatRepository }

func (historyChats) IsMember(context.Context, int64, int64) (bool, error) { return true, nil }

type historyMessages struct{ repository.MessageRepository }

func (historyMessages) GetHistory(_ context.Context, chatID, lastID int64, limit int) ([]*domain.Message, error) {
	if limit < 1 || limit > 101 {
		panic("unbounded repository request")
	}
	rows := make([]*domain.Message, limit)
	for i := range rows {
		rows[i] = &domain.Message{ID: int64(i + 1), CreatedAt: time.Now()}
	}
	return rows, nil
}

func TestHistoryLimitsAcrossGRPCAndGateway(t *testing.T) {
	users := &rpcSessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "history-rpc-test-secret-at-least-32-bytes")
	userServer := NewUserGRPCServer(users, auth)
	listener := bufconn.Listen(1 << 20)
	server := grpcgo.NewServer(grpcgo.MaxRecvMsgSize(MaxRequestSize), grpcgo.UnaryInterceptor(userServer.AuthInterceptor))
	chatServer := NewChatGRPCServer(historyMessages{}, *service.NewChatService(historyChats{}, historyMessages{}))
	chatpb.RegisterChatServiceServer(server, chatServer)
	userpb.RegisterUserServiceServer(server, userServer)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	conn, err := grpcgo.NewClient("passthrough:///history", grpcgo.WithTransportCredentials(insecure.NewCredentials()), grpcgo.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	token, _ := auth.GenerateToken(1, 1)
	ctx, cancel := context.WithTimeout(metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token)), 5*time.Second)
	defer cancel()
	client := chatpb.NewChatServiceClient(conn)
	for _, tc := range []struct {
		name  string
		req   *chatpb.GetChatHistoryRequest
		want  codes.Code
		count int
	}{
		{"default", &chatpb.GetChatHistoryRequest{ChatId: 1}, codes.OK, 50},
		{"maximum", &chatpb.GetChatHistoryRequest{ChatId: 1, Limit: 100}, codes.OK, 100},
		{"negative", &chatpb.GetChatHistoryRequest{ChatId: 1, Limit: -1}, codes.InvalidArgument, 0},
		{"too large", &chatpb.GetChatHistoryRequest{ChatId: 1, Limit: 101}, codes.InvalidArgument, 0},
		{"integer maximum", &chatpb.GetChatHistoryRequest{ChatId: 1, Limit: 1<<31 - 1}, codes.InvalidArgument, 0},
		{"negative cursor", &chatpb.GetChatHistoryRequest{ChatId: 1, LastMessageId: -1}, codes.InvalidArgument, 0},
		{"invalid chat", &chatpb.GetChatHistoryRequest{}, codes.InvalidArgument, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res, err := client.GetChatHistory(ctx, tc.req)
			if status.Code(err) != tc.want {
				t.Fatalf("status=%v; want %v", err, tc.want)
			}
			if err == nil && (len(res.Messages) != tc.count || !res.HasMore) {
				t.Fatalf("count=%d more=%v", len(res.Messages), res.HasMore)
			}
		})
	}
	mux := runtime.NewServeMux(runtime.WithIncomingHeaderMatcher(CustomMatcher))
	if err = chatpb.RegisterChatServiceHandler(context.Background(), mux, conn); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		query string
		want  int
	}{{"", 200}, {"?limit=0", 200}, {"?limit=-1", 400}, {"?limit=101", 400}, {"?last_message_id=-1", 400}, {"?limit=2147483648", 400}} {
		r := httptest.NewRequest("GET", "/api/chats/1/messages"+tc.query, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != tc.want {
			t.Fatalf("%s: status=%d want=%d", tc.query, w.Code, tc.want)
		}
		if w.Code == 200 {
			var body struct {
				Messages []json.RawMessage `json:"messages"`
				More     bool              `json:"hasMore"`
			}
			if err = json.Unmarshal(w.Body.Bytes(), &body); err != nil || len(body.Messages) != 50 || !body.More {
				t.Fatalf("default page invalid: count=%d more=%v error=%v", len(body.Messages), body.More, err)
			}
		}
	}
	_, err = userpb.NewUserServiceClient(conn).UpdateProfile(ctx, &userpb.UpdateProfileRequest{Bio: strings.Repeat("x", MaxRequestSize)})
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized native RPC reached service", err)
	}
}
