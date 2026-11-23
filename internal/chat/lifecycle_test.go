package chat

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func TestDisconnectAfterHubStops(t *testing.T) {
	h := NewHub()
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { h.run(ctx); close(stopped) }()
	cancel()
	<-stopped
	returned := make(chan struct{})
	go func() { h.DisconnectUser(1); close(returned) }()
	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		// Release the old implementation's blocked sender before reporting failure.
		select {
		case request := <-h.revoke:
			close(request.done)
		case <-returned:
		}
		<-returned
		t.Fatal("session revocation blocks forever after the hub has stopped")
	}
}

type waitingMessages struct {
	repository.MessageRepository
	entered  chan struct{}
	returned chan struct{}
}

func (m *waitingMessages) SaveAuthorized(ctx context.Context, msg *domain.Message) (*domain.Chat, []int64, error) {
	close(m.entered)
	<-ctx.Done()
	close(m.returned)
	return nil, nil, ctx.Err()
}
func TestHubStopCancelsFrameAndJoinsWebSocketPumps(t *testing.T) {
	h := NewHub()
	go h.Run(context.Background())
	t.Cleanup(h.Stop)
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "lifecycle-test-secret")
	token, _ := auth.GenerateToken(1, 1)
	messages := &waitingMessages{entered: make(chan struct{}), returned: make(chan struct{})}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(h, w, r, auth, users, messages, nil) }))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(t, auth, token, server.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(incomingMessage{ChatID: 1, Content: "in flight"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-messages.entered:
	case <-time.After(time.Second):
		t.Fatal("save did not start")
	}
	h.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Wait(ctx); err != nil {
		t.Fatal("pumps did not stop", err)
	}
	select {
	case <-messages.returned:
	default:
		t.Fatal("frame ignored shutdown cancellation")
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("socket survived hub shutdown")
	}
	h.UnregisterClient(&Client{})
	h.DisconnectUser(1)
	if h.RegisterClient(ctx, &Client{}) || h.Enqueue(ctx, &DirectMessage{}) {
		t.Fatal("stopped hub accepts work")
	}
}
func TestHubQueueAndRegistrationHonorCancellation(t *testing.T) {
	h := NewHub()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if h.RegisterClient(ctx, &Client{}) {
		t.Fatal("registered without an owner loop")
	}
	for i := 0; i < cap(h.Direct); i++ {
		h.Direct <- &DirectMessage{}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if h.Enqueue(ctx2, &DirectMessage{}) || !errors.Is(ctx2.Err(), context.DeadlineExceeded) {
		t.Fatal("queue ignored cancellation")
	}
	h.Stop()
	go h.Run(context.Background())
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := h.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
}
