package chat

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
)

type lockedLog struct {
	sync.Mutex
	bytes.Buffer
}

func (b *lockedLog) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}
func (b *lockedLog) text() string { b.Lock(); defer b.Unlock(); return b.Buffer.String() }

func TestWebSocketLogsDoNotExposeFramesOrCloseReasons(t *testing.T) {
	var output lockedLog
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	h := NewHub()
	go h.Run(context.Background())
	t.Cleanup(h.Stop)
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "logging-test-secret")
	token, err := auth.GenerateToken(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	messages := &recordingMessages{meta: &domain.Chat{Type: "group"}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(h, w, r, auth, users, messages, nil) }))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(t, auth, token, server.URL), nil)
	if err != nil {
		t.Fatal("WebSocket handshake failed")
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	const marker = "private-content-must-not-be-logged"
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"content":"`+marker+`"`)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteJSON(incomingMessage{ChatID: 1, Content: "still works"}); err != nil {
		t.Fatal(err)
	}
	_, response, err := conn.ReadMessage()
	if err != nil || !bytes.Contains(response, []byte("still works")) {
		t.Fatal("malformed frame broke ordinary delivery")
	}
	if err := conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, marker), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	_, _, _ = conn.ReadMessage()
	h.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	text := output.text()
	if strings.Contains(text, marker) || strings.Contains(text, token) {
		t.Fatal("WebSocket log exposes private client data")
	}
	if !strings.Contains(text, "Invalid WebSocket JSON") {
		t.Fatal("malformed frame diagnostic missing")
	}
}
