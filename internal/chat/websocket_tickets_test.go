package chat

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/zever4/Zchat/internal/service"
)

func webSocketTestURL(t *testing.T, auth *service.AuthService, raw, serverURL string) string {
	t.Helper()
	u, err := url.Parse(serverURL)
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := auth.IssueWebSocketTicket(context.Background(), raw, u.Host)
	if err != nil {
		t.Fatal("ticket issuance failed", err)
	}
	return "ws" + strings.TrimPrefix(serverURL, "http") + "?ticket=" + ticket.Ticket
}

func TestWebSocketTicketConcurrentHandshakeAndReplay(t *testing.T) {
	hub := startTestHub(t)
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "websocket-ticket-test-secret")
	raw, _ := auth.GenerateToken(1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(hub, w, r, auth, users, nil, nil) }))
	defer srv.Close()
	address := webSocketTestURL(t, auth, raw, srv.URL)
	var successes atomic.Int64
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			conn, res, err := websocket.DefaultDialer.Dial(address, nil)
			if conn != nil {
				defer conn.Close()
			}
			if err == nil {
				successes.Add(1)
				conn.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := conn.ReadMessage(); err != nil {
					t.Error("authorized socket failed", err)
				}
			} else if res == nil || res.StatusCode != 401 {
				t.Error("replay returned unexpected result")
			}
		}()
	}
	group.Wait()
	if successes.Load() != 1 {
		t.Fatal("ticket created multiple sockets", successes.Load())
	}
}

func TestWebSocketTicketRejectsOriginAndLegacyCredentialsBeforeConsume(t *testing.T) {
	hub := startTestHub(t)
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "websocket-ticket-test-secret")
	raw, _ := auth.GenerateToken(1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(hub, w, r, auth, users, nil, nil) }))
	defer srv.Close()
	address := webSocketTestURL(t, auth, raw, srv.URL)
	base := "ws" + strings.TrimPrefix(srv.URL, "http")
	for _, tc := range []struct {
		url     string
		headers http.Header
		status  int
	}{
		{address, http.Header{"Origin": {"http://evil.test"}}, 403},
		{address, http.Header{"Origin": {"null"}}, 403},
		{base + "?token=" + raw, nil, 401},
		{address + "&token=" + raw, nil, 401},
		{address + "&ticket=duplicate", nil, 401},
		{address, http.Header{"Authorization": {"Bearer " + raw}}, 401},
	} {
		conn, res, err := websocket.DefaultDialer.Dial(tc.url, tc.headers)
		if conn != nil {
			conn.Close()
		}
		if err == nil || res == nil || res.StatusCode != tc.status {
			t.Fatalf("forbidden credential/origin accepted: expected %d", tc.status)
		}
	}
	conn, _, err := websocket.DefaultDialer.Dial(address, http.Header{"Origin": {srv.URL}})
	if err != nil {
		t.Fatal("bad Origin/query burned legitimate ticket", err)
	}
	conn.Close()
}

func TestFailedWebSocketUpgradeBurnsTicket(t *testing.T) {
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "websocket-ticket-test-secret")
	raw, _ := auth.GenerateToken(1, 1)
	address := webSocketTestURL(t, auth, raw, "http://example.test")
	r := httptest.NewRequest("GET", address, nil)
	w := httptest.NewRecorder()
	ServeWs(NewHub(), w, r, auth, users, nil, nil)
	if w.Code != 400 {
		t.Fatal("invalid upgrade returned unexpected result", w.Code)
	}
	w = httptest.NewRecorder()
	ServeWs(NewHub(), w, r, auth, users, nil, nil)
	if w.Code != 401 {
		t.Fatal("failed upgrade ticket reusable", w.Code)
	}
}
