package chat

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func TestIdleWebSocketClosesAtJWTExpiry(t *testing.T) {
	hub := startTestHub(t)
	users := &sessionUsers{}
	users.version.Store(1)
	const secret = "test-only-expiry-secret"
	auth := service.NewAuthService(users, secret)
	raw, _ := auth.GenerateToken(1, 1)
	claims, _ := auth.ParseToken(raw)
	claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(2 * time.Second))
	raw, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(hub, w, r, auth, users, nil, nil) }))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(t, auth, raw, server.URL), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(claims.ExpiresAt.Add(time.Second))
	if _, _, err = conn.ReadMessage(); err != nil {
		t.Fatal(err)
	}
	if _, _, err = conn.ReadMessage(); err == nil {
		t.Fatal("expired idle socket still readable")
	}
	if time.Now().Before(claims.ExpiresAt.Time) {
		t.Fatal("socket closed before expiry")
	}
	// The read deadline bounds transport closure. Wall time after ReadMessage
	// also includes scheduling delay of this test goroutine under -race/load.
	if networkError, ok := err.(net.Error); ok && networkError.Timeout() {
		t.Fatal("socket did not close within its expiry read deadline", err)
	}
}

func TestCurrentLogoutClosesMatchingTabsOnly(t *testing.T) {
	hub := startTestHub(t)
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "test-only-secret")
	one, _ := auth.GenerateToken(1, 1)
	two, _ := auth.GenerateToken(1, 1)
	claims, _ := auth.ParseToken(one)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(hub, w, r, auth, users, nil, nil) }))
	defer server.Close()
	var sockets []*websocket.Conn
	for _, raw := range []string{one, one, two} {
		conn, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(t, auth, raw, server.URL), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err = conn.ReadMessage(); err != nil {
			t.Fatal(err)
		}
		sockets = append(sockets, conn)
	}
	if err := auth.RevokeSessions(1, func() error { return nil }, func() { hub.DisconnectSession(1, claims.ID) }); err != nil {
		t.Fatal(err)
	}
	for _, conn := range sockets[:2] {
		if _, _, err := conn.ReadMessage(); err == nil {
			t.Fatal("old tab survived current logout")
		}
	}
	hub.Direct <- &DirectMessage{RecipientIDs: []int64{1}, Payload: []byte("other login survives")}
	if _, raw, err := sockets[2].ReadMessage(); err != nil || string(raw) != "other login survives" {
		t.Fatal("independent login disconnected", err)
	}
}

type sessionUsers struct {
	repository.UserRepository
	version     atomic.Int64
	reads       atomic.Int64
	firstRead   chan struct{}
	releaseRead chan struct{}
	offline     atomic.Bool
}

func (u *sessionUsers) GetSessionVersion(context.Context, int64, string) (int, error) {
	if u.offline.Load() {
		return 0, errors.New("DB offline")
	}
	version := int(u.version.Load())
	if u.reads.Add(1) == 1 && u.firstRead != nil {
		close(u.firstRead)
		<-u.releaseRead
	}
	return version, nil
}

func TestWebSocketAuthenticationOutageIsUnavailable(t *testing.T) {
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "test-only-secret")
	token, _ := auth.GenerateToken(1, 1)
	url := webSocketTestURL(t, auth, token, "http://example.test")
	users.offline.Store(true)
	w := httptest.NewRecorder()
	ServeWs(NewHub(), w, httptest.NewRequest("GET", url, nil), auth, users, nil, nil)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatal("DB outage returned invalid session", w.Code)
	}
}
func (u *sessionUsers) GetByID(context.Context, int64) (*domain.User, error) {
	return &domain.User{ID: 1, Username: "test"}, nil
}

func TestRevocationClosesAllSocketsAndRejectsOldHandshake(t *testing.T) {
	hub := startTestHub(t)
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "test-only-secret")
	old, _ := auth.GenerateToken(1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(hub, w, r, auth, users, nil, nil) }))
	defer srv.Close()
	dial := func(token string) *websocket.Conn {
		t.Helper()
		c, _, err := websocket.DefaultDialer.Dial(webSocketTestURL(t, auth, token, srv.URL), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err = c.ReadMessage(); err != nil {
			t.Fatal(err)
		}
		return c
	}
	first, second := dial(old), dial(old)
	staleURL := webSocketTestURL(t, auth, old, srv.URL)
	if err := auth.RevokeSessions(1, func() error { users.version.Add(1); return nil }, func() { hub.DisconnectUser(1) }); err != nil {
		t.Fatal(err)
	}
	for _, c := range []*websocket.Conn{first, second} {
		c.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := c.ReadMessage(); err == nil {
			t.Fatal("revoked connection still readable")
		}
	}
	c, res, err := websocket.DefaultDialer.Dial(staleURL, nil)
	if c != nil {
		c.Close()
	}
	if err == nil || res == nil || res.StatusCode != http.StatusUnauthorized {
		t.Fatal("revoked handshake accepted")
	}
	fresh, _ := auth.GenerateToken(1, 2)
	dial(fresh)
}

func TestRevocationWaitsForInFlightOperationAndRejectsQueuedFrame(t *testing.T) {
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "test-only-secret")
	token, _ := auth.GenerateToken(1, 1)
	unlock := auth.LockSession(1) // An earlier frame is still being processed.
	started, done := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		auth.RevokeSessions(1, func() error { users.version.Add(1); return nil }, func() {})
		close(done)
	}()
	<-started
	select {
	case <-done:
		t.Fatal("revocation returned before earlier frame completed")
	case <-time.After(20 * time.Millisecond):
	}
	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("revocation blocked")
	}
	c := &Client{ID: 1, authService: auth, token: token} // nil repositories: revoked frames must never reach them.
	for _, kind := range []string{"", "typing", "call_invite", "call_accept", "call_reject", "call_end"} {
		if c.processAuthenticated(incomingMessage{ChatID: 1, Content: "forbidden", Type: kind}) {
			t.Fatalf("revoked %q frame accepted", kind)
		}
	}
}

func TestHandshakeValidatedBeforeRevocationCannotRegisterAfterIt(t *testing.T) {
	hub := startTestHub(t)
	users := &sessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "test-only-secret")
	token, _ := auth.GenerateToken(1, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ServeWs(hub, w, r, auth, users, nil, nil) }))
	defer srv.Close()
	url := webSocketTestURL(t, auth, token, srv.URL)
	users.reads.Store(0)
	users.firstRead, users.releaseRead = make(chan struct{}), make(chan struct{})
	done := make(chan int, 1)
	go func() {
		c, res, err := websocket.DefaultDialer.Dial(url, nil)
		if c != nil {
			c.Close()
		}
		if err != nil && res != nil {
			done <- res.StatusCode
		} else {
			done <- 0
		}
	}()
	<-users.firstRead
	auth.RevokeSessions(1, func() error { users.version.Add(1); return nil }, func() { hub.DisconnectUser(1) })
	close(users.releaseRead)
	select {
	case status := <-done:
		if status != http.StatusUnauthorized {
			t.Fatalf("delayed handshake status %d", status)
		}
	case <-time.After(time.Second):
		t.Fatal("handshake blocked")
	}
}
