package calls

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/livekit/protocol/auth"
	"github.com/livekit/protocol/livekit"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

const sessionA = "ad548fd6-e187-438d-9b54-afc5c5325663"
const sessionB = "af548fd6-e187-438d-9b54-afc5c5325663"

type callUsers struct {
	repository.UserRepository
	mu      sync.Mutex
	invalid map[string]bool
	offline bool
}

func (u *callUsers) GetSessionVersion(ctx context.Context, _ int64, id string) (int, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if u.offline {
		return 0, errors.New("offline")
	}
	if u.invalid[id] {
		return 0, domain.ErrInvalidSession
	}
	return 1, nil
}
func (u *callUsers) invalidate(id string) { u.mu.Lock(); defer u.mu.Unlock(); u.invalid[id] = true }

type callMembers struct {
	mu     sync.Mutex
	denied map[int64]bool
	err    error
}

func (m *callMembers) IsMember(ctx context.Context, chatID, userID int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return false, m.err
	}
	return !m.denied[userID], ctx.Err()
}
func (m *callMembers) kick(id int64) { m.mu.Lock(); defer m.mu.Unlock(); m.denied[id] = true }

type fakeRooms struct {
	livekit.RoomService
	mu         sync.Mutex
	people     map[string][]*livekit.ParticipantInfo
	removed    []string
	sockets    map[string]*websocket.Conn
	fail       bool
	failRemove bool
}

func (f *fakeRooms) ListRooms(context.Context, *livekit.ListRoomsRequest) (*livekit.ListRoomsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return nil, errors.New("offline")
	}
	r := &livekit.ListRoomsResponse{}
	for name := range f.people {
		r.Rooms = append(r.Rooms, &livekit.Room{Name: name})
	}
	return r, nil
}
func (f *fakeRooms) ListParticipants(_ context.Context, r *livekit.ListParticipantsRequest) (*livekit.ListParticipantsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &livekit.ListParticipantsResponse{Participants: append([]*livekit.ParticipantInfo(nil), f.people[r.Room]...)}, nil
}
func (f *fakeRooms) RemoveParticipant(_ context.Context, r *livekit.RoomParticipantIdentity) (*livekit.RemoveParticipantResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failRemove {
		return nil, errors.New("SFU removal unavailable")
	}
	f.removed = append(f.removed, r.Identity)
	list := f.people[r.Room]
	for i, p := range list {
		if p.Identity == r.Identity {
			f.people[r.Room] = append(list[:i], list[i+1:]...)
			break
		}
	}
	if c := f.sockets[r.Identity]; c != nil {
		_ = c.Close()
	}
	return &livekit.RemoveParticipantResponse{}, nil
}

func setupGateway(t *testing.T, upstream string) (*Gateway, *callUsers, *callMembers, *fakeRooms) {
	t.Helper()
	users := &callUsers{invalid: map[string]bool{}}
	members := &callMembers{denied: map[int64]bool{}}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g, err := NewGateway(ctx, service.NewAuthService(users, "test-app-secret-at-least-32-bytes"), members, upstream, "test-key", "test-media-secret")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(g.proxy.Transport.(*http.Transport).CloseIdleConnections)
	rooms := &fakeRooms{people: map[string][]*livekit.ParticipantInfo{}, sockets: map[string]*websocket.Conn{}}
	g.rooms = rooms
	return g, users, members, rooms
}
func mediaToken(t *testing.T, identity, room string) string {
	t.Helper()
	v := &auth.VideoGrant{RoomJoin: true, Room: room}
	s, e := auth.NewAccessToken("test-key", "test-media-secret").SetIdentity(identity).SetVideoGrant(v).SetValidFor(time.Hour).ToJWT()
	if e != nil {
		t.Fatal(e)
	}
	return s
}

func TestCallAdmissionRevalidatesSessionAndMembership(t *testing.T) {
	var paths []string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		if r.Header.Get("Cookie") != "" {
			t.Error("cookie forwarded")
		}
		w.WriteHeader(200)
	}))
	defer up.Close()
	g, users, members, _ := setupGateway(t, up.URL)
	valid := mediaToken(t, "1:"+sessionA, "2")
	for _, tc := range []struct {
		name, token, path string
		status            int
		setup             func()
	}{
		{"valid", valid, "/rtc/rtc/v1/validate", 200, nil},
		{"legacy user-only token", mediaToken(t, "1", "2"), "/rtc/rtc", 401, nil},
		{"unsigned", valid + "x", "/rtc/rtc", 401, nil},
		{"management path", valid, "/rtc/twirp/livekit.RoomService/ListRooms", 404, nil},
		{"invalid room", mediaToken(t, "1:"+sessionA, "02"), "/rtc/rtc", 403, nil},
		{"kicked", valid, "/rtc/rtc", 403, func() { members.kick(1) }},
		{"revoked", valid, "/rtc/rtc", 401, func() { users.invalidate(sessionA) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setup != nil {
				tc.setup()
			}
			r := httptest.NewRequest("GET", tc.path+"?access_token="+url.QueryEscape(tc.token), nil)
			r.Header.Set("Cookie", "private-cookie")
			w := httptest.NewRecorder()
			g.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d wanted=%d", w.Code, tc.status)
			}
		})
	}
	if len(paths) != 1 || paths[0] != "/rtc/v1/validate" {
		t.Fatal("unauthorized request reached SFU or path rewrite failed", paths)
	}
	users.mu.Lock()
	users.invalid = map[string]bool{}
	users.offline = true
	users.mu.Unlock()
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest("GET", "/rtc/rtc?access_token="+valid, nil))
	if w.Code != 503 {
		t.Fatal("DB failure opened admission", w.Code)
	}
}

func TestCallRevocationPreservesIndependentLoginAndReconcilesOrphans(t *testing.T) {
	g, users, members, rooms := setupGateway(t, "http://127.0.0.1:1")
	a, b, other := "1:"+sessionA, "1:"+sessionB, "2:"+sessionB
	rooms.people["10"] = []*livekit.ParticipantInfo{{Identity: a}, {Identity: b}, {Identity: other}}
	if err := g.DisconnectSession(context.Background(), 1, sessionA); err != nil {
		t.Fatal(err)
	}
	if len(rooms.removed) != 1 || rooms.removed[0] != a {
		t.Fatal("independent login removed", rooms.removed)
	}
	users.invalidate(sessionB)
	if err := g.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rooms.people["10"]) != 0 {
		t.Fatal("orphaned expired/revoked sessions remained")
	}
	users.mu.Lock()
	users.invalid = map[string]bool{}
	users.mu.Unlock()
	rooms.people["10"] = []*livekit.ParticipantInfo{{Identity: a}, {Identity: other}, {Identity: "legacy"}}
	members.kick(2)
	if err := g.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(rooms.people["10"]) != 1 || rooms.people["10"][0].Identity != a {
		t.Fatal("membership revalidation failed")
	}
}

func TestCallAdmissionBarrierEndsAtHandshakeAndRevocationClosesMedia(t *testing.T) {
	for _, kind := range []string{"session", "membership"} {
		t.Run(kind, func(t *testing.T) {
			entered, allow := make(chan struct{}), make(chan struct{})
			var rooms *fakeRooms
			identity := "1:" + sessionA
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-allow
				rooms.mu.Lock()
				rooms.people["2"] = []*livekit.ParticipantInfo{{Identity: identity}}
				c, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
				if err != nil {
					rooms.mu.Unlock()
					return
				}
				defer c.Close()
				rooms.sockets[identity] = c
				rooms.mu.Unlock()
				for {
					if _, _, err := c.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer up.Close()
			releaseAllow := sync.OnceFunc(func() { close(allow) })
			defer releaseAllow()
			g, users, members, f := setupGateway(t, up.URL)
			rooms = f
			proxy := httptest.NewServer(g)
			defer proxy.Close()
			dialed := make(chan *websocket.Conn, 1)
			go func() {
				c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(proxy.URL, "http")+"/rtc/rtc?access_token="+mediaToken(t, identity, "2"), nil)
				if err != nil {
					dialed <- nil
					return
				}
				dialed <- c
			}()
			<-entered
			mutated := make(chan struct{})
			revoked := make(chan error, 1)
			go func() {
				if kind == "membership" {
					revoked <- g.RevokeChat(context.Background(), 2, func() error { members.kick(1); close(mutated); return nil }, 1)
					return
				}
				revoked <- g.auth.RevokeSessionsContext(context.Background(), 1, func() error { users.invalidate(sessionA); close(mutated); return nil }, func() {
					if err := g.DisconnectSession(context.Background(), 1, sessionA); err != nil {
						t.Error(err)
					}
				})
			}()
			select {
			case <-mutated:
				t.Fatal("revocation passed in-flight SFU admission")
			case <-time.After(30 * time.Millisecond):
			}
			releaseAllow()
			c := <-dialed
			if c == nil {
				t.Fatal("failed websocket handshake")
			}
			defer c.Close()
			select {
			case err := <-revoked:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("call lifetime held revocation barrier")
			}
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			if _, _, err := c.ReadMessage(); err == nil {
				t.Fatal("active media socket survived revocation")
			} else {
				var timed net.Error
				if errors.As(err, &timed) && timed.Timeout() {
					t.Fatal("socket timed out rather than closing")
				}
			}
			w := httptest.NewRecorder()
			g.ServeHTTP(w, httptest.NewRequest("GET", "/rtc/rtc?access_token="+mediaToken(t, identity, "2"), nil))
			want := 401
			if kind == "membership" {
				want = 403
			}
			if w.Code != want {
				t.Fatal("saved grant rejoined", w.Code)
			}
		})
	}
}
func TestGroupMediaMutationFailureAndCleanupFailure(t *testing.T) {
	g, _, members, rooms := setupGateway(t, "http://127.0.0.1:1")
	rooms.people["1"] = []*livekit.ParticipantInfo{{Identity: "2:" + sessionB}}
	marker := errors.New("DB rollback")
	if err := g.RevokeChat(context.Background(), 1, func() error { return marker }, 2); !errors.Is(err, marker) || len(rooms.removed) != 0 {
		t.Fatal("failed mutation changed media", err)
	}
	if err := g.RevokeChat(context.Background(), 1, func() error { return nil }, 2); err != nil || len(rooms.removed) != 1 {
		t.Fatal("kick did not remove media", err)
	}
	rooms.people["1"] = []*livekit.ParticipantInfo{{Identity: "2:" + sessionB}}
	rooms.failRemove = true
	committed := false
	if err := g.RevokeChat(context.Background(), 1, func() error { committed = true; members.kick(2); return nil }, 2); !errors.Is(err, ErrMediaUnavailable) || !committed {
		t.Fatal("media failure hid committed DB removal", err)
	}
	rooms.failRemove = false
	if err := g.Reconcile(context.Background()); err != nil || len(rooms.people["1"]) != 0 {
		t.Fatal("reconciliation failed to retry media removal", err)
	}
}

func TestGatewayShutdownClosesUpgradedConnections(t *testing.T) {
	done := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		c, err := (&websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer c.Close()
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer up.Close()
	g, _, _, _ := setupGateway(t, up.URL)
	ctx, cancel := context.WithCancel(g.ctx)
	defer cancel()
	g.ctx = ctx
	proxy := httptest.NewServer(g)
	defer proxy.Close()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(proxy.URL, "http")+"/rtc/rtc?access_token="+mediaToken(t, "1:"+sessionA, "2"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown leaked upgraded upstream socket")
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = c.ReadMessage()
	var timed net.Error
	if err == nil || (errors.As(err, &timed) && timed.Timeout()) {
		t.Fatal("shutdown did not close client socket", err)
	}
}

func TestReconcileTemporaryDBOrAdmissionFailureDoesNotRevokeMedia(t *testing.T) {
	for _, kind := range []string{"database", "membership", "admission"} {
		t.Run(kind, func(t *testing.T) {
			g, users, members, rooms := setupGateway(t, "http://127.0.0.1:1")
			identity := "1:" + sessionA
			rooms.people["10"] = []*livekit.ParticipantInfo{{Identity: identity}}
			if kind == "database" {
				users.offline = true
			} else if kind == "membership" {
				members.err = errors.New("membership unavailable")
			} else {
				budget := admission.New(admission.DefaultConfig(), nil)
				g.auth.SetRequestAdmission(budget)
				release, e := budget.Try(t.Context(), 1, admission.Control)
				if e != nil {
					t.Fatal(e)
				}
				defer release()
			}
			if e := g.Reconcile(t.Context()); e == nil {
				t.Fatal("temporary failure was hidden")
			}
			if len(rooms.removed) != 0 || len(rooms.people["10"]) != 1 {
				t.Fatal("temporary failure removed participant")
			}
		})
	}
}

func TestReconcileBusyAccountDoesNotHideOtherRevocations(t *testing.T) {
	g, users, _, rooms := setupGateway(t, "http://127.0.0.1:1")
	budget := admission.New(admission.DefaultConfig(), nil)
	g.auth.SetRequestAdmission(budget)
	release, err := budget.Try(t.Context(), 1, admission.Control)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	busy, revoked := "1:"+sessionA, "2:"+sessionB
	rooms.people["10"] = []*livekit.ParticipantInfo{{Identity: busy}, {Identity: revoked}, {Identity: "legacy"}}
	users.invalidate(sessionB)
	if err := g.Reconcile(t.Context()); !errors.Is(err, admission.ErrBusy) {
		t.Fatal("partial check must report unavailability", err)
	}
	if len(rooms.people["10"]) != 1 || rooms.people["10"][0].Identity != busy || len(rooms.removed) != 2 {
		t.Fatal("busy participant hid another revocation", rooms.removed)
	}
	release()
	users.invalidate(sessionA)
	if err := g.Reconcile(t.Context()); err != nil || len(rooms.people["10"]) != 0 {
		t.Fatal("later check did not enforce the confirmed revocation", err)
	}
	if budget.Snapshot().Active != 0 {
		t.Fatal("reconciliation leaked admission")
	}
}
