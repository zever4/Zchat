package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func TestCommittedRevocationError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		err       error
		status    int
		committed string
	}{
		{"media pending after commit", errors.Join(service.ErrSessionMediaUnavailable, errors.New("SFU offline")), 503, "true"},
		{"DB unavailable", service.ErrAuthUnavailable, 503, ""},
		{"deadline", context.DeadlineExceeded, 503, ""},
		{"invalid session", domain.ErrInvalidSession, 401, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := httptest.NewRecorder()
			sessionError(rr, tc.err)
			if rr.Code != tc.status || rr.Header().Get("X-Zchat-Operation-Committed") != tc.committed {
				t.Fatalf("status=%d committed=%q", rr.Code, rr.Header().Get("X-Zchat-Operation-Committed"))
			}
		})
	}
}

type handlerSessionUsers struct {
	repository.UserRepository
	version atomic.Int64
	offline atomic.Bool
}

func (u *handlerSessionUsers) GetSessionVersion(ctx context.Context, _ int64, _ string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if u.offline.Load() {
		return 0, errors.New("DB offline")
	}
	return int(u.version.Load()), nil
}

func TestHTTPRevocationBarrierAndOutage(t *testing.T) {
	users := &handlerSessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "http-session-test-secret-at-least-32")
	token, _ := auth.GenerateToken(1, 1)
	request := func() *http.Request {
		r := httptest.NewRequest("POST", "/api/test", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}
	entered, release := make(chan struct{}), make(chan struct{})
	committed := atomic.Bool{}
	handler := ProtectSession(auth, func(w http.ResponseWriter, r *http.Request) {
		if id, err := authenticateRequest(r, auth); err != nil || id != 1 {
			t.Error("missing auth context")
		}
		close(entered)
		<-release
		committed.Store(true)
		w.WriteHeader(http.StatusNoContent)
	}, false)
	finished := make(chan struct{})
	go func() { handler.ServeHTTP(httptest.NewRecorder(), request()); close(finished) }()
	<-entered
	revoked := make(chan error, 1)
	go func() {
		revoked <- auth.RevokeSessions(1, func() error {
			if !committed.Load() {
				t.Error("revoked before operation commit")
			}
			users.version.Add(1)
			return nil
		}, func() {})
	}()
	select {
	case <-revoked:
		t.Fatal("revoke skipped in-flight HTTP request")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-finished
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	executed := false
	checked := ProtectSession(auth, func(http.ResponseWriter, *http.Request) { executed = true }, false)
	rr := httptest.NewRecorder()
	checked.ServeHTTP(rr, request())
	if rr.Code != 401 || executed {
		t.Fatal("old token reached handler", rr.Code)
	}
	users.offline.Store(true)
	rr = httptest.NewRecorder()
	checked.ServeHTTP(rr, request())
	if rr.Code != 503 || executed {
		t.Fatal("DB outage treated as logout", rr.Code)
	}
	users.offline.Store(false)
	users.version.Store(1)
	for _, headers := range [][]string{{token}, {"Basic " + token}, {"Bearer " + token, "Bearer " + token}} {
		r := httptest.NewRequest("POST", "/api/test", nil)
		for _, h := range headers {
			r.Header.Add("Authorization", h)
		}
		rr = httptest.NewRecorder()
		checked.ServeHTTP(rr, r)
		if rr.Code != 401 || executed {
			t.Fatal("bad/ambiguous header accepted", rr.Code)
		}
	}
}

func TestAuthRequestBodyValidation(t *testing.T) {
	auth := service.NewAuthService(nil, "body-validation-test-secret-32-bytes")
	for _, body := range []string{`{} {}`, strings.Repeat(" ", 4097) + `{}`} {
		r := httptest.NewRequest("POST", "/api/login", strings.NewReader(body))
		rr := httptest.NewRecorder()
		HandleLogin(auth).ServeHTTP(rr, r)
		if rr.Code != 400 {
			t.Fatal("invalid JSON/body accepted", rr.Code)
		}
		if rr.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("auth response cacheable")
		}
	}
	for _, body := range []string{`{"username":"u","email":"bad","password":"123456"}`, `{"username":"u","email":"user@example.invalid","password":""}`} {
		rr := httptest.NewRecorder()
		HandleRegister(auth).ServeHTTP(rr, httptest.NewRequest("POST", "/api/register", strings.NewReader(body)))
		if rr.Code != 400 {
			t.Fatal("invalid registration reached DB", rr.Code)
		}
	}
}

func TestAuthThrottleCannotTrustForwardedAddress(t *testing.T) {
	limiter := NewAuthThrottle()
	calls := 0
	handler := limiter.Wrap(func(w http.ResponseWriter, r *http.Request) { calls++; w.WriteHeader(204) })
	for i := 0; i < 61; i++ {
		request := httptest.NewRequest("POST", "/api/login", nil)
		request.RemoteAddr = "192.0.2.1:1234"
		request.Header.Set("X-Forwarded-For", time.Now().String())
		rr := httptest.NewRecorder()
		handler(rr, request)
		if i < 60 && rr.Code != 204 {
			t.Fatal("premature rate limit", rr.Code)
		}
		if i == 60 && (rr.Code != 429 || rr.Header().Get("Retry-After") == "") {
			t.Fatal("forwarded header bypassed throttle", rr.Code)
		}
	}
	if calls != 60 {
		t.Fatal("throttled request reached bcrypt")
	}
}
