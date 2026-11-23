package handlers

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func TestRequestLimitsBeforeHandler(t *testing.T) {
	for _, tc := range []struct {
		name, path, body string
		chunked          bool
		want             int
	}{
		{"declared oversized", "/api/settings", strings.Repeat(" ", maxJSONBody+1), false, 413},
		{"chunked oversized", "/api/settings", strings.Repeat(" ", maxJSONBody+1), true, 413},
		{"auth smaller limit", "/api/login", strings.Repeat(" ", 4097), true, 413},
		{"multiple values", "/api/settings", `{} {}`, false, 400},
		{"unknown JSON fields remain compatible", "/api/settings", `{"future":true}`, false, 204},
		{"upload keeps larger limit", "/api/files/upload", strings.Repeat("x", 1<<20), true, 204},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			mux := http.NewServeMux()
			mux.HandleFunc("POST "+tc.path, func(w http.ResponseWriter, r *http.Request) {
				called = true
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(204)
			})
			r := httptest.NewRequest("POST", tc.path, strings.NewReader(tc.body))
			if tc.chunked {
				r.ContentLength = -1
			}
			w := httptest.NewRecorder()
			LimitRequests(mux, mux).ServeHTTP(w, r)
			if w.Code != tc.want || called != (tc.want == 204) {
				t.Fatalf("status=%d called=%v; want %d", w.Code, called, tc.want)
			}
		})
	}
}

type bodySessionUsers struct {
	repository.UserRepository
	version   atomic.Int64
	validated chan struct{}
	once      sync.Once
}

func (u *bodySessionUsers) GetSessionVersion(context.Context, int64, string) (int, error) {
	u.once.Do(func() { close(u.validated) })
	return int(u.version.Load()), nil
}

func TestSlowBodiesDoNotHoldRevocationBarrier(t *testing.T) {
	for _, upload := range []bool{false, true} {
		t.Run(fmt.Sprintf("upload=%v", upload), func(t *testing.T) {
			tmp := t.TempDir()
			t.Setenv("TMPDIR", tmp)
			users := &bodySessionUsers{validated: make(chan struct{})}
			users.version.Store(1)
			auth := service.NewAuthService(users, "slow-body-session-test-secret-at-least-32")
			token, err := auth.GenerateToken(1, 1)
			if err != nil {
				t.Fatal(err)
			}
			path, contentType, body := "/api/settings", "application/json", []byte(`{"setting":true}`)
			if upload {
				path = "/api/files/upload"
				var buf bytes.Buffer
				writer := multipart.NewWriter(&buf)
				part, err := writer.CreateFormFile("file", "test.bin")
				if err != nil {
					t.Fatal(err)
				}
				_, _ = part.Write(bytes.Repeat([]byte("x"), (1<<20)+8192))
				_ = writer.Close()
				body, contentType = buf.Bytes(), writer.FormDataContentType()
			}
			called := atomic.Bool{}
			mux := http.NewServeMux()
			method := "PUT"
			if upload {
				method = "POST"
			}
			mux.HandleFunc(method+" "+path, ProtectSession(auth, func(w http.ResponseWriter, r *http.Request) { called.Store(true); w.WriteHeader(204) }, false, storageAt(t, t.TempDir())))
			server := httptest.NewServer(metrics.MetricsMiddleware(LimitRequests(mux, mux), mux))
			defer server.Close()
			conn, err := net.Dial("tcp", server.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			_, err = fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer %s\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n", method, path, token, contentType, len(body))
			if err != nil {
				t.Fatal(err)
			}
			_, _ = conn.Write(body[:len(body)-1])
			if upload {
				select {
				case <-users.validated:
				case <-time.After(time.Second):
					t.Fatal("upload did not authenticate before receiving bytes")
				}
			}
			revoked := make(chan error, 1)
			go func() {
				revoked <- auth.RevokeSessions(1, func() error { users.version.Store(2); return nil }, func() {})
			}()
			select {
			case err := <-revoked:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("incomplete body delayed session revocation")
			}
			if _, err = conn.Write(body[len(body)-1:]); err != nil {
				t.Fatal(err)
			}
			response, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			_, _ = io.Copy(io.Discard, response.Body)
			if response.StatusCode != 401 || called.Load() {
				t.Fatalf("revoked upload/request reached handler: status=%d called=%v", response.StatusCode, called.Load())
			}
			files, err := filepath.Glob(filepath.Join(tmp, "multipart-*"))
			if err != nil || len(files) != 0 {
				t.Fatalf("unpublished upload left temporary files: %v %v", files, err)
			}
		})
	}
}

func TestRequestBodyReadIsCancelledByServerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/settings", func(w http.ResponseWriter, r *http.Request) { t.Error("incomplete JSON reached handler") })
	server := httptest.NewUnstartedServer(metrics.MetricsMiddleware(LimitRequests(mux, mux), mux))
	server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	server.Start()
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = fmt.Fprint(conn, "PUT /api/settings HTTP/1.1\r\nHost: test\r\nContent-Length: 100\r\n\r\n{")
	cancel()
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("server cancellation did not unblock body read:", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 408 {
		t.Fatal("expected timeout", response.StatusCode)
	}
}

func TestRejectedUploadDoesNotDrainIncompleteBody(t *testing.T) {
	mux := http.NewServeMux()
	auth := service.NewAuthService(nil, "reject-body-session-test-secret-at-least-32")
	mux.HandleFunc("POST /api/files/upload", ProtectSession(auth, func(http.ResponseWriter, *http.Request) { t.Error("unauthenticated request reached handler") }, false))
	server := httptest.NewServer(metrics.MetricsMiddleware(LimitRequests(mux, mux), mux))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = fmt.Fprint(conn, "POST /api/files/upload HTTP/1.1\r\nHost: test\r\nContent-Length: 100\r\n\r\nx")
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("rejected upload attempted to drain body:", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatal("expected unauthorized", response.StatusCode)
	}
}

func TestSlowJSONBodyTimesOut(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/settings", func(http.ResponseWriter, *http.Request) { t.Error("incomplete body reached handler") })
	server := httptest.NewServer(metrics.MetricsMiddleware(LimitRequests(mux, mux), mux))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(7 * time.Second))
	start := time.Now()
	_, _ = fmt.Fprint(conn, "PUT /api/settings HTTP/1.1\r\nHost: test\r\nContent-Length: 100\r\n\r\n{")
	response, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal("body read deadline failed:", err)
	}
	defer response.Body.Close()
	if response.StatusCode != 408 || time.Since(start) > 6500*time.Millisecond {
		t.Fatal("expected bounded body timeout", response.StatusCode, time.Since(start))
	}
}

func TestCancelledBodyCleanupDoesNotDrainAfterResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/settings", func(http.ResponseWriter, *http.Request) { t.Error("incomplete body reached handler") })
	server := httptest.NewUnstartedServer(metrics.MetricsMiddleware(LimitRequests(mux, mux), mux))
	server.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	server.Start()
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = fmt.Fprint(conn, "PUT /api/settings HTTP/1.1\r\nHost: test\r\nContent-Length: 100\r\n\r\n{")
	cancel()
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 408 {
		t.Fatal(response.StatusCode)
	}
	// Keep the client write side open: closing it would hide a blocked body drain.
	// Observe server FIN instead of Shutdown: Go1.26.11 deliberately waits 500ms
	// AFTER CloseWrite to avoid truncating the error response with a TCP RST.
	// The same 500ms bound must still cover response bytes and body cleanup.
	_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err = io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal("cancelled request response did not finish:", err)
	}
	if _, err = reader.ReadByte(); err != io.EOF {
		t.Fatal("HTTP cleanup did not finish before server FIN:", err)
	}
}

func TestCompletedBodiesKeepConnectionReusable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /api/settings", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) })
	mux.HandleFunc("POST /api/files/upload", func(w http.ResponseWriter, r *http.Request) {
		defer cleanupMultipart(r)
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
		}
		w.WriteHeader(204)
	})
	server := httptest.NewServer(metrics.MetricsMiddleware(LimitRequests(mux, mux), mux))
	defer server.Close()
	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	reader := bufio.NewReader(conn)
	var multipartBody bytes.Buffer
	writer := multipart.NewWriter(&multipartBody)
	part, _ := writer.CreateFormFile("file", "test.bin")
	_, _ = part.Write([]byte("test"))
	_ = writer.Close()
	for _, request := range []struct{ method, path, contentType, body string }{
		{"PUT", "/api/settings", "application/json", `{"x":true}`},
		{"POST", "/api/files/upload", writer.FormDataContentType(), multipartBody.String()},
		{"PUT", "/api/settings", "application/json", `{"x":false}`},
	} {
		_, err = fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: test\r\nContent-Type: %s\r\nContent-Length: %d\r\n\r\n%s", request.method, request.path, request.contentType, len(request.body), request.body)
		if err != nil {
			t.Fatal(err)
		}
		response, err := http.ReadResponse(reader, nil)
		if err != nil {
			t.Fatal("completed body left an expired connection deadline:", err)
		}
		_ = response.Body.Close()
		if response.StatusCode != 204 {
			t.Fatal(response.StatusCode)
		}
	}
}
