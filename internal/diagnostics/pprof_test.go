package diagnostics

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestDisabledProfiler(t *testing.T) {
	stop, err := Start(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	stop()
}

func TestProfilerLifecycle(t *testing.T) {
	stop, err := Start(context.Background(), true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stop)
	if unexpectedStop, err := Start(context.Background(), true); err == nil {
		unexpectedStop()
		t.Fatal("second profiler should fail on occupied port")
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get("http://127.0.0.1:6060/debug/pprof/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	client.CloseIdleConnections()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("profile index status=%d", response.StatusCode)
	}
	stop()
	listener, err := net.Listen("tcp4", "127.0.0.1:6060")
	if err != nil {
		t.Fatalf("profiler did not release listener: %v", err)
	}
	listener.Close()
}

func TestProfileHandlerBounds(t *testing.T) {
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/debug/pprof/", 200},
		{"GET", "/debug/pprof/goroutine?debug=1", 200},
		{"GET", "/debug/pprof/cmdline", 404},
		{"GET", "/debug/pprof/trace", 404},
		{"POST", "/debug/pprof/profile", 405},
		{"GET", "/debug/pprof/profile?seconds=61", 400},
		{"GET", "/debug/pprof/heap?seconds=-1", 400},
		{"GET", "/debug/pprof/profile?seconds=NaN", 400},
	} {
		t.Run(tc.path+tc.method, func(t *testing.T) {
			response := httptest.NewRecorder()
			newHandler().ServeHTTP(response, httptest.NewRequest(tc.method, tc.path, nil))
			if response.Code != tc.status {
				t.Fatalf("status=%d, want %d", response.Code, tc.status)
			}
		})
	}
}
