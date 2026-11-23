package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/testutil"
)

func TestHealthcheckPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	health := HandleHealthcheck(db)
	check := func(ctx context.Context, status int, body string) {
		t.Helper()
		response := httptest.NewRecorder()
		health(response, httptest.NewRequestWithContext(ctx, http.MethodGet, "/health", nil))
		if response.Code != status || response.Body.String() != body {
			t.Fatalf("health = %d %q, want %d %q", response.Code, response.Body.String(), status, body)
		}
	}
	// No Redis client, address or process is needed for readiness.
	check(t.Context(), http.StatusOK, "ok")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	check(ctx, http.StatusServiceUnavailable, "")
	check(t.Context(), http.StatusOK, "ok")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	check(t.Context(), http.StatusServiceUnavailable, "")
}

func TestHealthcheckPoolWaitIsBoundedPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	response := httptest.NewRecorder()
	start := time.Now()
	HandleHealthcheck(db)(response, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/health", nil))
	if response.Code != http.StatusServiceUnavailable || response.Body.Len() != 0 {
		t.Fatalf("pool exhaustion health = %d %q", response.Code, response.Body.String())
	}
	if elapsed := time.Since(start); elapsed < 1500*time.Millisecond || elapsed > 4*time.Second {
		t.Fatalf("health pool wait did not respect 2s budget: %s", elapsed)
	}
}
