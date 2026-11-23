// Package diagnostics provides explicitly enabled, process-local profiling.
package diagnostics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"
)

// Start never exposes profiles on the application mux or a non-loopback address.
// Container users collect them with docker exec; no diagnostic port is published.
func Start(ctx context.Context, enabled bool) (func(), error) {
	if !enabled {
		return func() {}, nil
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:6060")
	if err != nil {
		return nil, fmt.Errorf("start local profiler: %w", err)
	}
	server := &http.Server{
		Handler: newHandler(), BaseContext: func(net.Listener) context.Context { return ctx },
		ReadHeaderTimeout: 5 * time.Second, WriteTimeout: 65 * time.Second, IdleTimeout: 15 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Closing the explicit listener is the only expected shutdown path.
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			listener.Close()
		}
	}()
	return func() { _ = server.Close(); <-done }, nil
}

func newHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/", pprof.Index)
	mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
	// No trace/cmdline endpoint. Bound delta-profile durations as well as CPU.
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/debug/pprof/cmdline" || r.URL.Path == "/debug/pprof/trace" {
			http.NotFound(w, r)
			return
		}
		if value := r.URL.Query().Get("seconds"); value != "" {
			seconds, err := strconv.Atoi(value)
			if err != nil || seconds <= 0 || seconds > 60 {
				http.Error(w, "Profile duration must be between 0 and 60 seconds", http.StatusBadRequest)
				return
			}
		}
		mux.ServeHTTP(w, r)
	})
}
