package handlers

import (
	"context"
	"database/sql"
	"net/http"
	"time"
)

// Readiness depends on PostgreSQL, the runtime's required persistence service.
// Optional notification services must not block ordinary chat or polling recovery.
func HandleHealthcheck(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}
}
