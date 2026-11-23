package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/zever4/Zchat/internal/auth"
	"github.com/zever4/Zchat/internal/service"
)

// This handler owns its session barrier; do not nest it in ProtectSession.
func HandleWebSocketTicket(s *service.AuthService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "private, no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !auth.SameOrigin(r) {
			http.Error(w, "Forbidden Origin", http.StatusForbidden)
			return
		}
		raw, err := requestToken(r)
		if err != nil {
			sessionError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		ticket, err := s.IssueWebSocketTicket(ctx, raw, r.Host)
		if errors.Is(err, service.ErrWebSocketTicketCapacity) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "Too many pending connections", http.StatusTooManyRequests)
			return
		}
		if err != nil {
			sessionError(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ticket)
	}
}
