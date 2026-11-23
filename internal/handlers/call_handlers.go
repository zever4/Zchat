package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"

	"github.com/zever4/Zchat/internal/calls"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

func getLivekitURL(r *http.Request) string {
	if envURL := os.Getenv("LIVEKIT_URL"); envURL != "" {
		return envURL
	}
	proto := "ws"
	if r.Header.Get("X-Forwarded-Proto") == "https" || r.TLS != nil {
		proto = "wss"
	}

	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}

	// Both nginx and direct Go access enter the session-aware signaling gateway.
	return fmt.Sprintf("%s://%s/rtc/", proto, host)
}

type JoinCallResponse struct {
	Token string `json:"token"`
	URL   string `json:"url"`
}

func HandleJoinCall(authService *service.AuthService, chatRepo repository.ChatRepository, userRepo repository.UserRepository, gateway *calls.Gateway) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chatIDstr := r.URL.Query().Get("chat")
		chatID, err := strconv.ParseInt(chatIDstr, 10, 64)
		if chatIDstr == "" || err != nil || chatID <= 0 {
			http.Error(w, "invalid or missing chatID", http.StatusBadRequest)
			return
		}
		userID, authErr := authenticateRequest(r, authService)
		if authErr != nil {
			http.Error(w, "invalid or missing user id", http.StatusUnauthorized)
			return
		}
		claims, ok := r.Context().Value(authenticatedSessionKey{}).(*service.SessionClaims)
		if !ok || claims.UserID != userID {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if !gateway.Enabled() {
			http.Error(w, "Calls disabled", http.StatusServiceUnavailable)
			return
		}
		unlock, err := gateway.LockChat(r.Context(), chatID)
		if err != nil {
			sessionError(w, err)
			return
		}
		defer unlock()

		member, err := chatRepo.IsMember(r.Context(), chatID, userID)
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		if !member {
			http.Error(w, "user is not a chat member", http.StatusForbidden)
			return
		}

		user, err := userRepo.GetByID(r.Context(), userID)
		if err != nil {
			http.Error(w, "failed to load user profile", http.StatusInternalServerError)
			return
		}

		token, err := calls.GenerateLivekitToken(chatID, userID, claims.ID, user.Username, user.AvatarURL)
		if err != nil {
			http.Error(w, "err generating token", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(JoinCallResponse{
			Token: token,
			URL:   getLivekitURL(r),
		})
	}
}
