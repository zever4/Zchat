package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"context"
	"github.com/zever4/Zchat/internal/chat"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"time"
)

func HandleUploadAvatar(authService *service.AuthService, userRepo repository.UserRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", 401)
			return
		}
		defer cleanupMultipart(r)
		avatar, release, err := avatarUpload(w, r)
		if err != nil {
			uploadError(w, err)
			return
		}
		defer release()
		old, err := userRepo.ReplaceAvatar(r.Context(), userID, avatar)
		release() // Metadata operation finished; physical cleanup must not hold login.
		if err != nil {
			// An unknown SQL COMMIT may already reference the new avatar. Retain
			// its file and charge; do not remove the previous avatar either.
			log.Printf("replace avatar: %v", err)
			http.Error(w, "Failed to update avatar", 500)
			return
		}
		removeAvatar(r, old)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"avatar_url": avatar})
	}
}

func HandleChangePassword(authService *service.AuthService, hub *chat.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, err := requestToken(r)
		if err != nil {
			sessionError(w, err)
			return
		}
		var req struct {
			OldPassword string `json:"old_password"`
			NewPassword string `json:"new_password"`
		}
		if err := decodeAuthJSON(w, r, &req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := authService.ChangePassword(ctx, raw, req.OldPassword, req.NewPassword, hub.DisconnectUser); err != nil {
			sessionError(w, err)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}
}
