package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"

	"github.com/zever4/Zchat/internal/chat"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
)

var settingsKeyCodeRe = regexp.MustCompile(`^[A-Za-z0-9]+$`)

func HandleGetSettings(authService *service.AuthService, userRepo repository.UserRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		settings, err := userRepo.GetSettings(r.Context(), userID)
		if err != nil {
			log.Printf("get settings error: %v", err)
			http.Error(w, "Failed to load settings", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(settings)
	}
}

func HandleUpdateSettings(authService *service.AuthService, userRepo repository.UserRepository) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		userID, err := authenticateRequest(r, authService)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		var settings domain.UserSettings
		if err := json.NewDecoder(r.Body).Decode(&settings); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		if settings.DNDFromHour < 0 || settings.DNDFromHour > 23 || settings.DNDToHour < 0 || settings.DNDToHour > 23 {
			http.Error(w, "DND hours must be in range 0..23", http.StatusBadRequest)
			return
		}
		if settings.MicrophoneMode == "" {
			settings.MicrophoneMode = "toggle"
		}
		if settings.MicrophoneMode != "toggle" && settings.MicrophoneMode != "radio" {
			http.Error(w, "microphone_mode must be toggle or radio", http.StatusBadRequest)
			return
		}
		if settings.MicrophonePTTKey != "" {
			if len(settings.MicrophonePTTKey) > 32 || !settingsKeyCodeRe.MatchString(settings.MicrophonePTTKey) {
				http.Error(w, "microphone_ptt_key has invalid format", http.StatusBadRequest)
				return
			}
			switch settings.MicrophonePTTKey {
			case "AltLeft", "AltRight", "ControlLeft", "ControlRight", "ShiftLeft", "ShiftRight", "MetaLeft", "MetaRight", "Escape":
				http.Error(w, "microphone_ptt_key is not allowed", http.StatusBadRequest)
				return
			}
		}
		if settings.ScreenShareMode == "" {
			settings.ScreenShareMode = "1080p"
		}
		switch settings.ScreenShareMode {
		case "720p", "1080p", "1440p":
		default:
			http.Error(w, "screen_share_resolution must be 720p, 1080p or 1440p", http.StatusBadRequest)
			return
		}
		switch settings.ScreenShareFPS {
		case 0:
			settings.ScreenShareFPS = 30
		case 10, 15, 30, 60:
		default:
			http.Error(w, "screen_share_fps must be 10, 15, 30 or 60", http.StatusBadRequest)
			return
		}

		if err := userRepo.UpsertSettings(r.Context(), userID, &settings); err != nil {
			log.Printf("update settings error: %v", err)
			http.Error(w, "Failed to save settings", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}
}

func HandleLogoutAllSessions(authService *service.AuthService, userRepo repository.UserRepository, hub *chat.Hub) http.HandlerFunc {
	return HandleLogoutSession(authService, hub, true)
}
