package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
)

func decodeAuthJSON(w http.ResponseWriter, r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(v); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("expected a single JSON object")
	}
	return nil
}

func HandleLogin(auth *service.AuthService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var credentials struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := decodeAuthJSON(w, r, &credentials); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		token, username, avatarURL, err := auth.Login(ctx, credentials.Email, credentials.Password)
		if err != nil {
			switch {
			case errors.Is(err, service.ErrAuthBusy), errors.Is(err, admission.ErrBusy):
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Too many authentication requests", http.StatusTooManyRequests)
			case errors.Is(err, domain.ErrInvalidCredentials):
				http.Error(w, "Unauthorized", http.StatusUnauthorized)
			default:
				http.Error(w, "Login temporarily unavailable", http.StatusServiceUnavailable)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": token, "username": username, "avatar_url": avatarURL})
	}
}

func HandleRegister(auth *service.AuthService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Username string `json:"username"`
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := decodeAuthJSON(w, r, &req); err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		token, err := auth.Register(ctx, req.Username, req.Email, req.Password)
		if err != nil {
			var pgErr *pq.Error
			switch {
			case errors.Is(err, service.ErrAuthBusy), errors.Is(err, admission.ErrBusy):
				w.Header().Set("Retry-After", "1")
				http.Error(w, "Too many authentication requests", http.StatusTooManyRequests)
			case errors.Is(err, domain.ErrInvalidInput):
				http.Error(w, "Invalid username/email/password (6 characters minimum, 72 password bytes maximum)", http.StatusBadRequest)
			case errors.As(err, &pgErr) && pgErr.Code == "23505":
				http.Error(w, "Username or email already registered", http.StatusConflict)
			default:
				http.Error(w, "Registration temporarily unavailable", http.StatusServiceUnavailable)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"token": token, "username": strings.TrimSpace(req.Username)})
	}
}
