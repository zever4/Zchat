package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/zever4/Zchat/internal/admission"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/zever4/Zchat/internal/chat"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/uploadstorage"
)

type authenticatedSessionKey struct{}
type sessionReleaseKey struct{}

// Only terminal, already-authorized file responses may finish the operation
// before returning. Network backpressure must not hold the revocation barrier.
// DB/FS authorization must finish first; no protected work may follow release.
func releaseDownloadSession(r *http.Request) {
	if release, ok := r.Context().Value(sessionReleaseKey{}).(func()); ok {
		release()
	}
}

// Apply only to native authenticated handlers. Gateway RPCs take the same barrier
// in the gRPC interceptor; nesting read locks can deadlock behind a queued revoke.
func ProtectSession(auth *service.AuthService, next http.HandlerFunc, allowAnonymous bool, stores ...*uploadstorage.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(stores) > 0 && stores[0] != nil {
			r = r.WithContext(context.WithValue(r.Context(), storageContextKey{}, stores[0]))
		}
		if allowAnonymous && len(r.Header.Values("Authorization")) == 0 {
			next(w, r)
			return
		}
		raw, err := requestToken(r)
		if err != nil {
			if _, upload := uploadLimit(r.Pattern); upload {
				w.Header().Set("Connection", "close")
			}
			sessionError(w, err)
			return
		}
		w.Header().Set("Cache-Control", "private, no-store")
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		claims, unlock, err := auth.AuthorizeContext(ctx, raw)
		if err != nil {
			if _, upload := uploadLimit(r.Pattern); upload {
				w.Header().Set("Connection", "close")
			}
			sessionError(w, err)
			return
		}
		if _, upload := uploadLimit(r.Pattern); upload {
			// A slow sender must not delay revocation, even for a colliding stripe.
			// Recheck the session after receiving bytes and before any DB/FS publish.
			unlock()
			m := requestStorage(r)
			if m == nil {
				uploadError(w, errStorageUnavailable)
				return
			}
			limit, _ := uploadLimit(r.Pattern)
			fileBound := limit - multipartOverhead
			// A known HTTP body bounds its file too. Streaming still enforces this
			// bound; a forged Content-Length cannot enlarge the reserved footprint.
			if r.ContentLength > 0 {
				fileBound = min(fileBound, r.ContentLength)
			}
			ticket, err := m.Begin(ctx, claims.UserID, fileBound)
			if err != nil {
				uploadError(w, err)
				return
			}
			defer func() {
				if err := ticket.Finish(); err != nil {
					log.Printf("upload cleanup (quota retained): %v", err)
				}
			}()
			r = r.WithContext(ctx)
			prepared, err := parseManagedUpload(w, r, ticket)
			if err != nil {
				uploadError(w, err)
				return
			}
			ctx = context.WithValue(ctx, preparedUploadKey{}, prepared)
			owner := claims.UserID
			prepared.reauthorize = func(parent context.Context) (func(), error) {
				current, release, err := auth.AuthorizeContext(parent, raw)
				if err != nil {
					return nil, err
				}
				if current.UserID != owner {
					release()
					return nil, domain.ErrInvalidSession
				}
				return release, nil
			}
			claims, unlock, err = auth.AuthorizeContext(ctx, raw)
			if err != nil {
				sessionError(w, err)
				return
			}
		}
		release := sync.OnceFunc(unlock)
		defer release()
		ctx = context.WithValue(ctx, authenticatedSessionKey{}, claims)
		ctx = context.WithValue(ctx, sessionReleaseKey{}, release)
		next(w, r.WithContext(ctx))
	}
}

func requestToken(r *http.Request) (string, error) {
	if len(r.Header.Values("Authorization")) != 1 {
		return "", domain.ErrInvalidSession
	}
	return service.BearerToken(r.Header.Get("Authorization"))
}

func sessionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, admission.ErrBusy):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Server busy; retry later", http.StatusTooManyRequests)
	case errors.Is(err, service.ErrSessionMediaUnavailable):
		writePendingCallCleanup(w, "Session revoked; call server unavailable, media disconnection will be retried")
	case errors.Is(err, service.ErrAuthBusy):
		w.Header().Set("Retry-After", "1")
		http.Error(w, "Too many authentication requests", http.StatusTooManyRequests)
	case errors.Is(err, domain.ErrInvalidSession):
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	case errors.Is(err, domain.ErrInvalidCredentials):
		http.Error(w, "Неверный текущий пароль", http.StatusForbidden)
	case errors.Is(err, domain.ErrInvalidInput):
		http.Error(w, "Пароль должен содержать минимум 6 символов и максимум 72 байта", http.StatusBadRequest)
	case errors.Is(err, service.ErrAuthUnavailable), errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		http.Error(w, "Authentication temporarily unavailable", http.StatusServiceUnavailable)
	default:
		http.Error(w, "Session operation failed", http.StatusInternalServerError)
	}
}

// Only use after the DB mutation committed. A plain 503 is otherwise ambiguous:
// clients must not repeat a password change or keep an already-revoked session.
func writePendingCallCleanup(w http.ResponseWriter, message string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Zchat-Operation-Committed", "true")
	http.Error(w, message, http.StatusServiceUnavailable)
}

func HandleLogoutSession(auth *service.AuthService, hub *chat.Hub, all bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, err := requestToken(r)
		if err != nil {
			sessionError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		if err := auth.Logout(ctx, raw, all, hub.DisconnectSession); err != nil {
			sessionError(w, err)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]bool{"ok": true})
	}
}
