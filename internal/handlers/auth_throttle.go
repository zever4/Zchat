package handlers

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type authClientBudget struct {
	limiter *rate.Limiter
	seen    time.Time
}
type AuthThrottle struct {
	mu      sync.Mutex
	clients map[string]*authClientBudget
	global  *rate.Limiter
}

func NewAuthThrottle() *AuthThrottle {
	return &AuthThrottle{clients: make(map[string]*authClientBudget), global: rate.NewLimiter(60, 120)}
}

// RemoteAddr is used deliberately: untrusted X-Forwarded-For cannot bypass limits.
// A deployment behind a proxy needs an explicit trusted-proxy policy.
func (a *AuthThrottle) Wrap(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		now := time.Now()
		a.mu.Lock()
		entry := a.clients[host]
		if entry == nil {
			if len(a.clients) >= 4096 {
				for key, value := range a.clients {
					if now.Sub(value.seen) > 10*time.Minute {
						delete(a.clients, key)
					}
				}
			}
			if len(a.clients) < 4096 {
				entry = &authClientBudget{limiter: rate.NewLimiter(2, 60)}
				a.clients[host] = entry
			}
		}
		allowed := false
		if entry != nil {
			entry.seen = now
			allowed = entry.limiter.AllowN(now, 1) && a.global.AllowN(now, 1)
		}
		a.mu.Unlock()
		reject := func() {
			w.Header().Set("Retry-After", "1")
			w.Header().Set("Cache-Control", "no-store")
			http.Error(w, "Too many authentication requests", http.StatusTooManyRequests)
		}
		if !allowed {
			reject()
			return
		}
		next(w, r)
	}
}
