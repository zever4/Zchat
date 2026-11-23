package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

const WebSocketTicketLifetime = 30 * time.Second
const maxWebSocketTickets = 4096
const maxSessionWebSocketTickets = 16

var ErrWebSocketTicketCapacity = errors.New("pending WebSocket ticket capacity exhausted")

type WebSocketTicket struct {
	Ticket    string    `json:"ticket"`
	ExpiresAt time.Time `json:"expires_at"`
}

type pendingWebSocketTicket struct {
	token, sessionID, host string
	expiresAt              time.Time
}

type webSocketTickets struct {
	mu       sync.Mutex
	entries  map[[32]byte]pendingWebSocketTicket
	sessions map[string]int
	now      func() time.Time
}

func newWebSocketTickets() *webSocketTickets {
	return &webSocketTickets{entries: make(map[[32]byte]pendingWebSocketTicket), sessions: make(map[string]int), now: time.Now}
}

// Issue and handshake validation use the same session/revocation barrier as
// ordinary operations. No JWT is sent in a WebSocket URL or stored in the ticket.
func (s *AuthService) IssueWebSocketTicket(ctx context.Context, raw, host string) (WebSocketTicket, error) {
	claims, unlock, err := s.AuthorizeContext(ctx, raw)
	if err != nil {
		return WebSocketTicket{}, err
	}
	defer unlock()
	return s.wsTickets.issue(raw, host, claims)
}

func (t *webSocketTickets) issue(raw, host string, claims *SessionClaims) (WebSocketTicket, error) {
	if host == "" {
		return WebSocketTicket{}, domain.ErrInvalidSession
	}
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return WebSocketTicket{}, ErrAuthUnavailable
	}
	value := base64.RawURLEncoding.EncodeToString(bytes[:])
	key := sha256.Sum256([]byte(value))
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	t.prune(now)
	if len(t.entries) >= maxWebSocketTickets || t.sessions[claims.ID] >= maxSessionWebSocketTickets {
		return WebSocketTicket{}, ErrWebSocketTicketCapacity
	}
	expiresAt := now.Add(WebSocketTicketLifetime)
	if claims.ExpiresAt.Before(expiresAt) {
		expiresAt = claims.ExpiresAt.Time
	}
	if !now.Before(expiresAt) {
		return WebSocketTicket{}, domain.ErrInvalidSession
	}
	if _, exists := t.entries[key]; exists {
		return WebSocketTicket{}, ErrAuthUnavailable
	}
	t.entries[key] = pendingWebSocketTicket{token: raw, sessionID: claims.ID, host: strings.ToLower(host), expiresAt: expiresAt}
	t.sessions[claims.ID]++
	return WebSocketTicket{Ticket: value, ExpiresAt: expiresAt}, nil
}

// Consume atomically removes the ticket BEFORE any DB lookup or upgrade. Even a
// failed handshake burns it; callers must issue a fresh ticket and reauthorize
// its session before registering a socket. State is local to one backend process.
func (s *AuthService) ConsumeWebSocketTicket(value, host string) (string, error) {
	if len(value) != 43 {
		return "", domain.ErrInvalidSession
	}
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(decoded) != 32 || base64.RawURLEncoding.EncodeToString(decoded) != value {
		return "", domain.ErrInvalidSession
	}
	key := sha256.Sum256([]byte(value))
	t := s.wsTickets
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.entries[key]
	if !ok {
		return "", domain.ErrInvalidSession
	}
	if !t.now().Before(entry.expiresAt) {
		t.remove(key, entry)
		return "", domain.ErrInvalidSession
	}
	if !strings.EqualFold(host, entry.host) {
		return "", domain.ErrInvalidSession
	}
	t.remove(key, entry)
	return entry.token, nil
}

// Also called by the existing joined session-cleanup task; no new goroutine.
func (s *AuthService) CleanupWebSocketTickets() {
	t := s.wsTickets
	t.mu.Lock()
	defer t.mu.Unlock()
	t.prune(t.now())
}

func (t *webSocketTickets) prune(now time.Time) {
	for key, entry := range t.entries {
		if !now.Before(entry.expiresAt) {
			t.remove(key, entry)
		}
	}
}

func (t *webSocketTickets) remove(key [32]byte, entry pendingWebSocketTicket) {
	delete(t.entries, key)
	t.sessions[entry.sessionID]--
	if t.sessions[entry.sessionID] == 0 {
		delete(t.sessions, entry.sessionID)
	}
}
