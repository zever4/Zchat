package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/zever4/Zchat/internal/domain"
)

func TestWebSocketTicketOneUseHostAndExpiry(t *testing.T) {
	s := NewAuthService(&validationUsers{}, "ticket-test-secret")
	raw, _ := s.GenerateToken(1, 1)
	now := time.Now()
	s.wsTickets.now = func() time.Time { return now }
	ticket, err := s.IssueWebSocketTicket(context.Background(), raw, "example.test:8080")
	if err != nil || len(ticket.Ticket) != 43 || strings.Contains(ticket.Ticket, ".") || ticket.Ticket == raw || !ticket.ExpiresAt.Equal(now.Add(30*time.Second)) {
		t.Fatal("invalid ticket format/TTL", err)
	}
	if _, err := s.ConsumeWebSocketTicket(ticket.Ticket, "other.test:8080"); !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal("ticket used at another host", err)
	}
	var successes atomic.Int64
	var group sync.WaitGroup
	for i := 0; i < 64; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			token, err := s.ConsumeWebSocketTicket(ticket.Ticket, "EXAMPLE.test:8080")
			if err == nil {
				if token != raw {
					t.Error("wrong session credential")
				}
				successes.Add(1)
			} else if !errors.Is(err, domain.ErrInvalidSession) {
				t.Error("unexpected consume error", err)
			}
		}()
	}
	group.Wait()
	if successes.Load() != 1 {
		t.Fatal("ticket consumed more/less than once", successes.Load())
	}
	for _, value := range []string{"", raw, strings.Repeat("x", 4096), ticket.Ticket + "=", strings.Repeat("!", 43)} {
		if _, err := s.ConsumeWebSocketTicket(value, "example.test:8080"); !errors.Is(err, domain.ErrInvalidSession) {
			t.Fatal("malformed/JWT credential accepted")
		}
	}
	claims, _ := s.ParseToken(raw)
	claims.ExpiresAt = jwt.NewNumericDate(now.Add(4 * time.Second))
	short, err := s.wsTickets.issue(raw, "example.test:8080", claims)
	if err != nil || !short.ExpiresAt.Equal(claims.ExpiresAt.Time) {
		t.Fatal("ticket outlives session", err)
	}
	now = short.ExpiresAt
	if _, err := s.ConsumeWebSocketTicket(short.Ticket, "example.test:8080"); !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal("expired ticket accepted", err)
	}
	if len(s.wsTickets.entries) != 0 || len(s.wsTickets.sessions) != 0 {
		t.Fatal("consumed/expired tickets kept capacity")
	}
}

func TestWebSocketTicketCapacityAndCleanup(t *testing.T) {
	s := NewAuthService(&validationUsers{}, "ticket-test-secret")
	raw, _ := s.GenerateToken(1, 1)
	claims, _ := s.ParseToken(raw)
	now := time.Now()
	s.wsTickets.now = func() time.Time { return now }
	var first WebSocketTicket
	for i := 0; i < maxSessionWebSocketTickets; i++ {
		ticket, err := s.wsTickets.issue(raw, "example.test", claims)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = ticket
		}
	}
	if _, err := s.wsTickets.issue(raw, "example.test", claims); !errors.Is(err, ErrWebSocketTicketCapacity) {
		t.Fatal("per-session limit ignored", err)
	}
	s.ConsumeWebSocketTicket(first.Ticket, "example.test")
	if _, err := s.wsTickets.issue(raw, "example.test", claims); err != nil {
		t.Fatal("consumed ticket did not free capacity", err)
	}
	for i := maxSessionWebSocketTickets; i < maxWebSocketTickets; i++ {
		copy := *claims
		copy.ID = fmt.Sprint(i)
		if _, err := s.wsTickets.issue(raw, "example.test", &copy); err != nil {
			t.Fatal("global limit too small", err)
		}
	}
	claims.ID = "new-session"
	if _, err := s.wsTickets.issue(raw, "example.test", claims); !errors.Is(err, ErrWebSocketTicketCapacity) {
		t.Fatal("global limit ignored", err)
	}
	now = now.Add(WebSocketTicketLifetime)
	s.CleanupWebSocketTickets()
	if len(s.wsTickets.entries) != 0 || len(s.wsTickets.sessions) != 0 {
		t.Fatal("expired state retained")
	}
	if _, err := s.wsTickets.issue(raw, "example.test", claims); err != nil {
		t.Fatal("cleanup did not free capacity", err)
	}
}

func TestWebSocketTicketIssuanceValidatesSession(t *testing.T) {
	u := &validationUsers{}
	s := NewAuthService(u, "ticket-test-secret")
	raw, _ := s.GenerateToken(1, 1)
	u.err = domain.ErrInvalidSession
	if _, err := s.IssueWebSocketTicket(context.Background(), raw, "example.test"); !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal("revoked session issued ticket", err)
	}
	u.err = errors.New("offline")
	if _, err := s.IssueWebSocketTicket(context.Background(), raw, "example.test"); !errors.Is(err, ErrAuthUnavailable) {
		t.Fatal("outage treated as invalid session", err)
	}
	u.err = nil
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.IssueWebSocketTicket(ctx, raw, "example.test"); err == nil || len(s.wsTickets.entries) != 0 {
		t.Fatal("cancelled request issued ticket")
	}
}
