package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/zever4/Zchat/internal/service"
)

func TestWebSocketTicketHTTPAuthOriginAndCapacity(t *testing.T) {
	users := &handlerSessionUsers{}
	users.version.Store(1)
	auth := service.NewAuthService(users, "http-ticket-test-secret")
	raw, _ := auth.GenerateToken(1, 1)
	h := HandleWebSocketTicket(auth)
	request := func(header string) *http.Request {
		r := httptest.NewRequest("POST", "http://example.test:8080/api/ws-ticket", nil)
		if header != "" {
			r.Header.Set("Authorization", header)
		}
		return r
	}
	for _, tc := range []struct {
		header string
		status int
	}{
		{"", 401}, {raw, 401}, {"Bearer invalid", 401}, {"Bearer " + raw, 200},
	} {
		w := httptest.NewRecorder()
		h(w, request(tc.header))
		if w.Code != tc.status || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") || strings.Contains(w.Body.String(), raw) {
			t.Fatal("auth/status/cache/credential leak", w.Code)
		}
		if w.Code == 200 {
			var ticket service.WebSocketTicket
			if json.Unmarshal(w.Body.Bytes(), &ticket) != nil || len(ticket.Ticket) != 43 || ticket.ExpiresAt.IsZero() {
				t.Fatal("missing ticket response")
			}
		}
	}
	r := request("Bearer " + raw)
	r.Header.Set("Origin", "http://evil.test")
	w := httptest.NewRecorder()
	h(w, r)
	if w.Code != 403 {
		t.Fatal("cross-origin issuance accepted")
	}
	r = request("Bearer " + raw)
	r.Header.Add("Authorization", "Bearer "+raw)
	w = httptest.NewRecorder()
	h(w, r)
	if w.Code != 401 {
		t.Fatal("ambiguous credentials accepted")
	}
	users.offline.Store(true)
	w = httptest.NewRecorder()
	h(w, request("Bearer "+raw))
	if w.Code != 503 {
		t.Fatal("outage invalidated login", w.Code)
	}
	users.offline.Store(false)
	users.version.Add(1)
	w = httptest.NewRecorder()
	h(w, request("Bearer "+raw))
	if w.Code != 401 {
		t.Fatal("revoked session issued ticket", w.Code)
	}
	fresh, _ := auth.GenerateToken(1, 2)
	for i := 0; i < 16; i++ {
		w = httptest.NewRecorder()
		h(w, request("Bearer "+fresh))
		if w.Code != 200 {
			t.Fatal("pending capacity too small", w.Code)
		}
	}
	w = httptest.NewRecorder()
	h(w, request("Bearer "+fresh))
	if w.Code != 429 || w.Header().Get("Retry-After") != "1" {
		t.Fatal("capacity not bounded", w.Code)
	}
}
