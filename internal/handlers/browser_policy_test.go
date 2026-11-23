package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBrowserPolicyRejectsSourceInjection(t *testing.T) {
	for _, raw := range []string{
		"javascript:alert(1)", "file:///tmp/key", "wss://user:private@call.example/",
		"wss://call.example/?token=private", "wss://call.example/#private", "wss://*.example/",
		"wss://call.example:bad/", "wss://call.example:0/", "wss://call.example:65536/",
		"wss://[not-an-ip]:7880/",
		"wss://call.example/\r\nscript-src *", "wss://call.example;script-src/",
	} {
		if _, err := NewBrowserPolicy(raw); err == nil {
			t.Errorf("unsafe LIVEKIT_URL accepted")
		} else if strings.Contains(err.Error(), "private") {
			t.Error("config error includes credential")
		}
	}
	p, err := NewBrowserPolicy("")
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"", "user@host", "host/path", "host?query", "host#fragment", "host:", "host:0", "host:65536", "[not-an-ip]:7880", "host;script-src", "host'", "*.example", "host\r\nInjected: yes"} {
		called := false
		h := p.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
		r := httptest.NewRequest("GET", "http://example.test/chat.html", nil)
		r.Host = host
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != 400 || called || w.Header().Get("Content-Security-Policy") != "" {
			t.Errorf("unsafe Host reached static handler or policy: status=%d", w.Code)
		}
	}
}

func TestBrowserPolicyAllowsOnlyExplicitDestinations(t *testing.T) {
	p, err := NewBrowserPolicy("wss://calls.example:7443/rtc/")
	if err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "http://localhost:18080/chat.html", nil)
	// Forwarded headers from arbitrary direct requests cannot expand the policy.
	r.Header.Set("X-Forwarded-Host", "attacker.example")
	r.Header.Set("X-Forwarded-Proto", "https")
	w := httptest.NewRecorder()
	p.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })).ServeHTTP(w, r)
	policy := w.Header().Get("Content-Security-Policy")
	for _, expected := range []string{"script-src 'self'", "script-src-attr 'none'", "style-src 'self'", "style-src-attr 'unsafe-inline'", "https://calls.example:7443", "wss://calls.example:7443", "ws://localhost:18080", "https://api.giphy.com", "object-src 'none'", "frame-ancestors 'none'", "base-uri 'none'"} {
		if !strings.Contains(policy, expected) {
			t.Errorf("missing directive/source: %s", expected)
		}
	}
	for _, bad := range []string{"attacker.example", "unsafe-eval", "script-src 'self' 'unsafe-inline'", "connect-src *", " ws: ", " wss: ", " https: "} {
		if strings.Contains(policy, bad) {
			t.Errorf("policy widened: %s", bad)
		}
	}
	if len(w.Header().Values("Content-Security-Policy")) != 1 || w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatal("security headers missing/duplicated")
	}
	for _, tc := range []struct{ raw, source string }{{"ws://127.0.0.1:7880/rtc/", "http://127.0.0.1:7880"}, {"https://call.example/rtc/", "wss://call.example"}, {"http://[::1]:7880/", "ws://[::1]:7880"}} {
		p, err := NewBrowserPolicy(tc.raw)
		if err != nil {
			t.Fatal(err)
		}
		w := httptest.NewRecorder()
		p.Wrap(http.NotFoundHandler()).ServeHTTP(w, r)
		if !strings.Contains(w.Header().Get("Content-Security-Policy"), tc.source) {
			t.Error("LiveKit protocol pair missing")
		}
	}
	p, _ = NewBrowserPolicy("")
	r.Host = "[::1]:28080"
	w = httptest.NewRecorder()
	p.Wrap(http.NotFoundHandler()).ServeHTTP(w, r)
	if strings.Contains(w.Header().Get("Content-Security-Policy"), "ws://[::1]:7880") || !strings.Contains(w.Header().Get("Content-Security-Policy"), "ws://[::1]:28080") {
		t.Fatal("calls must use the authenticated app gateway")
	}
}
