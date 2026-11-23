package auth

import (
	"net/http/httptest"
	"testing"
)

func TestSameOrigin(t *testing.T) {
	for _, tc := range []struct {
		origin string
		allow  bool
	}{
		{"http://example.test:8080", true}, {"https://EXAMPLE.test:8080", true},
		{"null", false}, {"", false}, {"http://evil.test:8080", false},
		{"http://example.test", false}, {"http://example.test:8080/", false},
		{"http://user@example.test:8080", false}, {"http://example.test:8080?", false},
		{"http://example.test:8080#", false}, {"ws://example.test:8080", false},
		{"http://example.test:8080 http://evil.test", false},
	} {
		r := httptest.NewRequest("POST", "http://example.test:8080/api/ws-ticket", nil)
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("X-Forwarded-Host", "evil.test")
		if SameOrigin(r) != tc.allow {
			t.Errorf("origin %q: allowed=%v", tc.origin, !tc.allow)
		}
	}
	r := httptest.NewRequest("GET", "http://example.test:8080/ws", nil)
	if !SameOrigin(r) {
		t.Fatal("native request without Origin rejected")
	}
	r.Header.Add("Origin", "http://example.test:8080")
	r.Header.Add("Origin", "http://example.test:8080")
	if SameOrigin(r) {
		t.Fatal("duplicate Origin accepted")
	}
}
