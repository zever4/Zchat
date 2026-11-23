package handlers

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

var browserHostname = regexp.MustCompile(`^[a-zA-Z0-9.-]+$`)

type BrowserPolicy struct {
	callSources []string
}

// Validate configuration once, before startup opens database/listener resources.
// LiveKit uses HTTP preflight requests as well as WebSocket signaling.
func NewBrowserPolicy(livekitURL string) (*BrowserPolicy, error) {
	p := &BrowserPolicy{}
	if livekitURL == "" {
		return p, nil
	}
	u, err := url.Parse(livekitURL)
	if err != nil || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" ||
		strings.ContainsAny(livekitURL, "\r\n\t ") || !validBrowserHost(u.Host) {
		return nil, errors.New("LIVEKIT_URL must be an HTTP(S) or WS(S) URL without credentials, query or fragment")
	}
	switch u.Scheme {
	case "http", "ws":
		p.callSources = []string{"http://" + u.Host, "ws://" + u.Host}
	case "https", "wss":
		p.callSources = []string{"https://" + u.Host, "wss://" + u.Host}
	default:
		return nil, errors.New("LIVEKIT_URL must use HTTP(S) or WS(S)")
	}
	return p, nil
}

func validBrowserHost(host string) bool {
	u, err := url.Parse("http://" + host)
	if err != nil || host == "" || strings.HasSuffix(host, ":") || u.Host != host || u.User != nil ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	ip := net.ParseIP(u.Hostname())
	if strings.HasPrefix(host, "[") && ip == nil {
		return false
	}
	if ip == nil && !browserHostname.MatchString(u.Hostname()) {
		return false
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return true
}

// Apply to static browser responses only. Protected downloads have their own
// sandbox policy; never replace it or wrap the gateway/session barrier here.
func (p *BrowserPolicy) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !validBrowserHost(r.Host) {
			http.Error(w, "Invalid Host", http.StatusBadRequest)
			return
		}
		connect := []string{"'self'", "blob:", "ws://" + r.Host, "wss://" + r.Host, "https://api.giphy.com"}
		connect = append(connect, p.callSources...)
		w.Header().Set("Content-Security-Policy", strings.Join([]string{
			"default-src 'none'",
			"script-src 'self'",
			"script-src-attr 'none'",
			"style-src 'self'",
			// Existing style attributes and runtime layout styles are kept. This
			// permits inline CSS attributes, never inline JavaScript or style tags.
			"style-src-attr 'unsafe-inline'",
			"font-src 'self'",
			"img-src 'self' blob: https://giphy.com https://*.giphy.com https://media.tenor.com",
			"media-src 'self' blob:",
			"connect-src " + strings.Join(connect, " "),
			"worker-src 'self' blob:",
			"object-src 'none'", "base-uri 'none'", "frame-src 'none'",
			"frame-ancestors 'none'", "form-action 'self'",
		}, "; "))
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		next.ServeHTTP(w, r)
	})
}
