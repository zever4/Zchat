package auth

import (
	"net/http"
	"net/url"
	"strings"
)

// SameOrigin permits native clients without Origin. Browser clients must use
// the request's host (including port); untrusted forwarded headers are ignored.
// The backend may be behind TLS termination, so only HTTP(S) origins are allowed
// and the transport scheme is not inferred from forwarded headers.
func SameOrigin(r *http.Request) bool {
	values := r.Header.Values("Origin")
	if r.Host == "" {
		return false
	}
	if len(values) == 0 {
		return true
	}
	if len(values) != 1 || values[0] == "" {
		return false
	}
	u, err := url.Parse(values[0])
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil &&
		u.Host != "" && strings.EqualFold(u.Host, r.Host) && u.Path == "" &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && !strings.Contains(values[0], "#")
}
