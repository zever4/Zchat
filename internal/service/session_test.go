package service

import (
	"errors"
	"testing"
)

func TestFailedRevocationDoesNotDisconnectAndReleasesBarrier(t *testing.T) {
	auth := NewAuthService(nil, "test-only-secret")
	want := errors.New("database unavailable")
	disconnected := false
	err := auth.RevokeSessions(1, func() error { return want }, func() { disconnected = true })
	if !errors.Is(err, want) || disconnected {
		t.Fatal("failed mutation reported success or disconnected sockets")
	}
	unlock := auth.LockSession(1)
	unlock()
}
