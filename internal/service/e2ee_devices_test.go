package service

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/testutil"
)

func TestE2EEDeviceCredentialAndReauthPostgres(t *testing.T) {
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := NewAuthService(users, "synthetic-device-service-secret-at-least-32")
	const password = "Synthetic-service-password"
	token, err := auth.Register(t.Context(), "alice", "alice@device-service.invalid", password)
	if err != nil {
		t.Fatal(err)
	}
	claims, _ := auth.ParseToken(token)
	s := NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	raw := bytes.Repeat([]byte{37}, 32)
	hash := E2EEDeviceCredentialHash(raw)
	key := bytes.Repeat([]byte{42}, 33)
	key[0] = 5
	req := domain.E2EEDeviceRegistration{RequestID: uuid.NewString(), IdentityKey: key, CredentialHash: hash[:], RegistrationID: 1}
	if _, err := s.Register(t.Context(), claims.UserID, req, bytes.Repeat([]byte{9}, 32)); !errors.Is(err, domain.ErrE2EEDeviceCredential) {
		t.Fatal("registration without possession", err)
	}
	result, err := s.Register(t.Context(), claims.UserID, req, raw)
	if err != nil {
		t.Fatal(err)
	}
	d, unlock, err := s.Authorize(t.Context(), claims.UserID, result.Device.GenerationID, raw)
	if err != nil {
		t.Fatal(err)
	}
	unlock()
	if d.UserID != claims.UserID {
		t.Fatal("wrong subject")
	}
	if _, _, err := s.Authorize(t.Context(), claims.UserID, uuid.NewString(), raw); !errors.Is(err, domain.ErrE2EEDeviceCredential) {
		t.Fatal("generation not bound", err)
	}
	next := req
	next.RequestID = uuid.NewString()
	next.ExpectedGenerationID = result.Device.GenerationID
	nextRaw := bytes.Repeat([]byte{38}, 32)
	nextHash := E2EEDeviceCredentialHash(nextRaw)
	next.CredentialHash = nextHash[:]
	notifications := 0
	s.SetDeviceReplacer(func(uid int64, generation string) {
		notifications++
		if uid != claims.UserID || generation != result.Device.GenerationID {
			t.Error("wrong device invalidated")
		}
	})
	if _, err = s.Replace(t.Context(), claims.UserID, next, "wrong", nextRaw); !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatal("wrong password accepted", err)
	}
	if notifications != 0 {
		t.Fatal("failed replacement disconnected device")
	}
	if _, err = s.Replace(t.Context(), claims.UserID, next, password, nextRaw); err != nil {
		t.Fatal(err)
	}
	replay, err := s.Replace(t.Context(), claims.UserID, next, password, nextRaw)
	if err != nil || !replay.Replayed || notifications != 1 {
		t.Fatal("replacement retry not idempotent", err)
	}
	if _, _, err = s.Authorize(t.Context(), claims.UserID, result.Device.GenerationID, raw); !errors.Is(err, domain.ErrE2EEDeviceCredential) {
		t.Fatal("old device authorized", err)
	}
	if _, err = auth.ValidateTokenContext(t.Context(), token); err != nil {
		t.Fatal("device replacement revoked login session", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = s.Register(canceled, claims.UserID, req, raw); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
}

func TestE2EEReplacementRateLimitIsPerAccountAndBounded(t *testing.T) {
	s := NewE2EEDeviceService(nil, nil)
	for range 5 {
		if !s.allowReplacement(1) {
			t.Fatal("initial attempt throttled")
		}
	}
	if s.allowReplacement(1) {
		t.Fatal("unbounded password guesses")
	}
	if !s.allowReplacement(2) {
		t.Fatal("another account shared a rate bucket")
	}
	for i := int64(3); i <= 1024; i++ {
		if !s.allowReplacement(i) {
			t.Fatal("limiter filled too early")
		}
	}
	if s.allowReplacement(1025) || len(s.limits) != 1024 {
		t.Fatal("unbounded limiter map")
	}
	entry := s.limits[1]
	entry.touched = time.Now().Add(-11 * time.Minute)
	s.limits[1] = entry
	if !s.allowReplacement(1025) || len(s.limits) != 1024 {
		t.Fatal("idle limiter did not expire")
	}
}
