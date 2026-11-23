package service

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

type validationUsers struct {
	repository.UserRepository
	reads atomic.Int64
	err   error
}

func (u *validationUsers) GetSessionVersion(ctx context.Context, _ int64, _ string) (int, error) {
	u.reads.Add(1)
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return 1, u.err
}

func TestSessionJWTValidation(t *testing.T) {
	const secret = "validation-test-secret-with-32-bytes"
	users := &validationUsers{}
	auth := NewAuthService(users, secret)
	token, err := auth.GenerateToken(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := auth.ParseToken(token)
	if err != nil || parsed.UserID != 1 {
		t.Fatalf("generated token: %v", err)
	}
	if _, err = auth.ValidateToken(token); err != nil {
		t.Fatal(err)
	}
	var base jwt.MapClaims
	unsigned := jwt.NewParser()
	if _, _, err = unsigned.ParseUnverified(token, &base); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(jwt.MapClaims)
		method jwt.SigningMethod
	}{
		{"no exp", func(c jwt.MapClaims) { delete(c, "exp") }, nil},
		{"no iat", func(c jwt.MapClaims) { delete(c, "iat") }, nil},
		{"no ver", func(c jwt.MapClaims) { delete(c, "ver") }, nil},
		{"no jti", func(c jwt.MapClaims) { delete(c, "jti") }, nil},
		{"no sub", func(c jwt.MapClaims) { delete(c, "sub") }, nil},
		{"no issuer", func(c jwt.MapClaims) { delete(c, "iss") }, nil},
		{"no audience", func(c jwt.MapClaims) { delete(c, "aud") }, nil},
		{"wrong issuer", func(c jwt.MapClaims) { c["iss"] = "another-app" }, nil},
		{"wrong audience", func(c jwt.MapClaims) { c["aud"] = "another-app" }, nil},
		{"zero subject", func(c jwt.MapClaims) { c["sub"] = "0" }, nil},
		{"negative subject", func(c jwt.MapClaims) { c["sub"] = "-1" }, nil},
		{"fractional subject", func(c jwt.MapClaims) { c["sub"] = 1.5 }, nil},
		{"fractional string subject", func(c jwt.MapClaims) { c["sub"] = "1.5" }, nil},
		{"noncanonical subject", func(c jwt.MapClaims) { c["sub"] = "01" }, nil},
		{"overflow subject", func(c jwt.MapClaims) { c["sub"] = "9223372036854775808" }, nil},
		{"fractional version", func(c jwt.MapClaims) { c["ver"] = 1.5 }, nil},
		{"string version", func(c jwt.MapClaims) { c["ver"] = "1" }, nil},
		{"zero version", func(c jwt.MapClaims) { c["ver"] = 0 }, nil},
		{"expired", func(c jwt.MapClaims) { c["exp"] = time.Now().Add(-time.Second).Unix() }, nil},
		{"future iat", func(c jwt.MapClaims) { c["iat"] = time.Now().Add(time.Hour).Unix() }, nil},
		{"future nbf", func(c jwt.MapClaims) { c["nbf"] = time.Now().Add(time.Hour).Unix() }, nil},
		{"excess lifetime", func(c jwt.MapClaims) { c["exp"] = time.Now().Add(73 * time.Hour).Unix() }, nil},
		{"null expiry", func(c jwt.MapClaims) { c["exp"] = nil }, nil},
		{"invalid session id", func(c jwt.MapClaims) { c["jti"] = "not-a-uuid" }, nil},
		{"zero session id", func(c jwt.MapClaims) { c["jti"] = "00000000-0000-0000-0000-000000000000" }, nil},
		{"HS512", func(c jwt.MapClaims) {}, jwt.SigningMethodHS512},
		{"HS384", func(c jwt.MapClaims) {}, jwt.SigningMethodHS384},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := jwt.MapClaims{}
			for k, v := range base {
				c[k] = v
			}
			tc.mutate(c)
			method := tc.method
			if method == nil {
				method = jwt.SigningMethodHS256
			}
			raw, err := jwt.NewWithClaims(method, c).SignedString([]byte(secret))
			if err != nil {
				t.Fatal(err)
			}
			reads := users.reads.Load()
			if _, err = auth.ValidateToken(raw); !errors.Is(err, domain.ErrInvalidSession) {
				t.Fatalf("accepted invalid claims: %v", err)
			}
			if users.reads.Load() != reads {
				t.Fatal("invalid JWT reached DB")
			}
		})
	}
	t.Run("large exact subject", func(t *testing.T) {
		raw, _ := auth.GenerateToken(9007199254740993, 1)
		id, err := auth.ValidateToken(raw)
		if err != nil || id != 9007199254740993 {
			t.Fatalf("precision lost: %d %v", id, err)
		}
	})
	t.Run("unique per login", func(t *testing.T) {
		second, _ := auth.GenerateToken(1, 1)
		if second == token {
			t.Fatal("identical session tokens")
		}
	})
	t.Run("wrong signature", func(t *testing.T) {
		bad, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, base).SignedString([]byte("another-secret"))
		if _, err := auth.ValidateToken(bad); !errors.Is(err, domain.ErrInvalidSession) {
			t.Fatal(err)
		}
	})
	t.Run("unsigned", func(t *testing.T) {
		raw, _ := jwt.NewWithClaims(jwt.SigningMethodNone, base).SignedString(jwt.UnsafeAllowNoneSignatureType)
		if _, err := auth.ValidateToken(raw); !errors.Is(err, domain.ErrInvalidSession) {
			t.Fatal(err)
		}
	})
	t.Run("DB outage is not logout", func(t *testing.T) {
		users.err = errors.New("DB offline")
		if _, err := auth.ValidateToken(token); !errors.Is(err, ErrAuthUnavailable) {
			t.Fatal(err)
		}
		users.err = domain.ErrInvalidSession
		if _, err := auth.ValidateToken(token); !errors.Is(err, domain.ErrInvalidSession) {
			t.Fatal(err)
		}
		users.err = nil
	})
	t.Run("canceled request propagates", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := auth.ValidateTokenContext(ctx, token); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestSessionBarrierCancellation(t *testing.T) {
	auth := NewAuthService(nil, "test-secret")
	unlock := auth.LockSession(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	mutated := false
	err := auth.RevokeSessionsContext(ctx, 1, func() error { mutated = true; return nil }, func() {})
	if !errors.Is(err, context.DeadlineExceeded) || mutated {
		t.Fatalf("canceled revoke: %v", err)
	}
	unlock()
	lock := auth.sessionLocks[1]
	if err := lock.Acquire(context.Background(), sessionLockWeight); err != nil {
		t.Fatal(err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if _, err := auth.LockSessionContext(ctx2, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	lock.Release(sessionLockWeight)
	unlock = auth.LockSession(1)
	unlock()
}

func TestAuthConfigurationAndInputs(t *testing.T) {
	for _, s := range []string{"", "short", strings.Repeat("x", 31), " " + strings.Repeat("x", 32)} {
		if ValidateJWTSecret(s) == nil {
			t.Fatal("weak config accepted")
		}
	}
	if err := ValidateJWTSecret(strings.Repeat("x", 32)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"", "12345", strings.Repeat("x", 73), "\xff123456"} {
		if ValidatePassword(p) == nil {
			t.Fatal("bad password accepted")
		}
	}
	for _, p := range []string{"123456", "пароль", strings.Repeat("x", 72)} {
		if err := ValidatePassword(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, h := range []string{"", "token", "Basic token", "Bearer x y"} {
		if _, err := BearerToken(h); err == nil {
			t.Fatal("bad header accepted")
		}
	}
	if raw, err := BearerToken("bearer token"); err != nil || raw != "token" {
		t.Fatal(err)
	}
}

type busyCredentialUsers struct{ repository.UserRepository }

func (busyCredentialUsers) GetByEmail(context.Context, string) (*domain.User, error) {
	return &domain.User{ID: 1, PasswordHash: "unused"}, nil
}
func (busyCredentialUsers) GetSessionVersion(context.Context, int64, string) (int, error) {
	return 1, nil
}

func TestPasswordCapacityDoesNotBecomeInvalidCredentials(t *testing.T) {
	auth := NewAuthService(busyCredentialUsers{}, "password-capacity-test-secret-32-bytes")
	for i := 0; i < cap(auth.passwordSlots); i++ {
		auth.passwordSlots <- struct{}{}
	}
	if token, _, _, err := auth.Login(context.Background(), "user@example.invalid", "password"); !errors.Is(err, ErrAuthBusy) || token != "" {
		t.Fatal("busy login looked like wrong credentials", err)
	}
	if _, err := auth.Register(context.Background(), "username", "user@example.invalid", "password"); !errors.Is(err, ErrAuthBusy) {
		t.Fatal("register exceeded bcrypt capacity", err)
	}
	token, _ := auth.GenerateToken(1, 1)
	disconnected := false
	if err := auth.ChangePassword(context.Background(), token, "password", "new-password", func(int64) { disconnected = true }); !errors.Is(err, ErrAuthBusy) || disconnected {
		t.Fatal("busy password change mutated sessions", err)
	}
	for i := 0; i < cap(auth.passwordSlots); i++ {
		<-auth.passwordSlots
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := auth.hashPassword(ctx, "password"); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled bcrypt work started", err)
	}
	if _, err := auth.hashPassword(context.Background(), "password"); err != nil {
		t.Fatal("capacity did not recover", err)
	}
}
