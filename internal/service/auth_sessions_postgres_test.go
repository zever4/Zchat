package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/auth"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

func TestAuthSessionsPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	ctx := context.Background()
	const password = "Original-session-password-2026"
	const changed = "Changed-session-password-2026"
	newAuth := func() *AuthService {
		return NewAuthService(repository.NewPgUserRepository(db), "postgres-session-secret-at-least-32-bytes")
	}
	reset := func() { execFriendSQL(t, db, `TRUNCATE users RESTART IDENTITY CASCADE`) }
	register := func(t *testing.T, a *AuthService) (string, string) {
		t.Helper()
		email := uuid.NewString() + "@sessions.invalid"
		token, err := a.Register(ctx, "user_"+uuid.NewString(), email, password)
		if err != nil {
			t.Fatal(err)
		}
		return token, email
	}
	assertValid := func(t *testing.T, a *AuthService, token string, want bool) {
		t.Helper()
		_, err := a.ValidateToken(token)
		if (err == nil) != want {
			claims, parseErr := a.ParseToken(token)
			if parseErr != nil {
				t.Logf("JWT parse rejected before DB validation: %v at %s", parseErr, time.Now().UTC().Format(time.RFC3339Nano))
				_, detail := jwt.ParseWithClaims(token, new(SessionClaims), func(*jwt.Token) (any, error) { return []byte(a.jwtSecret), nil }, jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithIssuer("zchat"), jwt.WithAudience("zchat"), jwt.WithStrictDecoding())
				t.Logf("JWT validator diagnostic: %v", detail)
			} else {
				version, lookupErr := a.repo.GetSessionVersion(ctx, claims.UserID, claims.ID)
				t.Logf("session diagnostic: JWT version=%d DB version=%d lookup=%v issued=%s expires=%s now=%s", claims.Version, version, lookupErr, claims.IssuedAt.Time.UTC().Format(time.RFC3339Nano), claims.ExpiresAt.Time.UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
			}
			t.Fatalf("token valid=%v wanted=%v err=%v", err == nil, want, err)
		}
	}
	t.Run("independent sessions, current logout and all logout", func(t *testing.T) {
		reset()
		a := newAuth()
		first, email := register(t, a)
		second, _, _, err := a.Login(ctx, email, password)
		if err != nil {
			t.Fatal(err)
		}
		c1, _ := a.ParseToken(first)
		c2, _ := a.ParseToken(second)
		if first == second || c1.ID == c2.ID {
			t.Fatal("login reused session")
		}
		var disconnected string
		if err := a.Logout(ctx, first, false, func(id int64, sid string) {
			if id != c1.UserID {
				t.Error("wrong user")
			}
			disconnected = sid
		}); err != nil {
			t.Fatal(err)
		}
		if disconnected != c1.ID {
			t.Fatal("wrong session disconnected")
		}
		assertValid(t, a, first, false)
		assertValid(t, a, second, true)
		if err := a.Logout(ctx, second, true, func(_ int64, sid string) { disconnected = sid }); err != nil {
			t.Fatal(err)
		}
		if disconnected != "" {
			t.Fatal("logout-all only disconnected one session")
		}
		assertValid(t, a, second, false)
		fresh, _, _, err := a.Login(ctx, email, password)
		if err != nil {
			t.Fatal(err)
		}
		assertValid(t, a, fresh, true)
	})
	t.Run("password revokes all sessions and old credentials", func(t *testing.T) {
		reset()
		a := newAuth()
		first, email := register(t, a)
		second, _, _, _ := a.Login(ctx, email, password)
		disconnected := false
		if err := a.ChangePassword(ctx, first, password, changed, func(int64) { disconnected = true }); err != nil {
			t.Fatal(err)
		}
		if !disconnected {
			t.Fatal("no disconnect")
		}
		assertValid(t, a, first, false)
		assertValid(t, a, second, false)
		if token, _, _, err := a.Login(ctx, email, password); !errors.Is(err, domain.ErrInvalidCredentials) || token != "" {
			t.Fatal("old credentials accepted", err)
		}
		fresh, _, _, err := a.Login(ctx, email, changed)
		if err != nil {
			t.Fatal(err)
		}
		assertValid(t, a, fresh, true)
	})
	t.Run("wrong password and invalid new password preserve sessions", func(t *testing.T) {
		reset()
		a := newAuth()
		token, _ := register(t, a)
		for _, tc := range []struct{ old, new string }{{"wrong", changed}, {password, "12345"}, {password, string(make([]byte, 73))}} {
			disconnected := false
			if err := a.ChangePassword(ctx, token, tc.old, tc.new, func(int64) { disconnected = true }); err == nil || disconnected {
				t.Fatal("invalid change succeeded")
			}
			assertValid(t, a, token, true)
		}
	})
	t.Run("signed unpersisted token and another user's session denied", func(t *testing.T) {
		reset()
		a := newAuth()
		token, _ := register(t, a)
		other, _ := register(t, a)
		fake, _ := a.GenerateToken(1, 1)
		assertValid(t, a, fake, false)
		claims, _ := a.ParseToken(other)
		forged, err := a.signToken(1, 1, claims.ID, time.Now().Truncate(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		assertValid(t, a, forged, false)
		assertValid(t, a, token, true)
	})
	t.Run("DB expiry and deleted user denied", func(t *testing.T) {
		reset()
		a := newAuth()
		token, _ := register(t, a)
		execFriendSQL(t, db, `UPDATE auth_sessions SET expires_at=now()-interval '1 second'`)
		assertValid(t, a, token, false)
		execFriendSQL(t, db, `DELETE FROM users`)
		assertValid(t, a, token, false)
	})
	for _, mode := range []string{"delete failure", "deferred commit failure"} {
		t.Run("password rollback: "+mode, func(t *testing.T) {
			reset()
			a := newAuth()
			token, email := register(t, a)
			query := `CREATE FUNCTION fail_auth() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected session failure'; END $$; `
			if mode == "delete failure" {
				query += `CREATE TRIGGER fail_auth BEFORE DELETE ON auth_sessions FOR EACH ROW EXECUTE FUNCTION fail_auth()`
			} else {
				query += `CREATE CONSTRAINT TRIGGER fail_auth AFTER UPDATE ON users DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_auth()`
			}
			execFriendSQL(t, db, query)
			disconnected := false
			if err := a.ChangePassword(ctx, token, password, changed, func(int64) { disconnected = true }); err == nil || disconnected {
				t.Fatal("failed password change reported success")
			}
			table := "auth_sessions"
			if mode != "delete failure" {
				table = "users"
			}
			execFriendSQL(t, db, fmt.Sprintf(`DROP TRIGGER fail_auth ON %s; DROP FUNCTION fail_auth()`, table))
			assertValid(t, a, token, true)
			if _, _, _, err := a.Login(ctx, email, password); err != nil {
				t.Fatal("old password not preserved", err)
			}
			if _, _, _, err := a.Login(ctx, email, changed); err == nil {
				t.Fatal("failed password became active")
			}
		})
	}
	for _, table := range []string{"user_settings", "auth_sessions"} {
		t.Run("registration rollback: "+table, func(t *testing.T) {
			reset()
			a := newAuth()
			execFriendSQL(t, db, fmt.Sprintf(`CREATE FUNCTION fail_register() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected registration failure'; END $$; CREATE TRIGGER fail_register BEFORE INSERT ON %s FOR EACH ROW EXECUTE FUNCTION fail_register()`, table))
			token, err := a.Register(ctx, "registration_failure", "failure@sessions.invalid", password)
			if err == nil || token != "" {
				t.Fatal("failed registration returned token")
			}
			execFriendSQL(t, db, fmt.Sprintf(`DROP TRIGGER fail_register ON %s; DROP FUNCTION fail_register()`, table))
			for _, name := range []string{"users", "user_settings", "auth_sessions"} {
				var count int
				if err := db.QueryRow(`SELECT count(*) FROM ` + name).Scan(&count); err != nil || count != 0 {
					t.Fatalf("partial registration %s: %d %v", name, count, err)
				}
			}
		})
	}
	t.Run("issuance commit failure never returns usable token", func(t *testing.T) {
		reset()
		a := newAuth()
		token, email := register(t, a)
		execFriendSQL(t, db, `CREATE FUNCTION fail_issue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected issue commit failure'; END $$; CREATE CONSTRAINT TRIGGER fail_issue AFTER INSERT ON auth_sessions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_issue()`)
		issued, _, _, err := a.Login(ctx, email, password)
		if err == nil || issued != "" {
			t.Fatal("failed session issuance returned token")
		}
		execFriendSQL(t, db, `DROP TRIGGER fail_issue ON auth_sessions; DROP FUNCTION fail_issue()`)
		assertValid(t, a, token, true)
		var count int
		_ = db.QueryRow(`SELECT count(*) FROM auth_sessions`).Scan(&count)
		if count != 1 {
			t.Fatal("orphan session", count)
		}
	})
	t.Run("rotation commits before queued stale issuance", func(t *testing.T) {
		reset()
		a := newAuth()
		token, _ := register(t, a)
		claims, _ := a.ParseToken(token)
		users := repository.NewPgUserRepository(db)
		snapshot, _ := users.GetByID(ctx, claims.UserID)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`UPDATE users SET password_hash='changed',token_version=token_version+1 WHERE id=$1`, claims.UserID); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			done <- users.CreateSession(ctx, claims.UserID, snapshot.PasswordHash, snapshot.TokenVersion, uuid.NewString(), time.Now().Add(time.Hour))
		}()
		waitForGroupLock(t, db, "SELECT password_hash,token_version")
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; !errors.Is(err, domain.ErrInvalidCredentials) {
			t.Fatalf("stale credentials issued session: %v", err)
		}
		assertValid(t, a, token, false)
	})
	t.Run("issuance commits before rotation; issued token revoked", func(t *testing.T) {
		reset()
		a := newAuth()
		token, email := register(t, a)
		claims, _ := a.ParseToken(token)
		execFriendSQL(t, db, `CREATE FUNCTION pause_issue() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(hashtext(current_schema()),NEW.user_id::integer); RETURN NEW; END $$; CREATE TRIGGER pause_issue BEFORE INSERT ON auth_sessions FOR EACH ROW EXECUTE FUNCTION pause_issue()`)
		control, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer control.Rollback()
		if _, err = control.Exec(`SELECT pg_advisory_xact_lock(hashtext(current_schema()),$1::integer)`, claims.UserID); err != nil {
			t.Fatal(err)
		}
		type loginResult struct {
			token string
			err   error
		}
		issued := make(chan loginResult, 1)
		go func() { raw, _, _, err := a.Login(ctx, email, password); issued <- loginResult{raw, err} }()
		waitForGroupLock(t, db, "INSERT INTO auth_sessions")
		revoked := make(chan error, 1)
		// Independent service instance bypasses local barrier, so DB ordering is tested.
		other := newAuth()
		go func() { revoked <- other.ChangePassword(ctx, token, password, changed, func(int64) {}) }()
		waitForGroupLock(t, db, "UPDATE users SET password_hash")
		if err = control.Commit(); err != nil {
			t.Fatal(err)
		}
		result := <-issued
		if result.err != nil || result.token == "" {
			t.Fatal(result.err)
		}
		if err = <-revoked; err != nil {
			t.Fatal(err)
		}
		execFriendSQL(t, db, `DROP TRIGGER pause_issue ON auth_sessions; DROP FUNCTION pause_issue()`)
		assertValid(t, a, result.token, false)
	})
	t.Run("concurrent password changes have exactly one winner", func(t *testing.T) {
		reset()
		a := newAuth()
		token, email := register(t, a)
		var wg sync.WaitGroup
		start := make(chan struct{})
		results := make(chan struct {
			password string
			err      error
		}, 2)
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				next := fmt.Sprintf("Concurrent-new-password-%d", i)
				err := newAuth().ChangePassword(ctx, token, password, next, func(int64) {})
				results <- struct {
					password string
					err      error
				}{next, err}
			}(i)
		}
		close(start)
		wg.Wait()
		close(results)
		winners := 0
		for result := range results {
			if result.err == nil {
				winners++
				if _, _, _, err := a.Login(ctx, email, result.password); err != nil {
					t.Fatal(err)
				}
			}
		}
		if winners != 1 {
			t.Fatal("lost-update password race", winners)
		}
		assertValid(t, a, token, false)
	})
	t.Run("parallel login creates distinct active sessions", func(t *testing.T) {
		reset()
		a := newAuth()
		_, email := register(t, a)
		tokens := make(chan string, 8)
		errs := make(chan error, 8)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				token, _, _, err := a.Login(ctx, email, password)
				tokens <- token
				errs <- err
			}()
		}
		wg.Wait()
		close(tokens)
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatal(err)
			}
		}
		seen := map[string]bool{}
		for token := range tokens {
			if seen[token] {
				t.Fatal("duplicate token")
			}
			seen[token] = true
			assertValid(t, a, token, true)
		}
	})
	t.Run("secret rotation permits fresh login despite obsolete session rows", func(t *testing.T) {
		reset()
		a := newAuth()
		token, email := register(t, a)
		claims, _ := a.ParseToken(token)
		users := repository.NewPgUserRepository(db)
		snapshot, err := users.GetByID(ctx, claims.UserID)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 31; i++ {
			if err := users.CreateSession(ctx, claims.UserID, snapshot.PasswordHash, claims.Version, uuid.NewString(), time.Now().Add(time.Hour)); err != nil {
				t.Fatal(err)
			}
		}
		rotated := NewAuthService(users, "rotated-secret-with-more-than-32-bytes")
		assertValid(t, rotated, token, false)
		fresh, _, _, err := rotated.Login(ctx, email, password)
		if err != nil {
			t.Fatal("obsolete sessions prevented fresh login", err)
		}
		assertValid(t, rotated, fresh, true)
		if err := rotated.Logout(ctx, fresh, true, func(int64, string) {}); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := db.QueryRow(`SELECT count(*) FROM auth_sessions`).Scan(&count); err != nil || count != 0 {
			t.Fatal("logout-all left obsolete sessions", count, err)
		}
	})
	t.Run("expired cleanup preserves live sessions", func(t *testing.T) {
		reset()
		a := newAuth()
		token, _ := register(t, a)
		claims, _ := a.ParseToken(token)
		_, err := db.Exec(`INSERT INTO auth_sessions(id,user_id,token_version,expires_at) VALUES($1,$2,1,now()-interval '1 hour')`, uuid.NewString(), claims.UserID)
		if err != nil {
			t.Fatal(err)
		}
		users := repository.NewPgUserRepository(db)
		if err := users.CleanupExpiredSessions(ctx); err != nil {
			t.Fatal(err)
		}
		assertValid(t, a, token, true)
		var count int
		_ = db.QueryRow(`SELECT count(*) FROM auth_sessions`).Scan(&count)
		if count != 1 {
			t.Fatal(count)
		}
	})
	t.Run("canceled rotation waiting for row lock rolls back", func(t *testing.T) {
		reset()
		a := newAuth()
		token, _ := register(t, a)
		claims, _ := a.ParseToken(token)
		users := repository.NewPgUserRepository(db)
		user, _ := users.GetByID(ctx, claims.UserID)
		hash, _ := auth.Hash(changed)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`SELECT 1 FROM users WHERE id=$1 FOR UPDATE`, claims.UserID); err != nil {
			t.Fatal(err)
		}
		limited, cancel := context.WithTimeout(ctx, 60*time.Millisecond)
		defer cancel()
		if err := users.ChangePasswordAndRevoke(limited, claims.UserID, claims.Version, user.PasswordHash, hash); err == nil {
			t.Fatal("canceled operation succeeded")
		}
		_ = tx.Rollback()
		assertValid(t, a, token, true)
		current, _ := users.GetByID(ctx, claims.UserID)
		if current.PasswordHash != user.PasswordHash {
			t.Fatal("canceled password changed")
		}
	})
}

// Media failure must not undo committed DB revocation or revoke another login.
func TestAuthMediaRevocationPostgres(t *testing.T) {
	db, _ := friendPostgres(t)
	ctx := context.Background()
	a := NewAuthService(repository.NewPgUserRepository(db), "postgres-media-secret-at-least-32-bytes")
	password := "Media-session-password-2026!"
	email := uuid.NewString() + "@media.invalid"
	first, err := a.Register(ctx, "media_"+uuid.NewString(), email, password)
	if err != nil {
		t.Fatal(err)
	}
	second, _, _, err := a.Login(ctx, email, password)
	if err != nil {
		t.Fatal(err)
	}
	claims, _ := a.ParseToken(first)
	calls := 0
	a.SetSessionRevoker(func(_ context.Context, uid int64, sid string) error {
		calls++
		if uid != claims.UserID || sid != claims.ID {
			t.Error("wrong media scope")
		}
		if _, err := a.ValidateToken(first); !errors.Is(err, domain.ErrInvalidSession) {
			t.Error("media notified before DB revocation committed")
		}
		return errors.New("SFU temporarily unavailable")
	})
	if err := a.Logout(ctx, first, false, func(int64, string) {}); !errors.Is(err, ErrSessionMediaUnavailable) {
		t.Fatal("media failure hidden", err)
	}
	if calls != 1 {
		t.Fatal("missing media notification")
	}
	if _, err := a.ValidateToken(first); !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal("failed media cleanup restored session", err)
	}
	if _, err := a.ValidateToken(second); err != nil {
		t.Fatal("independent login revoked", err)
	}
}
