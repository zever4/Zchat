package service

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/pkg/postgres"
	"github.com/zever4/Zchat/internal/repository"
)

// Each invocation owns a disposable schema on the explicitly configured test DB.
// All connections use that schema; persistent application tables are untouched.
func friendPostgres(t *testing.T) (*sql.DB, *FriendService) {
	t.Helper()
	dsn := os.Getenv("ZCHAT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set ZCHAT_TEST_DATABASE_URL to run PostgreSQL transaction tests")
	}
	admin, err := postgres.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("friend_tx_%d", time.Now().UnixNano())
	if _, err = admin.Exec(`CREATE SCHEMA ` + pq.QuoteIdentifier(schema)); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		defer admin.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + pq.QuoteIdentifier(schema) + ` CASCADE`); err != nil {
			t.Error(err)
		}
	})
	scoped := dsn
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		q.Set("application_name", schema)
		u.RawQuery = q.Encode()
		scoped = u.String()
	} else {
		scoped += " search_path=" + schema + " application_name=" + schema
	}
	db, err := postgres.Open(scoped)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(24)
	t.Cleanup(func() { db.Close() })
	files, err := filepath.Glob("../../migrations/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatal("migrations unavailable", err)
	}
	for _, file := range files {
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		up := strings.SplitN(string(content), "-- +goose Down", 2)[0]
		if _, err = db.Exec(up); err != nil {
			t.Fatalf("migration %s: %v", file, err)
		}
	}
	users := repository.NewPgUserRepository(db)
	svc := NewFriendService(repository.NewPgFriendRequestRepository(db), repository.NewPgChatRepository(db), users, repository.NewPgFriendTransactor(db))
	return db, svc
}

func resetFriends(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`TRUNCATE users,chats RESTART IDENTITY CASCADE;
 INSERT INTO users(id,user_uuid,username,email,password_hash) VALUES
 (1,'tx-1','sender','tx1@example.invalid','unused'),(2,'tx-2','recipient','tx2@example.invalid','unused'),(3,'tx-3','outsider','tx3@example.invalid','unused');
 INSERT INTO user_settings(user_id) VALUES(1),(2),(3)`)
	if err != nil {
		t.Fatal(err)
	}
}
func pendingFriend(t *testing.T, s *FriendService) int64 {
	t.Helper()
	r, err := s.SendRequest(context.Background(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	return r.ID
}
func friendState(t *testing.T, db *sql.DB, status string, chats, requests int) {
	t.Helper()
	var gotStatus string
	var gotChats, gotMembers, gotRequests int
	if err := db.QueryRow(`SELECT status FROM friend_requests ORDER BY id LIMIT 1`).Scan(&gotStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM chats`).Scan(&gotChats); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM chat_members`).Scan(&gotMembers); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM friend_requests`).Scan(&gotRequests); err != nil {
		t.Fatal(err)
	}
	if gotStatus != status || gotChats != chats || gotMembers != 2*chats || gotRequests != requests {
		t.Fatalf("status=%s chats=%d members=%d requests=%d; want %s/%d/%d/%d", gotStatus, gotChats, gotMembers, gotRequests, status, chats, 2*chats, requests)
	}
}
func execFriendSQL(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}
}

func TestOpenPrivateChatPostgres(t *testing.T) {
	db, svc := friendPostgres(t)
	ctx := context.Background()
	resetFriends(t, db)
	if _, err := svc.OpenPrivateChat(ctx, 1, 1); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal("self chat:", err)
	}
	if _, err := svc.OpenPrivateChat(ctx, 1, 99); !errors.Is(err, domain.ErrUserNotFound) {
		t.Fatal("missing recipient:", err)
	}
	if _, err := svc.OpenPrivateChat(ctx, 1, 2); !errors.Is(err, domain.ErrDirectMessagesDisabled) {
		t.Fatal("default disallows strangers:", err)
	}

	users := repository.NewPgUserRepository(db)
	settings, err := users.GetSettings(ctx, 2)
	if err != nil || settings.AllowDirectMessagesFromNonFriends == nil || *settings.AllowDirectMessagesFromNonFriends {
		t.Fatalf("default recipient settings: %+v, %v", settings, err)
	}
	allow := true
	settings.AllowDirectMessagesFromNonFriends = &allow
	if err := users.UpsertSettings(ctx, 2, settings); err != nil {
		t.Fatal(err)
	}
	settings.AllowDirectMessagesFromNonFriends = nil // Older settings PUT payload.
	if err := users.UpsertSettings(ctx, 2, settings); err != nil {
		t.Fatal(err)
	}
	stored, err := users.GetSettings(ctx, 2)
	if err != nil || stored.AllowDirectMessagesFromNonFriends == nil || !*stored.AllowDirectMessagesFromNonFriends {
		t.Fatalf("legacy write changed audience: %+v, %v", stored, err)
	}

	requestID := pendingFriend(t, svc)
	execFriendSQL(t, db, `UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=1`)
	start := make(chan struct{})
	type result struct {
		chat *domain.Chat
		err  error
	}
	opened := make(chan result, 4)
	for range cap(opened) {
		go func() {
			<-start
			chat, err := svc.OpenPrivateChat(ctx, 1, 2)
			opened <- result{chat, err}
		}()
	}
	accepted := make(chan error, 1)
	go func() {
		<-start
		_, err := svc.AcceptRequest(ctx, requestID, 2)
		accepted <- err
	}()
	close(start)
	var id int64
	for range cap(opened) {
		result := <-opened
		if result.err != nil || result.chat == nil || result.chat.ID <= 0 || result.chat.OtherUserID != 2 {
			t.Fatalf("open chat: %+v, %v", result.chat, result.err)
		}
		if id != 0 && result.chat.ID != id {
			t.Fatalf("parallel open created %d and %d", id, result.chat.ID)
		}
		id = result.chat.ID
	}
	if err := <-accepted; err != nil {
		t.Fatal(err)
	}
	friendState(t, db, "accepted", 1, 1)

	execFriendSQL(t, db, `UPDATE user_settings SET allow_direct_messages=FALSE`)
	if _, err := svc.OpenPrivateChat(ctx, 3, 2); !errors.Is(err, domain.ErrDirectMessagesDisabled) {
		t.Fatal("disabled recipient still accepts strangers:", err)
	}
	existing, err := svc.OpenPrivateChat(ctx, 2, 1)
	if err != nil || existing == nil || existing.ID != id || existing.OtherUserID != 1 {
		t.Fatalf("existing reversed chat: %+v, %v", existing, err)
	}
	friendState(t, db, "accepted", 1, 1)
}

func TestFriendTransactionsPostgres(t *testing.T) {
	db, s := friendPostgres(t)
	ctx := context.Background()
	for _, mode := range []string{"explicit accept", "cross request"} {
		t.Run("rollback on member insert failure/"+mode, func(t *testing.T) {
			resetFriends(t, db)
			id := pendingFriend(t, s)
			execFriendSQL(t, db, `CREATE FUNCTION fail_members() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected membership failure'; END $$;
 CREATE TRIGGER fail_members BEFORE INSERT ON chat_members FOR EACH ROW EXECUTE FUNCTION fail_members()`)
			if mode == "explicit accept" {
				chat, err := s.AcceptRequest(ctx, id, 2)
				if err == nil || chat != nil {
					t.Fatal("failed accept returned success")
				}
			} else {
				req, err := s.SendRequest(ctx, 2, 1)
				if err == nil || req != nil {
					t.Fatal("failed cross accept returned success")
				}
			}
			friendState(t, db, "pending", 0, 1)
			execFriendSQL(t, db, `DROP TRIGGER fail_members ON chat_members; DROP FUNCTION fail_members()`)
			if _, err := s.AcceptRequest(ctx, id, 2); err != nil {
				t.Fatal(err)
			}
			friendState(t, db, "accepted", 1, 1)
		})
	}
	t.Run("commit failure rolls back status chat and members", func(t *testing.T) {
		resetFriends(t, db)
		id := pendingFriend(t, s)
		execFriendSQL(t, db, `CREATE FUNCTION fail_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected commit failure'; END $$;
 CREATE CONSTRAINT TRIGGER fail_commit AFTER UPDATE ON friend_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fail_commit()`)
		chat, err := s.AcceptRequest(ctx, id, 2)
		if err == nil || chat != nil {
			t.Fatal("commit failure returned success")
		}
		friendState(t, db, "pending", 0, 1)
		execFriendSQL(t, db, `DROP TRIGGER fail_commit ON friend_requests; DROP FUNCTION fail_commit()`)
	})
	t.Run("parallel accept retries return one chat", func(t *testing.T) {
		resetFriends(t, db)
		id := pendingFriend(t, s)
		const n = 16
		start := make(chan struct{})
		results := make(chan *domain.Chat, n)
		errs := make(chan error, n)
		for i := 0; i < n; i++ {
			go func() { <-start; chat, err := s.AcceptRequest(ctx, id, 2); results <- chat; errs <- err }()
		}
		close(start)
		var chatID int64
		for i := 0; i < n; i++ {
			chat := <-results
			err := <-errs
			if err != nil || chat == nil {
				t.Fatalf("accept: %v", err)
			}
			if i == 0 {
				chatID = chat.ID
			}
			if chat.ID != chatID || chat.OtherUserID != 1 {
				t.Fatal("retry returned a different chat/participant")
			}
		}
		friendState(t, db, "accepted", 1, 1)
	})
	t.Run("opposite simultaneous requests create one friendship", func(t *testing.T) {
		resetFriends(t, db)
		start := make(chan struct{})
		errs := make(chan error, 2)
		for _, pair := range [][2]int64{{1, 2}, {2, 1}} {
			go func(a, b int64) { <-start; _, err := s.SendRequest(ctx, a, b); errs <- err }(pair[0], pair[1])
		}
		close(start)
		for i := 0; i < 2; i++ {
			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		}
		friendState(t, db, "accepted", 1, 1)
	})
	t.Run("parallel same direction requests do not duplicate", func(t *testing.T) {
		resetFriends(t, db)
		start := make(chan struct{})
		errs := make(chan error, 12)
		for i := 0; i < 12; i++ {
			go func() { <-start; _, err := s.SendRequest(ctx, 1, 2); errs <- err }()
		}
		close(start)
		success := 0
		for i := 0; i < 12; i++ {
			err := <-errs
			if err == nil {
				success++
			} else if !errors.Is(err, domain.ErrFriendRequestExists) {
				t.Fatal(err)
			}
		}
		if success != 1 {
			t.Fatalf("successful requests=%d", success)
		}
		friendState(t, db, "pending", 0, 1)
	})
	t.Run("accept competes with reject without inconsistent state", func(t *testing.T) {
		for i := 0; i < 8; i++ {
			resetFriends(t, db)
			id := pendingFriend(t, s)
			start := make(chan struct{})
			errs := make(chan error, 2)
			go func() { <-start; _, err := s.AcceptRequest(ctx, id, 2); errs <- err }()
			go func() { <-start; errs <- s.RejectRequest(ctx, id, 2) }()
			close(start)
			first, second := <-errs, <-errs
			if (first == nil) == (second == nil) {
				t.Fatalf("expected one winner: %v, %v", first, second)
			}
			if first != nil && !errors.Is(first, domain.ErrFriendRequestNotPending) {
				t.Fatal(first)
			}
			if second != nil && !errors.Is(second, domain.ErrFriendRequestNotPending) {
				t.Fatal(second)
			}
			var status string
			if err := db.QueryRow(`SELECT status FROM friend_requests WHERE id=$1`, id).Scan(&status); err != nil {
				t.Fatal(err)
			}
			count := 0
			if status == "accepted" {
				count = 1
			} else if status != "rejected" {
				t.Fatal(status)
			}
			friendState(t, db, status, count, 1)
		}
	})
	t.Run("rejected cannot be accepted and reverse resend reuses row", func(t *testing.T) {
		resetFriends(t, db)
		id := pendingFriend(t, s)
		if err := s.RejectRequest(ctx, id, 2); err != nil {
			t.Fatal(err)
		}
		if err := s.RejectRequest(ctx, id, 2); err != nil {
			t.Fatal("reject retry", err)
		}
		if _, err := s.AcceptRequest(ctx, id, 2); !errors.Is(err, domain.ErrFriendRequestNotPending) {
			t.Fatal(err)
		}
		r, err := s.SendRequest(ctx, 2, 1)
		if err != nil || r.ID != id || r.FromUserID != 2 || r.ToUserID != 1 {
			t.Fatalf("reverse resend: %+v %v", r, err)
		}
		if _, err = s.AcceptRequest(ctx, id, 2); !errors.Is(err, domain.ErrFriendRequestNotFound) {
			t.Fatal("old recipient authorized", err)
		}
		if _, err = s.AcceptRequest(ctx, id, 1); err != nil {
			t.Fatal(err)
		}
		friendState(t, db, "accepted", 1, 1)
	})
	t.Run("privacy denial preserves friendship without opening a chat", func(t *testing.T) {
		resetFriends(t, db)
		id := pendingFriend(t, s)
		if _, err := s.AcceptRequest(ctx, id, 3); !errors.Is(err, domain.ErrFriendRequestNotFound) {
			t.Fatal(err)
		}
		execFriendSQL(t, db, `UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=1`)
		if chat, err := s.AcceptRequest(ctx, id, 2); err != nil || chat != nil {
			t.Fatalf("accept without chat: %+v %v", chat, err)
		}
		friendState(t, db, "accepted", 0, 1)
		execFriendSQL(t, db, `UPDATE user_settings SET allow_direct_messages=TRUE WHERE user_id=1`)
		chat, err := s.AcceptRequest(ctx, id, 2)
		if err != nil {
			t.Fatal(err)
		}
		execFriendSQL(t, db, `UPDATE user_settings SET allow_direct_messages=FALSE,allow_friend_requests=FALSE`)
		retry, err := s.AcceptRequest(ctx, id, 2)
		if err != nil || retry.ID != chat.ID {
			t.Fatalf("existing chat retry: %v", err)
		}
		friendState(t, db, "accepted", 1, 1)
	})
	t.Run("cancel while waiting for pair lock leaves pending", func(t *testing.T) {
		resetFriends(t, db)
		id := pendingFriend(t, s)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`SELECT id FROM users WHERE id=1 FOR UPDATE`); err != nil {
			t.Fatal(err)
		}
		timeout, cancel := context.WithTimeout(ctx, 70*time.Millisecond)
		defer cancel()
		if chat, err := s.AcceptRequest(timeout, id, 2); err == nil || chat != nil {
			t.Fatal("canceled accept succeeded")
		}
		if err = tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		friendState(t, db, "pending", 0, 1)
	})
	t.Run("settings update committed before creation is respected", func(t *testing.T) {
		resetFriends(t, db)
		id := pendingFriend(t, s)
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err = tx.Exec(`UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=1`); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { _, err := s.AcceptRequest(ctx, id, 2); done <- err }()
		// Observe the real database wait rather than infer concurrency from elapsed time.
		waitForSettingsLock(t, db)
		if err = tx.Commit(); err != nil {
			t.Fatal(err)
		}
		if err = <-done; err != nil {
			t.Fatal(err)
		}
		friendState(t, db, "accepted", 0, 1)
	})
	t.Run("creation holds settings lock until commit", func(t *testing.T) {
		resetFriends(t, db)
		id := pendingFriend(t, s)
		locked, release := make(chan struct{}), make(chan struct{})
		wrapped := *s
		wrapped.transactions = pausedFriendTransaction{inner: s.transactions, locked: locked, release: release}
		accepted := make(chan error, 1)
		go func() { _, err := wrapped.AcceptRequest(ctx, id, 2); accepted <- err }()
		<-locked
		updated := make(chan error, 1)
		go func() {
			_, err := db.Exec(`UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=1`)
			updated <- err
		}()
		var once sync.Once
		defer once.Do(func() { close(release) })
		waitForSettingsLock(t, db)
		once.Do(func() { close(release) })
		if err := <-accepted; err != nil {
			t.Fatal(err)
		}
		if err := <-updated; err != nil {
			t.Fatal(err)
		}
		friendState(t, db, "accepted", 1, 1)
	})
}

type pausedFriendTransaction struct {
	inner   repository.FriendTransactor
	locked  chan struct{}
	release chan struct{}
}

func (p pausedFriendTransaction) WithPair(ctx context.Context, a, b int64, fn func(repository.FriendRequestRepository, repository.ChatRepository, repository.UserRepository) error) error {
	return p.inner.WithPair(ctx, a, b, func(fr repository.FriendRequestRepository, cr repository.ChatRepository, ur repository.UserRepository) error {
		close(p.locked)
		select {
		case <-p.release:
		case <-ctx.Done():
			return ctx.Err()
		}
		return fn(fr, cr, ur)
	})
}
func waitForSettingsLock(t *testing.T, db *sql.DB) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var count int
		err := db.QueryRow(`SELECT count(*) FROM pg_stat_activity WHERE application_name=current_setting('application_name') AND wait_event_type='Lock' AND query ILIKE '%user_settings%'`).Scan(&count)
		if err != nil {
			t.Fatal(err)
		}
		if count > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected a real PostgreSQL settings lock wait")
}
