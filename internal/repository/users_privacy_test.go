package repository

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/pkg/postgres"
)

// Uses session-local temporary tables; never modifies persistent application data.
func TestSearchEmailVisibilityPostgres(t *testing.T) {
	dsn := os.Getenv("ZCHAT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set ZCHAT_TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	db, err := postgres.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, `CREATE TEMP TABLE users (id BIGINT, user_uuid TEXT, username TEXT, email TEXT, created_at TIMESTAMPTZ DEFAULT NOW(), avatar_url TEXT DEFAULT '');
 CREATE TEMP TABLE user_settings (user_id BIGINT, show_email BOOLEAN);
 CREATE TEMP TABLE friend_requests (from_user_id BIGINT, to_user_id BIGINT, status TEXT);
 INSERT INTO users (id,user_uuid,username,email) VALUES
 (1,'u1','privacy_hidden','hidden@example.invalid'),(2,'u2','privacy_visible','visible@example.invalid'),
 (3,'u3','privacy_missing','missing@example.invalid'),(4,'u4','privacy_self','self@example.invalid');
 INSERT INTO user_settings VALUES (1,FALSE),(2,TRUE),(4,TRUE);
 INSERT INTO friend_requests VALUES (4,1,'accepted'),(2,4,'accepted'),(4,3,'accepted');`)
	if err != nil {
		t.Fatal(err)
	}
	users, err := NewPgUserRepository(db).SearchByUsername(ctx, "privacy_", 4, 20)
	if err != nil {
		t.Fatal(err)
	}
	friends, err := NewPgFriendRequestRepository(db).GetFriends(ctx, 4)
	if err != nil {
		t.Fatal(err)
	}
	for name, results := range map[string][]*domain.User{"search": users, "friends": friends} {
		if len(results) != 3 {
			t.Fatalf("%s: got %d users", name, len(results))
		}
		for _, u := range results {
			if u.UUID != "u"+strconv.FormatInt(u.ID, 10) {
				t.Fatalf("%s: public identity does not match numeric user ID", name)
			}
			want := ""
			if u.ID == 2 {
				want = "visible@example.invalid"
			}
			if u.ID == 4 || u.Email != want {
				t.Fatalf("%s: unexpected result: %+v", name, u)
			}
		}
	}
}
