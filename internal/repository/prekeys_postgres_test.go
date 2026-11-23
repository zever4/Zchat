package repository

import (
	"bytes"
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
)

// Apply the actual migrations in a private schema: a hand-written test table
// could accidentally include the missing "id" column and conceal schema drift.
func prekeyPostgres(t *testing.T) *sql.DB {
	t.Helper()
	return prekeyPostgresThrough(t, "")
}

func prekeyPostgresThrough(t *testing.T, lastMigration string) *sql.DB {
	t.Helper()
	dsn := os.Getenv("ZCHAT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set ZCHAT_TEST_DATABASE_URL to run PostgreSQL prekey tests")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	admin, err := postgres.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := fmt.Sprintf("prekey_test_%d", time.Now().UnixNano())
	if _, err := admin.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := admin.ExecContext(ctx, "DROP SCHEMA "+pq.QuoteIdentifier(schema)+" CASCADE"); err != nil {
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
	db.SetMaxOpenConns(12)
	t.Cleanup(func() { db.Close() })
	files, err := filepath.Glob("../../migrations/*.sql")
	if err != nil || len(files) == 0 {
		t.Fatal("migrations unavailable", err)
	}
	for _, file := range files {
		if lastMigration != "" && filepath.Base(file) > lastMigration {
			break
		}
		content, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		up := strings.SplitN(string(content), "-- +goose Down", 2)[0]
		if _, err := db.ExecContext(ctx, up); err != nil {
			t.Fatalf("migration %s: %v", file, err)
		}
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO users(user_uuid,username,email,password_hash) VALUES
	 ('prekey-a','prekey_a','prekey-a@example.invalid','unused'),
	 ('prekey-b','prekey_b','prekey-b@example.invalid','unused');
	 INSERT INTO device_keys(user_id,device_id,registration_id,identity_key,signed_pre_key_id,
	 signed_pre_key_public,signed_pre_key_signature,signed_pre_key_timestamp)
	 SELECT user_id,device_id,1,'identity',1,'signed','signature',1
	 FROM (VALUES ('prekey-a',1),('prekey-a',2),('prekey-b',1)) AS devices(user_id,device_id)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// These bytes are deliberately opaque SQL fixtures, not valid crypto material.
// Parsing and signature verification belong to the libsignal tests.
var prekeyPublic = []byte("public-key")

type acquiredPrekey struct {
	userID   string
	deviceID int
	keyID    uint32
	public   []byte
}

func TestPreKeyAcquirePostgres(t *testing.T) {
	db := prekeyPostgres(t)
	ec := NewPgOneTimePreKeyRepository(db)
	pqRepo := NewPgKyberPreKeyRepository(db)
	for _, table := range []string{"one_time_prekeys", "kyber_prekeys"} {
		t.Run(table, func(t *testing.T) {
			insert := func(t *testing.T, user string, device int, ids ...uint32) {
				t.Helper()
				var err error
				if table == "one_time_prekeys" {
					keys := make([]domain.OneTimePreKey, len(ids))
					for i, id := range ids {
						keys[i] = domain.OneTimePreKey{KeyID: id, PublicKey: prekeyPublic}
					}
					err = ec.BulkInsert(t.Context(), user, device, keys)
				} else {
					keys := make([]domain.KyberPreKey, len(ids))
					for i, id := range ids {
						keys[i] = domain.KyberPreKey{KeyID: id, PublicKey: prekeyPublic, Signature: []byte("signature")}
					}
					err = pqRepo.BulkInsert(t.Context(), user, device, keys)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			acquire := func(ctx context.Context, user string, device int) (acquiredPrekey, error) {
				if table == "one_time_prekeys" {
					key, err := ec.Acquire(ctx, user, device)
					if err != nil {
						return acquiredPrekey{}, err
					}
					return acquiredPrekey{key.UserID, key.DeviceID, key.KeyID, key.PublicKey}, nil
				}
				key, err := pqRepo.Acquire(ctx, user, device)
				if err != nil {
					return acquiredPrekey{}, err
				}
				if key.IsLastResort || !bytes.Equal(key.Signature, []byte("signature")) {
					return acquiredPrekey{}, errors.New("unexpected Kyber metadata")
				}
				return acquiredPrekey{key.UserID, key.DeviceID, key.KeyID, key.PublicKey}, nil
			}
			reset := func(t *testing.T) {
				t.Helper()
				if _, err := db.ExecContext(t.Context(), "TRUNCATE "+table); err != nil {
					t.Fatal(err)
				}
			}
			wantKey := func(t *testing.T, user string, device int, id uint32) {
				t.Helper()
				key, err := acquire(t.Context(), user, device)
				if err != nil || key.userID != user || key.deviceID != device || key.keyID != id || !bytes.Equal(key.public, prekeyPublic) {
					t.Fatalf("acquire %s/%d: key=%+v err=%v; want key %d", user, device, key, err, id)
				}
				var consumed bool
				if err := db.QueryRowContext(t.Context(), "SELECT used AND used_at IS NOT NULL FROM "+table+
					" WHERE user_id=$1 AND device_id=$2 AND key_id=$3", user, device, id).Scan(&consumed); err != nil || !consumed {
					t.Fatalf("key not durably marked used: %v", err)
				}
			}

			t.Run("composite key scope and exhaustion", func(t *testing.T) {
				reset(t)
				for _, owner := range []struct {
					user   string
					device int
				}{{"prekey-a", 1}, {"prekey-a", 2}, {"prekey-b", 1}} {
					insert(t, owner.user, owner.device, 7, 8)
				}
				wantKey(t, "prekey-a", 1, 7)
				wantKey(t, "prekey-a", 1, 8)
				if _, err := acquire(t.Context(), "prekey-a", 1); !errors.Is(err, domain.ErrPreKeyPoolExhausted) {
					t.Fatalf("expected exhaustion, got %v", err)
				}
				wantKey(t, "prekey-a", 2, 7)
				wantKey(t, "prekey-b", 1, 7)
			})

			t.Run("concurrent consumers receive distinct keys", func(t *testing.T) {
				reset(t)
				ids := make([]uint32, 32)
				for i := range ids {
					ids[i] = uint32(i + 1)
				}
				insert(t, "prekey-a", 1, ids...)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				results := make(chan acquiredPrekey, 36)
				var wg sync.WaitGroup
				start := make(chan struct{})
				for range 36 {
					wg.Go(func() {
						<-start
						key, err := acquire(ctx, "prekey-a", 1)
						if err == nil {
							results <- key
						} else if !errors.Is(err, domain.ErrPreKeyPoolExhausted) {
							t.Errorf("concurrent acquire: %v", err)
						}
					})
				}
				close(start)
				wg.Wait()
				close(results)
				seen := make(map[uint32]bool)
				for key := range results {
					if seen[key.keyID] || key.userID != "prekey-a" || key.deviceID != 1 || !bytes.Equal(key.public, prekeyPublic) {
						t.Errorf("duplicate or incorrect key: %+v", key)
					}
					seen[key.keyID] = true
				}
				if len(seen) != len(ids) {
					t.Fatalf("got %d unique keys, want %d", len(seen), len(ids))
				}
			})

			t.Run("locked key skipped and available after rollback", func(t *testing.T) {
				reset(t)
				insert(t, "prekey-a", 1, 1, 2)
				tx, err := db.BeginTx(t.Context(), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.ExecContext(t.Context(), "SELECT key_id FROM "+table+
					" WHERE user_id='prekey-a' AND device_id=1 AND key_id=1 FOR UPDATE"); err != nil {
					t.Fatal(err)
				}
				wantKey(t, "prekey-a", 1, 2)
				if err := tx.Rollback(); err != nil {
					t.Fatal(err)
				}
				wantKey(t, "prekey-a", 1, 1)
			})

			t.Run("failed update preserves key and reports database error", func(t *testing.T) {
				reset(t)
				insert(t, "prekey-a", 1, 1)
				if _, err := db.ExecContext(t.Context(), "ALTER TABLE "+table+" ADD CONSTRAINT fail_acquire CHECK (NOT used)"); err != nil {
					t.Fatal(err)
				}
				if _, err := acquire(t.Context(), "prekey-a", 1); err == nil || errors.Is(err, domain.ErrPreKeyPoolExhausted) {
					t.Errorf("database error became success/exhaustion: %v", err)
				}
				if _, err := db.ExecContext(t.Context(), "ALTER TABLE "+table+" DROP CONSTRAINT fail_acquire"); err != nil {
					t.Fatal(err)
				}
				wantKey(t, "prekey-a", 1, 1)
			})

			t.Run("cancellation does not consume key", func(t *testing.T) {
				reset(t)
				insert(t, "prekey-a", 1, 1)
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				if _, err := acquire(ctx, "prekey-a", 1); !errors.Is(err, context.Canceled) {
					t.Fatalf("expected cancellation, got %v", err)
				}
				wantKey(t, "prekey-a", 1, 1)
			})
		})
	}
}

func TestKyberLastResortPostgres(t *testing.T) {
	db := prekeyPostgres(t)
	repo := NewPgKyberPreKeyRepository(db)
	if err := repo.BulkInsert(t.Context(), "prekey-a", 1, []domain.KyberPreKey{
		{KeyID: 1, PublicKey: prekeyPublic, Signature: []byte("last-resort"), IsLastResort: true},
		{KeyID: 5, PublicKey: prekeyPublic, Signature: []byte("one-time")},
	}); err != nil {
		t.Fatal(err)
	}
	for i, wantID := range []uint32{5, 1, 1, 1} {
		key, err := repo.Acquire(t.Context(), "prekey-a", 1)
		if err != nil || key.KeyID != wantID || key.IsLastResort != (i > 0) {
			t.Fatalf("acquire %d: key=%+v err=%v", i, key, err)
		}
	}
	var used bool
	var usedAt sql.NullTime
	if err := db.QueryRowContext(t.Context(), `SELECT used,used_at FROM kyber_prekeys
	 WHERE user_id='prekey-a' AND device_id=1 AND key_id=1`).Scan(&used, &usedAt); err != nil || used || usedAt.Valid {
		t.Fatalf("last-resort was consumed: used=%t used_at=%v err=%v", used, usedAt, err)
	}
	if available, err := repo.CountAvailable(t.Context(), "prekey-a", 1); err != nil || available != 0 {
		t.Fatalf("one-time available=%d err=%v; want 0", available, err)
	}
}
