package repository

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/domain"
)

func prekeyIDMigration(t *testing.T) (string, string) {
	t.Helper()
	data, err := os.ReadFile("../../migrations/018_e2ee_key_ids.sql")
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(string(data), "-- +goose Down", 2)
	if len(parts) != 2 {
		t.Fatal("migration must have both Up and Down")
	}
	return parts[0], parts[1]
}

// Mirror Goose's transaction boundary, including rollback on SQL failure.
func applyPrekeyIDMigration(ctx context.Context, db *sql.DB, query string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, query); err != nil {
		return err
	}
	return tx.Commit()
}

func prekeySnapshot(t *testing.T, db *sql.DB) []string {
	t.Helper()
	var snapshot []string
	for _, table := range []string{"device_keys", "one_time_prekeys", "kyber_prekeys", "encrypted_messages"} {
		rows, err := db.QueryContext(t.Context(), "SELECT row_to_json(t)::text FROM "+pq.QuoteIdentifier(table)+" AS t ORDER BY row_to_json(t)::text")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				rows.Close()
				t.Fatal(err)
			}
			snapshot = append(snapshot, table+":"+value)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}

func TestPreKeyIDMigrationPostgres(t *testing.T) {
	up, down := prekeyIDMigration(t)
	t.Run("up and down preserve keys flags timestamps and nullable message IDs", func(t *testing.T) {
		db := prekeyPostgresThrough(t, "017_message_receipts.sql")
		if _, err := db.ExecContext(t.Context(), `INSERT INTO one_time_prekeys(user_id,device_id,key_id,public_key,used,used_at)
	 VALUES('prekey-a',1,1,'public',TRUE,'2026-01-01T00:00:00Z');
	 INSERT INTO kyber_prekeys(user_id,device_id,key_id,public_key,signature,is_last_resort)
	 VALUES('prekey-a',1,2,'public','signature',TRUE);
	 INSERT INTO encrypted_messages(message_id,sender_id,sender_device_id,recipient_id,recipient_device_id,ciphertext,message_type,timestamp)
	 VALUES('migration-envelope','prekey-b',1,'prekey-a',1,'ciphertext',1,1)`); err != nil {
			t.Fatal(err)
		}
		before := prekeySnapshot(t, db)
		for _, query := range []string{up, down, up} {
			if err := applyPrekeyIDMigration(t.Context(), db, query); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(prekeySnapshot(t, db), before) {
				t.Fatal("migration changed existing key/message data")
			}
		}
	})
	t.Run("invalid legacy ID aborts up without deleting data", func(t *testing.T) {
		db := prekeyPostgresThrough(t, "017_message_receipts.sql")
		if _, err := db.ExecContext(t.Context(), `INSERT INTO one_time_prekeys(user_id,device_id,key_id,public_key)
	 VALUES('prekey-a',1,-1,'public')`); err != nil {
			t.Fatal(err)
		}
		before := prekeySnapshot(t, db)
		var pgErr *pq.Error
		if err := applyPrekeyIDMigration(t.Context(), db, up); !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatalf("expected legacy check failure, got %v", err)
		}
		if !reflect.DeepEqual(prekeySnapshot(t, db), before) {
			t.Fatal("failed migration changed existing data")
		}
		var dataType string
		if err := db.QueryRowContext(t.Context(), `SELECT data_type FROM information_schema.columns
	 WHERE table_schema=current_schema() AND table_name='device_keys' AND column_name='registration_id'`).Scan(&dataType); err != nil || dataType != "integer" {
			t.Fatalf("failed Up left partial schema: %s %v", dataType, err)
		}
	})
	t.Run("down refuses narrowing without losing large IDs", func(t *testing.T) {
		db := prekeyPostgres(t)
		repo := NewPgOneTimePreKeyRepository(db)
		if err := repo.BulkInsert(t.Context(), "prekey-a", 1, []domain.OneTimePreKey{{KeyID: 4294967295, PublicKey: prekeyPublic}}); err != nil {
			t.Fatal(err)
		}
		before := prekeySnapshot(t, db)
		var pgErr *pq.Error
		if err := applyPrekeyIDMigration(t.Context(), db, down); !errors.As(err, &pgErr) || pgErr.Code != "22003" {
			t.Fatalf("expected narrowing failure, got %v", err)
		}
		if !reflect.DeepEqual(prekeySnapshot(t, db), before) {
			t.Fatal("failed Down changed existing data")
		}
		key, err := repo.Acquire(t.Context(), "prekey-a", 1)
		if err != nil || key.KeyID != 4294967295 {
			t.Fatalf("large ID lost after failed Down: %v", err)
		}
	})
}
