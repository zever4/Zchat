// Package testutil holds opt-in integration fixtures. It is not used by runtime.
package testutil

import (
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/pkg/postgres"
)

// Postgres applies the actual migrations in a task-owned disposable schema.
// The caller must supply an explicitly selected test DB, never the app DSN.
func Postgres(t *testing.T) *sql.DB {
	return postgresFixture(t, true)
}

func EmptyPostgres(t *testing.T) *sql.DB {
	return postgresFixture(t, false)
}

func postgresFixture(t *testing.T, applyMigrations bool) *sql.DB {
	t.Helper()
	dsn := os.Getenv("ZCHAT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set ZCHAT_TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	admin, err := postgres.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { admin.Close() })
	schema := "e2ee_device_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = admin.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schema)); err != nil {
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
	t.Cleanup(func() { db.Close() })
	if !applyMigrations {
		return db
	}
	_, filename, _, _ := runtime.Caller(0)
	files, err := filepath.Glob(filepath.Join(filepath.Dir(filename), "../../migrations/*.sql"))
	if err != nil || len(files) == 0 {
		t.Fatal("migrations unavailable", err)
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		up := strings.SplitN(string(data), "-- +goose Down", 2)[0]
		if _, err = db.ExecContext(ctx, up); err != nil {
			t.Fatalf("migration %s: %v", filepath.Base(file), err)
		}
	}
	return db
}
