package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"
)

type testProxy struct {
	listener net.Listener
	drop     atomic.Bool
	mu       sync.Mutex
	sockets  map[net.Conn]struct{}
	wg       sync.WaitGroup
}

func newTestProxy(t *testing.T, upstream string) *testProxy {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &testProxy{listener: l, sockets: make(map[net.Conn]struct{})}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			p.mu.Lock()
			p.sockets[client] = struct{}{}
			p.mu.Unlock()
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer client.Close()
				if upstream == "" {
					_, _ = io.Copy(io.Discard, client)
					return
				}
				server, err := net.DialTimeout("tcp", upstream, time.Second)
				if err != nil {
					return
				}
				p.mu.Lock()
				p.sockets[server] = struct{}{}
				p.mu.Unlock()
				defer server.Close()
				p.wg.Add(1)
				go func() { defer p.wg.Done(); p.copy(server, client); server.Close(); client.Close() }()
				p.copy(client, server)
			}()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		// Accept may have returned just before Close; wait for its tracked sockets
		// to become visible, then close until all proxy goroutines have finished.
		finished := make(chan struct{})
		go func() { p.wg.Wait(); close(finished) }()
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			p.mu.Lock()
			for socket := range p.sockets {
				socket.Close()
			}
			p.mu.Unlock()
			select {
			case <-finished:
				return
			case <-ticker.C:
			case <-deadline.C:
				t.Error("proxy leaked goroutines")
				return
			}
		}
	})
	return p
}
func (p *testProxy) copy(dst net.Conn, src net.Conn) {
	buf := make([]byte, 32768)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.drop.Load() {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}
func assertBounded(t *testing.T, work func(context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	done := make(chan error, 1)
	go func() { done <- work(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Fatalf("wrong cancellation: %v", err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("cancellation exceeded budget")
		}
	case <-time.After(time.Second):
		t.Fatal("operation stuck after its context expired")
	}
}

func TestHandshakeCancellation(t *testing.T) {
	p := newTestProxy(t, "") // TCP succeeds; PostgreSQL startup never replies.
	db, err := Open("postgres://test:test@" + p.listener.Addr().String() + "/test?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	assertBounded(t, db.PingContext)
}

func TestPostgresCancellationAndRecovery(t *testing.T) {
	dsn := os.Getenv("ZCHAT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ZCHAT_TEST_DATABASE_URL is required for real PostgreSQL")
	}
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	p := newTestProxy(t, net.JoinHostPort(cfg.Host, fmt.Sprint(cfg.Port)))
	proxyCfg := cfg.Clone()
	proxyCfg.Host, _, _ = net.SplitHostPort(p.listener.Addr().String())
	_, proxyPort, _ := net.SplitHostPort(p.listener.Addr().String())
	var port int
	fmt.Sscan(proxyPort, &port)
	proxyCfg.Port = uint16(port)
	// Keep all credentials/search_path/SSL options; replace only the destination.
	newPool := func(t *testing.T) *sql.DB {
		t.Helper()
		c := &connector{config: proxyCfg.Clone(), groups: make(map[*socketDialer]struct{})}
		db := sql.OpenDB(c)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { p.drop.Store(false); c.close(); db.Close() })
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			t.Fatal(err)
		}
		return db
	}
	for _, operation := range []string{"ping", "exec", "query", "begin", "prepare", "stmt exec", "stmt query", "commit", "rollback"} {
		t.Run(operation, func(t *testing.T) {
			db := newPool(t)
			var tx *sql.Tx
			var stmt *sql.Stmt
			var cancelTx context.CancelFunc
			if operation == "commit" || operation == "rollback" {
				var ctx context.Context
				ctx, cancelTx = context.WithCancel(context.Background())
				defer cancelTx()
				tx, err = db.BeginTx(ctx, nil)
				if err != nil {
					t.Fatal(err)
				}
			}
			if operation == "stmt exec" || operation == "stmt query" {
				stmt, err = db.PrepareContext(context.Background(), "SELECT $1::int")
				if err != nil {
					t.Fatal(err)
				}
				defer stmt.Close()
			}
			p.drop.Store(true)
			assertBounded(t, func(ctx context.Context) error {
				switch operation {
				case "ping":
					return db.PingContext(ctx)
				case "exec":
					_, err := db.ExecContext(ctx, "SELECT 1")
					return err
				case "query":
					return db.QueryRowContext(ctx, "SELECT 1").Scan(new(int))
				case "begin":
					_, err := db.BeginTx(ctx, nil)
					return err
				case "prepare":
					_, err := db.PrepareContext(ctx, "SELECT $1::int")
					return err
				case "stmt exec":
					_, err := stmt.ExecContext(ctx, 1)
					return err
				case "stmt query":
					return stmt.QueryRowContext(ctx, 1).Scan(new(int))
				default:
					stop := context.AfterFunc(ctx, cancelTx)
					defer stop()
					if operation == "commit" {
						return tx.Commit()
					}
					err := tx.Rollback()
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return err
				}
			})
			p.drop.Store(false)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var value int
			if err := db.QueryRowContext(ctx, "SELECT 42").Scan(&value); err != nil || value != 42 {
				t.Fatalf("pool failed to recover: value=%d err=%v", value, err)
			}
		})
	}
	t.Run("metadata, multiple results, array parameters and released cancellation", func(t *testing.T) {
		db := newPool(t)
		var before, after int
		if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&before); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		rows, err := db.QueryContext(ctx, "SELECT 1::bigint; SELECT 2::bigint")
		if err != nil {
			t.Fatal(err)
		}
		types, err := rows.ColumnTypes()
		if err != nil || len(types) != 1 || types[0].DatabaseTypeName() != "INT8" {
			t.Fatalf("metadata lost: %v", err)
		}
		if !rows.Next() {
			t.Fatal("missing first result")
		}
		if err := rows.Scan(new(int64)); err != nil {
			t.Fatal(err)
		}
		if rows.Next() {
			t.Fatal("unexpected extra row in first result")
		}
		if !rows.NextResultSet() || !rows.Next() {
			t.Fatal("missing second result")
		}
		rows.Close()
		cancel() // Must not close a healthy connection after its operation ended.
		if err := db.QueryRow("SELECT pg_backend_pid()").Scan(&after); err != nil || before != after {
			t.Fatalf("late cancellation replaced healthy connection: %d/%d %v", before, after, err)
		}
		var sum int
		if err := db.QueryRowContext(context.Background(), "SELECT sum(v) FROM unnest($1::int[]) AS v", pq.Array([]int{1, 2, 3})).Scan(&sum); err != nil || sum != 6 {
			t.Fatalf("array/reuse: %d %v", sum, err)
		}
	})
	t.Run("closing pool interrupts idle prepared statement deallocation", func(t *testing.T) {
		// Use Open's Driver binding so Close can shut transports before sql.DB.
		db := newPool(t)
		stmt, err := db.PrepareContext(context.Background(), "SELECT 1")
		if err != nil {
			t.Fatal(err)
		}
		defer stmt.Close()
		p.drop.Store(true)
		start := time.Now()
		if err := Close(db); err != nil {
			t.Fatal(err)
		}
		if time.Since(start) > time.Second {
			t.Fatal("pool close blocked on idle statement")
		}
		p.drop.Store(false)
	})
}

func TestStartupCancellationDuringGooseMigration(t *testing.T) {
	dsn := os.Getenv("ZCHAT_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("ZCHAT_TEST_DATABASE_URL is required")
	}
	admin, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("startup_cancel_%d", time.Now().UnixNano())
	if _, err := admin.Exec("CREATE SCHEMA " + pq.QuoteIdentifier(schema)); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec("DROP SCHEMA " + pq.QuoteIdentifier(schema) + " CASCADE")
	cfg, err := pq.NewConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range map[string]string{"DB_HOST": cfg.Host, "DB_PORT": fmt.Sprint(cfg.Port), "DB_USER": cfg.User, "DB_PASS": cfg.Password, "DB_NAME": cfg.Database, "DB_SSLMODE": string(cfg.SSLMode), "PGOPTIONS": "-c search_path=" + schema} {
		t.Setenv(key, value)
	}
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.Mkdir(filepath.Join(dir, "migrations"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "migrations", "001_slow.sql"), []byte("-- +goose Up\nCREATE TABLE startup_probe(id int);\nSELECT pg_sleep(1);\n-- +goose Down\nDROP TABLE startup_probe;\n"), 0600); err != nil {
		t.Fatal(err)
	}
	assertBounded(t, func(ctx context.Context) error {
		db, err := InitPostgresDB(ctx)
		if db != nil {
			db.Close()
		}
		return err
	})
}
