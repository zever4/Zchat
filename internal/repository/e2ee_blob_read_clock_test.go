//go:build linux

package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/domain"
)

var errBlobReadCommit = errors.New("synthetic read COMMIT response loss")

// Only the final clock query or the reply to a real read COMMIT is faulted.
// Every auth/identity/link/state query and lock uses the actual PostgreSQL pool.
// No immutable dates/triggers/crypto clocks are modified.
type blobReadFault struct {
	now        atomic.Pointer[time.Time]
	clockCalls atomic.Int32
	failCommit atomic.Bool
}
type blobReadConnector struct {
	driver.Connector
	fault *blobReadFault
}

func (c blobReadConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &blobReadConn{Conn: conn, fault: c.fault}, nil
}

type blobReadConn struct {
	driver.Conn
	fault *blobReadFault
}

func (c *blobReadConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.TrimSpace(query) == "SELECT clock_timestamp()" {
		if at := c.fault.now.Load(); at != nil {
			c.fault.clockCalls.Add(1)
			return &blobReadClockRow{at: *at}, nil
		}
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, query, args)
}
func (c *blobReadConn) Begin() (driver.Tx, error) {
	tx, err := c.Conn.Begin()
	if err != nil {
		return nil, err
	}
	return blobReadTx{Tx: tx, fault: c.fault}, nil
}
func (c *blobReadConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return blobReadTx{Tx: tx, fault: c.fault}, nil
}

type blobReadTx struct {
	driver.Tx
	fault *blobReadFault
}

func (tx blobReadTx) Commit() error {
	if err := tx.Tx.Commit(); err != nil {
		return err
	}
	if tx.fault.failCommit.Swap(false) {
		return errBlobReadCommit
	}
	return nil
}

type blobReadClockRow struct {
	at   time.Time
	done bool
}

func (*blobReadClockRow) Columns() []string { return []string{"clock_timestamp"} }
func (*blobReadClockRow) Close() error      { return nil }
func (r *blobReadClockRow) Next(v []driver.Value) error {
	if r.done {
		return io.EOF
	}
	v[0] = r.at
	r.done = true
	return nil
}

func blobReadPool(t *testing.T, db *sql.DB, fault *blobReadFault) *sql.DB {
	t.Helper()
	var schema string
	if err := db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("ZCHAT_TEST_DATABASE_URL"))
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("explicit PostgreSQL test URL required")
	}
	q := u.Query()
	q.Set("search_path", schema)
	q.Set("application_name", "blob_read_target")
	u.RawQuery = q.Encode()
	connector, err := pq.NewConnector(u.String())
	if err != nil {
		t.Fatal("fixture connector")
	}
	pool := sql.OpenDB(blobReadConnector{Connector: connector, fault: fault})
	t.Cleanup(func() { pool.Close() })
	return pool
}

func TestBlobReadAcceptedDeadlineIsExclusiveAndIndependentOfStagingPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	req := readAttachmentRequest(t, w.Attempt.Reservation)
	fault := new(blobReadFault)
	repo := NewPgE2EEBlobRepository(blobReadPool(t, f.f.db, fault))
	deadline := time.UnixMilli(receipt.ExpiresAtUnixMs)
	for _, at := range []time.Time{deadline.Add(-time.Millisecond), deadline, deadline.Add(time.Millisecond)} {
		fault.now.Store(&at)
		target, err := repo.ResolveRead(t.Context(), 2, w.StorageID, req)
		if at.Before(deadline) {
			if err != nil || target == nil || !target.ObservedAt.Equal(at) || !target.ExpiresAt.Equal(deadline) {
				t.Fatal("staging deadline hid accepted file", target, err)
			}
		} else if target != nil || !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
			t.Fatal("exclusive accepted deadline ignored", target, err)
		}
	}
	if fault.clockCalls.Load() != 3 {
		t.Fatal("final clock hook not exercised")
	}
	if _, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, req); err != nil {
		t.Fatal("fixture mutated production dates", err)
	}
	f.quota(t, 64, 1, 1)
}

func TestBlobReadCommitFailureNeverReturnsTargetOrAbsencePostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	req := readAttachmentRequest(t, w.Attempt.Reservation)
	fault := new(blobReadFault)
	repo := NewPgE2EEBlobRepository(blobReadPool(t, f.f.db, fault))
	for _, absent := range []bool{false, true} {
		r := cloneBlobRead(req)
		if absent {
			r.Binding.MessageId = uuid.NewString()
		}
		fault.failCommit.Store(true)
		got, err := repo.ResolveRead(t.Context(), 2, w.StorageID, r)
		if got != nil || !errors.Is(err, errBlobReadCommit) || errors.Is(err, domain.ErrE2EEBlobReadUnavailable) || fault.failCommit.Load() {
			t.Fatal("failed read COMMIT became a fact", got, err)
		}
		got, err = repo.ResolveRead(t.Context(), 2, w.StorageID, r)
		if absent {
			if got != nil || !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
				t.Fatal("absence replay", got, err)
			}
		} else if got == nil || err != nil {
			t.Fatal("target replay", got, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got, err := repo.ResolveRead(ctx, 2, w.StorageID, req); got != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled lookup returned authority", got, err)
	}
}

func TestBlobReadClockIsTakenAfterAttemptLockAndRechecksRecipientPostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	receipt := acceptAttachment(t, f, w.Attempt.Reservation)
	req := readAttachmentRequest(t, w.Attempt.Reservation)
	fault := new(blobReadFault)
	before := time.UnixMilli(receipt.ExpiresAtUnixMs).Add(-time.Millisecond)
	fault.now.Store(&before)
	pool := blobReadPool(t, f.f.db, fault)
	repo := NewPgE2EEBlobRepository(pool)
	lock, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.ExecContext(t.Context(), `SELECT attempt_id FROM e2ee_blob_attempts WHERE attempt_id=$1 FOR UPDATE`, w.Attempt.AttemptID); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	go func() {
		got, err := repo.ResolveRead(ctx, 2, w.StorageID, req)
		if got != nil {
			result <- errors.New("returned a target after deadline lock wait")
			return
		}
		result <- err
	}()
	barrier, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	for {
		var blocked bool
		if err = f.f.db.QueryRowContext(barrier, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='blob_read_target' AND wait_event_type='Lock')`).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-barrier.Done():
			t.Fatal("read did not reach SQL lock barrier")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if fault.clockCalls.Load() != 0 {
		t.Fatal("used pre-lock clock")
	}
	at := time.UnixMilli(receipt.ExpiresAtUnixMs)
	fault.now.Store(&at)
	if err = lock.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal("used stale pre-wait clock", err)
	}
	if fault.clockCalls.Load() != 1 {
		t.Fatal("missing final clock")
	}
	// An earlier valid snapshot is never reusable authority after replacement.
	if got, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, req); got == nil || err != nil {
		t.Fatal(got, err)
	}
	f.f.replace(t, 2, f.gen(2))
	if got, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, req); got != nil || !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("stale recipient authority", got, err)
	}
}

func TestBlobReadMetadataDoesNotClaimFileExistencePostgres(t *testing.T) {
	f, w, files, data := blobWriterFixture(t)
	readyAttachment(t, f, w, files, data)
	acceptAttachment(t, f, w.Attempt.Reservation)
	req := readAttachmentRequest(t, w.Attempt.Reservation)
	// Trusted primitive simulates external file loss; this is NOT accepted GC.
	proof, err := files.RemoveVerified(t.Context(), w.Attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if err = proof.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := f.blobs.ResolveRead(t.Context(), 2, w.StorageID, req); got == nil || err != nil {
		t.Fatal("metadata started pretending to check files", got, err)
	}
	f.quota(t, 64, 1, 1)
}
