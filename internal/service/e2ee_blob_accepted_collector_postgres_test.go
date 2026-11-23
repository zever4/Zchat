//go:build linux

package service

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
)

type lostAcceptedAdmission struct {
	*repository.PgE2EEBlobRepository
	lost atomic.Bool
}

func (m *lostAcceptedAdmission) ClaimAcceptedCleanup(ctx context.Context, req *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	x, err := m.PgE2EEBlobRepository.ClaimAcceptedCleanup(ctx, req)
	if err == nil && m.lost.CompareAndSwap(false, true) {
		return nil, errBlobSynthetic
	}
	return x, err
}

func TestAcceptedBlobCollectorAutomaticAckAndLostAdmissionRetryPostgres(t *testing.T) {
	f, _, read := acceptedDownloadPostgres(t)
	before, _ := f.budget.Usage()
	m := &lostAcceptedAdmission{PgE2EEBlobRepository: f.repo}
	f.upload.repo = m
	acknowledgeAcceptedFile(t, f, read)
	events := startCollector(t, f.upload)
	awaitCollector(t, events, func(e collectorObservation) bool {
		return e.sweep.Lane == domain.E2EEBlobCleanupAccepted && e.sweep.Failed == 1
	})
	// The lost reply leaves a durable queue row; replay does not need a scan of
	// historical ACKs/receipts or a second goroutine/task for this file.
	awaitCollector(t, events, func(e collectorObservation) bool {
		return e.sweep.Lane == domain.E2EEBlobCleanupAccepted && e.sweep.Reclaimed == 1
	})
	assertReclaimed(t, f)
	after, _ := f.budget.Usage()
	if !m.lost.Load() || after.Bytes >= before.Bytes || f.budget.Active() {
		t.Fatal("automatic accepted retry did not settle owned physical bytes", before, after)
	}
	var pending, acks int
	if err := f.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_accepted_blob_queue),(SELECT count(*) FROM e2ee_file_acknowledgements)`).Scan(&pending, &acks); err != nil || pending != 0 || acks != 1 {
		t.Fatal("projection not drained or immutable ACK removed", pending, acks, err)
	}
}

func TestAcceptedBlobCollectorExpiresWithoutAckAndRetainsLateAckPostgres(t *testing.T) {
	f, _, read := acceptedDownloadPostgres(t)
	// Disposable fixture clock on ONE scoped connection, including the stable
	// discovery clock. No immutable receipt/date or production pool is changed.
	f.db.SetMaxOpenConns(1)
	f.db.SetMaxIdleConns(1)
	var schema string
	if err := f.db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `SET search_path=`+pq.QuoteIdentifier(schema)+`,pg_catalog`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `CREATE TABLE accepted_collector_clock AS SELECT expires_at+INTERVAL '1 millisecond' AS at FROM e2ee_send_receipts;
        CREATE FUNCTION clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE AS 'SELECT at FROM accepted_collector_clock';
        CREATE FUNCTION statement_timestamp() RETURNS timestamptz LANGUAGE sql STABLE AS 'SELECT at FROM accepted_collector_clock';
        ALTER TABLE e2ee_file_acknowledgements ALTER COLUMN acknowledged_at SET DEFAULT date_trunc('milliseconds',clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	events := startCollector(t, f.upload)
	awaitCollector(t, events, func(e collectorObservation) bool {
		return e.sweep.Lane == domain.E2EEBlobCleanupAccepted && e.sweep.Reclaimed == 1
	})
	assertReclaimed(t, f)
	var reason string
	var acks int
	if err := f.db.QueryRowContext(t.Context(), `SELECT deletion_reason,(SELECT count(*) FROM e2ee_file_acknowledgements) FROM e2ee_blob_attempts`).Scan(&reason, &acks); err != nil || reason != domain.E2EEBlobReclaimAcceptedExpired || acks != 0 {
		t.Fatal("expiry required ACK or changed first cause", reason, acks, err)
	}
	acknowledgeAcceptedFile(t, f, read)
	if err := f.db.QueryRowContext(t.Context(), `SELECT deletion_reason,(SELECT count(*) FROM e2ee_accepted_blob_queue) FROM e2ee_blob_attempts`).Scan(&reason, &acks); err != nil || reason != domain.E2EEBlobReclaimAcceptedExpired || acks != 0 {
		t.Fatal("late ACK resurrected queue or rewrote expiry", reason, acks, err)
	}
}
