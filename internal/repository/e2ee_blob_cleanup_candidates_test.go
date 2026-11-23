//go:build linux

package repository

import (
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
)

func TestBlobCleanupCandidateLanesCursorsAndIndexPostgres(t *testing.T) {
	f, first, files, data := blobWriterFixture(t)
	if _, err := f.blobs.BeginWrite(t.Context(), 1, first); err != nil {
		t.Fatal(err)
	}
	ids := map[string]string{"writing": first.Attempt.AttemptID}
	writers := map[string]string{"writing": first.WriterID}
	for _, kind := range []string{"reserved", "expired reserved", "expired ready", "expired writing", "deleting", "linked"} {
		r := f.request(1)
		r.Blob.Sha256 = first.Attempt.Reservation.Blob.Sha256
		g := f.reserve(t, 1, r)
		ids[kind] = g.AttemptID
		if kind != "reserved" && kind != "expired reserved" {
			w := &domain.E2EEBlobWriteRequest{Attempt: &domain.E2EEBlobAttemptRequest{Reservation: r, AttemptID: g.AttemptID, Revision: g.Revision}, WriterID: uuid.NewString(), StorageID: files.StorageID()}
			writers[kind] = w.WriterID
			if _, err := f.blobs.BeginWrite(t.Context(), 1, w); err != nil {
				t.Fatal(err)
			}
			if kind == "expired ready" || kind == "linked" {
				if _, err := f.blobs.CompleteWrite(t.Context(), 1, w, writeBlobProof(t, files, w, data)); err != nil {
					t.Fatal(err)
				}
				if kind == "linked" {
					acceptAttachment(t, f, r)
				}
			}
			if kind == "deleting" {
				if _, err := f.blobs.ClaimCleanup(t.Context(), cleanupRequest(w, domain.E2EEBlobCleanupFailedWriter)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	// Test-only time travel, with equal deadlines to exercise the UUID tie-breaker.
	deadline := time.Now().UTC().Add(-time.Hour).Truncate(time.Millisecond)
	tx, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"expired reserved", "expired ready", "expired writing"} {
		if _, err = tx.ExecContext(t.Context(), `UPDATE e2ee_blob_attempts SET expires_at=$2::timestamptz,reserved_at=$2::timestamptz-INTERVAL '1 hour' WHERE attempt_id=$1`, ids[kind], deadline); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for _, lane := range []domain.E2EEBlobCleanupLane{domain.E2EEBlobCleanupUnfinished, domain.E2EEBlobCleanupExpiredStaging} {
		want := []string{ids["writing"], ids["expired writing"], ids["deleting"]}
		if lane == domain.E2EEBlobCleanupExpiredStaging {
			want = []string{ids["expired reserved"], ids["expired ready"]}
		}
		sort.Strings(want)
		var after *domain.E2EEBlobCleanupPosition
		for _, id := range want {
			page, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), lane, after, 1)
			if err != nil || len(page) != 1 || page[0].Request.AttemptID != id {
				t.Fatal(lane, page, err)
			}
			item := page[0]
			if item.Request.StorageID != files.StorageID() || item.Request.Revision != 1 {
				t.Fatal(item)
			}
			if id == ids["writing"] {
				if item.Request.Reason != domain.E2EEBlobCleanupFailedWriter || item.Request.ExpectedWriterID != writers["writing"] {
					t.Fatal(item)
				}
			} else if item.Request.Reason != domain.E2EEBlobCleanupExpired {
				t.Fatal(item)
			}
			if lane == domain.E2EEBlobCleanupExpiredStaging && !item.Position.ExpiresAt.Equal(deadline) {
				t.Fatal(item)
			}
			after = &item.Position
		}
		if page, err := f.blobs.CleanupCandidates(t.Context(), files.StorageID(), lane, after, 1); err != nil || len(page) != 0 {
			t.Fatal(page, err)
		}
	}
	f.quota(t, 448, 7, 7) // Discovery never changes claims, quota or file state.
	if _, err = f.blobs.CleanupCandidates(t.Context(), uuid.NewString(), 0, nil, 1); !errors.Is(err, domain.ErrE2EEBlobStorage) {
		t.Fatal("foreign root accepted", err)
	}
	for _, bad := range []struct {
		lane  domain.E2EEBlobCleanupLane
		after *domain.E2EEBlobCleanupPosition
		limit int
	}{{domain.E2EEBlobCleanupLaneCount, nil, 1}, {0, nil, 0}, {0, nil, 129}, {0, &domain.E2EEBlobCleanupPosition{AttemptID: "invalid"}, 1}, {1, &domain.E2EEBlobCleanupPosition{AttemptID: uuid.NewString()}, 1}, {0, &domain.E2EEBlobCleanupPosition{AttemptID: uuid.NewString(), ExpiresAt: deadline}, 1}} {
		if _, err = f.blobs.CleanupCandidates(t.Context(), files.StorageID(), bad.lane, bad.after, bad.limit); err == nil {
			t.Fatal("invalid scan accepted", bad)
		}
	}
	// Prove the cursor predicates have usable indexes; this tiny fixture does
	// NOT measure the planner's natural choice or throughput under real load.
	tx, err = f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(t.Context(), `SET LOCAL enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		query, index string
		args         []any
	}{{blobUnfinishedCandidates, "e2ee_blob_attempt_unfinished", []any{"00000000-0000-0000-0000-000000000000", 1}}, {blobExpiredCandidates, "e2ee_blob_attempt_expiry", []any{deadline.Add(-time.Hour), "00000000-0000-0000-0000-000000000000", 1}}} {
		var plan string
		if err = tx.QueryRowContext(t.Context(), "EXPLAIN (FORMAT JSON) "+test.query, test.args...).Scan(&plan); err != nil || !strings.Contains(plan, test.index) {
			t.Fatal(test.index, plan, err)
		}
	}
}

func TestBlobCleanupIndexMigrationRoundTripPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 31); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 31); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname='e2ee_blob_attempt_unfinished'`).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
}
