package repository

import (
	"context"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

const blobUnfinishedCandidates = `SELECT attempt_id::text,message_id::text,revision,state,
    COALESCE(writer_id::text,''),COALESCE(storage_id::text,''),expires_at<=statement_timestamp()
    FROM e2ee_blob_attempts WHERE state IN ('writing','deleting')
    AND (deletion_reason IS NULL OR deletion_reason NOT IN ('file_acknowledged','accepted_expired')) AND attempt_id>$1::uuid
    ORDER BY attempt_id LIMIT $2`
const blobExpiredCandidates = `SELECT attempt_id::text,message_id::text,revision,expires_at
    FROM e2ee_blob_attempts WHERE state IN ('reserved','ready') AND expires_at<=statement_timestamp()
    AND (expires_at,attempt_id)>($1::timestamptz,$2::uuid) ORDER BY expires_at,attempt_id LIMIT $3`
const blobAcceptedCandidates = `SELECT attempt_id::text,message_id::text,revision,due_at
    FROM e2ee_accepted_blob_queue WHERE storage_id=$1::uuid AND due_at<=statement_timestamp()
    AND (due_at,attempt_id)>($2::timestamptz,$3::uuid) ORDER BY due_at,attempt_id LIMIT $4`

// CleanupCandidates reads a bounded indexed page without locking producers or
// authorizing deletion. The owned coordinator must revalidate each via Cleanup.
func (r *PgE2EEBlobRepository) CleanupCandidates(parent context.Context, storage string, lane domain.E2EEBlobCleanupLane, after *domain.E2EEBlobCleanupPosition, limit int) ([]domain.E2EEBlobCleanupCandidate, error) {
	if err := boundary.ValidateBlobStorage(storage); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 128 || lane >= domain.E2EEBlobCleanupLaneCount {
		return nil, domain.ErrInvalidInput
	}
	if after != nil {
		if err := boundary.ValidateBlobStorage(after.AttemptID); err != nil {
			return nil, err
		}
		if (lane == domain.E2EEBlobCleanupUnfinished) != after.ExpiresAt.IsZero() {
			return nil, domain.ErrInvalidInput
		}
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = checkBlobStorage(ctx, tx, storage); err != nil {
		return nil, err
	}
	id := "00000000-0000-0000-0000-000000000000"
	at := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	if after != nil {
		id = after.AttemptID
		at = after.ExpiresAt
	}
	query := blobUnfinishedCandidates
	args := []any{id, limit}
	if lane == domain.E2EEBlobCleanupExpiredStaging {
		query = blobExpiredCandidates
		args = []any{at, id, limit}
	} else if lane == domain.E2EEBlobCleanupAccepted {
		query = blobAcceptedCandidates
		args = []any{storage, at, id, limit}
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	result := make([]domain.E2EEBlobCleanupCandidate, 0, limit)
	for rows.Next() {
		e := domain.E2EEBlobCleanupCandidate{Request: domain.E2EEBlobCleanupRequest{StorageID: storage, Reason: domain.E2EEBlobCleanupExpired}}
		if lane == domain.E2EEBlobCleanupAccepted {
			a := &domain.E2EEAcceptedBlobCleanupRequest{StorageID: storage}
			err = rows.Scan(&a.AttemptID, &a.MessageID, &a.Revision, &e.Position.ExpiresAt)
			e.Request.MessageID, e.Request.AttemptID, e.Request.Revision = a.MessageID, a.AttemptID, a.Revision
			e.AcceptedRequest = a
		} else if lane == domain.E2EEBlobCleanupExpiredStaging {
			err = rows.Scan(&e.Request.AttemptID, &e.Request.MessageID, &e.Request.Revision, &e.Position.ExpiresAt)
		} else {
			var state, writer, bound string
			var expired bool
			err = rows.Scan(&e.Request.AttemptID, &e.Request.MessageID, &e.Request.Revision, &state, &writer, &bound, &expired)
			if err == nil && bound != "" && bound != storage {
				err = domain.ErrE2EEBlobStorage
			}
			if err == nil && state == "writing" && !expired {
				e.Request.Reason = domain.E2EEBlobCleanupFailedWriter
				e.Request.ExpectedWriterID = writer
			}
		}
		if err != nil {
			rows.Close()
			return nil, err
		}
		e.Position.AttemptID = e.Request.AttemptID
		if err = boundary.ValidateBlobCleanup(&e.Request); err != nil {
			rows.Close()
			return nil, err
		}
		if e.AcceptedRequest != nil {
			e.Request = domain.E2EEBlobCleanupRequest{}
		}
		result = append(result, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}
