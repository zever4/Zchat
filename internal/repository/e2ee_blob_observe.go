package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

// Observe reads an EXACT operation, including a superseded attempt. It never
// INSERTs a claim, renews a lease, changes head/quota or infers Send acceptance.
// Missing observations are returned only AFTER the authorized read COMMIT.
func (r *PgE2EEBlobRepository) Observe(parent context.Context, uid int64, req *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateBlobReservation(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	claim, err := authorizeBlobReservation(ctx, tx, uid, req)
	if err != nil {
		return nil, err
	}
	err = checkE2EEMessageClaim(ctx, tx, *claim)
	if errors.Is(err, sql.ErrNoRows) {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, domain.ErrE2EEBlobOperationNotFound
	}
	if err != nil {
		return nil, err
	}
	g := &domain.E2EEBlobReservation{MessageID: req.MessageID}
	var expected uint64
	var predecessor sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT attempt_id::text,operation_id::text,revision,reserved_at,expires_at,state,expected_revision,expected_attempt_id::text
        FROM e2ee_blob_attempts WHERE message_id=$1 AND operation_id=$2 FOR SHARE`, req.MessageID, req.OperationID).
		Scan(&g.AttemptID, &g.OperationID, &g.Revision, &g.ReservedAt, &g.ExpiresAt, &g.State, &expected, &predecessor)
	if errors.Is(err, sql.ErrNoRows) {
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return nil, domain.ErrE2EEBlobOperationNotFound
	}
	if err != nil {
		return nil, err
	}
	if expected != req.ExpectedRevision || predecessor.String != req.ExpectedAttemptID {
		return nil, domain.ErrE2EEMessageConflict
	}
	if err = observeBlobLease(ctx, tx, g); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return g, nil
}
