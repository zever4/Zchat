package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

const (
	blobFileAcknowledged = domain.E2EEBlobReclaimFileAcknowledged
	blobAcceptedExpired  = domain.E2EEBlobReclaimAcceptedExpired
)

func acceptedCleanupReason(reason string) bool {
	return reason == blobFileAcknowledged || reason == blobAcceptedExpired
}

func acceptedCleanupBinding(input *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanupRequest, error) {
	if input == nil {
		return nil, domain.ErrInvalidInput
	}
	// Reuse identifier/range validation only. This private adapter does not
	// authorize a staging deletion or let a caller choose an accepted reason.
	r := &domain.E2EEBlobCleanupRequest{StorageID: input.StorageID, MessageID: input.MessageID, AttemptID: input.AttemptID, Revision: input.Revision, Reason: domain.E2EEBlobCleanupExpired}
	if err := boundary.ValidateBlobCleanup(r); err != nil {
		return nil, err
	}
	return r, nil
}

// ClaimAcceptedCleanup commits deletion admission for exactly the immutable
// accepted attempt. No FS/body I/O or active-owner/epoch check: retained files
// from closed/sealed/replaced scopes still expire. FILEACK is independent of
// descriptor ACK. Unknown COMMIT returns no authority; replay recovers the fact.
// The service must cancel/join all readers AFTER admission and BEFORE unlink.
func (r *PgE2EEBlobRepository) ClaimAcceptedCleanup(parent context.Context, input *domain.E2EEAcceptedBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	req, err := acceptedCleanupBinding(input)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err = lockCleanupClaim(ctx, tx, req, true); err != nil {
		return nil, err
	}
	a, err := readCleanupAttempt(ctx, tx, req)
	if err != nil {
		return nil, err
	}
	switch a.result.State {
	case "linked":
		var expires time.Time
		var ack sql.NullTime
		err = tx.QueryRowContext(ctx, `SELECT s.expires_at,f.acknowledged_at
            FROM e2ee_message_blobs b JOIN e2ee_send_receipts s USING(message_id)
            LEFT JOIN e2ee_file_acknowledgements f USING(message_id)
            WHERE b.message_id=$1 AND b.attempt_id=$2 AND b.revision=$3 FOR SHARE OF s,b`, req.MessageID, req.AttemptID, req.Revision).Scan(&expires, &ack)
		if err != nil {
			return nil, err
		}
		// Clock AFTER all lock waits. Never use the one-hour staging deadline
		// or a caller's clock/HTTP/message disposition as deletion authority.
		var now time.Time
		if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
			return nil, err
		}
		switch {
		case ack.Valid:
			a.result.Reason = blobFileAcknowledged
		case !now.Before(expires):
			a.result.Reason = blobAcceptedExpired
		default:
			return nil, domain.ErrE2EEBlobCleanup
		}
		if _, err = tx.ExecContext(ctx, `UPDATE e2ee_blob_attempts SET state='deleting',deletion_reason=$2 WHERE attempt_id=$1`, req.AttemptID, a.result.Reason); err != nil {
			return nil, err
		}
		a.result.State = "deleting"
	case "deleting", "reclaimed":
		if !acceptedCleanupReason(a.result.Reason) {
			return nil, domain.ErrE2EEBlobCleanup
		}
	default:
		return nil, domain.ErrE2EEBlobCleanup
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &a.result, nil
}

// CompleteAcceptedReclaim requires the same live root/attempt-bound unlink+sync
// proof as staging cleanup. Reclaim fact and logical byte release commit once;
// receipt/link/ACK and retained claim/attempt counts remain immutable/charged.
func (r *PgE2EEBlobRepository) CompleteAcceptedReclaim(parent context.Context, input *domain.E2EEAcceptedBlobCleanupRequest, proof *blobstore.Removed) (*domain.E2EEBlobCleanup, error) {
	req, err := acceptedCleanupBinding(input)
	if err != nil {
		return nil, err
	}
	return r.completeReclaim(parent, req, proof, true)
}
