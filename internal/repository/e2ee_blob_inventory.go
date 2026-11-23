package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

// Inventory is a bounded startup cursor, not authorization or a mutation proof.
// Root ownership and a quiescent coordinator are required across the whole scan.
func (r *PgE2EEBlobRepository) Inventory(parent context.Context, storageID, after string, limit int) ([]domain.E2EEBlobInventoryEntry, error) {
	if err := boundary.ValidateBlobStorage(storageID); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 128 {
		return nil, domain.ErrInvalidInput
	}
	if after != "" {
		if err := boundary.ValidateBlobStorage(after); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = checkBlobStorage(ctx, tx, storageID); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT a.attempt_id::text,a.message_id::text,a.revision,a.state,COALESCE(a.writer_id::text,''),COALESCE(a.storage_id::text,''),
        a.charged_bytes,c.blob_sha256,a.expires_at<=clock_timestamp(),EXISTS(SELECT 1 FROM e2ee_message_blobs b
            WHERE (b.message_id,b.revision,b.attempt_id)=(a.message_id,a.revision,a.attempt_id)),c.sender_user_id,
        COALESCE(a.deletion_reason,''),a.reclaimed_at
        FROM e2ee_blob_attempts a JOIN e2ee_message_claims c ON c.message_id=a.message_id
        WHERE ($1='' OR a.attempt_id>NULLIF($1,'')::uuid) ORDER BY a.attempt_id LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	result := make([]domain.E2EEBlobInventoryEntry, 0, limit)
	for rows.Next() {
		e := domain.E2EEBlobInventoryEntry{Cleanup: domain.E2EEBlobCleanupRequest{StorageID: storageID}}
		var hash []byte
		var reclaimed sql.NullTime
		if err = rows.Scan(&e.Cleanup.AttemptID, &e.Cleanup.MessageID, &e.Cleanup.Revision, &e.State, &e.WriterID, &e.BoundStorageID, &e.Bytes, &hash, &e.Expired, &e.Accepted, &e.SenderUserID, &e.DeletionReason, &reclaimed); err != nil {
			rows.Close()
			return nil, err
		}
		if len(hash) != 32 {
			rows.Close()
			return nil, domain.ErrE2EEMessageState
		}
		copy(e.SHA256[:], hash)
		if reclaimed.Valid {
			at := reclaimed.Time
			e.ReclaimedAt = &at
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
