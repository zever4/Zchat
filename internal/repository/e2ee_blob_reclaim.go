package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

// Explicit initialization under the filesystem's exclusive owner lock. Never
// rebind to a fresh/other directory just because expected files are missing.
func (r *PgE2EEBlobRepository) BindStorage(parent context.Context, id string) error {
	if err := boundary.ValidateBlobStorage(id); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO e2ee_blob_storage(storage_id)VALUES($1)ON CONFLICT(singleton)DO NOTHING`, id); err != nil {
		return err
	}
	if err = checkBlobStorage(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit()
}
func checkBlobStorage(ctx context.Context, tx *sql.Tx, id string) error {
	var found string
	err := tx.QueryRowContext(ctx, `SELECT storage_id::text FROM e2ee_blob_storage WHERE singleton`).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) || err == nil && found != id {
		return domain.ErrE2EEBlobStorage
	}
	return err
}

// ClaimCleanup is an INTERNAL system operation, NOT a user arbitrary-delete API.
// No active-owner/epoch gate: expired old scopes must remain reclaimable.
// Selected accepted attempts are never eligible here. Unselected predecessors
// remain reclaimable after another attempt of the same message was accepted.
func (r *PgE2EEBlobRepository) ClaimCleanup(parent context.Context, req *domain.E2EEBlobCleanupRequest) (*domain.E2EEBlobCleanup, error) {
	if err := boundary.ValidateBlobCleanup(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	c, err := lockCleanupClaim(ctx, tx, req, false)
	if err != nil {
		return nil, err
	}
	a, err := readCleanupAttempt(ctx, tx, req)
	if err != nil {
		return nil, err
	}
	if a.result.State != "deleting" && a.result.State != "reclaimed" {
		if a.result.State == "linked" {
			return nil, domain.ErrE2EEBlobCleanup
		}
		var eligible bool
		switch req.Reason {
		case domain.E2EEBlobCleanupExpired:
			if err = tx.QueryRowContext(ctx, `SELECT expires_at<=clock_timestamp()FROM e2ee_blob_attempts WHERE attempt_id=$1`, req.AttemptID).Scan(&eligible); err != nil {
				return nil, err
			}
		case domain.E2EEBlobCleanupFailedWriter:
			eligible = a.result.State == "writing" && a.writer.Valid && a.writer.String == req.ExpectedWriterID
		case domain.E2EEBlobCleanupInactiveScope:
			err = tx.QueryRowContext(ctx, `SELECT c.state<>'active' OR c.active_epoch<>$2 OR x.state<>'active' OR EXISTS(
                SELECT 1 FROM e2ee_devices WHERE generation_id IN ($3,$4) AND retired_at IS NOT NULL)
                FROM e2ee_conversations c JOIN e2ee_conversation_epochs x ON x.conversation_id=c.conversation_id AND x.epoch=$2 WHERE c.conversation_id=$1`, c.chat, *c.epoch, c.lowGen, c.highGen).Scan(&eligible)
			if err != nil {
				return nil, err
			}
		}
		if !eligible {
			return nil, domain.ErrE2EEBlobCleanup
		}
		if _, err = tx.ExecContext(ctx, `UPDATE e2ee_blob_attempts SET state='deleting',deletion_reason=$2 WHERE attempt_id=$1`, req.AttemptID, string(req.Reason)); err != nil {
			return nil, err
		}
		a.result.State = "deleting"
		a.result.Reason = string(req.Reason)
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &a.result, nil
}

// CompleteReclaim requires a live root-bound removal lease through the SQL
// COMMIT. Global/account bytes and the immutable reclaim fact change together;
// retained claim/attempt counts deliberately remain charged as evidence.
func (r *PgE2EEBlobRepository) CompleteReclaim(parent context.Context, req *domain.E2EEBlobCleanupRequest, proof *blobstore.Removed) (result *domain.E2EEBlobCleanup, err error) {
	if err = boundary.ValidateBlobCleanup(req); err != nil {
		return nil, err
	}
	return r.completeReclaim(parent, req, proof, false)
}

func (r *PgE2EEBlobRepository) completeReclaim(parent context.Context, req *domain.E2EEBlobCleanupRequest, proof *blobstore.Removed, accepted bool) (result *domain.E2EEBlobCleanup, err error) {
	err = proof.Guard(req.StorageID, req.AttemptID, func() error {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		tx, e := r.db.BeginTx(ctx, nil)
		if e != nil {
			return e
		}
		defer tx.Rollback()
		c, e := lockCleanupClaim(ctx, tx, req, accepted)
		if e != nil {
			return e
		}
		q, e := lockBlobQuota(ctx, tx)
		if e != nil {
			return e
		}
		var accountBytes int64
		if e = tx.QueryRowContext(ctx, `SELECT reserved_bytes FROM e2ee_blob_account_quota WHERE user_id=$1 FOR UPDATE`, c.sender).Scan(&accountBytes); e != nil {
			return e
		}
		a, e := readCleanupAttempt(ctx, tx, req)
		if e != nil {
			return e
		}
		if accepted && !acceptedCleanupReason(a.result.Reason) {
			return domain.ErrE2EEBlobCleanup
		}
		if a.result.State == "reclaimed" {
			if e = tx.Commit(); e != nil {
				return e
			}
			result = &a.result
			return nil
		}
		if a.result.State != "deleting" {
			return domain.ErrE2EEBlobCleanup
		}
		if q.global.bytes < a.bytes || accountBytes < a.bytes {
			return domain.ErrE2EEMessageState
		}
		if _, e = tx.ExecContext(ctx, `UPDATE e2ee_blob_global_quota SET reserved_bytes=reserved_bytes-$1 WHERE singleton`, a.bytes); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, `UPDATE e2ee_blob_account_quota SET reserved_bytes=reserved_bytes-$2 WHERE user_id=$1`, c.sender, a.bytes); e != nil {
			return e
		}
		var at time.Time
		if e = tx.QueryRowContext(ctx, `UPDATE e2ee_blob_attempts SET state='reclaimed',reclaimed_at=date_trunc('milliseconds',clock_timestamp()) WHERE attempt_id=$1 RETURNING reclaimed_at`, req.AttemptID).Scan(&at); e != nil {
			return e
		}
		a.result.State = "reclaimed"
		a.result.ReclaimedAt = &at
		if e = tx.Commit(); e != nil {
			return e
		}
		result = &a.result
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// Discover immutable targets without locks, then acquire canonical users ->
// devices -> conversation/epoch -> common claim. Finish then locks quotas before
// the attempt. No FS/body I/O is performed under any of these SQL locks.
func lockCleanupClaim(ctx context.Context, tx *sql.Tx, r *domain.E2EEBlobCleanupRequest, acceptedOnly bool) (*e2eeMessageClaim, error) {
	if err := checkBlobStorage(ctx, tx, r.StorageID); err != nil {
		return nil, err
	}
	c := &e2eeMessageClaim{message: r.MessageID}
	var epoch sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT conversation_id::text,sender_user_id,low_user_id,high_user_id,low_generation_id::text,high_generation_id::text,wire_version,crypto_epoch
        FROM e2ee_message_claims WHERE message_id=$1`, r.MessageID).Scan(&c.chat, &c.sender, &c.low, &c.high, &c.lowGen, &c.highGen, &c.version, &epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEBlobCAS
	}
	if err != nil {
		return nil, err
	}
	if c.version != 3 || !epoch.Valid {
		return nil, domain.ErrE2EEBlobCleanup
	}
	ep := uint64(epoch.Int64)
	c.epoch = &ep
	if err = lockAcquisitionRows(ctx, tx, `SELECT id FROM users WHERE id IN ($1,$2) ORDER BY id FOR SHARE`, c.low, c.high); err != nil {
		return nil, err
	}
	if err = lockAcquisitionRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id IN ($1,$2) ORDER BY user_id FOR SHARE`, c.lowGen, c.highGen); err != nil {
		return nil, err
	}
	var found string
	if err = tx.QueryRowContext(ctx, `SELECT conversation_id::text FROM e2ee_conversations WHERE conversation_id=$1 FOR SHARE`, c.chat).Scan(&found); err != nil {
		return nil, err
	}
	var lockedEpoch uint64
	if err = tx.QueryRowContext(ctx, `SELECT epoch FROM e2ee_conversation_epochs WHERE conversation_id=$1 AND epoch=$2 FOR SHARE`, c.chat, ep).Scan(&lockedEpoch); err != nil {
		return nil, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT message_id::text FROM e2ee_message_claims WHERE message_id=$1 FOR UPDATE`, r.MessageID).Scan(&found); err != nil {
		return nil, err
	}
	var accepted bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM e2ee_send_receipts WHERE message_id=$1)`, r.MessageID).Scan(&accepted); err != nil {
		return nil, err
	}
	var selected bool
	if accepted {
		var attempt string
		var revision uint64
		if err = tx.QueryRowContext(ctx, `SELECT b.attempt_id::text,b.revision FROM e2ee_message_blobs b JOIN e2ee_send_receipts s USING(message_id)
            WHERE b.message_id=$1 AND s.wire_version=3`, r.MessageID).Scan(&attempt, &revision); err != nil {
			return nil, err
		}
		selected = attempt == r.AttemptID && revision == r.Revision
	}
	if acceptedOnly != selected {
		return nil, domain.ErrE2EEBlobCleanup
	}
	return c, nil
}

type cleanupAttempt struct {
	result domain.E2EEBlobCleanup
	bytes  int64
	writer sql.NullString
}

func readCleanupAttempt(ctx context.Context, tx *sql.Tx, r *domain.E2EEBlobCleanupRequest) (*cleanupAttempt, error) {
	a := &cleanupAttempt{result: domain.E2EEBlobCleanup{StorageID: r.StorageID, MessageID: r.MessageID, AttemptID: r.AttemptID, Revision: r.Revision}}
	var storage sql.NullString
	var revision uint64
	var reclaimed sql.NullTime
	err := tx.QueryRowContext(ctx, `SELECT revision,state,charged_bytes,writer_id::text,storage_id::text,COALESCE(deletion_reason,''),reclaimed_at
        FROM e2ee_blob_attempts WHERE message_id=$1 AND attempt_id=$2 FOR UPDATE`, r.MessageID, r.AttemptID).
		Scan(&revision, &a.result.State, &a.bytes, &a.writer, &storage, &a.result.Reason, &reclaimed)
	if errors.Is(err, sql.ErrNoRows) || err == nil && revision != r.Revision {
		return nil, domain.ErrE2EEBlobCAS
	}
	if err != nil {
		return nil, err
	}
	if storage.Valid && storage.String != r.StorageID {
		return nil, domain.ErrE2EEBlobStorage
	}
	if r.ExpectedWriterID != "" && (!a.writer.Valid || a.writer.String != r.ExpectedWriterID) {
		return nil, domain.ErrE2EEBlobCAS
	}
	if reclaimed.Valid {
		at := reclaimed.Time
		a.result.ReclaimedAt = &at
	}
	return a, nil
}
