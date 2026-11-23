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

// BeginWrite grants metadata authority to ONE server writer. A same-writer replay
// returns its old lease only; it never means that reopening a file is safe.
func (r *PgE2EEBlobRepository) BeginWrite(ctx context.Context, uid int64, req *domain.E2EEBlobWriteRequest) (*domain.E2EEBlobWriteLease, error) {
	return r.blobWriteTransition(ctx, uid, req, "begin")
}

// CheckWrite periodically revalidates ownership/current epoch/head/writer/expiry.
// It opens no file and grants no new writer or Ready transition.
func (r *PgE2EEBlobRepository) CheckWrite(ctx context.Context, uid int64, req *domain.E2EEBlobWriteRequest) (*domain.E2EEBlobWriteLease, error) {
	return r.blobWriteTransition(ctx, uid, req, "check")
}

// CompleteWrite requires a genuine filesystem result with the EXACT public
// attempt/size/hash, plus a still-current DB writer. The caller holds short login
// and device authorization barriers. Neither method holds SQL locks during I/O.
func (r *PgE2EEBlobRepository) CompleteWrite(ctx context.Context, uid int64, req *domain.E2EEBlobWriteRequest, w *blobstore.Written) (*domain.E2EEBlobWriteLease, error) {
	if err := boundary.ValidateBlobWrite(req); err != nil {
		return nil, err
	}
	a := w.Attempt()
	ref := req.Attempt.Reservation.Blob
	if w.StorageID() != req.StorageID || a.ID != req.Attempt.AttemptID || a.Bytes != int64(ref.CiphertextBytes) || a.SHA256 != [32]byte(ref.Sha256) {
		return nil, domain.ErrInvalidInput
	}
	return r.blobWriteTransition(ctx, uid, req, "complete")
}

func (r *PgE2EEBlobRepository) blobWriteTransition(parent context.Context, uid int64, req *domain.E2EEBlobWriteRequest, phase string) (*domain.E2EEBlobWriteLease, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateBlobWrite(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = checkBlobStorage(ctx, tx, req.StorageID); err != nil {
		return nil, err
	}
	s := req.Attempt
	rsv := s.Reservation
	claim, err := authorizeBlobReservation(ctx, tx, uid, rsv)
	if err != nil {
		return nil, err
	}
	// SELECT the existing common claim rather than calling the INSERT first-use
	// helper. A forged upload must not create a namespace/quota reservation.
	err = checkE2EEMessageClaim(ctx, tx, *claim)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEBlobCAS
	}
	if err != nil {
		return nil, err
	}
	g, expectedRev, expectedAttempt, err := readBlobHead(ctx, tx, rsv.MessageID)
	if err != nil {
		return nil, err
	}
	if g == nil || g.AttemptID != s.AttemptID || g.Revision != s.Revision || g.OperationID != rsv.OperationID {
		return nil, domain.ErrE2EEBlobCAS
	}
	if expectedRev != rsv.ExpectedRevision || expectedAttempt != rsv.ExpectedAttemptID {
		return nil, domain.ErrE2EEMessageConflict
	}
	var writer sql.NullString
	var storage sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT state,writer_id::text,storage_id::text FROM e2ee_blob_attempts WHERE attempt_id=$1 FOR UPDATE`, g.AttemptID).Scan(&g.State, &writer, &storage)
	if err != nil {
		return nil, err
	}
	if err = observeBlobLease(ctx, tx, g); err != nil {
		return nil, err
	}
	if g.Expired {
		return nil, domain.ErrE2EEBlobLeaseExpired
	}
	if writer.Valid && writer.String != req.WriterID {
		return nil, domain.ErrE2EEBlobCAS
	}
	if storage.Valid && storage.String != req.StorageID {
		return nil, domain.ErrE2EEBlobStorage
	}
	target := ""
	switch phase {
	case "begin":
		if g.State == "reserved" && !writer.Valid {
			target = "writing"
		} else if g.State != "writing" || !writer.Valid {
			return nil, domain.ErrE2EEBlobWriteState
		}
	case "check":
		if g.State != "writing" || !writer.Valid {
			return nil, domain.ErrE2EEBlobWriteState
		}
	case "complete":
		if !writer.Valid {
			return nil, domain.ErrE2EEBlobWriteState
		}
		if g.State == "writing" {
			target = "ready"
		} else if g.State != "ready" {
			return nil, domain.ErrE2EEBlobWriteState
		}
	default:
		return nil, domain.ErrInvalidInput
	}
	if target != "" {
		res, e := tx.ExecContext(ctx, `UPDATE e2ee_blob_attempts SET state=$2,writer_id=$3,storage_id=$5 WHERE attempt_id=$1 AND state=$4 AND expires_at>clock_timestamp()`, g.AttemptID, target, req.WriterID, g.State, req.StorageID)
		if e != nil {
			return nil, blobUniqueConflict(e)
		}
		n, e := res.RowsAffected()
		if e != nil {
			return nil, e
		}
		if n != 1 {
			return nil, domain.ErrE2EEBlobLeaseExpired
		}
		g.State = target
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &domain.E2EEBlobWriteLease{Grant: *g, WriterID: req.WriterID, StorageID: req.StorageID}, nil
}
