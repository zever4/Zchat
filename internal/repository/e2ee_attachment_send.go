package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	"google.golang.org/protobuf/proto"
)

// Called ONLY at actual Ready/link gates, never around a whole transaction.
// Generic commit/auth/transport errors cannot become a bound retry observation.
func blobSendGateFailure(e *preparedE2EESend, err error) error {
	if e.blob == nil || e.epoch == nil || (err != domain.ErrE2EEBlobNotReadyForSend && err != domain.ErrE2EEBlobLeaseExpired) {
		return err
	}
	return &domain.E2EEBlobSendFailure{Cause: err, Binding: &pb.EpochSendBinding{
		ProtocolVersion: e.version(), ConversationId: e.ConversationId, Epoch: *e.epoch, MessageId: e.MessageId,
		Sender: proto.Clone(e.Sender).(*pb.DeviceRef), Recipient: proto.Clone(e.Recipient).(*pb.DeviceRef), EnvelopeDigest: append([]byte(nil), e.digest[:]...),
	}}
}

type readySendBlob struct {
	attempt  string
	revision uint64
}

// Caller holds canonical route/delivery locks. Never allocate a missing claim or
// change storage quota here. Reservation stays immutable, with no Signal digest.
func lockReadySendBlob(ctx context.Context, tx *sql.Tx, claim e2eeMessageClaim) (*readySendBlob, error) {
	if err := checkE2EEMessageClaim(ctx, tx, claim); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrE2EEBlobNotReadyForSend
		}
		return nil, err
	}
	var state string
	var expires time.Time
	var writer, storage sql.NullString
	result := &readySendBlob{}
	err := tx.QueryRowContext(ctx, `SELECT a.attempt_id::text,a.revision,a.state,a.expires_at,a.writer_id::text,a.storage_id::text
        FROM e2ee_blob_heads h JOIN e2ee_blob_attempts a ON (a.message_id,a.revision,a.attempt_id)=(h.message_id,h.revision,h.attempt_id)
        WHERE h.message_id=$1 FOR UPDATE OF a`, claim.message).Scan(&result.attempt, &result.revision, &state, &expires, &writer, &storage)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEBlobNotReadyForSend
	}
	if err != nil {
		return nil, err
	}
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	if !now.Before(expires) {
		return nil, domain.ErrE2EEBlobLeaseExpired
	}
	if state != "ready" || !writer.Valid || !storage.Valid {
		return nil, domain.ErrE2EEBlobNotReadyForSend
	}
	return result, nil
}

func linkSendBlob(ctx context.Context, tx *sql.Tx, message string, ready *readySendBlob) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO e2ee_message_blobs(message_id,revision,attempt_id) VALUES($1,$2,$3)`, message, ready.revision, ready.attempt); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE e2ee_blob_attempts SET state='linked' WHERE attempt_id=$1 AND state='ready' AND expires_at>clock_timestamp()`, ready.attempt)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return domain.ErrE2EEBlobLeaseExpired
	}
	return nil
}

// Receipt-first retry consults immutable metadata only, never current head/state
// or the file. ACK, expiry, seal or recipient replacement cannot erase this fact.
func checkSavedSendBlob(ctx context.Context, tx *sql.Tx, frame *preparedE2EESend) error {
	var id, attempt string
	var size, revision int64
	var hash []byte
	err := tx.QueryRowContext(ctx, `SELECT c.blob_id::text,c.blob_bytes,c.blob_sha256,b.attempt_id::text,b.revision
        FROM e2ee_message_blobs b JOIN e2ee_message_claims c USING(message_id) WHERE b.message_id=$1`, frame.MessageId).Scan(&id, &size, &hash, &attempt, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrE2EEMessageState
	}
	if err != nil {
		return err
	}
	if revision <= 0 || !recoveryUUID(attempt) || size <= 0 {
		return domain.ErrE2EEMessageState
	}
	if frame.blob.BlobId != id || frame.blob.CiphertextBytes != uint64(size) || !bytes.Equal(frame.blob.Sha256, hash) {
		return domain.ErrE2EEMessageConflict
	}
	return nil
}
