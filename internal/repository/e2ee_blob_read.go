package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// ResolveRead chooses ONLY the immutable accepted attempt. Current upload heads,
// operation/lease TTL, inbox presence, descriptor ACK, sender retirement and the
// present conversation/epoch state do not choose or authorize that object.
// uid is supplied by the authenticated coordinator; storageID by its owned root.
// No SQL lock survives this short transaction. This snapshot does not pin bytes.
func (r *PgE2EEBlobRepository) ResolveRead(parent context.Context, uid int64, storageID string, input *domain.E2EEBlobReadRequest) (*domain.E2EEBlobReadTarget, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateBlobReadRequest(input); err != nil {
		return nil, err
	}
	if err := boundary.ValidateBlobStorage(storageID); err != nil {
		return nil, err
	}
	// Keep the returned observation independent of later input mutations.
	req := &domain.E2EEBlobReadRequest{Context: proto.Clone(input.Context).(*pb.ClientContext), Binding: proto.Clone(input.Binding).(*pb.EpochSendBinding), Blob: proto.Clone(input.Blob).(*v2.BlobRef)}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	target, outcome := resolveBlobRead(ctx, tx, uid, storageID, req)
	if outcome != nil && !errors.Is(outcome, domain.ErrE2EEBlobReadUnavailable) {
		return nil, outcome
	}
	// A failed/canceled COMMIT is neither a target nor authoritative absence.
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return target, outcome
}

func resolveBlobRead(ctx context.Context, tx *sql.Tx, uid int64, storageID string, req *domain.E2EEBlobReadRequest) (*domain.E2EEBlobReadTarget, error) {
	gen := req.Context.DeviceGenerationId
	if err := lockDeliveryRows(ctx, tx, `SELECT id FROM users WHERE id=$1 FOR SHARE`, uid, 1); err != nil {
		return nil, err
	}
	if err := lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE user_id=$1 AND generation_id=$2 FOR SHARE`, []any{uid, gen}, 1); err != nil {
		return nil, err
	}
	account, err := currentDeliveryOwner(ctx, tx, uid, gen)
	if err != nil {
		return nil, err
	}
	if account != req.Binding.Recipient.UserId {
		return nil, domain.ErrE2EEBlobReadUnavailable
	}
	if err = checkBlobStorage(ctx, tx, storageID); err != nil {
		return nil, err
	}
	// Shared canonical claim lock serializes with Send and future accepted GC.
	// Wrong recipients do not lock another account's claim.
	var id string
	err = tx.QueryRowContext(ctx, `SELECT message_id::text FROM e2ee_message_claims WHERE message_id=$1 AND recipient_user_id=$2 AND recipient_generation_id=$3 AND wire_version=3 FOR SHARE`, req.Binding.MessageId, uid, gen).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEBlobReadUnavailable
	}
	if err != nil {
		return nil, err
	}
	var chat, sender, senderGen, recipient, recipientGen, blob, state string
	var epoch sql.NullInt64
	var size int64
	var hash, digest []byte
	var writer, storage sql.NullString
	var consistent, acknowledged bool
	target := &domain.E2EEBlobReadTarget{Request: req}
	err = tx.QueryRowContext(ctx, `SELECT c.conversation_id::text,c.crypto_epoch,su.user_uuid,c.sender_generation_id::text,ru.user_uuid,c.recipient_generation_id::text,
        c.blob_id::text,c.blob_bytes,c.blob_sha256,r.envelope_digest,r.stored_at,
        b.attempt_id::text,b.revision,a.storage_id::text,a.writer_id::text,a.state,
        r.wire_version=3 AND c.envelope_digest IS NULL AND
        ROW(r.conversation_id,r.crypto_epoch,r.sender_user_id,r.sender_generation_id,r.recipient_user_id,r.recipient_generation_id)
        IS NOT DISTINCT FROM ROW(c.conversation_id,c.crypto_epoch,c.sender_user_id,c.sender_generation_id,c.recipient_user_id,c.recipient_generation_id),
        EXISTS(SELECT 1 FROM e2ee_file_acknowledgements ack WHERE ack.message_id=c.message_id)
        FROM e2ee_message_claims c JOIN e2ee_send_receipts r USING(message_id) JOIN e2ee_message_blobs b USING(message_id)
        JOIN e2ee_blob_attempts a ON (a.message_id,a.revision,a.attempt_id)=(b.message_id,b.revision,b.attempt_id)
        JOIN users su ON su.id=c.sender_user_id JOIN users ru ON ru.id=c.recipient_user_id
        WHERE c.message_id=$1 FOR SHARE OF r,b,a`, id).
		Scan(&chat, &epoch, &sender, &senderGen, &recipient, &recipientGen, &blob, &size, &hash, &digest, &target.StoredAt,
			&target.AttemptID, &target.Revision, &storage, &writer, &state, &consistent, &acknowledged)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEBlobReadUnavailable
	}
	if err != nil {
		return nil, err
	}
	if !consistent || !epoch.Valid || epoch.Int64 < 0 || size <= 0 || len(hash) != 32 || len(digest) != 32 ||
		target.Revision == 0 || !recoveryUUID(target.AttemptID) || target.StoredAt.UnixMilli() <= 0 {
		return nil, domain.ErrE2EEMessageState
	}
	b := req.Binding
	if b.ConversationId != chat || b.Epoch != uint64(epoch.Int64) || b.Sender.UserId != sender || b.Sender.GenerationId != senderGen ||
		b.Recipient.UserId != recipient || b.Recipient.GenerationId != recipientGen || !bytes.Equal(b.EnvelopeDigest, digest) ||
		req.Blob.BlobId != blob || req.Blob.CiphertextBytes != uint64(size) || !bytes.Equal(req.Blob.Sha256, hash) {
		return nil, domain.ErrE2EEBlobReadUnavailable
	}
	// The claim lock serializes this observation with FILEACK/accepted GC.
	if state != "linked" || acknowledged {
		return nil, domain.ErrE2EEBlobReadUnavailable
	}
	if !storage.Valid || storage.String != storageID || !writer.Valid || !recoveryUUID(writer.String) {
		return nil, domain.ErrE2EEBlobStorage
	}
	target.StorageID = storage.String
	target.ExpiresAt = target.StoredAt.Add(domain.E2EEBlobAcceptedRetention)
	// Take the DB clock AFTER every lock wait. Staging expires_at is irrelevant.
	if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&target.ObservedAt); err != nil {
		return nil, err
	}
	if !target.ObservedAt.Before(target.ExpiresAt) {
		return nil, domain.ErrE2EEBlobReadUnavailable
	}
	return target, nil
}
