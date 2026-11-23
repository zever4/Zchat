package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sort"
	"time"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// Metadata-only repository. The native RPC supplies authenticated uid while
// holding live session/device guards through this transaction.
// Native durable cache proof must precede dispatch. SQL cannot attest client disk.
// No file I/O/GC, Send/descriptor-ACK mutation, current-epoch or download TTL gate.
func (r *PgE2EEBlobRepository) AcknowledgeFiles(parent context.Context, uid int64, input *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateFileAcknowledgements(input); err != nil {
		return nil, err
	}
	req := proto.Clone(input).(*v2.AcknowledgeFilesRequest)
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	gen := req.Context.DeviceGenerationId
	if err = lockDeliveryRows(ctx, tx, `SELECT id FROM users WHERE id=$1 FOR SHARE`, uid, 1); err != nil {
		return nil, err
	}
	if err = lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE user_id=$1 AND generation_id=$2 FOR SHARE`, []any{uid, gen}, 1); err != nil {
		return nil, err
	}
	account, err := currentDeliveryOwner(ctx, tx, uid, gen)
	if err != nil {
		return nil, err
	}
	if account != req.Files[0].Binding.Recipient.UserId {
		return nil, domain.ErrE2EEFileAckUnavailable
	}
	order := make([]int, len(req.Files))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(i, j int) bool {
		return req.Files[order[i]].Binding.MessageId < req.Files[order[j]].Binding.MessageId
	})
	response := &v2.FileAcknowledgements{Files: make([]*v2.FileAcknowledgementReceipt, len(req.Files))}
	for _, i := range order {
		a := req.Files[i]
		if err = lockFileAckIdentity(ctx, tx, uid, gen, a); err != nil {
			return nil, err
		}
		var at time.Time
		err = tx.QueryRowContext(ctx, `INSERT INTO e2ee_file_acknowledgements(message_id) VALUES($1)
            ON CONFLICT(message_id) DO NOTHING RETURNING acknowledged_at`, a.Binding.MessageId).Scan(&at)
		if errors.Is(err, sql.ErrNoRows) {
			err = tx.QueryRowContext(ctx, `SELECT acknowledged_at FROM e2ee_file_acknowledgements WHERE message_id=$1`, a.Binding.MessageId).Scan(&at)
		}
		if err != nil {
			return nil, err
		}
		response.Files[i] = &v2.FileAcknowledgementReceipt{Acknowledgement: a, AcknowledgedAtUnixMs: at.UnixMilli()}
	}
	if err = boundary.ValidateFileAcknowledgementReceipts(req, response); err != nil {
		return nil, domain.ErrE2EEMessageState
	}
	// Nothing leaves this method on failed/cancelled/unknown COMMIT. Exact
	// replay recovers the original first ACK timestamp without a new fact.
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return response, nil
}

func lockFileAckIdentity(ctx context.Context, tx *sql.Tx, uid int64, gen string, a *v2.FileAcknowledgement) error {
	b := a.Binding
	var id string
	err := tx.QueryRowContext(ctx, `SELECT message_id::text FROM e2ee_message_claims
        WHERE message_id=$1 AND recipient_user_id=$2 AND recipient_generation_id=$3 AND wire_version=3 FOR UPDATE`, b.MessageId, uid, gen).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrE2EEFileAckUnavailable
	}
	if err != nil {
		return err
	}
	var chat, sender, senderGen, recipient, recipientGen, blob string
	var epoch sql.NullInt64
	var size int64
	var hash, digest []byte
	var consistent bool
	err = tx.QueryRowContext(ctx, `SELECT c.conversation_id::text,c.crypto_epoch,su.user_uuid,c.sender_generation_id::text,ru.user_uuid,c.recipient_generation_id::text,
        c.blob_id::text,c.blob_bytes,c.blob_sha256,r.envelope_digest,
        r.wire_version=3 AND c.envelope_digest IS NULL AND
        ROW(r.conversation_id,r.crypto_epoch,r.sender_user_id,r.sender_generation_id,r.recipient_user_id,r.recipient_generation_id)
        IS NOT DISTINCT FROM ROW(c.conversation_id,c.crypto_epoch,c.sender_user_id,c.sender_generation_id,c.recipient_user_id,c.recipient_generation_id)
        FROM e2ee_message_claims c JOIN e2ee_send_receipts r USING(message_id) JOIN e2ee_message_blobs l USING(message_id)
        JOIN users su ON su.id=c.sender_user_id JOIN users ru ON ru.id=c.recipient_user_id
        WHERE c.message_id=$1 FOR SHARE OF r,l`, id).Scan(&chat, &epoch, &sender, &senderGen, &recipient, &recipientGen, &blob, &size, &hash, &digest, &consistent)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrE2EEFileAckUnavailable
	}
	if err != nil {
		return err
	}
	if !consistent || !epoch.Valid || epoch.Int64 < 0 || size <= 0 || len(hash) != 32 || len(digest) != 32 {
		return domain.ErrE2EEMessageState
	}
	if b.ConversationId != chat || b.Epoch != uint64(epoch.Int64) || b.Sender.UserId != sender || b.Sender.GenerationId != senderGen ||
		b.Recipient.UserId != recipient || b.Recipient.GenerationId != recipientGen || !bytes.Equal(b.EnvelopeDigest, digest) ||
		a.Blob.BlobId != blob || a.Blob.CiphertextBytes != uint64(size) || !bytes.Equal(a.Blob.Sha256, hash) {
		return domain.ErrE2EEFileAckUnavailable
	}
	// Deliberately do not inspect inbox/descriptor ACK, epoch head, sender
	// retirement, filesystem state or expires_at. Late ACK retains only a fact.
	return nil
}
