package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// Reads are non-destructive, byte/count bounded and have no client skip cursor.
// Previously accepted messages from a retired SENDER remain eligible for the
// still-current recipient; closure prevents new sends, not reading accepted data.
func (r *PgE2EEDeliveryRepository) Pending(parent context.Context, userID int64, req *pb.GetPendingMessagesRequest) (*pb.PendingMessages, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidatePendingMessages(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := lockDeliveryRows(ctx, tx, `SELECT id FROM users WHERE id=$1 FOR SHARE`, userID, 1); err != nil {
		return nil, err
	}
	if err := lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE user_id=$1 AND generation_id=$2 FOR SHARE`, []any{userID, req.Context.DeviceGenerationId}, 1); err != nil {
		return nil, err
	}
	account, err := currentDeliveryOwner(ctx, tx, userID, req.Context.DeviceGenerationId)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT i.sequence,i.envelope,r.message_id::text,r.conversation_id::text,r.sender_generation_id::text,su.user_uuid,
        r.envelope_digest,r.expires_at FROM e2ee_message_inbox i JOIN e2ee_send_receipts r USING(message_id)
        JOIN e2ee_devices sd ON sd.generation_id=r.sender_generation_id JOIN users su ON su.id=sd.user_id
        WHERE i.recipient_generation_id=$1 AND r.recipient_generation_id=$1 AND r.recipient_user_id=$2 AND r.wire_version=1 AND i.expires_at>statement_timestamp()
        AND NOT EXISTS (SELECT 1 FROM e2ee_message_removals m WHERE m.message_id=i.message_id)
        AND NOT EXISTS (SELECT 1 FROM e2ee_message_acknowledgements a WHERE a.message_id=i.message_id)
        ORDER BY i.sequence LIMIT $3`, req.Context.DeviceGenerationId, userID, boundary.MaxInboxMessages)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := &pb.PendingMessages{}
	for rows.Next() {
		var wire, digest []byte
		var id, conversation, senderGen, senderAccount string
		var expires time.Time
		var sequence int64
		if err := rows.Scan(&sequence, &wire, &id, &conversation, &senderGen, &senderAccount, &digest, &expires); err != nil {
			return nil, err
		}
		e := new(pb.Envelope)
		if proto.Unmarshal(wire, e) != nil || boundary.ValidateEnvelope(e) != nil || e.MessageId != id || e.ConversationId != conversation ||
			e.Sender.UserId != senderAccount || e.Sender.GenerationId != senderGen || e.Sender.DeviceId != 1 ||
			e.Recipient.UserId != account || e.Recipient.GenerationId != req.Context.DeviceGenerationId || e.Recipient.DeviceId != 1 || sequence <= 0 {
			return nil, domain.ErrE2EEMessageState
		}
		computed, err := boundary.EnvelopeDigest(e)
		if err != nil || !bytes.Equal(computed[:], digest) {
			return nil, domain.ErrE2EEMessageState
		}
		delivery := &pb.Delivery{Sequence: sequence, Envelope: e, ExpiresAtUnixMs: expires.UnixMilli(), EnvelopeDigest: digest}
		result.Messages = append(result.Messages, delivery)
		if proto.Size(result) > boundary.MaxResponseBytes {
			result.Messages = result.Messages[:len(result.Messages)-1]
			if len(result.Messages) == 0 {
				return nil, domain.ErrE2EEMessageState
			}
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func currentDeliveryOwner(ctx context.Context, tx *sql.Tx, userID int64, generation string) (string, error) {
	var account string
	err := tx.QueryRowContext(ctx, `SELECT u.user_uuid FROM e2ee_devices d JOIN users u ON u.id=d.user_id
        WHERE d.user_id=$1 AND d.generation_id=$2 AND d.retired_at IS NULL`, userID, generation).Scan(&account)
	if errors.Is(err, sql.ErrNoRows) {
		return "", domain.ErrE2EEGenerationChanged
	}
	return account, err
}

// query is static SQL; args can be a scalar or an explicit parameter tuple.
func lockDeliveryRows(ctx context.Context, tx *sql.Tx, query string, args any, expected int) error {
	values, ok := args.([]any)
	if !ok {
		values = []any{args}
	}
	rows, err := tx.QueryContext(ctx, query, values...)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != expected {
		return domain.ErrE2EEMessageState
	}
	return nil
}
