package repository

import (
	"bytes"
	"context"
	"database/sql"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

func validateEpochDeliveryContext(c *pb.ClientContext) error {
	if c == nil || c.ProtocolVersion != 2 || !recoveryUUID(c.DeviceGenerationId) || len(c.ProtoReflect().GetUnknown()) != 0 {
		return domain.ErrInvalidInput
	}
	return nil
}

// PendingV2 is a bounded framing2/3 read with no v1 fallback. A seal blocks NEW
// sends, not retained accepted ciphertext from previous cryptographic epochs.
func (r *PgE2EEDeliveryRepository) PendingV2(parent context.Context, uid int64, c *pb.ClientContext) ([]domain.E2EEEpochDelivery, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := validateEpochDeliveryContext(c); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err = lockDeliveryRows(ctx, tx, `SELECT id FROM users WHERE id=$1 FOR SHARE`, uid, 1); err != nil {
		return nil, err
	}
	if err = lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE user_id=$1 AND generation_id=$2 FOR SHARE`, []any{uid, c.DeviceGenerationId}, 1); err != nil {
		return nil, err
	}
	account, err := currentDeliveryOwner(ctx, tx, uid, c.DeviceGenerationId)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT i.sequence,i.envelope,r.message_id::text,r.conversation_id::text,r.sender_generation_id::text,su.user_uuid,r.envelope_digest,r.expires_at,r.crypto_epoch,r.wire_version
        FROM e2ee_message_inbox i JOIN e2ee_send_receipts r USING(message_id) JOIN e2ee_devices sd ON sd.generation_id=r.sender_generation_id JOIN users su ON su.id=sd.user_id
        WHERE i.recipient_generation_id=$1 AND r.recipient_generation_id=$1 AND r.recipient_user_id=$2 AND r.wire_version IN (2,3) AND i.expires_at>statement_timestamp()
        AND NOT EXISTS(SELECT 1 FROM e2ee_message_removals m WHERE m.message_id=i.message_id)
        AND NOT EXISTS(SELECT 1 FROM e2ee_message_acknowledgements a WHERE a.message_id=i.message_id) ORDER BY i.sequence LIMIT $3`, c.DeviceGenerationId, uid, boundary.MaxInboxMessages)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []domain.E2EEEpochDelivery{}
	size := 0
	for rows.Next() {
		var seq int64
		var wire, digest []byte
		var id, chat, gen, user string
		var expires time.Time
		var epoch sql.NullInt64
		var version uint32
		if err = rows.Scan(&seq, &wire, &id, &chat, &gen, &user, &digest, &expires, &epoch, &version); err != nil {
			return nil, err
		}
		e, err := boundary.DecodeEpochEnvelope(wire)
		if err != nil || e.ProtocolVersion != version || !epoch.Valid || epoch.Int64 < 0 || *e.CryptoEpoch != uint64(epoch.Int64) || e.MessageId != id || e.ConversationId != chat || e.Sender.UserId != user || e.Sender.GenerationId != gen ||
			e.Recipient.UserId != account || e.Recipient.GenerationId != c.DeviceGenerationId || seq <= 0 {
			return nil, domain.ErrE2EEMessageState
		}
		computed, err := boundary.EpochEnvelopeDigest(e)
		if err != nil || !bytes.Equal(digest, computed[:]) {
			return nil, domain.ErrE2EEMessageState
		}
		// Conservative budget includes per-delivery sequence/expiry/digest/framing;
		// future RPC adapter must also enforce its final protobuf response size.
		need := proto.Size(e) + 128
		if size+need > boundary.MaxResponseBytes {
			break
		}
		size += need
		result = append(result, domain.E2EEEpochDelivery{Sequence: seq, Envelope: e, Digest: computed, ExpiresAt: expires})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// ACK metadata is shared registry/ledger material, not a v1 ciphertext reader.
// The required protocol2 context and framing2/3 receipts are checked independently.
func (r *PgE2EEDeliveryRepository) AcknowledgeV2(ctx context.Context, uid int64, req *pb.AcknowledgeMessagesRequest) error {
	if uid <= 0 || req == nil || len(req.ProtoReflect().GetUnknown()) != 0 || validateEpochDeliveryContext(req.Context) != nil || len(req.Messages) < 1 || len(req.Messages) > boundary.MaxAcknowledgements || proto.Size(req) > boundary.MaxRequestBytes {
		return domain.ErrInvalidInput
	}
	ids := map[string]bool{}
	for _, a := range req.Messages {
		if a == nil || !recoveryUUID(a.MessageId) || len(a.EnvelopeDigest) != 32 || ids[a.MessageId] || len(a.ProtoReflect().GetUnknown()) != 0 ||
			(a.Disposition != pb.AckDisposition_ACK_DISPOSITION_STORED && a.Disposition != pb.AckDisposition_ACK_DISPOSITION_REJECTED) {
			return domain.ErrInvalidInput
		}
		ids[a.MessageId] = true
	}
	return r.acknowledgePrepared(ctx, uid, req.Context.DeviceGenerationId, req.Messages, true)
}
