package repository

import (
	"bytes"
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type messageObservation struct {
	id, senderGen, recipientGen   string
	sender, recipient             int64
	digest                        []byte
	stored, expires               time.Time
	ack, removal                  sql.NullString
	ackedAt, removedAt, retiredAt sql.NullTime
	inbox                         bool
	bytes                         sql.NullInt64
}

// First discover immutable targets, then lock ALL users/devices/state rows in
// canonical order. Do not lock message1's pair and then message2's pair: opposite
// batches and Send must share one global order. Re-read facts after these locks.
func loadMessageObservations(ctx context.Context, tx *sql.Tx, userID int64, generation string, ids []string, sender bool) (map[string]*messageObservation, error) {
	owner := `recipient_user_id=$2 AND recipient_generation_id=$3`
	if sender {
		owner = `sender_user_id=$2 AND sender_generation_id=$3`
	}
	rows, err := tx.QueryContext(ctx, `SELECT message_id::text,sender_user_id,sender_generation_id::text,recipient_user_id,recipient_generation_id::text
        FROM e2ee_send_receipts WHERE message_id=ANY($1::uuid[]) AND `+owner, pq.Array(ids), userID, generation)
	if err != nil {
		return nil, err
	}
	users := map[int64]bool{}
	generations := map[string]bool{}
	count := 0
	for rows.Next() {
		var id, sg, rg string
		var s, r int64
		if err := rows.Scan(&id, &s, &sg, &r, &rg); err != nil {
			rows.Close()
			return nil, err
		}
		users[s] = true
		users[r] = true
		generations[sg] = true
		generations[rg] = true
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if count != len(ids) {
		return nil, domain.ErrE2EEMessageNotFound
	}
	userIDs := make([]int64, 0, len(users))
	for id := range users {
		userIDs = append(userIDs, id)
	}
	gens := make([]string, 0, len(generations))
	for gen := range generations {
		gens = append(gens, gen)
	}
	if err := lockDeliveryRows(ctx, tx, `SELECT id FROM users WHERE id=ANY($1::bigint[]) ORDER BY id FOR SHARE`, pq.Array(userIDs), len(users)); err != nil {
		return nil, err
	}
	if err := lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id=ANY($1::uuid[]) ORDER BY user_id,generation_id FOR SHARE`, pq.Array(gens), len(generations)); err != nil {
		return nil, err
	}
	if _, err := currentDeliveryOwner(ctx, tx, userID, generation); err != nil {
		return nil, err
	}
	if err := lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_delivery_state WHERE generation_id=ANY($1::uuid[]) ORDER BY user_id,generation_id FOR UPDATE`, pq.Array(gens), len(generations)); err != nil {
		return nil, err
	}
	return loadMessageFacts(ctx, tx, ids, false)
}

// Caller holds canonical user/device/state locks. Background cleanup cannot
// authenticate a retired owner, but shares exactly the same fact validation.
func loadMessageFacts(ctx context.Context, tx *sql.Tx, ids []string, nowait bool) (map[string]*messageObservation, error) {
	suffix := ""
	if nowait {
		suffix = " NOWAIT"
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.message_id::text,r.sender_user_id,r.sender_generation_id::text,r.recipient_user_id,r.recipient_generation_id::text,
        r.envelope_digest,r.stored_at,r.expires_at,a.disposition,a.acknowledged_at,m.reason,m.removed_at,
        i.message_id IS NOT NULL,octet_length(i.envelope),d.retired_at
        FROM e2ee_send_receipts r JOIN e2ee_devices d ON d.generation_id=r.recipient_generation_id
        LEFT JOIN e2ee_message_acknowledgements a USING(message_id) LEFT JOIN e2ee_message_removals m USING(message_id)
        LEFT JOIN e2ee_message_inbox i USING(message_id) WHERE r.message_id=ANY($1::uuid[]) ORDER BY r.message_id FOR UPDATE OF r`+suffix, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]*messageObservation, len(ids))
	for rows.Next() {
		v := new(messageObservation)
		if err := rows.Scan(&v.id, &v.sender, &v.senderGen, &v.recipient, &v.recipientGen, &v.digest, &v.stored, &v.expires, &v.ack, &v.ackedAt, &v.removal, &v.removedAt, &v.inbox, &v.bytes, &v.retiredAt); err != nil {
			return nil, err
		}
		if len(v.digest) != 32 || v.stored.UnixMilli() <= 0 || v.stored.Nanosecond()%int(time.Millisecond) != 0 || v.expires.Sub(v.stored) != domain.E2EEMessageRetention ||
			v.inbox == v.removal.Valid || v.inbox != v.bytes.Valid || v.inbox && v.bytes.Int64 <= 0 || v.ack.Valid != v.ackedAt.Valid || v.removal.Valid != v.removedAt.Valid ||
			v.ack.Valid && v.inbox || v.removal.String == "acknowledged" && !v.ack.Valid {
			return nil, domain.ErrE2EEMessageState
		}
		result[v.id] = v
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) != len(ids) {
		return nil, domain.ErrE2EEMessageState
	}
	return result, nil
}

// Removal changes counters exactly once. The persistence receipt/sequence and
// previous immutable facts remain; a late ACK never calls this for an absent body.
func removeMessageCopy(ctx context.Context, tx *sql.Tx, v *messageObservation, reason string, now time.Time) error {
	var size int64
	if err := tx.QueryRowContext(ctx, `DELETE FROM e2ee_message_inbox WHERE message_id=$1 AND recipient_generation_id=$2 RETURNING octet_length(envelope)`, v.id, v.recipientGen).Scan(&size); err != nil {
		return err
	}
	updated, err := tx.ExecContext(ctx, `UPDATE e2ee_delivery_state SET pending_messages=pending_messages-1,pending_bytes=pending_bytes-$2
        WHERE generation_id=$1 AND pending_messages>=1 AND pending_bytes>=$2`, v.recipientGen, size)
	if err != nil {
		return err
	}
	n, err := updated.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return domain.ErrE2EEMessageState
	}
	if now.Before(v.stored) {
		now = v.stored
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO e2ee_message_removals(message_id,reason,removed_at) VALUES($1,$2,$3)`, v.id, reason, now); err != nil {
		return err
	}
	v.inbox = false
	v.bytes = sql.NullInt64{}
	v.removal = sql.NullString{String: reason, Valid: true}
	v.removedAt = sql.NullTime{Time: now, Valid: true}
	return nil
}

func (r *PgE2EEDeliveryRepository) Acknowledge(parent context.Context, userID int64, req *pb.AcknowledgeMessagesRequest) error {
	if userID <= 0 {
		return domain.ErrInvalidInput
	}
	if err := boundary.ValidateAcknowledgements(req); err != nil {
		return err
	}
	return r.acknowledgePrepared(parent, userID, req.Context.DeviceGenerationId, req.Messages, false)
}

// Common durable ACK facts have no ciphertext decoder. Epoch ACK still requires
// a strict protocol2 context and framing2/3 ledger row; no active-epoch gate.
func (r *PgE2EEDeliveryRepository) acknowledgePrepared(parent context.Context, userID int64, generation string, messages []*pb.Acknowledgement, epochOnly bool) error {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ids := make([]string, len(messages))
	for i, ack := range messages {
		ids[i] = ack.MessageId
	}
	facts, err := loadMessageObservations(ctx, tx, userID, generation, ids, false)
	if err != nil {
		return err
	}
	if epochOnly {
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_send_receipts WHERE message_id=ANY($1::uuid[]) AND wire_version IN (2,3) AND crypto_epoch>=0`, pq.Array(ids)).Scan(&count); err != nil {
			return err
		}
		if count != len(ids) {
			return domain.ErrE2EEMessageConflict
		}
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT date_trunc('milliseconds',clock_timestamp())`).Scan(&now); err != nil {
		return err
	}
	for _, ack := range messages {
		v := facts[ack.MessageId]
		if !bytes.Equal(v.digest, ack.EnvelopeDigest) {
			return domain.ErrE2EEMessageConflict
		}
		disposition := "stored"
		if ack.Disposition == pb.AckDisposition_ACK_DISPOSITION_REJECTED {
			disposition = "rejected"
		}
		if v.ack.Valid {
			if v.ack.String != disposition {
				return domain.ErrE2EEMessageConflict
			}
			continue
		}
		// Delayed ACK after expiry records stronger recipient evidence without
		// overwriting the earlier removal fact or recreating the server copy.
		if _, err := tx.ExecContext(ctx, `INSERT INTO e2ee_message_acknowledgements(message_id,disposition,acknowledged_at) VALUES($1,$2,$3)`, v.id, disposition, maxMessageTime(now, v.stored)); err != nil {
			return err
		}
		if v.inbox {
			if err := removeMessageCopy(ctx, tx, v, "acknowledged", now); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func maxMessageTime(now, stored time.Time) time.Time {
	if now.Before(stored) {
		return stored
	}
	return now
}

// Bounded sender refresh finalizes expired/replaced recipient copies. ACK facts
// take precedence, including ACK after an earlier expiry removal. This is not a
// background/global cleanup worker; only the requested own messages are touched.
func (r *PgE2EEDeliveryRepository) Statuses(parent context.Context, userID int64, req *pb.GetMessageStatusesRequest) (*pb.MessageStatuses, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateMessageStatuses(req); err != nil {
		return nil, err
	}
	observations, err := r.statusesPrepared(parent, userID, req.Context.DeviceGenerationId, req.Messages, false)
	if err != nil {
		return nil, err
	}
	result := &pb.MessageStatuses{}
	for _, v := range observations {
		result.Messages = append(result.Messages, &pb.MessageStatus{Receipt: &pb.SendReceipt{MessageId: v.Receipt.MessageID, EnvelopeDigest: v.Receipt.Digest[:], StoredAtUnixMs: v.Receipt.StoredAt.UnixMilli(), ExpiresAtUnixMs: v.Receipt.ExpiresAt.UnixMilli()}, State: v.State, ObservedAtUnixMs: v.ObservedAt.UnixMilli()})
	}
	return result, nil
}
func (r *PgE2EEDeliveryRepository) statusesPrepared(parent context.Context, userID int64, generation string, messages []*pb.MessageReference, epochOnly bool) ([]domain.E2EEMessageStatus, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids := make([]string, len(messages))
	for i, ref := range messages {
		ids[i] = ref.MessageId
	}
	facts, err := loadMessageObservations(ctx, tx, userID, generation, ids, true)
	if err != nil {
		return nil, err
	}
	epochs := map[string]*uint64{}
	if epochOnly {
		rows, err := tx.QueryContext(ctx, `SELECT message_id::text,wire_version,crypto_epoch FROM e2ee_send_receipts WHERE message_id=ANY($1::uuid[])`, pq.Array(ids))
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			var version int
			var value sql.NullInt64
			if err = rows.Scan(&id, &version, &value); err != nil {
				rows.Close()
				return nil, err
			}
			if (version != 2 && version != 3) || !value.Valid || value.Int64 < 0 {
				rows.Close()
				return nil, domain.ErrE2EEMessageConflict
			}
			epoch := uint64(value.Int64)
			epochs[id] = &epoch
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
		if len(epochs) != len(ids) {
			return nil, domain.ErrE2EEMessageState
		}
	}
	var now time.Time
	if err := tx.QueryRowContext(ctx, `SELECT date_trunc('milliseconds',clock_timestamp())`).Scan(&now); err != nil {
		return nil, err
	}
	result := []domain.E2EEMessageStatus{}
	for _, ref := range messages {
		v := facts[ref.MessageId]
		if !bytes.Equal(v.digest, ref.EnvelopeDigest) {
			return nil, domain.ErrE2EEMessageConflict
		}
		if reason := messageRemovalReason(v, now); reason != "" {
			if err := removeMessageCopy(ctx, tx, v, reason, now); err != nil {
				return nil, err
			}
		}
		state := pb.MessageState_MESSAGE_STATE_PENDING
		observed := v.stored
		if v.ack.Valid {
			observed = v.ackedAt.Time
			switch v.ack.String {
			case "stored":
				state = pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED
			case "rejected":
				state = pb.MessageState_MESSAGE_STATE_RECIPIENT_REJECTED
			default:
				return nil, domain.ErrE2EEMessageState
			}
		} else if v.removal.Valid {
			observed = v.removedAt.Time
			switch v.removal.String {
			case "expired":
				state = pb.MessageState_MESSAGE_STATE_EXPIRED
			case "recipient_replaced":
				state = pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED
			default:
				return nil, domain.ErrE2EEMessageState
			}
		}
		var digest [32]byte
		copy(digest[:], v.digest)
		result = append(result, domain.E2EEMessageStatus{Receipt: domain.E2EESendReceipt{MessageID: v.id, Digest: digest, StoredAt: v.stored, ExpiresAt: v.expires, CryptoEpoch: epochs[v.id]}, State: state, ObservedAt: observed})
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// Both foreground refresh and background cleanup preserve this precedence.
// Only retirement of the RECIPIENT invalidates an otherwise-live server copy.
func messageRemovalReason(v *messageObservation, now time.Time) string {
	if !v.inbox {
		return ""
	}
	if v.retiredAt.Valid {
		return "recipient_replaced"
	}
	if !v.expires.After(now) {
		return "expired"
	}
	return ""
}
