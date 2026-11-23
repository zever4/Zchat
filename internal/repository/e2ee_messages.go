package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

type PgE2EEDeliveryRepository struct{ db *sql.DB }

func NewPgE2EEDeliveryRepository(db *sql.DB) *PgE2EEDeliveryRepository {
	return &PgE2EEDeliveryRepository{db: db}
}

// Send commits the immutable persistence receipt, recipient ciphertext inbox and
// bounded counters together. Lock order: users -> devices -> conversation ->
// delivery states, each pair in ascending user order. No network I/O under locks.
func (r *PgE2EEDeliveryRepository) Send(parent context.Context, userID int64, req *pb.SendMessageRequest) (*domain.E2EESendReceipt, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateSend(req); err != nil {
		return nil, err
	}
	digest, err := boundary.EnvelopeDigest(req.Envelope)
	if err != nil {
		return nil, err
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(req.Envelope)
	if err != nil {
		return nil, err
	}
	return r.sendPrepared(parent, userID, &preparedE2EESend{MessageId: req.Envelope.MessageId, ConversationId: req.Envelope.ConversationId,
		Sender: req.Envelope.Sender, Recipient: req.Envelope.Recipient, generation: req.Context.DeviceGenerationId, digest: digest, wire: wire})
}

// Input is built only after the chosen protocol's validation/digest/serialization.
// Epoch=nil belongs to the current prototype; v2 never attempts a v1 decoder.
type preparedE2EESend struct {
	MessageId, ConversationId, generation string
	Sender, Recipient                     *pb.DeviceRef
	digest                                [32]byte
	wire                                  []byte
	epoch                                 *uint64
	blob                                  *v2.BlobRef
}

func (e *preparedE2EESend) version() uint32 {
	if e.epoch == nil {
		return 1
	}
	if e.blob != nil {
		return 3
	}
	return 2
}

func (r *PgE2EEDeliveryRepository) sendPrepared(parent context.Context, userID int64, frame *preparedE2EESend) (*domain.E2EESendReceipt, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	e := frame
	digest, wire := e.digest, e.wire
	var low, high int64
	var lowGen, highGen string
	err = tx.QueryRowContext(ctx, `SELECT low_user_id,high_user_id,low_generation_id::text,high_generation_id::text FROM e2ee_conversations
        WHERE conversation_id=$1 AND ((low_user_id=$2 AND low_generation_id=$3) OR (high_user_id=$2 AND high_generation_id=$3))`, e.ConversationId, userID, e.generation).Scan(&low, &high, &lowGen, &highGen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEConversationNotFound
	}
	if err != nil {
		return nil, err
	}
	// Discovery above supplies only immutable lock targets. Recheck state after
	// locks, including retirement done through another backend or maintenance SQL.
	if err := lockAcquisitionRows(ctx, tx, `SELECT id FROM users WHERE id IN ($1,$2) ORDER BY id FOR SHARE`, low, high); err != nil {
		return nil, err
	}
	if err := lockAcquisitionRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id IN ($1,$2) ORDER BY user_id FOR SHARE`, lowGen, highGen); err != nil {
		return nil, err
	}
	var ownGen string
	err = tx.QueryRowContext(ctx, `SELECT generation_id::text FROM e2ee_devices WHERE user_id=$1 AND retired_at IS NULL`, userID).Scan(&ownGen)
	if errors.Is(err, sql.ErrNoRows) || err == nil && ownGen != e.generation {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if err != nil {
		return nil, err
	}
	var locked string
	if err := tx.QueryRowContext(ctx, `SELECT conversation_id::text FROM e2ee_conversations WHERE conversation_id=$1 FOR SHARE`, e.ConversationId).Scan(&locked); err != nil {
		return nil, err
	}
	c, err := getE2EEConversation(ctx, tx, userID, ownGen, e.ConversationId)
	if err != nil {
		return nil, err
	}
	if !messageDeviceMatches(e.Sender, c.OwnDevice) {
		return nil, domain.ErrInvalidInput
	}
	// An exact retry describes a past COMMIT, even after peer retirement/closure,
	// ACK or expiry. It neither extends expiry nor reconstructs a deleted inbox.
	if saved, err := findPreparedE2EESendReceipt(ctx, tx, userID, e); err != nil {
		return nil, err
	} else if saved != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return saved, nil
	}
	if e.epoch != nil {
		if err := gateEpochSend(ctx, tx, c, low, high, lowGen, highGen, e); err != nil {
			var fence *domain.E2EEEpochSealed
			if errors.As(err, &fence) {
				if commitErr := tx.Commit(); commitErr != nil {
					return nil, commitErr
				}
			}
			return nil, err
		}
	}
	if c.State != "active" {
		return nil, domain.ErrE2EEConversationClosed
	}
	if c.PeerDevice.RetiredAt != nil || !messageDeviceMatches(e.Recipient, c.PeerDevice) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if !c.OwnDevice.PublicMaterialReady || !c.PeerDevice.PublicMaterialReady {
		return nil, domain.ErrE2EEKeysNotReady
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_delivery_state(generation_id,user_id)
        SELECT generation_id,user_id FROM e2ee_devices WHERE generation_id IN ($1,$2) ORDER BY user_id ON CONFLICT DO NOTHING`, lowGen, highGen)
	if err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT user_id,last_sequence,sent_receipts,pending_messages,pending_bytes FROM e2ee_delivery_state
        WHERE generation_id IN ($1,$2) ORDER BY user_id FOR UPDATE`, lowGen, highGen)
	if err != nil {
		return nil, err
	}
	var own, peer deliveryState
	stateRows := 0
	for rows.Next() {
		var id int64
		var state deliveryState
		if err := rows.Scan(&id, &state.sequence, &state.sent, &state.pending, &state.bytes); err != nil {
			rows.Close()
			return nil, err
		}
		if id == userID {
			own = state
		} else {
			peer = state
		}
		stateRows++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if stateRows != 2 {
		return nil, domain.ErrE2EEMessageState
	}
	// Another concurrent request may have committed while waiting for the same
	// pair's state rows. Inspect its exact receipt before any quota/counter change.
	if saved, err := findPreparedE2EESendReceipt(ctx, tx, userID, e); err != nil {
		return nil, err
	} else if saved != nil {
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return saved, nil
	}
	claim := e2eeMessageClaim{message: e.MessageId, chat: c.ID, sender: userID, senderGen: ownGen,
		recipient: c.PeerDevice.UserID, recipientGen: c.PeerDevice.GenerationID, low: low, high: high, lowGen: lowGen, highGen: highGen,
		version: int(e.version()), epoch: e.epoch, digest: digest[:], blob: e.blob}
	var ready *readySendBlob
	if e.blob != nil {
		// The accepted receipt pins Signal digest. Never weaken or mutate the
		// immutable, pre-Signal blob reservation to perform this promotion.
		claim.digest = nil
		ready, err = lockReadySendBlob(ctx, tx, claim)
		if err != nil {
			return nil, blobSendGateFailure(e, err)
		}
	} else if _, err = claimE2EEMessage(ctx, tx, claim); err != nil {
		return nil, err
	}
	if own.sent >= domain.MaxE2EESendReceiptsPerGeneration || peer.pending >= domain.MaxE2EEPendingMessagesPerGeneration ||
		peer.bytes+int64(len(wire)) > domain.MaxE2EEPendingBytesPerGeneration || peer.sequence == math.MaxInt64 {
		return nil, domain.ErrE2EEMessageCapacity
	}
	result := &domain.E2EESendReceipt{MessageID: e.MessageId, Digest: digest, CryptoEpoch: e.epoch}
	if err := tx.QueryRowContext(ctx, `SELECT date_trunc('milliseconds',clock_timestamp())`).Scan(&result.StoredAt); err != nil {
		return nil, err
	}
	result.ExpiresAt = result.StoredAt.Add(domain.E2EEMessageRetention)
	columns := `message_id,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,envelope_digest,stored_at,expires_at`
	values := `$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13`
	args := []any{e.MessageId, c.ID, userID, ownGen, c.PeerDevice.UserID, c.PeerDevice.GenerationID, low, high, lowGen, highGen, digest[:], result.StoredAt, result.ExpiresAt}
	if e.epoch != nil {
		columns += `,wire_version,crypto_epoch`
		values += `,$14,$15`
		args = append(args, e.version(), *e.epoch)
	}
	inserted, err := tx.ExecContext(ctx, `INSERT INTO e2ee_send_receipts(`+columns+`) VALUES(`+values+`) ON CONFLICT(message_id) DO NOTHING`, args...)
	if err != nil {
		return nil, err
	}
	n, err := inserted.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n != 1 {
		return nil, domain.ErrE2EEMessageConflict
	}
	if ready != nil {
		if err = linkSendBlob(ctx, tx, e.MessageId, ready); err != nil {
			return nil, blobSendGateFailure(e, err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE e2ee_delivery_state SET sent_receipts=sent_receipts+1 WHERE generation_id=$1`, ownGen); err != nil {
		return nil, err
	}
	var sequence int64
	err = tx.QueryRowContext(ctx, `UPDATE e2ee_delivery_state SET last_sequence=last_sequence+1,pending_messages=pending_messages+1,pending_bytes=pending_bytes+$2
        WHERE generation_id=$1 RETURNING last_sequence`, c.PeerDevice.GenerationID, len(wire)).Scan(&sequence)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_message_inbox(recipient_generation_id,sequence,message_id,envelope,expires_at) VALUES($1,$2,$3,$4,$5)`, c.PeerDevice.GenerationID, sequence, e.MessageId, wire, result.ExpiresAt)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

type deliveryState struct{ sequence, sent, pending, bytes int64 }

func messageDeviceMatches(ref *pb.DeviceRef, d *domain.E2EEDevice) bool {
	return ref.UserId == d.UserUUID && ref.DeviceId == d.DeviceID && ref.GenerationId == d.GenerationID
}

func findPreparedE2EESendReceipt(ctx context.Context, tx *sql.Tx, userID int64, e *preparedE2EESend) (*domain.E2EESendReceipt, error) {
	var sender int64
	var generation, conversation, recipient string
	var savedDigest []byte
	var version int
	var epoch sql.NullInt64
	result := &domain.E2EESendReceipt{MessageID: e.MessageId}
	columns := `sender_user_id,sender_generation_id::text,conversation_id::text,recipient_generation_id::text,envelope_digest,stored_at,expires_at`
	targets := []any{&sender, &generation, &conversation, &recipient, &savedDigest, &result.StoredAt, &result.ExpiresAt}
	if e.epoch != nil {
		columns += `,wire_version,crypto_epoch`
		targets = append(targets, &version, &epoch)
	}
	err := tx.QueryRowContext(ctx, `SELECT `+columns+` FROM e2ee_send_receipts WHERE message_id=$1`, result.MessageID).Scan(targets...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if sender != userID || generation != e.generation || conversation != e.ConversationId || recipient != e.Recipient.GenerationId || !bytes.Equal(savedDigest, e.digest[:]) {
		return nil, domain.ErrE2EEMessageConflict
	}
	if e.epoch != nil {
		if uint32(version) != e.version() || !epoch.Valid || epoch.Int64 < 0 || uint64(epoch.Int64) != *e.epoch {
			return nil, domain.ErrE2EEMessageConflict
		}
		value := uint64(epoch.Int64)
		result.CryptoEpoch = &value
	}
	if e.blob != nil {
		if err = checkSavedSendBlob(ctx, tx, e); err != nil {
			return nil, err
		}
	}
	if result.StoredAt.UnixMilli() <= 0 || result.StoredAt.Nanosecond()%int(time.Millisecond) != 0 || result.ExpiresAt.Sub(result.StoredAt) != domain.E2EEMessageRetention {
		return nil, domain.ErrE2EEMessageState
	}
	result.Digest = e.digest
	return result, nil
}
