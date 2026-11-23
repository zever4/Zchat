package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type PgE2EEBlobRepository struct{ db *sql.DB }

func NewPgE2EEBlobRepository(db *sql.DB) *PgE2EEBlobRepository { return &PgE2EEBlobRepository{db: db} }

// Configure is explicit and idempotent during opt-in runtime startup. Different
// processes cannot silently apply different quotas. Changing a live budget
// needs a separate reviewed operation; opening a repository never resets it.
func (r *PgE2EEBlobRepository) Configure(parent context.Context, l domain.E2EEBlobLimits) error {
	if !l.Valid() {
		return domain.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_blob_global_quota(max_bytes,account_max_bytes,max_claims,account_max_claims,max_attempts,account_max_attempts)
        VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(singleton) DO NOTHING`, l.GlobalBytes, l.AccountBytes, l.GlobalClaims, l.AccountClaims, l.GlobalAttempts, l.AccountAttempts)
	if err != nil {
		return err
	}
	q, err := lockBlobQuota(ctx, tx)
	if err != nil {
		return err
	}
	if q.limits != l {
		return domain.ErrE2EEBlobConfiguration
	}
	return tx.Commit()
}

// Reserve commits only metadata and full byte/count reservations, never Ready
// or a message receipt. The opt-in native RPC supplies authenticated ownership.
// Order: users, devices,
// conversation/epoch, shared UUID claim, global/account quota, attempts. Never
// hold these locks while reading/writing a file or performing network I/O.
func (r *PgE2EEBlobRepository) Reserve(parent context.Context, uid int64, req *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateBlobReservation(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	claim, err := authorizeBlobReservation(ctx, tx, uid, req)
	if err != nil {
		return nil, err
	}
	fresh, err := claimE2EEMessage(ctx, tx, *claim)
	if err != nil {
		return nil, err
	}
	current, expectedRev, expectedAttempt, err := readBlobHead(ctx, tx, req.MessageID)
	if err != nil {
		return nil, err
	}
	if current != nil && current.OperationID == req.OperationID {
		if expectedRev != req.ExpectedRevision || expectedAttempt != req.ExpectedAttemptID {
			return nil, domain.ErrE2EEMessageConflict
		}
		if err = observeBlobLease(ctx, tx, current); err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return current, nil
	}
	var accepted bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM e2ee_send_receipts WHERE message_id=$1)`, req.MessageID).Scan(&accepted); err != nil {
		return nil, err
	}
	if accepted {
		return nil, domain.ErrE2EEMessageState
	}
	if fresh != (current == nil) {
		return nil, domain.ErrE2EEMessageState
	}
	if current == nil && req.ExpectedRevision != 0 || current != nil && (current.Revision != req.ExpectedRevision || current.AttemptID != req.ExpectedAttemptID) {
		return nil, domain.ErrE2EEBlobCAS
	}
	// An operation UUID cannot be reused to mint another lease, including a
	// superseded operation for this same message. Unique INSERT below closes
	// cross-message races; its entire transaction rolls back on conflict.
	var used bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM e2ee_blob_attempts WHERE operation_id=$1)`, req.OperationID).Scan(&used); err != nil {
		return nil, err
	}
	if used {
		return nil, domain.ErrE2EEBlobCAS
	}
	q, err := lockBlobQuota(ctx, tx)
	if err != nil {
		return nil, err
	}
	a, err := lockBlobAccountQuota(ctx, tx, uid)
	if err != nil {
		return nil, err
	}
	if current != nil {
		// Quota waits may outlive a lease. Take the clock only after all earlier
		// locks, then fence the old attempt before installing its successor.
		if err = tx.QueryRowContext(ctx, `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1 FOR UPDATE`, current.AttemptID).Scan(&current.State); err != nil {
			return nil, err
		}
		if err = observeBlobLease(ctx, tx, current); err != nil {
			return nil, err
		}
		if current.State == "linked" {
			return nil, domain.ErrE2EEMessageState
		}
		if !current.Expired && current.State != "deleting" && current.State != "reclaimed" {
			return nil, domain.ErrE2EEBlobLeaseActive
		}
	}
	n := int64(req.Blob.CiphertextBytes)
	newClaim := int64(0)
	if fresh {
		newClaim = 1
	}
	if !q.global.fits(q.limits.GlobalBytes, q.limits.GlobalClaims, q.limits.GlobalAttempts, n, newClaim) ||
		!a.fits(q.limits.AccountBytes, q.limits.AccountClaims, q.limits.AccountAttempts, n, newClaim) {
		return nil, domain.ErrE2EEBlobCapacity
	}
	grant := &domain.E2EEBlobReservation{MessageID: req.MessageID, AttemptID: uuid.NewString(), OperationID: req.OperationID, Revision: req.ExpectedRevision + 1, State: "reserved"}
	if err = tx.QueryRowContext(ctx, `SELECT date_trunc('milliseconds',clock_timestamp())`).Scan(&grant.ReservedAt); err != nil {
		return nil, err
	}
	grant.ExpiresAt = grant.ReservedAt.Add(domain.E2EEBlobStagingLease)
	grant.ObservedAt = grant.ReservedAt
	var previous any
	if req.ExpectedAttemptID != "" {
		previous = req.ExpectedAttemptID
	}
	if current != nil && current.State != "reclaimed" {
		if _, err = tx.ExecContext(ctx, `UPDATE e2ee_blob_attempts SET state='deleting',deletion_reason=COALESCE(deletion_reason,'lease_renewed') WHERE attempt_id=$1`, current.AttemptID); err != nil {
			return nil, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_blob_attempts(attempt_id,message_id,revision,operation_id,expected_revision,expected_attempt_id,reserved_at,expires_at,charged_bytes)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, grant.AttemptID, grant.MessageID, grant.Revision, grant.OperationID, req.ExpectedRevision, previous, grant.ReservedAt, grant.ExpiresAt, n)
	if err != nil {
		return nil, blobUniqueConflict(err)
	}
	if current == nil {
		_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_blob_heads(message_id,revision,attempt_id) VALUES($1,$2,$3)`, grant.MessageID, grant.Revision, grant.AttemptID)
	} else {
		var changed sql.Result
		changed, err = tx.ExecContext(ctx, `UPDATE e2ee_blob_heads SET revision=$2,attempt_id=$3 WHERE message_id=$1 AND revision=$4 AND attempt_id=$5`,
			grant.MessageID, grant.Revision, grant.AttemptID, req.ExpectedRevision, req.ExpectedAttemptID)
		if err == nil {
			var n int64
			n, err = changed.RowsAffected()
			if err == nil && n != 1 {
				return nil, domain.ErrE2EEBlobCAS
			}
		}
	}
	if err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE e2ee_blob_global_quota SET reserved_bytes=reserved_bytes+$1,retained_claims=retained_claims+$2,retained_attempts=retained_attempts+1 WHERE singleton`, n, newClaim); err != nil {
		return nil, err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE e2ee_blob_account_quota SET reserved_bytes=reserved_bytes+$2,retained_claims=retained_claims+$3,retained_attempts=retained_attempts+1 WHERE user_id=$1`, uid, n, newClaim); err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return grant, nil
}

func authorizeBlobReservation(ctx context.Context, tx *sql.Tx, uid int64, r *domain.E2EEBlobReservationRequest) (*e2eeMessageClaim, error) {
	c := &e2eeMessageClaim{message: r.MessageID, chat: r.ConversationID, sender: uid, senderGen: r.Sender.GenerationId, recipientGen: r.Recipient.GenerationId,
		version: 3, epoch: r.CryptoEpoch, blob: r.Blob}
	err := tx.QueryRowContext(ctx, `SELECT low_user_id,high_user_id,low_generation_id::text,high_generation_id::text FROM e2ee_conversations
        WHERE conversation_id=$1 AND ((low_user_id=$2 AND low_generation_id=$3) OR (high_user_id=$2 AND high_generation_id=$3))`, c.chat, uid, c.senderGen).
		Scan(&c.low, &c.high, &c.lowGen, &c.highGen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEConversationNotFound
	}
	if err != nil {
		return nil, err
	}
	if err = lockAcquisitionRows(ctx, tx, `SELECT id FROM users WHERE id IN ($1,$2) ORDER BY id FOR SHARE`, c.low, c.high); err != nil {
		return nil, err
	}
	if err = lockAcquisitionRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id IN ($1,$2) ORDER BY user_id FOR SHARE`, c.lowGen, c.highGen); err != nil {
		return nil, err
	}
	var own string
	err = tx.QueryRowContext(ctx, `SELECT generation_id::text FROM e2ee_devices WHERE user_id=$1 AND retired_at IS NULL`, uid).Scan(&own)
	if errors.Is(err, sql.ErrNoRows) || err == nil && own != c.senderGen {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if err != nil {
		return nil, err
	}
	var chat string
	if err = tx.QueryRowContext(ctx, `SELECT conversation_id::text FROM e2ee_conversations WHERE conversation_id=$1 FOR SHARE`, c.chat).Scan(&chat); err != nil {
		return nil, err
	}
	conversation, err := getE2EEConversation(ctx, tx, uid, own, c.chat)
	if err != nil {
		return nil, err
	}
	if conversation.State != "active" {
		return nil, domain.ErrE2EEConversationClosed
	}
	if !messageDeviceMatches(r.Sender, conversation.OwnDevice) {
		return nil, domain.ErrInvalidInput
	}
	if conversation.PeerDevice.RetiredAt != nil || !messageDeviceMatches(r.Recipient, conversation.PeerDevice) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if !conversation.OwnDevice.PublicMaterialReady || !conversation.PeerDevice.PublicMaterialReady {
		return nil, domain.ErrE2EEKeysNotReady
	}
	var head uint64
	var state string
	err = tx.QueryRowContext(ctx, `SELECT c.active_epoch,x.state FROM e2ee_conversations c JOIN e2ee_conversation_epochs x
        ON x.conversation_id=c.conversation_id AND x.epoch=$2 WHERE c.conversation_id=$1 FOR SHARE OF c,x`, c.chat, *c.epoch).Scan(&head, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEEpochInactive
	}
	if err != nil {
		return nil, err
	}
	if head != *c.epoch || state != "active" {
		return nil, domain.ErrE2EEEpochInactive
	}
	c.recipient = conversation.PeerDevice.UserID
	return c, nil
}

func readBlobHead(ctx context.Context, tx *sql.Tx, message string) (*domain.E2EEBlobReservation, uint64, string, error) {
	g := &domain.E2EEBlobReservation{MessageID: message}
	var rev uint64
	var attempt sql.NullString
	err := tx.QueryRowContext(ctx, `SELECT a.attempt_id::text,a.operation_id::text,a.revision,a.reserved_at,a.expires_at,a.state,a.expected_revision,a.expected_attempt_id::text
        FROM e2ee_blob_heads h JOIN e2ee_blob_attempts a ON (a.message_id,a.revision,a.attempt_id)=(h.message_id,h.revision,h.attempt_id) WHERE h.message_id=$1`, message).
		Scan(&g.AttemptID, &g.OperationID, &g.Revision, &g.ReservedAt, &g.ExpiresAt, &g.State, &rev, &attempt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, "", nil
	}
	return g, rev, attempt.String, err
}

func observeBlobLease(ctx context.Context, tx *sql.Tx, g *domain.E2EEBlobReservation) error {
	if err := tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&g.ObservedAt); err != nil {
		return err
	}
	g.Expired = !g.ObservedAt.Before(g.ExpiresAt)
	return nil
}

type blobQuotaCounters struct{ bytes, claims, attempts int64 }

func (q blobQuotaCounters) fits(maxBytes, maxClaims, maxAttempts, n, claims int64) bool {
	return q.bytes >= 0 && q.claims >= 0 && q.attempts >= 0 && n <= maxBytes && q.bytes <= maxBytes-n && q.claims <= maxClaims-claims && q.attempts < maxAttempts
}

type blobQuota struct {
	limits domain.E2EEBlobLimits
	global blobQuotaCounters
}

func lockBlobQuota(ctx context.Context, tx *sql.Tx) (*blobQuota, error) {
	q := new(blobQuota)
	err := tx.QueryRowContext(ctx, `SELECT max_bytes,account_max_bytes,max_claims,account_max_claims,max_attempts,account_max_attempts,reserved_bytes,retained_claims,retained_attempts
        FROM e2ee_blob_global_quota WHERE singleton FOR UPDATE`).Scan(&q.limits.GlobalBytes, &q.limits.AccountBytes, &q.limits.GlobalClaims, &q.limits.AccountClaims, &q.limits.GlobalAttempts, &q.limits.AccountAttempts,
		&q.global.bytes, &q.global.claims, &q.global.attempts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEBlobConfiguration
	}
	return q, err
}
func lockBlobAccountQuota(ctx context.Context, tx *sql.Tx, uid int64) (blobQuotaCounters, error) {
	q := blobQuotaCounters{}
	if _, err := tx.ExecContext(ctx, `INSERT INTO e2ee_blob_account_quota(user_id) VALUES($1) ON CONFLICT DO NOTHING`, uid); err != nil {
		return q, err
	}
	err := tx.QueryRowContext(ctx, `SELECT reserved_bytes,retained_claims,retained_attempts FROM e2ee_blob_account_quota WHERE user_id=$1 FOR UPDATE`, uid).Scan(&q.bytes, &q.claims, &q.attempts)
	return q, err
}
