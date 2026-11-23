package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/domain"
)

type cleanupTarget struct {
	id       string
	position domain.E2EECleanupPosition
}

// Cleanup processes at most 32 copies under one 2s pass budget. Each copy commits
// independently. After is only scheduling progress, including busy/error rows;
// it wraps so neither skips nor restarts can discard durable work.
func (r *PgE2EEDeliveryRepository) Cleanup(parent context.Context, lane domain.E2EECleanupLane, after *domain.E2EECleanupPosition) (domain.E2EECleanupResult, error) {
	result := domain.E2EECleanupResult{After: after}
	if lane != domain.E2EECleanupExpired && lane != domain.E2EECleanupReplaced {
		return result, domain.ErrInvalidInput
	}
	if after != nil {
		id, err := uuid.Parse(after.Generation)
		if err != nil || id.Version() != 4 || id.Variant() != uuid.RFC4122 || id.String() != after.Generation || after.Sequence <= 0 || after.At.IsZero() {
			return result, domain.ErrInvalidInput
		}
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	targets, err := r.cleanupTargets(ctx, lane, after)
	if err != nil {
		return result, err
	}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		position := target.position
		result.After = &position
		result.Visited++
		removed, err := r.cleanupCopy(ctx, target.id)
		var pg *pq.Error
		if errors.As(err, &pg) && pg.Code == "55P03" {
			result.Busy++
			continue
		}
		if err != nil {
			return result, err
		}
		if removed {
			result.Removed++
		}
	}
	if len(targets) < domain.MaxE2EECleanupBatch {
		result.After = nil
	}
	return result, nil
}

func (r *PgE2EEDeliveryRepository) cleanupTargets(ctx context.Context, lane domain.E2EECleanupLane, after *domain.E2EECleanupPosition) ([]cleanupTarget, error) {
	// Both orders have indexed leading keys. Retired-device discovery touches
	// metadata/pending states, not retained receipts or ciphertext payloads.
	order, from, condition := "i.expires_at", `e2ee_message_inbox i`, `i.expires_at<=statement_timestamp()`
	if lane == domain.E2EECleanupReplaced {
		order = "d.retired_at"
		from = `e2ee_devices d JOIN e2ee_delivery_state s ON s.generation_id=d.generation_id
        JOIN e2ee_message_inbox i ON i.recipient_generation_id=d.generation_id`
		condition = `d.retired_at IS NOT NULL AND s.pending_messages>0`
	}
	args := []any{domain.MaxE2EECleanupBatch}
	if after != nil {
		condition += ` AND (` + order + `,i.recipient_generation_id,i.sequence)>($2,$3::uuid,$4)`
		args = append(args, after.At, after.Generation, after.Sequence)
	}
	rows, err := r.db.QueryContext(ctx, `SELECT i.message_id::text,`+order+`,i.recipient_generation_id::text,i.sequence FROM `+from+` WHERE `+condition+` ORDER BY `+order+`,i.recipient_generation_id,i.sequence LIMIT $1`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []cleanupTarget
	for rows.Next() {
		var target cleanupTarget
		if err := rows.Scan(&target.id, &target.position.At, &target.position.Generation, &target.position.Sequence); err != nil {
			return nil, err
		}
		result = append(result, target)
	}
	return result, rows.Err()
}

func (r *PgE2EEDeliveryRepository) cleanupCopy(ctx context.Context, id string) (bool, error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	// NOWAIT on canonical locks avoids hot accounts blocking unrelated cleanup.
	// These transaction-local limits also bound external/manual row-lock waits.
	if _, err = tx.ExecContext(ctx, `SET LOCAL lock_timeout='100ms'; SET LOCAL statement_timeout='1s'`); err != nil {
		return false, err
	}
	var sender, recipient int64
	var senderGen, recipientGen string
	if err = tx.QueryRowContext(ctx, `SELECT sender_user_id,recipient_user_id,sender_generation_id::text,recipient_generation_id::text FROM e2ee_send_receipts WHERE message_id=$1`, id).Scan(&sender, &recipient, &senderGen, &recipientGen); err != nil {
		return false, err
	}
	if err = lockDeliveryRows(ctx, tx, `SELECT id FROM users WHERE id=ANY($1::bigint[]) ORDER BY id FOR SHARE NOWAIT`, pq.Array([]int64{sender, recipient}), 2); err != nil {
		return false, err
	}
	gens := pq.Array([]string{senderGen, recipientGen})
	if err = lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id=ANY($1::uuid[]) ORDER BY user_id,generation_id FOR SHARE NOWAIT`, gens, 2); err != nil {
		return false, err
	}
	if err = lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_delivery_state WHERE generation_id=ANY($1::uuid[]) ORDER BY user_id,generation_id FOR UPDATE NOWAIT`, gens, 2); err != nil {
		return false, err
	}
	// No current-owner check here: accepted expired copies must also be cleaned
	// when sender/recipient installations have retired. Facts are re-read locked.
	facts, err := loadMessageFacts(ctx, tx, []string{id}, true)
	if err != nil {
		return false, err
	}
	v := facts[id]
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT date_trunc('milliseconds',clock_timestamp())`).Scan(&now); err != nil {
		return false, err
	}
	reason := messageRemovalReason(v, now)
	if reason == "" {
		return false, tx.Commit()
	}
	if err = removeMessageCopy(ctx, tx, v, reason, now); err != nil {
		return false, err
	}
	if err = tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}
