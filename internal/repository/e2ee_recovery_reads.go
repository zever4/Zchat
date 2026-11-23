package repository

import (
	"context"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

// Advisory refs only, <=33 SQL rows/<=32 results. Does not normalize all history
// or lock another participant after holding this owner's locks. Fresh exact Get
// rechecks both actors/state before any decision; an empty page proves nothing.
func (r *PgE2EERecoveryRepository) PendingRecoveries(parent context.Context, uid int64, gen, after string, limit int) (*domain.E2EEPendingRecoveries, error) {
	if uid <= 0 || !recoveryUUID(gen) || (after != "" && !recoveryUUID(after)) || limit < 1 || limit > domain.MaxE2EERecoveryPage {
		return nil, domain.ErrInvalidInput
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
	if err = lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE user_id=$1 AND generation_id=$2 FOR SHARE`, []any{uid, gen}, 1); err != nil {
		return nil, err
	}
	if _, err = currentDeliveryOwner(ctx, tx, uid, gen); err != nil {
		return nil, err
	}
	var cursor any
	if after != "" {
		cursor = after
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.conversation_id::text,r.proposal_id::text,r.expected_epoch,r.expected_revision,r.initiator_user_id=$1,r.created_at,r.expires_at
        FROM e2ee_epoch_recoveries r JOIN e2ee_conversations c USING(conversation_id)
        WHERE r.state='pending' AND c.state='active' AND r.expected_epoch=c.active_epoch AND r.expected_revision=c.epoch_revision
          AND r.expires_at>statement_timestamp() AND ($3::uuid IS NULL OR r.conversation_id>$3::uuid)
          AND ((r.initiator_user_id=$1 AND r.initiator_generation_id=$2) OR (r.peer_user_id=$1 AND r.peer_generation_id=$2))
        ORDER BY r.conversation_id LIMIT $4`, uid, gen, cursor, limit+1)
	if err != nil {
		return nil, err
	}
	page := &domain.E2EEPendingRecoveries{}
	for rows.Next() {
		var v domain.E2EEPendingRecoveryReference
		if err = rows.Scan(&v.ConversationID, &v.ProposalID, &v.ExpectedEpoch, &v.ExpectedRevision, &v.InitiatedByOwn, &v.CreatedAt, &v.ExpiresAt); err != nil {
			rows.Close()
			return nil, err
		}
		page.Proposals = append(page.Proposals, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if len(page.Proposals) > limit {
		page.Proposals = page.Proposals[:limit]
		page.NextConversationID = page.Proposals[limit-1].ConversationID
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return page, nil
}
