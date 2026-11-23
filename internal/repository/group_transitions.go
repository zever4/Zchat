package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

func lockGroup(ctx context.Context, tx *sql.Tx, chatID int64) error {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM chats WHERE id=$1 AND type='group' FOR UPDATE`, chatID).Scan(&id)
	if err == sql.ErrNoRows {
		return domain.ErrChatNotFound
	}
	return err
}

// Membership, status and audit describe one committed decision, never a partial one.
func (r *PgChatRepository) DecideGroupJoinRequest(ctx context.Context, chatID, actor, target int64, approve bool) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		if err := lockGroup(ctx, tx, chatID); err != nil {
			return err
		}
		// Lock the actual role row: concurrent demotion/removal must finish before
		// authorization, or wait until this decision commits.
		var role string
		err := tx.QueryRowContext(ctx, `SELECT role FROM chat_members WHERE chat_id=$1 AND user_id=$2 FOR SHARE`, chatID, actor).Scan(&role)
		if err == sql.ErrNoRows {
			return domain.ErrNotGroupOwner
		}
		if err != nil {
			return err
		}
		if role != "owner" && role != "admin" {
			return domain.ErrNotGroupOwner
		}
		var status string
		err = tx.QueryRowContext(ctx, `SELECT status FROM group_join_requests WHERE chat_id=$1 AND user_id=$2 FOR UPDATE`, chatID, target).Scan(&status)
		if err == sql.ErrNoRows {
			return domain.ErrJoinRequestNotFound
		}
		if err != nil {
			return err
		}
		bound := &PgChatRepository{db: tx}
		desired, action := "rejected", "join_request_rejected"
		if approve {
			desired, action = "approved", "join_request_approved"
		}
		if status == desired {
			if approve {
				member, err := bound.IsMember(ctx, chatID, target)
				if err != nil {
					return err
				}
				// An old approve retry must never restore a member who has since left.
				if !member {
					return domain.ErrJoinRequestNotPending
				}
			}
			return nil
		}
		if status != "pending" {
			return domain.ErrJoinRequestNotPending
		}
		if approve {
			if err = bound.AddMember(ctx, chatID, target); err != nil {
				return err
			}
		}
		if err = bound.UpdateGroupJoinRequestStatus(ctx, chatID, target, desired); err != nil {
			return err
		}
		details, _ := json.Marshal(map[string]int64{"target_user_id": target})
		return bound.AddGroupAuditLog(ctx, chatID, actor, action, string(details))
	})
}

// Invite joining shares the decision's group lock, so retries cannot reset a just
// approved request to pending. Invite validity and public/private mode stay locked.
func (r *PgChatRepository) JoinGroupByInvite(ctx context.Context, code string, userID int64) (*domain.Chat, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var group *domain.Chat
	var status string
	err := inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		bound := &PgChatRepository{db: tx}
		initial, err := bound.GetChatByInviteCode(ctx, code)
		if err != nil {
			return err
		}
		if err = lockGroup(ctx, tx, initial.ID); err != nil {
			return err
		}
		var linkID int64
		err = tx.QueryRowContext(ctx, `SELECT id FROM group_invite_links WHERE chat_id=$1 AND code=$2 AND is_active=TRUE FOR SHARE`, initial.ID, code).Scan(&linkID)
		if err == sql.ErrNoRows {
			return domain.ErrChatNotFound
		}
		if err != nil {
			return err
		}
		group, err = bound.GetChatByInviteCode(ctx, code)
		if err != nil {
			return err
		}
		member, err := bound.IsMember(ctx, group.ID, userID)
		if err != nil {
			return err
		}
		if member {
			status = "joined"
			return nil
		}
		action := "join_request_created"
		if group.IsPublic {
			if err = bound.AddMember(ctx, group.ID, userID); err != nil {
				return err
			}
			// A pending request from the former private mode is now fulfilled.
			if _, err = tx.ExecContext(ctx, `UPDATE group_join_requests SET status='approved',updated_at=CURRENT_TIMESTAMP WHERE chat_id=$1 AND user_id=$2`, group.ID, userID); err != nil {
				return err
			}
			status, action = "joined", "joined_by_invite"
		} else {
			req, err := bound.GetGroupJoinRequest(ctx, group.ID, userID)
			if err != nil {
				return err
			}
			status = "pending"
			if req != nil && req.Status == "pending" {
				return nil
			}
			if _, err = bound.CreateGroupJoinRequest(ctx, group.ID, userID); err != nil {
				return err
			}
		}
		details, _ := json.Marshal(map[string]string{"code": code})
		return bound.AddGroupAuditLog(ctx, group.ID, userID, action, string(details))
	})
	if err != nil {
		return nil, "", err
	}
	return group, status, nil
}
