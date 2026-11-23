package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

// Shared lock order: chat -> actor role -> target row. Every manager mutation
// checks authority inside the same transaction as the write, even after waiting.
func (r *PgChatRepository) withGroupManager(ctx context.Context, chatID, actor int64, work func(context.Context, *PgChatRepository, string) error) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		if err := lockGroup(ctx, tx, chatID); err != nil {
			return err
		}
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
		return work(ctx, &PgChatRepository{db: tx}, role)
	})
}

func (r *PgChatRepository) ChangeGroupMemberRole(ctx context.Context, chatID, actor, target int64, role string) error {
	if actor == target || (role != "member" && role != "admin") {
		return domain.ErrInvalidInput
	}
	return r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		owner, err := b.IsGroupOwner(ctx, chatID, actor)
		if err != nil {
			return err
		}
		if !owner {
			return domain.ErrNotGroupOwner
		}
		old, err := lockTargetRole(ctx, b, chatID, target)
		if err != nil {
			return err
		}
		if old == "owner" {
			return domain.ErrNotGroupOwner
		}
		if old == role {
			return nil
		}
		if err = b.SetMemberRole(ctx, chatID, target, role); err != nil {
			return err
		}
		action := "demoted_to_member"
		if role == "admin" {
			action = "promoted_to_admin"
		}
		details, _ := json.Marshal(map[string]any{"target_user_id": target, "new_role": role})
		return b.AddGroupAuditLog(ctx, chatID, actor, action, string(details))
	})
}
func lockTargetRole(ctx context.Context, b *PgChatRepository, chatID, target int64) (string, error) {
	var role string
	err := b.db.QueryRowContext(ctx, `SELECT role FROM chat_members WHERE chat_id=$1 AND user_id=$2 FOR UPDATE`, chatID, target).Scan(&role)
	if err == sql.ErrNoRows {
		return "", domain.ErrNotChatMember
	}
	return role, err
}
func (r *PgChatRepository) KickGroupMember(ctx context.Context, chatID, actor, target int64) error {
	if actor == target {
		return domain.ErrInvalidInput
	}
	return r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, actorRole string) error {
		targetRole, err := lockTargetRole(ctx, b, chatID, target)
		if err != nil {
			return err
		}
		if targetRole == "owner" || (targetRole == "admin" && actorRole != "owner") {
			return domain.ErrNotGroupOwner
		}
		if err = b.RemoveMember(ctx, chatID, target); err != nil {
			return err
		}
		details, _ := json.Marshal(map[string]int64{"target_user_id": target})
		return b.AddGroupAuditLog(ctx, chatID, actor, "member_kicked", string(details))
	})
}
func (r *PgChatRepository) ChangeGroupPermissions(ctx context.Context, chatID, actor int64, admins, hidden, anonymous *bool) error {
	if admins == nil && hidden == nil && anonymous == nil {
		return domain.ErrInvalidInput
	}
	return r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		if err := b.SetGroupPermissions(ctx, chatID, admins, hidden, anonymous); err != nil {
			return err
		}
		details := map[string]bool{}
		if admins != nil {
			details["admins_only_write"] = *admins
		}
		if hidden != nil {
			details["hide_members_list"] = *hidden
		}
		if anonymous != nil {
			details["anonymous_mode"] = *anonymous
		}
		raw, _ := json.Marshal(details)
		return b.AddGroupAuditLog(ctx, chatID, actor, "permissions_updated", string(raw))
	})
}

func (r *PgChatRepository) UpdateGroupSettings(ctx context.Context, chatID, actor int64, name string, public bool) error {
	return r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		return b.updateGroupSettings(ctx, chatID, actor, name, public)
	})
}
func (r *PgChatRepository) UpdateGroupAvatar(ctx context.Context, chatID, actor int64, avatar string) error {
	return r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		return b.updateGroupAvatar(ctx, chatID, actor, avatar)
	})
}
func (r *PgChatRepository) ReplaceGroupAvatar(ctx context.Context, chatID, actor int64, avatar string) (string, error) {
	var old string
	err := r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		if err := b.db.QueryRowContext(ctx, `SELECT COALESCE(avatar_url,'') FROM chats WHERE id=$1`, chatID).Scan(&old); err != nil {
			return err
		}
		return b.updateGroupAvatar(ctx, chatID, actor, avatar)
	})
	if err != nil {
		return "", err
	}
	return old, nil
}
func (r *PgChatRepository) SetGroupInviteActive(ctx context.Context, chatID, inviteID, actor int64, active bool) error {
	return r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		return b.setGroupInviteActive(ctx, chatID, inviteID, actor, active)
	})
}
func (r *PgChatRepository) DeleteGroupInviteLink(ctx context.Context, chatID, inviteID, actor int64) error {
	return r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		return b.deleteGroupInviteLink(ctx, chatID, inviteID, actor)
	})
}
func (r *PgChatRepository) CreateGroupInviteLink(ctx context.Context, chatID, actor int64) (*domain.GroupInviteLink, error) {
	var link *domain.GroupInviteLink
	err := r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		var err error
		link, err = b.createGroupInviteLink(ctx, chatID, actor)
		return err
	})
	if err != nil {
		return nil, err
	}
	return link, nil
}
func (r *PgChatRepository) RegenerateGroupInviteCode(ctx context.Context, chatID, actor int64) (string, error) {
	var code string
	err := r.withGroupManager(ctx, chatID, actor, func(ctx context.Context, b *PgChatRepository, _ string) error {
		var err error
		code, err = b.regenerateGroupInviteCode(ctx, chatID, actor)
		return err
	})
	if err != nil {
		return "", err
	}
	return code, nil
}
