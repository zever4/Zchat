package repository

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/lib/pq"
	"github.com/zever4/Zchat/internal/domain"
)

type PgChatRepository struct {
	db sqlExecutor
}

func NewPgChatRepository(db *sql.DB) *PgChatRepository {
	return &PgChatRepository{db: db}
}

func newInviteCode() (string, error) {
	// 12 hex chars is compact and enough for invite links.
	raw := make([]byte, 6)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505"
	}
	return false
}

func (r *PgChatRepository) CreateWithMembers(ctx context.Context, chatType string, userID1, userID2 int64) (*domain.Chat, error) {
	chat := &domain.Chat{Type: chatType}
	err := inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		var err error
		err = tx.QueryRowContext(ctx,
			`INSERT INTO chats (type) VALUES ($1) RETURNING id, created_at`,
			chatType,
		).Scan(&chat.ID, &chat.CreatedAt)
		if err != nil {
			return fmt.Errorf("failed to create chat: %w", err)
		}

		_, err = tx.ExecContext(ctx,
			`INSERT INTO chat_members (chat_id, user_id) VALUES ($1, $2), ($1, $3)`,
			chat.ID, userID1, userID2,
		)
		if err != nil {
			return fmt.Errorf("failed to add members: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return chat, nil
}

func (r *PgChatRepository) CreateGroup(ctx context.Context, ownerID int64, name string, isPublic bool) (*domain.Chat, error) {
	chat := &domain.Chat{
		Type:     "group",
		Name:     name,
		OwnerID:  ownerID,
		IsPublic: isPublic,
	}
	err := inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		var err error
		const maxAttempts = 8
		for range maxAttempts {
			inviteCode, codeErr := newInviteCode()
			if codeErr != nil {
				return fmt.Errorf("failed to generate invite code: %w", codeErr)
			}

			err = tx.QueryRowContext(ctx, `
			INSERT INTO chats (type, name, owner_id, is_public, invite_code)
			VALUES ('group', $1, $2, $3, $4)
			RETURNING id, created_at, invite_code, COALESCE(avatar_url, '')
		`, name, ownerID, isPublic, inviteCode).Scan(&chat.ID, &chat.CreatedAt, &chat.InviteCode, &chat.AvatarURL)
			if err == nil {
				break
			}
			if !isUniqueViolation(err) {
				return fmt.Errorf("failed to create group: %w", err)
			}
		}
		if err != nil {
			return fmt.Errorf("failed to create group after retries: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `INSERT INTO chat_members (chat_id, user_id, role) VALUES ($1, $2, 'owner')`, chat.ID, ownerID); err != nil {
			return fmt.Errorf("failed to add group owner as member: %w", err)
		}

		if _, err := tx.ExecContext(ctx, `
		INSERT INTO group_invite_links (chat_id, code, created_by, is_active)
		VALUES ($1, $2, $3, TRUE)
	`, chat.ID, chat.InviteCode, ownerID); err != nil {
			return fmt.Errorf("failed to create initial group invite: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	return chat, nil
}

func (r *PgChatRepository) GetUserChats(ctx context.Context, userID int64) ([]*domain.Chat, error) {
	query := `SELECT
				c.id,
				c.type,
				COALESCE(c.name, ''),
				c.created_at,
				COALESCE(c.owner_id, 0),
				COALESCE(c.is_public, TRUE),
				COALESCE(c.invite_code, ''),
				COALESCE(c.avatar_url, ''),
				COALESCE(c.admins_only_write, FALSE),
				COALESCE(c.hide_members_list, FALSE),
				COALESCE(c.anonymous_mode, FALSE),
				COALESCE(u2.username, ''),
				COALESCE(u2.id, 0),
				COALESCE(CASE WHEN c.type = 'group' THEN c.avatar_url ELSE u2.avatar_url END, '')
			FROM chats c
		JOIN chat_members cm ON c.id = cm.chat_id AND cm.user_id = $1
		LEFT JOIN LATERAL (
			SELECT u.id, u.username, COALESCE(u.avatar_url, '') AS avatar_url
			FROM chat_members cm2
			JOIN users u ON u.id = cm2.user_id
			WHERE cm2.chat_id = c.id AND cm2.user_id != $1
			ORDER BY u.id
			LIMIT 1
		) u2 ON c.type = 'private'
		ORDER BY c.created_at DESC`

	rows, err := r.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get user chats: %w", err)
	}
	defer rows.Close()

	var chats []*domain.Chat
	for rows.Next() {
		c := &domain.Chat{}
		if err := rows.Scan(
			&c.ID,
			&c.Type,
			&c.Name,
			&c.CreatedAt,
			&c.OwnerID,
			&c.IsPublic,
			&c.InviteCode,
			&c.AvatarURL,
			&c.AdminsOnlyWrite,
			&c.HideMembersList,
			&c.AnonymousMode,
			&c.OtherUsername,
			&c.OtherUserID,
			&c.OtherAvatarURL,
		); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		chats = append(chats, c)
	}
	return chats, rows.Err()
}

func (r *PgChatRepository) GetPrivateChatBetween(ctx context.Context, userID1, userID2 int64) (*domain.Chat, error) {
	query := `SELECT c.id, c.type, COALESCE(c.name, ''), c.created_at
		FROM chats c
		JOIN chat_members cm1 ON c.id = cm1.chat_id AND cm1.user_id = $1
		JOIN chat_members cm2 ON c.id = cm2.chat_id AND cm2.user_id = $2
		WHERE c.type = 'private'
		LIMIT 1`

	c := &domain.Chat{}
	err := r.db.QueryRowContext(ctx, query, userID1, userID2).Scan(&c.ID, &c.Type, &c.Name, &c.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get private chat: %w", err)
	}
	return c, nil
}

func (r *PgChatRepository) GetChatByID(ctx context.Context, chatID int64) (*domain.Chat, error) {
	query := `SELECT
				id,
				type,
				COALESCE(name, ''),
				created_at,
				COALESCE(owner_id, 0),
				COALESCE(is_public, TRUE),
				COALESCE(invite_code, ''),
				COALESCE(avatar_url, ''),
				COALESCE(admins_only_write, FALSE),
				COALESCE(hide_members_list, FALSE),
				COALESCE(anonymous_mode, FALSE)
			FROM chats
			WHERE id = $1`

	c := &domain.Chat{}
	err := r.db.QueryRowContext(ctx, query, chatID).Scan(
		&c.ID,
		&c.Type,
		&c.Name,
		&c.CreatedAt,
		&c.OwnerID,
		&c.IsPublic,
		&c.InviteCode,
		&c.AvatarURL,
		&c.AdminsOnlyWrite,
		&c.HideMembersList,
		&c.AnonymousMode,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrChatNotFound
		}
		return nil, fmt.Errorf("failed to get chat by id: %w", err)
	}
	return c, nil
}

func (r *PgChatRepository) GetChatByInviteCode(ctx context.Context, inviteCode string) (*domain.Chat, error) {
	query := `SELECT
			c.id,
			c.type,
			COALESCE(c.name, ''),
			c.created_at,
			COALESCE(c.owner_id, 0),
			COALESCE(c.is_public, TRUE),
			COALESCE(gil.code, ''),
			COALESCE(c.avatar_url, '')
		FROM group_invite_links gil
		JOIN chats c ON c.id = gil.chat_id
		WHERE gil.code = $1
		  AND gil.is_active = TRUE
		  AND c.type = 'group'
		LIMIT 1`

	c := &domain.Chat{}
	err := r.db.QueryRowContext(ctx, query, inviteCode).Scan(
		&c.ID,
		&c.Type,
		&c.Name,
		&c.CreatedAt,
		&c.OwnerID,
		&c.IsPublic,
		&c.InviteCode,
		&c.AvatarURL,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrChatNotFound
		}
		return nil, fmt.Errorf("failed to get chat by invite code: %w", err)
	}
	return c, nil
}

func (r *PgChatRepository) IsMember(ctx context.Context, chatID, userID int64) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM chat_members WHERE chat_id = $1 AND user_id = $2)`,
		chatID, userID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check membership: %w", err)
	}
	return exists, nil
}

func (r *PgChatRepository) IsGroupOwner(ctx context.Context, chatID, userID int64) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM chats
			WHERE id = $1 AND type = 'group' AND owner_id = $2
		)`,
		chatID, userID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check group owner: %w", err)
	}
	return exists, nil
}

func (r *PgChatRepository) AddMember(ctx context.Context, chatID, userID int64) error {
	if _, err := r.db.ExecContext(ctx, `INSERT INTO chat_members (chat_id, user_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`, chatID, userID); err != nil {
		return fmt.Errorf("failed to add member: %w", err)
	}
	return nil
}

func (r *PgChatRepository) updateGroupSettings(ctx context.Context, chatID, userID int64, name string, isPublic bool) error {
	canManage, err := r.IsGroupAdminOrOwner(ctx, chatID, userID)
	if err != nil {
		return err
	}
	if !canManage {
		return domain.ErrNotGroupOwner
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE chats
		SET name = $1, is_public = $2
		WHERE id = $3 AND type = 'group'
	`, name, isPublic, chatID)
	if err != nil {
		return fmt.Errorf("failed to update group settings: %w", err)
	}
	return nil
}

func (r *PgChatRepository) updateGroupAvatar(ctx context.Context, chatID, userID int64, avatarURL string) error {
	canManage, err := r.IsGroupAdminOrOwner(ctx, chatID, userID)
	if err != nil {
		return err
	}
	if !canManage {
		return domain.ErrNotGroupOwner
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE chats
		SET avatar_url = $1
		WHERE id = $2 AND type = 'group'
	`, avatarURL, chatID)
	if err != nil {
		return fmt.Errorf("failed to update group avatar: %w", err)
	}
	return nil
}

func (r *PgChatRepository) regenerateGroupInviteCode(ctx context.Context, chatID, ownerID int64) (string, error) {
	invite, err := r.createGroupInviteLink(ctx, chatID, ownerID)
	if err != nil {
		return "", err
	}

	_, err = r.db.ExecContext(ctx, `
		UPDATE chats
		SET invite_code = $1
		WHERE id = $2 AND type = 'group' AND owner_id = $3
	`, invite.Code, chatID, ownerID)
	if err != nil {
		return "", err
	}
	return invite.Code, nil
}

func (r *PgChatRepository) createGroupInviteLink(ctx context.Context, chatID, userID int64) (*domain.GroupInviteLink, error) {
	canManage, err := r.IsGroupAdminOrOwner(ctx, chatID, userID)
	if err != nil {
		return nil, err
	}
	if !canManage {
		return nil, domain.ErrNotGroupOwner
	}

	const maxAttempts = 8
	for range maxAttempts {
		code, err := newInviteCode()
		if err != nil {
			return nil, fmt.Errorf("failed to generate invite code: %w", err)
		}

		invite := &domain.GroupInviteLink{}
		err = r.db.QueryRowContext(ctx, `
			INSERT INTO group_invite_links (chat_id, code, created_by, is_active)
			VALUES ($1, $2, $3, TRUE)
 ON CONFLICT (code) DO NOTHING
			RETURNING id, chat_id, code, COALESCE(created_by, 0), is_active, created_at, deactivated_at
		`, chatID, code, userID).Scan(
			&invite.ID,
			&invite.ChatID,
			&invite.Code,
			&invite.CreatedBy,
			&invite.IsActive,
			&invite.CreatedAt,
			&invite.DeactivatedAt,
		)
		if err == nil {
			return invite, nil
		}
		if err == sql.ErrNoRows {
			continue
		}
		return nil, fmt.Errorf("failed to create group invite link: %w", err)
	}

	return nil, fmt.Errorf("failed to create group invite link after retries")
}

func (r *PgChatRepository) ListGroupInviteLinks(ctx context.Context, chatID int64) ([]*domain.GroupInviteLink, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			id,
			chat_id,
			code,
			COALESCE(created_by, 0),
			is_active,
			created_at,
			deactivated_at
		FROM group_invite_links
		WHERE chat_id = $1
		ORDER BY created_at DESC
	`, chatID)
	if err != nil {
		return nil, fmt.Errorf("failed to list group invite links: %w", err)
	}
	defer rows.Close()

	var invites []*domain.GroupInviteLink
	for rows.Next() {
		invite := &domain.GroupInviteLink{}
		if err := rows.Scan(
			&invite.ID,
			&invite.ChatID,
			&invite.Code,
			&invite.CreatedBy,
			&invite.IsActive,
			&invite.CreatedAt,
			&invite.DeactivatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		invites = append(invites, invite)
	}
	return invites, rows.Err()
}

func (r *PgChatRepository) setGroupInviteActive(ctx context.Context, chatID, inviteID, userID int64, isActive bool) error {
	canManage, err := r.IsGroupAdminOrOwner(ctx, chatID, userID)
	if err != nil {
		return err
	}
	if !canManage {
		return domain.ErrNotGroupOwner
	}

	res, err := r.db.ExecContext(ctx, `
		UPDATE group_invite_links
		SET
			is_active = $1,
			deactivated_at = CASE WHEN $1 THEN NULL ELSE CURRENT_TIMESTAMP END
		WHERE id = $2 AND chat_id = $3
	`, isActive, inviteID, chatID)
	if err != nil {
		return fmt.Errorf("failed to set group invite status: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return domain.ErrGroupInviteNotFound
	}
	return nil
}

func (r *PgChatRepository) deleteGroupInviteLink(ctx context.Context, chatID, inviteID, userID int64) error {
	canManage, err := r.IsGroupAdminOrOwner(ctx, chatID, userID)
	if err != nil {
		return err
	}
	if !canManage {
		return domain.ErrNotGroupOwner
	}

	res, err := r.db.ExecContext(ctx, `
		DELETE FROM group_invite_links
		WHERE id = $1 AND chat_id = $2
	`, inviteID, chatID)
	if err != nil {
		return fmt.Errorf("failed to delete group invite: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return domain.ErrGroupInviteNotFound
	}
	return nil
}

func (r *PgChatRepository) DeleteGroup(ctx context.Context, chatID, ownerID int64) error {
	res, err := r.db.ExecContext(ctx, `
		DELETE FROM chats
		WHERE id = $1 AND type = 'group' AND owner_id = $2
	`, chatID, ownerID)
	if err != nil {
		return fmt.Errorf("failed to delete group: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return domain.ErrNotGroupOwner
	}
	return nil
}

func (r *PgChatRepository) CreateGroupJoinRequest(ctx context.Context, chatID, userID int64) (*domain.GroupJoinRequest, error) {
	req := &domain.GroupJoinRequest{}
	err := r.db.QueryRowContext(ctx, `
		INSERT INTO group_join_requests (chat_id, user_id, status)
		VALUES ($1, $2, 'pending')
		ON CONFLICT (chat_id, user_id)
		DO UPDATE SET status = 'pending', updated_at = CURRENT_TIMESTAMP
		RETURNING id, chat_id, user_id, status, created_at, updated_at
	`, chatID, userID).Scan(
		&req.ID,
		&req.ChatID,
		&req.UserID,
		&req.Status,
		&req.CreatedAt,
		&req.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create group join request: %w", err)
	}
	return req, nil
}

func (r *PgChatRepository) GetGroupJoinRequest(ctx context.Context, chatID, userID int64) (*domain.GroupJoinRequest, error) {
	req := &domain.GroupJoinRequest{}
	err := r.db.QueryRowContext(ctx, `
		SELECT id, chat_id, user_id, status, created_at, updated_at
		FROM group_join_requests
		WHERE chat_id = $1 AND user_id = $2
	`, chatID, userID).Scan(
		&req.ID,
		&req.ChatID,
		&req.UserID,
		&req.Status,
		&req.CreatedAt,
		&req.UpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get group join request: %w", err)
	}
	return req, nil
}

func (r *PgChatRepository) ListGroupJoinRequests(ctx context.Context, chatID int64) ([]*domain.GroupJoinRequest, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			r.id,
			r.chat_id,
			r.user_id,
			r.status,
			r.created_at,
			r.updated_at,
			u.username,
			COALESCE(u.avatar_url, '')
		FROM group_join_requests r
		JOIN users u ON u.id = r.user_id
		WHERE r.chat_id = $1 AND r.status = 'pending'
		ORDER BY r.created_at ASC
	`, chatID)
	if err != nil {
		return nil, fmt.Errorf("failed to list group join requests: %w", err)
	}
	defer rows.Close()

	var requests []*domain.GroupJoinRequest
	for rows.Next() {
		req := &domain.GroupJoinRequest{}
		if err := rows.Scan(
			&req.ID,
			&req.ChatID,
			&req.UserID,
			&req.Status,
			&req.CreatedAt,
			&req.UpdatedAt,
			&req.Username,
			&req.UserAvatarURL,
		); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		requests = append(requests, req)
	}
	return requests, rows.Err()
}

func (r *PgChatRepository) UpdateGroupJoinRequestStatus(ctx context.Context, chatID, userID int64, status string) error {
	res, err := r.db.ExecContext(ctx, `
		UPDATE group_join_requests
		SET status = $1, updated_at = CURRENT_TIMESTAMP
		WHERE chat_id = $2 AND user_id = $3
	`, status, chatID, userID)
	if err != nil {
		return fmt.Errorf("failed to update group join request status: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return domain.ErrJoinRequestNotFound
	}
	return nil
}

func (r *PgChatRepository) AddGroupAuditLog(ctx context.Context, chatID, actorUserID int64, action string, detailsJSON string) error {
	if strings.TrimSpace(detailsJSON) == "" {
		detailsJSON = "{}"
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO group_audit_logs (chat_id, actor_user_id, action, details_json)
		VALUES ($1, $2, $3, $4::jsonb)
	`, chatID, actorUserID, action, detailsJSON)
	if err != nil {
		return fmt.Errorf("failed to add group audit log: %w", err)
	}
	return nil
}

func (r *PgChatRepository) ListGroupAuditLogs(ctx context.Context, chatID int64, limit int) ([]*domain.GroupAuditLog, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			l.id,
			l.chat_id,
			COALESCE(l.actor_user_id, 0),
			COALESCE(u.username, ''),
			l.action,
			COALESCE(l.details_json::text, '{}'),
			l.created_at
		FROM group_audit_logs l
		LEFT JOIN users u ON u.id = l.actor_user_id
		WHERE l.chat_id = $1
		ORDER BY l.created_at DESC
		LIMIT $2
	`, chatID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to list group audit logs: %w", err)
	}
	defer rows.Close()

	var logs []*domain.GroupAuditLog
	for rows.Next() {
		item := &domain.GroupAuditLog{}
		if err := rows.Scan(
			&item.ID,
			&item.ChatID,
			&item.ActorUserID,
			&item.ActorUsername,
			&item.Action,
			&item.DetailsJSON,
			&item.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		logs = append(logs, item)
	}
	return logs, rows.Err()
}

func (r *PgChatRepository) GetMembersWithInfo(ctx context.Context, chatID int64) ([]*domain.User, error) {
	query := `SELECT u.id, u.username, COALESCE(u.avatar_url, ''), COALESCE(u.bio, '')
		FROM chat_members cm
		JOIN users u ON cm.user_id = u.id
		WHERE cm.chat_id = $1
		ORDER BY u.username`

	rows, err := r.db.QueryContext(ctx, query, chatID)
	if err != nil {
		return nil, fmt.Errorf("failed to get members: %w", err)
	}
	defer rows.Close()

	var users []*domain.User
	for rows.Next() {
		u := &domain.User{}
		if err := rows.Scan(&u.ID, &u.Username, &u.AvatarURL, &u.Bio); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func (r *PgChatRepository) GetMemberRole(ctx context.Context, chatID, userID int64) (string, error) {
	var role string
	err := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(role, 'member') FROM chat_members WHERE chat_id = $1 AND user_id = $2`,
		chatID, userID,
	).Scan(&role)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", fmt.Errorf("failed to get member role: %w", err)
	}
	return role, nil
}

func (r *PgChatRepository) IsGroupAdminOrOwner(ctx context.Context, chatID, userID int64) (bool, error) {
	var exists bool
	err := r.db.QueryRowContext(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM chat_members
			WHERE chat_id = $1 AND user_id = $2 AND role IN ('admin', 'owner')
		)`,
		chatID, userID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check admin/owner: %w", err)
	}
	return exists, nil
}

func (r *PgChatRepository) SetMemberRole(ctx context.Context, chatID, targetUserID int64, role string) error {
	res, err := r.db.ExecContext(ctx,
		`UPDATE chat_members SET role = $1 WHERE chat_id = $2 AND user_id = $3`,
		role, chatID, targetUserID,
	)
	if err != nil {
		return fmt.Errorf("failed to set member role: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("member not found")
	}
	return nil
}

func (r *PgChatRepository) GetMembersWithRoles(ctx context.Context, chatID int64) ([]*domain.ChatMemberWithRole, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT u.id, u.username, COALESCE(u.avatar_url, ''), COALESCE(cm.role, 'member')
		FROM chat_members cm
		JOIN users u ON cm.user_id = u.id
		WHERE cm.chat_id = $1
		ORDER BY
			CASE cm.role WHEN 'owner' THEN 0 WHEN 'admin' THEN 1 ELSE 2 END,
			u.username
	`, chatID)
	if err != nil {
		return nil, fmt.Errorf("failed to get members with roles: %w", err)
	}
	defer rows.Close()

	var members []*domain.ChatMemberWithRole
	for rows.Next() {
		m := &domain.ChatMemberWithRole{}
		if err := rows.Scan(&m.UserID, &m.Username, &m.AvatarURL, &m.Role); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		members = append(members, m)
	}
	return members, rows.Err()
}

func (r *PgChatRepository) GetAdminsOnlyWrite(ctx context.Context, chatID int64) (bool, error) {
	var val bool
	err := r.db.QueryRowContext(ctx,
		`SELECT COALESCE(admins_only_write, FALSE) FROM chats WHERE id = $1`,
		chatID,
	).Scan(&val)
	if err != nil {
		return false, fmt.Errorf("failed to get admins_only_write: %w", err)
	}
	return val, nil
}

func (r *PgChatRepository) SetAdminsOnlyWrite(ctx context.Context, chatID int64, value bool) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE chats SET admins_only_write = $1 WHERE id = $2 AND type = 'group'`,
		value, chatID,
	)
	if err != nil {
		return fmt.Errorf("failed to set admins_only_write: %w", err)
	}
	return nil
}

func (r *PgChatRepository) SetGroupPermissions(ctx context.Context, chatID int64, adminsOnlyWrite, hideMembersList, anonymousMode *bool) error {
	assignments := make([]string, 0, 3)
	args := make([]any, 0, 4)

	if adminsOnlyWrite != nil {
		assignments = append(assignments, fmt.Sprintf("admins_only_write = $%d", len(args)+1))
		args = append(args, *adminsOnlyWrite)
	}
	if hideMembersList != nil {
		assignments = append(assignments, fmt.Sprintf("hide_members_list = $%d", len(args)+1))
		args = append(args, *hideMembersList)
	}
	if anonymousMode != nil {
		assignments = append(assignments, fmt.Sprintf("anonymous_mode = $%d", len(args)+1))
		args = append(args, *anonymousMode)
	}
	if len(assignments) == 0 {
		return nil
	}

	args = append(args, chatID)
	query := fmt.Sprintf(
		`UPDATE chats SET %s WHERE id = $%d AND type = 'group'`,
		strings.Join(assignments, ", "),
		len(args),
	)
	if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("failed to set group permissions: %w", err)
	}
	return nil
}

func (r *PgChatRepository) RemoveMember(ctx context.Context, chatID, userID int64) error {
	res, err := r.db.ExecContext(ctx,
		`DELETE FROM chat_members WHERE chat_id = $1 AND user_id = $2`,
		chatID, userID,
	)
	if err != nil {
		return fmt.Errorf("failed to remove member: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to read rows affected: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("member not found")
	}
	return nil
}

func (r *PgChatRepository) GetMemberIDs(ctx context.Context, chatID int64) ([]int64, error) {
	rows, err := r.db.QueryContext(ctx,
		`SELECT user_id FROM chat_members WHERE chat_id = $1 ORDER BY user_id`,
		chatID,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get member ids: %w", err)
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
