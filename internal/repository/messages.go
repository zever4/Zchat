package repository

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

type MessageRepository interface {
	SaveAuthorized(ctx context.Context, msg *domain.Message) (*domain.Chat, []int64, error)
	Save(ctx context.Context, msg *domain.Message) error
	GetHistory(ctx context.Context, chatID int64, lastID int64, limit int) ([]*domain.Message, error)
	SaveReturningID(ctx context.Context, msg *domain.Message) (int64, error)
	GetByID(ctx context.Context, id int64) (*domain.Message, error)
	UpdateContent(ctx context.Context, messageID, senderID int64, newContent string) error
	SoftDelete(ctx context.Context, messageID, senderID int64) error
}

type PgMessageRepository struct {
	db *sql.DB
}

func NewPgMessageRepository(db *sql.DB) *PgMessageRepository {
	return &PgMessageRepository{db: db}
}

// Saves msg into db. Returns error if one occured. Should just log this error.
func (r *PgMessageRepository) Save(ctx context.Context, msg *domain.Message) error {
	query := `INSERT INTO messages (sender_id, chat_id, content)
	VALUES ($1, $2, $3)`

	_, err := r.db.ExecContext(ctx, query, msg.SenderID, msg.ChatID, msg.Content)

	return err
}

func (r *PgMessageRepository) SaveReturningID(ctx context.Context, msg *domain.Message) (int64, error) {
	query := `INSERT INTO messages (sender_id, chat_id, content)
	VALUES ($1, $2, $3) RETURNING id`

	var id int64
	err := r.db.QueryRowContext(ctx, query, msg.SenderID, msg.ChatID, msg.Content).Scan(&id)
	return id, err
}

func (r *PgMessageRepository) GetByID(ctx context.Context, id int64) (*domain.Message, error) {
	query := `SELECT m.id, m.content, m.created_at, m.sender_id, m.chat_id, u.username, COALESCE(u.avatar_url, '')
		FROM messages m JOIN users u ON m.sender_id = u.id
		WHERE m.id = $1 AND (m.is_deleted IS NULL OR m.is_deleted = false)`
	msg := &domain.Message{}
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&msg.ID, &msg.Content, &msg.CreatedAt, &msg.SenderID, &msg.ChatID, &msg.SenderName, &msg.SenderAvatar)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (r *PgMessageRepository) UpdateContent(ctx context.Context, messageID, senderID int64, newContent string) error {
	return r.mutateOwnMessage(ctx, messageID, senderID, &newContent)
}

func (r *PgMessageRepository) SoftDelete(ctx context.Context, messageID, senderID int64) error {
	return r.mutateOwnMessage(ctx, messageID, senderID, nil)
}

func (r *PgMessageRepository) mutateOwnMessage(ctx context.Context, messageID, senderID int64, newContent *string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		var chatID int64
		err := tx.QueryRowContext(ctx, `SELECT chat_id FROM messages
			WHERE id=$1 AND sender_id=$2 AND is_deleted IS NOT TRUE`, messageID, senderID).Scan(&chatID)
		if err != nil {
			if err == sql.ErrNoRows {
				return domain.ErrMessageNotFound
			}
			return err
		}
		var result sql.Result
		if newContent != nil {
			if _, err := lockChatWriter(ctx, tx, chatID, senderID); err != nil {
				return err
			}
			result, err = tx.ExecContext(ctx, `UPDATE messages SET content=$3, updated_at=NOW()
				WHERE id=$1 AND sender_id=$2 AND is_deleted IS NOT TRUE`, messageID, senderID, *newContent)
		} else {
			// Current members can remove their own messages even in a read-only group.
			if _, _, err := lockChatMember(ctx, tx, chatID, senderID); err != nil {
				return err
			}
			result, err = tx.ExecContext(ctx, `UPDATE messages SET is_deleted=TRUE
				WHERE id=$1 AND sender_id=$2 AND is_deleted IS NOT TRUE`, messageID, senderID)
		}
		if err != nil {
			return err
		}
		rows, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if rows == 0 {
			return domain.ErrMessageNotFound
		}
		return nil
	})
}

func (r *PgMessageRepository) GetHistory(ctx context.Context, chatID int64, lastID int64, limit int) ([]*domain.Message, error) {
	// One additional row is allowed for the service's has_more probe.
	if chatID <= 0 || lastID < 0 || limit < 1 || limit > domain.MaxHistoryLimit+1 {
		return nil, domain.ErrInvalidInput
	}
	query := `SELECT
			m.id,
			m.content,
			m.created_at,
			CASE
				WHEN c.type = 'group' AND (m.anonymous_sender OR COALESCE(c.anonymous_mode, FALSE)) THEN 0
				ELSE m.sender_id
			END AS sender_id,
			CASE
				WHEN c.type = 'group' AND (m.anonymous_sender OR COALESCE(c.anonymous_mode, FALSE)) THEN COALESCE(NULLIF(c.name, ''), 'Группа')
				ELSE u.username
			END AS sender_name,
			CASE
				WHEN c.type = 'group' AND (m.anonymous_sender OR COALESCE(c.anonymous_mode, FALSE)) THEN COALESCE(c.avatar_url, '')
				ELSE COALESCE(u.avatar_url, '')
			END AS sender_avatar,
			c.type
		FROM messages m
		JOIN chats c ON c.id = m.chat_id
		JOIN users u ON m.sender_id = u.id
		WHERE m.chat_id = $1 AND m.is_deleted IS NOT TRUE AND ($2 = 0 OR m.id < $2)
		ORDER BY m.id DESC
		LIMIT $3;`

	rows, err := r.db.QueryContext(ctx, query, chatID, lastID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to get rows: %w", err)
	}
	defer rows.Close()

	var history []*domain.Message
	for rows.Next() {
		msg := &domain.Message{ChatID: chatID, Type: domain.MessageTypePrivate}
		err := rows.Scan(&msg.ID, &msg.Content, &msg.CreatedAt, &msg.SenderID, &msg.SenderName, &msg.SenderAvatar, &msg.Type)
		if err != nil {
			return nil, fmt.Errorf("rows.Scan error: %w", err)
		}

		history = append(history, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	slices.Reverse(history)

	return history, nil
}
