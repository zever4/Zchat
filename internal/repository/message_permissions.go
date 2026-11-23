package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

// SaveAuthorized holds the chat policy and sender membership/role stable until
// INSERT commits. DELETE/role changes and chat policy UPDATEs use the same rows.
// It returns only committed IDs and the server-owned metadata used for authorization.
func (r *PgMessageRepository) SaveAuthorized(ctx context.Context, msg *domain.Message) (*domain.Chat, []int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var chat *domain.Chat
	var recipients []int64
	var id int64
	var createdAt time.Time
	var replayed bool
	digest := sha256.Sum256([]byte(msg.Content))
	// A nil parameter is SQL NULL for older clients without receipt support.
	var clientID, contentHash any
	if msg.ClientMessageID != "" {
		clientID, contentHash = msg.ClientMessageID, digest[:]
	}
	err := inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		var err error
		chat, err = lockChatWriter(ctx, tx, msg.ChatID, msg.SenderID)
		if err != nil {
			return err
		}
		if path := messageAttachmentPath(msg.Content); path != "" {
			var exists bool
			if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM attachments WHERE path=$1 AND chat_id=$2)`, path, msg.ChatID).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return domain.ErrInvalidInput
			}
		}

		bound := &PgChatRepository{db: tx}
		recipients, err = bound.GetMemberIDs(ctx, msg.ChatID)
		if err != nil {
			return err
		}
		anonymous := chat.Type == "group" && chat.AnonymousMode
		err = tx.QueryRowContext(ctx, `INSERT INTO messages(sender_id,chat_id,content,client_message_id,client_content_hash,anonymous_sender)
			VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT (sender_id,client_message_id) DO NOTHING RETURNING id,created_at`,
			msg.SenderID, msg.ChatID, msg.Content, clientID, contentHash, anonymous).Scan(&id, &createdAt)
		if err == sql.ErrNoRows && clientID != nil {
			// READ COMMITTED's next statement sees a concurrent winner after its
			// commit. Do not UPDATE/rebroadcast an edited or deleted message.
			var storedChat int64
			var storedHash []byte
			err = tx.QueryRowContext(ctx, `SELECT id,created_at,chat_id,client_content_hash FROM messages
				WHERE sender_id=$1 AND client_message_id=$2`, msg.SenderID, clientID).Scan(&id, &createdAt, &storedChat, &storedHash)
			if err == nil && (storedChat != msg.ChatID || !bytes.Equal(storedHash, digest[:])) {
				return domain.ErrMessageIDConflict
			}
			replayed = err == nil
		}
		return err
	})
	if err != nil {
		return nil, nil, err
	}
	msg.ID, msg.CreatedAt = id, createdAt
	msg.Replayed = replayed
	return chat, recipients, nil
}

func lockChatWriter(ctx context.Context, tx *sql.Tx, chatID, userID int64) (*domain.Chat, error) {
	chat, role, err := lockChatMember(ctx, tx, chatID, userID)
	if err != nil {
		return nil, err
	}
	if chat.Type == "group" && chat.AdminsOnlyWrite && role != "owner" && role != "admin" {
		return nil, domain.ErrChatWriteForbidden
	}
	return chat, nil
}

// Always lock chat before membership, matching the order of group mutations.
func lockChatMember(ctx context.Context, tx *sql.Tx, chatID, userID int64) (*domain.Chat, string, error) {
	var id int64
	err := tx.QueryRowContext(ctx, `SELECT id FROM chats WHERE id=$1 FOR SHARE`, chatID).Scan(&id)
	if err == sql.ErrNoRows {
		return nil, "", domain.ErrChatNotFound
	}
	if err != nil {
		return nil, "", err
	}
	chat, err := (&PgChatRepository{db: tx}).GetChatByID(ctx, chatID)
	if err != nil {
		return nil, "", err
	}
	var role string
	err = tx.QueryRowContext(ctx, `SELECT role FROM chat_members WHERE chat_id=$1 AND user_id=$2 FOR SHARE`, chatID, userID).Scan(&role)
	if err == sql.ErrNoRows {
		return nil, "", domain.ErrNotChatMember
	}
	if err != nil {
		return nil, "", err
	}
	return chat, role, nil
}

var attachmentTag = regexp.MustCompile(`^\[(img|gif|voice)\](.*)\[/(img|gif|voice)\]$|^\[file:[^:]*:[0-9]+\](.*)\[/file\]$`)

func messageAttachmentPath(content string) string {
	m := attachmentTag.FindStringSubmatch(html.UnescapeString(content))
	if m == nil {
		return ""
	}
	path := m[4]
	if m[1] != "" && m[1] == m[3] {
		path = m[2]
	}
	if strings.HasPrefix(path, "/uploads/") {
		return path
	}
	return ""
}
