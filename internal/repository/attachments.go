package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

type AttachmentRepository interface {
	CreateAuthorized(context.Context, *domain.Attachment) error
	GetForUser(context.Context, string, int64) (*domain.Attachment, error)
}

type PgAttachmentRepository struct{ db *sql.DB }

func NewPgAttachmentRepository(db *sql.DB) *PgAttachmentRepository {
	return &PgAttachmentRepository{db: db}
}

func (r *PgAttachmentRepository) CreateAuthorized(ctx context.Context, a *domain.Attachment) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	return inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		if _, err := lockChatWriter(ctx, tx, a.ChatID, a.OwnerID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO attachments(path,chat_id,owner_id,name,mime_type,kind,size) VALUES($1,$2,$3,$4,$5,$6,$7)`, a.Path, a.ChatID, a.OwnerID, a.Name, a.MIME, a.Kind, a.Size)
		return err
	})
}

// A single statement checks current membership. Public groups still require membership.
func (r *PgAttachmentRepository) GetForUser(ctx context.Context, path string, userID int64) (*domain.Attachment, error) {
	a := &domain.Attachment{}
	err := r.db.QueryRowContext(ctx, `SELECT a.path,a.chat_id,a.owner_id,a.name,a.mime_type,a.kind,a.size
	 FROM attachments a JOIN chat_members m ON m.chat_id=a.chat_id AND m.user_id=$2 WHERE a.path=$1`, path, userID).Scan(&a.Path, &a.ChatID, &a.OwnerID, &a.Name, &a.MIME, &a.Kind, &a.Size)
	if err == sql.ErrNoRows {
		return nil, domain.ErrMessageNotFound
	}
	return a, err
}
