package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

type E2EEMessageRepository interface {
	Save(ctx context.Context, msg *domain.EncryptedMessage) error
	GetUndelivered(ctx context.Context, recipientID string, recipientDeviceID int) ([]*domain.EncryptedMessage, error)
	Delete(ctx context.Context, messageID string) error
	DeleteExpired(ctx context.Context, before time.Time) (int64, error)
}

type PgE2EEMessageRepository struct {
	db *sql.DB
}

func NewE2EEPgMessageRepository(db *sql.DB) *PgE2EEMessageRepository {
	return &PgE2EEMessageRepository{db: db}
}

func (r *PgE2EEMessageRepository) Save(ctx context.Context, msg *domain.EncryptedMessage) error {
	query := `INSERT INTO encrypted_messages 
		(message_id, sender_id, sender_device_id, recipient_id, recipient_device_id, 
		 ciphertext, message_type, pre_key_id, signed_pre_key_id, kyber_pre_key_id, timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (message_id) DO NOTHING`

	_, err := r.db.ExecContext(ctx, query,
		msg.MessageID,
		msg.SenderID,
		msg.SenderDeviceID,
		msg.RecipientID,
		msg.RecipientDeviceID,
		msg.Ciphertext,
		msg.MessageType,
		msg.PreKeyID,
		msg.SignedPreKeyID,
		msg.KyberPreKeyID,
		msg.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to save encrypted message: %w", err)
	}
	return nil
}

func (r *PgE2EEMessageRepository) GetUndelivered(ctx context.Context, recipientID string, recipientDeviceID int) ([]*domain.EncryptedMessage, error) {
	query := `SELECT message_id, sender_id, sender_device_id, recipient_id, recipient_device_id,
		ciphertext, message_type, pre_key_id, signed_pre_key_id, kyber_pre_key_id, timestamp, created_at
		FROM encrypted_messages
		WHERE recipient_id = $1 AND recipient_device_id = $2
		ORDER BY created_at ASC`

	rows, err := r.db.QueryContext(ctx, query, recipientID, recipientDeviceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get undelivered messages: %w", err)
	}
	defer rows.Close()

	var messages []*domain.EncryptedMessage
	for rows.Next() {
		msg := &domain.EncryptedMessage{}
		err := rows.Scan(
			&msg.MessageID,
			&msg.SenderID,
			&msg.SenderDeviceID,
			&msg.RecipientID,
			&msg.RecipientDeviceID,
			&msg.Ciphertext,
			&msg.MessageType,
			&msg.PreKeyID,
			&msg.SignedPreKeyID,
			&msg.KyberPreKeyID,
			&msg.Timestamp,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		messages = append(messages, msg)
	}

	return messages, rows.Err()
}

func (r *PgE2EEMessageRepository) Delete(ctx context.Context, messageID string) error {
	query := `DELETE FROM encrypted_messages WHERE message_id = $1`
	_, err := r.db.ExecContext(ctx, query, messageID)
	if err != nil {
		return fmt.Errorf("failed to delete message: %w", err)
	}
	return nil
}

func (r *PgE2EEMessageRepository) DeleteExpired(ctx context.Context, before time.Time) (int64, error) {
	query := `DELETE FROM encrypted_messages WHERE expires_at < $1`
	result, err := r.db.ExecContext(ctx, query, before)
	if err != nil {
		return 0, fmt.Errorf("failed to delete expired messages: %w", err)
	}
	n, _ := result.RowsAffected()
	return n, nil
}
