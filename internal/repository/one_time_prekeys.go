package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/zever4/Zchat/internal/domain"
)

type OneTimePreKeyRepository interface {
	BulkInsert(ctx context.Context, userID string, deviceID int, keys []domain.OneTimePreKey) error
	Acquire(ctx context.Context, userID string, deviceID int) (*domain.OneTimePreKey, error)
	CountAvailable(ctx context.Context, userID string, deviceID int) (int, error)
	DeleteByDevice(ctx context.Context, userID string, deviceID int) error
}

type PgOneTimePreKeyRepository struct {
	db *sql.DB
}

func NewPgOneTimePreKeyRepository(db *sql.DB) *PgOneTimePreKeyRepository {
	return &PgOneTimePreKeyRepository{db: db}
}

func (r *PgOneTimePreKeyRepository) BulkInsert(ctx context.Context, userID string, deviceID int, keys []domain.OneTimePreKey) error {
	query := `INSERT INTO one_time_prekeys (user_id, device_id, key_id, public_key) VALUES ($1, $2, $3, $4)`

	stmt, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, key := range keys {
		_, err := stmt.ExecContext(ctx, userID, deviceID, key.KeyID, key.PublicKey)
		if err != nil {
			return fmt.Errorf("failed to insert prekey %d: %w", key.KeyID, err)
		}
	}
	return nil
}

func (r *PgOneTimePreKeyRepository) Acquire(ctx context.Context, userID string, deviceID int) (*domain.OneTimePreKey, error) {
	// Select and consume under one row lock. key_id alone is not unique across
	// users/devices; join every component of the primary key when updating.
	query := `
		WITH selected AS (
			SELECT user_id, device_id, key_id FROM one_time_prekeys
			WHERE user_id = $1 AND device_id = $2 AND used = FALSE
			ORDER BY key_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE one_time_prekeys AS k
		SET used = TRUE, used_at = NOW()
		FROM selected
		WHERE k.user_id = selected.user_id
			AND k.device_id = selected.device_id
			AND k.key_id = selected.key_id
		RETURNING k.user_id, k.device_id, k.key_id, k.public_key`

	key := &domain.OneTimePreKey{}
	err := r.db.QueryRowContext(ctx, query, userID, deviceID).Scan(
		&key.UserID, &key.DeviceID, &key.KeyID, &key.PublicKey,
	)
	if err == sql.ErrNoRows {
		return nil, domain.ErrPreKeyPoolExhausted
	}
	if err != nil {
		return nil, fmt.Errorf("failed to acquire prekey: %w", err)
	}
	return key, nil
}

func (r *PgOneTimePreKeyRepository) CountAvailable(ctx context.Context, userID string, deviceID int) (int, error) {
	query := `SELECT COUNT(*) FROM one_time_prekeys WHERE user_id = $1 AND device_id = $2 AND used = FALSE`

	var count int
	err := r.db.QueryRowContext(ctx, query, userID, deviceID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count prekeys: %w", err)
	}
	return count, nil
}

func (r *PgOneTimePreKeyRepository) DeleteByDevice(ctx context.Context, userID string, deviceID int) error {
	query := `DELETE FROM one_time_prekeys WHERE user_id = $1 AND device_id = $2`
	_, err := r.db.ExecContext(ctx, query, userID, deviceID)
	if err != nil {
		return fmt.Errorf("failed to delete prekeys: %w", err)
	}
	return nil
}
