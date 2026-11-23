package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/zever4/Zchat/internal/domain"
)

type KyberPreKeyRepository interface {
	BulkInsert(ctx context.Context, userID string, deviceID int, keys []domain.KyberPreKey) error
	Acquire(ctx context.Context, userID string, deviceID int) (*domain.KyberPreKey, error)
	CountAvailable(ctx context.Context, userID string, deviceID int) (int, error)
	DeleteByDevice(ctx context.Context, userID string, deviceID int) error
}

type PgKyberPreKeyRepository struct {
	db *sql.DB
}

func NewPgKyberPreKeyRepository(db *sql.DB) *PgKyberPreKeyRepository {
	return &PgKyberPreKeyRepository{db: db}
}

func (r *PgKyberPreKeyRepository) BulkInsert(ctx context.Context, userID string, deviceID int, keys []domain.KyberPreKey) error {
	query := `INSERT INTO kyber_prekeys (user_id, device_id, key_id, public_key, signature, is_last_resort) VALUES ($1, $2, $3, $4, $5, $6)`

	stmt, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, key := range keys {
		_, err := stmt.ExecContext(ctx, userID, deviceID, key.KeyID, key.PublicKey, key.Signature, key.IsLastResort)
		if err != nil {
			return fmt.Errorf("failed to insert kyber prekey %d: %w", key.KeyID, err)
		}
	}
	return nil
}

func (r *PgKyberPreKeyRepository) Acquire(ctx context.Context, userID string, deviceID int) (*domain.KyberPreKey, error) {
	// Consume one-time keys first; last-resort stays reusable. Selection and
	// update share a row lock and match the complete device-scoped primary key.
	query := `
		WITH selected AS (
			SELECT user_id, device_id, key_id, is_last_resort FROM kyber_prekeys
			WHERE user_id = $1 AND device_id = $2
				AND (used = FALSE OR is_last_resort = TRUE)
			ORDER BY is_last_resort ASC, key_id ASC
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		UPDATE kyber_prekeys AS k
		SET used = CASE WHEN selected.is_last_resort THEN k.used ELSE TRUE END,
		    used_at = CASE WHEN selected.is_last_resort THEN k.used_at ELSE NOW() END
		FROM selected
		WHERE k.user_id = selected.user_id
			AND k.device_id = selected.device_id
			AND k.key_id = selected.key_id
		RETURNING k.user_id, k.device_id, k.key_id,
		          k.public_key, k.signature, k.is_last_resort`

	key := &domain.KyberPreKey{}
	err := r.db.QueryRowContext(ctx, query, userID, deviceID).Scan(
		&key.UserID, &key.DeviceID, &key.KeyID, &key.PublicKey, &key.Signature, &key.IsLastResort,
	)
	if err == sql.ErrNoRows {
		return nil, domain.ErrPreKeyPoolExhausted
	}
	if err != nil {
		return nil, fmt.Errorf("failed to acquire kyber prekey: %w", err)
	}
	return key, nil
}

func (r *PgKyberPreKeyRepository) CountAvailable(ctx context.Context, userID string, deviceID int) (int, error) {
	query := `SELECT COUNT(*) FROM kyber_prekeys WHERE user_id = $1 AND device_id = $2 AND used = FALSE AND is_last_resort = FALSE`

	var count int
	err := r.db.QueryRowContext(ctx, query, userID, deviceID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count kyber prekeys: %w", err)
	}
	return count, nil
}

func (r *PgKyberPreKeyRepository) DeleteByDevice(ctx context.Context, userID string, deviceID int) error {
	query := `DELETE FROM kyber_prekeys WHERE user_id = $1 AND device_id = $2`
	_, err := r.db.ExecContext(ctx, query, userID, deviceID)
	if err != nil {
		return fmt.Errorf("failed to delete kyber prekeys: %w", err)
	}
	return nil
}
