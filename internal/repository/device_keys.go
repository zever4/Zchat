package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/zever4/Zchat/internal/domain"
)

type DeviceKeyRepository interface {
	Upsert(ctx context.Context, dk *domain.DeviceKeys) error
	Get(ctx context.Context, userID string, deviceID int) (*domain.DeviceKeys, error)
	GetAllDevices(ctx context.Context, userID string) ([]*domain.DeviceKeys, error)
	DeleteDevice(ctx context.Context, userID string, deviceID int) error
}

type PgDeviceKeyRepository struct {
	db *sql.DB
}

func NewPgDeviceKeyRepository(db *sql.DB) *PgDeviceKeyRepository {
	return &PgDeviceKeyRepository{db: db}
}

func (r *PgDeviceKeyRepository) Upsert(ctx context.Context, dk *domain.DeviceKeys) error {
	query := `
		INSERT INTO device_keys 
			(user_id, device_id, registration_id, identity_key, signed_pre_key_id, 
			 signed_pre_key_public, signed_pre_key_signature, signed_pre_key_timestamp)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (user_id, device_id) DO UPDATE SET
			registration_id = EXCLUDED.registration_id,
			identity_key = EXCLUDED.identity_key,
			signed_pre_key_id = EXCLUDED.signed_pre_key_id,
			signed_pre_key_public = EXCLUDED.signed_pre_key_public,
			signed_pre_key_signature = EXCLUDED.signed_pre_key_signature,
			signed_pre_key_timestamp = EXCLUDED.signed_pre_key_timestamp,
			created_at = NOW()`

	_, err := r.db.ExecContext(ctx, query,
		dk.UserID, dk.DeviceID, dk.RegistrationID, dk.IdentityKey,
		dk.SignedPreKeyID, dk.SignedPreKeyPublic, dk.SignedPreKeySignature, dk.SignedPreKeyTimestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert device keys: %w", err)
	}
	return nil
}

func (r *PgDeviceKeyRepository) Get(ctx context.Context, userID string, deviceID int) (*domain.DeviceKeys, error) {
	query := `
		SELECT user_id, device_id, registration_id, identity_key, signed_pre_key_id,
		       signed_pre_key_public, signed_pre_key_signature, signed_pre_key_timestamp, created_at
		FROM device_keys
		WHERE user_id = $1 AND device_id = $2`

	dk := &domain.DeviceKeys{}
	err := r.db.QueryRowContext(ctx, query, userID, deviceID).Scan(
		&dk.UserID, &dk.DeviceID, &dk.RegistrationID, &dk.IdentityKey,
		&dk.SignedPreKeyID, &dk.SignedPreKeyPublic, &dk.SignedPreKeySignature,
		&dk.SignedPreKeyTimestamp, &dk.CreatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, domain.ErrDeviceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get device keys: %w", err)
	}
	return dk, nil
}

func (r *PgDeviceKeyRepository) GetAllDevices(ctx context.Context, userID string) ([]*domain.DeviceKeys, error) {
	query := `
		SELECT user_id, device_id, registration_id, identity_key, signed_pre_key_id,
		       signed_pre_key_public, signed_pre_key_signature, signed_pre_key_timestamp, created_at
		FROM device_keys
		WHERE user_id = $1`

	rows, err := r.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get devices: %w", err)
	}
	defer rows.Close()

	var devices []*domain.DeviceKeys
	for rows.Next() {
		dk := &domain.DeviceKeys{}
		err := rows.Scan(
			&dk.UserID, &dk.DeviceID, &dk.RegistrationID, &dk.IdentityKey,
			&dk.SignedPreKeyID, &dk.SignedPreKeyPublic, &dk.SignedPreKeySignature,
			&dk.SignedPreKeyTimestamp, &dk.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		devices = append(devices, dk)
	}
	return devices, rows.Err()
}

func (r *PgDeviceKeyRepository) DeleteDevice(ctx context.Context, userID string, deviceID int) error {
	query := `DELETE FROM device_keys WHERE user_id = $1 AND device_id = $2`
	_, err := r.db.ExecContext(ctx, query, userID, deviceID)
	if err != nil {
		return fmt.Errorf("failed to delete device: %w", err)
	}
	return nil
}
