package repository

import (
	"bytes"
	"context"
	"crypto/subtle"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/domain"
)

type PgE2EEDeviceRepository struct{ db *sql.DB }

func NewPgE2EEDeviceRepository(db *sql.DB) *PgE2EEDeviceRepository {
	return &PgE2EEDeviceRepository{db: db}
}

const e2eeDeviceColumns = `d.user_id,u.user_uuid,d.generation_id::text,d.device_id,d.registration_id,
    d.identity_key,d.credential_hash,d.public_material_ready,d.public_material_revision,d.retired_at`

func scanE2EEDevice(row interface{ Scan(...any) error }) (*domain.E2EEDevice, error) {
	d := new(domain.E2EEDevice)
	err := row.Scan(&d.UserID, &d.UserUUID, &d.GenerationID, &d.DeviceID, &d.RegistrationID,
		&d.IdentityKey, &d.CredentialHash, &d.PublicMaterialReady, &d.PublicMaterialRevision, &d.RetiredAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrDeviceNotFound
	}
	if err != nil {
		return nil, err
	}
	return d, nil
}

func (r *PgE2EEDeviceRepository) Own(ctx context.Context, userID int64) (string, *domain.E2EEDevice, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var account string
	if err := r.db.QueryRowContext(ctx, `SELECT user_uuid FROM users WHERE id=$1`, userID).Scan(&account); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil, domain.ErrUserNotFound
		}
		return "", nil, err
	}
	d, err := scanE2EEDevice(r.db.QueryRowContext(ctx, `SELECT `+e2eeDeviceColumns+`
        FROM e2ee_devices d JOIN users u ON u.id=d.user_id WHERE d.user_id=$1 AND d.retired_at IS NULL`, userID))
	if errors.Is(err, domain.ErrDeviceNotFound) {
		return account, nil, nil
	}
	return account, d, err
}

func (r *PgE2EEDeviceRepository) Register(ctx context.Context, userID int64, req domain.E2EEDeviceRegistration) (*domain.E2EEDeviceChange, error) {
	if req.ExpectedGenerationID != "" {
		return nil, domain.ErrInvalidInput
	}
	return r.change(ctx, userID, req, nil)
}
func (r *PgE2EEDeviceRepository) Replace(ctx context.Context, userID int64, req domain.E2EEDeviceRegistration, verified domain.E2EEReauthentication) (*domain.E2EEDeviceChange, error) {
	if req.ExpectedGenerationID == "" {
		return nil, domain.ErrInvalidInput
	}
	return r.change(ctx, userID, req, &verified)
}

func (r *PgE2EEDeviceRepository) change(parent context.Context, userID int64, req domain.E2EEDeviceRegistration, verified *domain.E2EEReauthentication) (*domain.E2EEDeviceChange, error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var account, passwordHash string
	var tokenVersion int
	// Serializes the no-device case too; locking only an active device cannot.
	err = tx.QueryRowContext(ctx, `SELECT user_uuid,password_hash,token_version FROM users WHERE id=$1 FOR UPDATE`, userID).Scan(&account, &passwordHash, &tokenVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrUserNotFound
	}
	if err != nil {
		return nil, err
	}
	if verified != nil && (passwordHash != verified.PasswordHash || tokenVersion != verified.TokenVersion) {
		return nil, domain.ErrInvalidCredentials
	}
	operation := "register"
	if verified != nil {
		operation = "replace"
	}
	var storedOperation, resultGeneration string
	var expected sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT operation,expected_generation_id::text,result_generation_id::text
        FROM e2ee_device_requests WHERE user_id=$1 AND request_id=$2`, userID, req.RequestID).Scan(&storedOperation, &expected, &resultGeneration)
	if err == nil {
		old, err := scanE2EEDevice(tx.QueryRowContext(ctx, `SELECT `+e2eeDeviceColumns+` FROM e2ee_devices d
            JOIN users u ON u.id=d.user_id WHERE d.user_id=$1 AND d.generation_id=$2`, userID, resultGeneration))
		if err != nil {
			return nil, err
		}
		if storedOperation != operation || expected.String != req.ExpectedGenerationID || old.RegistrationID != req.RegistrationID ||
			!bytes.Equal(old.IdentityKey, req.IdentityKey) || subtle.ConstantTimeCompare(old.CredentialHash, req.CredentialHash) != 1 {
			return nil, domain.ErrE2EERequestConflict
		}
		// An old replay must never reactivate a retired generation or return a
		// newer device to an installation that does not possess its credential.
		if old.RetiredAt != nil {
			return nil, domain.ErrE2EEGenerationChanged
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return &domain.E2EEDeviceChange{Device: old, Replayed: true, PreviousGenerationID: expected.String}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var active string
	var activeVerifier []byte
	err = tx.QueryRowContext(ctx, `SELECT generation_id::text,credential_hash FROM e2ee_devices WHERE user_id=$1 AND retired_at IS NULL`, userID).Scan(&active, &activeVerifier)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if operation == "register" && active != "" {
		return nil, domain.ErrE2EEDeviceConflict
	}
	if operation == "replace" && (active == "" || active != req.ExpectedGenerationID) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if operation == "replace" && subtle.ConstantTimeCompare(activeVerifier, req.CredentialHash) == 1 {
		// A new generation alone cannot revoke possession of the old secret.
		return nil, domain.ErrInvalidInput
	}
	if active != "" {
		result, err := tx.ExecContext(ctx, `UPDATE e2ee_devices SET retired_at=clock_timestamp(),public_material_ready=FALSE WHERE generation_id=$1 AND retired_at IS NULL`, active)
		if err != nil {
			return nil, err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return nil, err
		}
		if n != 1 {
			return nil, domain.ErrE2EEGenerationChanged
		}
	}
	generation := uuid.NewString()
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_devices(generation_id,user_id,registration_id,identity_key,credential_hash)
        VALUES($1,$2,$3,$4,$5)`, generation, userID, req.RegistrationID, req.IdentityKey, req.CredentialHash)
	if err != nil {
		return nil, err
	}
	var expectedArg any
	if req.ExpectedGenerationID != "" {
		expectedArg = req.ExpectedGenerationID
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_device_requests(user_id,request_id,operation,expected_generation_id,result_generation_id)
        VALUES($1,$2,$3,$4,$5)`, userID, req.RequestID, operation, expectedArg, generation)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &domain.E2EEDeviceChange{Device: &domain.E2EEDevice{UserID: userID, UserUUID: account, GenerationID: generation, DeviceID: 1,
		RegistrationID: req.RegistrationID, IdentityKey: bytes.Clone(req.IdentityKey), CredentialHash: bytes.Clone(req.CredentialHash)}, PreviousGenerationID: active}, nil
}
