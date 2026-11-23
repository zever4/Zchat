package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

// Account, settings and its first session either all commit or all roll back.
func (r *PgUserRepository) RegisterWithSession(ctx context.Context, user *domain.User, sessionID string, expires time.Time) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		users := &PgUserRepository{db: tx}
		id, err := users.Create(ctx, user)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO auth_sessions(id,user_id,token_version,expires_at) VALUES($1,$2,1,$3)`, sessionID, id, expires)
		return err
	})
	if err != nil {
		return 0, err
	}
	return user.ID, nil
}

// The users row serializes issuance with password/version changes across DB clients.
// A credential snapshot verified before a rotation cannot create a valid session after it.
func (r *PgUserRepository) CreateSession(ctx context.Context, userID int64, passwordHash string, version int, sessionID string, expires time.Time) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		var currentHash string
		var currentVersion int
		if err := tx.QueryRowContext(ctx, `SELECT password_hash,token_version FROM users WHERE id=$1 FOR UPDATE`, userID).Scan(&currentHash, &currentVersion); err != nil {
			return err
		}
		if currentHash != passwordHash || currentVersion != version {
			return domain.ErrInvalidCredentials
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM auth_sessions WHERE user_id=$1 AND expires_at<=now()`, userID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO auth_sessions(id,user_id,token_version,expires_at) VALUES($1,$2,$3,$4)`, sessionID, userID, version, expires)
		return err
	})
}

func (r *PgUserRepository) GetSessionVersion(ctx context.Context, userID int64, sessionID string) (int, error) {
	var version int
	err := r.db.QueryRowContext(ctx, `SELECT u.token_version FROM users u JOIN auth_sessions s ON s.user_id=u.id
 WHERE u.id=$1 AND s.id=$2 AND s.token_version=u.token_version AND s.expires_at>now()`, userID, sessionID).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, domain.ErrInvalidSession
	}
	return version, err
}

func (r *PgUserRepository) DeleteSession(ctx context.Context, userID int64, sessionID string) error {
	res, err := r.db.ExecContext(ctx, `DELETE FROM auth_sessions WHERE user_id=$1 AND id=$2`, userID, sessionID)
	return sessionMutationResult(res, err)
}

func (r *PgUserRepository) RevokeAllSessions(ctx context.Context, userID int64, version int) error {
	return r.rotateCredentials(ctx, userID, version, "", "")
}

func (r *PgUserRepository) ChangePasswordAndRevoke(ctx context.Context, userID int64, version int, oldHash, newHash string) error {
	return r.rotateCredentials(ctx, userID, version, oldHash, newHash)
}

func (r *PgUserRepository) rotateCredentials(ctx context.Context, userID int64, version int, oldHash, newHash string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		var res sql.Result
		var err error
		if newHash == "" {
			res, err = tx.ExecContext(ctx, `UPDATE users SET token_version=token_version+1 WHERE id=$1 AND token_version=$2`, userID, version)
		} else {
			res, err = tx.ExecContext(ctx, `UPDATE users SET password_hash=$3,token_version=token_version+1 WHERE id=$1 AND token_version=$2 AND password_hash=$4`, userID, version, newHash, oldHash)
		}
		if err := sessionMutationResult(res, err); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM auth_sessions WHERE user_id=$1`, userID)
		return err
	})
}

func sessionMutationResult(res sql.Result, err error) error {
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return domain.ErrInvalidSession
	}
	return nil
}

// Bounded maintenance, independent of whether an expired account logs in again.
func (r *PgUserRepository) CleanupExpiredSessions(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM auth_sessions WHERE id IN (SELECT id FROM auth_sessions WHERE expires_at<=now() ORDER BY expires_at LIMIT 1000)`)
	return err
}
