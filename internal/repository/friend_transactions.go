package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/zever4/Zchat/internal/domain"
)

// sqlExecutor binds repository reads and writes to either a pool or one transaction.
type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Nested repository operations join the caller's transaction and never commit it.
func inSQLTransaction(ctx context.Context, db sqlExecutor, work func(*sql.Tx) error) error {
	if tx, ok := db.(*sql.Tx); ok {
		return work(tx)
	}
	pool, ok := db.(*sql.DB)
	if !ok {
		return fmt.Errorf("unsupported SQL executor")
	}
	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = work(tx); err != nil {
		return err
	}
	return tx.Commit()
}

type FriendTransactor interface {
	WithPair(context.Context, int64, int64, func(FriendRequestRepository, ChatRepository, UserRepository) error) error
}

type PgFriendTransactor struct{ db *sql.DB }

func NewPgFriendTransactor(db *sql.DB) *PgFriendTransactor { return &PgFriendTransactor{db: db} }

func (r *PgFriendTransactor) WithPair(ctx context.Context, first, second int64, work func(FriendRequestRepository, ChatRepository, UserRepository) error) error {
	return inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		// Stable lock order also handles opposite-direction requests and overlapping pairs.
		rows, err := tx.QueryContext(ctx, `SELECT id FROM users WHERE id IN ($1,$2) ORDER BY id FOR UPDATE`, first, second)
		if err != nil {
			return err
		}
		count := 0
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			count++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if count != 2 {
			return domain.ErrUserNotFound
		}
		// Settings writers lock the same rows. Keep privacy checks valid through commit.
		for _, id := range orderedPair(first, second) {
			if _, err = tx.ExecContext(ctx, `INSERT INTO user_settings(user_id) VALUES($1) ON CONFLICT(user_id) DO NOTHING`, id); err != nil {
				return err
			}
			var locked int64
			if err = tx.QueryRowContext(ctx, `SELECT user_id FROM user_settings WHERE user_id=$1 FOR UPDATE`, id).Scan(&locked); err != nil {
				return err
			}
		}
		return work(&PgFriendRequestRepository{db: tx}, &PgChatRepository{db: tx}, &PgUserRepository{db: tx})
	})
}

func orderedPair(a, b int64) [2]int64 {
	if a > b {
		return [2]int64{b, a}
	}
	return [2]int64{a, b}
}
