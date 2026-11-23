package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/zever4/Zchat/internal/domain"
)

type PgFriendRequestRepository struct {
	db sqlExecutor
}

func NewPgFriendRequestRepository(db *sql.DB) *PgFriendRequestRepository {
	return &PgFriendRequestRepository{db: db}
}

func (r *PgFriendRequestRepository) Create(ctx context.Context, req *domain.FriendRequest) (int64, error) {
	query := `INSERT INTO friend_requests (from_user_id, to_user_id, status)
		VALUES ($1, $2, $3)
		RETURNING id`

	err := r.db.QueryRowContext(ctx, query, req.FromUserID, req.ToUserID, req.Status).Scan(&req.ID)
	if err != nil {
		return 0, fmt.Errorf("failed to create friend request: %w", err)
	}
	return req.ID, nil
}

func (r *PgFriendRequestRepository) GetByID(ctx context.Context, id int64) (*domain.FriendRequest, error) {
	query := `SELECT fr.id, fr.from_user_id, fr.to_user_id, fr.status, fr.created_at,
			u1.username, u2.username
		FROM friend_requests fr
		JOIN users u1 ON fr.from_user_id = u1.id
		JOIN users u2 ON fr.to_user_id = u2.id
		WHERE fr.id = $1`

	req := &domain.FriendRequest{}
	err := r.db.QueryRowContext(ctx, query, id).Scan(
		&req.ID, &req.FromUserID, &req.ToUserID, &req.Status, &req.CreatedAt,
		&req.FromUsername, &req.ToUsername,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrFriendRequestNotFound
		}
		return nil, fmt.Errorf("failed to get friend request: %w", err)
	}
	return req, nil
}

func (r *PgFriendRequestRepository) UpdateStatus(ctx context.Context, id int64, status string) error {
	query := `UPDATE friend_requests SET status = $1 WHERE id = $2`
	res, err := r.db.ExecContext(ctx, query, status, id)
	if err != nil {
		return fmt.Errorf("failed to update friend request: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return domain.ErrFriendRequestNotFound
	}
	return nil
}

func (r *PgFriendRequestRepository) GetPendingForUser(ctx context.Context, userID int64) ([]*domain.FriendRequest, error) {
	query := `SELECT fr.id, fr.from_user_id, fr.to_user_id, fr.status, fr.created_at,
			u.username as from_username
		FROM friend_requests fr
		JOIN users u ON fr.from_user_id = u.id
		WHERE fr.to_user_id = $1 AND fr.status = 'pending'
		ORDER BY fr.created_at DESC`

	rows, err := r.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get pending requests: %w", err)
	}
	defer rows.Close()

	var requests []*domain.FriendRequest
	for rows.Next() {
		req := &domain.FriendRequest{}
		if err := rows.Scan(&req.ID, &req.FromUserID, &req.ToUserID, &req.Status, &req.CreatedAt, &req.FromUsername); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		requests = append(requests, req)
	}
	return requests, rows.Err()
}

func (r *PgFriendRequestRepository) AreFriends(ctx context.Context, userID1, userID2 int64) (bool, error) {
	query := `SELECT COUNT(*) FROM friend_requests
		WHERE status = 'accepted'
		AND ((from_user_id = $1 AND to_user_id = $2)
		  OR (from_user_id = $2 AND to_user_id = $1))`

	var count int
	err := r.db.QueryRowContext(ctx, query, userID1, userID2).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check friendship: %w", err)
	}
	return count > 0, nil
}

func (r *PgFriendRequestRepository) GetFriends(ctx context.Context, userID int64) ([]*domain.User, error) {
	query := `SELECT u.id, u.user_uuid, u.username,
		CASE WHEN COALESCE(s.show_email, FALSE) THEN u.email ELSE '' END,
		u.created_at, u.avatar_url
		FROM users u
		LEFT JOIN user_settings s ON s.user_id = u.id
		JOIN friend_requests fr ON
			(fr.from_user_id = u.id AND fr.to_user_id = $1)
			OR (fr.to_user_id = u.id AND fr.from_user_id = $1)
		WHERE fr.status = 'accepted'
		ORDER BY u.username`

	rows, err := r.db.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, fmt.Errorf("failed to get friends: %w", err)
	}
	defer rows.Close()

	var friends []*domain.User
	for rows.Next() {
		u := &domain.User{}
		if err := rows.Scan(&u.ID, &u.UUID, &u.Username, &u.Email, &u.CreatedAt, &u.AvatarURL); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		friends = append(friends, u)
	}
	return friends, rows.Err()
}

func (r *PgFriendRequestRepository) GetBetweenUsers(ctx context.Context, userID1, userID2 int64) (*domain.FriendRequest, error) {
	query := `SELECT id, from_user_id, to_user_id, status, created_at
		FROM friend_requests
		WHERE (from_user_id = $1 AND to_user_id = $2)
		   OR (from_user_id = $2 AND to_user_id = $1)
 ORDER BY CASE status WHEN 'accepted' THEN 0 WHEN 'pending' THEN 1 ELSE 2 END, id
		LIMIT 1`

	req := &domain.FriendRequest{}
	err := r.db.QueryRowContext(ctx, query, userID1, userID2).Scan(
		&req.ID, &req.FromUserID, &req.ToUserID, &req.Status, &req.CreatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get friend request: %w", err)
	}
	return req, nil
}

// Reuse a rejected request in either direction, rather than creating a second pair row.
func (r *PgFriendRequestRepository) ReopenRequest(ctx context.Context, id, from, to int64) error {
	res, err := r.db.ExecContext(ctx, `UPDATE friend_requests SET from_user_id=$2,to_user_id=$3,status='pending',created_at=NOW() WHERE id=$1 AND status='rejected'`, id, from, to)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return domain.ErrFriendRequestNotPending
	}
	return nil
}
