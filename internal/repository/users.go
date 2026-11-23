package repository

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"log"
	"time"

	"github.com/zever4/Zchat/internal/domain"
)

type PgUserRepository struct {
	db sqlExecutor
}

func NewPgUserRepository(db *sql.DB) *PgUserRepository {
	return &PgUserRepository{db: db}
}

// Creates new user. Only Ssername, Email and PasswordHash fields needed.
// Returns id of new user, if an error occured returns -1
func (r *PgUserRepository) Create(ctx context.Context, user *domain.User) (int64, error) {
	query := `INSERT INTO users (username, email, password_hash, user_uuid)
		VALUES ($1, $2, $3, $4)
		RETURNING id`

	userUUID := uuid.New().String()
	err := r.db.QueryRowContext(ctx, query, user.Username, user.Email, user.PasswordHash, userUUID).Scan(&user.ID)
	if err != nil {
		return -1, fmt.Errorf("failed to create new user: %w", err)
	}

	if _, err := r.db.ExecContext(ctx, `INSERT INTO user_settings (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`, user.ID); err != nil {
		return -1, fmt.Errorf("failed to create default user settings: %w", err)
	}

	log.Printf("New user with id %d created", user.ID)
	return user.ID, nil
}

func (r *PgUserRepository) GetByEmail(ctx context.Context, email string) (*domain.User, error) {
	query := `SELECT id, user_uuid, username, password_hash, token_version, created_at, bio, avatar_url
	FROM users WHERE email = $1`

	user := &domain.User{Email: email}

	err := r.db.QueryRowContext(ctx, query, email).Scan(&user.ID, &user.UUID, &user.Username, &user.PasswordHash, &user.TokenVersion, &user.CreatedAt, &user.Bio, &user.AvatarURL)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return user, nil
}

func (r *PgUserRepository) GetByID(ctx context.Context, id int64) (*domain.User, error) {
	query := `SELECT username, user_uuid, email, password_hash, token_version, created_at, bio, avatar_url
	FROM users WHERE id = $1`

	user := &domain.User{ID: id}

	err := r.db.QueryRowContext(ctx, query, id).Scan(&user.Username, &user.UUID, &user.Email, &user.PasswordHash, &user.TokenVersion, &user.CreatedAt, &user.Bio, &user.AvatarURL)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to get user: %w", err)
	}

	return user, nil
}

func (r *PgUserRepository) SearchByUsername(ctx context.Context, query string, excludeUserID int64, limit int) ([]*domain.User, error) {
	q := `SELECT u.id, u.user_uuid, u.username,
		CASE WHEN COALESCE(s.show_email, FALSE) THEN u.email ELSE '' END,
		u.created_at, u.avatar_url FROM users u
		LEFT JOIN user_settings s ON s.user_id = u.id
		WHERE u.username ILIKE $1 AND u.id != $2
		ORDER BY u.username LIMIT $3`

	rows, err := r.db.QueryContext(ctx, q, "%"+query+"%", excludeUserID, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to search users: %w", err)
	}
	defer rows.Close()

	var users []*domain.User
	for rows.Next() {
		u := &domain.User{}
		if err := rows.Scan(&u.ID, &u.UUID, &u.Username, &u.Email, &u.CreatedAt, &u.AvatarURL); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func (r *PgUserRepository) GetAllUsers(ctx context.Context) ([]*domain.User, error) {
	query := `SELECT id, user_uuid, username, COALESCE(avatar_url, ''), COALESCE(bio, '') FROM users ORDER BY username`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get all users: %w", err)
	}
	defer rows.Close()

	var users []*domain.User
	for rows.Next() {
		u := &domain.User{}
		if err := rows.Scan(&u.ID, &u.UUID, &u.Username, &u.AvatarURL, &u.Bio); err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		users = append(users, u)
	}
	return users, rows.Err()
}

func (r *PgUserRepository) UpdateProfile(ctx context.Context, userID int64, bio string, avatarURL string) error {
	query := `UPDATE users SET bio = $1, avatar_url = $2 WHERE id = $3`
	_, err := r.db.ExecContext(ctx, query, bio, avatarURL, userID)
	if err != nil {
		return fmt.Errorf("failed to update profile: %w", err)
	}
	return nil
}

func (r *PgUserRepository) UpdateProfileBio(ctx context.Context, userID int64, bio string) error {
	query := `UPDATE users SET bio = $1 WHERE id = $2`
	_, err := r.db.ExecContext(ctx, query, bio, userID)
	if err != nil {
		return fmt.Errorf("failed to update profile: %w", err)
	}
	return nil
}

// Returns the avatar actually replaced, even with concurrent uploads. Bio is untouched.
func (r *PgUserRepository) ReplaceAvatar(ctx context.Context, userID int64, avatarURL string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var old string
	err := inSQLTransaction(ctx, r.db, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(avatar_url,'') FROM users WHERE id=$1 FOR UPDATE`, userID).Scan(&old); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE users SET avatar_url=$2 WHERE id=$1`, userID, avatarURL)
		return err
	})
	if err != nil {
		return "", err
	}
	return old, nil
}

func (r *PgUserRepository) GetTokenVersion(ctx context.Context, userID int64) (int, error) {
	var version int
	err := r.db.QueryRowContext(ctx, `SELECT token_version FROM users WHERE id = $1`, userID).Scan(&version)
	if err != nil {
		if err == sql.ErrNoRows {
			return 0, domain.ErrUserNotFound
		}
		return 0, fmt.Errorf("failed to get token version: %w", err)
	}
	return version, nil
}

func (r *PgUserRepository) ensureSettingsRow(ctx context.Context, userID int64) error {
	_, err := r.db.ExecContext(ctx, `INSERT INTO user_settings (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING`, userID)
	if err != nil {
		return fmt.Errorf("failed to ensure settings row: %w", err)
	}
	return nil
}

func (r *PgUserRepository) GetSettings(ctx context.Context, userID int64) (*domain.UserSettings, error) {
	if err := r.ensureSettingsRow(ctx, userID); err != nil {
		return nil, err
	}

	query := `SELECT
		notifications_enabled,
		notification_sound,
		desktop_notifications,
		dnd_enabled,
		dnd_from_hour,
		dnd_to_hour,
		allow_friend_requests,
		allow_direct_messages,
		show_online_status,
		show_email,
		COALESCE(microphone_mode, 'toggle'),
		COALESCE(microphone_ptt_key, ''),
		COALESCE(screen_share_resolution, '1080p'),
		COALESCE(screen_share_fps, 30),
		allow_direct_messages_from_non_friends
	FROM user_settings WHERE user_id = $1`

	settings := &domain.UserSettings{}
	err := r.db.QueryRowContext(ctx, query, userID).Scan(
		&settings.NotificationsEnabled,
		&settings.NotificationSound,
		&settings.DesktopNotifications,
		&settings.DNDEnabled,
		&settings.DNDFromHour,
		&settings.DNDToHour,
		&settings.AllowFriendRequests,
		&settings.AllowDirectMessages,
		&settings.ShowOnlineStatus,
		&settings.ShowEmail,
		&settings.MicrophoneMode,
		&settings.MicrophonePTTKey,
		&settings.ScreenShareMode,
		&settings.ScreenShareFPS,
		&settings.AllowDirectMessagesFromNonFriends,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrUserNotFound
		}
		return nil, fmt.Errorf("failed to get user settings: %w", err)
	}
	return settings, nil
}

func (r *PgUserRepository) UpsertSettings(ctx context.Context, userID int64, settings *domain.UserSettings) error {
	if settings == nil {
		return fmt.Errorf("settings payload is nil")
	}

	query := `INSERT INTO user_settings (
		user_id,
		notifications_enabled,
		notification_sound,
		desktop_notifications,
		dnd_enabled,
		dnd_from_hour,
		dnd_to_hour,
		allow_friend_requests,
		allow_direct_messages,
		show_online_status,
		show_email,
		microphone_mode,
		microphone_ptt_key,
		screen_share_resolution,
		screen_share_fps,
		allow_direct_messages_from_non_friends
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,COALESCE($16::boolean,FALSE))
	ON CONFLICT (user_id) DO UPDATE SET
		notifications_enabled = EXCLUDED.notifications_enabled,
		notification_sound = EXCLUDED.notification_sound,
		desktop_notifications = EXCLUDED.desktop_notifications,
		dnd_enabled = EXCLUDED.dnd_enabled,
		dnd_from_hour = EXCLUDED.dnd_from_hour,
		dnd_to_hour = EXCLUDED.dnd_to_hour,
		allow_friend_requests = EXCLUDED.allow_friend_requests,
		allow_direct_messages = EXCLUDED.allow_direct_messages,
		show_online_status = EXCLUDED.show_online_status,
		show_email = EXCLUDED.show_email,
		microphone_mode = EXCLUDED.microphone_mode,
		microphone_ptt_key = EXCLUDED.microphone_ptt_key,
		screen_share_resolution = EXCLUDED.screen_share_resolution,
		screen_share_fps = EXCLUDED.screen_share_fps,
		allow_direct_messages_from_non_friends = COALESCE($16::boolean,user_settings.allow_direct_messages_from_non_friends),
		updated_at = CURRENT_TIMESTAMP`

	_, err := r.db.ExecContext(ctx, query,
		userID,
		settings.NotificationsEnabled,
		settings.NotificationSound,
		settings.DesktopNotifications,
		settings.DNDEnabled,
		settings.DNDFromHour,
		settings.DNDToHour,
		settings.AllowFriendRequests,
		settings.AllowDirectMessages,
		settings.ShowOnlineStatus,
		settings.ShowEmail,
		settings.MicrophoneMode,
		settings.MicrophonePTTKey,
		settings.ScreenShareMode,
		settings.ScreenShareFPS,
		settings.AllowDirectMessagesFromNonFriends,
	)
	if err != nil {
		return fmt.Errorf("failed to upsert user settings: %w", err)
	}
	return nil
}
