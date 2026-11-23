package service

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/auth"
	"github.com/zever4/Zchat/internal/domain"
	repo "github.com/zever4/Zchat/internal/repository"
	"golang.org/x/sync/semaphore"
)

const TokenLifetime = 72 * time.Hour

var ErrAuthUnavailable = errors.New("session storage unavailable")
var ErrAuthBusy = errors.New("password processing capacity exhausted")
var ErrSessionMediaUnavailable = errors.New("session revoked; media disconnection pending")

type SessionClaims struct {
	UserID  int64 `json:"-"`
	Version int   `json:"ver"`
	jwt.RegisteredClaims
}

type AuthService struct {
	repo          repo.UserRepository
	jwtSecret     string
	passwordSlots chan struct{}
	requests      *admission.Gate
	wsTickets     *webSocketTickets
	// Configured once before serving requests. A failed notification never undoes DB revocation.
	sessionRevoker func(context.Context, int64, string) error
	// Shared by HTTP, unary gRPC, WS registration/frames and credential mutations.
	// This barrier is local to a single server process.
	sessionLocks [64]*semaphore.Weighted
}

func NewAuthService(repo repo.UserRepository, secret string) *AuthService {
	s := &AuthService{repo: repo, jwtSecret: secret, passwordSlots: make(chan struct{}, 8), wsTickets: newWebSocketTickets()}
	for i := range s.sessionLocks {
		s.sessionLocks[i] = semaphore.NewWeighted(sessionLockWeight)
	}
	return s
}

const sessionLockWeight int64 = 1 << 20

func ValidateJWTSecret(secret string) error {
	if len(secret) < 32 || strings.TrimSpace(secret) != secret {
		return errors.New("JWT_SECRET must contain at least 32 bytes without surrounding whitespace")
	}
	return nil
}

func ValidatePassword(password string) error {
	if !utf8.ValidString(password) || utf8.RuneCountInString(password) < 6 || len(password) > 72 {
		return domain.ErrInvalidInput
	}
	return nil
}

func (s *AuthService) Register(ctx context.Context, username, email, password string) (string, error) {
	release, err := s.admit(ctx, 0, admission.Login)
	if err != nil {
		return "", err
	}
	defer release()
	username, email = strings.TrimSpace(username), strings.TrimSpace(email)
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email || len(email) > 254 || !utf8.ValidString(username) || utf8.RuneCountInString(username) < 3 || utf8.RuneCountInString(username) > 50 || strings.ContainsAny(username, "\r\n\t\x00") {
		return "", domain.ErrInvalidInput
	}
	if err := ValidatePassword(password); err != nil {
		return "", err
	}
	hash, err := s.hashPassword(ctx, password)
	if err != nil {
		return "", err
	}
	now := time.Now().Truncate(time.Second)
	sessionID := uuid.NewString()
	id, err := s.repo.RegisterWithSession(ctx, &domain.User{Username: username, Email: email, PasswordHash: hash}, sessionID, now.Add(TokenLifetime))
	if err != nil {
		return "", err
	}
	return s.signToken(id, 1, sessionID, now)
}

func (s *AuthService) Login(ctx context.Context, email, password string) (string, string, string, error) {
	release, err := s.admit(ctx, 0, admission.Login)
	if err != nil {
		return "", "", "", err
	}
	defer release()
	if password == "" || len(password) > 72 || len(email) > 254 {
		return "", "", "", domain.ErrInvalidCredentials
	}
	user, err := s.repo.GetByEmail(ctx, strings.TrimSpace(email))
	if err != nil {
		if errors.Is(err, domain.ErrUserNotFound) {
			return "", "", "", domain.ErrInvalidCredentials
		}
		return "", "", "", ErrAuthUnavailable
	}
	if err := s.checkPassword(ctx, password, user.PasswordHash); err != nil {
		return "", "", "", err
	}
	unlock, err := s.LockSessionContext(ctx, user.ID)
	if err != nil {
		return "", "", "", err
	}
	defer unlock()
	// bcrypt ran outside the lock; re-read the credential snapshot after any queued revoke.
	current, err := s.repo.GetByID(ctx, user.ID)
	if err != nil {
		return "", "", "", ErrAuthUnavailable
	}
	if current.PasswordHash != user.PasswordHash || current.TokenVersion != user.TokenVersion {
		return "", "", "", domain.ErrInvalidCredentials
	}
	token, err := s.issueSession(ctx, current)
	if err != nil {
		return "", "", "", err
	}
	return token, current.Username, current.AvatarURL, nil
}

func (s *AuthService) issueSession(ctx context.Context, user *domain.User) (string, error) {
	token, err := s.GenerateToken(user.ID, user.TokenVersion)
	if err != nil {
		return "", err
	}
	claims, err := s.ParseToken(token)
	if err != nil {
		return "", err
	}
	if err := s.repo.CreateSession(ctx, user.ID, user.PasswordHash, user.TokenVersion, claims.ID, claims.ExpiresAt.Time); err != nil {
		return "", err
	}
	return token, nil
}

// Signing alone does not authorize a token: validation also requires a matching DB session.
func (s *AuthService) GenerateToken(userID int64, version int) (string, error) {
	if userID <= 0 || version <= 0 {
		return "", domain.ErrInvalidSession
	}
	now := time.Now().Truncate(time.Second)
	return s.signToken(userID, version, uuid.NewString(), now)
}

func (s *AuthService) signToken(userID int64, version int, sessionID string, now time.Time) (string, error) {
	claims := SessionClaims{UserID: userID, Version: version, RegisteredClaims: jwt.RegisteredClaims{
		Subject: strconv.FormatInt(userID, 10), ID: sessionID, Issuer: "zchat", Audience: jwt.ClaimStrings{"zchat"}, IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(TokenLifetime)),
	}}
	return jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(s.jwtSecret))
}

func (s *AuthService) ParseToken(raw string) (*SessionClaims, error) {
	if raw == "" || len(raw) > 4096 {
		return nil, domain.ErrInvalidSession
	}
	claims := new(SessionClaims)
	token, err := jwt.ParseWithClaims(raw, claims, func(token *jwt.Token) (any, error) { return []byte(s.jwtSecret), nil },
		jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithIssuer("zchat"), jwt.WithAudience("zchat"), jwt.WithStrictDecoding())
	if err != nil || !token.Valid {
		return nil, domain.ErrInvalidSession
	}
	userID, err := strconv.ParseInt(claims.Subject, 10, 64)
	if err != nil || userID <= 0 || strconv.FormatInt(userID, 10) != claims.Subject {
		return nil, domain.ErrInvalidSession
	}
	claims.UserID = userID
	id, err := uuid.Parse(claims.ID)
	if err != nil || id == uuid.Nil || id.String() != claims.ID || claims.UserID <= 0 || claims.Version <= 0 || claims.IssuedAt == nil || claims.ExpiresAt == nil || !claims.ExpiresAt.After(claims.IssuedAt.Time) || claims.ExpiresAt.Sub(claims.IssuedAt.Time) > TokenLifetime {
		return nil, domain.ErrInvalidSession
	}
	return claims, nil
}

func (s *AuthService) ValidateToken(raw string) (int64, error) {
	return s.ValidateTokenContext(context.Background(), raw)
}

func BearerToken(header string) (string, error) {
	fields := strings.Fields(header)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return "", domain.ErrInvalidSession
	}
	return fields[1], nil
}

// Callers hold unlock until the complete operation finishes. Never nest this barrier.
func (s *AuthService) AuthorizeContext(ctx context.Context, raw string) (*SessionClaims, func(), error) {
	release, err := s.admitToken(ctx, raw, admission.Data)
	if err != nil {
		return nil, nil, err
	}
	claims, err := s.ValidateSessionContext(ctx, raw)
	if err != nil {
		release()
		return nil, nil, err
	}
	unlock, err := s.LockSessionContext(ctx, claims.UserID)
	if err != nil {
		release()
		return nil, nil, err
	}
	current, err := s.ValidateSessionContext(ctx, raw)
	if err != nil {
		unlock()
		release()
		return nil, nil, err
	}
	return current, sync.OnceFunc(func() { unlock(); release() }), nil
}
func (s *AuthService) ValidateTokenContext(ctx context.Context, raw string) (int64, error) {
	claims, err := s.ValidateSessionContext(ctx, raw)
	if err != nil {
		return 0, err
	}
	return claims.UserID, nil
}
func (s *AuthService) ValidateSessionContext(ctx context.Context, raw string) (*SessionClaims, error) {
	claims, err := s.ParseToken(raw)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	version, err := s.repo.GetSessionVersion(ctx, claims.UserID, claims.ID)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidSession) || errors.Is(err, domain.ErrUserNotFound) {
			return nil, domain.ErrInvalidSession
		}
		return nil, fmt.Errorf("%w: %w", ErrAuthUnavailable, err)
	}
	if version != claims.Version || !time.Now().Before(claims.ExpiresAt.Time) {
		return nil, domain.ErrInvalidSession
	}
	return claims, nil
}

// SetSessionRevoker connects session mutations to independently authenticated media.
// Call only during startup, before serving requests.
func (s *AuthService) SetSessionRevoker(revoke func(context.Context, int64, string) error) {
	s.sessionRevoker = revoke
}

// Media tokens are signed separately and bind the DB session by identity. This
// barrier protects the complete upstream admission, not the lifetime of a call.
func (s *AuthService) AuthorizeCallSession(ctx context.Context, userID int64, sessionID string) (func(), error) {
	release, err := s.admit(ctx, userID, admission.Control)
	if err != nil {
		return nil, err
	}
	unlock, err := s.LockSessionContext(ctx, userID)
	if err != nil {
		release()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = s.repo.GetSessionVersion(ctx, userID, sessionID)
	if err != nil {
		unlock()
		release()
		if errors.Is(err, domain.ErrInvalidSession) || errors.Is(err, domain.ErrUserNotFound) {
			return nil, domain.ErrInvalidSession
		}
		return nil, fmt.Errorf("%w: %w", ErrAuthUnavailable, err)
	}
	return sync.OnceFunc(func() { unlock(); release() }), nil
}

func (s *AuthService) revokeMedia(ctx context.Context, userID int64, sessionID string) error {
	if s.sessionRevoker == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.sessionRevoker(ctx, userID, sessionID); err != nil {
		return ErrSessionMediaUnavailable
	}
	return nil
}

func (s *AuthService) ChangePassword(ctx context.Context, raw, oldPassword, newPassword string, disconnect func(int64)) error {
	release, err := s.admitToken(ctx, raw, admission.Control)
	if err != nil {
		return err
	}
	release = sync.OnceFunc(release)
	defer release()
	if err := ValidatePassword(newPassword); err != nil {
		return err
	}
	if oldPassword == "" || len(oldPassword) > 72 {
		return domain.ErrInvalidCredentials
	}
	claims, err := s.ValidateSessionContext(ctx, raw)
	if err != nil {
		return err
	}
	newHash, err := s.hashPassword(ctx, newPassword)
	if err != nil {
		return err
	}
	var mediaErr error
	err = s.RevokeSessionsContext(ctx, claims.UserID, func() error {
		current, err := s.ValidateSessionContext(ctx, raw)
		if err != nil {
			return err
		}
		user, err := s.repo.GetByID(ctx, current.UserID)
		if err != nil {
			return err
		}
		if err := s.checkPassword(ctx, oldPassword, user.PasswordHash); err != nil {
			return err
		}
		return s.repo.ChangePasswordAndRevoke(ctx, current.UserID, current.Version, user.PasswordHash, newHash)
	}, func() { release(); disconnect(claims.UserID); mediaErr = s.revokeMedia(ctx, claims.UserID, "") })
	if err != nil {
		return err
	}
	return mediaErr
}

func (s *AuthService) Logout(ctx context.Context, raw string, all bool, disconnect func(int64, string)) error {
	release, err := s.admitToken(ctx, raw, admission.Control)
	if err != nil {
		return err
	}
	release = sync.OnceFunc(release)
	defer release()
	claims, err := s.ValidateSessionContext(ctx, raw)
	if err != nil {
		return err
	}
	var mediaErr error
	err = s.RevokeSessionsContext(ctx, claims.UserID, func() error {
		current, err := s.ValidateSessionContext(ctx, raw)
		if err != nil {
			return err
		}
		if all {
			return s.repo.RevokeAllSessions(ctx, current.UserID, current.Version)
		}
		return s.repo.DeleteSession(ctx, current.UserID, current.ID)
	}, func() {
		release()
		id := claims.ID
		if all {
			id = ""
		}
		disconnect(claims.UserID, id)
		mediaErr = s.revokeMedia(ctx, claims.UserID, id)
	})
	if err != nil {
		return err
	}
	return mediaErr
}

func (s *AuthService) LockSession(userID int64) func() {
	unlock, _ := s.LockSessionContext(context.Background(), userID)
	return unlock
}

// Limit bcrypt work, not untrusted request-body reads or DB/network waits.
func (s *AuthService) acquirePasswordWork(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case s.passwordSlots <- struct{}{}:
		return func() { <-s.passwordSlots }, nil
	default:
		return nil, ErrAuthBusy
	}
}
func (s *AuthService) hashPassword(ctx context.Context, password string) (string, error) {
	unlock, err := s.acquirePasswordWork(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()
	return auth.Hash(password)
}
func (s *AuthService) checkPassword(ctx context.Context, password, hash string) error {
	unlock, err := s.acquirePasswordWork(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	if err := auth.Check(password, hash); err != nil {
		return domain.ErrInvalidCredentials
	}
	return nil
}

// CheckCurrentPassword reauthenticates an already authenticated subject. The
// caller holds the session barrier; the mutation must compare this snapshot in
// its transaction. This method neither creates nor revokes a login session.
func (s *AuthService) CheckCurrentPassword(ctx context.Context, userID int64, password string) (domain.E2EEReauthentication, error) {
	if userID <= 0 || password == "" || len(password) > 72 {
		return domain.E2EEReauthentication{}, domain.ErrInvalidCredentials
	}
	user, err := s.repo.GetByID(ctx, userID)
	if err != nil {
		return domain.E2EEReauthentication{}, ErrAuthUnavailable
	}
	if err := s.checkPassword(ctx, password, user.PasswordHash); err != nil {
		return domain.E2EEReauthentication{}, err
	}
	return domain.E2EEReauthentication{PasswordHash: user.PasswordHash, TokenVersion: user.TokenVersion}, nil
}
func (s *AuthService) LockSessionContext(ctx context.Context, userID int64) (func(), error) {
	lock := s.sessionLocks[uint64(userID)%uint64(len(s.sessionLocks))]
	if err := lock.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	return func() { lock.Release(1) }, nil
}
func (s *AuthService) RevokeSessions(userID int64, mutate func() error, disconnect func()) error {
	return s.RevokeSessionsContext(context.Background(), userID, mutate, disconnect)
}
func (s *AuthService) RevokeSessionsContext(ctx context.Context, userID int64, mutate func() error, disconnect func()) error {
	lock := s.sessionLocks[uint64(userID)%uint64(len(s.sessionLocks))]
	if err := lock.Acquire(ctx, sessionLockWeight); err != nil {
		return err
	}
	defer lock.Release(sessionLockWeight)
	if err := mutate(); err != nil {
		return err
	}
	disconnect()
	return nil
}
