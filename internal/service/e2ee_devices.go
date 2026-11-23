package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/domain"
	"golang.org/x/sync/semaphore"
	"golang.org/x/time/rate"
)

type E2EEDeviceRepository interface {
	Own(context.Context, int64) (string, *domain.E2EEDevice, error)
	Register(context.Context, int64, domain.E2EEDeviceRegistration) (*domain.E2EEDeviceChange, error)
	Replace(context.Context, int64, domain.E2EEDeviceRegistration, domain.E2EEReauthentication) (*domain.E2EEDeviceChange, error)
}

type replacementLimit struct {
	limiter *rate.Limiter
	touched time.Time
}

type E2EEDeviceService struct {
	repo  E2EEDeviceRepository
	auth  *AuthService
	locks [64]*semaphore.Weighted
	// Configure once during composition. Notification follows durable replacement.
	replaced func(int64, string)
	limitMu  sync.Mutex
	limits   map[int64]replacementLimit
}

func NewE2EEDeviceService(repo E2EEDeviceRepository, auth *AuthService) *E2EEDeviceService {
	s := &E2EEDeviceService{repo: repo, auth: auth, limits: make(map[int64]replacementLimit)}
	for i := range s.locks {
		s.locks[i] = semaphore.NewWeighted(sessionLockWeight)
	}
	return s
}
func (s *E2EEDeviceService) SetDeviceReplacer(fn func(int64, string)) { s.replaced = fn }
func (s *E2EEDeviceService) lock(ctx context.Context, uid int64, weight int64) (func(), error) {
	lock := s.locks[uint64(uid)%uint64(len(s.locks))]
	if err := lock.Acquire(ctx, weight); err != nil {
		return nil, err
	}
	return func() { lock.Release(weight) }, nil
}

func E2EEDeviceCredentialHash(credential []byte) [32]byte {
	h := sha256.New()
	h.Write([]byte("zchat.e2ee.device.v1\x00"))
	h.Write(credential)
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	return sum
}
func validDeviceAccount(account string) bool {
	id, err := uuid.Parse(account)
	return err == nil && len(account) == 36 && id.Version() == 4 && id.Variant() == uuid.RFC4122 && id.String() == account
}
func (s *E2EEDeviceService) Own(ctx context.Context, uid int64) (string, *domain.E2EEDevice, error) {
	if uid <= 0 {
		return "", nil, domain.ErrInvalidSession
	}
	account, d, err := s.repo.Own(ctx, uid)
	if err != nil {
		return "", nil, err
	}
	if !validDeviceAccount(account) {
		return "", nil, ErrAuthUnavailable
	}
	return account, d, nil
}
func verifyDeviceCredential(raw, expected []byte) bool {
	sum := E2EEDeviceCredentialHash(raw)
	return len(raw) == 32 && subtle.ConstantTimeCompare(sum[:], expected) == 1
}

// Caller must already hold the login-session barrier. Keep this device barrier
// until the short operation finishes; never hold it over a stream lifetime.
func (s *E2EEDeviceService) Authorize(ctx context.Context, uid int64, generation string, raw []byte) (*domain.E2EEDevice, func(), error) {
	if uid <= 0 {
		return nil, nil, domain.ErrInvalidSession
	}
	unlock, err := s.lock(ctx, uid, 1)
	if err != nil {
		return nil, nil, err
	}
	_, d, err := s.Own(ctx, uid)
	if err == nil && (d == nil || (generation != "" && d.GenerationID != generation) || !verifyDeviceCredential(raw, d.CredentialHash)) {
		err = domain.ErrE2EEDeviceCredential
	}
	if err != nil {
		unlock()
		return nil, nil, err
	}
	return d, unlock, nil
}

func (s *E2EEDeviceService) Register(ctx context.Context, uid int64, req domain.E2EEDeviceRegistration, raw []byte) (*domain.E2EEDeviceChange, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidSession
	}
	if req.ExpectedGenerationID != "" || !verifyDeviceCredential(raw, req.CredentialHash) {
		return nil, domain.ErrE2EEDeviceCredential
	}
	unlock, err := s.lock(ctx, uid, sessionLockWeight)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, _, err := s.Own(ctx, uid); err != nil {
		return nil, err
	}
	return s.repo.Register(ctx, uid, req)
}

// Bounded per-account rate limit plus AuthService's shared bcrypt CPU budget.
// Average 5 attempts/minute, burst 5; fail closed at 1024 live account limiters.
func (s *E2EEDeviceService) allowReplacement(uid int64) bool {
	s.limitMu.Lock()
	defer s.limitMu.Unlock()
	now := time.Now()
	entry, ok := s.limits[uid]
	if !ok {
		if len(s.limits) >= 1024 {
			for id, item := range s.limits {
				if now.Sub(item.touched) > 10*time.Minute {
					delete(s.limits, id)
				}
			}
			if len(s.limits) >= 1024 {
				return false
			}
		}
		entry.limiter = rate.NewLimiter(rate.Every(12*time.Second), 5)
	}
	entry.touched = now
	s.limits[uid] = entry
	return entry.limiter.AllowN(now, 1)
}
func (s *E2EEDeviceService) Replace(ctx context.Context, uid int64, req domain.E2EEDeviceRegistration, password string, raw []byte) (*domain.E2EEDeviceChange, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidSession
	}
	if req.ExpectedGenerationID == "" || !verifyDeviceCredential(raw, req.CredentialHash) {
		return nil, domain.ErrE2EEDeviceCredential
	}
	if !s.allowReplacement(uid) {
		return nil, domain.ErrE2EEReauthLimited
	}
	// Always reauthenticate, including an idempotent retry; never store password
	// in a request record. Repository compares the verified snapshot under lock.
	verified, err := s.auth.CheckCurrentPassword(ctx, uid, password)
	if err != nil {
		return nil, err
	}
	unlock, err := s.lock(ctx, uid, sessionLockWeight)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, _, err := s.Own(ctx, uid); err != nil {
		return nil, err
	}
	result, err := s.repo.Replace(ctx, uid, req, verified)
	if err == nil && !result.Replayed && s.replaced != nil {
		s.replaced(uid, result.PreviousGenerationID)
	}
	return result, err
}
