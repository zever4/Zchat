package service

import (
	"context"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/domain"
)

// Configure once during composition, before serving requests. All transports
// using this AuthService share the same gate. It never replaces authorization.
func (s *AuthService) SetRequestAdmission(g *admission.Gate) { s.requests = g }

func (s *AuthService) admit(ctx context.Context, id int64, class admission.Class) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.requests == nil {
		return func() {}, nil
	}
	return s.requests.Try(ctx, id, class)
}
func (s *AuthService) admitToken(ctx context.Context, raw string, class admission.Class) (func(), error) {
	if s.requests == nil {
		return s.admit(ctx, 0, class)
	}
	// Never use unverified JWT/email input to allocate per-account entries.
	claims, err := s.ParseToken(raw)
	if err != nil {
		return nil, err
	}
	return s.admit(ctx, claims.UserID, class)
}

// Only for an existing connection's server-owned account ID. The caller must
// retain its session barrier and DB validation AFTER admission, for every frame.
func (s *AuthService) AdmitMessage(ctx context.Context, id int64) (func(), error) {
	if id <= 0 {
		return nil, domain.ErrInvalidSession
	}
	return s.admit(ctx, id, admission.Data)
}

// Call signaling uses the same trusted connection identity and its reserved
// control allowance; arbitrary message types must stay in the data class.
func (s *AuthService) AdmitCallSignal(ctx context.Context, id int64) (func(), error) {
	if id <= 0 {
		return nil, domain.ErrInvalidSession
	}
	return s.admit(ctx, id, admission.Control)
}
