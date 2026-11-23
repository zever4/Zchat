package service

import (
	"context"
	"errors"
	"github.com/zever4/Zchat/internal/admission"
	"testing"
)

func TestRequestAdmissionSharedBeforeDBAndReleasedExactlyOnce(t *testing.T) {
	u := &validationUsers{}
	a := NewAuthService(u, "admission-test-secret-at-least32bytes")
	g := admission.New(admission.Config{Total: 3, Data: 2, Login: 1, PerAccount: 1, PerControl: 1}, nil)
	a.SetRequestAdmission(g)
	one, _ := a.GenerateToken(1, 1)
	two, _ := a.GenerateToken(2, 1)
	three, _ := a.GenerateToken(3, 1)
	_, r1, e := a.AuthorizeContext(t.Context(), one)
	if e != nil {
		t.Fatal(e)
	}
	_, r2, e := a.AuthorizeContext(t.Context(), two)
	if e != nil {
		t.Fatal(e)
	}
	reads := u.reads.Load()
	for _, raw := range []string{one, three} {
		if _, _, e := a.AuthorizeContext(t.Context(), raw); !errors.Is(e, admission.ErrBusy) {
			t.Fatal(e)
		}
	}
	if _, _, e := a.AuthorizeContext(t.Context(), "invalid"); e == nil || errors.Is(e, admission.ErrBusy) {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, e := a.AuthorizeContext(ctx, one); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if u.reads.Load() != reads {
		t.Fatal("rejected admission reached database")
	}
	control, e := a.AuthorizeCallSession(t.Context(), 1, "synthetic-sid")
	if e != nil {
		t.Fatal(e)
	}
	if g.Snapshot().Active != 3 {
		t.Fatal(g.Snapshot())
	}
	r1()
	r1()
	r2()
	control()
	control()
	if s := g.Snapshot(); s.Active != 0 || s.Accounts != 0 {
		t.Fatal(s)
	}
}
func TestRequestAdmissionDBFailureReleasesBudget(t *testing.T) {
	u := &validationUsers{err: errors.New("database unavailable")}
	a := NewAuthService(u, "admission-test-secret-at-least32bytes")
	g := admission.New(admission.DefaultConfig(), nil)
	a.SetRequestAdmission(g)
	raw, _ := a.GenerateToken(1, 1)
	if _, _, e := a.AuthorizeContext(t.Context(), raw); !errors.Is(e, ErrAuthUnavailable) {
		t.Fatal(e)
	}
	if g.Snapshot().Active != 0 || g.Snapshot().Accounts != 0 {
		t.Fatal(g.Snapshot())
	}
	if _, e := a.AuthorizeCallSession(t.Context(), 1, "synthetic"); !errors.Is(e, ErrAuthUnavailable) {
		t.Fatal(e)
	}
	if g.Snapshot().Active != 0 {
		t.Fatal("control budget leaked")
	}
}

type admissionLogoutUsers struct {
	validationUsers
	deleted int
}

func (u *admissionLogoutUsers) DeleteSession(context.Context, int64, string) error {
	u.deleted++
	return nil
}
func TestControlRemainsAvailableAndReleasesBeforeMediaDisconnect(t *testing.T) {
	u := &admissionLogoutUsers{}
	a := NewAuthService(u, "admission-test-secret-at-least32bytes")
	g := admission.New(admission.Config{Total: 3, Data: 2, Login: 1, PerAccount: 1, PerControl: 1}, nil)
	a.SetRequestAdmission(g)
	one, _ := a.GenerateToken(1, 1)
	two, _ := a.GenerateToken(2, 1)
	_, r1, e := a.AuthorizeContext(t.Context(), one)
	if e != nil {
		t.Fatal(e)
	}
	_, r2, e := a.AuthorizeContext(t.Context(), two)
	if e != nil {
		t.Fatal(e)
	}
	// Logout must still take its WRITE barrier. Release the same-account data
	// operation while keeping the global data lane saturated by other accounts.
	r1()
	three, _ := a.GenerateToken(3, 1)
	_, r3, e := a.AuthorizeContext(t.Context(), three)
	if e != nil {
		t.Fatal(e)
	}
	a.SetSessionRevoker(func(context.Context, int64, string) error {
		if g.Snapshot().Control != 0 {
			t.Error("network cleanup holds control slot")
		}
		return nil
	})
	if e = a.Logout(t.Context(), one, false, func(int64, string) {}); e != nil {
		t.Fatal(e)
	}
	if u.deleted != 1 || g.Snapshot().Control != 0 || g.Snapshot().Data != 2 {
		t.Fatal(u.deleted, g.Snapshot())
	}
	r2()
	r3()
}
func TestAnonymousPasswordBudgetAndBusyMutationDoNotReachDB(t *testing.T) {
	u := &validationUsers{}
	a := NewAuthService(u, "admission-test-secret-at-least32bytes")
	g := admission.New(admission.DefaultConfig(), nil)
	a.SetRequestAdmission(g)
	var releases []func()
	for i := 0; i < 2; i++ {
		r, e := g.Try(t.Context(), 0, admission.Login)
		if e != nil {
			t.Fatal(e)
		}
		releases = append(releases, r)
	}
	if _, e := a.Register(t.Context(), "name", "name@load.invalid", "password"); !errors.Is(e, admission.ErrBusy) {
		t.Fatal(e)
	}
	if _, _, _, e := a.Login(t.Context(), "name@load.invalid", "password"); !errors.Is(e, admission.ErrBusy) {
		t.Fatal(e)
	}
	for _, r := range releases {
		r()
	}
	raw, _ := a.GenerateToken(1, 1)
	r, e := g.Try(t.Context(), 1, admission.Control)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.ChangePassword(t.Context(), raw, "oldpassword", "newpassword", func(int64) {}); !errors.Is(e, admission.ErrBusy) {
		t.Fatal(e)
	}
	if e = a.Logout(t.Context(), raw, false, func(int64, string) {}); !errors.Is(e, admission.ErrBusy) {
		t.Fatal(e)
	}
	if u.reads.Load() != 0 {
		t.Fatal("busy password/mutation reached DB")
	}
	r()
}
