package service

import (
	"context"
	"github.com/zever4/Zchat/internal/admission"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/testutil"
	"testing"
)

func TestAdmissionPasswordRevokeWithSaturatedDataPostgres(t *testing.T) {
	db := testutil.Postgres(t)
	a := NewAuthService(repository.NewPgUserRepository(db), "admission-realPG-test-secret-atleast32")
	g := admission.New(admission.DefaultConfig(), nil)
	a.SetRequestAdmission(g)
	const oldPassword, newPassword = "Original-test-password", "New-test-password"
	first, err := a.Register(t.Context(), "admission_user", "admission@load.invalid", oldPassword)
	if err != nil {
		t.Fatal(err)
	}
	second, _, _, err := a.Login(t.Context(), "admission@load.invalid", oldPassword)
	if err != nil {
		t.Fatal(err)
	}
	var releases []func()
	for i := int64(100); i < 116; i++ {
		r, e := g.Try(t.Context(), i, admission.Data)
		if e != nil {
			t.Fatal(e)
		}
		releases = append(releases, r)
	}
	a.SetSessionRevoker(func(_ context.Context, _ int64, _ string) error {
		if g.Snapshot().Control != 0 {
			t.Error("media cleanup holds control")
		}
		return nil
	})
	if err = a.ChangePassword(t.Context(), first, oldPassword, newPassword, func(int64) {}); err != nil {
		t.Fatal("control starved", err)
	}
	for _, raw := range []string{first, second} {
		if _, e := a.ValidateTokenContext(t.Context(), raw); e == nil {
			t.Fatal("old session survived password change")
		}
	}
	for _, r := range releases {
		r()
	}
	fresh, _, _, err := a.Login(t.Context(), "admission@load.invalid", newPassword)
	if err != nil {
		t.Fatal(err)
	}
	if _, e := a.ValidateTokenContext(t.Context(), fresh); e != nil {
		t.Fatal(e)
	}
	if e := a.Logout(t.Context(), fresh, false, func(int64, string) {}); e != nil {
		t.Fatal(e)
	}
	if _, e := a.ValidateTokenContext(t.Context(), fresh); e == nil {
		t.Fatal("logout did not revoke session")
	}
	if s := g.Snapshot(); s.Active != 0 || s.Accounts != 0 {
		t.Fatal(s)
	}
}
