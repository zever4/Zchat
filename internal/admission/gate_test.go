package admission

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestBoundedDataWithReservedControlAndLogin(t *testing.T) {
	g := New(DefaultConfig(), nil)
	var release []func()
	for i := 1; i <= 16; i++ {
		r, err := g.Try(t.Context(), int64(i), Data)
		if err != nil {
			t.Fatal(err)
		}
		release = append(release, r)
	}
	if _, err := g.Try(t.Context(), 99, Data); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		r, err := g.Try(t.Context(), 0, Login)
		if err != nil {
			t.Fatal(err)
		}
		release = append(release, r)
	}
	for i := 1; i <= 2; i++ {
		r, err := g.Try(t.Context(), int64(i), Control)
		if err != nil {
			t.Fatal(err)
		}
		release = append(release, r)
	}
	if s := g.Snapshot(); s.Active != 20 || s.Data != 16 || s.Control != 2 || s.Login != 2 {
		t.Fatal(s)
	}
	if _, err := g.Try(t.Context(), 30, Control); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	for _, r := range release {
		r()
		r()
	}
	if s := g.Snapshot(); s.Active != 0 || s.Accounts != 0 || s.Peak != 20 || s.Rejected != 2 {
		t.Fatal(s)
	}
}
func TestAccountCannotConsumeOtherAccountsOrOwnControlSlot(t *testing.T) {
	g := New(DefaultConfig(), nil)
	var release []func()
	for i := 0; i < 4; i++ {
		r, e := g.Try(t.Context(), 1, Data)
		if e != nil {
			t.Fatal(e)
		}
		release = append(release, r)
	}
	if _, e := g.Try(t.Context(), 1, Data); !errors.Is(e, ErrBusy) {
		t.Fatal(e)
	}
	for _, id := range []int64{1, 2} {
		r, e := g.Try(t.Context(), id, Control)
		if e != nil {
			t.Fatal(e)
		}
		release = append(release, r)
	}
	r, e := g.Try(t.Context(), 2, Data)
	if e != nil {
		t.Fatal(e)
	}
	release = append(release, r)
	if _, e = g.Try(t.Context(), 1, Control); !errors.Is(e, ErrBusy) {
		t.Fatal(e)
	}
	for _, r := range release {
		r()
	}
	if g.Snapshot().Accounts != 0 {
		t.Fatal("account state leaked")
	}
}
func TestCanceledInvalidAndUnauthenticatedKeysNeverAllocate(t *testing.T) {
	g := New(DefaultConfig(), nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := g.Try(ctx, 1, Data); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	for _, v := range []struct {
		id    int64
		class Class
	}{{0, Data}, {-1, Control}, {1, Login}, {1, 99}} {
		if _, e := g.Try(t.Context(), v.id, v.class); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	if s := g.Snapshot(); s.Active != 0 || s.Accounts != 0 || s.Accepted != 0 {
		t.Fatal(s)
	}
}
func TestConcurrentAdmissionReleaseAndObserver(t *testing.T) {
	var g *Gate
	g = New(DefaultConfig(), func(string, string) { _ = g.Snapshot() })
	var workers sync.WaitGroup
	for i := 0; i < 200; i++ {
		workers.Go(func() {
			for j := 0; j < 100; j++ {
				release, err := g.Try(t.Context(), int64(i%40+1), Data)
				if err == nil {
					release()
				} else if !errors.Is(err, ErrBusy) {
					t.Error(err)
				}
			}
		})
	}
	workers.Wait()
	s := g.Snapshot()
	if s.Active != 0 || s.Accounts != 0 || s.PeakData > 16 || s.Peak > 20 || s.Accepted+s.Rejected != 20000 {
		t.Fatal(s)
	}
}
