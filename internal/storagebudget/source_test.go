package storagebudget

import (
	"errors"
	"testing"
)

func TestSourcesShareQuotaAndSameFilesystemFlight(t *testing.T) {
	c := testConfig()
	c.TotalBytes, c.AccountBytes = 10000, 10000
	b, err := New(c, nil, func() (Disk, error) { return Disk{FreeBytes: 1000, FreeEntries: 100, FSID: 1}, nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, fs := range []struct {
		id  string
		dev uint64
	}{{"same", 1}, {"other", 2}} {
		if err = b.AddSource(fs.id, nil, func() (Disk, error) { return Disk{FreeBytes: 1000, FreeEntries: 100, FSID: fs.dev}, nil }); err != nil {
			t.Fatal(err)
		}
	}
	r, err := b.Reserve(t.Context(), 1, Usage{600, 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.ReserveOn(t.Context(), 2, Usage{500, 1}, "same"); !errors.Is(err, ErrHeadroom) {
		t.Fatal("same disk ignored ordinary inflight", err)
	}
	s, err := b.ReserveOn(t.Context(), 2, Usage{500, 1}, "other")
	if err != nil {
		t.Fatal("unrelated disk blocked", err)
	}
	if err = r.Settle(Usage{400, 1}); err != nil {
		t.Fatal(err)
	}
	if err = s.Settle(Usage{}); err != nil {
		t.Fatal(err)
	}
	if u, n := b.Usage(); u != (Usage{400, 1}) || n != 0 || len(b.fsFlight) != 0 {
		t.Fatal(u, n, b.fsFlight)
	}
	c.AccountBytes = 600
	b = testBudget(t, c, []AccountUsage{{AccountID: 1, Usage: Usage{400, 1}}})
	if err = b.AddSource("private", []AccountUsage{{AccountID: 1, Usage: Usage{100, 1}}}, b.probe); err != nil {
		t.Fatal(err)
	}
	if _, err = b.Reserve(t.Context(), 1, Usage{101, 1}); !errors.Is(err, ErrQuota) {
		t.Fatal("per-account quota separated by root", err)
	}
	independent, err := b.ReserveOn(t.Context(), 2, Usage{101, 1}, "private")
	if err != nil {
		t.Fatal("different account blocked", err)
	}
	if err = independent.Settle(Usage{}); err != nil {
		t.Fatal(err)
	}
}

func TestSourceGlobalBytesAndEntryLimitsIncludeBothCensuses(t *testing.T) {
	for _, kind := range []string{"bytes", "entries"} {
		t.Run(kind, func(t *testing.T) {
			c := testConfig()
			c.TotalEntries, c.AccountEntries = 3, 3
			b := testBudget(t, c, []AccountUsage{{AccountID: 1, Usage: Usage{400, 1}}})
			if err := b.AddSource("private", []AccountUsage{{AccountID: 2, Usage: Usage{200, 1}}}, b.probe); err != nil {
				t.Fatal(err)
			}
			request := Usage{401, 1}
			if kind == "entries" {
				request = Usage{1, 2}
			}
			if _, err := b.ReserveOn(t.Context(), 3, request, "private"); !errors.Is(err, ErrQuota) {
				t.Fatal("separate global quota", kind, err)
			}
		})
	}
}

func TestSourceImportIsAtomicOnceAndRequiresQuiescence(t *testing.T) {
	b := testBudget(t, testConfig(), nil)
	if err := b.AddSource("bad", []AccountUsage{{AccountID: 1, Usage: Usage{100, 1}}, {AccountID: -1, Usage: Usage{1, 1}}}, b.probe); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if u, _ := b.Usage(); u != (Usage{}) {
		t.Fatal("partial seed imported", u)
	}
	if err := b.AddSource("overflow", []AccountUsage{{AccountID: 1, Usage: Usage{1<<63 - 1, 1}}, {AccountID: 1, Usage: Usage{1, 1}}}, b.probe); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	r, err := b.Reserve(t.Context(), 1, Usage{100, 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.AddSource("private", nil, b.probe); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if err = r.Settle(Usage{}); err != nil {
		t.Fatal(err)
	}
	reader, err := b.AcquireRead(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.AddSource("private", nil, b.probe); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	reader.Close()
	if err = b.AddSource("private", []AccountUsage{{AccountID: 1, Usage: Usage{700, 1}}}, b.probe); err != nil {
		t.Fatal("lower cap must preserve census", err)
	}
	before, _ := b.Usage()
	if err = b.AddSource("private", nil, b.probe); !errors.Is(err, ErrInvalid) {
		t.Fatal("duplicate source reset/import", err)
	}
	if u, _ := b.Usage(); u != before {
		t.Fatal(u, before)
	}
	if _, err = b.ReserveOn(t.Context(), 1, Usage{1, 1}, "private"); !errors.Is(err, ErrQuota) {
		t.Fatal(err)
	}
}

func TestUnavailablePrivateSourceDoesNotBlockOrdinaryReadsOrWrites(t *testing.T) {
	b := testBudget(t, testConfig(), nil)
	bad := errors.New("private root closed")
	if err := b.AddSource("private", nil, func() (Disk, error) { return Disk{}, bad }); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ReserveOn(t.Context(), 1, Usage{1, 1}, "private"); !errors.Is(err, bad) {
		t.Fatal(err)
	}
	r, err := b.Reserve(t.Context(), 1, Usage{1, 1})
	if err != nil {
		t.Fatal(err)
	}
	r.Settle(Usage{})
	reader, err := b.AcquireRead(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	if b.Active() {
		t.Fatal("leaked transfers")
	}
}
