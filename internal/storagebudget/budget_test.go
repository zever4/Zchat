package storagebudget

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
)

func testConfig() Config {
	return Config{TotalBytes: 1000, AccountBytes: 600, TotalEntries: 100, AccountEntries: 60, FreeBytes: 10, FreeEntries: 2, Streams: 64, AccountStreams: 64, Downloads: 64, AccountDownloads: 16}
}
func testBudget(t *testing.T, c Config, seed []AccountUsage) *Budget {
	t.Helper()
	b, err := New(c, seed, func() (Disk, error) { return Disk{FreeBytes: 100000, FreeEntries: 100000}, nil })
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestConfigDefaultsAndOverrides(t *testing.T) {
	c, err := ConfigFromEnv(func(string) string { return "" })
	if err != nil || c.TotalBytes != 50_000_000_000 || c.AccountBytes != 10_000_000_000 || c.FreeBytes != 1_000_000_000 {
		t.Fatal(c, err)
	}
	env := map[string]string{"STORAGE_TOTAL_GB": "20", "STORAGE_ACCOUNT_GB": "5", "STORAGE_FREE_GB": "2", "STORAGE_UPLOADS": "12", "STORAGE_ACCOUNT_UPLOADS": "3", "STORAGE_ACCOUNT_FILES": "10000"}
	c, err = ConfigFromEnv(func(k string) string { return env[k] })
	if err != nil || c.TotalBytes != 20_000_000_000 || c.AccountBytes != 5_000_000_000 || c.FreeBytes != 2_000_000_000 || c.Streams != 12 || c.AccountStreams != 3 || c.AccountEntries != 10000 {
		t.Fatal(c, err)
	}
	for _, bad := range []struct{ key, value string }{{"STORAGE_TOTAL_GB", "0"}, {"STORAGE_TOTAL_GB", "-1"}, {"STORAGE_TOTAL_GB", "1.5"}, {"STORAGE_TOTAL_GB", "18446744073709551615"}, {"STORAGE_ACCOUNT_GB", "51"}, {"STORAGE_UPLOADS", "0"}, {"STORAGE_UPLOADS", "1025"}, {"STORAGE_ACCOUNT_UPLOADS", "9"}, {"STORAGE_ACCOUNT_FILES", "200001"}} {
		if _, err := ConfigFromEnv(func(k string) string {
			if k == bad.key {
				return bad.value
			}
			return ""
		}); err == nil {
			t.Fatal("accepted invalid config", bad)
		}
	}
}
func TestReserveRetainedQuotaAndIdempotentSettlement(t *testing.T) {
	b := testBudget(t, testConfig(), []AccountUsage{{1, Usage{300, 3}}, {0, Usage{100, 1}}})
	r, err := b.Reserve(t.Context(), 1, Usage{200, 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Reserve(t.Context(), 1, Usage{101, 1}); !errors.Is(err, ErrQuota) {
		t.Fatal("account cap bypass", err)
	}
	if _, err = b.Reserve(t.Context(), 2, Usage{401, 1}); !errors.Is(err, ErrQuota) {
		t.Fatal("global cap bypass", err)
	}
	if err = r.Settle(Usage{201, 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err = r.Settle(Usage{80, 1}); err != nil {
		t.Fatal(err)
	}
	if err = r.Settle(Usage{}); !errors.Is(err, ErrClosed) {
		t.Fatal("double free", err)
	}
	if got, streams := b.Usage(); got != (Usage{480, 5}) || streams != 0 {
		t.Fatal(got, streams)
	}
	if err = b.Release(1, Usage{80, 1}); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.Usage(); got != (Usage{400, 4}) {
		t.Fatal(got)
	}
}
func TestReleaseCannotConsumeLiveReservationOfAnotherAccount(t *testing.T) {
	b := testBudget(t, testConfig(), []AccountUsage{{2, Usage{300, 3}}})
	r, err := b.Reserve(t.Context(), 1, Usage{100, 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Release(1, Usage{1, 0}); !errors.Is(err, ErrInvalid) {
		t.Fatal("released live account reservation using other account's retained bytes", err)
	}
	if err = r.Settle(Usage{100, 1}); err != nil {
		t.Fatal(err)
	}
	if err = b.Release(1, Usage{101, 1}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err = b.Release(1, Usage{100, 1}); err != nil {
		t.Fatal(err)
	}
}
func TestHeadroomIncludesAllInflightCopiesAndInodes(t *testing.T) {
	for _, kind := range []string{"bytes", "inodes"} {
		t.Run(kind, func(t *testing.T) {
			disk := Disk{FreeBytes: 100000, FreeEntries: 100000}
			if kind == "bytes" {
				disk.FreeBytes = 210
			} else {
				disk.FreeEntries = 4
			}
			b, err := New(testConfig(), nil, func() (Disk, error) { return disk, nil })
			if err != nil {
				t.Fatal(err)
			}
			r, err := b.Reserve(t.Context(), 1, Usage{100, 1})
			if err != nil {
				t.Fatal(err)
			}
			u := Usage{101, 1}
			if kind == "inodes" {
				u = Usage{1, 2}
			}
			if _, err = b.Reserve(t.Context(), 2, u); !errors.Is(err, ErrHeadroom) {
				t.Fatal("headroom overcommitted", err)
			}
			if err = r.Settle(Usage{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestConcurrentAdmissionNeverOvercommitsGlobalQuota(t *testing.T) {
	c := testConfig()
	c.AccountBytes = 1000
	b := testBudget(t, c, nil)
	var wg sync.WaitGroup
	var admitted atomic.Int32
	start := make(chan struct{})
	reservations := make(chan *Reservation, 32)
	for range 32 {
		wg.Go(func() {
			<-start
			r, err := b.Reserve(t.Context(), 1, Usage{100, 1})
			if err == nil {
				admitted.Add(1)
				reservations <- r
			} else if !errors.Is(err, ErrQuota) {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()
	close(reservations)
	if admitted.Load() != 10 {
		t.Fatal(admitted.Load())
	}
	for r := range reservations {
		if err := r.Settle(Usage{100, 1}); err != nil {
			t.Fatal(err)
		}
	}
	if got, n := b.Usage(); got != (Usage{1000, 10}) || n != 0 {
		t.Fatal(got, n)
	}
}
func TestStreamLimitsCancellationAndProbeFailures(t *testing.T) {
	c := testConfig()
	c.Streams = 2
	c.AccountStreams = 1
	b := testBudget(t, c, nil)
	r, err := b.Reserve(t.Context(), 1, Usage{1, 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Reserve(t.Context(), 1, Usage{1, 1}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	s, err := b.Reserve(t.Context(), 2, Usage{1, 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Reserve(t.Context(), 3, Usage{1, 1}); !errors.Is(err, ErrBusy) {
		t.Fatal(err)
	}
	if err = r.Settle(Usage{}); err != nil {
		t.Fatal(err)
	}
	if err = s.Settle(Usage{}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err = b.Reserve(ctx, 1, Usage{1, 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	bad := errors.New("statfs failure")
	b.probe = func() (Disk, error) { return Disk{}, bad }
	if _, err = b.Reserve(t.Context(), 1, Usage{1, 1}); !errors.Is(err, bad) {
		t.Fatal(err)
	}
	if got, n := b.Usage(); got != (Usage{}) || n != 0 {
		t.Fatal("failed admission leaked capacity", got, n)
	}
}
func TestLoweredCapsPreserveExistingAndPermitCleanup(t *testing.T) {
	c := testConfig()
	c.TotalBytes = 100
	c.AccountBytes = 100
	b := testBudget(t, c, []AccountUsage{{1, Usage{200, 3}}})
	if _, err := b.Reserve(t.Context(), 2, Usage{1, 1}); !errors.Is(err, ErrQuota) {
		t.Fatal(err)
	}
	if err := b.Release(1, Usage{150, 2}); err != nil {
		t.Fatal(err)
	}
	r, err := b.Reserve(t.Context(), 2, Usage{10, 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Settle(Usage{}); err != nil {
		t.Fatal(err)
	}
}
func TestFootprintIncludesEveryCopyAndRejectsOverflow(t *testing.T) {
	u, err := Footprint(5001, 2, 4096, 4096)
	if err != nil || u != (Usage{24576, 2}) {
		t.Fatal(u, err)
	}
	for _, args := range [][4]uint64{{0, 1, 4096, 4096}, {1, 0, 4096, 4096}, {1, 1, 0, 4096}, {1, 1, 4096, 0}, {math.MaxInt64, 2, 4096, 4096}, {1, math.MaxUint64, 4096, 4096}} {
		if _, err := Footprint(args[0], args[1], args[2], args[3]); err == nil {
			t.Fatal(args)
		}
	}
}
func TestFilesystemSampleSerializedWithSettlement(t *testing.T) {
	b := testBudget(t, testConfig(), nil)
	r, err := b.Reserve(t.Context(), 1, Usage{50, 1})
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	b.probe = func() (Disk, error) {
		close(entered)
		<-release
		return Disk{FreeBytes: 100000, FreeEntries: 100000}, nil
	}
	done := make(chan *Reservation, 1)
	go func() {
		s, err := b.Reserve(t.Context(), 2, Usage{50, 1})
		if err != nil {
			t.Error(err)
		}
		done <- s
	}()
	<-entered
	if b.mu.TryLock() {
		b.mu.Unlock()
		t.Fatal("free-space sample can outlive concurrent settlement")
	}
	once.Do(func() { close(release) })
	s := <-done
	if s == nil {
		t.Fatal("reserve failed")
	}
	if err = r.Settle(Usage{50, 1}); err != nil {
		t.Fatal(err)
	}
	if err = s.Settle(Usage{}); err != nil {
		t.Fatal(err)
	}
}

func TestReadPermitsRemainAvailableAboveByteCapAndAreBounded(t *testing.T) {
	c := testConfig()
	c.Downloads = 2
	c.AccountDownloads = 1
	c.TotalBytes = 100
	c.AccountBytes = 100
	b := testBudget(t, c, []AccountUsage{{AccountID: 2, Usage: Usage{Bytes: 200, Entries: 1}}})
	r, err := b.AcquireRead(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.AcquireRead(t.Context(), 1); !errors.Is(err, ErrBusy) {
		t.Fatal("account download cap bypass", err)
	}
	s, err := b.AcquireRead(t.Context(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.AcquireRead(t.Context(), 3); !errors.Is(err, ErrBusy) {
		t.Fatal("global download cap bypass", err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if b.readers != 0 {
		t.Fatal("reader permit leaked")
	}
	if _, ok := b.accounts[1]; ok {
		t.Fatal("empty reader account retained")
	}
}
func TestObservedDirectoryGrowthStopsNewWritesWithoutEviction(t *testing.T) {
	b := testBudget(t, testConfig(), []AccountUsage{{AccountID: 1, Usage: Usage{Bytes: 900, Entries: 1}}})
	if err := b.ObserveSystemGrowth(Usage{Bytes: 200}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Reserve(t.Context(), 2, Usage{1, 1}); !errors.Is(err, ErrQuota) {
		t.Fatal("unmeasured directory growth", err)
	}
	if err := b.Release(0, Usage{Bytes: 200}); err != nil {
		t.Fatal(err)
	}
	if used, _ := b.Usage(); used.Bytes != 900 {
		t.Fatal("history evicted", used)
	}
}
