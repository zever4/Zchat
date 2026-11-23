package storagebudget

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrInvalid  = errors.New("invalid storage budget configuration or usage")
	ErrQuota    = errors.New("storage quota exceeded")
	ErrHeadroom = errors.New("insufficient filesystem headroom")
	ErrBusy     = errors.New("storage transfer capacity exhausted")
	ErrClosed   = errors.New("storage reservation already settled")
)

type Usage struct{ Bytes, Entries uint64 }
type Disk struct{ FreeBytes, FreeEntries, FSID uint64 }
type AccountUsage struct {
	AccountID int64
	Usage     Usage
}
type account struct {
	usage, inflight Usage
	streams         int
	readers         int
}
type Budget struct {
	mu       sync.Mutex
	c        Config
	accounts map[int64]account
	total    Usage
	inflight Usage
	streams  int
	readers  int
	probe    func() (Disk, error)
	sources  map[string]func() (Disk, error)
	fsFlight map[uint64]Usage
}
type Reservation struct {
	b       *Budget
	account int64
	max     Usage
	settled bool
	fsid    uint64
}

// New seeds ALL retained files/metadata/orphans from a quiescent owned census.
// Account0 charges unattributed historical bytes globally. Lowering a cap below
// retained usage is allowed: refuse new writes, preserve history and cleanup.
func New(c Config, seed []AccountUsage, probe func() (Disk, error)) (*Budget, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if probe == nil {
		return nil, ErrInvalid
	}
	b := &Budget{c: c, accounts: make(map[int64]account), probe: probe, sources: make(map[string]func() (Disk, error)), fsFlight: make(map[uint64]Usage)}
	for _, s := range seed {
		if s.AccountID < 0 || !validUsage(s.Usage) {
			return nil, ErrInvalid
		}
		a := b.accounts[s.AccountID]
		if !addable(b.total, s.Usage) || !addable(a.usage, s.Usage) {
			return nil, ErrInvalid
		}
		b.total = add(b.total, s.Usage)
		a.usage = add(a.usage, s.Usage)
		b.accounts[s.AccountID] = a
	}
	return b, nil
}

// Reserve precedes the first body read, for the full worst-case simultaneous
// temp/final footprint and directory/inode allowances. A failed probe never
// becomes permission to write. No SQL/network/body I/O runs under this mutex.
func (b *Budget) Reserve(ctx context.Context, uid int64, max Usage) (*Reservation, error) {
	return b.ReserveOn(ctx, uid, max, "")
}

// ReserveOn shares quotas across roots but checks headroom only on this root's
// filesystem, including outstanding reservations from every root on that FS.
func (b *Budget) ReserveOn(ctx context.Context, uid int64, max Usage, source string) (*Reservation, error) {
	if uid <= 0 || !validUsage(max) || max.Bytes == 0 || max.Entries == 0 {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a := b.accounts[uid]
	if b.streams >= b.c.Streams || a.streams >= b.c.AccountStreams {
		return nil, ErrBusy
	}
	if !fits(b.total, max, b.c.TotalBytes, b.c.TotalEntries) || !fits(a.usage, max, b.c.AccountBytes, b.c.AccountEntries) {
		return nil, ErrQuota
	}
	// Serialize statfs with reservation/settlement: otherwise a stale free-space
	// sample could outlive another writer's settlement and miss its retained bytes.
	// Probe must perform local filesystem statistics only, never SQL/body/network.
	probe := b.probe
	if source != "" {
		probe = b.sources[source]
	}
	if probe == nil {
		return nil, ErrInvalid
	}
	disk, err := probe()
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	flight := b.fsFlight[disk.FSID]
	if !headroom(disk.FreeBytes, b.c.FreeBytes, flight.Bytes, max.Bytes) || !headroom(disk.FreeEntries, b.c.FreeEntries, flight.Entries, max.Entries) {
		return nil, ErrHeadroom
	}
	b.total = add(b.total, max)
	a.usage = add(a.usage, max)
	a.inflight = add(a.inflight, max)
	b.inflight = add(b.inflight, max)
	b.fsFlight[disk.FSID] = add(flight, max)
	b.streams++
	a.streams++
	b.accounts[uid] = a
	return &Reservation{b: b, account: uid, max: max, fsid: disk.FSID}, nil
}

// Settle replaces the worst-case charge with the physically retained footprint.
// The adapter must have joined/closed all producers and removed/synced temporary
// copies first. On uncertain cleanup it must retain the full reservation, NOT0.
func (r *Reservation) Settle(retained Usage) error {
	if r == nil || r.b == nil || !validUsage(retained) || retained.Bytes > r.max.Bytes || retained.Entries > r.max.Entries {
		return ErrInvalid
	}
	b := r.b
	b.mu.Lock()
	defer b.mu.Unlock()
	if r.settled {
		return ErrClosed
	}
	a := b.accounts[r.account]
	released := sub(r.max, retained)
	b.total = sub(b.total, released)
	a.usage = sub(a.usage, released)
	a.inflight = sub(a.inflight, r.max)
	b.inflight = sub(b.inflight, r.max)
	flight := sub(b.fsFlight[r.fsid], r.max)
	if flight == (Usage{}) {
		delete(b.fsFlight, r.fsid)
	} else {
		b.fsFlight[r.fsid] = flight
	}
	b.streams--
	a.streams--
	r.settled = true
	if a.usage == (Usage{}) && a.streams == 0 && a.readers == 0 {
		delete(b.accounts, r.account)
	} else {
		b.accounts[r.account] = a
	}
	return nil
}

// Retained deletion requires the same physical close/unlink/sync discipline as
// Settle. The owner supplies the census-verified account/charge, never a client.
func (b *Budget) Release(uid int64, u Usage) error {
	if uid < 0 || !validUsage(u) {
		return ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.accounts[uid]
	// Never release a live reservation through this retained-file interface.
	if b.total.Bytes < b.inflight.Bytes || b.total.Entries < b.inflight.Entries || a.usage.Bytes < a.inflight.Bytes || a.usage.Entries < a.inflight.Entries || u.Bytes > b.total.Bytes-b.inflight.Bytes || u.Entries > b.total.Entries-b.inflight.Entries || u.Bytes > a.usage.Bytes-a.inflight.Bytes || u.Entries > a.usage.Entries-a.inflight.Entries {
		return ErrInvalid
	}
	b.total = sub(b.total, u)
	a.usage = sub(a.usage, u)
	if a.usage == (Usage{}) && a.streams == 0 && a.readers == 0 {
		delete(b.accounts, uid)
	} else {
		b.accounts[uid] = a
	}
	return nil
}
func (b *Budget) Usage() (Usage, int) { b.mu.Lock(); defer b.mu.Unlock(); return b.total, b.streams }
func validUsage(u Usage) bool         { return u.Bytes <= 1<<63-1 && u.Entries <= 1<<63-1 }
func addable(a, c Usage) bool         { return a.Bytes <= 1<<63-1-c.Bytes && a.Entries <= 1<<63-1-c.Entries }
func add(a, c Usage) Usage            { return Usage{a.Bytes + c.Bytes, a.Entries + c.Entries} }
func sub(a, c Usage) Usage            { return Usage{a.Bytes - c.Bytes, a.Entries - c.Entries} }
func fits(a, c Usage, bytes, entries uint64) bool {
	return a.Bytes <= bytes && c.Bytes <= bytes-a.Bytes && a.Entries <= entries && c.Entries <= entries-a.Entries
}
func headroom(free, reserve, inflight, request uint64) bool {
	return free >= reserve && free-reserve >= inflight && free-reserve-inflight >= request
}

// ObserveSystemGrowth records measured directory metadata. Existing usage may
// exceed a lowered cap; it must still be charged before refusing new writes.
func (b *Budget) ObserveSystemGrowth(u Usage) error {
	return b.ObserveRetained(0, u)
}

// ObserveRetained records unexpected measured allocation, even above a cap.
// This never grants permission to produce it: subsequent admission is refused.
func (b *Budget) ObserveRetained(uid int64, u Usage) error {
	if uid < 0 || !validUsage(u) {
		return ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.accounts[uid]
	if !addable(b.total, u) || !addable(a.usage, u) {
		return ErrInvalid
	}
	b.total = add(b.total, u)
	a.usage = add(a.usage, u)
	b.accounts[uid] = a
	return nil
}

// Footprint rounds every possible copy separately and includes a per-entry
// allowance. The filesystem adapter chooses an allocation unit at least as
// large as its block size and verifies actual allocated blocks at completion.
func Footprint(bytes, copies, unit, allowance uint64) (Usage, error) {
	if bytes == 0 || copies == 0 || unit == 0 || allowance == 0 || bytes > 1<<63-1 || unit > 1<<63-1 || allowance > 1<<63-1 {
		return Usage{}, ErrInvalid
	}
	if bytes > 1<<63-1-(unit-1) {
		return Usage{}, ErrInvalid
	}
	rounded := ((bytes + unit - 1) / unit) * unit
	if rounded > 1<<63-1-allowance || copies > (1<<63-1)/(rounded+allowance) {
		return Usage{}, ErrInvalid
	}
	return Usage{(rounded + allowance) * copies, copies}, nil
}
