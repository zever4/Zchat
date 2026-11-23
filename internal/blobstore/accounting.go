package blobstore

import (
	"context"
	"errors"
	"sync"

	"github.com/zever4/Zchat/internal/storagebudget"
)

type physicalEntry struct {
	uid    int64
	u      storagebudget.Usage
	active bool
}

// Accounting connects this private root to the SAME budget as ordinary files.
// Only the coordinator supplies PG-verified immutable sender ownership. This is
// physical accounting, never a Send receipt, crypto repair or network readiness.
type Accounting struct {
	mu                      sync.Mutex
	store                   *Store
	budget                  *storagebudget.Budget
	entries                 map[string]physicalEntry
	unit, dirBytes          uint64
	artifacts, maxArtifacts uint64
}

type PhysicalReservation struct {
	a        *Accounting
	id       string
	max      storagebudget.Usage
	ticket   *storagebudget.Reservation
	finished bool // protected by a.mu
}

// AttachAccounting requires quiescence after startup reconciliation. Register
// once per process lifetime; reopening a source in a live budget is rejected.
func (s *Store) AttachAccounting(ctx context.Context, b *storagebudget.Budget, owners map[string]int64, max int) (result *Accounting, err error) {
	if b == nil {
		return nil, ErrInvalid
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if s.accounted || len(s.active) != 0 || s.physicalActive != 0 {
		s.mu.Unlock()
		return nil, ErrBusy
	}
	s.accounted = true
	s.physicalActive++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.physicalActive--
		if err != nil {
			s.accounted = false
		}
		s.mu.Unlock()
	}()
	snapshot, err := s.PhysicalCensus(ctx, max)
	if err != nil {
		return nil, err
	}
	if len(owners) != len(snapshot.Artifacts) {
		return nil, ErrIntegrity
	}
	dir, err := s.physicalDirectory()
	if err != nil {
		return nil, err
	}
	a := &Accounting{store: s, budget: b, entries: make(map[string]physicalEntry), unit: snapshot.Unit, dirBytes: dir.Bytes, artifacts: uint64(len(snapshot.Artifacts)), maxArtifacts: uint64(max)}
	seed := []storagebudget.AccountUsage{{Usage: snapshot.Metadata}}
	for _, file := range snapshot.Artifacts {
		uid := owners[file.AttemptID]
		if uid <= 0 || file.Part {
			return nil, ErrIntegrity
		}
		if _, duplicate := a.entries[file.AttemptID]; duplicate {
			return nil, ErrIntegrity
		}
		a.entries[file.AttemptID] = physicalEntry{uid: uid, u: file.Usage}
		seed = append(seed, storagebudget.AccountUsage{AccountID: uid, Usage: file.Usage})
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if err = b.AddSource(s.StorageID(), seed, s.PhysicalDisk); err != nil {
		return nil, err
	}
	return a, nil
}

func (a *Accounting) observeDirectory() error {
	u, err := a.store.physicalDirectory()
	if err != nil {
		return err
	}
	if u.Bytes > a.dirBytes {
		if err = a.budget.ObserveSystemGrowth(storagebudget.Usage{Bytes: u.Bytes - a.dirBytes}); err != nil {
			return err
		}
		a.dirBytes = u.Bytes
	}
	return nil
}

// Reserve precedes Store.Write/the first source read, after short PG admission.
// Two independently rounded copies conservatively cover interrupted publication
// names, allocation and metadata; actual hard-link publication uses one inode.
func (a *Accounting) Reserve(ctx context.Context, uid int64, attempt Attempt) (*PhysicalReservation, error) {
	if uid <= 0 || !validID(attempt.ID) || attempt.Bytes <= 0 {
		return nil, ErrInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, exists := a.entries[attempt.ID]; exists {
		return nil, ErrConflict
	}
	// Include both publication names, so accepted writes cannot grow beyond
	// the same bounded manifest the next startup is able to reconcile.
	if a.artifacts > a.maxArtifacts || a.maxArtifacts-a.artifacts < 2 {
		return nil, storagebudget.ErrQuota
	}
	a.store.mu.Lock()
	if a.store.closed {
		a.store.mu.Unlock()
		return nil, ErrClosed
	}
	a.store.physicalActive++
	a.store.mu.Unlock()
	keep := false
	defer func() {
		if !keep {
			a.store.mu.Lock()
			a.store.physicalActive--
			a.store.mu.Unlock()
		}
	}()
	if err := a.observeDirectory(); err != nil {
		return nil, err
	}
	max, err := storagebudget.Footprint(uint64(attempt.Bytes), 2, a.unit, physicalEntryAllowance)
	if err != nil {
		return nil, err
	}
	ticket, err := a.budget.ReserveOn(ctx, uid, max, a.store.StorageID())
	if err != nil {
		return nil, err
	}
	a.entries[attempt.ID] = physicalEntry{uid: uid, u: max, active: true}
	a.artifacts += max.Entries
	keep = true
	return &PhysicalReservation{a: a, id: attempt.ID, max: max, ticket: ticket}, nil
}

// Finish must run after Store.Write and its source-close/cancellation callbacks
// have joined, before the coordinator releases operation ownership. Errors keep
// worst-case quota until a live Removed proof establishes durable deletion.
func (r *PhysicalReservation) Finish(w *Written) error {
	if r == nil || r.a == nil {
		return ErrInvalid
	}
	a := r.a
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.finished {
		return storagebudget.ErrClosed
	}
	retained := r.max
	var excess storagebudget.Usage
	var err error
	if w != nil {
		if w.attempt.ID != r.id {
			err = ErrIntegrity
		} else {
			// Accounting completion must also work after the request is cancelled.
			var measured storagebudget.Usage
			measured, err = a.store.physicalWritten(context.Background(), w)
			if err == nil {
				retained = storagebudget.Usage{Bytes: min(measured.Bytes, r.max.Bytes), Entries: min(measured.Entries, r.max.Entries)}
				excess = storagebudget.Usage{Bytes: measured.Bytes - retained.Bytes, Entries: measured.Entries - retained.Entries}
			}
		}
	}
	err = errors.Join(err, a.observeDirectory())
	if err != nil {
		retained = r.max
	}
	e := a.entries[r.id]
	if e := a.budget.ObserveRetained(e.uid, excess); e != nil {
		return errors.Join(err, e)
	}
	if e := r.ticket.Settle(retained); e != nil {
		return errors.Join(err, e)
	}
	retained.Bytes += excess.Bytes
	retained.Entries += excess.Entries
	e.u, e.active = retained, false
	a.artifacts = a.artifacts - r.max.Entries + retained.Entries
	a.entries[r.id] = e
	r.finished = true
	a.store.mu.Lock()
	a.store.physicalActive--
	a.store.mu.Unlock()
	return err
}

// Forget accepts only this root's live unlink+directory-Sync proof. Physical
// bytes may be released even if the later PG logical-counter COMMIT is unknown;
// retry is idempotent and cannot free another attempt's reservation.
func (a *Accounting) Forget(id string, proof *Removed) error {
	return proof.Guard(a.store.StorageID(), id, func() error {
		a.mu.Lock()
		defer a.mu.Unlock()
		e, ok := a.entries[id]
		if !ok {
			return nil
		}
		if e.active {
			return ErrBusy
		}
		if err := a.observeDirectory(); err != nil {
			return err
		}
		if err := a.budget.Release(e.uid, e.u); err != nil {
			return err
		}
		delete(a.entries, id)
		a.artifacts -= e.u.Entries
		return nil
	})
}
