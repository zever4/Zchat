package storagebudget

// AddSource imports an exclusively owned census exactly once, before writers
// start. Sources last for this Budget's lifetime: no detach, reseed or quota
// reset on a root reopen. All validation precedes any mutation.
func (b *Budget) AddSource(id string, seed []AccountUsage, probe func() (Disk, error)) error {
	if id == "" || len(id) > 128 || probe == nil {
		return ErrInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streams != 0 || b.readers != 0 {
		return ErrBusy
	}
	if _, exists := b.sources[id]; exists {
		return ErrInvalid
	}
	total := b.total
	changes := make(map[int64]account)
	for _, s := range seed {
		if s.AccountID < 0 || !validUsage(s.Usage) {
			return ErrInvalid
		}
		a, ok := changes[s.AccountID]
		if !ok {
			a = b.accounts[s.AccountID]
		}
		if !addable(total, s.Usage) || !addable(a.usage, s.Usage) {
			return ErrInvalid
		}
		total = add(total, s.Usage)
		a.usage = add(a.usage, s.Usage)
		changes[s.AccountID] = a
	}
	b.total = total
	for uid, a := range changes {
		b.accounts[uid] = a
	}
	b.sources[id] = probe
	return nil
}

// Active includes all adapters. The startup owner must stop admission and join
// its coordinators before closing filesystem probes; this is a shutdown check,
// not permission to close another component concurrently.
func (b *Budget) Active() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.streams != 0 || b.readers != 0
}
