package storagebudget

import "context"

type ReadPermit struct {
	b      *Budget
	uid    int64
	closed bool
}

// DownloadCapacity bounds pending private-reader authentication/metadata work.
// It is immutable for this budget lifetime; actual open streams still acquire
// the shared global/account ReadPermit, including ordinary downloads.
func (b *Budget) DownloadCapacity() int { return b.c.Downloads }

// AcquireRead bounds open responses independently of upload quota. Reading
// remains possible above lowered byte caps. Anonymous avatars share global
// capacity without a small per-account bucket for an entire friend list.
func (b *Budget) AcquireRead(ctx context.Context, uid int64) (*ReadPermit, error) {
	if uid < 0 {
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
	if b.readers >= b.c.Downloads || uid > 0 && a.readers >= b.c.AccountDownloads {
		return nil, ErrBusy
	}
	b.readers++
	a.readers++
	b.accounts[uid] = a
	return &ReadPermit{b: b, uid: uid}, nil
}
func (p *ReadPermit) Close() error {
	if p == nil || p.b == nil {
		return ErrInvalid
	}
	b := p.b
	b.mu.Lock()
	defer b.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	a := b.accounts[p.uid]
	a.readers--
	b.readers--
	if a.usage == (Usage{}) && a.streams == 0 && a.readers == 0 {
		delete(b.accounts, p.uid)
	} else {
		b.accounts[p.uid] = a
	}
	return nil
}
