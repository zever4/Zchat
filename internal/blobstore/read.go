package blobstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"hash"
	"io"
	"sync"
	"sync/atomic"

	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/storagebudget"
)

// ReadLease owns an exact ciphertext inode, root pin and shared read permit.
// It is not database/JWT authorization, AEAD verification or a file ACK. Only
// the authenticated coordinator may choose the server-side attempt/root.
// No raw *os.File, Seek, WriteTo or ReaderFrom can bypass size/hash checks.
type ReadLease struct {
	store        *Store
	attempt      Attempt
	ctx          context.Context
	source       io.ReadCloser
	permit       *storagebudget.ReadPermit
	readMu       sync.Mutex
	remaining    int64
	digest       hash.Hash
	empty        int
	terminal     error
	interrupted  atomic.Bool
	closeSource  func() error
	stopCallback func() bool
	callbackDone chan struct{}
	finish       func() error
}

// OpenRead bounds readers with the SAME global/account download budget as
// ordinary files, independently of upload slots and lowered byte quotas. uid
// is the authenticated READER, not necessarily the stored file's sender.
// Membership/accounting alone is NOT acceptance or permission to send bytes.
func (a *Accounting) OpenRead(ctx context.Context, uid int64, target Attempt) (*ReadLease, error) {
	if a == nil || uid <= 0 || !validID(target.ID) || target.Bytes <= boundary.AttachmentBlobOverhead || target.Bytes > boundary.MaxAttachmentBlobBytes {
		return nil, ErrInvalid
	}
	permit, err := a.budget.AcquireRead(ctx, uid)
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	entry, exists := a.entries[target.ID]
	if !exists {
		err = ErrConflict
	} else if entry.active {
		err = ErrBusy
	} else {
		err = a.store.pinRead(ctx, target.ID)
	}
	a.mu.Unlock()
	if err != nil {
		permit.Close()
		return nil, err
	}
	f, info, err := openArtifact(a.store.root, target.ID+".blob", false)
	if err == nil && info.Size() != target.Bytes {
		err = ErrIntegrity
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		if f != nil {
			if closeErr := f.Close(); closeErr != nil {
				return nil, errors.Join(err, closeErr)
			}
		} // Uncertain close retains ownership.
		a.store.unpinRead(target.ID)
		permit.Close()
		return nil, err
	}
	return newReadLease(ctx, a.store, target, f, permit), nil
}

// Pin before opening: Remove/Write/Close must not pass an opening or active
// reader. Read count is bounded by the already acquired shared read permit;
// readers do not consume the write/cleanup operation slots.
func (s *Store) pinRead(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	if _, busy := s.active[id]; busy {
		return ErrBusy
	}
	if s.readers == nil {
		s.readers = make(map[string]int)
	}
	s.readers[id]++
	s.readCount++
	return nil
}
func (s *Store) unpinRead(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readers[id]--
	s.readCount--
	if s.readers[id] == 0 {
		delete(s.readers, id)
	}
}

func newReadLease(ctx context.Context, s *Store, a Attempt, source io.ReadCloser, permit *storagebudget.ReadPermit) *ReadLease {
	r := &ReadLease{store: s, attempt: a, ctx: ctx, source: source, permit: permit, remaining: a.Bytes, digest: sha256.New(), callbackDone: make(chan struct{})}
	r.closeSource = sync.OnceValue(source.Close)
	r.stopCallback = context.AfterFunc(ctx, func() { defer close(r.callbackDone); r.interrupt() })
	r.finish = sync.OnceValue(func() error {
		err := r.interrupt()
		if !r.stopCallback() {
			<-r.callbackDone
		}
		// Close the descriptor BEFORE waiting: its Close must unblock Read. A
		// kernel filesystem syscall may still delay the join; never drop pins.
		r.readMu.Lock()
		r.readMu.Unlock()
		if err != nil {
			return err
		} // Like ordinary readers, uncertain close keeps its pin/permit.
		r.store.unpinRead(r.attempt.ID)
		return r.permit.Close()
	})
	return r
}
func (r *ReadLease) Attempt() Attempt  { return r.attempt }
func (r *ReadLease) StorageID() string { return r.store.StorageID() }
func (r *ReadLease) interrupt() error  { r.interrupted.Store(true); return r.closeSource() }
func (r *ReadLease) stopped() error {
	if err := context.Cause(r.ctx); err != nil {
		return err
	}
	if r.interrupted.Load() {
		return ErrClosed
	}
	return nil
}

// Read exposes at most32KiB per call. EOF means exact size AND SHA have been
// checked, including an excess-byte probe. Some ciphertext may precede a later
// error; native must still verify complete size/SHA/AAD/AEAD before use/FILEACK.
func (r *ReadLease) Read(p []byte) (int, error) {
	r.readMu.Lock()
	defer r.readMu.Unlock()
	if err := r.stopped(); err != nil {
		return 0, err
	}
	if r.terminal != nil {
		return 0, r.terminal
	}
	if len(p) == 0 {
		return 0, nil
	}
	limit := min(int64(len(p)), int64(bufferBytes), r.remaining)
	n, err := r.source.Read(p[:int(limit)])
	if e := r.stopped(); e != nil {
		return 0, e
	}
	if n < 0 || int64(n) > limit {
		r.terminal = ErrIntegrity
		return 0, r.terminal
	}
	if n > 0 {
		r.empty = 0
		r.remaining -= int64(n)
		_, _ = r.digest.Write(p[:n])
	} else {
		r.empty++
		if r.empty >= 100 {
			r.terminal = io.ErrNoProgress
			return 0, r.terminal
		}
	}
	if err != nil && err != io.EOF {
		r.terminal = err
		return 0, err
	}
	if err == io.EOF && r.remaining != 0 {
		r.terminal = ErrIntegrity
		return 0, r.terminal
	}
	if r.remaining == 0 {
		if err != io.EOF {
			if e := r.probeEnd(); e != nil {
				r.terminal = e
				return 0, e
			}
		}
		if [32]byte(r.digest.Sum(nil)) != r.attempt.SHA256 {
			r.terminal = ErrIntegrity
			return 0, r.terminal
		}
		r.terminal = io.EOF
		return n, io.EOF
	}
	return n, nil
}
func (r *ReadLease) probeEnd() error {
	var extra [1]byte
	for range 100 {
		if err := r.stopped(); err != nil {
			return err
		}
		n, err := r.source.Read(extra[:])
		if e := r.stopped(); e != nil {
			return e
		}
		if n != 0 {
			return ErrIntegrity
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
	return io.ErrNoProgress
}

// Cancellation interrupts the descriptor but deliberately does NOT unpin the
// file: an owner may still be joining its network sink. Explicit Close joins
// the last Read and callback, then releases the inode/root/shared permit once.
func (r *ReadLease) Close() error {
	if r == nil {
		return nil
	}
	return r.finish()
}
