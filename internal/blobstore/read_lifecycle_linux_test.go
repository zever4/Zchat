package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/storagebudget"
)

func fakeReadLease(t *testing.T, s *Store, b *storagebudget.Budget, ctx context.Context, ref Attempt, src io.ReadCloser) *ReadLease {
	t.Helper()
	permit, err := b.AcquireRead(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.pinRead(ctx, ref.ID); err != nil {
		permit.Close()
		t.Fatal(err)
	}
	return newReadLease(ctx, s, ref, src, permit)
}
func awaitReadEvent(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Second):
		t.Fatal("read fixture barrier not reached")
	}
}

type heldRead struct {
	entered, closed, release chan struct{}
	closeOnce                sync.Once
	closeCalls               atomic.Int32
}

func (r *heldRead) Read(p []byte) (int, error) {
	close(r.entered)
	<-r.release
	copy(p, []byte{1})
	return 1, nil
}
func (r *heldRead) Close() error {
	r.closeCalls.Add(1)
	r.closeOnce.Do(func() { close(r.closed) })
	return nil
}

func TestCiphertextReadCancellationAndCloseJoinLastReadBeforeUnpin(t *testing.T) {
	s, a, _, b, _ := readFixture(t, 1, 1)
	ref := readFile(t, s, a, bytes.Repeat([]byte{49}, 64))
	src := &heldRead{entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(src.release) })
	ctx, cancel := context.WithCancelCause(t.Context())
	defer cancel(nil)
	r := fakeReadLease(t, s, b, ctx, ref, src)
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	readDone := make(chan error, 1)
	go func() {
		n, err := r.Read(make([]byte, 64))
		if n != 0 {
			readDone <- errors.New("cancelled read exposed another chunk")
			return
		}
		readDone <- err
	}()
	awaitReadEvent(t, src.entered)
	revoked := errors.New("synthetic revoked read scope")
	cancel(revoked)
	awaitReadEvent(t, src.closed)
	assertReadPins(t, s, b, ref.ID, 1)
	if _, err := s.RemoveVerified(t.Context(), ref.ID); !errors.Is(err, ErrBusy) {
		t.Fatal("cancel released live read pin", err)
	}
	if err := s.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("cancel released root before Read joined", err)
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- r.Close() }()
	select {
	case err := <-closeDone:
		t.Fatal("Close did not wait for last Read", err)
	case <-time.After(20 * time.Millisecond):
	}
	release.Do(func() { close(src.release) })
	if err := <-readDone; !errors.Is(err, revoked) {
		t.Fatal("lost cancellation cause", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if src.closeCalls.Load() != 1 {
		t.Fatal("source closed more than once")
	}
	assertReadPins(t, s, b, ref.ID, 0)
}

func TestCiphertextReadCancellationWithoutReadStillNeedsOwnerClose(t *testing.T) {
	s, a, _, b, _ := readFixture(t, 1, 1)
	ref := readFile(t, s, a, bytes.Repeat([]byte{50}, 64))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	r, err := a.OpenRead(ctx, 2, ref)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	cancel()
	awaitReadEvent(t, r.callbackDone)
	assertReadPins(t, s, b, ref.ID, 1)
	if _, err = b.AcquireRead(t.Context(), 2); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal("cancel freed permit before owner joined sink", err)
	}
	if _, err = r.Read(make([]byte, 64)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	assertReadPins(t, s, b, ref.ID, 0)
}

type stalledRead struct {
	data      []byte
	probeOnly bool
	calls     atomic.Int32
	closed    atomic.Int32
}

func (r *stalledRead) Read(p []byte) (int, error) {
	r.calls.Add(1)
	if r.probeOnly && len(r.data) > 0 {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	return 0, nil
}
func (r *stalledRead) Close() error { r.closed.Add(1); return nil }
func TestCiphertextReadNoProgressIsBoundedForBodyAndExcessProbe(t *testing.T) {
	for _, probe := range []bool{false, true} {
		t.Run(map[bool]string{false: "body", true: "probe"}[probe], func(t *testing.T) {
			s, a, _, b, _ := readFixture(t, 1, 1)
			data := bytes.Repeat([]byte{51}, 64)
			ref := readFile(t, s, a, data)
			src := &stalledRead{data: data, probeOnly: probe}
			r := fakeReadLease(t, s, b, t.Context(), ref, src)
			defer r.Close()
			if _, err := io.Copy(io.Discard, r); !errors.Is(err, io.ErrNoProgress) {
				t.Fatal("stalled reader did not terminate", err)
			}
			if src.calls.Load() > 101 {
				t.Fatal("unbounded empty reads")
			}
			assertReadPins(t, s, b, ref.ID, 1)
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			assertReadPins(t, s, b, ref.ID, 0)
		})
	}
}

type failingReadClose struct {
	io.ReadCloser
	calls   atomic.Int32
	failure error
}

func (r *failingReadClose) Close() error {
	r.calls.Add(1)
	if err := r.ReadCloser.Close(); err != nil {
		return err
	}
	return r.failure
}
func TestCiphertextReadUncertainCloseRetainsRootAndSharedPermit(t *testing.T) {
	s, a, _, b, _ := readFixture(t, 1, 1)
	data := bytes.Repeat([]byte{52}, 64)
	ref := readFile(t, s, a, data)
	want := errors.New("synthetic descriptor-close uncertainty")
	src := &failingReadClose{ReadCloser: io.NopCloser(bytes.NewReader(data)), failure: want}
	r := fakeReadLease(t, s, b, t.Context(), ref, src)
	for range 2 {
		if err := r.Close(); !errors.Is(err, want) {
			t.Fatal("uncertain close hidden", err)
		}
	}
	if src.calls.Load() != 1 {
		t.Fatal("hidden descriptor close retry")
	}
	assertReadPins(t, s, b, ref.ID, 1)
	if err := s.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("uncertain descriptor dropped owner", err)
	}
	if _, err := s.RemoveVerified(t.Context(), ref.ID); !errors.Is(err, ErrBusy) {
		t.Fatal("uncertain close allowed GC", err)
	}
	// Test-only cleanup of a fake, already closed in-memory source; production
	// has no force-unpin API and must investigate/restart on uncertain close.
	s.unpinRead(ref.ID)
	r.permit.Close()
}

func TestCiphertextReadCloseWithoutReadAndZeroLengthDoNotVerifyOrDelete(t *testing.T) {
	s, a, _, b, _ := readFixture(t, 1, 1)
	data := bytes.Repeat([]byte{53}, 64)
	ref := readFile(t, s, a, data)
	r := lease(t, a, 2, ref)
	if n, err := r.Read(nil); n != 0 || err != nil {
		t.Fatal(n, err)
	}
	if r.terminal != nil || r.remaining != ref.Bytes {
		t.Fatal("empty read claimed completion")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	assertReadPins(t, s, b, ref.ID, 0)
	if err := s.Verify(t.Context(), ref); err != nil {
		t.Fatal("early close removed data", err)
	}
}
