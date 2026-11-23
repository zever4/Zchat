//go:build linux

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type downloadRepositoryModel struct {
	E2EEBlobWriterRepository
	target  *domain.E2EEBlobReadTarget
	calls   atomic.Int32
	resolve func(context.Context, int32, *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error)
}

func (m *downloadRepositoryModel) ResolveRead(ctx context.Context, uid int64, storage string, req *domain.E2EEBlobReadRequest) (*domain.E2EEBlobReadTarget, error) {
	n := m.calls.Add(1)
	if uid != 2 || storage != m.target.StorageID {
		return nil, domain.ErrE2EEBlobMismatch
	}
	t := *m.target
	t.Request = copyBlobRead(m.target.Request)
	t.ObservedAt = time.Now()
	if m.resolve != nil {
		return m.resolve(ctx, n, &t)
	}
	return &t, nil
}

type downloadFixture struct {
	*uploadCoreFixture
	m      *downloadRepositoryModel
	req    *domain.E2EEBlobReadRequest
	budget *storagebudget.Budget
}

func downloadCore(t *testing.T) *downloadFixture {
	t.Helper()
	u := uploadCoreUnattached(t)
	b := testPhysicalBudget(t)
	a, err := u.s.files.AttachAccounting(t.Context(), b, nil, 128)
	if err != nil {
		t.Fatal(err)
	}
	u.s.physical, u.s.downloadLimit = a, b.DownloadCapacity()
	u.data = bytes.Repeat([]byte{0x42}, 32*1024*2+17)
	u.r = blobUploadIntent(u.data)
	preparedAt := time.Now()
	if _, err = u.s.Upload(t.Context(), "model-token", make([]byte, 32), u.r, io.NopCloser(bytes.NewReader(u.data))); err != nil {
		u.m.mu.Lock()
		state, checks, completes := u.m.state, u.m.checks, u.m.completes
		u.m.mu.Unlock()
		t.Fatalf("download fixture Upload failed after %s: %v (state=%s, writerChecks=%d, completes=%d, authCalls=%d); HEAD/download not invoked", time.Since(preparedAt), err, state, checks, completes, u.authCalls.Load())
	}
	r := u.r.Reservation
	req := &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Recipient.GenerationId}, Binding: &pb.EpochSendBinding{
		ProtocolVersion: 3, MessageId: r.MessageID, ConversationId: r.ConversationID, Epoch: *r.CryptoEpoch, Sender: r.Sender, Recipient: r.Recipient, EnvelopeDigest: bytes.Repeat([]byte{43}, 32)}, Blob: r.Blob}
	stored := time.Now().Add(-time.Second)
	m := &downloadRepositoryModel{E2EEBlobWriterRepository: u.m, target: &domain.E2EEBlobReadTarget{Request: copyBlobRead(req), StorageID: u.s.files.StorageID(), AttemptID: u.r.AttemptID, Revision: u.r.Revision, StoredAt: stored, ExpiresAt: stored.Add(domain.E2EEBlobAcceptedRetention)}}
	u.s.repo = m
	u.s.config.RecheckInterval = u.s.config.Timeout
	u.authCalls.Store(0)
	u.s.authorize = func(context.Context, string, string, []byte) (int64, func(), error) {
		u.authCalls.Add(1)
		u.guards.Add(1)
		return 2, func() { u.guards.Add(-1) }, nil
	}
	return &downloadFixture{u, m, req, b}
}

type downloadSink struct {
	bytes.Buffer
	writes, closes atomic.Int32
	write          func([]byte) (int, error)
}

func (s *downloadSink) Write(p []byte) (int, error) {
	s.writes.Add(1)
	if s.write != nil {
		return s.write(p)
	}
	return s.Buffer.Write(p)
}
func (s *downloadSink) Close() error { s.closes.Add(1); return nil }
func (f *downloadFixture) download(ctx context.Context, sink io.WriteCloser) error {
	return f.s.Download(ctx, "model-token", make([]byte, 32), f.req, sink)
}
func (f *downloadFixture) assertJoined(t *testing.T) {
	t.Helper()
	f.s.mu.Lock()
	count := len(f.s.downloads)
	f.s.mu.Unlock()
	if count != 0 || f.guards.Load() != 0 || f.budget.Active() {
		t.Fatal("download ownership/guard/permit leaked", count, f.guards.Load(), f.budget.Active())
	}
	if err := f.s.files.Verify(t.Context(), blobstore.Attempt{ID: f.r.AttemptID, Bytes: int64(len(f.data)), SHA256: [32]byte(f.req.Blob.Sha256)}); err != nil {
		t.Fatal(err)
	}
}
func TestBlobDownloadExactBytesAndFinalCheckOutsideIOGuards(t *testing.T) {
	f := downloadCore(t)
	sink := &downloadSink{}
	sink.write = func(p []byte) (int, error) {
		if f.guards.Load() != 0 || len(p) > 32<<10 {
			return 0, errors.New("guard held or unbounded write")
		}
		if _, err := f.s.files.RemoveVerified(t.Context(), f.r.AttemptID); !errors.Is(err, blobstore.ErrBusy) {
			return 0, errors.New("file not pinned during sink write")
		}
		return sink.Buffer.Write(p)
	}
	before, _ := f.budget.Usage()
	if err := f.download(t.Context(), sink); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sink.Bytes(), f.data) || sink.closes.Load() != 1 || sink.writes.Load() != 3 || f.m.calls.Load() != 3 {
		t.Fatal("bytes/checks/close changed")
	}
	if after, _ := f.budget.Usage(); before != after {
		t.Fatal("read changed retained charge")
	}
	f.assertJoined(t)
}
func TestBlobDownloadDeniesInitialAndRevalidationFailuresBeforeBytes(t *testing.T) {
	for _, name := range []string{"auth", "unknown commit", "unavailable", "changed attempt", "changed ref", "wrong root", "bad retention", "nil target", "recheck auth"} {
		t.Run(name, func(t *testing.T) {
			f := downloadCore(t)
			want := errBlobSynthetic
			if name == "auth" || name == "recheck auth" {
				f.s.authorize = func(context.Context, string, string, []byte) (int64, func(), error) {
					if f.authCalls.Add(1) == 1 && name == "recheck auth" {
						return 2, func() {}, nil
					}
					return 0, nil, want
				}
			} else {
				f.m.resolve = func(_ context.Context, n int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
					if n == 1 {
						return target, nil
					}
					switch name {
					case "unknown commit":
						return nil, want
					case "unavailable":
						return nil, domain.ErrE2EEBlobReadUnavailable
					case "changed attempt":
						target.AttemptID = uuid.NewString()
					case "changed ref":
						target.Request.Blob.Sha256[0] ^= 1
					case "wrong root":
						target.StorageID = uuid.NewString()
					case "bad retention":
						target.ExpiresAt = target.ExpiresAt.Add(time.Second)
					case "nil target":
						return nil, nil
					}
					return target, nil
				}
				if name == "unavailable" {
					want = domain.ErrE2EEBlobReadUnavailable
				} else if name != "unknown commit" {
					want = domain.ErrE2EEBlobMismatch
				}
			}
			sink := &downloadSink{}
			if err := f.download(t.Context(), sink); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if sink.writes.Load() != 0 || sink.closes.Load() != 1 {
				t.Fatal("bytes before fresh recheck or sink leaked")
			}
			f.assertJoined(t)
		})
	}
}
func TestBlobDownloadFinalRecheckAndSinkFailureCannotClaimSuccess(t *testing.T) {
	for _, name := range []string{"final auth", "final metadata", "short write", "write error", "corrupt file"} {
		t.Run(name, func(t *testing.T) {
			f := downloadCore(t)
			sink := &downloadSink{}
			want := errBlobSynthetic
			switch name {
			case "final auth":
				f.s.authorize = func(context.Context, string, string, []byte) (int64, func(), error) {
					if f.authCalls.Add(1) == 3 {
						return 0, nil, want
					}
					return 2, func() {}, nil
				}
			case "final metadata":
				f.m.resolve = func(_ context.Context, n int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
					if n == 3 {
						return nil, want
					}
					return target, nil
				}
			case "short write":
				sink.write = func(p []byte) (int, error) { return len(p) - 1, nil }
				want = io.ErrShortWrite
			case "write error":
				sink.write = func([]byte) (int, error) { return 0, want }
			case "corrupt file":
				data := append([]byte(nil), f.data...)
				data[0] ^= 1
				if err := os.WriteFile(filepath.Join(f.path, f.r.AttemptID+".blob"), data, 0600); err != nil {
					t.Fatal(err)
				}
				want = blobstore.ErrIntegrity
			}
			if err := f.download(t.Context(), sink); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if name == "corrupt file" {
				if err := os.WriteFile(filepath.Join(f.path, f.r.AttemptID+".blob"), f.data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			f.assertJoined(t)
		})
	}
}

type heldDownloadSink struct {
	entered, closed, release          chan struct{}
	writeOnce, closeOnce, releaseOnce sync.Once
	closes                            atomic.Int32
	autoRelease                       bool
}

func heldSink(auto bool) *heldDownloadSink {
	return &heldDownloadSink{entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{}), autoRelease: auto}
}
func (s *heldDownloadSink) Write([]byte) (int, error) {
	s.writeOnce.Do(func() { close(s.entered) })
	<-s.release
	return 0, io.ErrClosedPipe
}
func (s *heldDownloadSink) unblock() { s.releaseOnce.Do(func() { close(s.release) }) }
func (s *heldDownloadSink) Close() error {
	s.closes.Add(1)
	s.closeOnce.Do(func() { close(s.closed) })
	if s.autoRelease {
		s.unblock()
	}
	return nil
}
func awaitDownload(t *testing.T, c <-chan struct{}) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(2 * time.Second):
		t.Fatal("download barrier not reached")
	}
}
func awaitDownloadResult(t *testing.T, c <-chan error) error {
	t.Helper()
	select {
	case e := <-c:
		return e
	case <-time.After(2 * time.Second):
		t.Fatal("download did not join")
		return nil
	}
}

func TestBlobDownloadPeriodicRevocationAndUnavailableInterruptSink(t *testing.T) {
	for _, reason := range []error{domain.ErrInvalidSession, domain.ErrE2EEGenerationChanged, domain.ErrE2EEBlobReadUnavailable, ErrAuthUnavailable, errBlobSynthetic} {
		t.Run(reason.Error(), func(t *testing.T) {
			f := downloadCore(t)
			f.s.config.RecheckInterval = 10 * time.Millisecond
			sink := heldSink(true)
			f.m.resolve = func(ctx context.Context, n int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
				if n <= 2 {
					return target, nil
				}
				select {
				case <-sink.entered:
					return nil, reason
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			done := make(chan error, 1)
			go func() { done <- f.download(t.Context(), sink) }()
			awaitDownload(t, sink.entered)
			if err := awaitDownloadResult(t, done); !errors.Is(err, reason) {
				t.Fatal(err)
			}
			if sink.closes.Load() != 1 {
				t.Fatal("sink close not once")
			}
			f.assertJoined(t)
		})
	}
}
func TestBlobDownloadCloseWaitsLastSinkWriteBeforeRootUnlock(t *testing.T) {
	f := downloadCore(t)
	sink := heldSink(false)
	defer sink.unblock()
	done := make(chan error, 1)
	go func() { done <- f.download(t.Context(), sink) }()
	awaitDownload(t, sink.entered)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := f.s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	awaitDownload(t, sink.closed)
	if !f.budget.Active() {
		t.Fatal("shared permit released before sink joined")
	}
	if _, err := f.s.files.RemoveVerified(t.Context(), f.r.AttemptID); !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal(err)
	}
	if other, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1, WriteTimeout: time.Second}); other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("root unlocked before sink join", err)
	}
	sink.unblock()
	if err := awaitDownloadResult(t, done); !errors.Is(err, blobstore.ErrClosed) {
		t.Fatal(err)
	}
	if err := f.s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func TestBlobDownloadCancelBeforePinJoinsAndStopsDispatch(t *testing.T) {
	f := downloadCore(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.m.resolve = func(_ context.Context, n int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
		if n == 1 {
			close(entered)
			<-release
		}
		return target, nil
	}
	sink := &downloadSink{}
	done := make(chan error, 1)
	go func() { done <- f.download(t.Context(), sink) }()
	awaitDownload(t, entered)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := f.s.CancelDownloads(ctx, f.req.Binding.MessageId); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unjoined DB treated as complete", err)
	}
	once.Do(func() { close(release) })
	if err := awaitDownloadResult(t, done); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal(err)
	}
	if sink.writes.Load() != 0 {
		t.Fatal("dispatch after GC cancellation")
	}
	f.assertJoined(t)
}
func TestBlobDownloadPendingAdmissionBoundedBeforeAuth(t *testing.T) {
	f := downloadCore(t)
	f.s.downloadLimit = 1
	sink := heldSink(true)
	defer sink.unblock()
	done := make(chan error, 1)
	go func() { done <- f.download(t.Context(), sink) }()
	awaitDownload(t, sink.entered)
	before := f.authCalls.Load()
	extra := &downloadSink{}
	if err := f.download(t.Context(), extra); !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal(err)
	}
	if f.authCalls.Load() != before || extra.writes.Load() != 0 || extra.closes.Load() != 1 {
		t.Fatal("capacity rejected after auth or leaked sink")
	}
	if err := f.s.CancelDownloads(t.Context(), f.req.Binding.MessageId); err != nil {
		t.Fatal(err)
	}
	if err := awaitDownloadResult(t, done); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal(err)
	}
	f.assertJoined(t)
}
func TestBlobDownloadAcceptedDeadlineInterruptsBlockedSink(t *testing.T) {
	f := downloadCore(t)
	f.m.target.StoredAt = time.Now().Add(-domain.E2EEBlobAcceptedRetention + 500*time.Millisecond)
	f.m.target.ExpiresAt = f.m.target.StoredAt.Add(domain.E2EEBlobAcceptedRetention)
	sink := heldSink(true)
	done := make(chan error, 1)
	go func() { done <- f.download(t.Context(), sink) }()
	awaitDownload(t, sink.entered)
	if err := awaitDownloadResult(t, done); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal("deadline did not interrupt sink", err)
	}
	f.assertJoined(t)
}

func TestBlobDownloadRejectsInvalidUnavailableAndCancelledWithoutAuth(t *testing.T) {
	for _, name := range []string{"invalid", "not ready", "closed", "cancelled", "no read repository"} {
		t.Run(name, func(t *testing.T) {
			f := downloadCore(t)
			ctx := t.Context()
			var want error
			switch name {
			case "invalid":
				f.req.Binding.ProtocolVersion = 2
				want = boundary.ErrInvalid
			case "not ready":
				f.s.ready = false
				want = domain.ErrE2EEBlobNotReady
			case "closed":
				f.s.closed = true
				want = blobstore.ErrClosed
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				want = context.Canceled
			case "no read repository":
				f.s.repo = f.uploadCoreFixture.m
				want = domain.ErrE2EEBlobNotReady
			}
			sink := &downloadSink{}
			if err := f.download(ctx, sink); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if sink.writes.Load() != 0 || sink.closes.Load() != 1 || f.m.calls.Load() != 0 {
				t.Fatal("rejected operation touched data or leaked sink")
			}
			f.assertJoined(t)
		})
	}
}

func TestBlobDownloadParentCancellationAndOperationTimeoutInterruptSink(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "parent cause", true: "operation timeout"}[timeout], func(t *testing.T) {
			f := downloadCore(t)
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			want := errBlobSynthetic
			if timeout {
				f.s.config.Timeout = 300 * time.Millisecond
				f.s.config.RecheckInterval = f.s.config.Timeout
				want = context.DeadlineExceeded
			}
			sink := heldSink(true)
			done := make(chan error, 1)
			go func() { done <- f.download(ctx, sink) }()
			awaitDownload(t, sink.entered)
			if !timeout {
				cancel(want)
			}
			if err := awaitDownloadResult(t, done); !errors.Is(err, want) {
				t.Fatal(err)
			}
			if sink.closes.Load() != 1 {
				t.Fatal("sink not interrupted exactly once")
			}
			f.assertJoined(t)
		})
	}
}

func TestBlobDownloadCancelJoinsAllReadersOfMessage(t *testing.T) {
	f := downloadCore(t)
	first, second := heldSink(true), heldSink(false)
	defer second.unblock()
	a, b := make(chan error, 1), make(chan error, 1)
	go func() { a <- f.download(t.Context(), first) }()
	go func() { b <- f.download(t.Context(), second) }()
	awaitDownload(t, first.entered)
	awaitDownload(t, second.entered)
	if err := f.s.CancelDownloads(t.Context(), uuid.NewString()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := f.s.CancelDownloads(ctx, f.req.Binding.MessageId); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cancel joined only one reader", err)
	}
	if err := awaitDownloadResult(t, a); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal(err)
	}
	if _, err := f.s.files.RemoveVerified(t.Context(), f.r.AttemptID); !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal("last reader lost its pin", err)
	}
	second.unblock()
	if err := awaitDownloadResult(t, b); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal(err)
	}
	f.assertJoined(t)
}

func TestBlobDownloadSharedReadBudgetRefusesBeforeSink(t *testing.T) {
	f := downloadCore(t)
	var permits []*storagebudget.ReadPermit
	defer func() {
		for _, p := range permits {
			p.Close()
		}
	}()
	for range 1025 {
		p, err := f.budget.AcquireRead(t.Context(), 2)
		if errors.Is(err, storagebudget.ErrBusy) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		permits = append(permits, p)
	}
	if len(permits) == 0 || len(permits) > 1024 {
		t.Fatal("invalid budget capacity")
	}
	sink := &downloadSink{}
	if err := f.download(t.Context(), sink); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal(err)
	}
	if sink.writes.Load() != 0 || sink.closes.Load() != 1 || f.m.calls.Load() != 1 {
		t.Fatal("budget bypassed before pin/stream")
	}
	for _, p := range permits {
		p.Close()
	}
	f.assertJoined(t)
}
