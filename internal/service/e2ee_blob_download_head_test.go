//go:build linux

package service

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
)

type blobHeadSink struct{}

func (blobHeadSink) Write([]byte) (int, error) { return 0, domain.ErrInvalidInput }
func (blobHeadSink) Close() error              { return nil }

func TestBlobDownloadHeadPinsSafeFileWithoutReadingOrHashingBody(t *testing.T) {
	for _, name := range []string{"success", "missing", "wrong size", "body hash not read", "final metadata failure"} {
		t.Run(name, func(t *testing.T) {
			f := downloadCore(t)
			path := filepath.Join(f.path, f.r.AttemptID+".blob")
			var want error
			switch name {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				want = os.ErrNotExist
			case "wrong size":
				if err := os.Truncate(path, int64(len(f.data)-1)); err != nil {
					t.Fatal(err)
				}
				want = blobstore.ErrIntegrity
			case "body hash not read":
				data := append([]byte(nil), f.data...)
				data[0] ^= 1
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "final metadata failure":
				want = errBlobSynthetic
				f.m.resolve = func(_ context.Context, n int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
					if n == 3 {
						return nil, want
					}
					return target, nil
				}
			}
			err := f.s.Head(t.Context(), "model-token", make([]byte, 32), f.req, blobHeadSink{})
			if want == nil && err != nil || want != nil && !errors.Is(err, want) {
				t.Fatal(err)
			}
			if name == "success" || name == "body hash not read" {
				if f.m.calls.Load() != 3 {
					t.Fatal("HEAD skipped revalidation")
				}
			}
			if err := os.WriteFile(path, f.data, 0600); err != nil {
				t.Fatal(err)
			} // restore test-only damaged fixture
			f.assertJoined(t)
		})
	}
}

type downloadFinalizerModel struct {
	downloadSink
	finish func() error
	calls  int
}

func (s *downloadFinalizerModel) FinishDownload() error { s.calls++; return s.finish() }
func TestBlobDownloadFinalizerRunsAfterIntegrityAndFinalAuthWhilePinned(t *testing.T) {
	for _, name := range []string{"success", "finish error", "final metadata error"} {
		t.Run(name, func(t *testing.T) {
			f := downloadCore(t)
			sink := &downloadFinalizerModel{}
			sink.finish = func() error {
				if f.guards.Load() != 0 {
					t.Error("finalizer retained its auth guard")
				}
				if _, err := f.s.files.RemoveVerified(t.Context(), f.r.AttemptID); !errors.Is(err, blobstore.ErrBusy) {
					t.Error("finalizer lost file pin", err)
				}
				if name == "finish error" {
					return errBlobSynthetic
				}
				return nil
			}
			if name == "final metadata error" {
				f.m.resolve = func(_ context.Context, n int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
					if n == 3 {
						return nil, errBlobSynthetic
					}
					return target, nil
				}
			}
			err := f.download(t.Context(), sink)
			if name == "success" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, errBlobSynthetic) {
				t.Fatal(err)
			}
			if name == "final metadata error" {
				if sink.calls != 0 {
					t.Fatal("finish before final auth")
				}
			} else if sink.calls != 1 {
				t.Fatal("missing/duplicate finalization")
			}
			f.assertJoined(t)
		})
	}
}

type heldDownloadFinalizer struct {
	*heldDownloadSink
	finishing atomic.Bool
}

func (s *heldDownloadFinalizer) Write(p []byte) (int, error) { return len(p), nil }
func (s *heldDownloadFinalizer) FinishDownload() error {
	s.finishing.Store(true)
	_, err := s.heldDownloadSink.Write(nil)
	return err
}
func TestBlobDownloadFinalizerStillRevalidatesAndInterruptsBlockedFlush(t *testing.T) {
	for _, head := range []bool{false, true} {
		t.Run(map[bool]string{false: "GET", true: "HEAD"}[head], func(t *testing.T) {
			f := downloadCore(t)
			f.s.config.RecheckInterval = 10 * time.Millisecond
			sink := &heldDownloadFinalizer{heldDownloadSink: heldSink(true)}
			allow := make(chan struct{})
			defer sink.unblock()
			f.m.resolve = func(ctx context.Context, _ int32, target *domain.E2EEBlobReadTarget) (*domain.E2EEBlobReadTarget, error) {
				if sink.finishing.Load() {
					select {
					case <-allow:
						return nil, domain.ErrInvalidSession
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return target, nil
			}
			done := make(chan error, 1)
			go func() {
				if head {
					done <- f.s.Head(t.Context(), "model-token", make([]byte, 32), f.req, sink)
				} else {
					done <- f.download(t.Context(), sink)
				}
			}()
			awaitDownload(t, sink.entered)
			if _, err := f.s.files.RemoveVerified(t.Context(), f.r.AttemptID); !errors.Is(err, blobstore.ErrBusy) {
				t.Fatal("flush released file pin", err)
			}
			close(allow)
			if err := awaitDownloadResult(t, done); !errors.Is(err, domain.ErrInvalidSession) {
				t.Fatal(err)
			}
			if sink.closes.Load() != 1 {
				t.Fatal("flush not interrupted")
			}
			f.assertJoined(t)

		})
	}
}

var _ io.WriteCloser = (*heldDownloadFinalizer)(nil)
