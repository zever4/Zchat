//go:build linux

package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type runtimeDownloadSink struct {
	writes, closes int
	close          func()
}

func (s *runtimeDownloadSink) Write(p []byte) (int, error) { s.writes++; return len(p), nil }
func (s *runtimeDownloadSink) Close() error {
	s.closes++
	if s.close != nil {
		s.close()
	}
	return nil
}

func TestBlobRuntimeDownloadUsesExistingExposureGate(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tc := range []struct {
		name    string
		runtime *blobStorageRuntime
		want    error
	}{
		{"absent", nil, domain.ErrE2EEBlobNotReady},
		{"maintenance", &blobStorageRuntime{life: t.Context()}, domain.ErrE2EEBlobNotReady},
		{"closing", &blobStorageRuntime{life: t.Context(), closing: true, expose: true}, blobstore.ErrClosed},
		{"cancelled runtime", &blobStorageRuntime{life: ctx, expose: true}, blobstore.ErrClosed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &runtimeDownloadSink{}
			if err := tc.runtime.Download(t.Context(), "", nil, nil, sink); !errors.Is(err, tc.want) {
				t.Fatal(err)
			}
			if sink.writes != 0 || sink.closes != 1 {
				t.Fatal("rejected download leaked sink")
			}
		})
	}
}

func TestBlobRuntimeDownloadJoinsRejectedSinkCleanupBeforeUnlock(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	r := startBlobModel(t, c, web, b.Budget(), &blobRuntimeModel{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	sink := &runtimeDownloadSink{close: func() { close(entered); <-release }}
	done := make(chan error, 1)
	go func() { done <- r.Download(t.Context(), "", nil, nil, sink) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("sink cleanup did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := r.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("runtime skipped download cleanup", err)
	}
	if other, err := blobstore.Open(c.Root, c.Files); other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("runtime root released before sink cleanup", err)
	}
	once.Do(func() { close(release) })
	select {
	case err := <-done:
		if !errors.Is(err, boundary.ErrInvalid) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("download cleanup did not join")
	}
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestBlobRuntimeDownloadRoutesUseExistingOptInAndHeadGate(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		m := http.NewServeMux()
		r := &blobStorageRuntime{expose: enabled}
		r.Register(m)
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			w := httptest.NewRecorder()
			m.ServeHTTP(w, httptest.NewRequest(method, "/api/e2ee/attachments/11111111-1111-4111-8111-111111111111", nil))
			want := 404
			if enabled {
				want = 400
			}
			if w.Code != want {
				t.Fatal(method, enabled, w.Code)
			}
		}
	}
	for _, r := range []*blobStorageRuntime{nil, {life: t.Context()}, {life: t.Context(), expose: true, closing: true}} {
		if err := r.Head(t.Context(), "", nil, nil, nil); err == nil {
			t.Fatal("HEAD bypassed runtime gate")
		}
	}
}
