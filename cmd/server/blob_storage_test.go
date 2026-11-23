//go:build linux

package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/uploadstorage"
)

type blobRuntimeModel struct {
	blobRuntimeRepository
	configured     atomic.Int32
	configureError error
	query          func(context.Context)
	control        func(context.Context, bool) (*domain.E2EEBlobReservation, error)
	fileAck        func(context.Context) (*v2.FileAcknowledgements, error)
	fileStatuses   func(context.Context) (*v2.FileStatuses, error)
}

func (m *blobRuntimeModel) Reserve(ctx context.Context, _ int64, _ *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	return m.control(ctx, false)
}
func (m *blobRuntimeModel) Observe(ctx context.Context, _ int64, _ *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	return m.control(ctx, true)
}
func (m *blobRuntimeModel) AcknowledgeFiles(ctx context.Context, _ int64, _ *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
	return m.fileAck(ctx)
}
func (m *blobRuntimeModel) FileStatuses(ctx context.Context, _ int64, _ *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
	return m.fileStatuses(ctx)
}

func (m *blobRuntimeModel) Configure(context.Context, domain.E2EEBlobLimits) error {
	m.configured.Add(1)
	return m.configureError
}
func (*blobRuntimeModel) BindStorage(context.Context, string) error { return nil }
func (*blobRuntimeModel) Inventory(context.Context, string, string, int) ([]domain.E2EEBlobInventoryEntry, error) {
	return nil, nil
}
func (m *blobRuntimeModel) CleanupCandidates(c context.Context, _ string, _ domain.E2EEBlobCleanupLane, _ *domain.E2EEBlobCleanupPosition, _ int) ([]domain.E2EEBlobCleanupCandidate, error) {
	if m.query != nil {
		m.query(c)
	}
	return nil, c.Err()
}

func blobRuntimeDirectories(t *testing.T) (blobStorageConfig, *os.Root, *uploadstorage.Manager) {
	t.Helper()
	path := t.TempDir()
	webPath := filepath.Join(path, "web")
	private := filepath.Join(path, "blobs")
	for _, dir := range []string{webPath, private} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	web, err := os.OpenRoot(webPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { web.Close() })
	v := blobConfigValues()
	v["E2EE_BLOB_ROOT"] = private
	c, err := testBlobConfig(t, v)
	if err != nil {
		t.Fatal(err)
	}
	c.Collector.Interval, c.Collector.RetryBase = 10*time.Millisecond, 10*time.Millisecond
	shared, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	shared.FreeBytes, shared.FreeEntries = 1, 1
	m, err := uploadstorage.Open(t.Context(), filepath.Join(webPath, "uploads"), shared)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return c, web, m
}
func startBlobModel(t *testing.T, c blobStorageConfig, web *os.Root, b *storagebudget.Budget, m *blobRuntimeModel) *blobStorageRuntime {
	t.Helper()
	r, err := startBlobStorage(t.Context(), c, web, b, m, &service.AuthService{}, &service.E2EEDeviceService{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}
func TestBlobRuntimeDisabledHasNoSideEffectsOrRoute(t *testing.T) {
	m := &blobRuntimeModel{}
	r, err := startBlobStorage(t.Context(), blobStorageConfig{}, nil, nil, m, nil, nil, nil)
	if err != nil || r != nil || m.configured.Load() != 0 {
		t.Fatal(r, err)
	}
	mux := http.NewServeMux()
	r.Register(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("PUT", "/api/e2ee/blobs/anything", nil))
	if w.Code != 404 {
		t.Fatal(w.Code)
	}
}
func TestBlobRuntimeRejectsPublicMissingSymlinkAndUnsafeRoots(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	inside := filepath.Join(web.Name(), "private")
	if err := os.Mkdir(inside, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(filepath.Dir(c.Root), "link")
	if err := os.Symlink(c.Root, link); err != nil {
		t.Fatal(err)
	}
	publicLink := filepath.Join(filepath.Dir(c.Root), "public-alias")
	if err := os.Symlink(web.Name(), publicLink); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{web.Name(), inside, filepath.Join(publicLink, "private"), link, filepath.Join(c.Root, "missing")} {
		cfg := c
		cfg.Root = path
		m := &blobRuntimeModel{}
		if r, err := startBlobStorage(t.Context(), cfg, web, b.Budget(), m, &service.AuthService{}, &service.E2EEDeviceService{}, nil); r != nil || err == nil || m.configured.Load() != 0 {
			t.Fatal("unsafe root reached DB", path, r, err)
		}
	}
	if err := os.Chmod(c.Root, 0755); err != nil {
		t.Fatal(err)
	}
	if r, err := startBlobStorage(t.Context(), c, web, b.Budget(), &blobRuntimeModel{}, &service.AuthService{}, &service.E2EEDeviceService{}, nil); r != nil || !errors.Is(err, blobstore.ErrRoot) {
		t.Fatal(r, err)
	}
}
func TestRootedStaticServerCannotFollowPrivateSymlink(t *testing.T) {
	c, web, _ := blobRuntimeDirectories(t)
	if err := os.WriteFile(filepath.Join(c.Root, "secret.blob"), []byte("private-ciphertext"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(c.Root, filepath.Join(web.Name(), "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(web.Name(), "ok.txt"), []byte("public-asset"), 0600); err != nil {
		t.Fatal(err)
	}
	server := http.FileServerFS(web.FS())
	for _, tc := range []struct {
		path string
		want int
	}{{"/escape/secret.blob", 500}, {"/ok.txt", 200}} {
		w := httptest.NewRecorder()
		server.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.want || strings.Contains(w.Body.String(), "private-ciphertext") {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
	}
}
func TestBlobRuntimeCollectorAndCloseKeepOwnerThroughBlockedQuery(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	m := &blobRuntimeModel{query: func(context.Context) { close(entered); <-release }}
	r := startBlobModel(t, c, web, b.Budget(), m)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("collector did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := r.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if other, err := blobstore.Open(c.Root, c.Files); other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("root unlocked before query joined", err)
	}
	src := &closedRuntimeSource{}
	if _, err := r.Upload(t.Context(), "", nil, nil, src); !errors.Is(err, blobstore.ErrClosed) || src.reads != 0 || !src.closed {
		t.Fatal("closing runtime admitted source", err, src)
	}
	once.Do(func() { close(release) })
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if other, err := blobstore.Open(c.Root, c.Files); err != nil {
		t.Fatal(err)
	} else {
		other.Close()
	}
}

type closedRuntimeSource struct {
	reads  int
	closed bool
}

func (r *closedRuntimeSource) Read([]byte) (int, error) { r.reads++; return 0, io.EOF }
func (r *closedRuntimeSource) Close() error             { r.closed = true; return nil }

func TestBlobRuntimeCanceledStartupDoesNotCreateIdentity(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	m := &blobRuntimeModel{}
	if r, err := startBlobStorage(ctx, c, web, b.Budget(), m, &service.AuthService{}, &service.E2EEDeviceService{}, nil); r != nil || !errors.Is(err, context.Canceled) || m.configured.Load() != 0 {
		t.Fatal(r, err)
	}
	if _, err := os.Stat(filepath.Join(c.Root, ".store-id")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("canceled startup created root identity", err)
	}
}
func TestBlobRuntimeImmediateCloseJoinsCollector(t *testing.T) {
	for range 5 {
		c, web, b := blobRuntimeDirectories(t)
		r := startBlobModel(t, c, web, b.Budget(), &blobRuntimeModel{})
		if err := r.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
		select {
		case <-r.collectorDone:
		default:
			t.Fatal("close returned before collector wrapper joined")
		}
		if err := r.Close(t.Context()); err != nil {
			t.Fatal("repeated close", err)
		}
	}
}

func TestBlobRuntimeInitializedRootCannotBeSilentlyOmittedFromBudget(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	r := startBlobModel(t, c, web, b.Budget(), &blobRuntimeModel{})
	if err := r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	c.Enabled = false
	if result, err := startBlobStorage(t.Context(), c, web, b.Budget(), &blobRuntimeModel{}, nil, nil, nil); result != nil || err == nil {
		t.Fatal("initialized root omitted from accounting", result, err)
	}
}
func TestBlobRuntimeStartupErrorClosesRootWithoutReset(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	m := &blobRuntimeModel{configureError: domain.ErrE2EEBlobConfiguration}
	if r, err := startBlobStorage(t.Context(), c, web, b.Budget(), m, &service.AuthService{}, &service.E2EEDeviceService{}, nil); r != nil || !errors.Is(err, domain.ErrE2EEBlobConfiguration) {
		t.Fatal(r, err)
	}
	marker, err := os.ReadFile(filepath.Join(c.Root, ".store-id"))
	if err != nil {
		t.Fatal(err)
	}
	f, err := blobstore.Open(c.Root, c.Files)
	if err != nil {
		t.Fatal("startup error leaked owner", err)
	}
	f.Close()
	again, err := os.ReadFile(filepath.Join(c.Root, ".store-id"))
	if err != nil || string(marker) != string(again) {
		t.Fatal("startup failure reset root identity", err)
	}
}

func TestBlobRuntimeControlsJoinBeforeRootUnlock(t *testing.T) {
	for _, mode := range []string{"reserve", "observe", "file-ack", "file-status"} {
		t.Run(mode, func(t *testing.T) {
			c, web, b := blobRuntimeDirectories(t)
			entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var calls atomic.Int32
			m := &blobRuntimeModel{control: func(ctx context.Context, got bool) (*domain.E2EEBlobReservation, error) {
				calls.Add(1)
				if got != (mode == "observe") {
					t.Error("wrong control")
				}
				close(entered)
				<-ctx.Done()
				close(canceled)
				<-release
				return nil, ctx.Err()
			}}
			m.fileAck = func(ctx context.Context) (*v2.FileAcknowledgements, error) {
				_, err := m.control(ctx, false)
				return nil, err
			}
			m.fileStatuses = func(ctx context.Context) (*v2.FileStatuses, error) {
				_, err := m.control(ctx, false)
				return nil, err
			}
			r := startBlobModel(t, c, web, b.Budget(), m)
			method := func(ctx context.Context, uid int64, req *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
				if mode == "file-status" {
					_, err := r.FileStatuses(ctx, uid, nil)
					return nil, err
				}
				if mode == "file-ack" {
					_, err := r.AcknowledgeFiles(ctx, uid, nil)
					return nil, err
				}
				if mode == "observe" {
					return r.Observe(ctx, uid, req)
				}
				return r.Reserve(ctx, uid, req)
			}
			done := make(chan error, 1)
			go func() { _, err := method(t.Context(), 1, nil); done <- err }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("control did not start")
			}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
			defer cancel()
			if err := r.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Fatal("shutdown did not cancel control")
			}
			if other, err := blobstore.Open(c.Root, c.Files); other != nil || !errors.Is(err, blobstore.ErrBusy) {
				if other != nil {
					other.Close()
				}
				t.Fatal("root unlocked before control joined", err)
			}
			if _, err := method(t.Context(), 1, nil); !errors.Is(err, blobstore.ErrClosed) || calls.Load() != 1 {
				t.Fatal("new control admitted during close", err)
			}
			once.Do(func() { close(release) })
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if err := r.Close(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBlobRuntimeAttachmentSendSharesExposureCancelAndJoin(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	r := startBlobModel(t, c, web, b.Budget(), &blobRuntimeModel{})
	ctx, finish, err := r.BeginAttachmentSend(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var joined sync.Once
	defer joined.Do(finish)
	deadline, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err = r.Close(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Send not joined", err)
	}
	if ctx.Err() == nil {
		t.Fatal("Send context survived shutdown")
	}
	if other, err := blobstore.Open(c.Root, c.Files); other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("root unlocked before Send joined", err)
	}
	if _, _, err = r.BeginAttachmentSend(t.Context()); !errors.Is(err, blobstore.ErrClosed) {
		t.Fatal("admitted during close", err)
	}
	joined.Do(finish)
	if err = r.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	c, web, b = blobRuntimeDirectories(t)
	c.Expose = false
	r = startBlobModel(t, c, web, b.Budget(), &blobRuntimeModel{})
	if _, _, err = r.BeginAttachmentSend(t.Context()); !errors.Is(err, domain.ErrE2EEBlobNotReady) {
		t.Fatal("maintenance exposed Send", err)
	}
	var absent *blobStorageRuntime
	if _, _, err = absent.BeginAttachmentSend(t.Context()); !errors.Is(err, domain.ErrE2EEBlobNotReady) {
		t.Fatal("nil runtime admitted Send", err)
	}
}
func TestBlobRuntimeMaintenanceOnlyDeniesDirectControlsAndBody(t *testing.T) {
	c, web, b := blobRuntimeDirectories(t)
	c.Expose = false
	m := &blobRuntimeModel{control: func(context.Context, bool) (*domain.E2EEBlobReservation, error) {
		t.Error("maintenance reached user control")
		return nil, nil
	}}
	r := startBlobModel(t, c, web, b.Budget(), m)
	for _, method := range []func(context.Context, int64, *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error){r.Reserve, r.Observe} {
		if _, err := method(t.Context(), 1, nil); !errors.Is(err, domain.ErrE2EEBlobNotReady) {
			t.Fatal(err)
		}
	}
	if _, err := r.AcknowledgeFiles(t.Context(), 1, nil); !errors.Is(err, domain.ErrE2EEBlobNotReady) {
		t.Fatal("maintenance admitted FILEACK", err)
	}
	if _, err := r.FileStatuses(t.Context(), 1, nil); !errors.Is(err, domain.ErrE2EEBlobNotReady) {
		t.Fatal("maintenance admitted file status", err)
	}
	var absent *blobStorageRuntime
	if _, err := absent.FileStatuses(t.Context(), 1, nil); !errors.Is(err, domain.ErrE2EEBlobNotReady) {
		t.Fatal("nil runtime admitted file status", err)
	}
	src := &closedRuntimeSource{}
	if _, err := r.Upload(t.Context(), "", nil, nil, src); !errors.Is(err, domain.ErrE2EEBlobNotReady) || !src.closed || src.reads != 0 {
		t.Fatal("maintenance admitted body", err)
	}
}
