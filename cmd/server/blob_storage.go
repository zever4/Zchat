package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/handlers"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type blobRuntimeRepository interface {
	service.E2EEBlobControls
	service.E2EEBlobWriterRepository
	service.E2EEBlobReadRepository
	service.E2EEBlobInventory
	service.E2EEBlobReclaimer
	service.E2EEAcceptedBlobReclaimer
	service.E2EEBlobCleanupScanner
	Configure(context.Context, domain.E2EEBlobLimits) error
}

type blobStorageRuntime struct {
	upload        *service.E2EEBlobUploadService
	controls      service.E2EEBlobControls
	life          context.Context
	cancel        context.CancelCauseFunc
	collectorDone chan struct{}
	errors        chan error
	mu            sync.Mutex
	closing       bool
	expose        bool
	calls         sync.WaitGroup
	joined        chan struct{}
	closeMu       sync.Mutex
}

// The caller owns one lifetime: startup, upload admission, collector, shutdown.
// A failed startup is fatal to this Budget lifetime; never retry/reseed sources.
func startBlobStorage(parent context.Context, c blobStorageConfig, web *os.Root, budget *storagebudget.Budget, repo blobRuntimeRepository, auth *service.AuthService, devices *service.E2EEDeviceService, observe func(service.E2EEBlobCleanupSweep, error)) (result *blobStorageRuntime, err error) {
	if !c.Enabled {
		// An initialized root must stay in accounting even when its API is off.
		// Checking a known path is read-only; never create/adopt a disabled root.
		if filepath.IsAbs(c.Root) {
			if _, e := os.Lstat(filepath.Join(c.Root, ".store-id")); e == nil {
				return nil, errors.New("initialized private blob storage requires maintenance; disable only ENABLE_EXPERIMENTAL_E2EE")
			} else if !errors.Is(e, os.ErrNotExist) {
				return nil, e
			}
		}
		return nil, nil
	}
	if web == nil || budget == nil || repo == nil || auth == nil || devices == nil {
		return nil, domain.ErrInvalidInput
	}
	if !c.Limits.Valid() || c.Startup.MaxAttempts < int(c.Limits.GlobalAttempts) {
		return nil, domain.ErrInvalidInput
	}
	if err = c.Collector.Validate(); err != nil {
		return nil, err
	}
	if err = parent.Err(); err != nil {
		return nil, err
	}
	root, err := privateBlobRoot(c.Root, web)
	if err != nil {
		return nil, err
	}
	files, err := blobstore.Open(root, c.Files)
	if err != nil {
		return nil, err
	}
	upload, err := service.NewE2EEBlobUploadService(repo, files, auth, devices, c.Upload)
	if err != nil {
		return nil, errors.Join(err, files.Close())
	}
	life, cancel := context.WithCancelCause(parent)
	defer func() {
		if err != nil {
			cancel(err)
			err = errors.Join(err, upload.Close(context.Background()))
		}
	}()
	startup, stop := context.WithTimeout(life, c.Startup.Timeout)
	defer stop()
	if err = repo.Configure(startup, c.Limits); err != nil {
		return nil, fmt.Errorf("configure private blob quotas: %w", err)
	}
	if _, err = upload.Reconcile(startup, c.Startup); err != nil {
		return nil, fmt.Errorf("reconcile private blobs: %w", err)
	}
	if err = upload.AttachBudget(startup, budget, c.Startup); err != nil {
		return nil, fmt.Errorf("attach private blob budget: %w", err)
	}
	if err = startup.Err(); err != nil {
		return nil, err
	}
	r := &blobStorageRuntime{upload: upload, controls: repo, life: life, cancel: cancel, collectorDone: make(chan struct{}), errors: make(chan error, 1), expose: c.Expose}
	go func() {
		defer close(r.collectorDone)
		err := upload.RunCleanup(life, c.Collector, observe)
		if life.Err() == nil {
			if err == nil {
				err = errors.New("private blob collector stopped unexpectedly")
			}
			cancel(err)
			r.errors <- err
		}
	}()
	return r, nil
}

// Rooted public serving prevents a symlink in web from exposing an outside
// private directory. Validate real paths too, including symlinked ancestors.
func privateBlobRoot(name string, web *os.Root) (string, error) {
	if !filepath.IsAbs(name) {
		return "", blobstore.ErrRoot
	}
	name = filepath.Clean(name)
	info, err := os.Lstat(name)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", blobstore.ErrRoot
	}
	root, err := filepath.EvalSymlinks(name)
	if err != nil {
		return "", err
	}
	public, err := filepath.EvalSymlinks(web.Name())
	if err != nil {
		return "", err
	}
	public, err = filepath.Abs(public)
	if err != nil {
		return "", err
	}
	publicInfo, err := web.Stat(".")
	if err != nil {
		return "", err
	}
	if pathContains(public, root) || pathContains(root, public) || os.SameFile(info, publicInfo) {
		return "", blobstore.ErrRoot
	}
	return root, nil
}
func pathContains(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && (rel == "." || rel != ".." && !filepath.IsAbs(rel) && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func (r *blobStorageRuntime) Register(mux *http.ServeMux) {
	if r != nil && r.expose {
		mux.HandleFunc(boundary.BlobUploadPattern, handlers.HandleE2EEBlobUpload(r))
		mux.HandleFunc(boundary.BlobDownloadPattern, handlers.HandleE2EEBlobDownload(r))
	}
}

func (r *blobStorageRuntime) Upload(parent context.Context, token string, credential []byte, attempt *domain.E2EEBlobAttemptRequest, src io.ReadCloser) (*domain.E2EEBlobWriteLease, error) {
	ctx, finish, err := r.beginCall(parent)
	if err != nil {
		if src != nil {
			_ = src.Close()
		}
		return nil, err
	}
	defer finish()
	return r.upload.Upload(ctx, token, credential, attempt, src)
}

// Download shares exposure, cancellation and joins with uploads/control/Send.
func (r *blobStorageRuntime) Download(parent context.Context, token string, credential []byte, req *domain.E2EEBlobReadRequest, dst io.WriteCloser) error {
	ctx, finish, err := r.beginCall(parent)
	if err != nil {
		if dst != nil {
			err = errors.Join(err, dst.Close())
		}
		return err
	}
	defer finish()
	return r.upload.Download(ctx, token, credential, req, dst)
}

func (r *blobStorageRuntime) Head(parent context.Context, token string, credential []byte, req *domain.E2EEBlobReadRequest, dst io.WriteCloser) error {
	ctx, finish, err := r.beginCall(parent)
	if err != nil {
		if dst != nil {
			err = errors.Join(err, dst.Close())
		}
		return err
	}
	defer finish()
	return r.upload.Head(ctx, token, credential, req, dst)
}

// Native RPC authentication already holds short account/device guards. Do not
// nest them here; only metadata transactions run before the RPC releases them.
func (r *blobStorageRuntime) Reserve(parent context.Context, uid int64, req *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	ctx, finish, err := r.beginCall(parent)
	if err != nil {
		return nil, err
	}
	defer finish()
	return r.controls.Reserve(ctx, uid, req)
}
func (r *blobStorageRuntime) Observe(parent context.Context, uid int64, req *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	ctx, finish, err := r.beginCall(parent)
	if err != nil {
		return nil, err
	}
	defer finish()
	return r.controls.Observe(ctx, uid, req)
}

func (r *blobStorageRuntime) AcknowledgeFiles(parent context.Context, uid int64, req *v2.AcknowledgeFilesRequest) (*v2.FileAcknowledgements, error) {
	ctx, finish, err := r.beginCall(parent)
	if err != nil {
		return nil, err
	}
	defer finish()
	return r.controls.AcknowledgeFiles(ctx, uid, req)
}

func (r *blobStorageRuntime) FileStatuses(parent context.Context, uid int64, req *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
	ctx, finish, err := r.beginCall(parent)
	if err != nil {
		return nil, err
	}
	defer finish()
	return r.controls.FileStatuses(ctx, uid, req)
}

// A file Send shares exposure, cancellation and shutdown joins with its upload
// and metadata. The repository still resolves receipt/fence before live staging.
func (r *blobStorageRuntime) BeginAttachmentSend(parent context.Context) (context.Context, func(), error) {
	return r.beginCall(parent)
}

func (r *blobStorageRuntime) beginCall(parent context.Context) (context.Context, func(), error) {
	if r == nil {
		return nil, nil, domain.ErrE2EEBlobNotReady
	}
	r.mu.Lock()
	if r.closing || r.life.Err() != nil {
		r.mu.Unlock()
		return nil, nil, blobstore.ErrClosed
	}
	if !r.expose {
		r.mu.Unlock()
		return nil, nil, domain.ErrE2EEBlobNotReady
	}
	r.calls.Add(1)
	r.mu.Unlock()
	ctx, cancel := context.WithCancelCause(parent)
	finished := make(chan struct{})
	stop := context.AfterFunc(r.life, func() { defer close(finished); cancel(context.Cause(r.life)) })
	return ctx, func() {
		if !stop() {
			<-finished
		}
		cancel(nil)
		r.calls.Done()
	}, nil
}

// A timeout retains root/process ownership and the caller's shared resources.
// Repeated Close can join later. Exactly one join goroutine exists per runtime.
func (r *blobStorageRuntime) Close(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if !r.closing {
		r.closing = true
		r.cancel(blobstore.ErrClosed)
		r.joined = make(chan struct{})
		go func() { r.calls.Wait(); <-r.collectorDone; close(r.joined) }()
	}
	joined := r.joined
	r.mu.Unlock()
	select {
	case <-joined:
	default:
		select {
		case <-joined:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	return r.upload.Close(ctx)
}
