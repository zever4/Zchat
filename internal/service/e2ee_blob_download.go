package service

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

type E2EEBlobReadRepository interface {
	ResolveRead(context.Context, int64, string, *domain.E2EEBlobReadRequest) (*domain.E2EEBlobReadTarget, error)
}

type blobDownloadOperation struct {
	*blobUploadOperation
	messageID string
}

// Download owns dst immediately. Its Close must interrupt a concurrent Write
// and reject subsequent writes. Write must not retain p or launch unjoined
// background writers. No auth/SQL
// guard survives into file/sink I/O or callback joins. This internal coordinator
// does not install an HTTP route, acknowledge a file or change accepted metadata.
// Transfer timeout/recheck use the existing bounded blob-transfer configuration.
func (s *E2EEBlobUploadService) Download(parent context.Context, token string, credential []byte, input *domain.E2EEBlobReadRequest, dst io.WriteCloser) error {
	return s.download(parent, token, credential, input, dst, false)
}

// Head checks current authorization and the pinned file's safe leaf/exact size.
// It owns dst through header finalization/flush, without reading/hashing the
// body or issuing a reusable grant. Sink cancellation follows Download rules.
func (s *E2EEBlobUploadService) Head(parent context.Context, token string, credential []byte, input *domain.E2EEBlobReadRequest, dst io.WriteCloser) error {
	return s.download(parent, token, credential, input, dst, true)
}

// Transport-specific completion, still owned/cancellable and file-pinned. HTTP
// withholds its last byte until full integrity and final authorization succeed.
type E2EEBlobDownloadFinalizer interface{ FinishDownload() error }

func (s *E2EEBlobUploadService) download(parent context.Context, token string, credential []byte, input *domain.E2EEBlobReadRequest, dst io.WriteCloser, head bool) (err error) {
	if dst == nil {
		return domain.ErrInvalidInput
	}
	closeSink := sync.OnceValue(dst.Close)
	defer func() { err = errors.Join(err, closeSink()) }()
	if err = boundary.ValidateBlobReadRequest(input); err != nil {
		return err
	}
	req := copyBlobRead(input)
	credential = append([]byte(nil), credential...)
	repo, ok := s.repo.(E2EEBlobReadRepository)
	if !ok {
		return domain.ErrE2EEBlobNotReady
	}
	base, timeoutCancel := context.WithTimeout(parent, s.config.Timeout)
	defer timeoutCancel()
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	op := &blobDownloadOperation{&blobUploadOperation{cancel: cancel, done: make(chan struct{})}, req.Binding.MessageId}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return blobstore.ErrClosed
	}
	if !s.ready || s.physical == nil || s.initializing != nil || s.downloadLimit < 1 {
		s.mu.Unlock()
		return domain.ErrE2EEBlobNotReady
	}
	if len(s.downloads) >= s.downloadLimit {
		s.mu.Unlock()
		return blobstore.ErrBusy
	}
	// Register by immutable message BEFORE the first authorization/ResolveRead.
	// Future GC commits Deleting first, then cancels/joins this message. A reader
	// registered after that snapshot cannot resolve the now-unavailable record.
	s.downloads[op] = struct{}{}
	physical := s.physical
	s.mu.Unlock()
	var file *blobstore.ReadLease
	var stopWatch, stopSink func()
	defer func() {
		cancel(nil)
		err = errors.Join(err, closeSink())
		if stopSink != nil {
			stopSink()
		}
		if stopWatch != nil {
			stopWatch()
		}
		// The synchronous sink Write and its cancellation callback have joined
		// BEFORE Close releases the file/root/shared permit. Close may retain
		// ownership on uncertainty; no force-unpin or hidden unlink follows.
		if file != nil {
			err = errors.Join(err, file.Close())
		}
		s.mu.Lock()
		delete(s.downloads, op)
		close(op.done)
		s.mu.Unlock()
	}()
	// Register cancellation before authorization too: a pending metadata call
	// still belongs to the operation, and Close must join this callback.
	sinkDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(sinkDone); _ = closeSink() })
	stopSink = sync.OnceFunc(func() {
		if !stop() {
			<-sinkDone
		}
	})
	started := time.Now()
	uid, target, err := s.resolveBlobDownload(ctx, repo, token, credential, req, 0, nil)
	if err != nil {
		return err
	}
	// Anchor BEFORE the DB call: SQL/network delays cannot extend DB retention.
	deadline := started.Add(target.ExpiresAt.Sub(target.ObservedAt))
	stream, stopLease := context.WithDeadlineCause(ctx, deadline, domain.ErrE2EEBlobReadUnavailable)
	defer stopLease()
	// The sink must also be interrupted on the accepted-file deadline, not just
	// on the parent/operation timeout. Propagate it to the registered operation.
	leaseDone := make(chan struct{})
	stopExpiry := context.AfterFunc(stream, func() { defer close(leaseDone); cancel(context.Cause(stream)) })
	defer func() {
		if !stopExpiry() {
			<-leaseDone
		}
	}()
	ref := req.Blob
	file, err = physical.OpenRead(stream, uid, blobstore.Attempt{ID: target.AttemptID, Bytes: int64(ref.CiphertextBytes), SHA256: [32]byte(ref.Sha256)})
	if err != nil {
		if cause := context.Cause(stream); cause != nil {
			return cause
		}
		return err
	}
	// Pin/open happens outside auth/SQL guards. Recheck AFTER it, before bytes;
	// the earlier snapshot alone is never stream authority.
	if _, _, err = s.resolveBlobDownload(stream, repo, token, credential, req, uid, target); err != nil {
		return err
	}
	if !head {
		stopWatch = s.watchBlobDownload(stream, cancel, repo, uid, token, credential, req, target)
		buffer := make([]byte, 32<<10)
		for {
			if cause := context.Cause(stream); cause != nil {
				return cause
			}
			n, readErr := file.Read(buffer)
			if cause := context.Cause(stream); cause != nil {
				return cause
			}
			if n > 0 {
				written, writeErr := dst.Write(buffer[:n])
				if cause := context.Cause(stream); cause != nil {
					return cause
				}
				if writeErr != nil {
					return writeErr
				}
				if written != n {
					return io.ErrShortWrite
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
		stopWatch() // Join BEFORE reacquiring short authorization guards.
	}
	if cause := context.Cause(stream); cause != nil {
		return cause
	}
	_, _, err = s.resolveBlobDownload(stream, repo, token, credential, req, uid, target)
	if cause := context.Cause(stream); cause != nil {
		return cause
	}
	if err != nil {
		return err
	}
	if finish, ok := dst.(E2EEBlobDownloadFinalizer); ok {
		// Completion may block in a real socket; keep periodic checks during it too.
		stopWatch = s.watchBlobDownload(stream, cancel, repo, uid, token, credential, req, target)
		err = finish.FinishDownload()
		if cause := context.Cause(stream); cause != nil {
			return cause
		}
	}
	return err
}

func (s *E2EEBlobUploadService) resolveBlobDownload(ctx context.Context, repo E2EEBlobReadRepository, token string, credential []byte, req *domain.E2EEBlobReadRequest, expectedUID int64, expected *domain.E2EEBlobReadTarget) (int64, *domain.E2EEBlobReadTarget, error) {
	uid, release, err := s.authorize(ctx, token, req.Context.DeviceGenerationId, credential)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return 0, nil, cause
		}
		return 0, nil, err
	}
	defer release()
	if expectedUID != 0 && uid != expectedUID {
		return 0, nil, domain.ErrInvalidSession
	}
	started := time.Now()
	target, err := repo.ResolveRead(ctx, uid, s.files.StorageID(), req)
	if err != nil {
		if cause := context.Cause(ctx); cause != nil {
			return 0, nil, cause
		}
		return 0, nil, err
	}
	if !validBlobReadTarget(target, req, s.files.StorageID()) || expected != nil && !sameBlobReadTarget(target, expected) {
		return 0, nil, domain.ErrE2EEBlobMismatch
	}
	if cause := context.Cause(ctx); cause != nil {
		return 0, nil, cause
	}
	if !time.Now().Before(started.Add(target.ExpiresAt.Sub(target.ObservedAt))) {
		return 0, nil, domain.ErrE2EEBlobReadUnavailable
	}
	return uid, target, nil
}

func validBlobReadTarget(t *domain.E2EEBlobReadTarget, req *domain.E2EEBlobReadRequest, storage string) bool {
	return t != nil && t.Request != nil && t.StorageID == storage && boundary.ValidateBlobStorage(t.AttemptID) == nil &&
		t.Revision > 0 && t.StoredAt.UnixMilli() > 0 && !t.ObservedAt.Before(t.StoredAt) && t.ObservedAt.Before(t.ExpiresAt) &&
		t.ExpiresAt.Equal(t.StoredAt.Add(domain.E2EEBlobAcceptedRetention)) &&
		proto.Equal(t.Request.Context, req.Context) && proto.Equal(t.Request.Binding, req.Binding) && proto.Equal(t.Request.Blob, req.Blob)
}
func sameBlobReadTarget(a, b *domain.E2EEBlobReadTarget) bool {
	return a.StorageID == b.StorageID && a.AttemptID == b.AttemptID && a.Revision == b.Revision && a.StoredAt.Equal(b.StoredAt) && a.ExpiresAt.Equal(b.ExpiresAt)
}
func copyBlobRead(r *domain.E2EEBlobReadRequest) *domain.E2EEBlobReadRequest {
	return &domain.E2EEBlobReadRequest{Context: proto.Clone(r.Context).(*pb.ClientContext), Binding: proto.Clone(r.Binding).(*pb.EpochSendBinding), Blob: proto.Clone(r.Blob).(*v2.BlobRef)}
}

func (s *E2EEBlobUploadService) watchBlobDownload(parent context.Context, cancel context.CancelCauseFunc, repo E2EEBlobReadRepository, uid int64, token string, credential []byte, req *domain.E2EEBlobReadRequest, target *domain.E2EEBlobReadTarget) func() {
	ctx, stop := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(s.config.RecheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, _, err := s.resolveBlobDownload(ctx, repo, token, credential, req, uid, target); err != nil {
					if ctx.Err() == nil {
						cancel(err)
					}
					return
				}
			}
		}
	}()
	return sync.OnceFunc(func() { stop(); <-done })
}

// CancelDownloads is INTERNAL accepted-GC coordination, not deletion
// authority. Call only AFTER committing Deleting for this accepted message.
// Message registration precedes resolving/pinning, closing the GC/open gap.
// No unlink, quota release, file ACK or durable state transition happens here.
func (s *E2EEBlobUploadService) CancelDownloads(ctx context.Context, messageID string) error {
	s.mu.Lock()
	var ops []*blobDownloadOperation
	for op := range s.downloads {
		if op.messageID == messageID {
			op.cancel(domain.ErrE2EEBlobReadUnavailable)
			ops = append(ops, op)
		}
	}
	s.mu.Unlock()
	for _, op := range ops {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-op.done:
		}
	}
	return nil
}
