package service

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

type E2EEBlobWriterRepository interface {
	BeginWrite(context.Context, int64, *domain.E2EEBlobWriteRequest) (*domain.E2EEBlobWriteLease, error)
	CheckWrite(context.Context, int64, *domain.E2EEBlobWriteRequest) (*domain.E2EEBlobWriteLease, error)
	CompleteWrite(context.Context, int64, *domain.E2EEBlobWriteRequest, *blobstore.Written) (*domain.E2EEBlobWriteLease, error)
}
type E2EEBlobUploadConfig struct {
	MaxUploads               int
	Timeout, RecheckInterval time.Duration
}
type blobUploadOperation struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

// Authenticated upload/download coordinator. This object owns files until Close.
// Reconcile and AttachBudget must finish before uploads are admitted. Cleanup
// scheduling is explicitly started; construction is NOT storage readiness.
// HTTP is explicitly opt-in; attachment Send shares the owning runtime's gate.
type E2EEBlobUploadService struct {
	repo          E2EEBlobWriterRepository
	files         *blobstore.Store
	config        E2EEBlobUploadConfig
	authorize     func(context.Context, string, string, []byte) (int64, func(), error)
	mu            sync.Mutex
	active        map[string]*blobUploadOperation
	cleanups      map[string]*blobUploadOperation
	downloads     map[*blobDownloadOperation]struct{}
	downloadLimit int
	closed        bool
	ready         bool
	initializing  *blobUploadOperation
	collector     *blobUploadOperation
	physical      *blobstore.Accounting
}

func NewE2EEBlobUploadService(repo E2EEBlobWriterRepository, files *blobstore.Store, auth *AuthService, devices *E2EEDeviceService, c E2EEBlobUploadConfig) (*E2EEBlobUploadService, error) {
	if repo == nil || files == nil || auth == nil || devices == nil || c.MaxUploads < 1 || c.MaxUploads >= files.Capacity() ||
		c.Timeout <= 0 || c.Timeout > 5*time.Minute || c.RecheckInterval <= 0 || c.RecheckInterval > c.Timeout || c.RecheckInterval > 30*time.Second {
		return nil, domain.ErrInvalidInput
	}
	s := &E2EEBlobUploadService{repo: repo, files: files, config: c, active: make(map[string]*blobUploadOperation), cleanups: make(map[string]*blobUploadOperation), downloads: make(map[*blobDownloadOperation]struct{})}
	s.authorize = func(parent context.Context, token, generation string, credential []byte) (int64, func(), error) {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		claims, sessionRelease, err := auth.AuthorizeContext(ctx, token)
		if err != nil {
			return 0, nil, err
		}
		_, deviceRelease, err := devices.Authorize(ctx, claims.UserID, generation, credential)
		if err != nil {
			sessionRelease()
			return 0, nil, err
		}
		return claims.UserID, func() { deviceRelease(); sessionRelease() }, nil
	}
	return s, nil
}

// Upload owns src immediately; Close must unblock concurrent Read. Login/device
// barriers surround only short DB checks, never body/FS I/O or watcher joins.
// Any failure leaves metadata/bytes charged. In particular a lost Ready COMMIT
// response MUST NOT trigger file deletion: it may already be committed Ready.
func (s *E2EEBlobUploadService) Upload(parent context.Context, token string, credential []byte, input *domain.E2EEBlobAttemptRequest, src io.ReadCloser) (result *domain.E2EEBlobWriteLease, err error) {
	if src == nil {
		return nil, domain.ErrInvalidInput
	}
	closeSource := sync.OnceValue(src.Close)
	defer func() {
		err = errors.Join(err, closeSource())
		if err != nil {
			result = nil
		}
	}()
	if err = boundary.ValidateBlobAttempt(input); err != nil {
		return nil, err
	}
	req := &domain.E2EEBlobWriteRequest{Attempt: copyBlobAttempt(input), WriterID: uuid.NewString(), StorageID: s.files.StorageID()}
	credential = append([]byte(nil), credential...)
	base, timeoutCancel := context.WithTimeout(parent, s.config.Timeout)
	defer timeoutCancel()
	ctx, cancel := context.WithCancelCause(base)
	defer cancel(nil)
	op := &blobUploadOperation{cancel: cancel, done: make(chan struct{})}
	id := req.Attempt.AttemptID
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, blobstore.ErrClosed
	}
	if !s.ready || s.physical == nil || s.initializing != nil {
		s.mu.Unlock()
		return nil, domain.ErrE2EEBlobNotReady
	}
	if _, busy := s.active[id]; busy || len(s.active) >= s.config.MaxUploads {
		s.mu.Unlock()
		return nil, blobstore.ErrBusy
	}
	if _, busy := s.cleanups[id]; busy {
		s.mu.Unlock()
		return nil, blobstore.ErrBusy
	}
	s.active[id] = op
	s.mu.Unlock()
	defer func() { closeSource(); s.mu.Lock(); delete(s.active, id); close(op.done); s.mu.Unlock() }()
	rsv := req.Attempt.Reservation
	uid, release, err := s.authorize(ctx, token, rsv.Context.DeviceGenerationId, credential)
	if err != nil {
		return nil, err
	}
	started := time.Now()
	lease, err := s.repo.BeginWrite(ctx, uid, req)
	release()
	if err != nil {
		return nil, err
	}
	if !sameBlobWriter(lease, req, "writing") {
		return nil, domain.ErrE2EEBlobWriteState
	}
	// Use a duration from the DB clock, anchored BEFORE the DB call. This is
	// conservative across clock skew/lock/network delays; final SQL checks again.
	deadline := started.Add(lease.Grant.ExpiresAt.Sub(lease.Grant.ObservedAt))
	stream, cancelLease := context.WithDeadlineCause(ctx, deadline, domain.ErrE2EEBlobLeaseExpired)
	defer cancelLease()
	stopWatch := s.watchBlobWriter(stream, cancel, uid, token, credential, req)
	defer stopWatch()
	ref := rsv.Blob
	attempt := blobstore.Attempt{ID: id, Bytes: int64(ref.CiphertextBytes), SHA256: [32]byte(ref.Sha256)}
	physical, err := s.physical.Reserve(stream, uid, attempt)
	if err != nil {
		return nil, err
	}
	var written *blobstore.Written
	defer func() { err = errors.Join(err, physical.Finish(written)) }()
	written, writeErr := s.files.Write(stream, attempt, &blobUploadSource{src, closeSource})
	stopWatch() // Join outside auth/session/device barriers.
	if cause := context.Cause(stream); cause != nil {
		return nil, cause
	}
	if writeErr != nil {
		return nil, writeErr
	}
	owner, release, err := s.authorize(stream, token, rsv.Context.DeviceGenerationId, credential)
	if err != nil {
		return nil, err
	}
	defer release()
	if owner != uid {
		return nil, domain.ErrInvalidSession
	}
	result, err = s.repo.CompleteWrite(stream, uid, req, written)
	if err == nil && !sameBlobWriter(result, req, "ready") {
		return nil, domain.ErrE2EEBlobWriteState
	}
	return result, err
}

type blobUploadSource struct {
	io.ReadCloser
	close func() error
}

func (s *blobUploadSource) Close() error { return s.close() }

func (s *E2EEBlobUploadService) watchBlobWriter(parent context.Context, cancel context.CancelCauseFunc, uid int64, token string, credential []byte, req *domain.E2EEBlobWriteRequest) func() {
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
				owner, release, err := s.authorize(ctx, token, req.Attempt.Reservation.Context.DeviceGenerationId, credential)
				if err == nil {
					if owner != uid {
						err = domain.ErrInvalidSession
					} else {
						var state *domain.E2EEBlobWriteLease
						state, err = s.repo.CheckWrite(ctx, uid, req)
						if err == nil && !sameBlobWriter(state, req, "writing") {
							err = domain.ErrE2EEBlobWriteState
						}
					}
					release()
				}
				if err != nil {
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

func sameBlobWriter(l *domain.E2EEBlobWriteLease, r *domain.E2EEBlobWriteRequest, state string) bool {
	return l != nil && l.StorageID == r.StorageID && l.Grant.State == state && !l.Grant.Expired && l.WriterID == r.WriterID &&
		l.Grant.MessageID == r.Attempt.Reservation.MessageID && l.Grant.AttemptID == r.Attempt.AttemptID &&
		l.Grant.Revision == r.Attempt.Revision && l.Grant.OperationID == r.Attempt.Reservation.OperationID
}
func copyBlobAttempt(r *domain.E2EEBlobAttemptRequest) *domain.E2EEBlobAttemptRequest {
	c := *r
	reservation := *r.Reservation
	c.Reservation = &reservation
	epoch := *reservation.CryptoEpoch
	reservation.CryptoEpoch = &epoch
	reservation.Context = proto.Clone(reservation.Context).(*pb.ClientContext)
	reservation.Sender = proto.Clone(reservation.Sender).(*pb.DeviceRef)
	reservation.Recipient = proto.Clone(reservation.Recipient).(*pb.DeviceRef)
	reservation.Blob = proto.Clone(reservation.Blob).(*v2.BlobRef)
	return &c
}

// A future cleanup caller must first commit PG Deleting, then cancel/join this
// exact attempt, then Remove and finally release its quota. This does no unlink.
func (s *E2EEBlobUploadService) CancelUpload(ctx context.Context, id string) error {
	s.mu.Lock()
	op := s.active[id]
	if op != nil {
		op.cancel(domain.ErrE2EEBlobWriteState)
	}
	s.mu.Unlock()
	if op == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-op.done:
		return nil
	}
}

// Close retains the root process lock if an operation fails to join in time.
func (s *E2EEBlobUploadService) Close(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	s.ready = false
	ops := make([]*blobUploadOperation, 0, len(s.active))
	for _, op := range s.active {
		op.cancel(blobstore.ErrClosed)
		ops = append(ops, op)
	}
	for _, op := range s.cleanups {
		op.cancel(blobstore.ErrClosed)
		ops = append(ops, op)
	}
	for op := range s.downloads {
		op.cancel(blobstore.ErrClosed)
		ops = append(ops, op.blobUploadOperation)
	}
	if s.initializing != nil {
		s.initializing.cancel(blobstore.ErrClosed)
		ops = append(ops, s.initializing)
	}
	if s.collector != nil {
		s.collector.cancel(blobstore.ErrClosed)
		ops = append(ops, s.collector)
	}
	s.mu.Unlock()
	for _, op := range ops {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-op.done:
		}
	}
	return s.files.Close()
}
