//go:build linux

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/storagebudget"
)

var errBlobSynthetic = errors.New("synthetic upload outcome unavailable")

type uploadRepositoryModel struct {
	mu                                            sync.Mutex
	state                                         string
	writer                                        string
	checks, completes                             int
	checkErr, errorBeforeCommit, errorAfterCommit error
	completeHook                                  func()
	checkHook                                     func(context.Context)
}

func (m *uploadRepositoryModel) lease(r *domain.E2EEBlobWriteRequest) *domain.E2EEBlobWriteLease {
	now := time.Now()
	return &domain.E2EEBlobWriteLease{WriterID: r.WriterID, StorageID: r.StorageID, Grant: domain.E2EEBlobReservation{
		MessageID: r.Attempt.Reservation.MessageID, AttemptID: r.Attempt.AttemptID, Revision: r.Attempt.Revision, OperationID: r.Attempt.Reservation.OperationID,
		State: m.state, ReservedAt: now.Add(-time.Second), ExpiresAt: now.Add(4 * time.Second), ObservedAt: now}}
}
func (m *uploadRepositoryModel) BeginWrite(_ context.Context, _ int64, r *domain.E2EEBlobWriteRequest) (*domain.E2EEBlobWriteLease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.writer != "" && m.writer != r.WriterID {
		return nil, domain.ErrE2EEBlobCAS
	}
	m.writer = r.WriterID
	m.state = "writing"
	return m.lease(r), nil
}
func (m *uploadRepositoryModel) CheckWrite(ctx context.Context, _ int64, r *domain.E2EEBlobWriteRequest) (*domain.E2EEBlobWriteLease, error) {
	if m.checkHook != nil {
		m.checkHook(ctx)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checks++
	if m.checkErr != nil {
		return nil, m.checkErr
	}
	return m.lease(r), nil
}
func (m *uploadRepositoryModel) CompleteWrite(ctx context.Context, _ int64, r *domain.E2EEBlobWriteRequest, w *blobstore.Written) (*domain.E2EEBlobWriteLease, error) {
	if m.completeHook != nil {
		m.completeHook()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.completes++
	if w.Attempt().ID != r.Attempt.AttemptID {
		return nil, domain.ErrInvalidInput
	}
	if m.errorBeforeCommit != nil {
		return nil, m.errorBeforeCommit
	}
	m.state = "ready"
	if m.errorAfterCommit != nil {
		return nil, m.errorAfterCommit
	}
	return m.lease(r), nil
}

type uploadCoreFixture struct {
	s                 *E2EEBlobUploadService
	m                 *uploadRepositoryModel
	r                 *domain.E2EEBlobAttemptRequest
	data              []byte
	path              string
	guards, authCalls atomic.Int32
}

func uploadCore(t *testing.T) *uploadCoreFixture {
	t.Helper()
	f := uploadCoreUnattached(t)
	physical, err := f.s.files.AttachAccounting(t.Context(), testPhysicalBudget(t), nil, 128)
	if err != nil {
		t.Fatal(err)
	}
	f.s.physical = physical
	return f
}

func testPhysicalBudget(t *testing.T) *storagebudget.Budget {
	t.Helper()
	c, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	c.FreeBytes, c.FreeEntries = 1, 1
	b, err := storagebudget.New(c, nil, func() (storagebudget.Disk, error) {
		return storagebudget.Disk{FreeBytes: 1 << 40, FreeEntries: 1 << 30}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func uploadCoreUnattached(t *testing.T) *uploadCoreFixture {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	files, err := blobstore.Open(path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	m := &uploadRepositoryModel{state: "reserved"}
	s, err := NewE2EEBlobUploadService(m, files, &AuthService{}, &E2EEDeviceService{}, E2EEBlobUploadConfig{MaxUploads: 1, Timeout: 2 * time.Second, RecheckInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	// These model tests exercise only uploads; startup/PG reconciliation has
	// separate real-FS/PG tests. The production constructor starts not ready.
	s.ready = true
	data := bytes.Repeat([]byte{0x42}, 64)
	r := blobUploadIntent(data)
	f := &uploadCoreFixture{s: s, m: m, r: r, path: path, data: data}
	s.authorize = func(context.Context, string, string, []byte) (int64, func(), error) {
		f.authCalls.Add(1)
		f.guards.Add(1)
		return 1, func() { f.guards.Add(-1) }, nil
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return f
}

func blobUploadIntent(data []byte) *domain.E2EEBlobAttemptRequest {
	hash := sha256.Sum256(data)
	gen := uuid.NewString()
	epoch := uint64(0)
	return &domain.E2EEBlobAttemptRequest{AttemptID: uuid.NewString(), Revision: 1, Reservation: &domain.E2EEBlobReservationRequest{
		Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: gen}, MessageID: uuid.NewString(), ConversationID: uuid.NewString(), CryptoEpoch: &epoch,
		Sender: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: gen, DeviceId: 1}, Recipient: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: uuid.NewString(), DeviceId: 1},
		OperationID: uuid.NewString(), Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: uint64(len(data)), Sha256: hash[:]}}}
}

type uploadBlockedSource struct {
	entered, closed chan struct{}
	once            sync.Once
}

func blockedUploadSource() *uploadBlockedSource {
	return &uploadBlockedSource{entered: make(chan struct{}), closed: make(chan struct{})}
}
func (s *uploadBlockedSource) Read([]byte) (int, error) {
	close(s.entered)
	<-s.closed
	return 0, io.ErrClosedPipe
}
func (s *uploadBlockedSource) Close() error { s.once.Do(func() { close(s.closed) }); return nil }
func awaitUploadRead(t *testing.T, src *uploadBlockedSource) {
	t.Helper()
	select {
	case <-src.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("upload did not start reading")
	}
}

func TestBlobUploadUnknownReadyOutcomePreservesFile(t *testing.T) {
	for _, name := range []string{"success", "failed Ready", "lost Ready reply"} {
		t.Run(name, func(t *testing.T) {
			f := uploadCore(t)
			if name == "failed Ready" {
				f.m.errorBeforeCommit = errBlobSynthetic
			}
			if name == "lost Ready reply" {
				f.m.errorAfterCommit = errBlobSynthetic
			}
			got, err := f.s.Upload(t.Context(), "model-token", make([]byte, 32), f.r, io.NopCloser(bytes.NewReader(f.data)))
			if name == "success" {
				if err != nil || got == nil || got.Grant.State != "ready" {
					t.Fatal(got, err)
				}
			} else if got != nil || !errors.Is(err, errBlobSynthetic) {
				t.Fatal(got, err)
			}
			data, e := os.ReadFile(filepath.Join(f.path, f.r.AttemptID+".blob"))
			if e != nil || !bytes.Equal(data, f.data) {
				t.Fatal("deleted durable file after uncertain COMMIT", e)
			}
			if f.guards.Load() != 0 {
				t.Fatal("authorization guard leaked")
			}
		})
	}
}
func TestBlobUploadPeriodicFailureCancelsAndJoins(t *testing.T) {
	for _, reason := range []error{domain.ErrInvalidSession, domain.ErrE2EEDeviceCredential, domain.ErrE2EEEpochInactive, domain.ErrE2EEBlobCAS, ErrAuthUnavailable} {
		t.Run(reason.Error(), func(t *testing.T) {
			f := uploadCore(t)
			f.m.checkErr = reason
			src := blockedUploadSource()
			// Exercise cancellation of an ACTUALLY blocked Read. Under load the
			// first timer may otherwise revoke before the reader starts (valid
			// runtime behavior, but a different scenario from this assertion).
			f.m.checkHook = func(ctx context.Context) {
				select {
				case <-src.entered:
				case <-ctx.Done():
				}
			}
			done := make(chan error, 1)
			go func() { _, err := f.s.Upload(t.Context(), "model-token", make([]byte, 32), f.r, src); done <- err }()
			awaitUploadRead(t, src)
			select {
			case err := <-done:
				if !errors.Is(err, reason) {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("watcher/blocked source leaked")
			}
			if f.guards.Load() != 0 || f.m.completes != 0 {
				t.Fatal("leaked guard or invalid Ready", f.guards.Load(), f.m.completes)
			}
			if _, err := os.Stat(filepath.Join(f.path, f.r.AttemptID+".blob")); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("cancelled upload published file", err)
			}
		})
	}
}
func TestBlobUploadFinalAuthAndCapacityBeforeBody(t *testing.T) {
	f := uploadCore(t)
	f.s.config.RecheckInterval = time.Second
	var calls atomic.Int32
	f.s.authorize = func(context.Context, string, string, []byte) (int64, func(), error) {
		if calls.Add(1) > 1 {
			return 0, nil, domain.ErrInvalidSession
		}
		return 1, func() {}, nil
	}
	if got, err := f.s.Upload(t.Context(), "model-token", make([]byte, 32), f.r, io.NopCloser(bytes.NewReader(f.data))); got != nil || !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal(got, err)
	}
	if f.m.completes != 0 {
		t.Fatal("skipped final session recheck")
	}
	if _, err := os.Stat(filepath.Join(f.path, f.r.AttemptID+".blob")); err != nil {
		t.Fatal("lost unaccepted file after auth change", err)
	}
	// Different fixture: a blocked stream consumes the sole upload slot while
	// one filesystem slot remains available for a future cleanup coordinator.
	f2 := uploadCore(t)
	src := blockedUploadSource()
	done := make(chan error, 1)
	go func() { _, err := f2.s.Upload(t.Context(), "model-token", make([]byte, 32), f2.r, src); done <- err }()
	awaitUploadRead(t, src)
	if got, err := f2.s.Upload(t.Context(), "model-token", make([]byte, 32), f2.r, io.NopCloser(bytes.NewReader(f2.data))); got != nil || !errors.Is(err, blobstore.ErrBusy) {
		t.Fatal("duplicate upload admitted", got, err)
	}
	if err := f2.s.CancelUpload(t.Context(), f2.r.AttemptID); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, domain.ErrE2EEBlobWriteState) {
		t.Fatal(err)
	}
}
func TestBlobUploadCloseRetainsOwnerDuringReadyRPC(t *testing.T) {
	f := uploadCore(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	f.m.completeHook = func() { close(entered); <-release }
	done := make(chan error, 1)
	go func() {
		_, err := f.s.Upload(t.Context(), "model-token", make([]byte, 32), f.r, io.NopCloser(bytes.NewReader(f.data)))
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Ready RPC not reached")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
	defer cancel()
	if err := f.s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown did not wait for full coordinator operation", err)
	}
	other, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("owner released before RPC joined", err)
	}
	once.Do(func() { close(release) })
	if err = <-done; err == nil {
		t.Fatal("cancelled Ready returned success")
	}
	if err = f.s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
