package blobstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func privateTemp(t *testing.T) string {
	t.Helper()
	path := t.TempDir()
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	return path
}
func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	path := privateTemp(t)
	s, err := Open(path, Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, path
}
func attempt(data []byte) Attempt {
	return Attempt{ID: uuid.NewString(), Bytes: int64(len(data)), SHA256: sha256.Sum256(data)}
}
func source(data []byte) io.ReadCloser { return io.NopCloser(bytes.NewReader(data)) }
func onlyLock(t *testing.T, path string) {
	t.Helper()
	entries, err := os.ReadDir(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || entries[0].Name() != ".owner.lock" || entries[1].Name() != ".store-id" {
		t.Fatal("storage artifacts remain", entries)
	}
}

func TestCiphertextDurableWriteRemoveAndReopen(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{0x42}, bufferBytes*2+17)
	a := attempt(data)
	w, err := s.Write(t.Context(), a, source(data))
	if err != nil || w == nil || w.Attempt() != a {
		t.Fatal(w, err)
	}
	file, err := os.ReadFile(filepath.Join(path, a.ID+".blob"))
	if err != nil || !bytes.Equal(file, data) {
		t.Fatal("stored ciphertext changed", err)
	}
	info, err := os.Lstat(filepath.Join(path, a.ID+".blob"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	if _, err = os.Lstat(filepath.Join(path, a.ID+".part")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("temporary name published", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Open(path, Config{MaxConcurrent: 1, HeadroomBytes: 1 << 20, WriteTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if written, err := reopened.Write(t.Context(), a, source(data)); !errors.Is(err, ErrConflict) || written != nil {
		t.Fatal("adopted existing artifact as Ready", written, err)
	}
	if err = reopened.Remove(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	if err = reopened.Remove(t.Context(), a.ID); err != nil {
		t.Fatal("idempotent removal", err)
	}
	onlyLock(t, path)
}

func TestCiphertextSizeHashAndReaderFailures(t *testing.T) {
	for _, name := range []string{"short", "excess", "hash", "source error", "source close", "no progress"} {
		t.Run(name, func(t *testing.T) {
			s, path := testStore(t)
			data := bytes.Repeat([]byte{9}, 64)
			a := attempt(data)
			var src io.ReadCloser = source(data)
			want := ErrIntegrity
			switch name {
			case "short":
				src = source(data[:63])
			case "excess":
				src = source(append(data, 1))
			case "hash":
				a.SHA256[0]++
			case "source error":
				src = &errorSource{}
				want = errInjected
			case "source close":
				src = &closeErrorSource{Reader: bytes.NewReader(data)}
				want = errInjected
			case "no progress":
				src = noProgressSource{}
				want = io.ErrNoProgress
			}
			if w, err := s.Write(t.Context(), a, src); w != nil || !errors.Is(err, want) {
				t.Fatal(w, err)
			}
			onlyLock(t, path)
			if err := s.Remove(t.Context(), a.ID); err != nil {
				t.Fatal(err)
			}
		})
	}
}

var errInjected = errors.New("synthetic ciphertext storage failure")

type errorSource struct{}

func (*errorSource) Read([]byte) (int, error) { return 0, errInjected }
func (*errorSource) Close() error             { return nil }

type closeErrorSource struct{ *bytes.Reader }

func (*closeErrorSource) Close() error { return errInjected }

type noProgressSource struct{}

func (noProgressSource) Read([]byte) (int, error) { return 0, nil }
func (noProgressSource) Close() error             { return nil }

type spySource struct {
	data   []byte
	read   bool
	closed bool
}

func (r *spySource) Read(p []byte) (int, error) {
	r.read = true
	n := copy(p, r.data)
	r.data = r.data[n:]
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}
func (r *spySource) Close() error { r.closed = true; return nil }
func TestCiphertextRejectsInputAndHeadroomBeforeRead(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{1}, 64)
	a := attempt(data)
	for _, id := range []string{"", "../escape", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA", "cccccccc-cccc-1ccc-8ccc-cccccccccccc", "/tmp/escape", "x\\y"} {
		bad := a
		bad.ID = id
		src := &spySource{data: data}
		if w, err := s.Write(t.Context(), bad, src); w != nil || !errors.Is(err, ErrInvalid) || src.read || !src.closed {
			t.Fatal(id, w, err, src)
		}
		if err := s.Remove(t.Context(), id); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	for _, size := range []int64{0, 36, 52428837} {
		bad := a
		bad.Bytes = size
		if _, err := s.Write(t.Context(), bad, source(data)); !errors.Is(err, ErrInvalid) {
			t.Fatal(size, err)
		}
	}
	s.available = func() (uint64, error) { return s.config.HeadroomBytes + 63, nil }
	src := &spySource{data: data}
	if w, err := s.Write(t.Context(), a, src); w != nil || !errors.Is(err, ErrHeadroom) || src.read || !src.closed {
		t.Fatal(w, err, src)
	}
	onlyLock(t, path)
}

func TestCiphertextExistingTargetsAndSymlinksAreNotOverwritten(t *testing.T) {
	for _, suffix := range []string{".part", ".blob"} {
		t.Run(suffix, func(t *testing.T) {
			s, path := testStore(t)
			data := bytes.Repeat([]byte{2}, 64)
			a := attempt(data)
			old := []byte("retained artifact")
			name := filepath.Join(path, a.ID+suffix)
			if err := os.WriteFile(name, old, 0600); err != nil {
				t.Fatal(err)
			}
			src := &spySource{data: data}
			if w, err := s.Write(t.Context(), a, src); w != nil || !errors.Is(err, ErrConflict) || src.read {
				t.Fatal(w, err)
			}
			got, err := os.ReadFile(name)
			if err != nil || !bytes.Equal(got, old) {
				t.Fatal("overwrote/removed existing artifact", err)
			}
			if err = s.Remove(t.Context(), a.ID); err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), "keep.bin")
			if err = os.WriteFile(outside, old, 0600); err != nil {
				t.Fatal(err)
			}
			if err = os.Symlink(outside, name); err != nil {
				t.Fatal(err)
			}
			if _, err = s.Write(t.Context(), a, source(data)); err == nil {
				t.Fatal("followed symlink")
			}
			if err = s.Remove(t.Context(), a.ID); err != nil {
				t.Fatal("could not unlink symlink itself", err)
			}
			got, err = os.ReadFile(outside)
			if err != nil || !bytes.Equal(got, old) {
				t.Fatal("modified symlink target", err)
			}
			onlyLock(t, path)
		})
	}
}

type blockedSource struct {
	entered chan struct{}
	closed  chan struct{}
	once    sync.Once
}

func newBlockedSource() *blockedSource {
	return &blockedSource{entered: make(chan struct{}), closed: make(chan struct{})}
}
func (r *blockedSource) Read([]byte) (int, error) {
	close(r.entered)
	<-r.closed
	return 0, io.ErrClosedPipe
}
func (r *blockedSource) Close() error { r.once.Do(func() { close(r.closed) }); return nil }
func awaitRead(t *testing.T, r *blockedSource) {
	t.Helper()
	select {
	case <-r.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not start")
	}
}
func TestCiphertextCancellationCapacityAndHandleLifetime(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{3}, 64)
	a := attempt(data)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src := newBlockedSource()
	done := make(chan error, 1)
	go func() { _, err := s.Write(ctx, a, src); done <- err }()
	awaitRead(t, src)
	if err := s.Remove(t.Context(), a.ID); !errors.Is(err, ErrBusy) {
		t.Fatal("unlinked active writer", err)
	}
	if err := s.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("released owner with writer active", err)
	}
	if other, err := Open(path, s.config); other != nil || !errors.Is(err, ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("second owner", err)
	}
	if _, err := s.Write(t.Context(), a, source(data)); !errors.Is(err, ErrBusy) {
		t.Fatal("same attempt concurrent writer", err)
	}
	second := attempt(data)
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	src2 := newBlockedSource()
	done2 := make(chan error, 1)
	go func() { _, err := s.Write(ctx2, second, src2); done2 <- err }()
	awaitRead(t, src2)
	spy := &spySource{data: data}
	if _, err := s.Write(t.Context(), attempt(data), spy); !errors.Is(err, ErrBusy) || spy.read {
		t.Fatal("global stream bound", err)
	}
	cancel()
	cancel2()
	for _, ch := range []chan error{done, done2} {
		select {
		case err := <-ch:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("cancellation leaked writer")
		}
	}
	onlyLock(t, path)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Write(t.Context(), a, source(data)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

type faultFilesystem struct {
	filesystem
	phase string
	fired bool
	syncs int
	crash bool
}

func (f *faultFilesystem) hit(phase string) error {
	if f.phase == phase && !f.fired {
		f.fired = true
		if f.crash {
			os.Exit(23)
		}
		return errInjected
	}
	return nil
}
func (f *faultFilesystem) OpenExclusive(name string) (writerFile, error) {
	if err := f.hit("create error"); err != nil {
		return nil, err
	}
	w, err := f.filesystem.OpenExclusive(name)
	if err != nil {
		return nil, err
	}
	f.hit("after create")
	return &faultWriter{writerFile: w, owner: f}, nil
}
func (f *faultFilesystem) Link(a, b string) error {
	if err := f.hit("link error"); err != nil {
		return err
	}
	if err := f.filesystem.Link(a, b); err != nil {
		return err
	}
	f.hit("after link")
	return nil
}
func (f *faultFilesystem) Remove(name string) error {
	if err := f.hit("unlink error"); err != nil {
		return err
	}
	if err := f.filesystem.Remove(name); err != nil {
		return err
	}
	f.hit("after unlink")
	return nil
}
func (f *faultFilesystem) SyncDirectory() error {
	f.syncs++
	phase := "directory sync " + strconv.Itoa(f.syncs)
	if err := f.hit(phase + " error"); err != nil {
		return err
	}
	if err := f.filesystem.SyncDirectory(); err != nil {
		return err
	}
	f.hit("after " + phase)
	return nil
}

type faultWriter struct {
	writerFile
	owner *faultFilesystem
}

func (w *faultWriter) Write(p []byte) (int, error) {
	n, err := w.writerFile.Write(p)
	if err != nil {
		return n, err
	}
	return n, w.owner.hit("after write")
}
func (w *faultWriter) Sync() error {
	if err := w.owner.hit("file sync error"); err != nil {
		return err
	}
	if err := w.writerFile.Sync(); err != nil {
		return err
	}
	w.owner.hit("after file sync")
	return nil
}
func (w *faultWriter) Close() error {
	err := w.writerFile.Close()
	if err != nil {
		return err
	}
	return w.owner.hit("after file close")
}

func TestCiphertextFilesystemErrorsNeverReturnWritten(t *testing.T) {
	for _, phase := range []string{"create error", "after write", "file sync error", "after file close", "link error", "directory sync 1 error", "unlink error", "directory sync 2 error"} {
		t.Run(phase, func(t *testing.T) {
			s, path := testStore(t)
			data := bytes.Repeat([]byte{4}, 64)
			a := attempt(data)
			fault := &faultFilesystem{filesystem: s.fs, phase: phase}
			s.fs = fault
			if w, err := s.Write(t.Context(), a, source(data)); w != nil || !errors.Is(err, errInjected) || !fault.fired {
				t.Fatal(w, err, fault.fired)
			}
			// One-shot fault cleanup may succeed; quota release still requires a
			// later explicit Remove after the coordinator's Deleting/join fence.
			if err := s.Remove(t.Context(), a.ID); err != nil {
				t.Fatal(err)
			}
			onlyLock(t, path)
		})
	}
}

func TestCiphertextCrashChild(t *testing.T) {
	phase := os.Getenv("ZCHAT_BLOB_CRASH_PHASE")
	if phase == "" {
		t.Skip("child-process crash fixture")
	}
	s, err := Open(os.Getenv("ZCHAT_BLOB_CRASH_ROOT"), Config{MaxConcurrent: 1, HeadroomBytes: 1 << 20, WriteTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	f := &faultFilesystem{filesystem: s.fs, phase: phase, crash: true}
	s.fs = f
	data := bytes.Repeat([]byte{5}, 64)
	a := attempt(data)
	a.ID = os.Getenv("ZCHAT_BLOB_CRASH_ID")
	if _, err = s.Write(t.Context(), a, source(data)); err != nil {
		t.Fatal(err)
	}
	t.Fatal("crash phase not reached")
}
func TestCiphertextProcessCrashNeverAdoptsArtifacts(t *testing.T) {
	for _, phase := range []string{"after create", "after write", "after file sync", "after file close", "after link", "after directory sync 1", "after unlink", "after directory sync 2"} {
		t.Run(phase, func(t *testing.T) {
			path := privateTemp(t)
			id := uuid.NewString()
			exe, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, exe, "-test.run=^TestCiphertextCrashChild$")
			cmd.Env = append(os.Environ(), "ZCHAT_BLOB_CRASH_PHASE="+phase, "ZCHAT_BLOB_CRASH_ROOT="+path, "ZCHAT_BLOB_CRASH_ID="+id)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 23 {
				t.Fatalf("crash fixture failed: %v: %s", err, output)
			}
			s, err := Open(path, Config{MaxConcurrent: 1, HeadroomBytes: 1 << 20, WriteTimeout: 5 * time.Second})
			if err != nil {
				t.Fatal("owner lock survived dead process", err)
			}
			defer s.Close()
			data := bytes.Repeat([]byte{5}, 64)
			a := attempt(data)
			a.ID = id
			spy := &spySource{data: data}
			if w, err := s.Write(t.Context(), a, spy); w != nil || !errors.Is(err, ErrConflict) || spy.read {
				t.Fatal("orphan became Ready/re-encrypted", w, err)
			}
			// Models an EXPLICIT coordinator Deleting decision, not auto-adoption
			// or auto-GC on Open. PG coordination is a later integration step.
			if err = s.Remove(t.Context(), id); err != nil {
				t.Fatal(err)
			}
			onlyLock(t, path)
		})
	}
}

func TestCiphertextUnsafeRootAndOwnerLock(t *testing.T) {
	path := t.TempDir()
	cfg := Config{MaxConcurrent: 1, HeadroomBytes: 1 << 20, WriteTimeout: 5 * time.Second}
	if err := os.Chmod(path, 0755); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path, cfg); s != nil || !errors.Is(err, ErrRoot) {
		t.Fatal(s, err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "root-link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(link, cfg); s != nil || !errors.Is(err, ErrRoot) {
		t.Fatal(s, err)
	}
	outside := filepath.Join(t.TempDir(), "outside.lock")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(path, ".owner.lock")
	if err := os.Symlink(outside, lock); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path, cfg); s != nil || err == nil {
		t.Fatal("followed symlink owner lock", s, err)
	}
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(outside, lock); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path, cfg); s != nil || !errors.Is(err, ErrRoot) {
		t.Fatal("adopted hardlinked lock", s, err)
	}
	if data, err := os.ReadFile(outside); err != nil || string(data) != "keep" {
		t.Fatal("changed external lock", err)
	}
}

func TestCiphertextOwnDeadlineUnblocksSource(t *testing.T) {
	s, path := testStore(t)
	s.config.WriteTimeout = 75 * time.Millisecond
	src := newBlockedSource()
	done := make(chan error, 1)
	data := bytes.Repeat([]byte{11}, 64)
	go func() { _, err := s.Write(context.Background(), attempt(data), src); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("store deadline leaked a reader")
	}
	select {
	case <-src.closed:
	default:
		t.Fatal("deadline did not close source")
	}
	onlyLock(t, path)
}

func TestCiphertextHeadroomIncludesOtherActiveWrites(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{7}, 64)
	s.available = func() (uint64, error) { return s.config.HeadroomBytes + 64, nil }
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	src := newBlockedSource()
	done := make(chan error, 1)
	go func() { _, err := s.Write(ctx, attempt(data), src); done <- err }()
	awaitRead(t, src)
	spy := &spySource{data: data}
	if w, err := s.Write(t.Context(), attempt(data), spy); w != nil || !errors.Is(err, ErrHeadroom) || spy.read {
		t.Fatal("overcommitted disk headroom", w, err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	a := attempt(data)
	if _, err := s.Write(t.Context(), a, source(data)); err != nil {
		t.Fatal("did not release active budget", err)
	}
	if err := s.Remove(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	onlyLock(t, path)
}

type blockedDeletion struct{ filesystem }

func (blockedDeletion) Remove(string) error { return errInjected }
func TestCiphertextFailedCleanupRemainsObservable(t *testing.T) {
	for _, cause := range []string{"hash", "excess"} {
		t.Run(cause, func(t *testing.T) {
			s, path := testStore(t)
			data := bytes.Repeat([]byte{8}, 64)
			a := attempt(data)
			if cause == "hash" {
				a.SHA256[0]++
			} else {
				data = append(data, 1)
			}
			base := s.fs
			s.fs = blockedDeletion{base}
			if w, err := s.Write(t.Context(), a, source(data)); w != nil || !errors.Is(err, ErrIntegrity) || !errors.Is(err, errInjected) {
				t.Fatal(w, err)
			}
			info, err := os.Stat(filepath.Join(path, a.ID+".part"))
			if err != nil || info.Size() != a.Bytes {
				t.Fatal("lost evidence of failed unlink", info, err)
			}
			if err = s.Remove(t.Context(), a.ID); !errors.Is(err, errInjected) {
				t.Fatal("claimed successful reclamation", err)
			}
			s.fs = base
			if err = s.Remove(t.Context(), a.ID); err != nil {
				t.Fatal(err)
			}
			onlyLock(t, path)
		})
	}
}

type hookSource struct {
	*bytes.Reader
	hook func()
	once sync.Once
}

func (r *hookSource) Read(p []byte) (int, error) { r.once.Do(r.hook); return r.Reader.Read(p) }
func (*hookSource) Close() error                 { return nil }
func TestCiphertextPublicationDoesNotOverwriteRacingTarget(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{9}, 64)
	a := attempt(data)
	retained := []byte("racing target must survive")
	var hookErr error
	src := &hookSource{Reader: bytes.NewReader(data), hook: func() { hookErr = os.WriteFile(filepath.Join(path, a.ID+".blob"), retained, 0600) }}
	if w, err := s.Write(t.Context(), a, src); w != nil || !errors.Is(err, fs.ErrExist) || hookErr != nil {
		t.Fatal(w, err, hookErr)
	}
	got, err := os.ReadFile(filepath.Join(path, a.ID+".blob"))
	if err != nil || !bytes.Equal(got, retained) {
		t.Fatal("overwrote late target", err)
	}
	if _, err = os.Stat(filepath.Join(path, a.ID+".part")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatal("partial survived conflict", err)
	}
	if err = s.Remove(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
}

type waitCloseSource struct {
	entered chan struct{}
	release chan struct{}
}

func (*waitCloseSource) Read([]byte) (int, error) { return 0, io.EOF }
func (r *waitCloseSource) Close() error           { close(r.entered); <-r.release; return nil }
func TestCiphertextEarlyFailureClosesSourceBeforeReleasingOwner(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{10}, 64)
	a := attempt(data)
	if err := os.WriteFile(filepath.Join(path, a.ID+".blob"), data, 0600); err != nil {
		t.Fatal(err)
	}
	src := &waitCloseSource{entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	defer release.Do(func() { close(src.release) })
	done := make(chan error, 1)
	go func() { _, err := s.Write(t.Context(), a, src); done <- err }()
	select {
	case <-src.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("source not closed")
	}
	if err := s.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("released owner before source closed", err)
	}
	if err := s.Remove(t.Context(), a.ID); !errors.Is(err, ErrBusy) {
		t.Fatal("unlinked before operation finished", err)
	}
	release.Do(func() { close(src.release) })
	if err := <-done; !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := s.Remove(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
}
