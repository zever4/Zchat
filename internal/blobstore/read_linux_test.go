package blobstore

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/uploadstorage"
	"golang.org/x/sys/unix"
)

func readFixture(t *testing.T, global, perAccount int) (*Store, *Accounting, *uploadstorage.Manager, *storagebudget.Budget, string) {
	t.Helper()
	c, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	c.Downloads, c.AccountDownloads = global, perAccount
	c.FreeBytes, c.FreeEntries = 1, 1
	m, err := uploadstorage.Open(t.Context(), t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	s, path := testStore(t)
	a, err := s.AttachAccounting(t.Context(), m.Budget(), nil, 32)
	if err != nil {
		t.Fatal(err)
	}
	return s, a, m, m.Budget(), path
}
func readFile(t *testing.T, s *Store, a *Accounting, data []byte) Attempt {
	t.Helper()
	ref := attempt(data)
	op, err := a.Reserve(t.Context(), 1, ref)
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.Write(t.Context(), ref, source(data))
	if err != nil {
		t.Fatal(err)
	}
	if err = op.Finish(w); err != nil {
		t.Fatal(err)
	}
	return ref
}
func lease(t *testing.T, a *Accounting, uid int64, ref Attempt) *ReadLease {
	t.Helper()
	r, err := a.OpenRead(t.Context(), uid, ref)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Close(); err != nil {
			t.Error(err)
		}
	})
	return r
}
func assertReadPins(t *testing.T, s *Store, b *storagebudget.Budget, id string, n int) {
	t.Helper()
	s.mu.Lock()
	count, total := s.readers[id], s.readCount
	s.mu.Unlock()
	if count != n || total != n || b.Active() != (n != 0) {
		t.Fatal("pin/permit ownership drifted", count, total, n, b.Active())
	}
}

func TestCiphertextReadExactBytesAndPinUntilExplicitClose(t *testing.T) {
	s, a, m, b, path := readFixture(t, 4, 4)
	data := bytes.Repeat([]byte{42}, bufferBytes*3+17)
	ref := readFile(t, s, a, data)
	before, _ := b.Usage()
	r := lease(t, a, 2, ref)
	if r.Attempt() != ref || r.StorageID() != s.StorageID() {
		t.Fatal("wrong file binding")
	}
	if _, ok := any(r).(io.WriterTo); ok {
		t.Fatal("hash bypass via WriterTo")
	}
	if _, ok := any(r).(io.Seeker); ok {
		t.Fatal("hash bypass via Seek")
	}
	var got []byte
	p := make([]byte, bufferBytes*4)
	for {
		n, err := r.Read(p)
		if n > bufferBytes {
			t.Fatal("unbounded read")
		}
		got = append(got, p[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if !bytes.Equal(got, data) {
		t.Fatal("ciphertext changed")
	}
	assertReadPins(t, s, b, ref.ID, 1)
	if after, _ := b.Usage(); after != before {
		t.Fatal("read mutated retained storage accounting", after, before)
	}
	if _, err := s.RemoveVerified(t.Context(), ref.ID); !errors.Is(err, ErrBusy) {
		t.Fatal("unlink passed a live reader", err)
	}
	if _, err := s.Write(t.Context(), ref, source(data)); !errors.Is(err, ErrBusy) {
		t.Fatal("write passed a live reader", err)
	}
	if err := s.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("root closed before caller joined", err)
	}
	if err := m.Close(); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal("shared owner closed before reader", err)
	}
	if other, err := Open(path, s.config); other != nil || !errors.Is(err, ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("root ownership lost", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	assertReadPins(t, s, b, ref.ID, 0)
	if _, err := r.Read(p); !errors.Is(err, ErrClosed) {
		t.Fatal("read after close", err)
	}
	proof, err := s.RemoveVerified(t.Context(), ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if err = a.Forget(ref.ID, proof); err != nil {
		t.Fatal(err)
	}
}

func TestCiphertextReadMultipleReadersAllowUnrelatedCleanup(t *testing.T) {
	s, a, _, b, _ := readFixture(t, 4, 4)
	data := bytes.Repeat([]byte{43}, 64)
	ref := readFile(t, s, a, data)
	other := readFile(t, s, a, data)
	x, y := lease(t, a, 2, ref), lease(t, a, 3, ref)
	assertReadPins(t, s, b, ref.ID, 2)
	proof, err := s.RemoveVerified(t.Context(), other.ID)
	if err != nil {
		t.Fatal("reader consumed cleanup slot", err)
	}
	if err = a.Forget(other.ID, proof); err != nil {
		t.Fatal(err)
	}
	proof.Close()
	x.Close()
	assertReadPins(t, s, b, ref.ID, 1)
	if _, err = s.RemoveVerified(t.Context(), ref.ID); !errors.Is(err, ErrBusy) {
		t.Fatal("last reader not pinned", err)
	}
	got, err := io.ReadAll(y)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal(err)
	}
	y.Close()
	assertReadPins(t, s, b, ref.ID, 0)
}

func TestCiphertextReadSharesOrdinaryGlobalAndRecipientLimits(t *testing.T) {
	s, a, m, b, _ := readFixture(t, 2, 1)
	ref := readFile(t, s, a, bytes.Repeat([]byte{44}, 64))
	u, err := m.Begin(t.Context(), 1, 128)
	if err != nil {
		t.Fatal(err)
	}
	input, _, err := u.Spool(bytes.NewReader([]byte("ordinary file")))
	if err != nil {
		t.Fatal(err)
	}
	stored, err := u.Store(input, "file", ".bin")
	if err != nil {
		t.Fatal(err)
	}
	if err = u.Finish(); err != nil {
		t.Fatal(err)
	}
	ordinary, err := m.OpenFileFor(t.Context(), 2, stored.Name)
	if err != nil {
		t.Fatal(err)
	}
	defer ordinary.Close()
	if _, err = a.OpenRead(t.Context(), 2, ref); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal("same recipient bypassed ordinary read limit", err)
	}
	private := lease(t, a, 3, ref)
	if _, err = m.OpenFileFor(t.Context(), 4, stored.Name); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal("global read limit split by namespace", err)
	}
	ordinary.Close()
	second := lease(t, a, 2, ref) // recipient2 differs from stored uploader1.
	if _, err = a.OpenRead(t.Context(), 2, ref); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal("recipient limit bypass", err)
	}
	private.Close()
	second.Close()
	assertReadPins(t, s, b, ref.ID, 0)
}

func TestCiphertextReadAllowedAboveLoweredByteCaps(t *testing.T) {
	s, _ := testStore(t)
	data := bytes.Repeat([]byte{45}, 64)
	ref := attempt(data)
	if _, err := s.Write(t.Context(), ref, source(data)); err != nil {
		t.Fatal(err)
	}
	c, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	c.TotalBytes, c.AccountBytes, c.FreeBytes, c.FreeEntries = 1, 1, 1, 1
	m, err := uploadstorage.Open(t.Context(), t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	a, err := s.AttachAccounting(t.Context(), m.Budget(), map[string]int64{ref.ID: 1}, 8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.Reserve(t.Context(), 1, attempt(data)); !errors.Is(err, storagebudget.ErrQuota) {
		t.Fatal("lowered cap did not block writes", err)
	}
	r := lease(t, a, 2, ref)
	got, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatal("lowered write cap hid existing file", err)
	}
}

func TestCiphertextReadRejectsUnsafeMissingAndWrongSizeWithoutPermitLeak(t *testing.T) {
	for _, kind := range []string{"missing", "symlink", "hardlink", "public mode", "fifo", "directory", "short", "long", "wrong expected size"} {
		t.Run(kind, func(t *testing.T) {
			s, a, _, b, path := readFixture(t, 1, 1)
			data := bytes.Repeat([]byte{46}, 64)
			ref := readFile(t, s, a, data)
			name := filepath.Join(path, ref.ID+".blob")
			switch kind {
			case "wrong expected size":
				ref.Bytes++
			case "short":
				if err := os.Truncate(name, 63); err != nil {
					t.Fatal(err)
				}
			case "long":
				if err := os.Truncate(name, 65); err != nil {
					t.Fatal(err)
				}
			case "public mode":
				if err := os.Chmod(name, 0644); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				if err := os.Link(name, filepath.Join(t.TempDir(), "outside")); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.Remove(name); err != nil {
					t.Fatal(err)
				}
				switch kind {
				case "symlink":
					if err := os.Symlink("/etc/passwd", name); err != nil {
						t.Fatal(err)
					}
				case "fifo":
					if err := unix.Mkfifo(name, 0600); err != nil {
						t.Fatal(err)
					}
				case "directory":
					if err := os.Mkdir(name, 0700); err != nil {
						t.Fatal(err)
					}
				}
			}
			if r, err := a.OpenRead(t.Context(), 2, ref); r != nil || err == nil {
				if r != nil {
					r.Close()
				}
				t.Fatal("unsafe source opened", err)
			}
			assertReadPins(t, s, b, ref.ID, 0)
			p, err := b.AcquireRead(t.Context(), 2)
			if err != nil {
				t.Fatal("leaked read permit", err)
			}
			p.Close()
		})
	}
}

func TestCiphertextReadDetectsDigestMutationTruncationAndExcess(t *testing.T) {
	for _, kind := range []string{"expected hash", "changed bytes", "truncate after open", "grow after open"} {
		t.Run(kind, func(t *testing.T) {
			s, a, _, b, path := readFixture(t, 1, 1)
			data := bytes.Repeat([]byte{47}, bufferBytes*2+17)
			ref := readFile(t, s, a, data)
			if kind == "expected hash" {
				ref.SHA256[0] ^= 1
			}
			r := lease(t, a, 2, ref)
			name := filepath.Join(path, ref.ID+".blob")
			switch kind {
			case "changed bytes":
				data[0] ^= 1
				if err := os.WriteFile(name, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "truncate after open":
				if err := os.Truncate(name, ref.Bytes-1); err != nil {
					t.Fatal(err)
				}
			case "grow after open":
				f, err := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.Write([]byte{1})
				if err != nil {
					t.Fatal(err)
				}
				f.Close()
			}
			if _, err := io.Copy(io.Discard, r); !errors.Is(err, ErrIntegrity) {
				t.Fatal("corrupt ciphertext reached successful EOF", err)
			}
			if _, err := r.Read(make([]byte, 1)); !errors.Is(err, ErrIntegrity) {
				t.Fatal("integrity failure not sticky", err)
			}
			assertReadPins(t, s, b, ref.ID, 1)
			r.Close()
			assertReadPins(t, s, b, ref.ID, 0)
		})
	}
}

func TestCiphertextReadCopyNStillChecksFinalDigestAndExcess(t *testing.T) {
	for _, kind := range []string{"exact", "hash", "excess"} {
		t.Run(kind, func(t *testing.T) {
			s, a, _, b, path := readFixture(t, 1, 1)
			data := bytes.Repeat([]byte{54}, bufferBytes+17)
			ref := readFile(t, s, a, data)
			if kind == "hash" {
				ref.SHA256[0] ^= 1
			}
			r := lease(t, a, 2, ref)
			if kind == "excess" {
				f, err := os.OpenFile(filepath.Join(path, ref.ID+".blob"), os.O_APPEND|os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				_, err = f.Write([]byte{1})
				closeErr := f.Close()
				if err != nil || closeErr != nil {
					t.Fatal(err, closeErr)
				}
			}
			n, err := io.CopyN(io.Discard, r, ref.Bytes)
			if kind == "exact" {
				if err != nil || n != ref.Bytes {
					t.Fatal(n, err)
				}
			} else if !errors.Is(err, ErrIntegrity) {
				t.Fatal("CopyN bypassed final verification", n, err)
			}
			assertReadPins(t, s, b, ref.ID, 1)
		})
	}
}

func TestCiphertextReadInvalidCancelledAndUnfinishedInputs(t *testing.T) {
	s, a, _, b, _ := readFixture(t, 1, 1)
	data := bytes.Repeat([]byte{48}, 64)
	ref := readFile(t, s, a, data)
	for _, bad := range []Attempt{{}, {ID: "../file", Bytes: 64}, {ID: ref.ID, Bytes: 36}, {ID: ref.ID, Bytes: 1 << 62}, {ID: uuid.NewString(), Bytes: 64}} {
		if r, err := a.OpenRead(t.Context(), 2, bad); r != nil || err == nil {
			t.Fatal("invalid/unknown file admitted", err)
		}
	}
	if r, err := a.OpenRead(t.Context(), 0, ref); r != nil || !errors.Is(err, ErrInvalid) {
		t.Fatal("anonymous read", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if r, err := a.OpenRead(ctx, 2, ref); r != nil || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read opened", err)
	}
	op, err := a.Reserve(t.Context(), 1, attempt(data))
	if err != nil {
		t.Fatal(err)
	}
	if r, err := a.OpenRead(t.Context(), 2, Attempt{ID: op.id, Bytes: 64}); r != nil || !errors.Is(err, ErrBusy) {
		t.Fatal("read unfinished accounting", err)
	}
	if err = op.Finish(nil); err != nil {
		t.Fatal(err)
	}
	if r, err := a.OpenRead(t.Context(), 2, Attempt{ID: op.id, Bytes: 64}); r != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing file treated as readable", err)
	}
	assertReadPins(t, s, b, ref.ID, 0)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if r, err := a.OpenRead(t.Context(), 2, ref); r != nil || !errors.Is(err, ErrClosed) {
		t.Fatal("closed root opened", err)
	}
	assertReadPins(t, s, b, ref.ID, 0)
}
