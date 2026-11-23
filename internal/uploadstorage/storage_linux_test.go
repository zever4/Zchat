package uploadstorage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zever4/Zchat/internal/storagebudget"
)

func config(t *testing.T) storagebudget.Config {
	t.Helper()
	c, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func manager(t *testing.T) (*Manager, string) {
	t.Helper()
	path := t.TempDir()
	m, err := Open(t.Context(), path, config(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m, path
}
func upload(t *testing.T, m *Manager, uid int64, data []byte) Stored {
	t.Helper()
	u, err := m.Begin(t.Context(), uid, 1024)
	if err != nil {
		t.Fatal(err)
	}
	f, n, err := u.Spool(bytes.NewReader(data))
	if err != nil || n != int64(len(data)) {
		t.Fatal(n, err)
	}
	s, err := u.Store(f, "file", ".bin")
	if err != nil {
		t.Fatal(err)
	}
	if err = u.Finish(); err != nil {
		t.Fatal(err)
	}
	return s
}
func TestOwnedStorageCensusReopenAndQuota(t *testing.T) {
	m, path := manager(t)
	base, _ := m.Usage()
	s := upload(t, m, 7, []byte("retained bytes"))
	used, _ := m.Usage()
	if used.Bytes <= base.Bytes || !strings.Contains(s.Name, "_u7_") {
		t.Fatal(used, s)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	c := config(t)
	c.AccountBytes = 65536 // Existing data is preserved but new writes must fail.
	r, err := Open(t.Context(), path, c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = r.Begin(t.Context(), 7, 1); !errors.Is(err, storagebudget.ErrQuota) {
		t.Fatal("account quota lost after restart", err)
	}
	f, err := r.OpenFile(s.Name)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(f)
	if err != nil || string(got) != "retained bytes" {
		t.Fatal(string(got), err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if err = r.Remove(s.Name); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(filepath.Join(path, s.Name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
}
func TestDeletedOpenFileStaysChargedUntilReaderCloses(t *testing.T) {
	m, _ := manager(t)
	s := upload(t, m, 1, []byte("still open"))
	before, _ := m.Usage()
	reader, err := m.OpenFile(s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Remove(s.Name); err != nil {
		t.Fatal(err)
	}
	if after, _ := m.Usage(); after != before {
		t.Fatal("freed deleted-but-open file", before, after)
	}
	if err = m.Close(); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal("dropped owner with live reader", err)
	}
	if _, err = m.OpenFile(s.Name); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil || string(data) != "still open" {
		t.Fatal(string(data), err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	after, _ := m.Usage()
	if after.Bytes >= before.Bytes || after.Entries != before.Entries-1 {
		t.Fatal(after, before)
	}
}
func TestDeletionDuringUnsettledUploadHasNoDoubleRelease(t *testing.T) {
	m, _ := manager(t)
	base, _ := m.Usage()
	u, err := m.Begin(t.Context(), 2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	s, err := u.Store(strings.NewReader("value"), "avatar", ".png")
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.OpenFile(s.Name)
	if err != nil {
		t.Fatal(err)
	}
	if err = u.Discard(s.Name); err != nil {
		t.Fatal(err)
	}
	if err = u.Finish(); err != nil {
		t.Fatal(err)
	}
	if used, _ := m.Usage(); used.Bytes <= base.Bytes {
		t.Fatal("unsettled file with reader released")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if got, _ := m.Usage(); got != base {
		t.Fatal("double/missing release", base, got)
	}
}

type brokenReader struct{}

func (brokenReader) Read([]byte) (int, error) { return 0, errors.New("injected source failure") }
func TestPartialAndOversizeCleanupAndOwnerPin(t *testing.T) {
	m, path := manager(t)
	base, _ := m.Usage()
	for _, src := range []io.Reader{io.MultiReader(strings.NewReader("part"), brokenReader{}), strings.NewReader("oversize")} {
		u, err := m.Begin(t.Context(), 1, 7)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = u.Store(src, "file", ".bin"); err == nil {
			t.Fatal("accepted invalid stream")
		}
		if err = m.Close(); !errors.Is(err, storagebudget.ErrBusy) {
			t.Fatal(err)
		}
		if err = u.Finish(); err != nil {
			t.Fatal(err)
		}
		if got, _ := m.Usage(); got != base {
			t.Fatal("partial leak", got, base)
		}
	}
	items, err := os.ReadDir(filepath.Join(path, "files"))
	if err != nil || len(items) != 0 {
		t.Fatal(items, err)
	}
}
func TestRootLockAndUnsafeEntries(t *testing.T) {
	m, path := manager(t)
	if other, err := Open(t.Context(), path, config(t)); err == nil || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatal("second owner accepted")
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), filepath.Join(path, "files", "link")); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(t.Context(), path, config(t)); err == nil || other != nil {
		if other != nil {
			other.Close()
		}
		t.Fatal("accepted symlink census", err)
	}
}
func TestStartupReclaimsOnlyKnownTransientFilesAndCountsUnknownFiles(t *testing.T) {
	m, path := manager(t)
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	name, err := randomName("tmp", 4, "")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path, ".spool", name), []byte("abandoned"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(path, "files", "unattributed.bin"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	r, err := Open(context.Background(), path, config(t))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err = os.Stat(filepath.Join(path, ".spool", name)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if e := r.files["files/unattributed.bin"]; e == nil || e.uid != 0 || !e.charged {
		t.Fatal("unknown file disappeared from global census")
	}
}
