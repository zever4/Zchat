package blobstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

func TestStorageInventoryAndVerifyPreserveCiphertext(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{0x42}, 64)
	a := attempt(data)
	if _, err := s.Write(t.Context(), a, source(data)); err != nil {
		t.Fatal(err)
	}
	items, err := s.Inventory(t.Context(), 1)
	if err != nil || len(items) != 1 || items[0] != (Artifact{AttemptID: a.ID, Bytes: 64}) {
		t.Fatal(items, err)
	}
	if err = s.Verify(t.Context(), a); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(path, a.ID+".blob")
	for _, changed := range [][]byte{bytes.Repeat([]byte{99}, 64), data[:63]} {
		if err = os.WriteFile(name, changed, 0600); err != nil {
			t.Fatal(err)
		}
		if err = s.Verify(t.Context(), a); !errors.Is(err, ErrIntegrity) {
			t.Fatal("corrupt blob verified", err)
		}
		got, err := os.ReadFile(name)
		if err != nil || !bytes.Equal(got, changed) {
			t.Fatal("verification mutated blob", err)
		}
	}
}

func TestStorageInventoryRejectsUnsafeEntriesWithoutDeletion(t *testing.T) {
	for _, kind := range []string{"name", "symlink", "fifo", "directory", "public permissions", "too many links"} {
		t.Run(kind, func(t *testing.T) {
			s, path := testStore(t)
			name := filepath.Join(path, uuid.NewString()+".blob")
			switch kind {
			case "name":
				name = filepath.Join(path, "unrecognized")
				if err := os.WriteFile(name, []byte("retain"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), name); err != nil {
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
			case "public permissions":
				if err := os.WriteFile(name, []byte("retain"), 0644); err != nil {
					t.Fatal(err)
				}
			case "too many links":
				if err := os.WriteFile(name, []byte("retain"), 0600); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if err := os.Link(name, filepath.Join(path, uuid.NewString()+".part")); err != nil {
						t.Fatal(err)
					}
				}
			}
			if items, err := s.Inventory(t.Context(), 8); err == nil || items != nil {
				t.Fatal("accepted unsafe artifact", items, err)
			}
			if _, err := os.Lstat(name); err != nil {
				t.Fatal("inventory deleted entry", err)
			}
		})
	}
}

func TestStorageInventoryInterruptedPublicationAndCaps(t *testing.T) {
	s, path := testStore(t)
	data := bytes.Repeat([]byte{0x42}, 64)
	a := attempt(data)
	part, blob := filepath.Join(path, a.ID+".part"), filepath.Join(path, a.ID+".blob")
	if err := os.WriteFile(part, data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(part, blob); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Inventory(t.Context(), 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.Inventory(t.Context(), 1); !errors.Is(err, ErrHeadroom) {
		t.Fatal(err)
	}
	items, err := s.Inventory(t.Context(), 2)
	if err != nil || len(items) != 2 {
		t.Fatal(items, err)
	}
	if err = s.Verify(t.Context(), a); !errors.Is(err, ErrRoot) {
		t.Fatal("published extra link treated as Ready", err)
	}
	if err = s.Remove(t.Context(), a.ID); err != nil {
		t.Fatal(err)
	}
	onlyLock(t, path)
}
