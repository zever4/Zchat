package blobstore

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

func TestStorageIdentityStableAndRemovalLease(t *testing.T) {
	s, path := testStore(t)
	id := s.StorageID()
	if !validID(id) {
		t.Fatal("invalid root identity", id)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := Open(path, s.config)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if r.StorageID() != id {
		t.Fatal("reopen changed storage identity")
	}
	attempt := uuid.NewString()
	proof, err := r.RemoveVerified(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close()
	if err = r.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("dropped owner with live removal lease", err)
	}
	if err = proof.Guard(uuid.NewString(), attempt, func() error { t.Fatal("wrong root proof used"); return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err = proof.Guard(id, uuid.NewString(), func() error { t.Fatal("wrong attempt proof used"); return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan error, 1)
	go func() { done <- proof.Guard(id, attempt, func() error { close(entered); <-release; return nil }) }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- proof.Close() }()
	if err = r.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("root released before proof commit returned", err)
	}
	once.Do(func() { close(release) })
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	if err = <-closed; err != nil {
		t.Fatal(err)
	}
	if err = proof.Guard(id, attempt, func() error { return nil }); !errors.Is(err, ErrInvalid) {
		t.Fatal("closed proof reused", err)
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStorageMarkerCannotAdoptOrChangeIdentity(t *testing.T) {
	for _, kind := range []string{"missing with artifact", "malformed", "symlink", "hardlink", "fifo"} {
		t.Run(kind, func(t *testing.T) {
			path := privateTemp(t)
			marker := filepath.Join(path, ".store-id")
			switch kind {
			case "missing with artifact":
				if err := os.WriteFile(filepath.Join(path, uuid.NewString()+".blob"), []byte("retained"), 0600); err != nil {
					t.Fatal(err)
				}
			case "malformed":
				if err := os.WriteFile(marker, []byte("broken marker\n"), 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), marker); err != nil {
					t.Fatal(err)
				}
			case "hardlink":
				external := filepath.Join(t.TempDir(), "id")
				if err := os.WriteFile(external, []byte(uuid.NewString()+"\n"), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Link(external, marker); err != nil {
					t.Fatal(err)
				}
			case "fifo":
				if err := unix.Mkfifo(marker, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if s, err := Open(path, Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second}); err == nil || s != nil {
				if s != nil {
					s.Close()
				}
				t.Fatal("adopted unsafe/unidentified root", err)
			}
			if kind == "missing with artifact" {
				if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
					t.Fatal("silently assigned ID to populated root", err)
				}
			}
		})
	}
}
