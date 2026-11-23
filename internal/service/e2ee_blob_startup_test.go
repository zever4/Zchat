//go:build linux

package service

import (
	"context"
	"errors"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
)

type startupRepositoryModel struct {
	E2EEBlobWriterRepository
	rows  []domain.E2EEBlobInventoryEntry
	pages int
	hook  func()
}

func (m *startupRepositoryModel) BindStorage(context.Context, string) error { return nil }
func (m *startupRepositoryModel) Inventory(ctx context.Context, root, after string, limit int) ([]domain.E2EEBlobInventoryEntry, error) {
	if m.hook != nil {
		m.hook()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.pages++
	result := make([]domain.E2EEBlobInventoryEntry, 0, limit)
	for _, r := range m.rows {
		if r.Cleanup.AttemptID > after {
			r.Cleanup.StorageID = root
			result = append(result, r)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}
func TestBlobStartupBoundedPagingAndCaps(t *testing.T) {
	f := uploadCoreUnattached(t)
	f.s.ready = false
	m := &startupRepositoryModel{E2EEBlobWriterRepository: f.m}
	for range 130 {
		m.rows = append(m.rows, domain.E2EEBlobInventoryEntry{State: "reserved", Bytes: 64, Cleanup: domain.E2EEBlobCleanupRequest{MessageID: uuid.NewString(), AttemptID: uuid.NewString(), Revision: 1}})
	}
	sort.Slice(m.rows, func(i, j int) bool { return m.rows[i].Cleanup.AttemptID < m.rows[j].Cleanup.AttemptID })
	f.s.repo = m
	cfg := startupTestConfig()
	cfg.MaxAttempts = 129
	if _, err := f.s.Reconcile(t.Context(), cfg); !errors.Is(err, domain.ErrE2EEBlobCapacity) || f.s.Ready() {
		t.Fatal(err)
	}
	m.pages = 0
	cfg.MaxAttempts = 130
	r, err := f.s.Reconcile(t.Context(), cfg)
	if err != nil || r.Attempts != 130 || m.pages != 2 || !f.s.Ready() {
		t.Fatal(r, m.pages, err)
	}
}
func TestBlobStartupCloseKeepsRootUntilInventoryJoins(t *testing.T) {
	f := uploadCoreUnattached(t)
	f.s.ready = false
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	m := &startupRepositoryModel{E2EEBlobWriterRepository: f.m, hook: func() { close(entered); <-release }}
	f.s.repo = m
	done := make(chan error, 1)
	go func() { _, err := f.s.Reconcile(t.Context(), startupTestConfig()); done <- err }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("startup not reached")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := f.s.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	other, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: time.Second})
	if other != nil || !errors.Is(err, blobstore.ErrBusy) {
		if other != nil {
			other.Close()
		}
		t.Fatal("root owner dropped while startup blocked", err)
	}
	once.Do(func() { close(release) })
	if err = <-done; err == nil || f.s.Ready() {
		t.Fatal("cancelled startup became ready", err)
	}
	if err = f.s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
