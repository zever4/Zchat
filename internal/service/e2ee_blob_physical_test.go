//go:build linux

package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
)

func TestBlobPhysicalAttachShutdownKeepsRootUntilInventoryJoins(t *testing.T) {
	f := uploadCoreUnattached(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	m := &startupRepositoryModel{E2EEBlobWriterRepository: f.m, hook: func() { close(entered); <-release }}
	f.s.repo = m
	budget := testPhysicalBudget(t)
	done := make(chan error, 1)
	go func() { done <- f.s.AttachBudget(t.Context(), budget, startupTestConfig()) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("attach inventory not entered")
	}
	src := &physicalSpySource{}
	if _, err := f.s.Upload(t.Context(), "", nil, f.r, src); !errors.Is(err, domain.ErrE2EEBlobNotReady) || src.reads != 0 || !src.closed {
		t.Fatal("upload raced attachment census", src, err)
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
		t.Fatal("lost root during attach", err)
	}
	once.Do(func() { close(release) })
	if err = <-done; err == nil {
		t.Fatal("cancelled attach succeeded")
	}
	if err = f.s.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
}
