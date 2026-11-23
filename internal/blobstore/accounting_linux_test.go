package blobstore

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/uploadstorage"
)

func commonTestStorage(t *testing.T) (*uploadstorage.Manager, *storagebudget.Budget) {
	t.Helper()
	c, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	c.FreeBytes, c.FreeEntries = 1, 1
	c.AccountBytes = 1_000_000
	m, err := uploadstorage.Open(t.Context(), t.TempDir(), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m, m.Budget()
}

func TestPhysicalCensusUsesOneOperationSlot(t *testing.T) {
	s, err := Open(privateTemp(t), Config{MaxConcurrent: 1, HeadroomBytes: 1, WriteTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if snapshot, err := s.PhysicalCensus(t.Context(), 1); err != nil || snapshot.Metadata.Entries != 3 {
		t.Fatal(snapshot, err)
	}
}

func TestPhysicalAdmissionCannotOutgrowRestartManifest(t *testing.T) {
	_, b := commonTestStorage(t)
	s, _ := testStore(t)
	a, err := s.AttachAccounting(t.Context(), b, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte{42}, 64)
	ref := attempt(data)
	r, err := a.Reserve(t.Context(), 1, ref)
	if err != nil {
		t.Fatal(err)
	}
	w, err := s.Write(t.Context(), ref, source(data))
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Finish(w); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Reserve(t.Context(), 2, attempt(data)); !errors.Is(err, storagebudget.ErrQuota) {
		t.Fatal("new publication exceeds restart census bound", err)
	}
	p, err := s.RemoveVerified(t.Context(), ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Forget(ref.ID, p); err != nil {
		t.Fatal(err)
	}
	p.Close()
	second, err := a.Reserve(t.Context(), 2, attempt(data))
	if err != nil {
		t.Fatal("confirmed deletion did not restore manifest capacity", err)
	}
	if err = second.Finish(nil); err != nil {
		t.Fatal(err)
	}
}

func TestPrivateAndOrdinaryFilesShareAccountAndShutdownBudget(t *testing.T) {
	m, b := commonTestStorage(t)
	s, _ := testStore(t)
	a, err := s.AttachAccounting(t.Context(), b, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := b.Usage()
	data := bytes.Repeat([]byte{42}, 64)
	ref := attempt(data)
	r, err := a.Reserve(t.Context(), 1, ref)
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Close(); !errors.Is(err, storagebudget.ErrBusy) {
		t.Fatal("ordinary owner closed during private reservation", err)
	}
	if err = s.Close(); !errors.Is(err, ErrBusy) {
		t.Fatal("private owner closed before producer", err)
	}
	w, err := s.Write(t.Context(), ref, source(data))
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Finish(w); err != nil {
		t.Fatal(err)
	}
	u, _ := b.Usage()
	if u.Bytes <= base.Bytes || u.Entries != base.Entries+1 {
		t.Fatal(base, u)
	}
	if _, err = m.Begin(t.Context(), 1, 350000); !errors.Is(err, storagebudget.ErrQuota) {
		t.Fatal("ordinary upload ignored private account bytes", err)
	}
	other, err := m.Begin(t.Context(), 2, 350000)
	if err != nil {
		t.Fatal("independent account", err)
	}
	if err = other.Finish(); err != nil {
		t.Fatal(err)
	}
	proof, err := s.RemoveVerified(t.Context(), ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Forget(ref.ID, proof); err != nil {
		t.Fatal(err)
	}
	if err = a.Forget(ref.ID, proof); err != nil {
		t.Fatal("repeat freed twice", err)
	}
	if err = a.Forget(uuid.NewString(), proof); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong attempt proof", err)
	}
	proof.Close()
	if err = a.Forget(ref.ID, proof); !errors.Is(err, ErrInvalid) {
		t.Fatal("closed proof", err)
	}
	if got, _ := b.Usage(); got != base {
		t.Fatal("quota not restored after durable unlink", got, base)
	}
	ok, err := m.Begin(t.Context(), 1, 350000)
	if err != nil {
		t.Fatal(err)
	}
	ok.Finish()
	if _, err = s.AttachAccounting(t.Context(), b, nil, 16); !errors.Is(err, ErrBusy) {
		t.Fatal("attached same root twice", err)
	}
}

func TestFailedPrivateWriteRetainsFullChargeUntilRemovalProof(t *testing.T) {
	_, b := commonTestStorage(t)
	s, _ := testStore(t)
	a, err := s.AttachAccounting(t.Context(), b, nil, 16)
	if err != nil {
		t.Fatal(err)
	}
	base, _ := b.Usage()
	data := bytes.Repeat([]byte{42}, 64)
	ref := attempt(data)
	r, err := a.Reserve(t.Context(), 1, ref)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Write(t.Context(), ref, source(data[:10])); err == nil {
		t.Fatal("short data accepted")
	}
	if err = r.Finish(nil); err != nil {
		t.Fatal(err)
	}
	retained, n := b.Usage()
	if n != 0 || retained.Bytes != base.Bytes+r.max.Bytes || retained.Entries != base.Entries+2 {
		t.Fatal(retained, n, base, r.max)
	}
	wrong, _ := testStore(t)
	p, err := wrong.RemoveVerified(t.Context(), ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Forget(ref.ID, p); !errors.Is(err, ErrInvalid) {
		t.Fatal("foreign root proof accepted", err)
	}
	p.Close()
	p, err = s.RemoveVerified(t.Context(), ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Forget(ref.ID, p); err != nil {
		t.Fatal(err)
	}
	p.Close()
	if u, _ := b.Usage(); u != base {
		t.Fatal(u, base)
	}
}

func TestPhysicalCensusHasNoCryptoOrQuotaAuthority(t *testing.T) {
	_, b := commonTestStorage(t)
	s, _ := testStore(t)
	data := bytes.Repeat([]byte{42}, 64)
	ref := attempt(data)
	if _, err := s.Write(t.Context(), ref, source(data)); err != nil {
		t.Fatal(err)
	}
	before, _ := b.Usage()
	if _, err := s.AttachAccounting(t.Context(), b, nil, 16); !errors.Is(err, ErrIntegrity) {
		t.Fatal("unowned orphan adopted", err)
	}
	if u, _ := b.Usage(); u != before {
		t.Fatal("failed census changed budget", u, before)
	}
	a, err := s.AttachAccounting(t.Context(), b, map[string]int64{ref.ID: 7}, 16)
	if err != nil {
		t.Fatal(err)
	}
	if a.entries[ref.ID].uid != 7 {
		t.Fatal("lost sender ownership")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := a.Reserve(ctx, 7, attempt(bytes.Repeat([]byte{99}, 64))); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if b.Active() {
		t.Fatal("cancelled reserve leaked slot")
	}
}
