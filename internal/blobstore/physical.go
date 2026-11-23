package blobstore

import (
	"context"
	"errors"

	"github.com/zever4/Zchat/internal/storagebudget"
)

const physicalEntryAllowance uint64 = 65536

type PhysicalArtifact struct {
	Artifact
	Usage storagebudget.Usage
}
type PhysicalSnapshot struct {
	Artifacts []PhysicalArtifact
	Metadata  storagebudget.Usage
	Unit      uint64
}

// PhysicalDisk only probes the pinned owned filesystem. It performs no SQL,
// body read or quota mutation and may be called under the common budget mutex.
func (s *Store) PhysicalDisk() (storagebudget.Disk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storagebudget.Disk{}, ErrClosed
	}
	d, _, err := physicalDisk(s.owner)
	return d, err
}

// PhysicalCensus requires a quiescent coordinator. It grants no Written proof
// and never promotes an orphan. The coordinator checks every attempt against PG.
func (s *Store) PhysicalCensus(ctx context.Context, max int) (*PhysicalSnapshot, error) {
	if max < 1 || max > 20000 {
		return nil, ErrInvalid
	}
	if err := s.begin(ctx, "physical-census", 0); err != nil {
		return nil, err
	}
	defer s.end("physical-census")
	disk, unit, err := physicalDisk(s.owner)
	if err != nil {
		return nil, err
	}
	items, err := s.inventory(ctx, max)
	if err != nil {
		return nil, err
	}
	result := &PhysicalSnapshot{Unit: unit}
	for _, name := range []string{".", ".owner.lock", ".store-id"} {
		info, e := s.root.Lstat(name)
		if e != nil {
			return nil, e
		}
		u, e := physicalUsage(info, disk.FSID, unit)
		if e != nil {
			return nil, e
		}
		result.Metadata.Bytes += u.Bytes
		result.Metadata.Entries += u.Entries
	}
	for _, a := range items {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		name := a.AttemptID + ".blob"
		if a.Part {
			name = a.AttemptID + ".part"
		}
		f, info, e := openArtifact(s.root, name, true)
		if e != nil {
			return nil, e
		}
		u, e := physicalUsage(info, disk.FSID, unit)
		e = errors.Join(e, f.Close())
		if e != nil {
			return nil, e
		}
		result.Artifacts = append(result.Artifacts, PhysicalArtifact{a, u})
	}
	return result, nil
}

func (s *Store) physicalWritten(ctx context.Context, w *Written) (storagebudget.Usage, error) {
	if w == nil || w.storageID != s.storageID || !validID(w.attempt.ID) {
		return storagebudget.Usage{}, ErrInvalid
	}
	if err := s.begin(ctx, w.attempt.ID, 0); err != nil {
		return storagebudget.Usage{}, err
	}
	defer s.end(w.attempt.ID)
	disk, unit, err := physicalDisk(s.owner)
	if err != nil {
		return storagebudget.Usage{}, err
	}
	f, info, err := openArtifact(s.root, w.attempt.ID+".blob", false)
	if err != nil {
		return storagebudget.Usage{}, err
	}
	u, err := physicalUsage(info, disk.FSID, unit)
	if info.Size() != w.attempt.Bytes {
		err = errors.Join(err, ErrIntegrity)
	}
	return u, errors.Join(err, f.Close())
}

// Directory allocation can grow even after all temporary leaves are removed.
// Such growth is retained system usage, independently of artifact reclamation.
func (s *Store) physicalDirectory() (storagebudget.Usage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storagebudget.Usage{}, ErrClosed
	}
	d, unit, err := physicalDisk(s.owner)
	if err != nil {
		return storagebudget.Usage{}, err
	}
	info, err := s.root.Stat(".")
	if err != nil {
		return storagebudget.Usage{}, err
	}
	return physicalUsage(info, d.FSID, unit)
}
