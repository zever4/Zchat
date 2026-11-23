package uploadstorage

import (
	"context"
	"errors"
	"os"
	"path"
	"sync"

	"github.com/zever4/Zchat/internal/storagebudget"
)

type Reader struct {
	*os.File
	m      *Manager
	name   string
	e      *entry
	once   sync.Once
	err    error
	permit *storagebudget.ReadPermit
}

// OpenFile pins the inode until Reader.Close, independently of the HTTP session
// barrier. Unlink does not free its quota while a response still holds it open.
func (m *Manager) OpenFile(name string) (*Reader, error) {
	return m.OpenFileFor(context.Background(), 0, name)
}
func (m *Manager) OpenFileFor(ctx context.Context, uid int64, name string) (*Reader, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, storagebudget.ErrClosed
	}
	e := m.files[name]
	if e == nil || e.deleting || e.deleted {
		return nil, os.ErrNotExist
	}
	permit, err := m.budget.AcquireRead(ctx, uid)
	if err != nil {
		return nil, err
	}
	f, err := m.root.Open(name)
	if err != nil {
		permit.Close()
		return nil, err
	}
	info, err := f.Stat()
	if err == nil {
		_, err = measured(info, m.dev, m.unit)
	}
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		permit.Close()
		return nil, ErrUnsafe
	}
	e.readers++
	m.readers++
	return &Reader{File: f, m: m, name: name, e: e, permit: permit}, nil
}
func (r *Reader) Close() error {
	r.once.Do(func() {
		r.err = r.File.Close()
		r.m.mu.Lock()
		defer r.m.mu.Unlock()
		if r.err != nil {
			return
		} // Uncertain close retains inode/root ownership.
		r.e.readers--
		r.m.readers--
		r.permit.Close()
		if r.err == nil {
			r.err = r.m.freeDeleted(r.name, r.e)
		}
	})
	return r.err
}
func (m *Manager) freeDeleted(name string, e *entry) error {
	if !e.charged || !e.deleted || e.readers > 0 {
		return nil
	}
	if err := m.budget.Release(e.uid, e.charge); err != nil {
		return err
	}
	e.charged = false
	delete(m.files, name)
	return nil
}
func (m *Manager) Remove(name string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return storagebudget.ErrClosed
	}
	e := m.files[name]
	if e == nil {
		m.mu.Unlock()
		return nil
	}
	if e.removing {
		m.mu.Unlock()
		return storagebudget.ErrBusy
	}
	if e.deleted {
		err := m.freeDeleted(name, e)
		m.mu.Unlock()
		return err
	}
	e.deleting = true
	e.removing = true
	m.active++
	m.mu.Unlock()
	defer func() { m.mu.Lock(); e.removing = false; m.active--; m.mu.Unlock() }()
	err := m.root.Remove(name)
	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}
	err = errors.Join(err, m.syncDir(path.Dir(name)))
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	e.deleted = true
	return m.freeDeleted(name, e)
}
