// Package uploadstorage owns ordinary uploads, their temporary copies and read
// leases. The budget is reconstructed from an exclusive, bounded physical census.
package uploadstorage

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/zever4/Zchat/internal/storagebudget"
)

var (
	ErrUnsafe      = errors.New("unsafe upload storage entry or filesystem")
	ErrUnsupported = errors.New("managed server upload storage requires Linux")
	ErrTooLarge    = errors.New("upload exceeds its reserved size")
)

const entryAllowance uint64 = 65536
const censusLimit = 1000000

var ownerName = regexp.MustCompile(`^(?:avatar|media|file|tmp)_u([1-9][0-9]*)_[0-9a-f]{32}(?:\.[a-z0-9]+)?$`)
var extension = regexp.MustCompile(`^\.[a-z0-9]{1,8}$`)

type entry struct {
	uid                                  int64
	charge                               storagebudget.Usage
	readers                              int
	charged, deleting, deleted, removing bool
}
type Manager struct {
	mu              sync.Mutex
	root            *os.Root
	owner           *os.File
	dev, unit       uint64
	budget          *storagebudget.Budget
	files           map[string]*entry
	dirBytes        uint64
	active, readers int
	closed          bool
}

func Open(ctx context.Context, path string, c storagebudget.Config) (m *Manager, err error) {
	if err = c.Validate(); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrUnsafe
	}
	r, err := os.OpenRoot(path)
	if err != nil {
		return nil, err
	}
	f, dev, err := openOwner(r)
	if err != nil {
		r.Close()
		return nil, err
	}
	m = &Manager{root: r, owner: f, dev: dev, files: make(map[string]*entry)}
	defer func() {
		if err != nil {
			f.Close()
			r.Close()
		}
	}()
	for _, dir := range []string{"files", "media", ".spool"} {
		if err = r.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		info, e := r.Lstat(dir)
		if e != nil {
			return nil, e
		}
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return nil, ErrUnsafe
		}
		if _, e = measured(info, dev, 65536); e != nil {
			return nil, e
		}
	}
	_, m.unit, err = probe(r)
	if err != nil {
		return nil, err
	}
	seed, err := m.census(ctx)
	if err != nil {
		return nil, err
	}
	m.budget, err = storagebudget.New(c, seed, func() (storagebudget.Disk, error) { d, _, e := probe(r); return d, e })
	if err != nil {
		return nil, err
	}
	if err = m.syncDir("."); err != nil {
		return nil, err
	}
	return m, nil
}
func (m *Manager) census(ctx context.Context) ([]storagebudget.AccountUsage, error) {
	var seed []storagebudget.AccountUsage
	count := 0
	for _, dir := range []string{".", "files", "media", ".spool"} {
		d, err := m.root.Open(dir)
		if err != nil {
			return nil, err
		}
		info, err := d.Stat()
		if err != nil {
			d.Close()
			return nil, err
		}
		u, err := measured(info, m.dev, m.unit)
		if err != nil {
			d.Close()
			return nil, err
		}
		seed = append(seed, storagebudget.AccountUsage{Usage: u})
		m.dirBytes += u.Bytes
		for {
			if err = ctx.Err(); err != nil {
				d.Close()
				return nil, err
			}
			items, e := d.ReadDir(64)
			if e != nil && e != io.EOF {
				d.Close()
				return nil, e
			}
			for _, item := range items {
				count++
				if count > censusLimit {
					d.Close()
					return nil, ErrUnsafe
				}
				name := item.Name()
				p := name
				if dir != "." {
					p = dir + "/" + name
				}
				info, er := m.root.Lstat(p)
				if er != nil {
					d.Close()
					return nil, er
				}
				if dir == "." && (name == "files" || name == "media" || name == ".spool") {
					continue
				}
				u, er = measured(info, m.dev, m.unit)
				if er != nil || !info.Mode().IsRegular() {
					d.Close()
					return nil, ErrUnsafe
				}
				uid := accountOf(name)
				if dir == ".spool" {
					if uid <= 0 || !strings.HasPrefix(name, "tmp_u") {
						d.Close()
						return nil, ErrUnsafe
					}
					if er = m.root.Remove(p); er != nil {
						d.Close()
						return nil, er
					}
					continue
				}
				if name != ".storage-owner" {
					m.files[p] = &entry{uid: uid, charge: u, charged: true}
				}
				seed = append(seed, storagebudget.AccountUsage{AccountID: uid, Usage: u})
			}
			if e == io.EOF {
				break
			}
		}
		if err = d.Close(); err != nil {
			return nil, err
		}
	}
	if err := m.syncDir(".spool"); err != nil {
		return nil, err
	}
	return seed, nil
}
func accountOf(name string) int64 {
	v := ownerName.FindStringSubmatch(name)
	if v == nil {
		return 0
	}
	n, err := strconv.ParseInt(v[1], 10, 64)
	if err != nil {
		return 0
	}
	return n
}
func (m *Manager) syncDir(name string) error {
	d, err := m.root.Open(name)
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}
func (m *Manager) refreshDirs() error {
	var bytes uint64
	for _, name := range []string{".", "files", "media", ".spool"} {
		i, err := m.root.Stat(name)
		if err != nil {
			return err
		}
		u, err := measured(i, m.dev, m.unit)
		if err != nil {
			return err
		}
		bytes += u.Bytes
	}
	var err error
	if bytes > m.dirBytes {
		err = m.budget.ObserveSystemGrowth(storagebudget.Usage{Bytes: bytes - m.dirBytes})
	} else if bytes < m.dirBytes {
		err = m.budget.Release(0, storagebudget.Usage{Bytes: m.dirBytes - bytes})
	}
	if err == nil {
		m.dirBytes = bytes
	}
	return err
}
func (m *Manager) Begin(ctx context.Context, uid int64, limit int64) (*Upload, error) {
	if limit <= 0 {
		return nil, storagebudget.ErrInvalid
	}
	max, err := storagebudget.Footprint(uint64(limit), 2, m.unit, entryAllowance)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, storagebudget.ErrClosed
	}
	if err = m.refreshDirs(); err != nil {
		return nil, err
	}
	r, err := m.budget.Reserve(ctx, uid, max)
	if err != nil {
		return nil, err
	}
	m.active++
	return &Upload{m: m, ctx: ctx, uid: uid, limit: limit, ticket: r, max: max}, nil
}
func (m *Manager) Usage() (storagebudget.Usage, int) { return m.budget.Usage() }

// Budget is shared with other exclusively owned storage adapters at startup.
func (m *Manager) Budget() *storagebudget.Budget { return m.budget }
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if m.active > 0 || m.readers > 0 || m.budget.Active() {
		return storagebudget.ErrBusy
	}
	m.closed = true
	return errors.Join(m.owner.Close(), m.root.Close())
}
func randomName(prefix string, uid int64, ext string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + "_u" + strconv.FormatInt(uid, 10) + "_" + hex.EncodeToString(b[:]) + ext, nil
}

type Input struct {
	*os.File
	once sync.Once
	err  error
}

func (f *Input) Close() error { f.once.Do(func() { f.err = f.File.Close() }); return f.err }

type created struct {
	name string
	file *Input
	keep bool
	e    *entry
}
type Upload struct {
	m          *Manager
	ctx        context.Context
	uid, limit int64
	ticket     *storagebudget.Reservation
	max        storagebudget.Usage
	created    []*created
	finished   bool
	stored     bool
}
type Stored struct {
	Name string
	Size int64
}

func (u *Upload) newFile(dir, prefix, ext string) (*created, error) {
	name, err := randomName(prefix, u.uid, ext)
	if err != nil {
		return nil, err
	}
	if dir != "" {
		name = dir + "/" + name
	}
	f, err := u.m.root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	c := &created{name: name, file: &Input{File: f}}
	u.created = append(u.created, c)
	return c, nil
}

// Spool creates at most one controlled temporary file; never uses global TMPDIR.
func (u *Upload) Spool(src io.Reader) (*Input, int64, error) {
	if u.finished || len(u.created) > 0 {
		return nil, 0, storagebudget.ErrInvalid
	}
	c, err := u.newFile(".spool", "tmp", "")
	if err != nil {
		return nil, 0, err
	}
	n, err := u.copy(c.file.File, src)
	if err != nil {
		return nil, n, err
	}
	if err = c.file.Close(); err != nil {
		return nil, n, err
	}
	f, err := u.m.root.Open(c.name)
	if err != nil {
		return nil, n, err
	}
	c.file = &Input{File: f}
	return c.file, n, nil
}
func (u *Upload) copy(dst io.Writer, src io.Reader) (int64, error) {
	n, err := io.CopyBuffer(dst, io.LimitReader(contextReader{u.ctx, src}, u.limit), make([]byte, 32<<10))
	if err != nil {
		return n, err
	}
	if n == u.limit {
		var extra [1]byte
		k, e := io.ReadFull(contextReader{u.ctx, src}, extra[:])
		if k > 0 {
			return n, ErrTooLarge
		}
		if e != io.EOF {
			return n, e
		}
	}
	if n == 0 {
		return n, storagebudget.ErrInvalid
	}
	return n, u.ctx.Err()
}

type contextReader struct {
	ctx context.Context
	src io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.src.Read(p)
}
func (u *Upload) Store(src io.Reader, kind, ext string) (Stored, error) {
	if u.finished || u.stored || kind != "avatar" && kind != "file" && kind != "media" || !extension.MatchString(ext) {
		return Stored{}, storagebudget.ErrInvalid
	}
	u.stored = true
	dir := ""
	if kind == "file" {
		dir = "files"
	} else if kind == "media" {
		dir = "media"
	}
	c, err := u.newFile(dir, kind, ext)
	if err != nil {
		return Stored{}, err
	}
	n, err := u.copy(c.file.File, src)
	if err == nil {
		err = c.file.Sync()
	}
	var info os.FileInfo
	if err == nil {
		info, err = c.file.Stat()
	}
	err = errors.Join(err, c.file.Close())
	if err != nil {
		return Stored{}, err
	}
	charge, err := measured(info, u.m.dev, u.m.unit)
	if err != nil {
		return Stored{}, err
	}
	if charge.Bytes > u.max.Bytes || charge.Entries > u.max.Entries {
		return Stored{}, ErrUnsafe
	}
	if dir == "" {
		dir = "."
	}
	if err = u.m.syncDir(dir); err != nil {
		return Stored{}, err
	}
	e := &entry{uid: u.uid, charge: charge}
	u.m.mu.Lock()
	u.m.files[c.name] = e
	u.m.mu.Unlock()
	c.e = e
	c.keep = true
	return Stored{Name: c.name, Size: n}, nil
}

// Discard is only for a confirmed non-acceptance, not an uncertain SQL COMMIT.
func (u *Upload) Discard(name string) error {
	if u.finished {
		return storagebudget.ErrClosed
	}
	for _, c := range u.created {
		if c.name == name && c.e != nil {
			c.keep = false
			return u.m.Remove(name)
		}
	}
	return storagebudget.ErrInvalid
}
func (u *Upload) Finish() error {
	if u.finished {
		return storagebudget.ErrClosed
	}
	u.finished = true
	var err error
	closeUnknown := false
	touched := make(map[string]bool)
	for _, c := range u.created {
		touched[path.Dir(c.name)] = true
		closeErr := c.file.Close()
		closeUnknown = closeUnknown || closeErr != nil
		err = errors.Join(err, closeErr)
		if !c.keep {
			if c.e != nil {
				err = errors.Join(err, u.m.Remove(c.name))
			} else {
				e := u.m.root.Remove(c.name)
				if !errors.Is(e, os.ErrNotExist) {
					err = errors.Join(err, e)
				}
			}
		}
	}
	for dir := range touched {
		err = errors.Join(err, u.m.syncDir(dir))
	}
	u.m.mu.Lock()
	defer u.m.mu.Unlock()
	var retained storagebudget.Usage
	for _, c := range u.created {
		if c.e != nil {
			e := c.e
			if !e.deleted || e.readers > 0 {
				retained.Bytes += e.charge.Bytes
				retained.Entries += e.charge.Entries
			}
		}
	}
	if err != nil {
		retained = u.max
	}
	if e := u.ticket.Settle(retained); e != nil {
		err = errors.Join(err, e)
	} else {
		for _, c := range u.created {
			if c.e != nil {
				c.e.charged = true
				if c.e.deleted && c.e.readers == 0 {
					if retained == u.max {
						err = errors.Join(err, u.m.budget.Release(c.e.uid, c.e.charge))
					}
					delete(u.m.files, c.name)
				}
			}
		}
	}
	if !closeUnknown {
		u.m.active--
	}
	return err
}
