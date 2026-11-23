package uploadstorage

import (
	"errors"
	"math"
	"os"
	"syscall"

	"github.com/zever4/Zchat/internal/storagebudget"
	"golang.org/x/sys/unix"
)

func openOwner(root *os.Root) (*os.File, uint64, error) {
	d, err := root.Open(".")
	if err != nil {
		return nil, 0, err
	}
	defer d.Close()
	var st unix.Stat_t
	if err = unix.Fstat(int(d.Fd()), &st); err != nil {
		return nil, 0, err
	}
	if st.Uid != uint32(os.Geteuid()) || st.Mode&0022 != 0 {
		return nil, 0, ErrUnsafe
	}
	f, err := root.OpenFile(".storage-owner", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return nil, 0, err
	}
	var fs unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &fs); err != nil {
		f.Close()
		return nil, 0, err
	}
	if fs.Uid != st.Uid || fs.Mode&unix.S_IFMT != unix.S_IFREG || fs.Mode&0077 != 0 || fs.Nlink != 1 {
		f.Close()
		return nil, 0, ErrUnsafe
	}
	if err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, 0, storagebudget.ErrBusy
		}
		return nil, 0, err
	}
	return f, uint64(st.Dev), nil
}
func probe(root *os.Root) (storagebudget.Disk, uint64, error) {
	f, err := root.Open(".")
	if err != nil {
		return storagebudget.Disk{}, 0, err
	}
	defer f.Close()
	var s unix.Statfs_t
	var st unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &st); err != nil {
		return storagebudget.Disk{}, 0, err
	}
	if err = unix.Fstatfs(int(f.Fd()), &s); err != nil {
		return storagebudget.Disk{}, 0, err
	}
	if s.Bsize <= 0 || s.Bavail > math.MaxUint64/uint64(s.Bsize) || s.Files == 0 {
		return storagebudget.Disk{}, 0, ErrUnsafe
	}
	return storagebudget.Disk{FreeBytes: s.Bavail * uint64(s.Bsize), FreeEntries: s.Ffree, FSID: uint64(st.Dev)}, max(65536, uint64(s.Bsize)), nil
}
func measured(info os.FileInfo, dev uint64, unit uint64) (storagebudget.Usage, error) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(s.Dev) != dev || s.Uid != uint32(os.Geteuid()) || s.Mode&0022 != 0 || s.Blocks < 0 || s.Nlink != 1 && !info.IsDir() || !info.IsDir() && !info.Mode().IsRegular() {
		return storagebudget.Usage{}, ErrUnsafe
	}
	u, err := storagebudget.Footprint(uint64(max(info.Size(), 1)), 1, unit, entryAllowance)
	if err != nil {
		return u, err
	}
	if uint64(s.Blocks) > (1<<63-1-entryAllowance)/512 {
		return u, ErrUnsafe
	}
	u.Bytes = max(u.Bytes, uint64(s.Blocks)*512+entryAllowance)
	return u, nil
}
