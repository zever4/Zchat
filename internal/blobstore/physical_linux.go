package blobstore

import (
	"math"
	"os"
	"syscall"

	"github.com/zever4/Zchat/internal/storagebudget"
	"golang.org/x/sys/unix"
)

func physicalDisk(f *os.File) (storagebudget.Disk, uint64, error) {
	var st unix.Stat_t
	var fs unix.Statfs_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return storagebudget.Disk{}, 0, err
	}
	if err := unix.Fstatfs(int(f.Fd()), &fs); err != nil {
		return storagebudget.Disk{}, 0, err
	}
	if fs.Bsize <= 0 || fs.Bavail > math.MaxUint64/uint64(fs.Bsize) || fs.Files == 0 {
		return storagebudget.Disk{}, 0, ErrHeadroom
	}
	return storagebudget.Disk{FreeBytes: fs.Bavail * uint64(fs.Bsize), FreeEntries: fs.Ffree, FSID: uint64(st.Dev)}, max(65536, uint64(fs.Bsize)), nil
}

func physicalUsage(info os.FileInfo, dev, unit uint64) (storagebudget.Usage, error) {
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(st.Dev) != dev || st.Uid != uint32(os.Geteuid()) || st.Mode&0077 != 0 || st.Blocks < 0 || (!info.IsDir() && (!info.Mode().IsRegular() || st.Nlink < 1 || st.Nlink > 2)) {
		return storagebudget.Usage{}, ErrRoot
	}
	u, err := storagebudget.Footprint(uint64(max(info.Size(), 1)), 1, unit, physicalEntryAllowance)
	if err != nil || uint64(st.Blocks) > (1<<63-1-physicalEntryAllowance)/512 {
		return storagebudget.Usage{}, ErrRoot
	}
	u.Bytes = max(u.Bytes, uint64(st.Blocks)*512+physicalEntryAllowance)
	return u, nil
}
