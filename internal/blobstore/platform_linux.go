package blobstore

import (
	"errors"
	"github.com/google/uuid"
	"io"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

func openArtifact(root *os.Root, name string, allowPublicationLink bool) (*os.File, os.FileInfo, error) {
	f, err := root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	var stat unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &stat); err != nil {
		f.Close()
		return nil, nil, err
	}
	maxLinks := uint64(1)
	if allowPublicationLink {
		maxLinks = 2
	}
	if stat.Uid != uint32(os.Geteuid()) || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&0077 != 0 || stat.Nlink < 1 || stat.Nlink > maxLinks {
		f.Close()
		return nil, nil, ErrRoot
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	return f, info, nil
}

func lockRoot(root *os.Root) (*os.File, error) {
	d, err := root.Open(".")
	if err != nil {
		return nil, err
	}
	var rootStat unix.Stat_t
	err = errors.Join(unix.Fstat(int(d.Fd()), &rootStat), d.Close())
	if err != nil {
		return nil, err
	}
	if rootStat.Mode&unix.S_IFMT != unix.S_IFDIR || rootStat.Mode&0077 != 0 || rootStat.Uid != uint32(os.Geteuid()) {
		return nil, ErrRoot
	}
	file, err := root.OpenFile(".owner.lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	var lockStat unix.Stat_t
	err = unix.Fstat(int(file.Fd()), &lockStat)
	if err != nil {
		file.Close()
		return nil, err
	}
	if rootStat.Uid != uint32(os.Geteuid()) || lockStat.Uid != rootStat.Uid || lockStat.Mode&unix.S_IFMT != unix.S_IFREG || lockStat.Mode&0077 != 0 || lockStat.Nlink != 1 {
		file.Close()
		return nil, ErrRoot
	}
	if err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrBusy
		}
		return nil, err
	}
	return file, nil
}

// No automatic adoption of a populated directory without an identity. A full
// marker is synced even on reopen, covering a crash during its first publication.
func rootIdentity(root *os.Root) (string, error) {
	f, err := root.OpenFile(".store-id", os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if errors.Is(err, os.ErrNotExist) {
		d, e := root.Open(".")
		if e != nil {
			return "", e
		}
		entries, e := d.ReadDir(2)
		closeErr := d.Close()
		if e != nil && e != io.EOF {
			return "", e
		}
		if closeErr != nil {
			return "", closeErr
		}
		if len(entries) != 1 || entries[0].Name() != ".owner.lock" {
			return "", ErrRoot
		}
		id := uuid.NewString()
		f, e = root.OpenFile(".store-id", os.O_CREATE|os.O_EXCL|os.O_WRONLY|unix.O_NOFOLLOW, 0600)
		if e != nil {
			return "", e
		}
		n, e := f.Write([]byte(id + "\n"))
		if e == nil && n != 37 {
			e = io.ErrShortWrite
		}
		if e == nil {
			e = f.Sync()
		}
		e = errors.Join(e, f.Close())
		if e != nil {
			return "", e
		}
		if e = (rootFilesystem{root}).SyncDirectory(); e != nil {
			return "", e
		}
		return id, nil
	}
	if err != nil {
		return "", err
	}
	defer f.Close()
	var st unix.Stat_t
	if err = unix.Fstat(int(f.Fd()), &st); err != nil {
		return "", err
	}
	if st.Uid != uint32(os.Geteuid()) || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&0077 != 0 || st.Nlink != 1 {
		return "", ErrRoot
	}
	data, err := io.ReadAll(io.LimitReader(f, 38))
	if err != nil {
		return "", err
	}
	if len(data) != 37 || data[36] != '\n' || !validID(string(data[:36])) {
		return "", ErrRoot
	}
	if err = errors.Join(f.Sync(), f.Close()); err != nil {
		return "", err
	}
	if err = (rootFilesystem{root}).SyncDirectory(); err != nil {
		return "", err
	}
	return string(data[:36]), nil
}

func diskAvailable(file *os.File) (uint64, error) {
	var stat unix.Statfs_t
	if err := unix.Fstatfs(int(file.Fd()), &stat); err != nil {
		return 0, err
	}
	if stat.Bsize <= 0 || stat.Bavail > math.MaxUint64/uint64(stat.Bsize) {
		return 0, ErrHeadroom
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}
