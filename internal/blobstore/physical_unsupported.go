//go:build !linux

package blobstore

import (
	"github.com/zever4/Zchat/internal/storagebudget"
	"os"
)

func physicalDisk(*os.File) (storagebudget.Disk, uint64, error) {
	return storagebudget.Disk{}, 0, ErrUnsupported
}
func physicalUsage(os.FileInfo, uint64, uint64) (storagebudget.Usage, error) {
	return storagebudget.Usage{}, ErrUnsupported
}
