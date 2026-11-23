//go:build !linux

package uploadstorage

import (
	"github.com/zever4/Zchat/internal/storagebudget"
	"os"
)

func openOwner(*os.Root) (*os.File, uint64, error) { return nil, 0, ErrUnsupported }
func probe(*os.Root) (storagebudget.Disk, uint64, error) {
	return storagebudget.Disk{}, 0, ErrUnsupported
}
func measured(os.FileInfo, uint64, uint64) (storagebudget.Usage, error) {
	return storagebudget.Usage{}, ErrUnsupported
}
