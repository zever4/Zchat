// Package storagebudget accounts for retained storage and admitted write
// footprints together. It owns no files: adapters must seed an owned census and
// settle reservations only after their handles, deletion and directory sync end.
package storagebudget

import (
	"fmt"
	"strconv"
)

type Config struct {
	TotalBytes, AccountBytes     uint64
	TotalEntries, AccountEntries uint64
	FreeBytes, FreeEntries       uint64
	Streams, AccountStreams      int
	Downloads, AccountDownloads  int
}

// Decimal GB, as displayed to users. Headroom is checked against actual free
// space, so these caps never promise that a smaller VPS has 50GB available.
func ConfigFromEnv(get func(string) string) (Config, error) {
	c := Config{TotalBytes: 50_000_000_000, AccountBytes: 10_000_000_000, TotalEntries: 200000, AccountEntries: 50000, FreeBytes: 1_000_000_000, FreeEntries: 1024, Streams: 8, AccountStreams: 2, Downloads: 64, AccountDownloads: 16}
	for _, v := range []struct {
		key   string
		dst   *uint64
		scale uint64
	}{
		{"STORAGE_TOTAL_GB", &c.TotalBytes, 1_000_000_000}, {"STORAGE_ACCOUNT_GB", &c.AccountBytes, 1_000_000_000}, {"STORAGE_FREE_GB", &c.FreeBytes, 1_000_000_000},
		{"STORAGE_TOTAL_FILES", &c.TotalEntries, 1}, {"STORAGE_ACCOUNT_FILES", &c.AccountEntries, 1}, {"STORAGE_FREE_INODES", &c.FreeEntries, 1},
	} {
		if raw := get(v.key); raw != "" {
			n, err := strconv.ParseUint(raw, 10, 64)
			if err != nil || n == 0 || n > (1<<63-1)/v.scale {
				return Config{}, fmt.Errorf("invalid %s: positive bounded integer required", v.key)
			}
			*v.dst = n * v.scale
		}
	}
	for _, v := range []struct {
		key string
		dst *int
	}{{"STORAGE_UPLOADS", &c.Streams}, {"STORAGE_ACCOUNT_UPLOADS", &c.AccountStreams}, {"STORAGE_DOWNLOADS", &c.Downloads}, {"STORAGE_ACCOUNT_DOWNLOADS", &c.AccountDownloads}} {
		if raw := get(v.key); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > 1024 {
				return Config{}, fmt.Errorf("invalid %s: expected1..1024", v.key)
			}
			*v.dst = n
		}
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

func (c Config) Validate() error {
	if c.TotalBytes == 0 || c.TotalBytes > 1<<63-1 || c.AccountBytes == 0 || c.AccountBytes > c.TotalBytes || c.TotalEntries == 0 || c.TotalEntries > 1<<63-1 || c.AccountEntries == 0 || c.AccountEntries > c.TotalEntries || c.FreeBytes == 0 || c.FreeEntries == 0 || c.Streams < 1 || c.Streams > 1024 || c.AccountStreams < 1 || c.AccountStreams > c.Streams {
		return ErrInvalid
	}
	if c.Downloads < 1 || c.Downloads > 1024 || c.AccountDownloads < 1 || c.AccountDownloads > c.Downloads {
		return ErrInvalid
	}
	return nil
}
