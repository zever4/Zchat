package main

import (
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
)

type blobStorageConfig struct {
	Enabled   bool
	Expose    bool
	Root      string
	Limits    domain.E2EEBlobLimits
	Files     blobstore.Config
	Upload    service.E2EEBlobUploadConfig
	Startup   service.E2EEBlobStartupConfig
	Collector service.E2EEBlobCollectorConfig
}

// Opt-in only. Logical PG caps are persisted independently of the mutable
// physical budget: lowering STORAGE_* must not reset rows or prevent reopening
// existing history. A changed E2EE_BLOB_* PG cap deliberately fails at Configure.
func blobConfigFromEnv(get func(string) string, shared storagebudget.Config) (blobStorageConfig, error) {
	var c blobStorageConfig
	c.Root = get("E2EE_BLOB_ROOT")
	switch get("ENABLE_EXPERIMENTAL_E2EE_BLOBS") {
	case "", "false":
		return c, nil
	case "true":
		c.Enabled = true
	default:
		return c, fmt.Errorf("ENABLE_EXPERIMENTAL_E2EE_BLOBS must be true or false")
	}
	c.Expose = get("ENABLE_EXPERIMENTAL_E2EE") == "true"
	if err := shared.Validate(); err != nil {
		return c, err
	}
	if !filepath.IsAbs(c.Root) {
		return c, fmt.Errorf("E2EE_BLOB_ROOT must name an existing absolute private directory")
	}
	c.Limits = domain.E2EEBlobLimits{GlobalBytes: 50_000_000_000, AccountBytes: 10_000_000_000, GlobalClaims: 50000, AccountClaims: 25000, GlobalAttempts: 100000, AccountAttempts: 50000}
	for _, v := range []struct {
		key        string
		dst        *int64
		scale, max int64
	}{
		{"E2EE_BLOB_TOTAL_GB", &c.Limits.GlobalBytes, 1_000_000_000, (1<<63 - 1) / 1_000_000_000},
		{"E2EE_BLOB_ACCOUNT_GB", &c.Limits.AccountBytes, 1_000_000_000, (1<<63 - 1) / 1_000_000_000},
		{"E2EE_BLOB_TOTAL_CLAIMS", &c.Limits.GlobalClaims, 1, 100000}, {"E2EE_BLOB_ACCOUNT_CLAIMS", &c.Limits.AccountClaims, 1, 100000},
		{"E2EE_BLOB_TOTAL_ATTEMPTS", &c.Limits.GlobalAttempts, 1, 100000}, {"E2EE_BLOB_ACCOUNT_ATTEMPTS", &c.Limits.AccountAttempts, 1, 100000},
	} {
		if raw := get(v.key); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < 1 || n > v.max {
				return c, fmt.Errorf("invalid %s: positive bounded integer required", v.key)
			}
			*v.dst = n * v.scale
		}
	}
	if !c.Limits.Valid() || c.Limits.GlobalClaims > c.Limits.GlobalAttempts || c.Limits.AccountClaims > c.Limits.AccountAttempts {
		return c, fmt.Errorf("invalid E2EE blob logical quota relationships")
	}
	artifacts, startupSeconds, verifyGB := int64(20000), int64(60), int64(100)
	for _, v := range []struct {
		key      string
		dst      *int64
		min, max int64
	}{
		{"E2EE_BLOB_MAX_ARTIFACTS", &artifacts, 2, 20000}, {"E2EE_BLOB_STARTUP_SECONDS", &startupSeconds, 1, 600}, {"E2EE_BLOB_STARTUP_VERIFY_GB", &verifyGB, 1, (1<<63 - 1) / 1_000_000_000},
	} {
		if raw := get(v.key); raw != "" {
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || n < v.min || n > v.max {
				return c, fmt.Errorf("invalid %s: bounded integer required", v.key)
			}
			*v.dst = n
		}
	}
	maxUploads := min(shared.Streams, 63) // Store's final slot belongs to cleanup.
	c.Files = blobstore.Config{MaxConcurrent: maxUploads + 1, HeadroomBytes: shared.FreeBytes, WriteTimeout: 30 * time.Second}
	c.Upload = service.E2EEBlobUploadConfig{MaxUploads: maxUploads, Timeout: 30 * time.Second, RecheckInterval: time.Second}
	c.Startup = service.E2EEBlobStartupConfig{MaxAttempts: int(c.Limits.GlobalAttempts), MaxArtifacts: int(artifacts), MaxVerifyBytes: verifyGB * 1_000_000_000, Timeout: time.Duration(startupSeconds) * time.Second}
	c.Collector = service.E2EEBlobCollectorConfig{Batch: 32, MaxRetryEntries: 1024, Interval: 2 * time.Second, StepTimeout: 5 * time.Second, RetryBase: 2 * time.Second, RetryMax: time.Minute}
	return c, c.Collector.Validate()
}
