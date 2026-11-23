package main

import (
	"strings"
	"testing"

	"github.com/zever4/Zchat/internal/storagebudget"
)

func blobConfigValues() map[string]string {
	return map[string]string{"ENABLE_EXPERIMENTAL_E2EE": "true", "ENABLE_EXPERIMENTAL_E2EE_BLOBS": "true", "E2EE_BLOB_ROOT": "/private/blobs"}
}
func testBlobConfig(t *testing.T, values map[string]string) (blobStorageConfig, error) {
	t.Helper()
	shared, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	return blobConfigFromEnv(func(k string) string { return values[k] }, shared)
}
func TestBlobConfigDefaultOffAndExplicitPrerequisites(t *testing.T) {
	if c, err := testBlobConfig(t, nil); err != nil || c.Enabled {
		t.Fatal(c, err)
	}
	if c, err := testBlobConfig(t, map[string]string{"E2EE_BLOB_ROOT": "missing", "E2EE_BLOB_TOTAL_GB": "bad"}); err != nil || c.Enabled {
		t.Fatal("off mode touched private settings", c, err)
	}
	c, err := testBlobConfig(t, blobConfigValues())
	if err != nil {
		t.Fatal(err)
	}
	if !c.Enabled || c.Limits.GlobalBytes != 50_000_000_000 || c.Startup.MaxAttempts != 100000 || c.Startup.MaxArtifacts != 20000 || c.Files.MaxConcurrent != c.Upload.MaxUploads+1 {
		t.Fatal(c)
	}
	for key, value := range map[string]string{"ENABLE_EXPERIMENTAL_E2EE_BLOBS": "TRUE", "E2EE_BLOB_ROOT": "relative"} {
		v := blobConfigValues()
		v[key] = value
		if _, err := testBlobConfig(t, v); err == nil {
			t.Fatal(key, "accepted invalid opt-in")
		}
	}
	v := blobConfigValues()
	v["ENABLE_EXPERIMENTAL_E2EE"] = "false"
	if c, err := testBlobConfig(t, v); err != nil || !c.Enabled || c.Expose {
		t.Fatal("maintenance must survive API disable", c, err)
	}
}
func TestBlobConfigRejectsUnboundedOrInconsistentLimits(t *testing.T) {
	for key, value := range map[string]string{
		"E2EE_BLOB_TOTAL_GB": "0", "E2EE_BLOB_ACCOUNT_GB": "51", "E2EE_BLOB_TOTAL_CLAIMS": "100001", "E2EE_BLOB_ACCOUNT_CLAIMS": "50001",
		"E2EE_BLOB_TOTAL_ATTEMPTS": "100001", "E2EE_BLOB_ACCOUNT_ATTEMPTS": "100001", "E2EE_BLOB_MAX_ARTIFACTS": "20001", "E2EE_BLOB_STARTUP_SECONDS": "601", "E2EE_BLOB_STARTUP_VERIFY_GB": "9223372036854775807",
	} {
		t.Run(key, func(t *testing.T) {
			v := blobConfigValues()
			v[key] = value
			if _, err := testBlobConfig(t, v); err == nil {
				t.Fatal("accepted invalid bound")
			}
		})
	}
	v := blobConfigValues()
	v["E2EE_BLOB_TOTAL_GB"] = "synthetic-secret-invalid-number"
	if _, err := testBlobConfig(t, v); err == nil || strings.Contains(err.Error(), v["E2EE_BLOB_TOTAL_GB"]) {
		t.Fatal("invalid value leaked into error", err)
	}
}
func TestBlobLogicalQuotaDoesNotChangeWhenPhysicalCapIsLowered(t *testing.T) {
	v := blobConfigValues()
	get := func(k string) string { return v[k] }
	shared, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	first, err := blobConfigFromEnv(get, shared)
	if err != nil {
		t.Fatal(err)
	}
	shared.TotalBytes, shared.AccountBytes = 5_000_000_000, 1_000_000_000
	second, err := blobConfigFromEnv(get, shared)
	if err != nil || first.Limits != second.Limits {
		t.Fatal("physical config rewrote persisted logical cap", second, err)
	}
	shared.Streams = 1024
	third, err := blobConfigFromEnv(get, shared)
	if err != nil || third.Files.MaxConcurrent != 64 || third.Upload.MaxUploads != 63 {
		t.Fatal(third, err)
	}
}
