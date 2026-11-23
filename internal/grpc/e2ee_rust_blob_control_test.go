package grpc

import (
	"context"
	"encoding/hex"
	"google.golang.org/protobuf/proto"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Actual native generated client +Go auth/metadata transaction boundary. Public
// synthetic BlobRef only: no claim of real file crypto/local Signal promotion.
func TestE2EERustBlobControlPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN for the native blob metadata fixture")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("native test executable must be absolute")
	}
	f, repo, r := blobRPCPostgres(t)
	lost := &blobLostReserveResponse{E2EEBlobControls: repo}
	lost.lose.Store(true)
	f.epoch.WithBlobControls(lost)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	request := filepath.Join(dir, "request.pb")
	if err := os.WriteFile(ca, f.ca, 0600); err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(request, wire, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::epoch::blob::tests::go_blob_control_worker", "--nocapture")
	// Disposable JWT/device credential via stdin only; never args/env/logs.
	command.Stdin = strings.NewReader(strings.Join([]string{f.endpoint, ca, request, f.aliceToken, hex.EncodeToString(f.aliceRaw)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native blob metadata fixture: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "1 passed; 0 failed") || !strings.Contains(string(output), "zchat-native-blob-controls-complete:metadata-only") || lost.lose.Load() {
		t.Fatalf("native fixture incomplete: %s", output)
	}
	var attempts, claims, bytes, receipts int64
	if err = f.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*)FROM e2ee_blob_attempts),(SELECT count(*)FROM e2ee_message_claims),reserved_bytes,(SELECT count(*)FROM e2ee_send_receipts) FROM e2ee_blob_global_quota`).Scan(&attempts, &claims, &bytes, &receipts); err != nil || attempts != 1 || claims != 1 || bytes != 64 || receipts != 0 {
		t.Fatal("unexpected stored metadata", attempts, claims, bytes, receipts, err)
	}
}
