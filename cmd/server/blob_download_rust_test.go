//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/handlers"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/repository"
	"google.golang.org/protobuf/proto"
)

// Component interop: real TLS/HTTP/native verifier, auth/PG/private file. Opaque
// descriptor/file bytes do not prove Signal/AEAD/cache/file-ACK/desktop readiness.
func TestBlobLifecycleRustDownloadHTTPPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN for native encrypted-file read fixture")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("native binary must be absolute")
	}
	f := blobLifecyclePostgres(t)
	f.write(t)
	r := f.request.Reservation
	epoch := *r.CryptoEpoch
	send := &v2.SendMessageRequest{Context: r.Context, Envelope: &v2.Envelope{ProtocolVersion: 3, CryptoSuite: 1, MessageId: r.MessageID, ConversationId: r.ConversationID, Sender: r.Sender, Recipient: r.Recipient, CryptoEpoch: &epoch, MessageType: 1, Ciphertext: []byte("opaque synthetic native download descriptor"), Blob: r.Blob}}
	delivery := repository.NewPgE2EEDeliveryRepository(f.db)
	if _, err := delivery.SendV2(t.Context(), f.uid, send); err != nil {
		t.Fatal(err)
	}
	binding, err := boundary.EpochSendBinding(send.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	if err = delivery.AcknowledgeV2(t.Context(), 2, &pb.AcknowledgeMessagesRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Recipient.GenerationId}, Messages: []*pb.Acknowledgement{{MessageId: r.MessageID, EnvelopeDigest: binding.EnvelopeDigest, Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}); err != nil {
		t.Fatal(err)
	}
	token, _, _, err := f.auth.Login(t.Context(), "bob@blob-lifecycle.invalid", "Synthetic-lifecycle-password")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	f.runtime.Register(mux)
	cert, ca := nativeBlobTLS(t)
	server := httptest.NewUnstartedServer(metrics.MetricsMiddleware(handlers.LimitRequests(mux, mux), mux))
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	server.StartTLS()
	defer server.Close()
	bindingWire, err := proto.Marshal(binding)
	if err != nil {
		t.Fatal(err)
	}
	blobWire, err := proto.Marshal(r.Blob)
	if err != nil {
		t.Fatal(err)
	}
	input, err := json.Marshal(map[string]string{"origin": server.URL, "ca": string(ca), "jwt": token, "credential": hex.EncodeToString(f.credential), "binding": base64.RawURLEncoding.EncodeToString(bindingWire), "blob": base64.RawURLEncoding.EncodeToString(blobWire)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::epoch::blob_http::download::tests::go_blob_download_worker", "--nocapture")
	command.Stdin = bytes.NewReader(input)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native download: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "1 passed; 0 failed") || !strings.Contains(string(output), "zchat-native-blob-read-complete:sha-and-scope") {
		t.Fatal("native read fixture incomplete")
	}
	joined := make(chan struct{})
	go func() { f.runtime.calls.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("native read did not release runtime ownership")
	}
	if f.ordinary.Budget().Active() {
		t.Fatal("native read leaked shared admission")
	}
	var state string
	var count int
	if err = f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, f.request.AttemptID).Scan(&state); err != nil || state != "linked" {
		t.Fatal("native read changed accepted state", err)
	}
	if err = f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_message_blobs`).Scan(&count); err != nil || count != 1 {
		t.Fatal("native read removed accepted file link", err)
	}
}
