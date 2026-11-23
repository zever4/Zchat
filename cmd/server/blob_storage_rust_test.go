//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	internalGrpc "github.com/zever4/Zchat/internal/grpc"
	"github.com/zever4/Zchat/internal/handlers"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

func nativeBlobTLS(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	now := time.Now()
	rootKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	root := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Zchat isolated blob CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	rootDER, err := x509.CreateCertificate(rand.Reader, root, root, &rootKey.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, NotBefore: root.NotBefore, NotAfter: root.NotAfter, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, leaf, root, &key.PublicKey, rootKey)
	if err != nil {
		t.Fatal(err)
	}
	private, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: private}))
	if err != nil {
		t.Fatal(err)
	}
	return cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: rootDER})
}

type blobLostReadyReply struct {
	u     handlers.E2EEBlobUploader
	calls atomic.Int32
}

func (u *blobLostReadyReply) Upload(c context.Context, token string, credential []byte, r *domain.E2EEBlobAttemptRequest, body io.ReadCloser) (*domain.E2EEBlobWriteLease, error) {
	result, err := u.u.Upload(c, token, credential, r, body)
	if err == nil && u.calls.Add(1) == 1 {
		return nil, errors.New("synthetic lost successful reply AFTER Ready COMMIT")
	}
	return result, err
}

// Actual native AEAD file ->TLS PUT ->owned Go storage/shared accounting ->PG
// Ready ->gRPC reconciliation. The native test uses synthetic route/auth +test-only
// upload constructor; protected journal/source construction is tested separately.
func TestBlobLifecycleRustHTTPPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN for native ciphertext HTTP fixture")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("native test binary must be absolute")
	}
	f := blobLifecyclePostgres(t)
	lost := &blobLostReadyReply{u: f.runtime}
	mux := http.NewServeMux()
	mux.HandleFunc(boundary.BlobUploadPattern, handlers.HandleE2EEBlobUpload(lost))
	cert, caPEM := nativeBlobTLS(t)
	httpServer := httptest.NewUnstartedServer(handlers.LimitRequests(mux, mux))
	httpServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	httpServer.StartTLS()
	defer httpServer.Close()
	guard := internalGrpc.NewE2EEAuth(t.Context(), f.auth, f.devices)
	server := grpcgo.NewServer(grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: httpServer.TLS.Certificates, MinVersion: tls.VersionTLS12})), grpcgo.ChainUnaryInterceptor(guard.Unary(nil)))
	v2.RegisterMessengerServer(server, internalGrpc.NewE2EEEpochServer(nil).WithBlobControls(f.runtime))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); <-served }()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	request := filepath.Join(dir, "request.pb")
	if err = os.WriteFile(ca, caPEM, 0600); err != nil {
		t.Fatal(err)
	}
	r := f.request.Reservation
	wire, err := proto.Marshal(&v2.BlobLeaseRequest{Context: r.Context, MessageId: r.MessageID, ConversationId: r.ConversationID, CryptoEpoch: r.CryptoEpoch, Sender: r.Sender, Recipient: r.Recipient, Blob: r.Blob, OperationId: r.OperationID})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(request, wire, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::epoch::blob_http::tests::go_blob_http_worker", "--nocapture")
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), httpServer.URL, ca, request, f.token, hex.EncodeToString(f.credential)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("native ciphertext HTTP: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "1 passed; 0 failed") || !strings.Contains(string(output), "zchat-native-blob-http-complete:encrypted-body") || lost.calls.Load() != 1 {
		t.Fatalf("native upload fixture incomplete: %s", output)
	}
	var attempt string
	var count int
	var n int64
	var hash []byte
	if err = f.db.QueryRowContext(t.Context(), `SELECT a.attempt_id::text,a.charged_bytes,c.blob_sha256 FROM e2ee_blob_attempts a JOIN e2ee_message_claims c USING(message_id) WHERE a.state='ready'`).Scan(&attempt, &n, &hash); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(f.config.Root, attempt+".blob"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	if int64(len(data)) != n || !bytes.Equal(digest[:], hash) || !bytes.HasPrefix(data, []byte{'Z', 'C', 'A', 'T', 'T', 0, 1, 1}) || bytes.Contains(data, []byte("private-native-http-file-marker")) {
		t.Fatal("server did not retain exact protected file")
	}
	if err = f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_send_receipts`).Scan(&count); err != nil || count != 0 {
		t.Fatal("upload became message acceptance", count, err)
	}
}
