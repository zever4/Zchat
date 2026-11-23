package grpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Transport-only fixture: distinguish a local tonic timer from wire statuses.
// No application server, database, crypto state or production test route.
func TestE2EERustDeadlinesTLS(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN for actual native TLS deadline tests")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("Rust test binary must be an explicit absolute path")
	}
	certificate, ca := rustInteropCertificate(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	var mu sync.Mutex
	calls := make(map[string]int)
	timersDone := make(chan struct{}, 2)
	var malformed, cancelledHandlers int
	faults := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
		if info.FullMethod != pb.Messenger_GetCapabilities_FullMethodName && info.FullMethod != v2.Messenger_GetCapabilities_FullMethodName {
			return nil, status.Error(codes.Unimplemented, "fixture method")
		}
		md, _ := metadata.FromIncomingContext(ctx)
		deadline, hasDeadline := ctx.Deadline()
		remaining := time.Until(deadline)
		credential := md.Get(E2EEDeviceCredentialMetadata)
		if !hasDeadline || remaining <= 0 || remaining > 11*time.Second ||
			strings.Join(md.Get("authorization"), "") != "Bearer synthetic.jwt.token" ||
			len(credential) != 0 { // Capabilities is JWT-only on both clients.
			mu.Lock()
			malformed++
			mu.Unlock()
			return nil, status.Error(codes.InvalidArgument, "fixture request binding")
		}
		mu.Lock()
		calls[info.FullMethod]++
		n := calls[info.FullMethod]
		mu.Unlock()
		switch n {
		case 1, 4:
			// Even exactly matching timer text cannot prove a LOCAL timer.
			return nil, status.Error(codes.Canceled, "Timeout expired")
		case 2:
			return nil, status.Error(codes.DeadlineExceeded, "Timeout expired")
		case 3:
			<-ctx.Done() // No fabricated timeout reply; actual native timer expires.
			mu.Lock()
			cancelledHandlers++
			mu.Unlock()
			timersDone <- struct{}{}
			return nil, ctx.Err()
		default:
			return nil, status.Error(codes.ResourceExhausted, "fixture request budget")
		}
	}
	server := grpcgo.NewServer(
		grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})),
		grpcgo.UnaryInterceptor(faults),
	)
	pb.RegisterMessengerServer(server, &pb.UnimplementedMessengerServer{})
	v2.RegisterMessengerServer(server, &v2.UnimplementedMessengerServer{})
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	defer func() { server.Stop(); <-serveDone }()
	caPath := filepath.Join(t.TempDir(), "deadline-ca.pem")
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("ZCHAT_RUST_E2EE_TEST_WINDOWS") == "1" {
		converted, err := exec.Command("wslpath", "-w", caPath).Output()
		if err != nil {
			t.Fatal("convert fixture CA path:", err)
		}
		caPath = strings.TrimSpace(string(converted))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::deadline_tests::go_deadline_worker", "--nocapture")
	command.Stdin = strings.NewReader("https://" + listener.Addr().String() + "\n" + caPath + "\n")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-native-deadline-complete:v2")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Fatalf("actual native TLS deadline fixture failed: %v\n%s", err, output)
	}
	for range 2 {
		select {
		case <-timersDone:
		case <-ctx.Done():
			t.Fatal("server did not observe native deadline cancellation")
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls[pb.Messenger_GetCapabilities_FullMethodName] != 4 || calls[v2.Messenger_GetCapabilities_FullMethodName] != 4 || malformed != 0 || cancelledHandlers != 2 {
		t.Fatalf("deadline assertions drifted: calls=%v malformed=%d timed handlers=%d", calls, malformed, cancelledHandlers)
	}
	t.Log("actual registry and v2 TLS channels: remote Cancelled and DeadlineExceeded stay remote; native10s timer produces LocalDeadline; both server contexts terminate and original channels remain usable")
}
