package grpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"google.golang.org/protobuf/proto"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// Runs the real desktop session manager without a WebView. The selected binary
// must be the Windows experimental-e2ee test binary, using actual Windows DPAPI.
func TestE2EERustDesktopSessionPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_DESKTOP_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_DESKTOP_TEST_BIN for Windows desktop lifecycle acceptance")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("desktop test binary must be an absolute native path")
	}
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	for _, name := range []string{"desktop-alice", "desktop-bob"} {
		token, err := auth.Register(t.Context(), name, name+"@desktop.invalid", deviceTestPassword)
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
	}
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	guard := NewE2EEAuth(root, auth, devices)
	devices.SetDeviceReplacer(guard.DisconnectGeneration)
	keys := repository.NewPgE2EEPreKeyRepository(db)
	registry := NewE2EEDeviceServer(devices, service.NewE2EEPreKeyService(keys), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(db)), nil)
	epoch := NewE2EEEpochServer(service.NewE2EEEpochService(repository.NewPgE2EEDeliveryRepository(db), keys, repository.NewPgE2EERecoveryRepository(db)))
	var calls, deadlines, registers, replacements, inventories atomic.Int64
	var injected, lostRegister atomic.Bool
	var registrationsMu sync.Mutex
	registrations := make(map[string][][]byte)
	var captured int
	observe := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, next grpcgo.UnaryHandler) (any, error) {
		calls.Add(1)
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > 0 && time.Until(deadline) <= 11*time.Second {
			deadlines.Add(1)
		}
		switch info.FullMethod {
		case pb.Messenger_RegisterDevice_FullMethodName:
			registers.Add(1)
			registration := req.(*pb.RegisterDeviceRequest)
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(registration)
			if err != nil {
				return nil, err
			}
			registrationsMu.Lock()
			captured++
			if captured > 8 {
				registrationsMu.Unlock()
				return nil, status.Error(codes.ResourceExhausted, "desktop fixture capture budget")
			}
			registrations[registration.RegistrationRequestId] = append(registrations[registration.RegistrationRequestId], wire)
			registrationsMu.Unlock()
		case pb.Messenger_ReplaceOwnDevice_FullMethodName:
			replacements.Add(1)
		case pb.Messenger_GetPreKeyInventory_FullMethodName:
			if inventories.Add(1) == 4 {
				injected.Store(true)
				return nil, status.Error(codes.Unauthenticated, "desktop fixture session rejection")
			}
		}
		value, err := next(ctx, req)
		if err == nil && info.FullMethod == pb.Messenger_RegisterDevice_FullMethodName && lostRegister.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "desktop fixture lost Register reply after COMMIT")
		}
		return value, err
	}
	cert, ca := rustInteropCertificate(t)
	server := grpcgo.NewServer(grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})), grpcgo.MaxRecvMsgSize(MaxRequestSize), grpcgo.ChainUnaryInterceptor(observe, guard.Unary(NewUserGRPCServer(users, auth).AuthInterceptor)))
	RegisterOptionalEpochMessenger(server, "true", func() (*E2EEDeviceServer, *E2EEEpochServer) { return registry, epoch })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); <-done }()
	caPath := filepath.Join(t.TempDir(), "desktop-ca.pem")
	if err = os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	converted, err := exec.Command("wslpath", "-w", caPath).Output()
	if err != nil {
		t.Fatal("convert native fixture CA path:", err)
	}
	caPath = strings.TrimSpace(string(converted))
	ctx, stop := context.WithTimeout(t.Context(), 120*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "e2ee::integration_tests::go_desktop_session_worker", "--nocapture")
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), caPath, tokens[0], tokens[1]}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-desktop-session-complete:v1")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Fatalf("native desktop session: %v\n%s", err, output)
	}
	var deviceCount int
	if err = db.QueryRowContext(t.Context(), `SELECT count(*)FROM e2ee_devices`).Scan(&deviceCount); err != nil {
		t.Fatal(err)
	}
	if deviceCount != 2 || registers.Load() != 3 || replacements.Load() != 0 || !injected.Load() || !lostRegister.Load() || inventories.Load() < 6 || calls.Load() != deadlines.Load() {
		t.Fatalf("desktop lifecycle drift: devices=%d registers=%d replace=%d inventory=%d RPC=%d bounded=%d", deviceCount, registers.Load(), replacements.Load(), inventories.Load(), calls.Load(), deadlines.Load())
	}
	registrationsMu.Lock()
	defer registrationsMu.Unlock()
	var retries int
	for _, attempts := range registrations {
		for _, wire := range attempts[1:] {
			if !bytes.Equal(wire, attempts[0]) {
				t.Fatal("setup retry changed committed registration/identity/credential hash")
			}
			retries++
		}
	}
	if len(registrations) != 2 || retries != 1 {
		t.Fatal("expected two exact registrations and one lost-response replay")
	}
	t.Logf("actual Windows managed explicit setup/TLS/JWT/PG/DPAPI: login creates no keys/store; lost post-COMMIT Register reply exact retry; foreign installation blocked/no Replace; isolated account DBs, stale page retirement, logout/reopen, forced auth rejection and safe same-generation retry;2devices/3RegisterRPCs(2distinct exact operations)/0Replace,%dInventory,%d bounded RPC; no WebView acceptance", inventories.Load(), deadlines.Load())
}
