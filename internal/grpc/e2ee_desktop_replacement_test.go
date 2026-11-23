package grpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Runs the real desktop session manager without a WebView. The selected binary
// must be the Windows experimental-e2ee test binary, using actual Windows DPAPI.
func TestE2EERustDesktopReplacementPostgres(t *testing.T) {
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
	var ids []int64
	for _, name := range []string{"desktop-alice", "desktop-bob"} {
		token, err := auth.Register(t.Context(), name, name+"@desktop.invalid", deviceTestPassword)
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
		claims, err := auth.ParseToken(token)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, claims.UserID)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status)VALUES($1,$2,'accepted')`, ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	guard := NewE2EEAuth(root, auth, devices)
	devices.SetDeviceReplacer(guard.DisconnectGeneration)
	keys := repository.NewPgE2EEPreKeyRepository(db)
	registry := NewE2EEDeviceServer(devices, service.NewE2EEPreKeyService(keys), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(db)), nil)
	epoch := NewE2EEEpochServer(service.NewE2EEEpochService(repository.NewPgE2EEDeliveryRepository(db), keys, repository.NewPgE2EERecoveryRepository(db)))
	var calls, deadlines, registers, replacements, sends atomic.Int64
	var lost atomic.Bool
	var mu sync.Mutex
	operations := make(map[string][][]byte)
	observe := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, next grpcgo.UnaryHandler) (any, error) {
		calls.Add(1)
		if info.FullMethod == v2.Messenger_SendMessage_FullMethodName {
			sends.Add(1)
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > 0 && time.Until(deadline) <= 11*time.Second {
			deadlines.Add(1)
		}
		if info.FullMethod == pb.Messenger_RegisterDevice_FullMethodName {
			registers.Add(1)
		}
		if info.FullMethod == pb.Messenger_ReplaceOwnDevice_FullMethodName {
			replacements.Add(1)
			r := req.(*pb.ReplaceOwnDeviceRequest)
			// Do NOT clone/marshal/log the original request: it contains a password.
			public := &pb.ReplaceOwnDeviceRequest{ProtocolVersion: r.ProtocolVersion, ReplacementRequestId: r.ReplacementRequestId, ExpectedGenerationId: r.ExpectedGenerationId, RegistrationId: r.RegistrationId, IdentityKey: r.IdentityKey, CredentialHash: r.CredentialHash}
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(public)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			if replacements.Load() > 8 {
				mu.Unlock()
				return nil, status.Error(codes.ResourceExhausted, "replacement fixture budget")
			}
			operations[r.ReplacementRequestId] = append(operations[r.ReplacementRequestId], wire)
			mu.Unlock()
		}
		value, err := next(ctx, req)
		if err == nil && info.FullMethod == pb.Messenger_ReplaceOwnDevice_FullMethodName && lost.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "lost Replace reply after COMMIT")
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
	ctx, stop := context.WithTimeout(t.Context(), 150*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "e2ee::integration_tests::replacement_tests::go_desktop_replacement_worker", "--nocapture")
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), caPath, tokens[0], tokens[1], deviceTestPassword, strconv.FormatInt(ids[1], 10)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-desktop-replacement-complete:v1")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Fatalf("native desktop session: %v\n%s", err, output)
	}
	if sends.Load() != 0 {
		t.Fatalf("replacement replayed old-generation outbox: %d Send calls", sends.Load())
	}
	if registers.Load() != 2 || replacements.Load() != 6 || !lost.Load() || calls.Load() != deadlines.Load() {
		t.Fatalf("replacement drift: register=%d replace=%d RPC=%d bounded=%d", registers.Load(), replacements.Load(), calls.Load(), deadlines.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(operations) != 4 {
		t.Fatalf("expected4distinct replacement operations, got%d", len(operations))
	}
	var replayed int
	for _, attempts := range operations {
		for _, wire := range attempts[1:] {
			if !bytes.Equal(wire, attempts[0]) {
				t.Fatal("saved replacement changed CAS/request/material/credential")
			}
			replayed++
		}
	}
	if replayed != 2 {
		t.Fatalf("wrong-password + lost-response replay expected, got%d", replayed)
	}
	var closed int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_conversations WHERE closed_at IS NOT NULL`).Scan(&closed); err != nil {
		t.Fatal(err)
	}
	if closed != 1 {
		t.Fatalf("original encrypted conversation must close after replacement: %d", closed)
	}
	t.Logf("Windows native replacement/DPAPI + Go race/TLS/JWT/PG: wrong password distinct; identical pending replay after lost COMMIT; no silent CAS rebase; explicit refreshed confirmation; real libsignal v2 history retained and decryptable at rest across local generation replacement; archived-device marker retained; 2Register/6Replace(4distinct), %d boundedRPC; no WebView/archive-viewer acceptance", deadlines.Load())
}
