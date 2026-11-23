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

// Actual native text command implementation + Windows DPAPI; no WebView claims.
func TestE2EERustDesktopTextPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_DESKTOP_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_DESKTOP_TEST_BIN for Windows desktop text acceptance")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("desktop test binary must be absolute")
	}
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	var ids []int64
	for _, name := range []string{"text-alice", "text-bob"} {
		token, err := auth.Register(t.Context(), name, name+"@desktop.invalid", deviceTestPassword)
		if err != nil {
			t.Fatal(err)
		}
		claims, err := auth.ParseToken(token)
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
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
	var calls, deadlines, registers, replacements, acquisitions, retired atomic.Int64
	var lostAcquire, lostSend atomic.Bool
	var mu sync.Mutex
	var bundles [][]byte
	sends := make(map[string][][]byte)
	observe := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, next grpcgo.UnaryHandler) (any, error) {
		calls.Add(1)
		if d, ok := ctx.Deadline(); ok && time.Until(d) > 0 && time.Until(d) <= 11*time.Second {
			deadlines.Add(1)
		}
		switch info.FullMethod {
		case pb.Messenger_RegisterDevice_FullMethodName:
			registers.Add(1)
		case pb.Messenger_ReplaceOwnDevice_FullMethodName:
			replacements.Add(1)
		case pb.Messenger_AcquireBundle_FullMethodName, pb.Messenger_SendMessage_FullMethodName,
			pb.Messenger_GetPendingMessages_FullMethodName, pb.Messenger_AcknowledgeMessages_FullMethodName,
			pb.Messenger_GetMessageStatuses_FullMethodName:
			retired.Add(1)
		case v2.Messenger_AcquireBundle_FullMethodName:
			acquisitions.Add(1)
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(req.(*v2.AcquireBundleRequest))
			if err != nil {
				return nil, err
			}
			mu.Lock()
			bundles = append(bundles, wire)
			mu.Unlock()
		case v2.Messenger_SendMessage_FullMethodName:
			r := req.(*v2.SendMessageRequest)
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(r)
			if err != nil {
				return nil, err
			}
			if bytes.Contains(wire, []byte("desktop-text-sensitive-v1")) {
				return nil, status.Error(codes.Internal, "plaintext reached the wire")
			}
			mu.Lock()
			if len(sends) >= 32 || len(sends[r.GetEnvelope().GetMessageId()]) >= 16 {
				mu.Unlock()
				return nil, status.Error(codes.ResourceExhausted, "text capture budget")
			}
			sends[r.GetEnvelope().GetMessageId()] = append(sends[r.GetEnvelope().GetMessageId()], wire)
			mu.Unlock()
		}
		value, err := next(ctx, req)
		if err == nil && info.FullMethod == v2.Messenger_AcquireBundle_FullMethodName && lostAcquire.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "lost Acquire reply after COMMIT")
		}
		if err == nil && info.FullMethod == v2.Messenger_SendMessage_FullMethodName && req.(*v2.SendMessageRequest).GetEnvelope().GetMessageId() == "00000000-0000-4000-8000-000000000001" && lostSend.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "lost Send reply after COMMIT")
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
	caPath := filepath.Join(t.TempDir(), "desktop-text-ca.pem")
	if err = os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	converted, err := exec.Command("wslpath", "-w", caPath).Output()
	if err != nil {
		t.Fatal("convert native fixture CA path:", err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 180*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "e2ee::integration_tests::text_tests::go_desktop_text_worker", "--nocapture")
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), strings.TrimSpace(string(converted)), tokens[0], tokens[1], strconv.FormatInt(ids[1], 10)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-desktop-text-complete:v1")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Fatalf("native desktop text: %v\n%s", err, output)
	}
	if calls.Load() != deadlines.Load() || registers.Load() != 2 || replacements.Load() != 0 || acquisitions.Load() != 2 || retired.Load() != 0 || !lostAcquire.Load() || !lostSend.Load() {
		t.Fatalf("text drift: RPC=%d bounded=%d register=%d replace=%d acquire=%d retired=%d lostAcquire=%t lostSend=%t", calls.Load(), deadlines.Load(), registers.Load(), replacements.Load(), acquisitions.Load(), retired.Load(), lostAcquire.Load(), lostSend.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bundles) != 2 || !bytes.Equal(bundles[0], bundles[1]) {
		t.Fatal("lost Acquire reply changed the saved acquisition nonce/binding")
	}
	for _, id := range []string{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"} {
		if len(sends[id]) == 0 {
			t.Fatal("user text did not reach real Send handler")
		}
	}
	if len(sends["00000000-0000-4000-8000-000000000001"]) < 2 {
		t.Fatal("missing actual retry of Send after the lost post-COMMIT reply")
	}
	for _, attempts := range sends {
		for _, wire := range attempts[1:] {
			if !bytes.Equal(wire, attempts[0]) {
				t.Fatal("Send retry changed UUID/epoch/ciphertext")
			}
		}
	}
	var deviceCount, epochs, acquisitionCount, receipts, acknowledgements, inbox, proposals int
	if err = db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_devices),(SELECT count(*) FROM e2ee_conversation_epochs),(SELECT count(*) FROM e2ee_acquisitions),(SELECT count(*) FROM e2ee_send_receipts),(SELECT count(*) FROM e2ee_message_acknowledgements),(SELECT count(*) FROM e2ee_message_inbox),(SELECT count(*) FROM e2ee_epoch_recoveries)`).Scan(&deviceCount, &epochs, &acquisitionCount, &receipts, &acknowledgements, &inbox, &proposals); err != nil {
		t.Fatal(err)
	}
	if deviceCount != 2 || epochs != 1 || acquisitionCount != 1 || receipts != 13 || acknowledgements != 13 || inbox != 0 || proposals != 0 {
		t.Fatalf("text durable facts drifted: devices=%d epochs=%d acquisitions=%d receipts=%d ACK=%d inbox=%d proposals=%d", deviceCount, epochs, acquisitionCount, receipts, acknowledgements, inbox, proposals)
	}
	t.Logf("actual Windows native serde/text command/admission/DPAPI+Go-race/TLS/PG PASS: separate accounts/shared app root; explicit initial0; lost Acquire and Send COMMIT; exact byte retries, protected receive/reply, conflict rejection, page fence/8unacked budget, logout/reopen;2Register/0Replace/2exactAcquire/%dboundedRPC; no WebView acceptance", deadlines.Load())
}
