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

// Actual recovery command bridge, native signatures/DPAPI and Go persistence.
func TestE2EERustDesktopRecoveryPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_DESKTOP_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_DESKTOP_TEST_BIN for Windows recovery acceptance")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("desktop test binary must be absolute")
	}
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	var ids []int64
	for _, name := range []string{"recovery-alice", "recovery-bob"} {
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
	var calls, deadlines, replacements, retired atomic.Int64
	var lostProposal atomic.Bool
	var mu sync.Mutex
	proposals := make(map[string][][]byte)
	decisions := make(map[string][][]byte)
	messages := make(map[string]uint64)
	observe := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, next grpcgo.UnaryHandler) (any, error) {
		calls.Add(1)
		if d, ok := ctx.Deadline(); ok && time.Until(d) > 0 && time.Until(d) <= 11*time.Second {
			deadlines.Add(1)
		}
		switch info.FullMethod {
		case pb.Messenger_ReplaceOwnDevice_FullMethodName:
			replacements.Add(1)
		case pb.Messenger_AcquireBundle_FullMethodName, pb.Messenger_SendMessage_FullMethodName, pb.Messenger_GetPendingMessages_FullMethodName, pb.Messenger_AcknowledgeMessages_FullMethodName, pb.Messenger_GetMessageStatuses_FullMethodName:
			retired.Add(1)
		case v2.Messenger_ProposeEpochRecovery_FullMethodName, v2.Messenger_DecideEpochRecovery_FullMethodName:
			r := req.(*v2.RecoveryCommandRequest)
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(r)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			capture := proposals
			if info.FullMethod == v2.Messenger_DecideEpochRecovery_FullMethodName {
				capture = decisions
			}
			id := r.GetCommand().GetControl().GetRequestId()
			if len(capture) >= 8 || len(capture[id]) >= 16 {
				mu.Unlock()
				return nil, status.Error(codes.ResourceExhausted, "recovery capture budget")
			}
			capture[id] = append(capture[id], wire)
			mu.Unlock()
		case v2.Messenger_SendMessage_FullMethodName:
			r := req.(*v2.SendMessageRequest)
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(r)
			if err != nil {
				return nil, err
			}
			if bytes.Contains(wire, []byte("desktop-recovery-sensitive-v1")) {
				return nil, status.Error(codes.Internal, "plaintext on wire")
			}
			mu.Lock()
			messages[r.GetEnvelope().GetMessageId()] = r.GetEnvelope().GetCryptoEpoch()
			mu.Unlock()
		}
		value, err := next(ctx, req)
		if err == nil && info.FullMethod == v2.Messenger_ProposeEpochRecovery_FullMethodName && lostProposal.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "lost proposal reply after COMMIT")
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
	caPath := filepath.Join(t.TempDir(), "desktop-recovery-ca.pem")
	if err = os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	converted, err := exec.Command("wslpath", "-w", caPath).Output()
	if err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(t.Context(), 180*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "e2ee::integration_tests::recovery_integration_tests::go_desktop_recovery_worker", "--nocapture")
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), strings.TrimSpace(string(converted)), tokens[0], tokens[1], strconv.FormatInt(ids[1], 10)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-desktop-recovery-complete:v1")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Fatalf("native recovery: %v\n%s", err, output)
	}
	if calls.Load() != deadlines.Load() || replacements.Load() != 0 || retired.Load() != 0 || !lostProposal.Load() {
		t.Fatalf("recovery drift: RPC=%d bounded=%d replacement=%d retired=%d lostProposal=%t", calls.Load(), deadlines.Load(), replacements.Load(), retired.Load(), lostProposal.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(proposals) != 3 || len(decisions) != 3 {
		t.Fatalf("expected three explicit intents/decisions; got %d/%d", len(proposals), len(decisions))
	}
	for _, capture := range []map[string][][]byte{proposals, decisions} {
		for _, attempts := range capture {
			for _, wire := range attempts[1:] {
				if !bytes.Equal(wire, attempts[0]) {
					t.Fatal("saved signed recovery changed on retry")
				}
			}
		}
	}
	first, ok := messages["00000000-0000-4000-8000-000000000001"]
	if !ok || first != 0 {
		t.Fatal("missing epoch0 user text")
	}
	second, ok := messages["00000000-0000-4000-8000-000000000002"]
	if !ok || second != 1 {
		t.Fatal("missing epoch1 user text")
	}
	var deviceCount, epochCount, active, accepted, declined, cancelled int
	if err = db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_devices),(SELECT count(*) FROM e2ee_conversation_epochs),(SELECT count(*) FROM e2ee_conversation_epochs WHERE state='active'),(SELECT count(*) FROM e2ee_epoch_recoveries WHERE state='accepted'),(SELECT count(*) FROM e2ee_epoch_recoveries WHERE state='declined'),(SELECT count(*) FROM e2ee_epoch_recoveries WHERE state='cancelled')`).Scan(&deviceCount, &epochCount, &active, &accepted, &declined, &cancelled); err != nil {
		t.Fatal(err)
	}
	if deviceCount != 2 || epochCount != 2 || active != 1 || accepted != 1 || declined != 1 || cancelled != 1 {
		t.Fatalf("durable recovery drift: devices=%d epochs=%d active=%d accepted=%d declined=%d cancelled=%d", deviceCount, epochCount, active, accepted, declined, cancelled)
	}
	t.Logf("native bridge: signed decline/cancel/accept; lost-COMMIT recovery; epoch0/1 ciphertext; retained history after reopen; %d bounded RPCs", calls.Load())
}
