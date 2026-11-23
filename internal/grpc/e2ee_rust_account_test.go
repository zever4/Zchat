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

// Actual native account ownership, not renderer/GUI acceptance. Production
// registry/v2 graph on a disposable schema; faults lose replies after COMMIT.
func TestE2EERustAccountOwnerPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN for actual native account runtime tests")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("Rust test binary must be an explicit absolute path")
	}
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	var ids []int64
	for _, name := range []string{"owner-alice", "owner-bob"} {
		token, err := auth.Register(t.Context(), name, name+"@owner.invalid", deviceTestPassword)
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
	var lostSend, lostACK atomic.Bool
	var retired, deadlines, inventories atomic.Int64
	var mu sync.Mutex
	sends := make(map[string][][]byte)
	var captured int
	faults := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > 0 && time.Until(deadline) <= 11*time.Second {
			deadlines.Add(1)
		}
		switch info.FullMethod {
		case pb.Messenger_GetPreKeyInventory_FullMethodName:
			inventories.Add(1)
		case pb.Messenger_AcquireBundle_FullMethodName, pb.Messenger_SendMessage_FullMethodName, pb.Messenger_GetPendingMessages_FullMethodName, pb.Messenger_AcknowledgeMessages_FullMethodName, pb.Messenger_GetMessageStatuses_FullMethodName:
			retired.Add(1)
		case v2.Messenger_SendMessage_FullMethodName:
			r := req.(*v2.SendMessageRequest)
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(r)
			if err != nil {
				return nil, err
			}
			mu.Lock()
			captured++
			if captured > 64 {
				mu.Unlock()
				return nil, status.Error(codes.ResourceExhausted, "owner fixture capture budget")
			}
			sends[r.Envelope.MessageId] = append(sends[r.Envelope.MessageId], wire)
			mu.Unlock()
		}
		value, err := handler(ctx, req)
		if err == nil && info.FullMethod == v2.Messenger_SendMessage_FullMethodName && lostSend.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "owner fixture lost Send reply after COMMIT")
		}
		if err == nil && info.FullMethod == v2.Messenger_AcknowledgeMessages_FullMethodName && lostACK.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "owner fixture lost ACK reply after COMMIT")
		}
		return value, err
	}
	cert, ca := rustInteropCertificate(t)
	server := grpcgo.NewServer(grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})), grpcgo.MaxRecvMsgSize(MaxRequestSize), grpcgo.ChainUnaryInterceptor(guard.Unary(NewUserGRPCServer(users, auth).AuthInterceptor), faults))
	RegisterOptionalEpochMessenger(server, "true", func() (*E2EEDeviceServer, *E2EEEpochServer) { return registry, epoch })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); <-done }()
	caPath := filepath.Join(t.TempDir(), "owner-ca.pem")
	if err = os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("ZCHAT_RUST_E2EE_TEST_WINDOWS") == "1" {
		converted, err := exec.Command("wslpath", "-w", caPath).Output()
		if err != nil {
			t.Fatal("convert fixture CA path:", err)
		}
		caPath = strings.TrimSpace(string(converted))
	}
	ctx, stop := context.WithTimeout(t.Context(), 120*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::account_tests::go_account_owner_worker", "--nocapture")
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), caPath, tokens[0], tokens[1], strconv.FormatInt(ids[1], 10)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-native-account-owner-complete:v2")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Fatalf("native account owner: %v\n%s", err, output)
	}
	if !lostSend.Load() || !lostACK.Load() || retired.Load() != 0 || inventories.Load() < 3 || deadlines.Load() == 0 {
		t.Fatalf("owner faults/scheduling drifted: Send=%v ACK=%v retired=%d inventory=%d deadline=%d", lostSend.Load(), lostACK.Load(), retired.Load(), inventories.Load(), deadlines.Load())
	}
	var deviceCount, epochs, proposals, operations, receipts, acknowledgements, inbox, acquisitions int
	err = db.QueryRowContext(t.Context(), `SELECT (SELECT count(*)FROM e2ee_devices),(SELECT count(*)FROM e2ee_conversation_epochs),(SELECT count(*)FROM e2ee_epoch_recoveries),(SELECT count(*)FROM e2ee_recovery_operations),(SELECT count(*)FROM e2ee_send_receipts),(SELECT count(*)FROM e2ee_message_acknowledgements),(SELECT count(*)FROM e2ee_message_inbox),(SELECT count(*)FROM e2ee_acquisitions)`).Scan(&deviceCount, &epochs, &proposals, &operations, &receipts, &acknowledgements, &inbox, &acquisitions)
	if err != nil {
		t.Fatal(err)
	}
	if deviceCount != 2 || epochs != 2 || proposals != 1 || operations != 2 || receipts != 4 || acknowledgements != 4 || inbox != 0 || acquisitions != 2 {
		t.Fatalf("owner durable facts drifted: devices=%d epochs=%d proposals=%d ops=%d receipts=%d ACK=%d inbox=%d acquisitions=%d", deviceCount, epochs, proposals, operations, receipts, acknowledgements, inbox, acquisitions)
	}
	mu.Lock()
	defer mu.Unlock()
	retries := 0
	for _, attempts := range sends {
		if len(attempts) > 1 {
			retries++
		}
		for _, attempt := range attempts[1:] {
			if !bytes.Equal(attempt, attempts[0]) {
				t.Fatal("owner retry changed committed UUID/ciphertext")
			}
		}
	}
	if len(sends) != 4 || retries < 1 {
		t.Fatal("missing original durable Send retry")
	}
	t.Logf("owned native runtime:2independent account stores, foreground signed manual recovery0->1, serial inventory/delivery, lost post-COMMIT Send/ACK exact retry;logout/reopen preserves identity/history/epoch/session;2devices/2epochs/1proposal/2ops/4receipts/4ACK/0inbox/2acquisitions;%d bounded RPCs,0retired DATA; no Tauri UI acceptance", deadlines.Load())
}
