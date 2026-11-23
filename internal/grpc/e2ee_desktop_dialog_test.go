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
func TestE2EERustDesktopDialogPostgres(t *testing.T) {
	runE2EERustDesktopDialogPostgres(t, false)
}

func TestE2EERustDesktopSuccessorPostgres(t *testing.T) {
	runE2EERustDesktopDialogPostgres(t, true)
}

func runE2EERustDesktopDialogPostgres(t *testing.T, successor bool) {
	t.Helper()
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
	for _, name := range []string{"dialog-alice", "dialog-bob", "dialog-carol"} {
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
	var calls, deadlines, registers, replacements, creates, bobCreates, cryptoCalls atomic.Int64
	var lost, lostSuccessor, closedBeforeSuccessor atomic.Bool
	var mu sync.Mutex
	var attempts [][]byte
	observe := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, next grpcgo.UnaryHandler) (any, error) {
		calls.Add(1)
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > 0 && time.Until(deadline) <= 11*time.Second {
			deadlines.Add(1)
		}
		switch info.FullMethod {
		case pb.Messenger_RegisterDevice_FullMethodName:
			registers.Add(1)
		case pb.Messenger_ReplaceOwnDevice_FullMethodName:
			replacements.Add(1)
		case v2.Messenger_AcquireBundle_FullMethodName, v2.Messenger_SendMessage_FullMethodName, v2.Messenger_GetConversationEpochs_FullMethodName, pb.Messenger_AcquireBundle_FullMethodName:
			cryptoCalls.Add(1)
		}
		var attempt int64
		if info.FullMethod == pb.Messenger_CreateConversation_FullMethodName {
			creates.Add(1)
			request := req.(*pb.CreateConversationRequest)
			if request.PeerUserId == ids[1] {
				attempt = bobCreates.Add(1)
				wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
				if err != nil {
					return nil, err
				}
				mu.Lock()
				if len(attempts) >= 16 {
					mu.Unlock()
					return nil, status.Error(codes.ResourceExhausted, "dialog capture budget")
				}
				attempts = append(attempts, wire)
				mu.Unlock()
			}
		}
		if successor && attempt >= 7 && attempt <= 10 {
			if attempt == 7 {
				var all, closed int
				if e := db.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE closed_at IS NOT NULL) FROM e2ee_conversations`).Scan(&all, &closed); e != nil || all != 1 || closed != 1 {
					return nil, status.Error(codes.Internal, "expected only original closed chat before explicit successor")
				}
				closedBeforeSuccessor.Store(true)
			}
			if _, e := db.ExecContext(ctx, `UPDATE user_settings SET allow_direct_messages=$1 WHERE user_id=$2`, attempt != 7, ids[1]); e != nil {
				return nil, status.Error(codes.Internal, "successor fixture privacy update")
			}
			friendState := "accepted"
			if attempt == 8 {
				friendState = "rejected"
			}
			if _, e := db.ExecContext(ctx, `UPDATE friend_requests SET status=$1 WHERE from_user_id=$2 AND to_user_id=$3`, friendState, ids[0], ids[1]); e != nil {
				return nil, status.Error(codes.Internal, "successor fixture friendship update")
			}
			if _, e := db.ExecContext(ctx, `UPDATE e2ee_devices SET public_material_ready=$1 WHERE user_id=$2 AND retired_at IS NULL`, attempt != 9, ids[1]); e != nil {
				return nil, status.Error(codes.Internal, "successor fixture publication update")
			}
		}
		value, err := next(ctx, req)
		if successor && attempt >= 7 && attempt <= 10 {
			expected := codes.PermissionDenied
			if attempt == 9 {
				expected = codes.FailedPrecondition
			}
			if attempt == 10 {
				expected = codes.OK
			}
			if status.Code(err) != expected {
				return nil, status.Error(codes.Internal, "successor prerequisite fixture drift")
			}
			if attempt == 10 {
				lostSuccessor.Store(true)
				return nil, status.Error(codes.Unavailable, "lost successor Create reply after COMMIT")
			}
		}
		// Fixture-only transitions prove all retries preserve the same local
		// request while the real repository enforces current prerequisites.
		if attempt == 1 || attempt == 2 || attempt == 3 {
			expected := codes.FailedPrecondition
			if attempt == 2 {
				expected = codes.PermissionDenied
			}
			if attempt == 3 {
				expected = codes.OK
			}
			if status.Code(err) != expected {
				return nil, status.Error(codes.Internal, "dialog prerequisite fixture drift")
			}
			allowed := attempt == 2
			if _, e := db.ExecContext(ctx, `UPDATE user_settings SET allow_direct_messages=$1 WHERE user_id=$2`, allowed, ids[1]); e != nil {
				return nil, status.Error(codes.Internal, "dialog fixture privacy update")
			}
			if attempt == 3 {
				lost.Store(true)
				return nil, status.Error(codes.Unavailable, "lost Create reply after COMMIT")
			}
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
	ctx, stop := context.WithTimeout(t.Context(), 180*time.Second)
	defer stop()
	worker := "e2ee::integration_tests::dialog_tests::go_desktop_dialog_worker"
	if successor {
		worker = "e2ee::integration_tests::dialog_tests::go_desktop_successor_worker"
	}
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", worker, "--nocapture")
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), caPath, tokens[0], tokens[1], tokens[2], deviceTestPassword, strconv.FormatInt(ids[1], 10), strconv.FormatInt(ids[2], 10)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-desktop-dialog-complete:v1")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Fatalf("native desktop session: %v\n%s", err, output)
	}
	expectedBob, expectedAll := int64(6), int64(1)
	if successor {
		expectedBob, expectedAll = 15, 2
	}
	if registers.Load() != 3 || replacements.Load() != 1 || creates.Load() != expectedBob+1 || bobCreates.Load() != expectedBob || !lost.Load() || cryptoCalls.Load() != 0 || calls.Load() != deadlines.Load() {
		t.Fatalf("dialog drift: registers=%d replace=%d create=%d bobCreate=%d crypto=%d calls=%d bounded=%d", registers.Load(), replacements.Load(), creates.Load(), bobCreates.Load(), cryptoCalls.Load(), calls.Load(), deadlines.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	if int64(len(attempts)) != expectedBob {
		t.Fatal("unexpected number of durable Create attempts")
	}
	for _, wire := range attempts[1:6] {
		if !bytes.Equal(wire, attempts[0]) {
			t.Fatal("Create retry/reopen changed the saved operation or generation")
		}
	}
	if successor {
		if !lostSuccessor.Load() || !closedBeforeSuccessor.Load() || bytes.Equal(attempts[0], attempts[6]) {
			t.Fatal("expected explicit new request only after closure and lost successor COMMIT reply")
		}
		for _, wire := range attempts[7:] {
			if !bytes.Equal(wire, attempts[6]) {
				t.Fatal("successor retry/reopen/concurrency changed the saved request")
			}
		}
	}
	var all, closed int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*),count(*) FILTER(WHERE closed_at IS NOT NULL) FROM e2ee_conversations`).Scan(&all, &closed); err != nil {
		t.Fatal(err)
	}
	if int64(all) != expectedAll || closed != 1 {
		t.Fatalf("unexpected explicit conversation count: all=%d closed=%d", all, closed)
	}
	var savedRequests int64
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_conversation_requests`).Scan(&savedRequests); err != nil || savedRequests != expectedAll {
		t.Fatal("unexpected saved creation requests", err)
	}
	successorAttempts := 0
	if successor {
		successorAttempts = 9
	}
	t.Logf("actual Windows native serde/command/admission/DPAPI + Go-race/TLS/PG PASS: unpublished keys/privacy/nonfriend/foreign lookup denied;6byte-identical original Bob Create attempts across lost COMMIT/reopen; successor=%t,%dexact successor attempts/current privacy+friends+keys/lost COMMIT/reopen/concurrent retry/page fence; bounded metadata no keys;8unacked reply budget;3Register/1Replace/%dCreate/0epoch-acquire-send,%dboundedRPC; no WebView acceptance", successor, successorAttempts, creates.Load(), deadlines.Load())
}
