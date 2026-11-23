package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	redislib "github.com/redis/go-redis/v9"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/e2ee/wakeup"
	"github.com/zever4/Zchat/internal/redis"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

type inboxHintCapture struct {
	grpcgo.ServerStream
	frames *atomic.Int64
}

func (s *inboxHintCapture) SendMsg(message any) error {
	hint, ok := message.(*pb.InboxHint)
	if !ok || hint.ProtocolVersion != 1 || proto.Size(hint) > 2 {
		return fmt.Errorf("unexpected hint payload")
	}
	if err := s.ServerStream.SendMsg(message); err != nil {
		return err
	}
	s.frames.Add(1)
	return nil
}

// Explicitly owned disposable Redis only. No application container is stopped.
func TestE2EERustWakeupsPostgres(t *testing.T) {
	binary, addr, container := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN"), os.Getenv("ZCHAT_TEST_REDIS_ADDR"), os.Getenv("ZCHAT_TEST_REDIS_CONTAINER")
	if binary == "" || addr == "" || container == "" {
		t.Skip("set native test binary and task-owned disposable Redis address/container")
	}
	if !filepath.IsAbs(binary) || len(container) != 64 || strings.Trim(container, "0123456789abcdef") != "" {
		t.Fatal("invalid explicit fixture selection")
	}
	label, err := exec.Command("docker", "inspect", "--format", `{{index .Config.Labels "zchat.task"}}`, container).Output()
	if err != nil || strings.TrimSpace(string(label)) != "e2ee-wakeups" {
		t.Fatal("refusing to stop an unowned Redis")
	}
	// Restore it even if the worker fails while offline; outer harness owns removal.
	t.Cleanup(func() {
		if err := exec.Command("docker", "start", container).Run(); err != nil {
			t.Error("restore owned Redis", err)
		}
	})
	admin := redislib.NewClient(&redislib.Options{Addr: addr, DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second, ContextTimeoutEnabled: true})
	defer admin.Close()
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	var ids []int64
	for _, name := range []string{"wake-alice", "wake-bob"} {
		token, err := auth.Register(t.Context(), name, name+"@interop.invalid", deviceTestPassword)
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
	if _, err := db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')`, ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	certificate, ca := rustInteropCertificate(t)
	var endpoints []string
	var hints, pending, watches atomic.Int64
	var guards []*E2EEAuth
	for range 2 {
		devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
		guard := NewE2EEAuth(root, auth, devices)
		guard.watchLifetime = 1500 * time.Millisecond // Accelerate the actual bounded stream rotation.
		guards = append(guards, guard)
		hub := wakeup.New(redis.NewClient(addr))
		hubDone := make(chan struct{})
		hubCtx, hubCancel := context.WithCancel(root)
		go func() { defer close(hubDone); hub.Run(hubCtx) }()
		t.Cleanup(func() {
			hubCancel()
			select {
			case <-hubDone:
			case <-time.After(4 * time.Second):
				t.Error("hint hub leaked")
			}
		})
		implementation := newPrototypeDeviceServer(devices, service.NewE2EEPreKeyService(repository.NewPgE2EEPreKeyRepository(db)), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(db)), service.NewE2EEMessageService(repository.NewPgE2EEDeliveryRepository(db))).WithWakeups(hub)
		server := grpcgo.NewServer(grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})), grpcgo.MaxRecvMsgSize(MaxRequestSize),
			grpcgo.ChainUnaryInterceptor(guard.Unary(NewUserGRPCServer(users, auth).AuthInterceptor), func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
				result, err := handler(ctx, req)
				if err == nil && info.FullMethod == pb.Messenger_GetPendingMessages_FullMethodName {
					pending.Add(1)
				}
				return result, err
			}),
			grpcgo.ChainStreamInterceptor(guard.Stream(), func(srv any, ss grpcgo.ServerStream, info *grpcgo.StreamServerInfo, handler grpcgo.StreamHandler) error {
				watches.Add(1)
				return handler(srv, &inboxHintCapture{ss, &hints})
			}))
		RegisterOptionalMessenger(server, "true", func() pb.MessengerServer { return implementation })
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		t.Cleanup(func() { server.Stop(); listener.Close(); <-done })
		endpoints = append(endpoints, "https://"+listener.Addr().String())
	}
	caPath := filepath.Join(t.TempDir(), "wake-ca.pem")
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("ZCHAT_RUST_E2EE_TEST_WINDOWS") == "1" {
		converted, err := exec.Command("wslpath", "-w", caPath).Output()
		if err != nil {
			t.Fatal(err)
		}
		caPath = strings.TrimSpace(string(converted))
	}
	ctx, stop := context.WithTimeout(t.Context(), 85*time.Second)
	defer stop()
	wait := func(condition func() bool) error {
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		tick := time.NewTicker(25 * time.Millisecond)
		defer tick.Stop()
		for {
			if condition() {
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return fmt.Errorf("phase barrier timeout")
			case <-tick.C:
			}
		}
	}
	subscriptionsReady := func() bool {
		bounded, done := context.WithTimeout(ctx, time.Second)
		defer done()
		n, err := admin.PubSubNumSub(bounded, "zchat:e2ee:inbox-hint:v1").Result()
		return err == nil && n["zchat:e2ee:inbox-hint:v1"] == 2
	}
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::wakeup_tests::go_wakeup_worker", "--nocapture")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	output := &deliveryOutageOutput{control: func(phase string) error {
		var err error
		switch phase {
		case "wake-ready":
			err = wait(func() bool { return hints.Load() >= 1 && pending.Load() >= 1 && subscriptionsReady() })
		case "redis-stop":
			err = exec.CommandContext(ctx, "docker", "stop", "--time", "0", container).Run()
		case "redis-start":
			err = exec.CommandContext(ctx, "docker", "start", container).Run()
			if err == nil {
				err = wait(subscriptionsReady)
			}
		case "watch-reconnected":
			err = wait(func() bool { return watches.Load() >= 2 })
		default:
			err = fmt.Errorf("unknown native wake phase")
		}
		if err != nil {
			stop()
			return err
		}
		_, err = io.WriteString(stdin, "ready:"+phase+"\n")
		return err
	}}
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(stdin, strings.Join([]string{endpoints[0], endpoints[1], caPath, tokens[0], tokens[1], strconv.FormatInt(ids[1], 10)}, "\n")+"\n"); err != nil {
		stop()
		_ = command.Wait()
		t.Fatal("native configuration", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("native wake worker: %v\n%s", err, output.output.String())
	}
	if !strings.Contains(output.output.String(), "zchat-native-wakeup-complete:v1") || !strings.Contains(output.output.String(), "1 passed; 0 failed") {
		t.Fatal("native worker completion missing")
	}
	if hints.Load() < 2 || watches.Load() < 2 {
		t.Fatal("actual notification frames missing")
	}
	var receipts, inbox, stored, dirty int
	if err := db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_send_receipts),(SELECT count(*) FROM e2ee_message_inbox),(SELECT count(*) FROM e2ee_message_acknowledgements WHERE disposition='stored'),(SELECT count(*) FROM e2ee_delivery_state WHERE pending_messages<>0 OR pending_bytes<>0)`).Scan(&receipts, &inbox, &stored, &dirty); err != nil {
		t.Fatal(err)
	}
	if receipts != 4 || inbox != 0 || stored != 4 || dirty != 0 {
		t.Fatalf("wake recovery drift receipts=%d inbox=%d stored=%d pending=%d", receipts, inbox, stored, dirty)
	}
	if err := wait(func() bool {
		for _, guard := range guards {
			guard.mu.Lock()
			n := len(guard.streams)
			guard.mu.Unlock()
			if n != 0 {
				return false
			}
		}
		return true
	}); err != nil {
		t.Fatal("stream lease leak", err)
	}
	t.Logf("actual two TLS endpoints/native hints/Redis stop+restart/polling during outage/8exact retries/stream rotation+reconnect: frames=%d watches=%d pendingRPC=%d;4receipts/4Stored/0inbox/0pending\n%s", hints.Load(), watches.Load(), pending.Load(), output.output.String())
}
