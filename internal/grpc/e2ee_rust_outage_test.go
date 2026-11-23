package grpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

// Dedicated fixture only: no application listener/container or Redis is touched.
// Restarts recreate the TLS gRPC server AND Go service/auth graph on the same DB.
// They do not claim to kill/restart an OS process or PostgreSQL itself.
type deliveryOutageFixture struct {
	db          *sql.DB
	root        context.Context
	certificate tls.Certificate
	lifecycle   sync.Mutex
	server      *grpcgo.Server
	listener    net.Listener
	serveDone   chan error
	cancel      context.CancelFunc
	address     string
	mu          sync.Mutex
	armed       *deliveryOutageFault
	last        *deliveryOutageFault
	starts      int
	sends       map[string][][]byte
	faultSends  map[string]bool
	sendCalls   int
	acks        int
	pendingDone int
	sendDone    int
	deadlines   int
}

type deliveryOutageFault struct {
	method  string
	commit  bool
	drop    bool
	started chan struct{}
	done    chan struct{}
}

func (f *deliveryOutageFixture) start() error {
	f.lifecycle.Lock()
	defer f.lifecycle.Unlock()
	if f.server != nil {
		return fmt.Errorf("fixture already running")
	}
	address := f.address
	if address == "" {
		address = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	if f.address == "" {
		f.address = listener.Addr().String()
	}
	root, cancel := context.WithCancel(f.root)
	users := repository.NewPgUserRepository(f.db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(f.db), auth)
	guard := NewE2EEAuth(root, auth, devices)
	devices.SetDeviceReplacer(guard.DisconnectGeneration)
	implementation := newPrototypeDeviceServer(devices, service.NewE2EEPreKeyService(repository.NewPgE2EEPreKeyRepository(f.db)), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(f.db)), service.NewE2EEMessageService(repository.NewPgE2EEDeliveryRepository(f.db)))
	server := grpcgo.NewServer(
		grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{f.certificate}, MinVersion: tls.VersionTLS12})),
		grpcgo.MaxRecvMsgSize(MaxRequestSize),
		grpcgo.ChainUnaryInterceptor(guard.Unary(NewUserGRPCServer(users, auth).AuthInterceptor), f.intercept),
	)
	RegisterOptionalMessenger(server, "true", func() pb.MessengerServer { return implementation })
	f.server, f.listener, f.cancel = server, listener, cancel
	f.serveDone = make(chan error, 1)
	done := f.serveDone
	f.mu.Lock()
	f.starts++
	f.mu.Unlock()
	go func() { done <- server.Serve(listener) }()
	return nil
}

func (f *deliveryOutageFixture) stop() {
	f.lifecycle.Lock()
	defer f.lifecycle.Unlock()
	if f.server == nil {
		return
	}
	f.cancel()
	f.server.Stop() // Force-close actual HTTP/2/TLS connections, including in-flight RPC.
	_ = f.listener.Close()
	<-f.serveDone
	f.server = nil
}

func (f *deliveryOutageFixture) intercept(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
	f.mu.Lock()
	if info.FullMethod == pb.Messenger_SendMessage_FullMethodName {
		f.sendCalls++
		if f.sendCalls > 64 {
			f.mu.Unlock()
			return nil, fmt.Errorf("fixture Send capture budget exhausted")
		}
		r := req.(*pb.SendMessageRequest)
		wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(r)
		if err != nil {
			f.mu.Unlock()
			return nil, err
		}
		f.sends[r.Envelope.MessageId] = append(f.sends[r.Envelope.MessageId], wire)
	}
	if info.FullMethod == pb.Messenger_AcknowledgeMessages_FullMethodName {
		f.acks++
	}
	var fault *deliveryOutageFault
	if f.armed != nil && f.armed.method == info.FullMethod {
		fault, f.armed = f.armed, nil
		if info.FullMethod == pb.Messenger_SendMessage_FullMethodName {
			f.faultSends[req.(*pb.SendMessageRequest).Envelope.MessageId] = true
		}
	}
	f.mu.Unlock()
	if fault == nil {
		return handler(ctx, req)
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 11*time.Second {
		return nil, fmt.Errorf("native RPC deadline missing at fault boundary")
	}
	f.mu.Lock()
	f.deadlines++
	f.mu.Unlock()
	if fault.commit {
		if _, err := handler(ctx, req); err != nil {
			return nil, err
		}
	}
	close(fault.started) // For Send/ACK, the real repository has already COMMITted.
	if fault.drop {
		go f.stop() // Do not wait for this handler from inside the handler itself.
	}
	<-ctx.Done()
	f.mu.Lock()
	if info.FullMethod == pb.Messenger_GetPendingMessages_FullMethodName {
		f.pendingDone++
	} else if info.FullMethod == pb.Messenger_SendMessage_FullMethodName {
		f.sendDone++
	}
	f.mu.Unlock()
	close(fault.done)
	// No fabricated Unavailable/DeadlineExceeded response: transport closure or
	// the caller's actual timeout/cancel decides what the native client observes.
	return nil, ctx.Err()
}

func (f *deliveryOutageFixture) control(ctx context.Context, phase string) error {
	var method string
	commit, drop := false, false
	switch phase {
	case "arm-send-drop":
		method, commit, drop = pb.Messenger_SendMessage_FullMethodName, true, true
	case "arm-ack-drop":
		method, commit, drop = pb.Messenger_AcknowledgeMessages_FullMethodName, true, true
	case "arm-pending-stall":
		method = pb.Messenger_GetPendingMessages_FullMethodName
	case "arm-send-stall":
		method, commit = pb.Messenger_SendMessage_FullMethodName, true
	case "restart":
		f.stop() // Wait for a racing force-stop to finish before rebinding.
		return f.start()
	case "wait-started", "wait-cancelled":
		// A dropped client future must cancel the server before its normal10s
		// deadline; merely waiting for that deadline is not cancellation proof.
		barrier, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		f.mu.Lock()
		last := f.last
		f.mu.Unlock()
		if last == nil {
			return fmt.Errorf("no armed fixture fault")
		}
		wait := last.started
		if phase == "wait-cancelled" {
			wait = last.done
		}
		select {
		case <-wait:
			return nil
		case <-barrier.Done():
			return fmt.Errorf("fixture %s barrier: %w", phase, barrier.Err())
		}
	case "done":
		return nil
	default:
		return fmt.Errorf("unknown fixture phase")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.armed != nil {
		return fmt.Errorf("previous fixture fault was not consumed")
	}
	f.armed = &deliveryOutageFault{method: method, commit: commit, drop: drop, started: make(chan struct{}), done: make(chan struct{})}
	f.last = f.armed
	return nil
}

// A bounded stdout controller, not a messenger/network implementation. Only
// constant phase names cross this channel. JWTs are written directly to stdin.
type deliveryOutageOutput struct {
	mu      sync.Mutex
	output  bytes.Buffer
	partial string
	control func(string) error
}

func (w *deliveryOutageOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.output.Len()+len(p) > 1<<20 {
		return 0, fmt.Errorf("native fixture output limit exceeded")
	}
	_, _ = w.output.Write(p)
	w.partial += string(p)
	if len(w.partial) > 65536 {
		return 0, fmt.Errorf("native fixture line limit exceeded")
	}
	for {
		line, rest, ok := strings.Cut(w.partial, "\n")
		if !ok {
			break
		}
		w.partial = rest
		if phase, ok := strings.CutPrefix(strings.TrimSuffix(line, "\r"), "zchat-outage-control:"); ok {
			if err := w.control(phase); err != nil {
				return 0, err
			}
		}
	}
	return len(p), nil
}

func TestE2EERustDeliveryOutagePostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN for actual native TLS/PG outage tests")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("native binary must have an explicit absolute path")
	}
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	var ids []int64
	for _, name := range []string{"outage-alice", "outage-bob"} {
		token, err := auth.Register(t.Context(), name, name+"@interop.invalid", deviceTestPassword)
		if err != nil {
			t.Fatal(err)
		}
		claims, err := auth.ParseToken(token)
		if err != nil {
			t.Fatal(err)
		}
		tokens, ids = append(tokens, token), append(ids, claims.UserID)
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')", ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	certificate, ca := rustInteropCertificate(t)
	fixture := &deliveryOutageFixture{db: db, root: t.Context(), certificate: certificate, sends: make(map[string][][]byte), faultSends: make(map[string]bool)}
	if err := fixture.start(); err != nil {
		t.Fatal(err)
	}
	defer fixture.stop()
	caPath := filepath.Join(t.TempDir(), "outage-ca.pem")
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
	ctx, cancel := context.WithTimeout(t.Context(), 120*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::outage_tests::go_outage_worker", "--nocapture")
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	output := &deliveryOutageOutput{control: func(phase string) error {
		if err := fixture.control(ctx, phase); err != nil {
			cancel()
			return err
		}
		_, err := io.WriteString(stdin, "ready:"+phase+"\n")
		return err
	}}
	command.Stdout, command.Stderr = output, output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	config := strings.Join([]string{"https://" + fixture.address, caPath, tokens[0], tokens[1], strconv.FormatInt(ids[1], 10)}, "\n") + "\n"
	if _, err := io.WriteString(stdin, config); err != nil {
		cancel()
		_ = command.Wait()
		t.Fatal("write native fixture configuration:", err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("actual native outage fixture failed: %v\n%s", err, output.output.String())
	}
	if !strings.Contains(output.output.String(), "zchat-native-outage-complete:v1") || !strings.Contains(output.output.String(), "1 passed; 0 failed") {
		t.Fatalf("native outage worker did not finish: %s", output.output.String())
	}
	// The transcript contains only static phase names and redacted error codes;
	// initial JWT configuration is stdin-only and is never echoed by the worker.
	t.Logf("native outage phase transcript:\n%s", output.output.String())
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.starts != 3 || fixture.acks != 5 || fixture.pendingDone != 3 || fixture.sendDone != 2 || fixture.deadlines != 6 || len(fixture.sends) != 4 {
		t.Fatalf("outage barriers drifted: starts=%d ACKs=%d cancelled pending=%d cancelled sends=%d deadlines=%d messageIDs=%d", fixture.starts, fixture.acks, fixture.pendingDone, fixture.sendDone, fixture.deadlines, len(fixture.sends))
	}
	total, retried := 0, 0
	for _, attempts := range fixture.sends {
		total += len(attempts)
		if len(attempts) >= 2 {
			retried++
		}
		for _, wire := range attempts[1:] {
			if !bytes.Equal(wire, attempts[0]) {
				t.Fatal("retry changed persisted request/UUID/ciphertext")
			}
		}
	}
	// drive_until may cancel the next in-flight Send after observing its target
	// receipt. That legitimately adds an exact retry, even without another fault.
	// Require recovery of BOTH deliberately faulted IDs, not a timing-specific
	// total. All attempts above must remain byte-identical; capture is capped64.
	if len(fixture.faultSends) != 2 {
		t.Fatalf("expected two distinct faulted Send IDs: got %d", len(fixture.faultSends))
	}
	for id := range fixture.faultSends {
		if len(fixture.sends[id]) < 2 {
			t.Fatal("a deliberately lost/cancelled Send was not retried exactly")
		}
	}
	var devices, retired, conversations, acquisitions, receipts, inbox, stored, removals, pending, usedEC, usedPQ int
	if err := db.QueryRowContext(t.Context(), `SELECT
        (SELECT count(*) FROM e2ee_devices),(SELECT count(*) FROM e2ee_devices WHERE retired_at IS NOT NULL),
        (SELECT count(*) FROM e2ee_conversations),(SELECT count(*) FROM e2ee_acquisitions),
        (SELECT count(*) FROM e2ee_send_receipts),(SELECT count(*) FROM e2ee_message_inbox),
        (SELECT count(*) FROM e2ee_message_acknowledgements WHERE disposition='stored'),(SELECT count(*) FROM e2ee_message_removals),
        (SELECT count(*) FROM e2ee_delivery_state WHERE pending_messages<>0 OR pending_bytes<>0),
        (SELECT count(*) FROM e2ee_ec_prekeys WHERE used),(SELECT count(*) FROM e2ee_kyber_prekeys WHERE used)`).Scan(&devices, &retired, &conversations, &acquisitions, &receipts, &inbox, &stored, &removals, &pending, &usedEC, &usedPQ); err != nil {
		t.Fatal(err)
	}
	if devices != 2 || retired != 0 || conversations != 1 || acquisitions != 1 || receipts != 4 || inbox != 0 || stored != 4 || removals != 4 || pending != 0 || usedEC != 1 || usedPQ != 1 {
		t.Fatalf("outage recovery DB drift: devices=%d retired=%d chats=%d acquisitions=%d receipts=%d inbox=%d stored=%d removals=%d pending=%d EC=%d PQ=%d", devices, retired, conversations, acquisitions, receipts, inbox, stored, removals, pending, usedEC, usedPQ)
	}
	t.Logf("actual TLS server/service graph started3times on same endpoint; post-COMMIT Send+ACK replies lost by forced connection close; original native channels reconnect; worker restart recovers exact outbox/ACK; pending caller cancellation/default10sdeadline/runner round timeout and post-COMMIT Send cancellation reach server context;%dSendRPC/4exact ciphertext IDs/%didentical retries/%dretried IDs/2faulted Send IDs/5ACKRPC/4Stored/4removals/0inbox/0pending;2devices/0retired/1chat/1acquisition/usedEC1/PQ1; no resets/new bootstrap; no app/PG container restart or Redis dependency", total, total-len(fixture.sends), retried)
}
