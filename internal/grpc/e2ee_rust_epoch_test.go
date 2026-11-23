package grpc

import (
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Real production v2/registry graph +JWT/device auth+TLS+PostgreSQL and the
// actual Rust protected crypto and production v2 delivery/recovery driver.
// Driver recovery includes lost control replies, worker reopen and forced TLS
// connection drops after Send/ACK COMMIT. Desktop consent UI is not covered.
func TestE2EERustEpochTransportPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN to the native Rust e2ee unit-test binary")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("Rust test binary must be an explicit absolute path")
	}
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	var ids []int64
	for _, name := range []string{"epochalice", "epochbob"} {
		token, err := auth.Register(t.Context(), name, name+"@epoch-interop.invalid", deviceTestPassword)
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
	devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	guard := NewE2EEAuth(root, auth, devices)
	devices.SetDeviceReplacer(guard.DisconnectGeneration)
	keys := repository.NewPgE2EEPreKeyRepository(db)
	registry := NewE2EEDeviceServer(devices, service.NewE2EEPreKeyService(keys), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(db)), nil)
	epoch := NewE2EEEpochServer(service.NewE2EEEpochService(repository.NewPgE2EEDeliveryRepository(db), keys, repository.NewPgE2EERecoveryRepository(db)))
	var lostSend, lostPropose, lostAccept, lostACK, badCatalog, badBundle, badGet, badFence, badStatus, badPending atomic.Bool
	var expiredAcquire, badExpiry atomic.Bool
	var runnerPhase, runnerOldDeferred, runnerProposalLost, runnerAcceptLost, runnerSendDropped, runnerACKDropped atomic.Bool
	var droppedConnections atomic.Int64
	var watchAttempts atomic.Int64
	var dropConnections func() int
	var firstMessage atomic.Value
	var retiredData, deadlines, discoveries, credentialDiscovery atomic.Int64
	faults := func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
		if info.FullMethod == v2.Messenger_ProposeEpochRecovery_FullMethodName && req.(*v2.RecoveryCommandRequest).Command.Control.ExpectedEpoch == 1 {
			runnerPhase.Store(true)
		}
		if runnerPhase.Load() && info.FullMethod == v2.Messenger_SendMessage_FullMethodName && req.(*v2.SendMessageRequest).Envelope.GetCryptoEpoch() == 1 && runnerOldDeferred.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "runner old Send deferred before COMMIT")
		}
		if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) > 0 && time.Until(deadline) <= 11*time.Second {
			deadlines.Add(1)
		}
		if info.FullMethod == pb.Messenger_GetOwnDevice_FullMethodName {
			discoveries.Add(1)
			md, _ := metadata.FromIncomingContext(ctx)
			if len(md.Get(E2EEDeviceCredentialMetadata)) != 0 {
				credentialDiscovery.Add(1)
			}
		}
		switch info.FullMethod {
		case pb.Messenger_AcquireBundle_FullMethodName, pb.Messenger_SendMessage_FullMethodName, pb.Messenger_GetPendingMessages_FullMethodName, pb.Messenger_AcknowledgeMessages_FullMethodName, pb.Messenger_GetMessageStatuses_FullMethodName:
			retiredData.Add(1)
		}
		if info.FullMethod == v2.Messenger_AcquireBundle_FullMethodName && badBundle.Load() && expiredAcquire.CompareAndSwap(false, true) {
			if err := expireEpochAcquisitionFixture(ctx, db, req.(*v2.AcquireBundleRequest)); err != nil {
				return nil, status.Error(codes.Internal, "synthetic expiry setup failed")
			}
		}
		value, err := handler(ctx, req)
		if err != nil {
			if info.FullMethod == v2.Messenger_AcquireBundle_FullMethodName && status.Code(err) == codes.FailedPrecondition {
				details := status.Convert(err).Details()
				if len(details) == 1 {
					if f, ok := details[0].(*v2.AcquisitionExpiredFailure); ok && badExpiry.CompareAndSwap(false, true) {
						copy := proto.Clone(f).(*v2.AcquisitionExpiredFailure)
						copy.Request.CryptoEpoch = proto.Uint64(1)
						forged, e := status.New(codes.FailedPrecondition, "synthetic expiry binding fault").WithDetails(copy)
						if e != nil {
							return nil, e
						}
						return nil, forged.Err()
					}
				}
			}
			if info.FullMethod == v2.Messenger_SendMessage_FullMethodName && status.Code(err) == codes.FailedPrecondition {
				s := status.Convert(err)
				details := s.Details()
				if len(details) == 1 {
					if f, ok := details[0].(*v2.EpochSealedFailure); ok && badFence.CompareAndSwap(false, true) {
						copy := proto.Clone(f).(*v2.EpochSealedFailure)
						copy.Recovery.Decision.Signature[0] ^= 1
						forged, e := status.New(codes.FailedPrecondition, "synthetic signature fault").WithDetails(copy)
						if e != nil {
							return nil, e
						}
						return nil, forged.Err()
					}
				}
			}
			return value, err
		}
		if runnerPhase.Load() {
			switch info.FullMethod {
			case v2.Messenger_ProposeEpochRecovery_FullMethodName:
				if req.(*v2.RecoveryCommandRequest).Command.Control.ExpectedEpoch == 1 && runnerProposalLost.CompareAndSwap(false, true) {
					return nil, status.Error(codes.Unavailable, "runner proposal reply lost after COMMIT")
				}
			case v2.Messenger_DecideEpochRecovery_FullMethodName:
				if req.(*v2.RecoveryCommandRequest).Command.Control.ExpectedEpoch == 1 && runnerAcceptLost.CompareAndSwap(false, true) {
					return nil, status.Error(codes.Unavailable, "runner accept reply lost after COMMIT")
				}
			case v2.Messenger_SendMessage_FullMethodName:
				if req.(*v2.SendMessageRequest).Envelope.GetCryptoEpoch() == 2 && runnerSendDropped.CompareAndSwap(false, true) {
					droppedConnections.Add(int64(dropConnections()))
				}
			case v2.Messenger_AcknowledgeMessages_FullMethodName:
				if runnerACKDropped.CompareAndSwap(false, true) {
					droppedConnections.Add(int64(dropConnections()))
				}
			}
		}
		switch info.FullMethod {
		case v2.Messenger_SendMessage_FullMethodName:
			if lostSend.CompareAndSwap(false, true) {
				firstMessage.Store(req.(*v2.SendMessageRequest).Envelope.MessageId)
				return nil, status.Error(codes.Unavailable, "synthetic post-COMMIT lost Send response")
			}
		case v2.Messenger_AcknowledgeMessages_FullMethodName:
			ack := req.(*pb.AcknowledgeMessagesRequest)
			if len(ack.Messages) == 1 && ack.Messages[0].MessageId == firstMessage.Load() && lostACK.CompareAndSwap(false, true) {
				return nil, status.Error(codes.Unavailable, "synthetic post-COMMIT lost recipient ACK response")
			}
		case v2.Messenger_ProposeEpochRecovery_FullMethodName:
			if lostPropose.CompareAndSwap(false, true) {
				return nil, status.Error(codes.Unavailable, "synthetic post-COMMIT lost proposal response")
			}
		case v2.Messenger_DecideEpochRecovery_FullMethodName:
			if lostAccept.CompareAndSwap(false, true) {
				return nil, status.Error(codes.Unavailable, "synthetic post-COMMIT lost acceptance response")
			}
		case v2.Messenger_GetConversationEpochs_FullMethodName:
			if badCatalog.CompareAndSwap(false, true) {
				v := proto.Clone(value.(*v2.ConversationEpochs)).(*v2.ConversationEpochs)
				v.Epochs[0].Epoch = nil
				return v, nil
			}
		case v2.Messenger_AcquireBundle_FullMethodName:
			if badBundle.CompareAndSwap(false, true) {
				v := proto.Clone(value.(*v2.AcquiredBundle)).(*v2.AcquiredBundle)
				v.SignedPreKey.Signature[0] ^= 1
				return v, nil
			}
		case v2.Messenger_GetEpochRecovery_FullMethodName:
			if badGet.CompareAndSwap(false, true) {
				v := proto.Clone(value.(*v2.RecoveryRecord)).(*v2.RecoveryRecord)
				v.Outcome.Proposal.Signature[0] ^= 1
				return v, nil
			}
		case v2.Messenger_GetMessageStatuses_FullMethodName:
			if badStatus.CompareAndSwap(false, true) {
				v := proto.Clone(value.(*v2.MessageStatuses)).(*v2.MessageStatuses)
				v.Messages[0].Receipt.CryptoEpoch = proto.Uint64(v.Messages[0].Receipt.GetCryptoEpoch() + 1)
				return v, nil
			}
		case v2.Messenger_GetPendingMessages_FullMethodName:
			if badPending.CompareAndSwap(false, true) {
				v := proto.Clone(value.(*v2.PendingMessages)).(*v2.PendingMessages)
				v.Messages[0].Envelope.CryptoEpoch = proto.Uint64(v.Messages[0].Envelope.GetCryptoEpoch() + 1)
				return v, nil
			}
		}
		return value, nil
	}
	cert, ca := rustInteropCertificate(t)
	s := grpcgo.NewServer(grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})), grpcgo.MaxRecvMsgSize(MaxRequestSize), grpcgo.ChainUnaryInterceptor(faults, guard.Unary(NewUserGRPCServer(users, auth).AuthInterceptor)), grpcgo.ChainStreamInterceptor(func(srv any, stream grpcgo.ServerStream, info *grpcgo.StreamServerInfo, next grpcgo.StreamHandler) error {
		if info.FullMethod == pb.Messenger_WatchInbox_FullMethodName {
			watchAttempts.Add(1)
		}
		return next(srv, stream)
	}))
	RegisterOptionalEpochMessenger(s, "true", func() (*E2EEDeviceServer, *E2EEEpochServer) { return registry, epoch })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &epochTrackedListener{Listener: listener, connections: make(map[*epochTrackedConn]struct{})}
	dropConnections = l.dropConnections
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	defer func() { s.Stop(); l.Close(); <-done }()
	caPath := filepath.Join(t.TempDir(), "test-ca.pem")
	if err = os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("ZCHAT_RUST_E2EE_TEST_WINDOWS") == "1" {
		converted, e := exec.Command("wslpath", "-w", caPath).Output()
		if e != nil {
			t.Fatal("convert test CA path:", e)
		}
		caPath = strings.TrimSpace(string(converted))
	}
	ctx, stop := context.WithTimeout(t.Context(), 120*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::epoch::tests::interop::go_epoch_transport_worker", "--nocapture")
	// Synthetic JWTs through stdin only. Native discovers account UUIDs using
	// JWT; this fixture never supplies UUID/path/credential/private crypto keys.
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + l.Addr().String(), caPath, tokens[0], tokens[1], strconv.FormatInt(ids[1], 10)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Rust v2 transport: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "1 passed; 0 failed") || !strings.Contains(string(output), "zchat-native-epoch-transport-complete:v2") || !strings.Contains(string(output), "zchat-native-epoch0-local-commit:v2") || !strings.Contains(string(output), "zchat-native-epoch0-signal-bootstrap:v2") || !strings.Contains(string(output), "zchat-native-epoch0-durable-outbox:v2") || !strings.Contains(string(output), "zchat-native-epoch0-protected-receive-ack:v2") || !strings.Contains(string(output), "zchat-native-epoch0-protected-status:v2") || !strings.Contains(string(output), "zchat-native-epoch0-automatic-confirmation:v2") || !strings.Contains(string(output), "zchat-native-epoch0-protected-quarantine:v2") || !strings.Contains(string(output), "zchat-native-epoch0-protected-acquisition-renewal:v2") || !strings.Contains(string(output), "zchat-native-protected-recovery-intent:v2") || !strings.Contains(string(output), "zchat-native-protected-epoch-transition:v2") || !strings.Contains(string(output), "zchat-native-epoch-runner-complete:v2") {
		t.Fatalf("Rust worker did not complete: %s", output)
	}
	for name, fault := range map[string]*atomic.Bool{"lost Send": &lostSend, "lost recipient ACK": &lostACK, "lost proposal": &lostPropose, "lost accept": &lostAccept, "catalog presence": &badCatalog, "bundle signature": &badBundle, "recovery signature": &badGet, "fence signature": &badFence, "status epoch": &badStatus, "inbox epoch": &badPending, "expired acquisition": &expiredAcquire, "expiry binding": &badExpiry, "runner old deferred": &runnerOldDeferred, "runner proposal lost": &runnerProposalLost, "runner accept lost": &runnerAcceptLost, "runner Send connection drop": &runnerSendDropped, "runner ACK connection drop": &runnerACKDropped} {
		if !fault.Load() {
			t.Fatal("unexercised fault", name)
		}
	}
	if droppedConnections.Load() < 2 {
		t.Fatal("runner did not lose actual TLS connections", droppedConnections.Load())
	}
	if watchAttempts.Load() == 0 {
		t.Fatal("optional hint failure fallback was not exercised")
	}
	if retiredData.Load() != 0 || discoveries.Load() < 2 || credentialDiscovery.Load() != 0 || deadlines.Load() < 30 {
		t.Fatal("wrong v2 route/auth/deadline observations", retiredData.Load(), discoveries.Load(), credentialDiscovery.Load(), deadlines.Load())
	}
	var deviceCount, epochs, proposals, operations, receipts, acknowledged, inbox, acquisitions, rejected int
	err = db.QueryRowContext(t.Context(), `SELECT (SELECT count(*)FROM e2ee_devices),(SELECT count(*)FROM e2ee_conversation_epochs),(SELECT count(*)FROM e2ee_epoch_recoveries),(SELECT count(*)FROM e2ee_recovery_operations),(SELECT count(*)FROM e2ee_send_receipts),(SELECT count(*)FROM e2ee_message_acknowledgements),(SELECT count(*)FROM e2ee_message_inbox),(SELECT count(*)FROM e2ee_acquisitions),(SELECT count(*)FROM e2ee_message_acknowledgements WHERE disposition='rejected')`).Scan(&deviceCount, &epochs, &proposals, &operations, &receipts, &acknowledged, &inbox, &acquisitions, &rejected)
	if err != nil {
		t.Fatal(err)
	}
	if deviceCount != 2 || epochs != 3 || proposals != 2 || operations != 5 || receipts != 11 || acknowledged != 11 || inbox != 0 || acquisitions != 4 || rejected != 1 {
		t.Fatal("unexpected persisted facts", deviceCount, epochs, proposals, operations, receipts, acknowledged, inbox, acquisitions, rejected)
	}
	t.Logf("production v2/registry TLS +JWT/current-device auth; %d bounded RPC deadlines,0retired DATA calls; protected initial0 Signal PreKey->automatic confirmation->Signal, reply exact retry/reopen after discarded server receipt, no reply loop; invalid ciphertext does NOT auto-reject; explicit protected quarantine and Rejected ACK/reopen/exact retry after discarded remote ACK; signature/pin/epoch/cursor rejection; original receive replay and fresh Signal receive after server sealing; lost Send/proposal/accept/ACK responses;2devices/3epochs/2proposals/5ops/11receipts/11protectedACKrows(1Rejected)/0inbox/4acquisitions; exact typed expiry->one new nonce->reopen->Signal bootstrap; protected statuses COMMIT/reopen and terminal queues; protected signed proposal/manual native acceptance+freeze before RPC, exact retry/reopen after lost replies; protected local activation+archive/reopen; unknown old Send blocks until exact fence or original receipt, no peer-ACK gate; real epoch1 PreKey/automatic confirmation/Signal then retained old receive+ACK; production v2 runner discovers pending without consent, replays saved control bytes after lost replies+worker reopen, settles old unknown receipt/fence and activates epoch2/bootstrap automatically; mixed old/new delivery/confirmation/status/ACK through bounded runner; forced post-COMMIT Send+ACK TLS connection drops and original-channel reconnect; no UI consent proof", deadlines.Load())
}

// Only connections accepted by this disposable fixture are closed. The server,
// listening endpoint and PostgreSQL stay alive; original tonic channels reconnect.
type epochTrackedListener struct {
	net.Listener
	mu          sync.Mutex
	connections map[*epochTrackedConn]struct{}
}
type epochTrackedConn struct {
	net.Conn
	owner *epochTrackedListener
}

func (l *epochTrackedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	w := &epochTrackedConn{Conn: c, owner: l}
	l.mu.Lock()
	l.connections[w] = struct{}{}
	l.mu.Unlock()
	return w, nil
}
func (c *epochTrackedConn) Close() error {
	c.owner.mu.Lock()
	delete(c.owner.connections, c)
	c.owner.mu.Unlock()
	return c.Conn.Close()
}
func (l *epochTrackedListener) dropConnections() int {
	l.mu.Lock()
	connections := make([]*epochTrackedConn, 0, len(l.connections))
	for c := range l.connections {
		connections = append(connections, c)
	}
	l.mu.Unlock()
	for _, c := range connections {
		_ = c.Close()
	}
	return len(connections)
}
