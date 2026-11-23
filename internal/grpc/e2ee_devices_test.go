package grpc

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

const deviceTestPassword = "Synthetic-device-password-2026"
const deviceTestJWTSecret = "synthetic-e2ee-jwt-secret-at-least-32"

type deviceRPCFixture struct {
	db                   *sql.DB
	auth                 *service.AuthService
	devices              *service.E2EEDeviceService
	guard                *E2EEAuth
	client               pb.MessengerClient
	conn                 *grpcgo.ClientConn
	server               *E2EEDeviceServer
	rootCancel           context.CancelFunc
	aliceToken, bobToken string
	aliceID, bobID       int64
	aliceRaw             []byte
	registration         *pb.RegisterDeviceRequest
	device               *pb.OwnDeviceState
	exited               chan struct{}
	deadline             chan time.Time
	blockWrite           atomic.Bool
	writeStarted         chan struct{}
	writeFinished        chan error
}

// Authentication probe only: sends a sentinel, not a real encrypted message or
// durable delivery. Production StreamMessages remains unavailable in this slice.
type deviceStreamProbe struct {
	*E2EEDeviceServer
	deadline chan time.Time
	fixture  *deviceRPCFixture
}

func (p *deviceStreamProbe) StreamMessages(_ *pb.StreamMessagesRequest, stream grpcgo.ServerStreamingServer[pb.Delivery]) error {
	deadline, _ := stream.Context().Deadline()
	p.deadline <- deadline
	if p.fixture.blockWrite.Load() {
		// gRPC can queue the first message and debit more than its initial
		// write quota. Fill that quota with a frame larger than the receiver's
		// static stream window, then wait on a second Send with no reader.
		if err := stream.Send(&pb.Delivery{Envelope: &pb.Envelope{Ciphertext: make([]byte, 256<<10)}}); err != nil {
			p.fixture.writeFinished <- err
			return err
		}
		close(p.fixture.writeStarted)
		err := stream.Send(&pb.Delivery{Sequence: 2})
		p.fixture.writeFinished <- err
		return err
	}
	if err := stream.Send(&pb.Delivery{Sequence: 1}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
func newDeviceRPCFixture(t *testing.T, probe bool, dialOptions ...grpcgo.DialOption) *deviceRPCFixture {
	t.Helper()
	f := &deviceRPCFixture{db: testutil.Postgres(t), exited: make(chan struct{}, 8), deadline: make(chan time.Time, 8), writeStarted: make(chan struct{}), writeFinished: make(chan error, 1)}
	users := repository.NewPgUserRepository(f.db)
	f.auth = service.NewAuthService(users, deviceTestJWTSecret)
	var err error
	f.aliceToken, err = f.auth.Register(t.Context(), "alice", "alice@device.invalid", deviceTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	f.bobToken, err = f.auth.Register(t.Context(), "bob", "bob@device.invalid", deviceTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	claims, _ := f.auth.ParseToken(f.aliceToken)
	f.aliceID = claims.UserID
	claims, _ = f.auth.ParseToken(f.bobToken)
	f.bobID = claims.UserID
	f.devices = service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(f.db), f.auth)
	root, cancel := context.WithCancel(t.Context())
	f.rootCancel = cancel
	t.Cleanup(cancel)
	f.guard = NewE2EEAuth(root, f.auth, f.devices)
	f.guard.pollInterval = 100 * time.Millisecond
	f.devices.SetDeviceReplacer(f.guard.DisconnectGeneration)
	f.auth.SetSessionRevoker(func(_ context.Context, uid int64, sid string) error { f.guard.DisconnectSession(uid, sid); return nil })
	f.server = newPrototypeDeviceServer(f.devices, service.NewE2EEPreKeyService(repository.NewPgE2EEPreKeyRepository(f.db)), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(f.db)), service.NewE2EEMessageService(repository.NewPgE2EEDeliveryRepository(f.db)))
	// Stop normally only closes transports; interceptor defers may still be
	// removing stream leases. Join handlers before asserting teardown invariants.
	server := grpcgo.NewServer(grpcgo.WaitForHandlers(true), grpcgo.MaxRecvMsgSize(MaxRequestSize), grpcgo.UnaryInterceptor(f.guard.Unary(NewUserGRPCServer(users, f.auth).AuthInterceptor)), grpcgo.ChainStreamInterceptor(f.guard.Stream(), func(srv any, ss grpcgo.ServerStream, info *grpcgo.StreamServerInfo, handler grpcgo.StreamHandler) error {
		defer func() { f.exited <- struct{}{} }()
		return handler(srv, ss)
	}))
	var implementation pb.MessengerServer = f.server
	if probe {
		implementation = &deviceStreamProbe{f.server, f.deadline, f}
	}
	RegisterOptionalMessenger(server, "true", func() pb.MessengerServer { return implementation })
	listener := bufconn.Listen(1 << 20)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		listener.Close()
		<-done
		f.guard.mu.Lock()
		defer f.guard.mu.Unlock()
		if len(f.guard.streams) != 0 {
			t.Error("stream lease leaked")
		}
	})
	options := []grpcgo.DialOption{
		grpcgo.WithTransportCredentials(insecure.NewCredentials()),
		grpcgo.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
	}
	conn, err := grpcgo.NewClient("passthrough:///e2ee-devices", append(options, dialOptions...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	f.client = pb.NewMessengerClient(conn)
	f.conn = conn
	f.aliceRaw = make([]byte, 32)
	if _, err = rand.Read(f.aliceRaw); err != nil {
		t.Fatal(err)
	}
	key := bytes.Repeat([]byte{42}, 33)
	key[0] = 5
	hash := service.E2EEDeviceCredentialHash(f.aliceRaw)
	f.registration = &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 16383, IdentityKey: key, CredentialHash: hash[:]}
	f.device, err = f.client.RegisterDevice(f.ctx(t, f.aliceToken, f.aliceRaw), f.registration)
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *deviceRPCFixture) ctx(t *testing.T, token string, raw []byte) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	md := metadata.Pairs("authorization", "Bearer "+token)
	if raw != nil {
		md.Append(E2EEDeviceCredentialMetadata, string(raw))
	}
	return metadata.NewOutgoingContext(ctx, md)
}
func rpcCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("RPC code=%s wanted=%s: %v", status.Code(err), want, err)
	}
}
func (f *deviceRPCFixture) streamRequest() *pb.StreamMessagesRequest {
	return &pb.StreamMessagesRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.device.ActiveDevice.Reference.GenerationId}}
}
func (f *deviceRPCFixture) open(t *testing.T, token string) grpcgo.ServerStreamingClient[pb.Delivery] {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	t.Cleanup(cancel)
	return f.openContext(t, token, ctx)
}
func (f *deviceRPCFixture) openContext(t *testing.T, token string, ctx context.Context) grpcgo.ServerStreamingClient[pb.Delivery] {
	t.Helper()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+token, E2EEDeviceCredentialMetadata, string(f.aliceRaw)))
	stream, err := f.client.StreamMessages(ctx, f.streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil || first.Sequence != 1 {
		t.Fatal("auth probe did not start", err)
	}
	return stream
}
func (f *deviceRPCFixture) waitExit(t *testing.T) {
	t.Helper()
	select {
	case <-f.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled stream handler did not exit")
	}
}

func TestE2EERPCStreamRevokedBeforeFirstRequestPostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, true)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+f.aliceToken, E2EEDeviceCredentialMetadata, string(f.aliceRaw)))
	stream, err := f.conn.NewStream(ctx, &pb.Messenger_ServiceDesc.Streams[0], pb.Messenger_StreamMessages_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		f.guard.mu.Lock()
		n := len(f.guard.streams)
		f.guard.mu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("stream headers not admitted")
		}
		time.Sleep(time.Millisecond)
	}
	// Deliberately send no request frame and do not close the client send side.
	if err := f.auth.Logout(t.Context(), f.aliceToken, false, func(int64, string) {}); err != nil {
		t.Fatal(err)
	}
	err = stream.RecvMsg(new(pb.Delivery))
	rpcCode(t, err, codes.Unauthenticated)
	f.waitExit(t)
}

func TestE2EERPCRevocationUnblocksTransportWritePostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, true, grpcgo.WithStaticStreamWindowSize(64<<10))
	f.blockWrite.Store(true)
	_, err := f.client.StreamMessages(f.ctx(t, f.aliceToken, f.aliceRaw), f.streamRequest())
	if err != nil {
		t.Fatal(err)
	}
	// Keep the client receive side unread throughout the revocation.
	select {
	case <-f.writeStarted:
	case err := <-f.writeFinished:
		t.Fatalf("writer stopped before the block: %v", err)
	case <-time.After(time.Second):
		t.Fatal("writer did not start")
	}
	select {
	case err := <-f.writeFinished:
		t.Fatalf("fixture did not force a blocked transport write: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := f.auth.Logout(t.Context(), f.aliceToken, false, func(int64, string) {}); err != nil {
		t.Fatal(err)
	}
	f.waitExit(t)
	select {
	case err := <-f.writeFinished:
		if err == nil {
			t.Fatal("blocked write reported success after revocation")
		}
	default:
		t.Fatal("revocation left transport writer blocked")
	}
}

func TestE2EERPCStreamCapacityAndFirstRequestDeadlinePostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, true)
	f.guard.firstRequestTimeout = 100 * time.Millisecond
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	stalled, err := f.conn.NewStream(ctx, &pb.Messenger_ServiceDesc.Streams[0], pb.Messenger_StreamMessages_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	err = stalled.RecvMsg(new(pb.Delivery))
	rpcCode(t, err, codes.DeadlineExceeded)
	f.waitExit(t)
	streams := make([]grpcgo.ServerStreamingClient[pb.Delivery], 0, maxE2EEStreamsPerAccount)
	for range maxE2EEStreamsPerAccount {
		streams = append(streams, f.open(t, f.aliceToken))
	}
	excess, err := f.client.StreamMessages(ctx, f.streamRequest())
	if err == nil {
		_, err = excess.Recv()
	}
	rpcCode(t, err, codes.ResourceExhausted)
	f.rootCancel()
	for _, stream := range streams {
		_, err := stream.Recv()
		rpcCode(t, err, codes.Canceled)
		f.waitExit(t)
	}
}

func TestE2EERPCDeviceOwnershipPostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, false)
	caps, err := f.client.GetCapabilities(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || !caps.DeviceRegistrationAvailable || caps.MessagingAvailable || caps.MaxActiveDevices != 1 {
		t.Fatal("false capability/readiness", err)
	}
	if f.device.ActiveDevice.PublicMaterialReady {
		t.Fatal("new device is prematurely ready")
	}
	for _, md := range []metadata.MD{
		{}, metadata.Pairs("authorization", f.aliceToken),
		metadata.Pairs("authorization", "Bearer "+f.aliceToken, "authorization", "Bearer "+f.aliceToken),
		metadata.Pairs("grpc-metadata-authorization", "Bearer "+f.aliceToken),
	} {
		_, err := f.client.GetOwnDevice(metadata.NewOutgoingContext(t.Context(), md), &pb.Empty{})
		rpcCode(t, err, codes.Unauthenticated)
	}
	own, err := f.client.GetOwnDevice(f.ctx(t, f.bobToken, nil), &pb.Empty{})
	if err != nil || own.ActiveDevice != nil || own.UserId == f.device.UserId {
		t.Fatal("own lookup leaked another device", err)
	}
	again, err := f.client.RegisterDevice(f.ctx(t, f.aliceToken, f.aliceRaw), f.registration)
	if err != nil || !proto.Equal(again, f.device) {
		t.Fatal("lost-response retry changed result", err)
	}
	changed := proto.Clone(f.registration).(*pb.RegisterDeviceRequest)
	changed.IdentityKey[2] ^= 1
	_, err = f.client.RegisterDevice(f.ctx(t, f.aliceToken, f.aliceRaw), changed)
	rpcCode(t, err, codes.AlreadyExists)
	changed = proto.Clone(f.registration).(*pb.RegisterDeviceRequest)
	changed.RegistrationRequestId = uuid.NewString()
	_, err = f.client.RegisterDevice(f.ctx(t, f.aliceToken, f.aliceRaw), changed)
	rpcCode(t, err, codes.AlreadyExists)
	changed = proto.Clone(f.registration).(*pb.RegisterDeviceRequest)
	changed.ProtocolVersion = 2
	_, err = f.client.RegisterDevice(f.ctx(t, f.aliceToken, f.aliceRaw), changed)
	rpcCode(t, err, codes.FailedPrecondition)
	_, err = f.client.RegisterDevice(f.ctx(t, f.aliceToken, nil), f.registration)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.RegisterDevice(f.ctx(t, f.aliceToken, bytes.Repeat([]byte{7}, 32)), f.registration)
	rpcCode(t, err, codes.Unauthenticated)
	duplicated := metadata.Pairs("authorization", "Bearer "+f.aliceToken, E2EEDeviceCredentialMetadata, string(f.aliceRaw), E2EEDeviceCredentialMetadata, string(f.aliceRaw))
	_, err = f.client.RegisterDevice(metadata.NewOutgoingContext(t.Context(), duplicated), f.registration)
	rpcCode(t, err, codes.Unauthenticated)
	// Missing device possession cannot be replaced by a valid JWT or a claimed generation.
	// A valid owner now reaches the implemented Send validator; the absent
	// envelope is invalid. Foreign/replaced credentials must fail before it.
	req := &pb.SendMessageRequest{Context: f.streamRequest().Context}
	for _, tc := range []struct {
		token string
		raw   []byte
		want  codes.Code
	}{
		{f.aliceToken, nil, codes.Unauthenticated}, {f.bobToken, f.aliceRaw, codes.Unauthenticated},
		{f.aliceToken, bytes.Repeat([]byte{8}, 32), codes.Unauthenticated}, {f.aliceToken, f.aliceRaw, codes.InvalidArgument},
	} {
		_, err = f.client.SendMessage(f.ctx(t, tc.token, tc.raw), req)
		rpcCode(t, err, tc.want)
	}
	req.Context.DeviceGenerationId = uuid.NewString()
	_, err = f.client.SendMessage(f.ctx(t, f.aliceToken, f.aliceRaw), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.server.GetOwnDevice(t.Context(), &pb.Empty{})
	rpcCode(t, err, codes.Unauthenticated)
	stream, err := f.client.StreamMessages(f.ctx(t, f.aliceToken, f.aliceRaw), f.streamRequest())
	if err == nil {
		_, err = stream.Recv()
	}
	rpcCode(t, err, codes.FailedPrecondition)
}

func TestE2EERPCReplacementRevokesOnlyOldDevicePostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, true)
	stream := f.open(t, f.aliceToken)
	newRaw := bytes.Repeat([]byte{29}, 32)
	hash := service.E2EEDeviceCredentialHash(newRaw)
	req := &pb.ReplaceOwnDeviceRequest{ProtocolVersion: 1, ReplacementRequestId: uuid.NewString(), ExpectedGenerationId: f.device.ActiveDevice.Reference.GenerationId, RegistrationId: 2, IdentityKey: bytes.Clone(f.registration.IdentityKey), CredentialHash: hash[:], CurrentPassword: "synthetic-wrong-password"}
	req.IdentityKey[2] ^= 1
	_, err := f.client.ReplaceOwnDevice(f.ctx(t, f.aliceToken, newRaw), req)
	rpcCode(t, err, codes.Unauthenticated)
	if strings.Contains(err.Error(), req.CurrentPassword) {
		t.Fatal("password leaked in error")
	}
	current, err := f.client.GetOwnDevice(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || !proto.Equal(current, f.device) {
		t.Fatal("bad password mutated device", err)
	}
	req.CurrentPassword = deviceTestPassword
	next, err := f.client.ReplaceOwnDevice(f.ctx(t, f.aliceToken, newRaw), req)
	if err != nil || next.ActiveDevice.Reference.GenerationId == req.ExpectedGenerationId {
		t.Fatal("explicit replacement failed", err)
	}
	_, err = stream.Recv()
	rpcCode(t, err, codes.Unauthenticated)
	f.waitExit(t)
	replay, err := f.client.ReplaceOwnDevice(f.ctx(t, f.aliceToken, newRaw), req)
	if err != nil || !proto.Equal(replay, next) {
		t.Fatal("replacement retry changed generation", err)
	}
	if _, err = f.auth.ValidateTokenContext(t.Context(), f.aliceToken); err != nil {
		t.Fatal("device replacement revoked ordinary login", err)
	}
	_, err = f.client.SendMessage(f.ctx(t, f.aliceToken, f.aliceRaw), &pb.SendMessageRequest{Context: f.streamRequest().Context})
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.SendMessage(f.ctx(t, f.aliceToken, newRaw), &pb.SendMessageRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: next.ActiveDevice.Reference.GenerationId}})
	rpcCode(t, err, codes.InvalidArgument)
	stale := proto.Clone(req).(*pb.ReplaceOwnDeviceRequest)
	stale.ReplacementRequestId = uuid.NewString()
	_, err = f.client.ReplaceOwnDevice(f.ctx(t, f.aliceToken, newRaw), stale)
	rpcCode(t, err, codes.FailedPrecondition)
}

func TestE2EERPCStreamsLogoutAndPasswordPostgres(t *testing.T) {
	t.Run("logout current preserves another login and logout all closes it", func(t *testing.T) {
		f := newDeviceRPCFixture(t, true)
		second, _, _, err := f.auth.Login(t.Context(), "alice@device.invalid", deviceTestPassword)
		if err != nil {
			t.Fatal(err)
		}
		a, b := f.open(t, f.aliceToken), f.open(t, second)
		if err = f.auth.Logout(t.Context(), f.aliceToken, false, func(int64, string) {}); err != nil {
			t.Fatal(err)
		}
		_, err = a.Recv()
		rpcCode(t, err, codes.Unauthenticated)
		f.waitExit(t)
		if _, err = f.client.GetOwnDevice(f.ctx(t, second, nil), &pb.Empty{}); err != nil {
			t.Fatal("independent login revoked", err)
		}
		f.guard.mu.Lock()
		remaining := len(f.guard.streams)
		f.guard.mu.Unlock()
		if remaining != 1 {
			t.Fatalf("remaining streams=%d", remaining)
		}
		if err = f.auth.Logout(t.Context(), second, true, func(int64, string) {}); err != nil {
			t.Fatal(err)
		}
		_, err = b.Recv()
		rpcCode(t, err, codes.Unauthenticated)
		f.waitExit(t)
		_, d, err := repository.NewPgE2EEDeviceRepository(f.db).Own(t.Context(), f.aliceID)
		if err != nil || d.GenerationID != f.device.ActiveDevice.Reference.GenerationId {
			t.Fatal("logout erased identity", err)
		}
	})
	t.Run("password change closes stream and preserves device", func(t *testing.T) {
		f := newDeviceRPCFixture(t, true)
		stream := f.open(t, f.aliceToken)
		if err := f.auth.ChangePassword(t.Context(), f.aliceToken, deviceTestPassword, "Synthetic-new-device-password", func(int64) {}); err != nil {
			t.Fatal(err)
		}
		_, err := stream.Recv()
		rpcCode(t, err, codes.Unauthenticated)
		f.waitExit(t)
		token, _, _, err := f.auth.Login(t.Context(), "alice@device.invalid", "Synthetic-new-device-password")
		if err != nil {
			t.Fatal(err)
		}
		own, err := f.client.GetOwnDevice(f.ctx(t, token, nil), &pb.Empty{})
		if err != nil || !proto.Equal(own, f.device) {
			t.Fatal("password change lost device", err)
		}
	})
}

func TestE2EERPCStreamFallbackAndLifetimePostgres(t *testing.T) {
	for _, mode := range []string{"DB session expiry", "DB generation replacement", "DB outage", "JWT expiry", "client cancellation", "server shutdown"} {
		t.Run(mode, func(t *testing.T) {
			f := newDeviceRPCFixture(t, true)
			token := f.aliceToken
			if mode == "JWT expiry" {
				claims, _ := f.auth.ParseToken(token)
				claims.ExpiresAt = jwt.NewNumericDate(time.Now().Add(2 * time.Second))
				var err error
				token, err = jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(deviceTestJWTSecret))
				if err != nil {
					t.Fatal(err)
				}
			}
			transport, cancelTransport := context.WithTimeout(t.Context(), 90*time.Second)
			t.Cleanup(cancelTransport)
			stream := f.openContext(t, token, transport)
			deadline := <-f.deadline
			if mode != "JWT expiry" && time.Until(deadline) < time.Minute {
				t.Fatal("unary timeout inherited by stream")
			}
			want := codes.Unauthenticated
			switch mode {
			case "DB session expiry":
				claims, _ := f.auth.ParseToken(token)
				if _, err := f.db.ExecContext(t.Context(), `UPDATE auth_sessions SET expires_at=now()-interval '1 second' WHERE id=$1`, claims.ID); err != nil {
					t.Fatal(err)
				}
			case "DB generation replacement":
				// Bypass local notification deliberately: periodic authoritative DB check.
				if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET retired_at=now() WHERE user_id=$1`, f.aliceID); err != nil {
					t.Fatal(err)
				}
			case "DB outage":
				if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE auth_sessions RENAME TO auth_sessions_offline`); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE auth_sessions_offline RENAME TO auth_sessions`); err != nil {
						t.Error(err)
					}
				}()
				want = codes.Unavailable
			case "client cancellation":
				// Canceling the incoming transport must release the monitor and lease.
				cancelTransport()
				want = codes.Canceled
			case "server shutdown":
				f.rootCancel()
				want = codes.Canceled
			}
			_, err := stream.Recv()
			rpcCode(t, err, want)
			f.waitExit(t)
		})
	}
}

func TestE2EERPCStreamRejectsUnownedRequestsPostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, true)
	for _, tc := range []struct {
		token      string
		raw        []byte
		generation string
		version    uint32
	}{
		{f.aliceToken, nil, f.device.ActiveDevice.Reference.GenerationId, 1},
		{f.bobToken, f.aliceRaw, f.device.ActiveDevice.Reference.GenerationId, 1},
		{f.aliceToken, f.aliceRaw, uuid.NewString(), 1},
		{f.aliceToken, f.aliceRaw, f.device.ActiveDevice.Reference.GenerationId, 2},
	} {
		stream, err := f.client.StreamMessages(f.ctx(t, tc.token, tc.raw), &pb.StreamMessagesRequest{Context: &pb.ClientContext{ProtocolVersion: tc.version, DeviceGenerationId: tc.generation}})
		if err == nil {
			_, err = stream.Recv()
		}
		want := codes.Unauthenticated
		if tc.version == 2 {
			want = codes.FailedPrecondition
		}
		rpcCode(t, err, want)
	}
	// JWT-only access to a stream is never a substitute for device possession.
	f.guard.mu.Lock()
	remaining := len(f.guard.streams)
	f.guard.mu.Unlock()
	if remaining != 0 {
		t.Fatal("failed admission leaked stream")
	}
}
