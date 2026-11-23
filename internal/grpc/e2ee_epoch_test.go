package grpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type epochRPCFixture struct {
	*deviceRPCFixture
	v2       v2.MessengerClient
	registry pb.MessengerClient
	bob      *pb.OwnDeviceState
	bobRaw   []byte
	chat     *pb.Conversation
	epoch    *E2EEEpochServer
	endpoint string
	ca       []byte
}

func newEpochRPCFixture(t *testing.T, beforeAuth ...grpcgo.UnaryServerInterceptor) *epochRPCFixture {
	t.Helper()
	// Reuse disposable account/registry setup. Its old bufconn prototype is
	// test-only; the separately served TLS graph below uses production defaults.
	f := &epochRPCFixture{deviceRPCFixture: newDeviceRPCFixture(t, false), bobRaw: bytes.Repeat([]byte{86}, 32)}
	keys := repository.NewPgE2EEPreKeyRepository(f.db)
	registry := NewE2EEDeviceServer(f.devices, service.NewE2EEPreKeyService(keys), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(f.db)), nil)
	epoch := NewE2EEEpochServer(service.NewE2EEEpochService(repository.NewPgE2EEDeliveryRepository(f.db), keys, repository.NewPgE2EERecoveryRepository(f.db)))
	f.epoch = epoch
	cert, ca := rustInteropCertificate(t)
	interceptors := append(slices.Clone(beforeAuth), f.guard.Unary(nil))
	s := grpcgo.NewServer(grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})), grpcgo.MaxRecvMsgSize(MaxRequestSize), grpcgo.ChainUnaryInterceptor(interceptors...))
	RegisterOptionalEpochMessenger(s, "true", func() (*E2EEDeviceServer, *E2EEEpochServer) { return registry, epoch })
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.endpoint, f.ca = "https://"+l.Addr().String(), ca
	done := make(chan error, 1)
	go func() { done <- s.Serve(l) }()
	t.Cleanup(func() { s.Stop(); l.Close(); <-done })
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		t.Fatal("invalid test CA")
	}
	conn, err := grpcgo.NewClient(l.Addr().String(), grpcgo.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	f.v2 = v2.NewMessengerClient(conn)
	f.registry = pb.NewMessengerClient(conn)
	hash := service.E2EEDeviceCredentialHash(f.bobRaw)
	f.bob, err = f.registry.RegisterDevice(f.ctx(t, f.bobToken, f.bobRaw), &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 16383, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:]})
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range []struct {
		token string
		raw   []byte
		gen   string
	}{{f.aliceToken, f.aliceRaw, f.gen(1)}, {f.bobToken, f.bobRaw, f.gen(2)}} {
		if _, err = f.registry.UploadPreKeys(f.ctx(t, a.token, a.raw), testutil.PublicPreKeyUpload(t, a.gen)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = f.db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status)VALUES($1,$2,'accepted')`, f.aliceID, f.bobID); err != nil {
		t.Fatal(err)
	}
	f.chat, err = f.registry.CreateConversation(f.ctx(t, f.aliceToken, f.aliceRaw), &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.gen(1)}, CreationRequestId: uuid.NewString(), PeerUserId: f.bobID})
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *epochRPCFixture) gen(who int) string {
	if who == 1 {
		return f.device.ActiveDevice.Reference.GenerationId
	}
	return f.bob.ActiveDevice.Reference.GenerationId
}
func (f *epochRPCFixture) wire(who int) *pb.ClientContext {
	return &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: f.gen(who)}
}
func (f *epochRPCFixture) ownerContext(t *testing.T, who int) context.Context {
	if who == 1 {
		return f.ctx(t, f.aliceToken, f.aliceRaw)
	}
	return f.ctx(t, f.bobToken, f.bobRaw)
}
func (f *epochRPCFixture) message(epoch uint64) *v2.SendMessageRequest {
	return &v2.SendMessageRequest{Context: f.wire(1), Envelope: &v2.Envelope{ProtocolVersion: 2, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: f.chat.ConversationId, Sender: f.device.ActiveDevice.Reference, Recipient: f.bob.ActiveDevice.Reference, MessageType: 1, Ciphertext: []byte("opaque synthetic ciphertext, NOT actual Signal exchange"), CryptoEpoch: proto.Uint64(epoch)}}
}
func (f *epochRPCFixture) proposal() *pb.SignedRecoveryControl {
	participant := func(d *pb.Device) *pb.RecoveryParticipant {
		hash := sha256.Sum256(d.IdentityKey)
		return &pb.RecoveryParticipant{Device: d.Reference, IdentityFingerprint: hash[:]}
	}
	id := uuid.NewString()
	// Signature SHAPES only; actual peer signature verification belongs to the
	// native integration step, not this Go ownership/transaction/TLS test.
	return &pb.SignedRecoveryControl{Control: &pb.RecoveryControl{ProtocolVersion: 2, FormatVersion: 1, CryptoSuite: 1, Action: pb.RecoveryAction_RECOVERY_ACTION_PROPOSE, ConversationId: f.chat.ConversationId, RequestId: id, ProposalId: id, Initiator: participant(f.device.ActiveDevice), Peer: participant(f.bob.ActiveDevice)}, Signature: bytes.Repeat([]byte{42}, 64)}
}
func rpcRecoveryDecision(p *pb.SignedRecoveryControl) *pb.SignedRecoveryControl {
	a := proto.Clone(p).(*pb.SignedRecoveryControl)
	a.Control.Action = pb.RecoveryAction_RECOVERY_ACTION_ACCEPT
	a.Control.RequestId = uuid.NewString()
	d, _ := boundary.RecoveryDigest(p.Control)
	a.Control.ProposalDigest = d[:]
	return a
}

func TestE2EEEpochRPCRecoveryAndRetainedDeliveryTLSPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	caps, err := f.v2.GetCapabilities(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || caps.ProtocolVersion != 2 || caps.MessagingAvailable || caps.RecoveryAvailable {
		t.Fatal("premature readiness", err)
	}
	old := f.message(0)
	receipt, err := f.v2.SendMessage(f.ownerContext(t, 1), old)
	if err != nil || boundary.ValidateEpochReceipt(old, receipt) != nil {
		t.Fatal("send receipt", err)
	}
	acq := &v2.AcquireBundleRequest{Context: f.wire(1), ConversationId: f.chat.ConversationId, AcquisitionId: uuid.NewString(), ExpectedPeer: f.bob.ActiveDevice.Reference, CryptoEpoch: proto.Uint64(0)}
	if _, err = f.v2.AcquireBundle(f.ownerContext(t, 1), acq); err != nil {
		t.Fatal(err)
	}
	p := f.proposal()
	request := &v2.RecoveryCommandRequest{Context: f.wire(1), Command: p}
	pending, err := f.v2.ProposeEpochRecovery(f.ownerContext(t, 1), request)
	if err != nil || pending.Outcome.State != pb.RecoveryState_RECOVERY_STATE_PENDING {
		t.Fatal("proposal", err)
	}
	page, err := f.v2.GetPendingEpochRecoveries(f.ownerContext(t, 2), &v2.GetPendingEpochRecoveriesRequest{Context: f.wire(2), Limit: 1})
	if err != nil || len(page.Proposals) != 1 || page.Proposals[0].InitiatedByOwnDevice {
		t.Fatal("peer polling", err)
	}
	catReq := &v2.GetConversationEpochsRequest{Context: f.wire(1), ConversationId: f.chat.ConversationId, Limit: 1}
	cat, err := f.v2.GetConversationEpochs(f.ownerContext(t, 1), catReq)
	if err != nil || cat.ActiveEpoch == nil || *cat.ActiveEpoch != 0 || cat.EpochRevision == nil || len(cat.Epochs) != 1 || cat.Epochs[0].Epoch == nil || *cat.Epochs[0].Epoch != 0 || cat.Epochs[0].BootstrapDevice != nil {
		t.Fatal("proposal advanced epoch or lost initial presence", err)
	}
	// Competing new proposal yields advisory conflict, never a sealed-send proof.
	_, err = f.v2.ProposeEpochRecovery(f.ownerContext(t, 1), &v2.RecoveryCommandRequest{Context: f.wire(1), Command: f.proposal()})
	s := status.Convert(err)
	if s.Code() != codes.Aborted || len(s.Details()) != 1 {
		t.Fatal("recovery conflict", err)
	}
	if _, ok := s.Details()[0].(*v2.RecoveryConflict); !ok {
		t.Fatal("wrong advisory detail")
	}
	accepted, err := f.v2.DecideEpochRecovery(f.ownerContext(t, 2), &v2.RecoveryCommandRequest{Context: f.wire(2), Command: rpcRecoveryDecision(p)})
	if err != nil || accepted.Outcome.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED || accepted.ResolvedAtUnixMs == nil {
		t.Fatal("peer acceptance", err)
	}
	exact, err := f.v2.GetEpochRecovery(f.ownerContext(t, 1), &v2.GetEpochRecoveryRequest{Context: f.wire(1), ConversationId: f.chat.ConversationId, ProposalId: p.Control.ProposalId})
	if err != nil || !proto.Equal(exact, accepted) {
		t.Fatal("authoritative exact lookup", err)
	}
	again, err := f.v2.SendMessage(f.ownerContext(t, 1), old)
	if err != nil || !proto.Equal(again, receipt) {
		t.Fatal("committed replay became fence", err)
	}
	unsent := f.message(0)
	_, err = f.v2.SendMessage(f.ownerContext(t, 1), unsent)
	s = status.Convert(err)
	if s.Code() != codes.FailedPrecondition || len(s.Details()) != 1 {
		t.Fatal("missing exact fence", err)
	}
	detail, ok := s.Details()[0].(*v2.EpochSealedFailure)
	if !ok {
		t.Fatal("wrong fence detail")
	}
	binding, _ := boundary.EpochSendBinding(unsent.Envelope)
	if boundary.ValidateEpochSealedFence(detail.Fence, binding, detail.Recovery) != nil {
		t.Fatal("unbound fence")
	}
	if boundary.ValidateEpochSealedFence(detail.Fence, mustSendBinding(t, old.Envelope), detail.Recovery) == nil {
		t.Fatal("fence settles another UUID")
	}
	sealed := &domain.E2EEEpochSealed{Fence: detail.Fence, Recovery: detail.Recovery}
	if len(status.Convert(epochSendStatus(sealed, old)).Details()) != 0 || len(status.Convert(epochStatus(sealed)).Details()) != 0 {
		t.Fatal("adapter attached fence to wrong Send or another method")
	}
	if _, err = f.v2.AcquireBundle(f.ownerContext(t, 1), acq); status.Code(err) != codes.FailedPrecondition || len(status.Convert(err).Details()) != 0 {
		t.Fatal("sealed acquire replay/fence", err)
	}
	acq.AcquisitionId = uuid.NewString()
	acq.CryptoEpoch = proto.Uint64(1)
	if _, err = f.v2.AcquireBundle(f.ownerContext(t, 1), acq); err != nil {
		t.Fatal("creator bootstrap", err)
	}
	if _, err = f.v2.AcquireBundle(f.ownerContext(t, 2), &v2.AcquireBundleRequest{Context: f.wire(2), ConversationId: f.chat.ConversationId, AcquisitionId: uuid.NewString(), ExpectedPeer: f.device.ActiveDevice.Reference, CryptoEpoch: proto.Uint64(1)}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("peer competing bootstrap", err)
	}
	cat, err = f.v2.GetConversationEpochs(f.ownerContext(t, 1), catReq)
	if err != nil || cat.GetActiveEpoch() != 1 || !cat.Epochs[0].Sealed || cat.NextAfterEpoch == nil || *cat.NextAfterEpoch != 0 {
		t.Fatal("paged sealed catalog", err)
	}
	catReq.AfterEpoch = cat.NextAfterEpoch
	cat, err = f.v2.GetConversationEpochs(f.ownerContext(t, 1), catReq)
	if err != nil || len(cat.Epochs) != 1 || cat.Epochs[0].GetEpoch() != 1 || !proto.Equal(cat.Epochs[0].BootstrapDevice, f.device.ActiveDevice.Reference) || cat.NextAfterEpoch != nil {
		t.Fatal("new epoch creator", err)
	}
	page, err = f.v2.GetPendingEpochRecoveries(f.ownerContext(t, 2), &v2.GetPendingEpochRecoveriesRequest{Context: f.wire(2), Limit: 32})
	if err != nil || len(page.Proposals) != 0 {
		t.Fatal("terminal recovery listed", err)
	}
	inbox, err := f.v2.GetPendingMessages(f.ownerContext(t, 2), &v2.GetPendingMessagesRequest{Context: f.wire(2)})
	if err != nil || len(inbox.Messages) != 1 || !proto.Equal(inbox.Messages[0].Envelope, old.Envelope) {
		t.Fatal("sealed receive lost", err)
	}
	d, _ := boundary.EpochEnvelopeDigest(old.Envelope)
	if _, err = f.v2.AcknowledgeMessages(f.ownerContext(t, 2), &pb.AcknowledgeMessagesRequest{Context: f.wire(2), Messages: []*pb.Acknowledgement{{MessageId: old.Envelope.MessageId, EnvelopeDigest: d[:], Disposition: 1}}}); err != nil {
		t.Fatal("sealed ACK", err)
	}
	statusReq := &pb.GetMessageStatusesRequest{Context: f.wire(1), Messages: []*pb.MessageReference{{MessageId: old.Envelope.MessageId, EnvelopeDigest: d[:]}}}
	observed, err := f.v2.GetMessageStatuses(f.ownerContext(t, 1), statusReq)
	if err != nil || boundary.ValidateEpochStatusResponse(statusReq, observed) != nil || observed.Messages[0].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED {
		t.Fatal("v2 status", err)
	}
}
func mustSendBinding(t *testing.T, e *v2.Envelope) *pb.EpochSendBinding {
	t.Helper()
	v, err := boundary.EpochSendBinding(e)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestE2EEEpochRPCAuthAndV1RetirementTLSPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	r := &v2.GetPendingMessagesRequest{Context: f.wire(1)}
	for _, ctx := range []context.Context{t.Context(), f.ctx(t, f.aliceToken, nil), f.ctx(t, f.aliceToken, f.bobRaw), f.ctx(t, f.bobToken, f.aliceRaw)} {
		_, err := f.v2.GetPendingMessages(ctx, r)
		rpcCode(t, err, codes.Unauthenticated)
	}
	wrong := proto.Clone(r).(*v2.GetPendingMessagesRequest)
	wrong.Context.ProtocolVersion = 1
	_, err := f.v2.GetPendingMessages(f.ownerContext(t, 1), wrong)
	rpcCode(t, err, codes.FailedPrecondition)
	wrong = proto.Clone(r).(*v2.GetPendingMessagesRequest)
	wrong.Context.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	_, err = f.v2.GetPendingMessages(f.ownerContext(t, 1), wrong)
	rpcCode(t, err, codes.InvalidArgument)
	oversized := f.message(0)
	oversized.Envelope.Ciphertext = make([]byte, boundary.MaxRequestBytes)
	_, err = f.v2.SendMessage(f.ownerContext(t, 1), oversized)
	rpcCode(t, err, codes.InvalidArgument)
	// Every old DATA method is closed in the production graph, even with a
	// valid JWT, credential and v1 context. Registry methods above still work.
	old := &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.gen(1)}
	_, err = f.registry.AcquireBundle(f.ownerContext(t, 1), &pb.AcquireBundleRequest{Context: old})
	rpcCode(t, err, codes.FailedPrecondition)
	_, err = f.registry.SendMessage(f.ownerContext(t, 1), &pb.SendMessageRequest{Context: old})
	rpcCode(t, err, codes.FailedPrecondition)
	_, err = f.registry.GetPendingMessages(f.ownerContext(t, 1), &pb.GetPendingMessagesRequest{Context: old})
	rpcCode(t, err, codes.FailedPrecondition)
	_, err = f.registry.AcknowledgeMessages(f.ownerContext(t, 1), &pb.AcknowledgeMessagesRequest{Context: old})
	rpcCode(t, err, codes.FailedPrecondition)
	_, err = f.registry.GetMessageStatuses(f.ownerContext(t, 1), &pb.GetMessageStatusesRequest{Context: old})
	rpcCode(t, err, codes.FailedPrecondition)
	if err = f.auth.Logout(t.Context(), f.aliceToken, false, func(int64, string) {}); err != nil {
		t.Fatal(err)
	}
	_, err = f.v2.GetCapabilities(f.ownerContext(t, 1), &pb.Empty{})
	rpcCode(t, err, codes.Unauthenticated)
	// Replacement invalidates the OTHER old crypto installation without
	// changing Bob's JWT; both route prefixes use the same device barrier.
	raw := bytes.Repeat([]byte{87}, 32)
	hash := service.E2EEDeviceCredentialHash(raw)
	next, err := f.registry.ReplaceOwnDevice(f.ctx(t, f.bobToken, raw), &pb.ReplaceOwnDeviceRequest{ProtocolVersion: 1, ReplacementRequestId: uuid.NewString(), ExpectedGenerationId: f.gen(2), RegistrationId: 16383, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:], CurrentPassword: deviceTestPassword})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.v2.GetPendingEpochRecoveries(f.ownerContext(t, 2), &v2.GetPendingEpochRecoveriesRequest{Context: f.wire(2), Limit: 32})
	rpcCode(t, err, codes.Unauthenticated) // Old raw credential is rejected first.
	_, err = f.v2.GetPendingEpochRecoveries(f.ctx(t, f.bobToken, raw), &v2.GetPendingEpochRecoveriesRequest{Context: f.wire(2), Limit: 32})
	rpcCode(t, err, codes.Unauthenticated) // Credential authenticates the current generation only.
	page, err := f.v2.GetPendingEpochRecoveries(f.ctx(t, f.bobToken, raw), &v2.GetPendingEpochRecoveriesRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: next.ActiveDevice.Reference.GenerationId}, Limit: 32})
	if err != nil || len(page.Proposals) != 0 {
		// Safe diagnostics for a transient session rejection: never log tokens,
		// credentials, account IDs or password material.
		_, parseErr := f.auth.ParseToken(f.bobToken)
		_, sessionErr := f.auth.ValidateSessionContext(t.Context(), f.bobToken)
		_, unlock, deviceErr := f.devices.Authorize(t.Context(), f.bobID, next.ActiveDevice.Reference.GenerationId, raw)
		if unlock != nil {
			unlock()
		}
		t.Logf("replacement access diagnostic: jwt_valid=%t session_valid=%t device_valid=%t", parseErr == nil, sessionErr == nil, deviceErr == nil)
		// Preserve strict production validation; only categorize the failing test
		// token locally. No JWT contents or session identifiers leave this test.
		claims := new(service.SessionClaims)
		_, why := jwt.ParseWithClaims(f.bobToken, claims, func(*jwt.Token) (any, error) { return []byte(deviceTestJWTSecret), nil },
			jwt.WithValidMethods([]string{"HS256"}), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithIssuer("zchat"), jwt.WithAudience("zchat"), jwt.WithStrictDecoding())
		t.Logf("replacement JWT diagnostic: future_iat=%t expired=%t signature=%t", errors.Is(why, jwt.ErrTokenUsedBeforeIssued), errors.Is(why, jwt.ErrTokenExpired), errors.Is(why, jwt.ErrTokenSignatureInvalid))
		if claims.ID != "" {
			var exists, versionMatches, unexpired bool
			dbErr := f.db.QueryRowContext(t.Context(), `SELECT EXISTS(SELECT 1 FROM auth_sessions WHERE user_id=$1 AND id=$2),
                EXISTS(SELECT 1 FROM auth_sessions s JOIN users u ON u.id=s.user_id WHERE u.id=$1 AND s.id=$2 AND s.token_version=u.token_version),
                EXISTS(SELECT 1 FROM auth_sessions WHERE user_id=$1 AND id=$2 AND expires_at>now())`, f.bobID, claims.ID).Scan(&exists, &versionMatches, &unexpired)
			t.Logf("replacement DB diagnostic: query_ok=%t session_exists=%t version_matches=%t unexpired=%t", dbErr == nil, exists, versionMatches, unexpired)
		}
		if claims.IssuedAt != nil {
			t.Logf("replacement clock diagnostic: iat_age_ms=%d", time.Since(claims.IssuedAt.Time).Milliseconds())
		}
		t.Fatal("current replacement installation lost access", err)
	}
}

func TestE2EEEpochRPCFailuresNeverInventFence(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, domain.ErrE2EERecoveryNotFound, domain.ErrE2EEEpochInactive, errors.New("private SQL diagnostic"), &domain.E2EEEpochSealed{}, (*domain.E2EEEpochSealed)(nil), (*domain.E2EERecoveryConflict)(nil)} {
		mapped := epochSendStatus(err, &v2.SendMessageRequest{})
		if status.Code(mapped) == codes.OK || len(status.Convert(mapped).Details()) != 0 || status.Convert(mapped).Message() == "private SQL diagnostic" {
			t.Fatal("invented fence/private diagnostic", mapped)
		}
	}
	if _, err := epochReply(&v2.PendingMessages{Messages: []*v2.Delivery{{Envelope: &v2.Envelope{Ciphertext: make([]byte, boundary.MaxResponseBytes)}}}}, nil); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded RPC response", err)
	}
	if _, err := epochReply((*v2.SendReceipt)(nil), nil); status.Code(err) != codes.Unavailable {
		t.Fatal("nil result", err)
	}
	// A generic JWT/principal without a checked current device cannot call v2.
	s := NewE2EEEpochServer(nil)
	ctx := context.WithValue(t.Context(), e2eePrincipalKey{}, e2eePrincipal{userID: 1})
	_, err := s.GetPendingMessages(ctx, &v2.GetPendingMessagesRequest{})
	rpcCode(t, err, codes.Unauthenticated)
}
