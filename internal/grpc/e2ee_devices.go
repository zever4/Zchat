package grpc

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/e2ee/wakeup"
	"github.com/zever4/Zchat/internal/service"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Experimental registry, key, conversation and durable inbox API. Desktop UI
// acceptance remains pending; core delivery alone is not messaging readiness.
type E2EEDeviceServer struct {
	pb.UnimplementedMessengerServer
	devices       *service.E2EEDeviceService
	keys          *service.E2EEPreKeyService
	conversations *service.E2EEConversationService
	messages      *service.E2EEMessageService
	wakeups       *wakeup.Hub
	prototypeData bool // Enabled ONLY by the explicit prototype constructor in _test.go.
}

// Configure before registering/serving; never swap a live hub.
func (s *E2EEDeviceServer) WithWakeups(hub *wakeup.Hub) *E2EEDeviceServer {
	s.wakeups = hub
	return s
}

func NewE2EEDeviceServer(devices *service.E2EEDeviceService, keys *service.E2EEPreKeyService, conversations *service.E2EEConversationService, messages *service.E2EEMessageService) *E2EEDeviceServer {
	return &E2EEDeviceServer{devices: devices, keys: keys, conversations: conversations, messages: messages}
}
func deviceProto(d *domain.E2EEDevice) *pb.Device {
	if d == nil {
		return nil
	}
	return &pb.Device{Reference: &pb.DeviceRef{UserId: d.UserUUID, DeviceId: d.DeviceID, GenerationId: d.GenerationID}, RegistrationId: d.RegistrationID, IdentityKey: d.IdentityKey, PublicMaterialReady: d.PublicMaterialReady, PublicMaterialRevision: d.PublicMaterialRevision}
}
func (s *E2EEDeviceServer) UploadPreKeys(ctx context.Context, req *pb.UploadPreKeysRequest) (*pb.UploadReceipt, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if s.keys == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE key publication is not ready")
	}
	result, err := s.keys.Publish(ctx, uid, req)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return &pb.UploadReceipt{BatchId: result.BatchID, BatchDigest: result.Digest[:], PublicationRevision: result.Revision}, nil
}
func (s *E2EEDeviceServer) GetPreKeyInventory(ctx context.Context, req *pb.GetPreKeyInventoryRequest) (*pb.PreKeyInventory, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if s.keys == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE key inventory is not ready")
	}
	result, err := s.keys.Inventory(ctx, uid, req)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return result, nil
}
func (s *E2EEDeviceServer) AcquireBundle(ctx context.Context, req *pb.AcquireBundleRequest) (*pb.AcquiredBundle, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if !s.prototypeData {
		return nil, retiredE2EEData()
	}
	if s.keys == nil {
		return nil, status.Error(codes.FailedPrecondition, "E2EE key acquisition is not ready")
	}
	result, err := s.keys.Acquire(ctx, uid, req)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return result, nil
}
func owner(ctx context.Context) (int64, error) {
	p, ok := ctx.Value(e2eePrincipalKey{}).(e2eePrincipal)
	if !ok || p.userID <= 0 {
		return 0, status.Error(codes.Unauthenticated, "missing E2EE principal")
	}
	return p.userID, nil
}
func (s *E2EEDeviceServer) GetCapabilities(ctx context.Context, _ *pb.Empty) (*pb.Capabilities, error) {
	if _, err := owner(ctx); err != nil {
		return nil, err
	}
	return &pb.Capabilities{ProtocolVersion: boundary.ProtocolVersion, CryptoSuites: []pb.CryptoSuite{pb.CryptoSuite_CRYPTO_SUITE_SIGNAL_PQ_1}, MaxActiveDevices: 1,
		Limits: &pb.Limits{MaxRequestBytes: boundary.MaxRequestBytes, MaxTextBytes: boundary.MaxTextBytes, MaxKeysPerKindPerUpload: boundary.MaxKeysPerKind, MaxAcknowledgements: boundary.MaxAcknowledgements, MaxInboxMessages: boundary.MaxInboxMessages, MaxResponseBytes: boundary.MaxResponseBytes}, DeviceRegistrationAvailable: true, MessagingAvailable: false}, nil
}
func (s *E2EEDeviceServer) GetOwnDevice(ctx context.Context, _ *pb.Empty) (*pb.OwnDeviceState, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	account, d, err := s.devices.Own(ctx, uid)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return &pb.OwnDeviceState{UserId: account, ActiveDevice: deviceProto(d)}, nil
}
func (s *E2EEDeviceServer) RegisterDevice(ctx context.Context, req *pb.RegisterDeviceRequest) (*pb.OwnDeviceState, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if err := boundary.ValidateRegistration(req); err != nil {
		return nil, e2eeStatus(err)
	}
	raw, err := e2eeCredential(ctx)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	result, err := s.devices.Register(ctx, uid, domain.E2EEDeviceRegistration{RequestID: req.RegistrationRequestId, RegistrationID: req.RegistrationId, IdentityKey: req.IdentityKey, CredentialHash: req.CredentialHash}, raw)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return &pb.OwnDeviceState{UserId: result.Device.UserUUID, ActiveDevice: deviceProto(result.Device)}, nil
}
func (s *E2EEDeviceServer) ReplaceOwnDevice(ctx context.Context, req *pb.ReplaceOwnDeviceRequest) (*pb.OwnDeviceState, error) {
	uid, err := owner(ctx)
	if err != nil {
		return nil, err
	}
	if err := boundary.ValidateReplacement(req); err != nil {
		return nil, e2eeStatus(err)
	}
	raw, err := e2eeCredential(ctx)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	result, err := s.devices.Replace(ctx, uid, domain.E2EEDeviceRegistration{RequestID: req.ReplacementRequestId, ExpectedGenerationID: req.ExpectedGenerationId, RegistrationID: req.RegistrationId, IdentityKey: req.IdentityKey, CredentialHash: req.CredentialHash}, req.CurrentPassword, raw)
	if err != nil {
		return nil, e2eeStatus(err)
	}
	return &pb.OwnDeviceState{UserId: result.Device.UserUUID, ActiveDevice: deviceProto(result.Device)}, nil
}
func (s *E2EEDeviceServer) StreamMessages(_ *pb.StreamMessagesRequest, stream grpcgo.ServerStreamingServer[pb.Delivery]) error {
	if _, err := owner(stream.Context()); err != nil {
		return err
	}
	return status.Error(codes.FailedPrecondition, "E2EE messaging is not ready")
}
