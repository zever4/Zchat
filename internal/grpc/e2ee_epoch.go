package grpc

import (
	"context"
	"errors"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/e2ee/wakeup"
	"github.com/zever4/Zchat/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type E2EEEpochServer struct {
	v2.UnimplementedMessengerServer
	service *service.E2EEEpochService
	wakeups *wakeup.Hub
	blobs   service.E2EEBlobControls
}

func NewE2EEEpochServer(s *service.E2EEEpochService) *E2EEEpochServer {
	return &E2EEEpochServer{service: s}
}
func (s *E2EEEpochServer) WithWakeups(h *wakeup.Hub) *E2EEEpochServer { s.wakeups = h; return s }
func (s *E2EEEpochServer) WithBlobControls(b service.E2EEBlobControls) *E2EEEpochServer {
	s.blobs = b
	return s
}
func retiredE2EEData() error {
	return status.Error(codes.FailedPrecondition, "E2EE data protocol v1 is retired; use protocol v2")
}
func epochOwner(ctx context.Context, c *pb.ClientContext) (int64, error) {
	p, ok := ctx.Value(e2eePrincipalKey{}).(e2eePrincipal)
	if !ok || p.userID <= 0 || p.device == nil {
		return 0, status.Error(codes.Unauthenticated, "missing authenticated E2EE device")
	}
	if err := boundary.ValidateEpochContext(c); err != nil {
		return 0, e2eeStatus(err)
	}
	if p.device.UserID != p.userID || p.device.GenerationID != c.DeviceGenerationId {
		return 0, e2eeStatus(domain.ErrE2EEDeviceCredential)
	}
	return p.userID, nil
}
func epochStatus(err error) error {
	if err == nil {
		return nil
	}
	var conflict *domain.E2EERecoveryConflict
	if errors.As(err, &conflict) && conflict != nil {
		detail := &v2.RecoveryConflict{ActiveEpoch: proto.Uint64(conflict.Epoch), EpochRevision: proto.Uint64(conflict.Revision), PendingProposalId: conflict.PendingProposalID}
		s, e := status.New(codes.Aborted, "E2EE recovery head or pending proposal changed").WithDetails(detail)
		if e == nil {
			return s.Err()
		}
	}
	switch {
	case errors.Is(err, domain.ErrE2EERecoveryNotFound):
		return status.Error(codes.NotFound, "E2EE recovery unavailable")
	case errors.Is(err, domain.ErrE2EERecoveryCapacity):
		return status.Error(codes.ResourceExhausted, "E2EE retained recovery capacity exhausted")
	case errors.Is(err, domain.ErrE2EEEpochInactive):
		return status.Error(codes.FailedPrecondition, "E2EE epoch is not active")
	case errors.Is(err, domain.ErrE2EEBootstrapInitiator):
		return status.Error(codes.FailedPrecondition, "E2EE recovery bootstrap belongs to proposal creator")
	default:
		return e2eeStatus(err)
	}
}

// Only Send may carry this detail, validated against its ACTUAL request. A
// malformed/wrong-context typed error becomes generic failure, never a fence.
func epochSendStatus(err error, req *v2.SendMessageRequest) error {
	var blob *domain.E2EEBlobSendFailure
	if errors.As(err, &blob) {
		return epochBlobSendStatus(blob, req)
	}
	var sealed *domain.E2EEEpochSealed
	if errors.As(err, &sealed) && sealed != nil && req != nil {
		binding, e := boundary.EpochSendBinding(req.Envelope)
		if e == nil && boundary.ValidateEpochSealedFence(sealed.Fence, binding, sealed.Recovery) == nil {
			detail := &v2.EpochSealedFailure{Fence: sealed.Fence, Recovery: sealed.Recovery}
			if proto.Size(detail) <= boundary.MaxResponseBytes {
				s, e := status.New(codes.FailedPrecondition, "E2EE epoch is sealed for this exact message").WithDetails(detail)
				if e == nil {
					return s.Err()
				}
			}
		}
		return e2eeStatus(domain.ErrE2EEMessageState)
	}
	return epochStatus(err)
}

// An expiry proof belongs only to this exact authorized v2 Acquire RPC. Other
// failures and methods cannot produce it through the shared error mapper.
func epochAcquireStatus(err error, req *v2.AcquireBundleRequest) error {
	if !errors.Is(err, domain.ErrE2EEAcquisitionExpired) {
		return epochStatus(err)
	}
	if boundary.ValidateEpochAcquisition(req) != nil {
		return status.Error(codes.Unavailable, "E2EE acquisition expiry detail unavailable")
	}
	detail := &v2.AcquisitionExpiredFailure{Request: proto.Clone(req).(*v2.AcquireBundleRequest)}
	s, e := status.New(codes.FailedPrecondition, "E2EE acquisition expired for this exact request").WithDetails(detail)
	if e != nil {
		return status.Error(codes.Unavailable, "E2EE acquisition expiry detail unavailable")
	}
	return s.Err()
}
func epochReply[T proto.Message](value T, err error) (T, error) {
	var zero T
	if err != nil {
		return zero, epochStatus(err)
	}
	if !value.ProtoReflect().IsValid() {
		return zero, e2eeStatus(domain.ErrE2EEMessageState)
	}
	if proto.Size(value) > boundary.MaxResponseBytes {
		return zero, status.Error(codes.ResourceExhausted, "E2EE response exceeds byte limit")
	}
	return value, nil
}
func (s *E2EEEpochServer) ready() error {
	if s.service == nil {
		return status.Error(codes.FailedPrecondition, "E2EE epoch core is not configured")
	}
	return nil
}
func (s *E2EEEpochServer) GetCapabilities(ctx context.Context, r *pb.Empty) (*v2.Capabilities, error) {
	if _, err := owner(ctx); err != nil {
		return nil, err
	}
	if r == nil || len(r.ProtoReflect().GetUnknown()) != 0 {
		return nil, e2eeStatus(boundary.ErrInvalid)
	}
	return &v2.Capabilities{ProtocolVersion: 2, MaxActiveDevices: 1, CryptoSuites: []pb.CryptoSuite{pb.CryptoSuite_CRYPTO_SUITE_SIGNAL_PQ_1},
		Limits: &pb.Limits{MaxRequestBytes: boundary.MaxRequestBytes, MaxTextBytes: boundary.MaxTextBytes, MaxKeysPerKindPerUpload: boundary.MaxKeysPerKind, MaxAcknowledgements: boundary.MaxAcknowledgements, MaxInboxMessages: boundary.MaxInboxMessages, MaxResponseBytes: boundary.MaxResponseBytes}, MessagingAvailable: false, RecoveryAvailable: false}, nil
}
func (s *E2EEEpochServer) SendMessage(ctx context.Context, r *v2.SendMessageRequest) (*v2.SendReceipt, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	result, err := s.service.Send(ctx, uid, r)
	if err != nil {
		return nil, epochSendStatus(err, r)
	}
	if s.wakeups != nil {
		s.wakeups.Notify(r.Envelope.Recipient.GenerationId)
	}
	return epochReply(result, nil)
}
func (s *E2EEEpochServer) AcquireBundle(ctx context.Context, r *v2.AcquireBundleRequest) (*v2.AcquiredBundle, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	result, err := s.service.Acquire(ctx, uid, r)
	if err != nil {
		return nil, epochAcquireStatus(err, r)
	}
	return epochReply(result, nil)
}
func (s *E2EEEpochServer) GetPendingMessages(ctx context.Context, r *v2.GetPendingMessagesRequest) (*v2.PendingMessages, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	rows, err := s.service.Pending(ctx, uid, r)
	if err != nil {
		return nil, epochStatus(err)
	}
	result := &v2.PendingMessages{}
	for _, v := range rows {
		result.Messages = append(result.Messages, &v2.Delivery{Sequence: v.Sequence, Envelope: v.Envelope, ExpiresAtUnixMs: v.ExpiresAt.UnixMilli(), EnvelopeDigest: v.Digest[:]})
	}
	return epochReply(result, nil)
}
func (s *E2EEEpochServer) AcknowledgeMessages(ctx context.Context, r *pb.AcknowledgeMessagesRequest) (*pb.Empty, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	if err = s.service.Acknowledge(ctx, uid, r); err != nil {
		return nil, epochStatus(err)
	}
	return &pb.Empty{}, nil
}
func (s *E2EEEpochServer) GetMessageStatuses(ctx context.Context, r *pb.GetMessageStatusesRequest) (*v2.MessageStatuses, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	return epochReply(s.service.Statuses(ctx, uid, r))
}
func recoveryReply(v *domain.E2EERecovery, err error) (*v2.RecoveryRecord, error) {
	if err != nil {
		return nil, epochStatus(err)
	}
	if v == nil || v.Outcome == nil || boundary.ValidateRecoveryOutcome(v.Outcome, v.Outcome.Proposal) != nil || v.CreatedAt.UnixMilli() <= 0 || !v.ExpiresAt.After(v.CreatedAt) {
		return nil, e2eeStatus(domain.ErrE2EERecoveryState)
	}
	if (v.Outcome.State == pb.RecoveryState_RECOVERY_STATE_PENDING) != (v.ResolvedAt == nil) || v.ResolvedAt != nil && v.ResolvedAt.Before(v.CreatedAt) {
		return nil, e2eeStatus(domain.ErrE2EERecoveryState)
	}
	r := &v2.RecoveryRecord{Outcome: v.Outcome, CreatedAtUnixMs: v.CreatedAt.UnixMilli(), ExpiresAtUnixMs: v.ExpiresAt.UnixMilli()}
	if v.ResolvedAt != nil {
		r.ResolvedAtUnixMs = proto.Int64(v.ResolvedAt.UnixMilli())
	}
	return epochReply(r, nil)
}
func (s *E2EEEpochServer) ProposeEpochRecovery(ctx context.Context, r *v2.RecoveryCommandRequest) (*v2.RecoveryRecord, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	return recoveryReply(s.service.RecoveryCommand(ctx, uid, r, true))
}
func (s *E2EEEpochServer) DecideEpochRecovery(ctx context.Context, r *v2.RecoveryCommandRequest) (*v2.RecoveryRecord, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	return recoveryReply(s.service.RecoveryCommand(ctx, uid, r, false))
}
func (s *E2EEEpochServer) GetEpochRecovery(ctx context.Context, r *v2.GetEpochRecoveryRequest) (*v2.RecoveryRecord, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	return recoveryReply(s.service.Recovery(ctx, uid, r))
}
func (s *E2EEEpochServer) GetPendingEpochRecoveries(ctx context.Context, r *v2.GetPendingEpochRecoveriesRequest) (*v2.PendingEpochRecoveries, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	page, err := s.service.PendingRecoveries(ctx, uid, r)
	if err != nil {
		return nil, epochStatus(err)
	}
	result := &v2.PendingEpochRecoveries{NextConversationId: page.NextConversationID}
	for _, v := range page.Proposals {
		result.Proposals = append(result.Proposals, &v2.PendingRecoveryReference{ConversationId: v.ConversationID, ProposalId: v.ProposalID, ExpectedEpoch: v.ExpectedEpoch, ExpectedRevision: v.ExpectedRevision, InitiatedByOwnDevice: v.InitiatedByOwn, CreatedAtUnixMs: v.CreatedAt.UnixMilli(), ExpiresAtUnixMs: v.ExpiresAt.UnixMilli()})
	}
	return epochReply(result, nil)
}
func (s *E2EEEpochServer) GetConversationEpochs(ctx context.Context, r *v2.GetConversationEpochsRequest) (*v2.ConversationEpochs, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	if err = s.ready(); err != nil {
		return nil, err
	}
	page, err := s.service.Catalog(ctx, uid, r)
	if err != nil {
		return nil, epochStatus(err)
	}
	result := &v2.ConversationEpochs{ProtocolVersion: 2, Conversation: conversationProto(page.Conversation), ActiveEpoch: proto.Uint64(page.ActiveEpoch), EpochRevision: proto.Uint64(page.Revision), NextAfterEpoch: page.NextAfterEpoch}
	for _, v := range page.Epochs {
		e := &v2.EpochRecord{Epoch: proto.Uint64(v.Epoch), Revision: proto.Uint64(v.Revision), Sealed: v.Sealed, CreatedByProposalId: v.CreatedByProposalID, SealedByProposalId: v.SealedByProposalID, CreatedAtUnixMs: v.CreatedAt.UnixMilli()}
		if v.SealedAt != nil {
			e.SealedAtUnixMs = proto.Int64(v.SealedAt.UnixMilli())
		}
		if v.BootstrapUserID != 0 {
			d := page.Conversation.OwnDevice
			if d.UserID != v.BootstrapUserID {
				d = page.Conversation.PeerDevice
			}
			if d.UserID != v.BootstrapUserID {
				return nil, e2eeStatus(domain.ErrE2EERecoveryState)
			}
			e.BootstrapDevice = deviceProto(d).Reference
		}
		result.Epochs = append(result.Epochs, e)
	}
	return epochReply(result, nil)
}
