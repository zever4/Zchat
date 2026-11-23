package service

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type E2EEEpochMessages interface {
	SendV2(context.Context, int64, *v2.SendMessageRequest) (*v2.SendReceipt, error)
	PendingV2(context.Context, int64, *pb.ClientContext) ([]domain.E2EEEpochDelivery, error)
	AcknowledgeV2(context.Context, int64, *pb.AcknowledgeMessagesRequest) error
	StatusesV2(context.Context, int64, *pb.GetMessageStatusesRequest) (*v2.MessageStatuses, error)
}
type E2EEEpochKeys interface {
	AcquireV2(context.Context, int64, *v2.AcquireBundleRequest) (*v2.AcquiredBundle, error)
}
type E2EEEpochRecoveries interface {
	Propose(context.Context, int64, string, *pb.SignedRecoveryControl) (*domain.E2EERecovery, error)
	Decide(context.Context, int64, string, *pb.SignedRecoveryControl) (*domain.E2EERecovery, error)
	Get(context.Context, int64, string, string, string) (*domain.E2EERecovery, error)
	PendingRecoveries(context.Context, int64, string, string, int) (*domain.E2EEPendingRecoveries, error)
	EpochCatalog(context.Context, int64, string, string, *uint64, int) (*domain.E2EEEpochCatalog, error)
}

// Explicit opt-in owned by the same lifetime as blob HTTP and metadata. The
// returned context is canceled at storage shutdown. The owner waits for release
// before closing storage.
// Metadata support or a configured repository alone does not admit file Send.
type E2EEAttachmentSendAdmission interface {
	BeginAttachmentSend(context.Context) (context.Context, func(), error)
}
type E2EEEpochService struct {
	messages    E2EEEpochMessages
	keys        E2EEEpochKeys
	recoveries  E2EEEpochRecoveries
	attachments E2EEAttachmentSendAdmission
}

// Configure before serving; this is not a hot-swappable feature flag.
func (s *E2EEEpochService) WithAttachmentAdmission(a E2EEAttachmentSendAdmission) *E2EEEpochService {
	s.attachments = a
	return s
}

func NewE2EEEpochService(messages E2EEEpochMessages, keys E2EEEpochKeys, recoveries E2EEEpochRecoveries) *E2EEEpochService {
	return &E2EEEpochService{messages: messages, keys: keys, recoveries: recoveries}
}
func (s *E2EEEpochService) Send(ctx context.Context, uid int64, r *v2.SendMessageRequest) (*v2.SendReceipt, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochSend(r); err != nil {
		return nil, err
	}
	if r.Envelope.ProtocolVersion == 3 {
		if s.attachments == nil {
			return nil, boundary.ErrUnsupportedVersion
		}
		owned, release, err := s.attachments.BeginAttachmentSend(ctx)
		if err != nil {
			return nil, err
		}
		defer release()
		ctx = owned
	}
	return s.messages.SendV2(ctx, uid, r)
}
func (s *E2EEEpochService) Acquire(ctx context.Context, uid int64, r *v2.AcquireBundleRequest) (*v2.AcquiredBundle, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochAcquisition(r); err != nil {
		return nil, err
	}
	return s.keys.AcquireV2(ctx, uid, r)
}
func (s *E2EEEpochService) Pending(ctx context.Context, uid int64, r *v2.GetPendingMessagesRequest) ([]domain.E2EEEpochDelivery, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochPendingRequest(r); err != nil {
		return nil, err
	}
	return s.messages.PendingV2(ctx, uid, r.Context)
}
func (s *E2EEEpochService) Acknowledge(ctx context.Context, uid int64, r *pb.AcknowledgeMessagesRequest) error {
	if uid <= 0 {
		return domain.ErrInvalidInput
	}
	return s.messages.AcknowledgeV2(ctx, uid, r)
}
func (s *E2EEEpochService) Statuses(ctx context.Context, uid int64, r *pb.GetMessageStatusesRequest) (*v2.MessageStatuses, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochStatusRequest(r); err != nil {
		return nil, err
	}
	return s.messages.StatusesV2(ctx, uid, r)
}
func (s *E2EEEpochService) RecoveryCommand(ctx context.Context, uid int64, r *v2.RecoveryCommandRequest, propose bool) (*domain.E2EERecovery, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochRecoveryCommand(r, propose); err != nil {
		return nil, err
	}
	if propose {
		return s.recoveries.Propose(ctx, uid, r.Context.DeviceGenerationId, r.Command)
	}
	return s.recoveries.Decide(ctx, uid, r.Context.DeviceGenerationId, r.Command)
}
func (s *E2EEEpochService) Recovery(ctx context.Context, uid int64, r *v2.GetEpochRecoveryRequest) (*domain.E2EERecovery, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochRecoveryGet(r); err != nil {
		return nil, err
	}
	return s.recoveries.Get(ctx, uid, r.Context.DeviceGenerationId, r.ConversationId, r.ProposalId)
}
func (s *E2EEEpochService) PendingRecoveries(ctx context.Context, uid int64, r *v2.GetPendingEpochRecoveriesRequest) (*domain.E2EEPendingRecoveries, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochRecoveryPending(r); err != nil {
		return nil, err
	}
	return s.recoveries.PendingRecoveries(ctx, uid, r.Context.DeviceGenerationId, r.AfterConversationId, int(r.Limit))
}
func (s *E2EEEpochService) Catalog(ctx context.Context, uid int64, r *v2.GetConversationEpochsRequest) (*domain.E2EEEpochCatalog, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochCatalogRequest(r); err != nil {
		return nil, err
	}
	return s.recoveries.EpochCatalog(ctx, uid, r.Context.DeviceGenerationId, r.ConversationId, r.AfterEpoch, int(r.Limit))
}
