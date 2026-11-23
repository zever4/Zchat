package repository

import (
	"context"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

// Retained epochs remain queryable; ACK outranks an older expiry removal. No
// epoch activity gate and no conversion of prototype receipts without an epoch.
func (r *PgE2EEDeliveryRepository) StatusesV2(ctx context.Context, uid int64, req *pb.GetMessageStatusesRequest) (*v2.MessageStatuses, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochStatusRequest(req); err != nil {
		return nil, err
	}
	observations, err := r.statusesPrepared(ctx, uid, req.Context.DeviceGenerationId, req.Messages, true)
	if err != nil {
		return nil, err
	}
	result := &v2.MessageStatuses{}
	for _, v := range observations {
		r := v.Receipt
		result.Messages = append(result.Messages, &v2.MessageStatus{Receipt: &v2.SendReceipt{MessageId: r.MessageID, EnvelopeDigest: r.Digest[:], StoredAtUnixMs: r.StoredAt.UnixMilli(), ExpiresAtUnixMs: r.ExpiresAt.UnixMilli(), CryptoEpoch: r.CryptoEpoch}, State: v.State, ObservedAtUnixMs: v.ObservedAt.UnixMilli()})
	}
	if boundary.ValidateEpochStatusResponse(req, result) != nil {
		return nil, domain.ErrE2EEMessageState
	}
	return result, nil
}
