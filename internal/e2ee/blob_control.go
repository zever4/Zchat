package e2ee

import (
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	"google.golang.org/protobuf/proto"
)

// Convert only validated public metadata. Owned copies prevent a caller from
// mutating route/ref/context while a short repository operation is in progress.
func BlobReservationFromWire(r *v2.BlobLeaseRequest) (*domain.E2EEBlobReservationRequest, error) {
	if !recoveryKnown(r) {
		return nil, invalid("blob control request")
	}
	if r.Context == nil || r.Sender == nil || r.Recipient == nil || r.Blob == nil || r.CryptoEpoch == nil {
		return nil, invalid("blob control presence")
	}
	epoch := *r.CryptoEpoch
	d := &domain.E2EEBlobReservationRequest{Context: proto.Clone(r.Context).(*pb.ClientContext), MessageID: r.MessageId, ConversationID: r.ConversationId, CryptoEpoch: &epoch,
		Sender: proto.Clone(r.Sender).(*pb.DeviceRef), Recipient: proto.Clone(r.Recipient).(*pb.DeviceRef), Blob: proto.Clone(r.Blob).(*v2.BlobRef), OperationID: r.OperationId, ExpectedRevision: r.ExpectedRevision, ExpectedAttemptID: r.ExpectedAttemptId}
	if err := ValidateBlobReservation(d); err != nil {
		return nil, err
	}
	return d, nil
}

// ValidateBlobLease binds a point-in-time observation to its exact original
// operation. A Ready/Linked observation is never a message persistence receipt.
func ValidateBlobLease(req *v2.BlobLeaseRequest, g *v2.BlobLease) error {
	if _, err := BlobReservationFromWire(req); err != nil {
		return err
	}
	if !recoveryKnown(g) || g.MessageId != req.MessageId || g.OperationId != req.OperationId ||
		!uuid4(g.AttemptId) || g.Revision != req.ExpectedRevision+1 ||
		g.ReservedAtUnixMs <= 0 || g.ExpiresAtUnixMs <= g.ReservedAtUnixMs ||
		g.ExpiresAtUnixMs-g.ReservedAtUnixMs != domain.E2EEBlobStagingLease.Milliseconds() ||
		g.ObservedAtUnixMs < g.ReservedAtUnixMs ||
		g.Expired != (g.ObservedAtUnixMs >= g.ExpiresAtUnixMs) {
		return invalid("blob lease observation")
	}
	switch g.State {
	case v2.BlobLeaseState_BLOB_LEASE_STATE_RESERVED, v2.BlobLeaseState_BLOB_LEASE_STATE_WRITING,
		v2.BlobLeaseState_BLOB_LEASE_STATE_READY, v2.BlobLeaseState_BLOB_LEASE_STATE_DELETING,
		v2.BlobLeaseState_BLOB_LEASE_STATE_RECLAIMED, v2.BlobLeaseState_BLOB_LEASE_STATE_LINKED:
		return nil
	default:
		return invalid("blob lease state")
	}
}
