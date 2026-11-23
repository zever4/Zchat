package grpc

import (
	"context"
	"errors"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *E2EEEpochServer) ReserveBlob(ctx context.Context, r *v2.BlobLeaseRequest) (*v2.BlobLease, error) {
	return s.blobControl(ctx, r, false)
}
func (s *E2EEEpochServer) ObserveBlob(ctx context.Context, r *v2.BlobLeaseRequest) (*v2.BlobLease, error) {
	return s.blobControl(ctx, r, true)
}

func (s *E2EEEpochServer) blobControl(ctx context.Context, r *v2.BlobLeaseRequest, observe bool) (*v2.BlobLease, error) {
	uid, err := epochOwner(ctx, r.GetContext())
	if err != nil {
		return nil, err
	}
	req, err := boundary.BlobReservationFromWire(r)
	if err != nil {
		return nil, epochStatus(err)
	}
	if s.blobs == nil {
		return nil, epochBlobStatus(domain.ErrE2EEBlobNotReady, r, observe)
	}
	var grant *domain.E2EEBlobReservation
	if observe {
		grant, err = s.blobs.Observe(ctx, uid, req)
	} else {
		grant, err = s.blobs.Reserve(ctx, uid, req)
	}
	if err != nil {
		return nil, epochBlobStatus(err, r, observe)
	}
	return blobLeaseReply(r, grant)
}

// Only the Observe method can carry committed absence. Transport/commit/auth
// failures never become absence or permission to replace a message/key/session.
func epochBlobStatus(err error, req *v2.BlobLeaseRequest, observe bool) error {
	if err == nil {
		return nil
	}
	var code codes.Code
	var reason v2.BlobFailureReason
	var message string
	switch {
	case observe && errors.Is(err, domain.ErrE2EEBlobOperationNotFound):
		code, reason, message = codes.NotFound, v2.BlobFailureReason_BLOB_FAILURE_REASON_OPERATION_NOT_OBSERVED, "E2EE blob operation not observed"
	case errors.Is(err, domain.ErrE2EEBlobCAS):
		code, reason, message = codes.Aborted, v2.BlobFailureReason_BLOB_FAILURE_REASON_CAS_CHANGED, "E2EE blob reservation head changed"
	case errors.Is(err, domain.ErrE2EEBlobLeaseActive):
		code, reason, message = codes.FailedPrecondition, v2.BlobFailureReason_BLOB_FAILURE_REASON_ACTIVE_LEASE, "E2EE blob lease is still active"
	case errors.Is(err, domain.ErrE2EEMessageConflict):
		code, reason, message = codes.AlreadyExists, v2.BlobFailureReason_BLOB_FAILURE_REASON_BINDING_CONFLICT, "E2EE blob binding conflicts with saved data"
	case errors.Is(err, domain.ErrE2EEBlobCapacity):
		code, reason, message = codes.ResourceExhausted, v2.BlobFailureReason_BLOB_FAILURE_REASON_CAPACITY, "E2EE blob reservation capacity exhausted"
	case errors.Is(err, domain.ErrE2EEBlobNotReady):
		code, reason, message = codes.FailedPrecondition, v2.BlobFailureReason_BLOB_FAILURE_REASON_STORAGE_UNAVAILABLE, "E2EE blob storage is not available"
	default:
		return epochStatus(err)
	}
	if _, err := boundary.BlobReservationFromWire(req); err != nil {
		return status.Error(codes.Unavailable, "E2EE blob failure detail unavailable")
	}
	detail := &v2.BlobFailure{Reason: reason, Request: proto.Clone(req).(*v2.BlobLeaseRequest)}
	if proto.Size(detail) > boundary.MaxResponseBytes {
		return status.Error(codes.Unavailable, "E2EE blob failure detail unavailable")
	}
	s, e := status.New(code, message).WithDetails(detail)
	if e != nil {
		return status.Error(codes.Unavailable, "E2EE blob failure detail unavailable")
	}
	return s.Err()
}

func blobLeaseReply(req *v2.BlobLeaseRequest, g *domain.E2EEBlobReservation) (*v2.BlobLease, error) {
	if g == nil {
		return nil, e2eeStatus(domain.ErrE2EEMessageState)
	}
	states := map[string]v2.BlobLeaseState{
		"reserved": v2.BlobLeaseState_BLOB_LEASE_STATE_RESERVED, "writing": v2.BlobLeaseState_BLOB_LEASE_STATE_WRITING,
		"ready": v2.BlobLeaseState_BLOB_LEASE_STATE_READY, "deleting": v2.BlobLeaseState_BLOB_LEASE_STATE_DELETING,
		"reclaimed": v2.BlobLeaseState_BLOB_LEASE_STATE_RECLAIMED, "linked": v2.BlobLeaseState_BLOB_LEASE_STATE_LINKED,
	}
	r := &v2.BlobLease{MessageId: g.MessageID, OperationId: g.OperationID, AttemptId: g.AttemptID, Revision: g.Revision,
		ReservedAtUnixMs: g.ReservedAt.UnixMilli(), ExpiresAtUnixMs: g.ExpiresAt.UnixMilli(), ObservedAtUnixMs: g.ObservedAt.UnixMilli(), State: states[g.State], Expired: g.Expired}
	if boundary.ValidateBlobLease(req, r) != nil {
		return nil, e2eeStatus(domain.ErrE2EEMessageState)
	}
	return epochReply(r, nil)
}
