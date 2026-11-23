package grpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestE2EEFailureReasons(t *testing.T) {
	for _, tc := range []struct {
		err    error
		code   codes.Code
		reason pb.FailureReason
	}{
		{domain.ErrInvalidSession, codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_SESSION_INVALID},
		{domain.ErrUserNotFound, codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_SESSION_INVALID},
		{domain.ErrE2EEDeviceCredential, codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_DEVICE_CREDENTIAL_INVALID},
		{domain.ErrInvalidCredentials, codes.Unauthenticated, pb.FailureReason_FAILURE_REASON_PASSWORD_INVALID},
		{domain.ErrE2EEGenerationChanged, codes.FailedPrecondition, pb.FailureReason_FAILURE_REASON_GENERATION_CHANGED},
		{domain.ErrE2EEAcquisitionExpired, codes.FailedPrecondition, pb.FailureReason_FAILURE_REASON_ACQUISITION_EXPIRED},
	} {
		t.Run(tc.err.Error(), func(t *testing.T) {
			// Wrapping must not leak diagnostics or break stable classification.
			s := status.Convert(e2eeStatus(fmt.Errorf("private diagnostic: %w", tc.err)))
			if s.Code() != tc.code || len(s.Details()) != 1 {
				t.Fatalf("wrong code/details: %v", s)
			}
			detail, ok := s.Details()[0].(*pb.Failure)
			if !ok || detail.Reason != tc.reason {
				t.Fatalf("wrong typed reason: %v", s.Details())
			}
			if s.Message() == "" || s.Message() == "private diagnostic: "+tc.err.Error() {
				t.Fatal("expected sanitized message")
			}
		})
	}
}

func TestE2EEGenericFailuresDoNotClaimRecoveryReason(t *testing.T) {
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, errors.New("private storage error"), domain.ErrE2EERequestConflict} {
		s := status.Convert(e2eeStatus(err))
		if len(s.Details()) != 0 || s.Message() == "private storage error" {
			t.Fatalf("unexpected reason or private diagnostic: %v", s)
		}
	}
	if e2eeStatus(nil) != nil {
		t.Fatal("nil must stay nil")
	}
}
