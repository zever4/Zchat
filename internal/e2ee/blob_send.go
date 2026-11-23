package e2ee

import (
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

// ValidateBlobSendFailure binds a compact metadata error to the entire actual
// attachment Send, including its BlobRef through the canonical envelope digest.
// It authorizes neither message settlement nor lease renewal.
func ValidateBlobSendFailure(r *v2.SendMessageRequest, f *v2.BlobSendFailure) error {
	if err := ValidateEpochSend(r); err != nil {
		return err
	}
	if r.Envelope.ProtocolVersion != AttachmentEnvelopeVersion || !recoveryKnown(f) {
		return invalid("blob Send failure binding")
	}
	switch f.Reason {
	case v2.BlobSendFailureReason_BLOB_SEND_FAILURE_REASON_NOT_READY, v2.BlobSendFailureReason_BLOB_SEND_FAILURE_REASON_LEASE_EXPIRED:
	default:
		return invalid("blob Send failure reason")
	}
	expected, err := EpochSendBinding(r.Envelope)
	if err != nil || !proto.Equal(f.Binding, expected) || proto.Size(f) > MaxResponseBytes {
		return invalid("blob Send failure binding")
	}
	return nil
}
