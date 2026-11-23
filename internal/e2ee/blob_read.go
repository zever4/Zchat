package e2ee

import "github.com/zever4/Zchat/internal/domain"

// Shape validation, not download authorization. Recipient-only routing is
// deliberate: the sender retains its own protected preparation/file spool.
func ValidateBlobReadRequest(r *domain.E2EEBlobReadRequest) error {
	if r == nil {
		return invalid("blob read request")
	}
	if err := epochContext(r.Context); err != nil {
		return err
	}
	if err := ValidateEpochSendBinding(r.Binding); err != nil {
		return err
	}
	if r.Binding.ProtocolVersion != AttachmentEnvelopeVersion || r.Context.DeviceGenerationId != r.Binding.Recipient.GenerationId {
		return invalid("blob read recipient binding")
	}
	return ValidateBlobRef(r.Blob)
}
