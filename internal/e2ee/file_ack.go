package e2ee

import (
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

const MaxFileAcknowledgements = 32

// Shape only, never authorization or evidence that a native cache was saved.
// A late ACK may refer to an accepted old epoch; no current-epoch/lease/clock gate.
func ValidateFileAcknowledgements(r *v2.AcknowledgeFilesRequest) error {
	if !recoveryKnown(r) || epochContext(r.Context) != nil || len(r.Files) < 1 || len(r.Files) > MaxFileAcknowledgements || proto.Size(r) > MaxRequestBytes {
		return invalid("file acknowledgement batch")
	}
	ids := make(map[string]bool, len(r.Files))
	recipient := ""
	for _, a := range r.Files {
		if !recoveryKnown(a) || ValidateEpochSendBinding(a.Binding) != nil || a.Binding.ProtocolVersion != AttachmentEnvelopeVersion || ValidateBlobRef(a.Blob) != nil {
			return invalid("file acknowledgement identity")
		}
		if ids[a.Binding.MessageId] || a.Binding.Recipient.GenerationId != r.Context.DeviceGenerationId || recipient != "" && recipient != a.Binding.Recipient.UserId {
			return invalid("file acknowledgement recipient or duplicate")
		}
		ids[a.Binding.MessageId] = true
		recipient = a.Binding.Recipient.UserId
	}
	return nil
}

func ValidateFileAcknowledgementReceipts(r *v2.AcknowledgeFilesRequest, response *v2.FileAcknowledgements) error {
	if err := ValidateFileAcknowledgements(r); err != nil {
		return err
	}
	if !recoveryKnown(response) || len(response.Files) != len(r.Files) || proto.Size(response) > MaxResponseBytes {
		return invalid("file acknowledgement response")
	}
	for i, receipt := range response.Files {
		if !recoveryKnown(receipt) || receipt.AcknowledgedAtUnixMs <= 0 || !proto.Equal(receipt.Acknowledgement, r.Files[i]) {
			return invalid("file acknowledgement receipt binding")
		}
	}
	return nil
}
