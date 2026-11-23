package e2ee

import (
	"bytes"
	"math"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

const MaxFileStatuses = 32
const FileStatusFormat = 1
const FileRetentionMilliseconds int64 = 720 * 60 * 60 * 1000

// Shape/binding only; authentication, receipt existence and COMMIT are separate.
func ValidateFileStatusRequest(r *v2.GetFileStatusesRequest) error {
	if !recoveryKnown(r) || epochContext(r.Context) != nil || len(r.Files) < 1 || len(r.Files) > MaxFileStatuses || proto.Size(r) > MaxRequestBytes {
		return invalid("file status batch")
	}
	ids := map[string]bool{}
	sender := ""
	for _, ref := range r.Files {
		if !recoveryKnown(ref) || ValidateEpochSendBinding(ref.Binding) != nil || ref.Binding.ProtocolVersion != AttachmentEnvelopeVersion || ValidateBlobRef(ref.Blob) != nil {
			return invalid("file status reference")
		}
		own := ref.Binding.Sender
		if ids[ref.Binding.MessageId] || own.GenerationId != r.Context.DeviceGenerationId || sender != "" && sender != own.UserId {
			return invalid("file status sender or duplicate")
		}
		ids[ref.Binding.MessageId] = true
		sender = own.UserId
	}
	return nil
}

func ValidateFileStatuses(r *v2.GetFileStatusesRequest, response *v2.FileStatuses) error {
	if err := ValidateFileStatusRequest(r); err != nil {
		return err
	}
	if !recoveryKnown(response) || len(response.Files) != len(r.Files) || proto.Size(response) > MaxResponseBytes {
		return invalid("file status response")
	}
	for i, s := range response.Files {
		if !recoveryKnown(s) || s.FormatVersion != FileStatusFormat || !proto.Equal(s.Reference, r.Files[i]) || !recoveryKnown(s.Receipt) {
			return invalid("file status response binding")
		}
		b, receipt := r.Files[i].Binding, s.Receipt
		stored := receipt.StoredAtUnixMs
		if receipt.CryptoEpoch == nil || *receipt.CryptoEpoch != b.Epoch || receipt.MessageId != b.MessageId || !bytes.Equal(receipt.EnvelopeDigest, b.EnvelopeDigest) ||
			stored <= 0 || stored > math.MaxInt64-FileRetentionMilliseconds || receipt.ExpiresAtUnixMs != stored+int64(30*24*60*60*1000) ||
			s.ExpiresAtUnixMs != stored+FileRetentionMilliseconds || s.ObservedAtUnixMs < stored || s.ObservedAtUnixMs != response.Files[0].ObservedAtUnixMs {
			return invalid("file status receipt or deadline")
		}
		for _, fact := range []*int64{s.AcknowledgedAtUnixMs, s.RecipientReplacedAtUnixMs} {
			if fact != nil && (*fact < stored || *fact > s.ObservedAtUnixMs) {
				return invalid("file status fact time")
			}
		}
	}
	return nil
}
