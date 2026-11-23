package e2ee

import (
	"math"

	"github.com/zever4/Zchat/internal/domain"
)

func ValidateBlobReservation(r *domain.E2EEBlobReservationRequest) error {
	if r == nil {
		return invalid("blob reservation")
	}
	if err := epochContext(r.Context); err != nil {
		return err
	}
	if err := epochValue(r.CryptoEpoch); err != nil {
		return err
	}
	if err := epochDevices(r.Sender, r.Recipient); err != nil {
		return err
	}
	if err := ValidateBlobRef(r.Blob); err != nil {
		return err
	}
	if !uuid4(r.MessageID) || !uuid4(r.ConversationID) || !uuid4(r.OperationID) ||
		r.Context.DeviceGenerationId != r.Sender.GenerationId || r.ExpectedRevision >= math.MaxInt64 ||
		(r.ExpectedRevision == 0 && r.ExpectedAttemptID != "") ||
		(r.ExpectedRevision != 0 && !uuid4(r.ExpectedAttemptID)) {
		return invalid("blob reservation binding")
	}
	return nil
}

func ValidateBlobAttempt(r *domain.E2EEBlobAttemptRequest) error {
	if r == nil {
		return invalid("blob attempt")
	}
	if err := ValidateBlobReservation(r.Reservation); err != nil {
		return err
	}
	if r.Revision == 0 || r.Revision > math.MaxInt64 || !uuid4(r.AttemptID) {
		return invalid("blob attempt selection")
	}
	return nil
}
func ValidateBlobWrite(r *domain.E2EEBlobWriteRequest) error {
	if r == nil {
		return invalid("blob writer")
	}
	if err := ValidateBlobAttempt(r.Attempt); err != nil {
		return err
	}
	if !uuid4(r.WriterID) || !uuid4(r.StorageID) {
		return invalid("blob writer identifier")
	}
	return nil
}

func ValidateBlobStorage(id string) error {
	if !uuid4(id) {
		return invalid("blob storage identity")
	}
	return nil
}
func ValidateBlobCleanup(r *domain.E2EEBlobCleanupRequest) error {
	if r == nil || !uuid4(r.StorageID) || !uuid4(r.MessageID) || !uuid4(r.AttemptID) || r.Revision == 0 || r.Revision > math.MaxInt64 {
		return invalid("blob cleanup binding")
	}
	if r.ExpectedWriterID != "" && !uuid4(r.ExpectedWriterID) {
		return invalid("blob cleanup writer")
	}
	switch r.Reason {
	case domain.E2EEBlobCleanupExpired, domain.E2EEBlobCleanupInactiveScope:
	case domain.E2EEBlobCleanupFailedWriter:
		if r.ExpectedWriterID == "" {
			return invalid("failed writer identity")
		}
	default:
		return invalid("blob cleanup reason")
	}
	return nil
}
