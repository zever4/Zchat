package e2ee

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
)

const BlobUploadPattern = "PUT /api/e2ee/blobs/{attempt_id}"
const BlobUploadMetadataHeader = "X-Zchat-Blob-Metadata"
const BlobDeviceCredentialHeader = "X-Zchat-Device-Credential"
const MaxBlobUploadMetadataHeader = 4096

// V1 is a canonical JSON object encoded as unpadded base64url in one header.
// Counter strings preserve the complete integer range across native/JS clients.
// All fields are public route/ref metadata, never file keys, names or MIME.
// Field order/encoding is part of V1; changed formats require a new version.
type blobUploadMetadata struct {
	Version             uint32 `json:"v"`
	MessageID           string `json:"message_id"`
	ConversationID      string `json:"conversation_id"`
	CryptoEpoch         uint64 `json:"crypto_epoch,string"`
	SenderUserID        string `json:"sender_user_id"`
	SenderGenerationID  string `json:"sender_generation_id"`
	RecipientUserID     string `json:"recipient_user_id"`
	RecipientGeneration string `json:"recipient_generation_id"`
	BlobID              string `json:"blob_id"`
	CiphertextBytes     uint64 `json:"ciphertext_bytes,string"`
	SHA256              string `json:"sha256"`
	OperationID         string `json:"operation_id"`
	ExpectedRevision    uint64 `json:"expected_revision,string"`
	ExpectedAttemptID   string `json:"expected_attempt_id"`
	Revision            uint64 `json:"revision,string"`
	AttemptID           string `json:"attempt_id"`
}

func EncodeBlobUploadMetadata(r *domain.E2EEBlobAttemptRequest) (string, error) {
	if err := ValidateBlobAttempt(r); err != nil {
		return "", err
	}
	s := r.Reservation
	w := blobUploadMetadata{Version: 1, MessageID: s.MessageID, ConversationID: s.ConversationID,
		CryptoEpoch: *s.CryptoEpoch, SenderUserID: s.Sender.UserId, SenderGenerationID: s.Sender.GenerationId,
		RecipientUserID: s.Recipient.UserId, RecipientGeneration: s.Recipient.GenerationId, BlobID: s.Blob.BlobId,
		CiphertextBytes: s.Blob.CiphertextBytes, SHA256: hex.EncodeToString(s.Blob.Sha256), OperationID: s.OperationID,
		ExpectedRevision: s.ExpectedRevision, ExpectedAttemptID: s.ExpectedAttemptID, Revision: r.Revision, AttemptID: r.AttemptID}
	data, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(data)
	if len(encoded) > MaxBlobUploadMetadataHeader {
		return "", invalid("blob upload metadata length")
	}
	return encoded, nil
}

func DecodeBlobUploadMetadata(header, attemptID string) (*domain.E2EEBlobAttemptRequest, error) {
	if header == "" || len(header) > MaxBlobUploadMetadataHeader {
		return nil, invalid("blob upload metadata length")
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(header)
	if err != nil {
		return nil, invalid("blob upload metadata encoding")
	}
	var w blobUploadMetadata
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&w); err != nil {
		return nil, invalid("blob upload metadata JSON")
	}
	canonical, err := json.Marshal(w)
	if err != nil || !bytes.Equal(data, canonical) || base64.RawURLEncoding.EncodeToString(data) != header || w.Version != 1 || w.AttemptID != attemptID {
		return nil, invalid("blob upload metadata binding")
	}
	hash, err := hex.DecodeString(w.SHA256)
	if err != nil || hex.EncodeToString(hash) != w.SHA256 {
		return nil, invalid("blob upload digest")
	}
	r := &domain.E2EEBlobAttemptRequest{AttemptID: w.AttemptID, Revision: w.Revision, Reservation: &domain.E2EEBlobReservationRequest{
		Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: w.SenderGenerationID}, MessageID: w.MessageID, ConversationID: w.ConversationID, CryptoEpoch: &w.CryptoEpoch,
		Sender: &pb.DeviceRef{UserId: w.SenderUserID, GenerationId: w.SenderGenerationID, DeviceId: 1}, Recipient: &pb.DeviceRef{UserId: w.RecipientUserID, GenerationId: w.RecipientGeneration, DeviceId: 1},
		Blob: &v2.BlobRef{FormatVersion: 1, BlobId: w.BlobID, CiphertextBytes: w.CiphertextBytes, Sha256: hash}, OperationID: w.OperationID, ExpectedRevision: w.ExpectedRevision, ExpectedAttemptID: w.ExpectedAttemptID}}
	if err = ValidateBlobAttempt(r); err != nil {
		return nil, err
	}
	return r, nil
}
