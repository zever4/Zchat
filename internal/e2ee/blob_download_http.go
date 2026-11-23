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

// GET also matches HEAD in ServeMux. V1 fixes control2/envelope3/blob1/device1.
const BlobDownloadPattern = "GET /api/e2ee/attachments/{message_id}"
const BlobDownloadMetadataHeader = "X-Zchat-Blob-Read-Metadata"
const MaxBlobDownloadMetadataHeader = 4096

type blobDownloadMetadata struct {
	Version               uint32 `json:"v"`
	MessageID             string `json:"message_id"`
	ConversationID        string `json:"conversation_id"`
	CryptoEpoch           uint64 `json:"crypto_epoch,string"`
	SenderUserID          string `json:"sender_user_id"`
	SenderGenerationID    string `json:"sender_generation_id"`
	RecipientUserID       string `json:"recipient_user_id"`
	RecipientGenerationID string `json:"recipient_generation_id"`
	EnvelopeDigest        string `json:"envelope_digest"`
	BlobID                string `json:"blob_id"`
	CiphertextBytes       uint64 `json:"ciphertext_bytes,string"`
	SHA256                string `json:"sha256"`
}

// Only public binding/ref fields. No attempt/root/path/key/filename/credential.
func EncodeBlobDownloadMetadata(r *domain.E2EEBlobReadRequest) (string, error) {
	if err := ValidateBlobReadRequest(r); err != nil {
		return "", err
	}
	b := r.Binding
	w := blobDownloadMetadata{Version: 1, MessageID: b.MessageId, ConversationID: b.ConversationId, CryptoEpoch: b.Epoch,
		SenderUserID: b.Sender.UserId, SenderGenerationID: b.Sender.GenerationId, RecipientUserID: b.Recipient.UserId, RecipientGenerationID: b.Recipient.GenerationId,
		EnvelopeDigest: hex.EncodeToString(b.EnvelopeDigest), BlobID: r.Blob.BlobId, CiphertextBytes: r.Blob.CiphertextBytes, SHA256: hex.EncodeToString(r.Blob.Sha256)}
	data, err := json.Marshal(w)
	if err != nil {
		return "", err
	}
	header := base64.RawURLEncoding.EncodeToString(data)
	if len(header) > MaxBlobDownloadMetadataHeader {
		return "", invalid("blob download metadata length")
	}
	return header, nil
}
func DecodeBlobDownloadMetadata(header, messageID string) (*domain.E2EEBlobReadRequest, error) {
	if header == "" || len(header) > MaxBlobDownloadMetadataHeader {
		return nil, invalid("blob download metadata length")
	}
	data, err := base64.RawURLEncoding.Strict().DecodeString(header)
	if err != nil {
		return nil, invalid("blob download encoding")
	}
	var w blobDownloadMetadata
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err = d.Decode(&w); err != nil {
		return nil, invalid("blob download JSON")
	}
	canonical, err := json.Marshal(w)
	if err != nil || !bytes.Equal(data, canonical) || base64.RawURLEncoding.EncodeToString(data) != header || w.Version != 1 || w.MessageID != messageID {
		return nil, invalid("blob download binding")
	}
	hash, err := hex.DecodeString(w.SHA256)
	if err != nil || hex.EncodeToString(hash) != w.SHA256 {
		return nil, invalid("blob download hash")
	}
	digest, err := hex.DecodeString(w.EnvelopeDigest)
	if err != nil || hex.EncodeToString(digest) != w.EnvelopeDigest {
		return nil, invalid("blob download digest")
	}
	r := &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: w.RecipientGenerationID}, Binding: &pb.EpochSendBinding{
		ProtocolVersion: 3, MessageId: w.MessageID, ConversationId: w.ConversationID, Epoch: w.CryptoEpoch, Sender: &pb.DeviceRef{UserId: w.SenderUserID, GenerationId: w.SenderGenerationID, DeviceId: 1},
		Recipient: &pb.DeviceRef{UserId: w.RecipientUserID, GenerationId: w.RecipientGenerationID, DeviceId: 1}, EnvelopeDigest: digest}, Blob: &v2.BlobRef{FormatVersion: 1, BlobId: w.BlobID, CiphertextBytes: w.CiphertextBytes, Sha256: hash}}
	if err = ValidateBlobReadRequest(r); err != nil {
		return nil, err
	}
	return r, nil
}
