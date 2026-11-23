// Package e2ee defines the v1 wire boundary. Validation is not authorization,
// libsignal verification, or proof that a recipient durably stored a message.
package e2ee

import (
	"errors"
	"fmt"
	"math"
	"unicode/utf8"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/proto"
)

const (
	ProtocolVersion     = 1
	MaxRequestBytes     = 48 << 10 // Below the existing global 64 KiB gRPC receive limit.
	MaxTextBytes        = 2000
	MaxKeysPerKind      = 100
	MaxAcknowledgements = 100
	MaxInboxMessages    = 32
	MaxResponseBytes    = 64 << 10
)

var ErrInvalid = errors.New("invalid E2EE contract")
var ErrUnsupportedVersion = errors.New("unsupported E2EE protocol version")

func invalid(field string) error {
	// Field names are static; never include plaintext, credentials or request dumps.
	return fmt.Errorf("%w: %s", ErrInvalid, field)
}

func version(value uint32) error {
	if value != ProtocolVersion {
		return ErrUnsupportedVersion
	}
	return nil
}

func uuid4(value string) bool {
	if len(value) != 36 {
		return false
	}
	id, err := uuid.Parse(value)
	return err == nil && id.Version() == 4 && id.Variant() == uuid.RFC4122 && id.String() == value
}

func wireSize(message proto.Message) error {
	if proto.Size(message) > MaxRequestBytes {
		return invalid("request size")
	}
	return nil
}

func ValidateContext(ctx *pb.ClientContext) error {
	if ctx == nil || !uuid4(ctx.DeviceGenerationId) {
		return invalid("device context")
	}
	return version(ctx.ProtocolVersion)
}

func ValidateDevice(ref *pb.DeviceRef) error {
	if ref == nil || !uuid4(ref.UserId) || !uuid4(ref.GenerationId) || ref.DeviceId < 1 || ref.DeviceId > 127 {
		return invalid("device reference")
	}
	if len(ref.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown device fields")
	}
	return nil
}

// Public EC parsing and signature verification still belong to libsignal.
func ecPublic(key []byte) bool { return len(key) == 33 && key[0] == 0x05 }

func ValidateRegistration(req *pb.RegisterDeviceRequest) error {
	if req == nil || !uuid4(req.RegistrationRequestId) || !ecPublic(req.IdentityKey) || len(req.CredentialHash) != 32 {
		return invalid("registration")
	}
	if err := version(req.ProtocolVersion); err != nil {
		return err
	}
	return wireSize(req)
}

func ValidateReplacement(req *pb.ReplaceOwnDeviceRequest) error {
	if req == nil || !uuid4(req.ReplacementRequestId) || !uuid4(req.ExpectedGenerationId) ||
		!ecPublic(req.IdentityKey) || len(req.CredentialHash) != 32 ||
		len(req.CurrentPassword) == 0 || len(req.CurrentPassword) > 72 || !utf8.ValidString(req.CurrentPassword) {
		return invalid("explicit device replacement")
	}
	if err := version(req.ProtocolVersion); err != nil {
		return err
	}
	return wireSize(req)
}

func ValidateCreateConversation(req *pb.CreateConversationRequest) error {
	if req == nil || !uuid4(req.CreationRequestId) || req.PeerUserId <= 0 || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("conversation creation")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown conversation context")
	}
	return wireSize(req)
}

func ValidateGetConversation(req *pb.GetConversationRequest) error {
	if req == nil || !uuid4(req.ConversationId) || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("conversation lookup")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown conversation context")
	}
	return wireSize(req)
}

func ValidateUpload(req *pb.UploadPreKeysRequest) error {
	if req == nil || !uuid4(req.BatchId) || req.ExpectedRevision >= math.MaxInt64 || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("upload batch")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown publication context")
	}
	if err := wireSize(req); err != nil {
		return err
	}
	if len(req.OneTimePreKeys) > MaxKeysPerKind || len(req.KyberPreKeys) > MaxKeysPerKind ||
		(req.SignedPreKey == nil && len(req.OneTimePreKeys)+len(req.KyberPreKeys) == 0) {
		return invalid("upload key count")
	}
	if key := req.SignedPreKey; key != nil && (!ecPublic(key.PublicKey) || len(key.Signature) != 64 || key.CreatedAtUnixMs < 0 || len(key.ProtoReflect().GetUnknown()) != 0) {
		return invalid("signed prekey")
	}
	ids := make(map[uint32]bool)
	for _, key := range req.OneTimePreKeys {
		if key == nil || !ecPublic(key.PublicKey) || ids[key.KeyId] || len(key.ProtoReflect().GetUnknown()) != 0 {
			return invalid("EC prekey")
		}
		ids[key.KeyId] = true
	}
	ids = make(map[uint32]bool)
	lastResort := false
	for _, key := range req.KyberPreKeys {
		// V1's only KEM public format is the pinned library's Kyber1024.
		if key == nil || len(key.PublicKey) != 1569 || key.PublicKey[0] != 0x08 || len(key.Signature) != 64 ||
			ids[key.KeyId] || (key.IsLastResort && lastResort) || len(key.ProtoReflect().GetUnknown()) != 0 {
			return invalid("Kyber prekey")
		}
		ids[key.KeyId] = true
		lastResort = lastResort || key.IsLastResort
	}
	return nil
}

func ValidateAcquisition(req *pb.AcquireBundleRequest) error {
	if req == nil || !uuid4(req.AcquisitionId) || !uuid4(req.ConversationId) || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("bundle acquisition")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown acquisition context")
	}
	if err := ValidateDevice(req.ExpectedPeer); err != nil {
		return err
	}
	return wireSize(req)
}

// Structural/public binding only. Signal identity/signature trust is verified
// by libsignal in the native client, before creating or changing a session.
func ValidateAcquiredBundle(req *pb.AcquireBundleRequest, bundle *pb.AcquiredBundle) error {
	if err := ValidateAcquisition(req); err != nil {
		return err
	}
	if bundle == nil || bundle.Device == nil || bundle.SignedPreKey == nil || bundle.KyberPreKey == nil || bundle.NotAfterUnixMs <= 0 ||
		bundle.AcquisitionId != req.AcquisitionId || bundle.ConversationId != req.ConversationId || !proto.Equal(bundle.Device.Reference, req.ExpectedPeer) ||
		!ecPublic(bundle.Device.IdentityKey) || !bundle.Device.PublicMaterialReady || bundle.Device.PublicMaterialRevision > math.MaxInt64 ||
		len(bundle.ProtoReflect().GetUnknown()) != 0 || len(bundle.Device.ProtoReflect().GetUnknown()) != 0 {
		return invalid("acquired bundle")
	}
	if err := wireSize(bundle); err != nil {
		return err
	}
	keys := &pb.UploadPreKeysRequest{Context: req.Context, BatchId: req.AcquisitionId, SignedPreKey: bundle.SignedPreKey, KyberPreKeys: []*pb.KyberPreKey{bundle.KyberPreKey}}
	if bundle.OneTimePreKey != nil {
		keys.OneTimePreKeys = []*pb.OneTimePreKey{bundle.OneTimePreKey}
	}
	return ValidateUpload(keys)
}

func ValidateEnvelope(envelope *pb.Envelope) error {
	if envelope == nil {
		return invalid("envelope")
	}
	if err := version(envelope.ProtocolVersion); err != nil {
		return err
	}
	if err := wireSize(envelope); err != nil {
		return err
	}
	if !uuid4(envelope.MessageId) || !uuid4(envelope.ConversationId) ||
		envelope.CryptoSuite != pb.CryptoSuite_CRYPTO_SUITE_SIGNAL_PQ_1 || len(envelope.Ciphertext) == 0 ||
		len(envelope.ProtoReflect().GetUnknown()) != 0 {
		return invalid("envelope fields")
	}
	if envelope.MessageType != pb.CiphertextType_CIPHERTEXT_TYPE_PREKEY_SIGNAL && envelope.MessageType != pb.CiphertextType_CIPHERTEXT_TYPE_SIGNAL {
		return invalid("ciphertext type")
	}
	if err := ValidateDevice(envelope.Sender); err != nil {
		return err
	}
	if err := ValidateDevice(envelope.Recipient); err != nil {
		return err
	}
	if envelope.Sender.UserId == envelope.Recipient.UserId {
		return invalid("private recipient")
	}
	return nil
}

func ValidateSend(req *pb.SendMessageRequest) error {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("send request")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown send context")
	}
	if err := ValidateEnvelope(req.Envelope); err != nil {
		return err
	}
	if req.Context.DeviceGenerationId != req.Envelope.Sender.GenerationId {
		return invalid("sender generation binding")
	}
	return wireSize(req)
}

func ValidateStream(req *pb.StreamMessagesRequest) error {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("stream request")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown stream context")
	}
	return wireSize(req)
}

func ValidateAcknowledgements(req *pb.AcknowledgeMessagesRequest) error {
	if req == nil || len(req.Messages) < 1 || len(req.Messages) > MaxAcknowledgements || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("acknowledgement count")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown ACK context")
	}
	if err := wireSize(req); err != nil {
		return err
	}
	ids := make(map[string]bool)
	for _, ack := range req.Messages {
		if ack == nil || !uuid4(ack.MessageId) || len(ack.EnvelopeDigest) != 32 || ids[ack.MessageId] || len(ack.ProtoReflect().GetUnknown()) != 0 ||
			(ack.Disposition != pb.AckDisposition_ACK_DISPOSITION_STORED && ack.Disposition != pb.AckDisposition_ACK_DISPOSITION_REJECTED) {
			return invalid("acknowledgement")
		}
		ids[ack.MessageId] = true
	}
	return nil
}

func ValidatePendingMessages(req *pb.GetPendingMessagesRequest) error {
	if req == nil || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("inbox request")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown inbox context")
	}
	return wireSize(req)
}

func ValidateMessageStatuses(req *pb.GetMessageStatusesRequest) error {
	if req == nil || len(req.Messages) < 1 || len(req.Messages) > MaxAcknowledgements || len(req.ProtoReflect().GetUnknown()) != 0 {
		return invalid("status request")
	}
	if err := ValidateContext(req.Context); err != nil {
		return err
	}
	if len(req.Context.ProtoReflect().GetUnknown()) != 0 {
		return invalid("unknown status context")
	}
	ids := make(map[string]bool)
	for _, ref := range req.Messages {
		if ref == nil || !uuid4(ref.MessageId) || len(ref.EnvelopeDigest) != 32 || ids[ref.MessageId] || len(ref.ProtoReflect().GetUnknown()) != 0 {
			return invalid("status reference")
		}
		ids[ref.MessageId] = true
	}
	return wireSize(req)
}
