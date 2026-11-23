package e2ee

// Epoch data validation and canonical binding (text2/attachment3); no storage I/O.
import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"unicode/utf8"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

func epochValue(value *uint64) error {
	if value == nil || *value > math.MaxInt64 {
		return invalid("required crypto epoch")
	}
	return nil
}

func epochContext(c *pb.ClientContext) error {
	if !recoveryKnown(c) || !uuid4(c.DeviceGenerationId) {
		return invalid("v2 device context")
	}
	return recoveryVersion(c.ProtocolVersion)
}

func epochDevices(a, b *pb.DeviceRef) error {
	if ValidateDevice(a) != nil || ValidateDevice(b) != nil || a.DeviceId != 1 || b.DeviceId != 1 ||
		a.UserId == b.UserId || a.GenerationId == b.GenerationId {
		return invalid("v2 participant binding")
	}
	return nil
}

func ValidateEpochEnvelope(e *v2.Envelope) error {
	if !recoveryKnown(e) {
		return invalid("v2 envelope")
	}
	if err := epochFramingVersion(e.ProtocolVersion); err != nil {
		return err
	}
	if e.ProtocolVersion == AttachmentEnvelopeVersion {
		if err := ValidateBlobRef(e.Blob); err != nil {
			return err
		}
	} else if e.Blob != nil {
		return invalid("blob requires envelope3")
	}
	if err := epochValue(e.CryptoEpoch); err != nil {
		return err
	}
	if !uuid4(e.MessageId) || !uuid4(e.ConversationId) || e.CryptoSuite != pb.CryptoSuite_CRYPTO_SUITE_SIGNAL_PQ_1 || len(e.Ciphertext) == 0 ||
		(e.MessageType != pb.CiphertextType_CIPHERTEXT_TYPE_PREKEY_SIGNAL && e.MessageType != pb.CiphertextType_CIPHERTEXT_TYPE_SIGNAL) {
		return invalid("v2 envelope fields")
	}
	if err := epochDevices(e.Sender, e.Recipient); err != nil {
		return err
	}
	return wireSize(e)
}

// The expected epoch must come from an authorized conversation/epoch catalog,
// not the same untrusted envelope. Archived receiving epochs are checked too.
func ValidateEpochEnvelopeAtEpoch(e *v2.Envelope, expected uint64) error {
	if err := ValidateEpochEnvelope(e); err != nil {
		return err
	}
	if expected > math.MaxInt64 || *e.CryptoEpoch != expected {
		return invalid("data epoch binding")
	}
	return nil
}

func ValidateEpochSend(r *v2.SendMessageRequest) error {
	if !recoveryKnown(r) {
		return invalid("v2 Send")
	}
	if err := epochContext(r.Context); err != nil {
		return err
	}
	if err := ValidateEpochEnvelope(r.Envelope); err != nil {
		return err
	}
	if r.Context.DeviceGenerationId != r.Envelope.Sender.GenerationId {
		return invalid("v2 Send generation")
	}
	return wireSize(r)
}

// EpochEnvelopeFrame preserves text2 and binds the public reference in attachment3.
// Presence/version are validated before encoding; there is no digest fallback.
func EpochEnvelopeFrame(e *v2.Envelope) ([]byte, error) {
	if err := ValidateEpochEnvelope(e); err != nil {
		return nil, err
	}
	b := []byte("zchat.e2ee.envelope.v2\x00")
	if e.ProtocolVersion == AttachmentEnvelopeVersion {
		b = []byte("zchat.e2ee.envelope.v3\x00")
	}
	u32 := func(v uint32) { b = binary.BigEndian.AppendUint32(b, v) }
	u32(e.ProtocolVersion)
	u32(uint32(e.CryptoSuite))
	b = append(b, e.MessageId...)
	b = append(b, e.ConversationId...)
	b = binary.BigEndian.AppendUint64(b, *e.CryptoEpoch)
	for _, d := range []*pb.DeviceRef{e.Sender, e.Recipient} {
		b = append(b, d.UserId...)
		u32(d.DeviceId)
		b = append(b, d.GenerationId...)
	}
	u32(uint32(e.MessageType))
	if ref := e.Blob; ref != nil {
		u32(ref.FormatVersion)
		b = append(b, ref.BlobId...)
		b = binary.BigEndian.AppendUint64(b, ref.CiphertextBytes)
		b = append(b, ref.Sha256...)
	}
	u32(uint32(len(e.Ciphertext)))
	b = append(b, e.Ciphertext...)
	return b, nil
}

func EpochEnvelopeDigest(e *v2.Envelope) ([32]byte, error) {
	b, err := EpochEnvelopeFrame(e)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

func DecodeEpochEnvelope(raw []byte) (*v2.Envelope, error) {
	if len(raw) > MaxRequestBytes {
		return nil, invalid("v2 envelope wire size")
	}
	e := new(v2.Envelope)
	if proto.Unmarshal(raw, e) != nil {
		return nil, invalid("v2 envelope wire")
	}
	if err := ValidateEpochEnvelope(e); err != nil {
		return nil, err
	}
	return e, nil
}

// EpochSendBinding derives routing/digest from the exact v2 envelope. This is
// data, not an opaque native storage/COMMIT proof or a renderer-issued authority.
func EpochSendBinding(e *v2.Envelope) (*pb.EpochSendBinding, error) {
	digest, err := EpochEnvelopeDigest(e)
	if err != nil {
		return nil, err
	}
	return &pb.EpochSendBinding{ProtocolVersion: e.ProtocolVersion, ConversationId: e.ConversationId, Epoch: *e.CryptoEpoch, MessageId: e.MessageId,
		Sender: proto.Clone(e.Sender).(*pb.DeviceRef), Recipient: proto.Clone(e.Recipient).(*pb.DeviceRef), EnvelopeDigest: append([]byte(nil), digest[:]...)}, nil
}

func ValidateEpochReceipt(r *v2.SendMessageRequest, receipt *v2.SendReceipt) error {
	if err := ValidateEpochSend(r); err != nil {
		return err
	}
	if !recoveryKnown(receipt) || epochValue(receipt.CryptoEpoch) != nil {
		return invalid("v2 receipt")
	}
	digest, _ := EpochEnvelopeDigest(r.Envelope)
	const retentionMS = int64(30 * 24 * 60 * 60 * 1000)
	if receipt.MessageId != r.Envelope.MessageId || *receipt.CryptoEpoch != *r.Envelope.CryptoEpoch || !bytes.Equal(receipt.EnvelopeDigest, digest[:]) ||
		receipt.StoredAtUnixMs <= 0 || receipt.StoredAtUnixMs > math.MaxInt64-retentionMS || receipt.ExpiresAtUnixMs != receipt.StoredAtUnixMs+retentionMS {
		return invalid("v2 receipt binding")
	}
	return nil
}

// Call only after Signal authenticated decryption in native; the Go messaging
// server never receives plaintext. This helper does NOT authenticate ciphertext.
func ValidateEpochPayloadBinding(p *v2.EncryptedPayload, e *v2.Envelope) error {
	if err := ValidateEpochEnvelope(e); err != nil {
		return err
	}
	if !recoveryKnown(p) || epochValue(p.CryptoEpoch) != nil {
		return invalid("v2 payload")
	}
	if p.MessageId != e.MessageId || p.ConversationId != e.ConversationId || *p.CryptoEpoch != *e.CryptoEpoch ||
		!proto.Equal(p.Sender, e.Sender) || !proto.Equal(p.Recipient, e.Recipient) || p.CreatedAtUnixMs < 0 || proto.Size(p) > MaxRequestBytes {
		return invalid("v2 payload binding")
	}
	if epochDevices(p.Sender, p.Recipient) != nil {
		return invalid("v2 payload devices")
	}
	switch c := p.Content.(type) {
	case *v2.EncryptedPayload_Text:
		if e.ProtocolVersion != 2 || p.FormatVersion != 2 || c == nil || !recoveryKnown(c.Text) || len(c.Text.Text) == 0 || len(c.Text.Text) > MaxTextBytes || !utf8.ValidString(c.Text.Text) {
			return invalid("v2 text")
		}
	case *v2.EncryptedPayload_SessionConfirmation:
		if e.ProtocolVersion != 2 || p.FormatVersion != 2 || c == nil || !recoveryKnown(c.SessionConfirmation) {
			return invalid("v2 confirmation")
		}
		s := c.SessionConfirmation
		if !uuid4(s.InReplyToMessageId) || s.InReplyToMessageId == p.MessageId || epochValue(s.CryptoEpoch) != nil || *s.CryptoEpoch != *p.CryptoEpoch {
			return invalid("v2 confirmation binding")
		}
	case *v2.EncryptedPayload_Attachment:
		if e.ProtocolVersion != AttachmentEnvelopeVersion || c == nil {
			return invalid("private attachment payload")
		}
		if err := validateAttachment(p, c.Attachment); err != nil {
			return err
		}
		a, ref := c.Attachment, e.Blob
		if a.BlobId != ref.BlobId || binary.BigEndian.Uint64(a.PrivateDescriptor[65:])+AttachmentBlobOverhead != ref.CiphertextBytes || !bytes.Equal(a.PrivateDescriptor[33:65], ref.Sha256) {
			return invalid("private attachment/public reference binding")
		}
	default:
		return invalid("unsupported v2 payload")
	}
	return nil
}

func ValidateEpochAcquisition(r *v2.AcquireBundleRequest) error {
	if !recoveryKnown(r) || !uuid4(r.AcquisitionId) || !uuid4(r.ConversationId) || epochValue(r.CryptoEpoch) != nil {
		return invalid("v2 acquisition")
	}
	if err := epochContext(r.Context); err != nil {
		return err
	}
	if ValidateDevice(r.ExpectedPeer) != nil || r.ExpectedPeer.DeviceId != 1 || r.ExpectedPeer.GenerationId == r.Context.DeviceGenerationId {
		return invalid("v2 acquisition peer")
	}
	return wireSize(r)
}

// Structural only. Native must additionally pin identity/registration to its
// authoritative conversation and verify libsignal public-key signatures.
func ValidateEpochAcquiredBundle(r *v2.AcquireBundleRequest, b *v2.AcquiredBundle) error {
	if err := ValidateEpochAcquisition(r); err != nil {
		return err
	}
	if !recoveryKnown(b) {
		return invalid("v2 bundle")
	}
	if err := recoveryVersion(b.ProtocolVersion); err != nil {
		return err
	}
	if epochValue(b.CryptoEpoch) != nil || *b.CryptoEpoch != *r.CryptoEpoch || b.AcquisitionId != r.AcquisitionId || b.ConversationId != r.ConversationId ||
		!recoveryKnown(b.Device) || !proto.Equal(b.Device.Reference, r.ExpectedPeer) || ValidateDevice(b.Device.Reference) != nil || !ecPublic(b.Device.IdentityKey) ||
		b.Device.RegistrationId < 1 || b.Device.RegistrationId > 16383 || !b.Device.PublicMaterialReady || b.Device.PublicMaterialRevision == 0 || b.Device.PublicMaterialRevision > math.MaxInt64 ||
		b.NotAfterUnixMs <= 0 || !recoveryKnown(b.SignedPreKey) || !recoveryKnown(b.KyberPreKey) {
		return invalid("v2 bundle binding")
	}
	s, pq := b.SignedPreKey, b.KyberPreKey
	if !ecPublic(s.PublicKey) || len(s.Signature) != 64 || s.CreatedAtUnixMs < 0 || len(pq.PublicKey) != 1569 || pq.PublicKey[0] != 8 || len(pq.Signature) != 64 {
		return invalid("v2 public key shape")
	}
	if b.OneTimePreKey != nil && (!recoveryKnown(b.OneTimePreKey) || !ecPublic(b.OneTimePreKey.PublicKey)) {
		return invalid("v2 EC key")
	}
	return wireSize(b)
}
