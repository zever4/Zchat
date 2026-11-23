package e2ee

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"testing"

	common "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

const blobVectors = "../../proto/fixtures/attachment-envelope-v3/"

func blobEnvelope(t *testing.T) *v2.Envelope {
	t.Helper()
	data, err := os.ReadFile(blobVectors + "envelope.bin")
	if err != nil {
		t.Fatal(err)
	}
	e := new(v2.Envelope)
	if err = proto.Unmarshal(data, e); err != nil {
		t.Fatal(err)
	}
	return e
}
func blobReceipt(t *testing.T, e *v2.Envelope) *v2.SendReceipt {
	t.Helper()
	digest, err := EpochEnvelopeDigest(e)
	if err != nil {
		t.Fatal(err)
	}
	return &v2.SendReceipt{MessageId: e.MessageId, EnvelopeDigest: digest[:], StoredAtUnixMs: 1700000000000, ExpiresAtUnixMs: 1700000000000 + 30*24*60*60*1000, CryptoEpoch: proto.Uint64(*e.CryptoEpoch)}
}
func TestAttachmentEnvelopeSharedV3FrameAndFullWidthEpoch(t *testing.T) {
	e := blobEnvelope(t)
	frame, err := EpochEnvelopeFrame(e)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := os.ReadFile(blobVectors + "envelope.frame")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Frame  string `json:"expected_frame_hex"`
		Digest string `json:"expected_sha256_hex"`
	}
	data, err := os.ReadFile(blobVectors + "design-vector.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	decoded, err := hex.DecodeString(vector.Frame)
	if err != nil {
		t.Fatal(err)
	}
	digest, _ := EpochEnvelopeDigest(e)
	if len(frame) != 354 || !bytes.Equal(frame, expected) || !bytes.Equal(frame, decoded) || hex.EncodeToString(digest[:]) != vector.Digest {
		t.Fatal("independent v3 frame/hash mismatch")
	}
	if e.ProtocolVersion != 3 || *e.CryptoEpoch != 1<<53+17 || e.Blob.CiphertextBytes != 37 {
		t.Fatal("v3 presence/precision lost")
	}
	if ValidateEpochSend(epochSend(e)) != nil || ValidateEpochEnvelopeAtEpoch(e, *e.CryptoEpoch) != nil || ValidateEpochEnvelopeAtEpoch(e, *e.CryptoEpoch+1) == nil {
		t.Fatal("epoch3/context2 binding failed")
	}
	wire, err := proto.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	reordered := append(append([]byte{}, wire[2:]...), wire[:2]...)
	roundtrip, err := DecodeEpochEnvelope(reordered)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := EpochEnvelopeDigest(roundtrip)
	if digest != got {
		t.Fatal("protobuf order changed canonical hash")
	}
	binding, err := EpochSendBinding(e)
	if err != nil || binding.ProtocolVersion != 3 || !bytes.Equal(binding.EnvelopeDigest, digest[:]) {
		t.Fatal("lost frame version in recovery binding", err)
	}
}
func TestAttachmentReferenceAndEveryStableFieldBindReceiptDigest(t *testing.T) {
	base := blobEnvelope(t)
	old, _ := EpochEnvelopeDigest(base)
	receipt := blobReceipt(t, base)
	cases := map[string]func(*v2.Envelope){
		"message":              func(e *v2.Envelope) { e.MessageId = requestID },
		"chat":                 func(e *v2.Envelope) { e.ConversationId = requestID },
		"epoch":                func(e *v2.Envelope) { e.CryptoEpoch = proto.Uint64(0) },
		"sender":               func(e *v2.Envelope) { e.Sender.UserId = requestID },
		"sender generation":    func(e *v2.Envelope) { e.Sender.GenerationId = requestID },
		"recipient":            func(e *v2.Envelope) { e.Recipient.UserId = requestID },
		"recipient generation": func(e *v2.Envelope) { e.Recipient.GenerationId = requestID },
		"cipher type":          func(e *v2.Envelope) { e.MessageType = common.CiphertextType_CIPHERTEXT_TYPE_SIGNAL },
		"cipher bytes":         func(e *v2.Envelope) { e.Ciphertext = append(e.Ciphertext, 4) },
		"blob id":              func(e *v2.Envelope) { e.Blob.BlobId = requestID },
		"blob bytes":           func(e *v2.Envelope) { e.Blob.CiphertextBytes++ },
		"blob digest":          func(e *v2.Envelope) { e.Blob.Sha256[0] ^= 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := proto.Clone(base).(*v2.Envelope)
			mutate(e)
			digest, err := EpochEnvelopeDigest(e)
			if err != nil || digest == old {
				t.Fatal("stable public field omitted from digest", err)
			}
			if ValidateEpochReceipt(epochSend(e), receipt) == nil {
				t.Fatal("old receipt accepted for changed envelope")
			}
		})
	}
}
func TestAttachmentEnvelopeRejectsMissingCrossVersionAndMalformedReferences(t *testing.T) {
	base := blobEnvelope(t)
	cases := map[string]func(*v2.Envelope){
		"missing blob":     func(e *v2.Envelope) { e.Blob = nil },
		"v2 blob":          func(e *v2.Envelope) { e.ProtocolVersion = 2 },
		"old framing":      func(e *v2.Envelope) { e.ProtocolVersion = 1 },
		"future framing":   func(e *v2.Envelope) { e.ProtocolVersion = 4 },
		"missing epoch":    func(e *v2.Envelope) { e.CryptoEpoch = nil },
		"overflow epoch":   func(e *v2.Envelope) { e.CryptoEpoch = proto.Uint64(math.MaxUint64) },
		"ref version":      func(e *v2.Envelope) { e.Blob.FormatVersion = 2 },
		"zero ref version": func(e *v2.Envelope) { e.Blob.FormatVersion = 0 },
		"URI":              func(e *v2.Envelope) { e.Blob.BlobId = "https://example.test/blob" },
		"uppercase":        func(e *v2.Envelope) { e.Blob.BlobId = "CCCCCCCC-CCCC-4CCC-8CCC-CCCCCCCCCCCC" },
		"UUID version":     func(e *v2.Envelope) { e.Blob.BlobId = "cccccccc-cccc-1ccc-8ccc-cccccccccccc" },
		"empty size":       func(e *v2.Envelope) { e.Blob.CiphertextBytes = 0 },
		"overhead only":    func(e *v2.Envelope) { e.Blob.CiphertextBytes = 36 },
		"oversize":         func(e *v2.Envelope) { e.Blob.CiphertextBytes = MaxAttachmentBlobBytes + 1 },
		"size overflow":    func(e *v2.Envelope) { e.Blob.CiphertextBytes = math.MaxUint64 },
		"short hash":       func(e *v2.Envelope) { e.Blob.Sha256 = e.Blob.Sha256[:31] },
		"long hash":        func(e *v2.Envelope) { e.Blob.Sha256 = append(e.Blob.Sha256, 0) },
		"unknown ref":      func(e *v2.Envelope) { e.Blob.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown envelope": func(e *v2.Envelope) { e.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			e := proto.Clone(base).(*v2.Envelope)
			mutate(e)
			if ValidateEpochEnvelope(e) == nil {
				t.Fatal("invalid blob envelope accepted")
			}
			if _, err := EpochEnvelopeFrame(e); err == nil {
				t.Fatal("invalid envelope got canonical digest")
			}
		})
	}
	e := proto.Clone(base).(*v2.Envelope)
	e.Blob.CiphertextBytes = MaxAttachmentBlobBytes
	if ValidateEpochEnvelope(e) != nil {
		t.Fatal("valid file maximum rejected")
	}
	e = proto.Clone(base).(*v2.Envelope)
	e.ProtocolVersion, e.Blob = 2, nil
	if ValidateEpochEnvelope(e) != nil {
		t.Fatal("ordinary framing2 changed")
	}
}
func TestAttachmentPrivateDescriptorMustMatchPublicReference(t *testing.T) {
	p, e := attachmentFixture(t)
	if ValidateEpochPayloadBinding(p, e) != nil {
		t.Fatal("fixture mismatch")
	}
	for _, mutate := range []func(*v2.EncryptedPayload){
		func(p *v2.EncryptedPayload) { p.GetAttachment().BlobId = requestID },
		func(p *v2.EncryptedPayload) { p.GetAttachment().PrivateDescriptor[33] ^= 1 },
		func(p *v2.EncryptedPayload) { binary.BigEndian.PutUint64(p.GetAttachment().PrivateDescriptor[65:], 43) },
	} {
		copy := proto.Clone(p).(*v2.EncryptedPayload)
		mutate(copy)
		if ValidateEpochPayloadBinding(copy, e) == nil {
			t.Fatal("private/public identity mismatch accepted")
		}
	}
	text := proto.Clone(p).(*v2.EncryptedPayload)
	text.FormatVersion = 2
	text.Content = &v2.EncryptedPayload_Text{Text: &common.TextMessage{Text: "bound text"}}
	if ValidateEpochPayloadBinding(text, e) == nil {
		t.Fatal("text in attachment envelope accepted")
	}
	e.ProtocolVersion, e.Blob = 2, nil
	if ValidateEpochPayloadBinding(p, e) == nil || ValidateEpochPayloadBinding(text, e) != nil {
		t.Fatal("private attachment type confusion")
	}
}
func TestAttachmentReceiptAndSignedRecoveryFenceKeepControlVersionTwo(t *testing.T) {
	// Use the signed public recovery fixture's actual route and full-width epoch.
	e := epochEnvelope(t)
	e.ProtocolVersion, e.Blob = 3, proto.Clone(blobEnvelope(t).Blob).(*v2.BlobRef)
	receipt := blobReceipt(t, e)
	if ValidateEpochReceipt(epochSend(e), receipt) != nil {
		t.Fatal("bound receipt rejected")
	}
	binding, err := EpochSendBinding(e)
	if err != nil {
		t.Fatal(err)
	}
	accepted := recoveryOutcome(t, "accept")
	digest, _ := RecoveryDigest(accepted.Proposal.Control)
	fence := &common.EpochSealedFence{Binding: binding, ProposalDigest: digest[:], NextEpoch: accepted.NewEpoch, EpochRevision: accepted.EpochRevision}
	if ValidateEpochSealedFence(fence, binding, accepted) != nil {
		t.Fatal("v3 binding / signed control2 rejected")
	}
	bad := proto.Clone(binding).(*common.EpochSendBinding)
	bad.ProtocolVersion = 2
	if ValidateEpochSealedFence(fence, bad, accepted) == nil {
		t.Fatal("relabelled fence accepted")
	}
	e.Blob.Sha256[0] ^= 1
	changed, _ := EpochSendBinding(e)
	if ValidateEpochSealedFence(fence, changed, accepted) == nil {
		t.Fatal("changed ref bypassed exact fence binding")
	}
	control := proto.Clone(accepted.Proposal.Control).(*common.RecoveryControl)
	control.ProtocolVersion = 3
	if ValidateRecoveryControl(control) == nil {
		t.Fatal("framing3 enabled recovery-control3")
	}
	req := epochSend(e)
	req.Context.ProtocolVersion = 3
	if ValidateEpochSend(req) == nil {
		t.Fatal("framing3 enabled API-context3")
	}
	if ValidateEpochVersionBinding(3, 0, 0) == nil {
		t.Fatal("strict control/acquisition boundary relaxed")
	}
}
