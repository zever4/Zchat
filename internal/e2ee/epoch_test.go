package e2ee

import (
	"bytes"
	"encoding/hex"
	"math"
	"os"
	"strings"
	"testing"

	common "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

func epochFixture(t *testing.T, name string, m proto.Message) []byte {
	t.Helper()
	b, err := os.ReadFile("../../proto/fixtures/epoch-data-v2/" + name)
	if err != nil {
		t.Fatal(err)
	}
	if m != nil {
		if err := proto.Unmarshal(b, m); err != nil {
			t.Fatal(err)
		}
	}
	return b
}
func epochEnvelope(t *testing.T) *v2.Envelope {
	t.Helper()
	e := new(v2.Envelope)
	epochFixture(t, "envelope.bin", e)
	return e
}
func epochSend(e *v2.Envelope) *v2.SendMessageRequest {
	return &v2.SendMessageRequest{Context: &common.ClientContext{ProtocolVersion: 2, DeviceGenerationId: e.Sender.GenerationId}, Envelope: e}
}

func TestEpochV2SharedFramesAndBindings(t *testing.T) {
	hashes := strings.Fields(string(epochFixture(t, "frames.sha256", nil)))
	for _, name := range []string{"initial", "envelope"} {
		t.Run(name, func(t *testing.T) {
			e := new(v2.Envelope)
			wire := epochFixture(t, name+".bin", e)
			if err := ValidateEpochEnvelope(e); err != nil {
				t.Fatal(err)
			}
			if ValidateEpochEnvelopeAtEpoch(e, *e.CryptoEpoch) != nil || ValidateEpochEnvelopeAtEpoch(e, *e.CryptoEpoch+1) == nil {
				t.Fatal("authoritative epoch not checked")
			}
			if e.CryptoEpoch == nil || name == "initial" && *e.CryptoEpoch != 0 || name == "envelope" && *e.CryptoEpoch != 1<<53+17 {
				t.Fatal("epoch presence/width lost")
			}
			frame, err := EpochEnvelopeFrame(e)
			if err != nil || !bytes.Equal(frame, epochFixture(t, name+".frame", nil)) {
				t.Fatal("shared frame mismatch", err)
			}
			digest, err := EpochEnvelopeDigest(e)
			if err != nil {
				t.Fatal(err)
			}
			match := false
			for i := 0; i+1 < len(hashes); i += 2 {
				if hashes[i+1] == name+".frame" && hashes[i] == hex.EncodeToString(digest[:]) {
					match = true
				}
			}
			if !match {
				t.Fatal("shared digest mismatch")
			}
			if !bytes.Equal(wire[:2], []byte{8, 2}) {
				t.Fatal("unexpected protocol field")
			}
			reordered := append(append([]byte{}, wire[2:]...), wire[:2]...)
			decoded, err := DecodeEpochEnvelope(reordered)
			if err != nil {
				t.Fatal(err)
			}
			got, err := EpochEnvelopeDigest(decoded)
			if err != nil || got != digest {
				t.Fatal("protobuf order changed digest")
			}
			if err := ValidateEpochSend(epochSend(e)); err != nil {
				t.Fatal(err)
			}
		})
	}
	e := epochEnvelope(t)
	for _, name := range []string{"payload.bin", "confirmation.bin"} {
		p := new(v2.EncryptedPayload)
		epochFixture(t, name, p)
		if err := ValidateEpochPayloadBinding(p, e); err != nil {
			t.Fatal(name, err)
		}
	}
	for _, mutate := range []func(*v2.Envelope){
		func(e *v2.Envelope) { *e.CryptoEpoch++ }, func(e *v2.Envelope) { e.MessageId = requestID }, func(e *v2.Envelope) { e.ConversationId = requestID },
		func(e *v2.Envelope) { e.Sender.UserId = requestID }, func(e *v2.Envelope) { e.Sender.GenerationId = requestID }, func(e *v2.Envelope) { e.Recipient.UserId = requestID },
		func(e *v2.Envelope) { e.Recipient.GenerationId = requestID }, func(e *v2.Envelope) { e.MessageType = common.CiphertextType_CIPHERTEXT_TYPE_SIGNAL }, func(e *v2.Envelope) { e.Ciphertext = append(e.Ciphertext, 4) },
	} {
		changed := proto.Clone(e).(*v2.Envelope)
		mutate(changed)
		before, _ := EpochEnvelopeDigest(e)
		after, err := EpochEnvelopeDigest(changed)
		if err != nil || before == after {
			t.Fatal("unbound digest field", err)
		}
	}
}

func TestEpochV2ExplicitInitialZeroBindsPayloadBundleAndReceipt(t *testing.T) {
	e := new(v2.Envelope)
	epochFixture(t, "initial.bin", e)
	p := new(v2.EncryptedPayload)
	epochFixture(t, "payload.bin", p)
	p.CryptoEpoch = proto.Uint64(0)
	if err := ValidateEpochPayloadBinding(p, e); err != nil {
		t.Fatal(err)
	}
	r := new(v2.AcquireBundleRequest)
	b := new(v2.AcquiredBundle)
	epochFixture(t, "acquire.bin", r)
	epochFixture(t, "bundle.bin", b)
	r.CryptoEpoch = proto.Uint64(0)
	b.CryptoEpoch = proto.Uint64(0)
	if err := ValidateEpochAcquiredBundle(r, b); err != nil {
		t.Fatal(err)
	}
	s := new(v2.SendReceipt)
	epochFixture(t, "receipt.bin", s)
	s.CryptoEpoch = proto.Uint64(0)
	digest, err := EpochEnvelopeDigest(e)
	if err != nil {
		t.Fatal(err)
	}
	s.EnvelopeDigest = digest[:]
	if err := ValidateEpochReceipt(epochSend(e), s); err != nil {
		t.Fatal(err)
	}
	for _, message := range []proto.Message{e, p, r, b, s} {
		data, err := proto.Marshal(message)
		if err != nil {
			t.Fatal(err)
		}
		decoded := message.ProtoReflect().Type().New().Interface()
		if proto.Unmarshal(data, decoded) != nil || !proto.Equal(message, decoded) {
			t.Fatal("explicit zero presence lost on wire")
		}
	}
}

func TestEpochV2RejectsMissingEpochAndLegacyWire(t *testing.T) {
	e := epochEnvelope(t)
	for name, mutate := range map[string]func(*v2.Envelope){
		"missing epoch": func(e *v2.Envelope) { e.CryptoEpoch = nil }, "overflow": func(e *v2.Envelope) { e.CryptoEpoch = proto.Uint64(math.MaxUint64) },
		"v1": func(e *v2.Envelope) { e.ProtocolVersion = 1 }, "future version": func(e *v2.Envelope) { e.ProtocolVersion = 3 },
		"device2": func(e *v2.Envelope) { e.Sender.DeviceId = 2 }, "same account": func(e *v2.Envelope) { e.Recipient.UserId = e.Sender.UserId },
		"same generation": func(e *v2.Envelope) { e.Recipient.GenerationId = e.Sender.GenerationId }, "missing peer": func(e *v2.Envelope) { e.Recipient = nil },
		"UUID": func(e *v2.Envelope) { e.MessageId = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" }, "suite": func(e *v2.Envelope) { e.CryptoSuite = 99 },
		"type": func(e *v2.Envelope) { e.MessageType = 99 }, "ciphertext": func(e *v2.Envelope) { e.Ciphertext = nil },
		"oversize":         func(e *v2.Envelope) { e.Ciphertext = make([]byte, MaxRequestBytes) },
		"unknown envelope": func(e *v2.Envelope) { e.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown device":   func(e *v2.Envelope) { e.Sender.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(e).(*v2.Envelope)
			mutate(changed)
			if _, err := EpochEnvelopeDigest(changed); err == nil {
				t.Fatal("invalid envelope accepted")
			}
		})
	}
	for _, wire := range [][]byte{{0x80}, make([]byte, MaxRequestBytes+1), nil} {
		if _, err := DecodeEpochEnvelope(wire); err == nil {
			t.Fatal("invalid raw wire accepted")
		}
	}
	old := envelopeFixture(t)
	wire, _ := proto.Marshal(old)
	if _, err := DecodeEpochEnvelope(wire); err == nil {
		t.Fatal("legacy reader still enabled")
	}
	// Relabelling the version does not manufacture the mandatory epoch field.
	old.ProtocolVersion = 2
	wire, _ = proto.Marshal(old)
	if _, err := DecodeEpochEnvelope(wire); err == nil {
		t.Fatal("missing epoch silently inferred as0")
	}
	for _, mutate := range []func(*v2.SendMessageRequest){func(r *v2.SendMessageRequest) { r.Context.ProtocolVersion = 1 }, func(r *v2.SendMessageRequest) { r.Context.DeviceGenerationId = requestID }, func(r *v2.SendMessageRequest) { r.Context = nil }, func(r *v2.SendMessageRequest) { r.Envelope = nil }, func(r *v2.SendMessageRequest) { r.Context.ProtoReflect().SetUnknown([]byte{0x78, 1}) }, func(r *v2.SendMessageRequest) { r.ProtoReflect().SetUnknown([]byte{0x78, 1}) }} {
		r := epochSend(proto.Clone(e).(*v2.Envelope))
		mutate(r)
		if ValidateEpochSend(r) == nil {
			t.Fatal("invalid Send context accepted")
		}
	}
	e.CryptoEpoch = proto.Uint64(math.MaxInt64)
	if err := ValidateEpochEnvelope(e); err != nil {
		t.Fatal(err)
	}
}

func TestEpochV2AuthenticatedPayloadAndConfirmationBinding(t *testing.T) {
	e := epochEnvelope(t)
	p := new(v2.EncryptedPayload)
	epochFixture(t, "payload.bin", p)
	for name, mutate := range map[string]func(*v2.EncryptedPayload){
		"old format": func(p *v2.EncryptedPayload) { p.FormatVersion = 1 }, "missing epoch": func(p *v2.EncryptedPayload) { p.CryptoEpoch = nil }, "other epoch": func(p *v2.EncryptedPayload) { *p.CryptoEpoch++ },
		"message": func(p *v2.EncryptedPayload) { p.MessageId = requestID }, "chat": func(p *v2.EncryptedPayload) { p.ConversationId = requestID },
		"sender": func(p *v2.EncryptedPayload) { p.Sender.GenerationId = requestID }, "recipient": func(p *v2.EncryptedPayload) { p.Recipient.GenerationId = requestID },
		"time": func(p *v2.EncryptedPayload) { p.CreatedAtUnixMs = -1 }, "missing content": func(p *v2.EncryptedPayload) { p.Content = nil },
		"empty text": func(p *v2.EncryptedPayload) { p.GetText().Text = "" }, "long text": func(p *v2.EncryptedPayload) { p.GetText().Text = strings.Repeat("x", MaxTextBytes+1) },
		"invalid UTF8":    func(p *v2.EncryptedPayload) { p.GetText().Text = string([]byte{0xff}) },
		"unknown payload": func(p *v2.EncryptedPayload) { p.ProtoReflect().SetUnknown([]byte{0x78, 1}) }, "unknown text": func(p *v2.EncryptedPayload) { p.GetText().ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(p).(*v2.EncryptedPayload)
			mutate(changed)
			if ValidateEpochPayloadBinding(changed, e) == nil {
				t.Fatal("unbound payload accepted")
			}
		})
	}
	c := new(v2.EncryptedPayload)
	epochFixture(t, "confirmation.bin", c)
	for _, mutate := range []func(*v2.SessionConfirmation){func(c *v2.SessionConfirmation) { c.CryptoEpoch = nil }, func(c *v2.SessionConfirmation) { *c.CryptoEpoch++ }, func(c *v2.SessionConfirmation) { c.InReplyToMessageId = e.MessageId }, func(c *v2.SessionConfirmation) { c.InReplyToMessageId = "" }, func(c *v2.SessionConfirmation) { c.ProtoReflect().SetUnknown([]byte{0x78, 1}) }} {
		changed := proto.Clone(c).(*v2.EncryptedPayload)
		mutate(changed.GetSessionConfirmation())
		if ValidateEpochPayloadBinding(changed, e) == nil {
			t.Fatal("cross-epoch/invalid confirmation accepted")
		}
	}
}

func TestEpochV2ReceiptBindsEpochDigestAndFixedRetention(t *testing.T) {
	r := epochSend(epochEnvelope(t))
	receipt := new(v2.SendReceipt)
	epochFixture(t, "receipt.bin", receipt)
	if err := ValidateEpochReceipt(r, receipt); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*v2.SendReceipt){func(s *v2.SendReceipt) { s.CryptoEpoch = nil }, func(s *v2.SendReceipt) { *s.CryptoEpoch++ }, func(s *v2.SendReceipt) { s.MessageId = requestID }, func(s *v2.SendReceipt) { s.EnvelopeDigest[0] ^= 1 }, func(s *v2.SendReceipt) { s.ExpiresAtUnixMs++ }, func(s *v2.SendReceipt) { s.StoredAtUnixMs = math.MaxInt64 }, func(s *v2.SendReceipt) { s.StoredAtUnixMs = 0 }, func(s *v2.SendReceipt) { s.ProtoReflect().SetUnknown([]byte{0x78, 1}) }} {
		s := proto.Clone(receipt).(*v2.SendReceipt)
		mutate(s)
		if ValidateEpochReceipt(r, s) == nil {
			t.Fatal("invalid receipt accepted")
		}
	}
	// An exact expired replay still proves the original COMMIT, never a new TTL.
	if err := ValidateEpochReceipt(r, receipt); err != nil {
		t.Fatal(err)
	}
}

func TestEpochV2AcquisitionAndPublicBundleBinding(t *testing.T) {
	r := new(v2.AcquireBundleRequest)
	b := new(v2.AcquiredBundle)
	epochFixture(t, "acquire.bin", r)
	epochFixture(t, "bundle.bin", b)
	if err := ValidateEpochAcquiredBundle(r, b); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*v2.AcquiredBundle){
		"epoch": func(b *v2.AcquiredBundle) { *b.CryptoEpoch++ }, "absent epoch": func(b *v2.AcquiredBundle) { b.CryptoEpoch = nil }, "protocol": func(b *v2.AcquiredBundle) { b.ProtocolVersion = 1 },
		"acquisition": func(b *v2.AcquiredBundle) { b.AcquisitionId = b.ConversationId }, "chat": func(b *v2.AcquiredBundle) { b.ConversationId = b.AcquisitionId },
		"device": func(b *v2.AcquiredBundle) { b.Device.Reference.GenerationId = requestID }, "registration": func(b *v2.AcquiredBundle) { b.Device.RegistrationId = 16384 },
		"revision": func(b *v2.AcquiredBundle) { b.Device.PublicMaterialRevision = 0 }, "ready": func(b *v2.AcquiredBundle) { b.Device.PublicMaterialReady = false },
		"identity": func(b *v2.AcquiredBundle) { b.Device.IdentityKey[0] = 0 }, "signed": func(b *v2.AcquiredBundle) { b.SignedPreKey = nil }, "PQ": func(b *v2.AcquiredBundle) { b.KyberPreKey = nil },
		"signed signature": func(b *v2.AcquiredBundle) { b.SignedPreKey.Signature = nil }, "PQ format": func(b *v2.AcquiredBundle) { b.KyberPreKey.PublicKey[0] = 0 },
		"EC format": func(b *v2.AcquiredBundle) { b.OneTimePreKey.PublicKey = nil }, "lease": func(b *v2.AcquiredBundle) { b.NotAfterUnixMs = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(b).(*v2.AcquiredBundle)
			mutate(changed)
			if ValidateEpochAcquiredBundle(r, changed) == nil {
				t.Fatal("unbound bundle accepted")
			}
		})
	}
	for _, mutate := range []func(*v2.AcquireBundleRequest){func(r *v2.AcquireBundleRequest) { r.CryptoEpoch = nil }, func(r *v2.AcquireBundleRequest) { r.CryptoEpoch = proto.Uint64(math.MaxUint64) }, func(r *v2.AcquireBundleRequest) { r.Context.ProtocolVersion = 1 }, func(r *v2.AcquireBundleRequest) { r.ExpectedPeer.GenerationId = r.Context.DeviceGenerationId }, func(r *v2.AcquireBundleRequest) { r.ExpectedPeer.DeviceId = 2 }} {
		changed := proto.Clone(r).(*v2.AcquireBundleRequest)
		mutate(changed)
		if ValidateEpochAcquisition(changed) == nil {
			t.Fatal("invalid acquire accepted")
		}
	}
	for _, target := range []func(*v2.AcquiredBundle) proto.Message{func(b *v2.AcquiredBundle) proto.Message { return b }, func(b *v2.AcquiredBundle) proto.Message { return b.Device }, func(b *v2.AcquiredBundle) proto.Message { return b.Device.Reference }, func(b *v2.AcquiredBundle) proto.Message { return b.SignedPreKey }, func(b *v2.AcquiredBundle) proto.Message { return b.OneTimePreKey }, func(b *v2.AcquiredBundle) proto.Message { return b.KyberPreKey }} {
		changed := proto.Clone(b).(*v2.AcquiredBundle)
		target(changed).ProtoReflect().SetUnknown([]byte{0x78, 1})
		if ValidateEpochAcquiredBundle(r, changed) == nil {
			t.Fatal("unknown bundle fields accepted")
		}
	}
	optional := proto.Clone(b).(*v2.AcquiredBundle)
	optional.OneTimePreKey = nil
	optional.KyberPreKey = publicationFixture(t).KyberPreKeys[1]
	if err := ValidateEpochAcquiredBundle(r, optional); err != nil {
		t.Fatal("optional EC/last-resort PQ rejected", err)
	}
}

func TestEpochV2DigestConnectsExactSendToRecoveryFence(t *testing.T) {
	e := epochEnvelope(t)
	binding, err := EpochSendBinding(e)
	if err != nil {
		t.Fatal(err)
	}
	o := recoveryOutcome(t, "accept")
	digest, err := RecoveryDigest(o.Proposal.Control)
	if err != nil {
		t.Fatal(err)
	}
	f := &common.EpochSealedFence{Binding: binding, ProposalDigest: digest[:], NextEpoch: o.NewEpoch, EpochRevision: o.EpochRevision}
	if err := ValidateEpochSealedFence(f, binding, o); err != nil {
		t.Fatal(err)
	}
	oldDigest, err := EnvelopeDigest(envelopeFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	f.Binding.EnvelopeDigest = oldDigest[:]
	expected, err := EpochSendBinding(e)
	if err != nil {
		t.Fatal(err)
	}
	if ValidateEpochSealedFence(f, expected, o) == nil {
		t.Fatal("legacy digest accepted for v2 Send")
	}
	e.Sender.GenerationId = requestID
	if expected.Sender.GenerationId == requestID {
		t.Fatal("Send binding aliases mutable envelope")
	}
}
