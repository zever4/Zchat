package e2ee

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	legacy "github.com/zever4/Zchat/gen/messenger"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const requestID = "77777777-7777-4777-8777-777777777777"

func envelopeFixture(t *testing.T) *pb.Envelope {
	t.Helper()
	data, err := os.ReadFile("../../proto/fixtures/e2ee-v1-envelope.bin")
	if err != nil {
		t.Fatal(err)
	}
	envelope := new(pb.Envelope)
	if err := proto.Unmarshal(data, envelope); err != nil {
		t.Fatal(err)
	}
	text, err := os.ReadFile("../../proto/fixtures/e2ee-v1-envelope.textproto")
	if err != nil {
		t.Fatal(err)
	}
	fromText := new(pb.Envelope)
	if err := prototext.Unmarshal(text, fromText); err != nil || !proto.Equal(envelope, fromText) {
		t.Fatalf("binary/text fixture drift: %v", err)
	}
	return envelope
}

func deviceContext(envelope *pb.Envelope) *pb.ClientContext {
	return &pb.ClientContext{ProtocolVersion: ProtocolVersion, DeviceGenerationId: envelope.Sender.GenerationId}
}

func TestEnvelopeContract(t *testing.T) {
	if err := ValidateEnvelope(envelopeFixture(t)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*pb.Envelope)
	}{
		{"unsupported version", func(e *pb.Envelope) { e.ProtocolVersion = 2 }},
		{"missing version", func(e *pb.Envelope) { e.ProtocolVersion = 0 }},
		{"missing sender", func(e *pb.Envelope) { e.Sender = nil }},
		{"missing recipient", func(e *pb.Envelope) { e.Recipient = nil }},
		{"device zero", func(e *pb.Envelope) { e.Sender.DeviceId = 0 }},
		{"device 128", func(e *pb.Envelope) { e.Sender.DeviceId = 128 }},
		{"device narrowing attack", func(e *pb.Envelope) { e.Sender.DeviceId = 257 }},
		{"UUID alias", func(e *pb.Envelope) { e.MessageId = strings.ReplaceAll(e.MessageId, "-", "") }},
		{"UUID nil", func(e *pb.Envelope) { e.ConversationId = "00000000-0000-0000-0000-000000000000" }},
		{"UUID uppercase", func(e *pb.Envelope) { e.Sender.UserId = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" }},
		{"wrong variant", func(e *pb.Envelope) { e.MessageId = "55555555-5555-4555-0555-555555555555" }},
		{"missing generation", func(e *pb.Envelope) { e.Recipient.GenerationId = "" }},
		{"unknown crypto suite", func(e *pb.Envelope) { e.CryptoSuite = pb.CryptoSuite(99) }},
		{"group key type", func(e *pb.Envelope) { e.MessageType = pb.CiphertextType(3) }},
		{"empty ciphertext", func(e *pb.Envelope) { e.Ciphertext = nil }},
		{"oversized ciphertext", func(e *pb.Envelope) { e.Ciphertext = make([]byte, MaxRequestBytes) }},
		{"self conversation", func(e *pb.Envelope) { e.Recipient.UserId = e.Sender.UserId }},
		{"unknown envelope field", func(e *pb.Envelope) {
			e.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
		}},
		{"unknown device field", func(e *pb.Envelope) {
			e.Sender.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := envelopeFixture(t)
			tc.mutate(e)
			if err := ValidateEnvelope(e); err == nil {
				t.Fatal("invalid envelope accepted")
			}
			if _, err := EnvelopeDigest(e); err == nil {
				t.Fatal("invalid envelope hashed")
			}
		})
	}
	for _, deviceID := range []uint32{1, 127} {
		e := envelopeFixture(t)
		e.Sender.DeviceId = deviceID
		if err := ValidateEnvelope(e); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnvelopeDigestStableAcrossWireOrderAndBindsEveryField(t *testing.T) {
	e := envelopeFixture(t)
	digest, err := EnvelopeDigest(e)
	if err != nil || hex.EncodeToString(digest[:]) != "9c2252f36ab1d07773dc5a6ca84504ef8af4a6e66e277022391ef84e49e85190" {
		t.Fatalf("shared frame digest drift: %v", err)
	}
	wire, err := proto.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	// Move the known version field to the end: valid protobuf, same semantic envelope.
	if !bytes.Equal(wire[:2], []byte{8, 1}) {
		t.Fatal("unexpected fixture version encoding")
	}
	reordered := append(append([]byte{}, wire[2:]...), wire[:2]...)
	decoded := new(pb.Envelope)
	if err := proto.Unmarshal(reordered, decoded); err != nil {
		t.Fatal(err)
	}
	got, err := EnvelopeDigest(decoded)
	if err != nil || got != digest {
		t.Fatal("digest depends on protobuf field order")
	}
	for _, mutate := range []func(*pb.Envelope){
		func(e *pb.Envelope) { e.MessageId = requestID },
		func(e *pb.Envelope) { e.ConversationId = requestID },
		func(e *pb.Envelope) { e.Sender.UserId = requestID },
		func(e *pb.Envelope) { e.Sender.DeviceId = 2 },
		func(e *pb.Envelope) { e.Sender.GenerationId = requestID },
		func(e *pb.Envelope) { e.Recipient.UserId = requestID },
		func(e *pb.Envelope) { e.Recipient.DeviceId = 2 },
		func(e *pb.Envelope) { e.Recipient.GenerationId = requestID },
		func(e *pb.Envelope) { e.MessageType = pb.CiphertextType_CIPHERTEXT_TYPE_SIGNAL },
		func(e *pb.Envelope) { e.Ciphertext = append(e.Ciphertext, 4) },
	} {
		changed := proto.Clone(e).(*pb.Envelope)
		mutate(changed)
		got, err := EnvelopeDigest(changed)
		if err != nil || got == digest {
			t.Fatal("changed routing/ciphertext retained receipt digest")
		}
	}
}

func uploadFixture(t *testing.T) *pb.UploadPreKeysRequest {
	t.Helper()
	data, err := os.ReadFile("../../proto/fixtures/prekey-upload-v1.bin")
	if err != nil {
		t.Fatal(err)
	}
	keys := new(legacy.PreKeyBundleUpload)
	if err := proto.Unmarshal(data, keys); err != nil {
		t.Fatal(err)
	}
	upload := &pb.UploadPreKeysRequest{Context: deviceContext(envelopeFixture(t)), BatchId: requestID,
		SignedPreKey: &pb.SignedPreKey{KeyId: keys.SignedPreKey.KeyId, PublicKey: keys.SignedPreKey.PublicKey, Signature: keys.SignedPreKey.Signature, CreatedAtUnixMs: int64(keys.SignedPreKeyTimestamp)}}
	for _, key := range keys.OneTimePreKeys {
		upload.OneTimePreKeys = append(upload.OneTimePreKeys, &pb.OneTimePreKey{KeyId: key.KeyId, PublicKey: key.PublicKey})
	}
	for _, key := range keys.KyberPreKeys {
		upload.KyberPreKeys = append(upload.KyberPreKeys, &pb.KyberPreKey{KeyId: key.KeyId, PublicKey: key.PublicKey, Signature: key.Signature, IsLastResort: key.IsLastResort})
	}
	return upload
}

func TestUploadContract(t *testing.T) {
	if err := ValidateUpload(uploadFixture(t)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*pb.UploadPreKeysRequest)
	}{
		{"missing context", func(u *pb.UploadPreKeysRequest) { u.Context = nil }},
		{"missing batch", func(u *pb.UploadPreKeysRequest) { u.BatchId = "" }},
		{"nil EC", func(u *pb.UploadPreKeysRequest) { u.OneTimePreKeys[0] = nil }},
		{"nil PQ", func(u *pb.UploadPreKeysRequest) { u.KyberPreKeys[0] = nil }},
		{"EC duplicate ID", func(u *pb.UploadPreKeysRequest) { u.OneTimePreKeys = append(u.OneTimePreKeys, u.OneTimePreKeys[0]) }},
		{"PQ duplicate ID", func(u *pb.UploadPreKeysRequest) { u.KyberPreKeys = append(u.KyberPreKeys, u.KyberPreKeys[0]) }},
		{"two last resort", func(u *pb.UploadPreKeysRequest) { u.KyberPreKeys[0].IsLastResort = true }},
		{"wrong EC tag", func(u *pb.UploadPreKeysRequest) { u.OneTimePreKeys[0].PublicKey[0] = 7 }},
		{"wrong PQ tag", func(u *pb.UploadPreKeysRequest) { u.KyberPreKeys[0].PublicKey[0] = 7 }},
		{"short signature", func(u *pb.UploadPreKeysRequest) { u.SignedPreKey.Signature = u.SignedPreKey.Signature[:63] }},
		{"negative timestamp", func(u *pb.UploadPreKeysRequest) { u.SignedPreKey.CreatedAtUnixMs = -1 }},
		{"no material", func(u *pb.UploadPreKeysRequest) { u.SignedPreKey = nil; u.OneTimePreKeys = nil; u.KyberPreKeys = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := uploadFixture(t)
			tc.mutate(u)
			if err := ValidateUpload(u); err == nil {
				t.Fatal("invalid upload accepted")
			}
		})
	}
	// A pool can contain 100 KEM keys, but publication must be split by encoded bytes.
	u := uploadFixture(t)
	key := u.KyberPreKeys[0]
	u.KyberPreKeys = nil
	for i := range 100 {
		copy := proto.Clone(key).(*pb.KyberPreKey)
		copy.KeyId = uint32(i)
		u.KyberPreKeys = append(u.KyberPreKeys, copy)
	}
	if proto.Size(u) <= MaxRequestBytes || ValidateUpload(u) == nil {
		t.Fatal("oversized 100-Kyber upload accepted")
	}
	u = uploadFixture(t)
	u.SignedPreKey = nil
	u.OneTimePreKeys = nil
	u.KyberPreKeys = u.KyberPreKeys[1:]
	if err := ValidateUpload(u); err != nil {
		t.Fatal("last-resort-only rotation rejected", err)
	}
	if ValidateUpload(nil) == nil || ValidateEnvelope(nil) == nil {
		t.Fatal("nil request accepted")
	}
}

func TestRequestsBindGenerationAndAcknowledgeExactEnvelope(t *testing.T) {
	e := envelopeFixture(t)
	ctx := deviceContext(e)
	r := &pb.SendMessageRequest{Context: ctx, Envelope: e}
	if err := ValidateSend(r); err != nil {
		t.Fatal(err)
	}
	if err := ValidateStream(&pb.StreamMessagesRequest{Context: ctx}); err != nil {
		t.Fatal(err)
	}
	ctx.DeviceGenerationId = requestID
	if ValidateSend(r) == nil {
		t.Fatal("stale sender generation accepted")
	}
	ctx.DeviceGenerationId = e.Sender.GenerationId
	create := &pb.CreateConversationRequest{Context: ctx, CreationRequestId: requestID, PeerUserId: 2}
	if ValidateCreateConversation(create) != nil {
		t.Fatal("valid creation rejected")
	}
	create.PeerUserId = 0
	if ValidateCreateConversation(create) == nil {
		t.Fatal("invalid peer accepted")
	}
	get := &pb.GetConversationRequest{Context: ctx, ConversationId: e.ConversationId}
	if ValidateGetConversation(get) != nil {
		t.Fatal("valid lookup rejected")
	}
	acquire := &pb.AcquireBundleRequest{Context: ctx, ConversationId: e.ConversationId, AcquisitionId: requestID, ExpectedPeer: e.Recipient}
	if ValidateAcquisition(acquire) != nil {
		t.Fatal("valid acquisition rejected")
	}
	acquire.ExpectedPeer = nil
	if ValidateAcquisition(acquire) == nil {
		t.Fatal("unbound acquisition accepted")
	}
	ack := &pb.AcknowledgeMessagesRequest{Context: ctx, Messages: []*pb.Acknowledgement{{MessageId: e.MessageId, EnvelopeDigest: make([]byte, 32), Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}
	if ValidateAcknowledgements(ack) != nil {
		t.Fatal("valid ACK rejected")
	}
	ack.Messages = append(ack.Messages, ack.Messages[0])
	if ValidateAcknowledgements(ack) == nil {
		t.Fatal("duplicate ACK accepted")
	}
	ack.Messages = ack.Messages[:1]
	ack.Messages[0].Disposition = 0
	if ValidateAcknowledgements(ack) == nil {
		t.Fatal("unknown disposition accepted")
	}
	ack.Messages[0].Disposition = pb.AckDisposition_ACK_DISPOSITION_REJECTED
	ack.Messages[0].EnvelopeDigest = nil
	if ValidateAcknowledgements(ack) == nil {
		t.Fatal("ACK without digest accepted")
	}
	ctx.ProtocolVersion = 2
	if !errors.Is(ValidateContext(ctx), ErrUnsupportedVersion) {
		t.Fatal("version fallback allowed")
	}
}

func TestRegistrationDoesNotClaimAccountOrReplacement(t *testing.T) {
	keys := uploadFixture(t)
	r := &pb.RegisterDeviceRequest{ProtocolVersion: ProtocolVersion, RegistrationRequestId: requestID, RegistrationId: 4294967295, IdentityKey: keys.OneTimePreKeys[0].PublicKey, CredentialHash: make([]byte, 32)}
	if err := ValidateRegistration(r); err != nil {
		t.Fatal(err)
	}
	fields := r.ProtoReflect().Descriptor().Fields()
	if fields.ByName("user_id") != nil || fields.ByName("password") != nil || fields.ByName("replace") != nil {
		t.Fatal("registration can impersonate/replace an account")
	}
	r.CredentialHash = nil
	if ValidateRegistration(r) == nil {
		t.Fatal("missing device verifier accepted")
	}
	if ValidateRegistration(nil) == nil || ValidateSend(nil) == nil || ValidateStream(nil) == nil || ValidateAcquisition(nil) == nil || ValidateAcknowledgements(nil) == nil {
		t.Fatal("nil request accepted")
	}
}

func TestReplacementIsExplicitGenerationBoundAndValidationErrorsContainNoPassword(t *testing.T) {
	key := uploadFixture(t).OneTimePreKeys[0].PublicKey
	r := &pb.ReplaceOwnDeviceRequest{ProtocolVersion: ProtocolVersion, ReplacementRequestId: requestID,
		ExpectedGenerationId: envelopeFixture(t).Sender.GenerationId, IdentityKey: key, CredentialHash: make([]byte, 32), CurrentPassword: "synthetic-password-canary"}
	if err := ValidateReplacement(r); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.ReplaceOwnDeviceRequest){
		func(r *pb.ReplaceOwnDeviceRequest) { r.ExpectedGenerationId = "" },
		func(r *pb.ReplaceOwnDeviceRequest) { r.ReplacementRequestId = "" },
		func(r *pb.ReplaceOwnDeviceRequest) { r.CurrentPassword = "" },
		func(r *pb.ReplaceOwnDeviceRequest) { r.CurrentPassword = strings.Repeat("x", 73) },
		func(r *pb.ReplaceOwnDeviceRequest) { r.CurrentPassword = string([]byte{0xff}) },
	} {
		changed := proto.Clone(r).(*pb.ReplaceOwnDeviceRequest)
		mutate(changed)
		err := ValidateReplacement(changed)
		if err == nil || strings.Contains(err.Error(), "synthetic-password-canary") {
			t.Fatal("invalid replacement accepted or credential leaked")
		}
	}
	if ValidateReplacement(nil) == nil {
		t.Fatal("nil replacement accepted")
	}
}

func TestLimitsApplyToFullSendWrapperAndAckCount(t *testing.T) {
	e := envelopeFixture(t)
	e.Ciphertext = make([]byte, MaxRequestBytes-proto.Size(e)+len(e.Ciphertext))
	// A longer protobuf length varint can increase the size by up to two bytes.
	for proto.Size(e) > MaxRequestBytes {
		e.Ciphertext = e.Ciphertext[:len(e.Ciphertext)-1]
	}
	if proto.Size(e) != MaxRequestBytes || ValidateEnvelope(e) != nil {
		t.Fatal("boundary envelope rejected")
	}
	if ValidateSend(&pb.SendMessageRequest{Context: deviceContext(e), Envelope: e}) == nil {
		t.Fatal("wrapper overhead bypassed request size limit")
	}
	acks := &pb.AcknowledgeMessagesRequest{Context: deviceContext(e)}
	for i := range MaxAcknowledgements {
		acks.Messages = append(acks.Messages, &pb.Acknowledgement{
			MessageId:      fmt.Sprintf("%08x-7777-4777-8777-777777777777", i),
			EnvelopeDigest: make([]byte, 32), Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED,
		})
	}
	if err := ValidateAcknowledgements(acks); err != nil {
		t.Fatal(err)
	}
	acks.Messages = append(acks.Messages, &pb.Acknowledgement{
		MessageId: requestID, EnvelopeDigest: make([]byte, 32), Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED,
	})
	if ValidateAcknowledgements(acks) == nil {
		t.Fatal("oversized acknowledgement batch accepted")
	}
}
