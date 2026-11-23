package e2ee

import (
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func acquiredFixture(t *testing.T) (*pb.AcquireBundleRequest, *pb.AcquiredBundle) {
	t.Helper()
	envelope := envelopeFixture(t)
	keys := publicationFixture(t)
	req := &pb.AcquireBundleRequest{Context: deviceContext(envelope), ConversationId: envelope.ConversationId, AcquisitionId: requestID, ExpectedPeer: envelope.Recipient}
	b := &pb.AcquiredBundle{AcquisitionId: req.AcquisitionId, ConversationId: req.ConversationId, Device: &pb.Device{Reference: proto.Clone(req.ExpectedPeer).(*pb.DeviceRef), RegistrationId: 4294967295, IdentityKey: keys.OneTimePreKeys[0].PublicKey, PublicMaterialReady: true, PublicMaterialRevision: 1}, SignedPreKey: keys.SignedPreKey, OneTimePreKey: keys.OneTimePreKeys[0], KyberPreKey: keys.KyberPreKeys[0], NotAfterUnixMs: 1700000000000}
	return req, b
}

func TestAcquisitionContractAndPublicResponse(t *testing.T) {
	req, b := acquiredFixture(t)
	if err := ValidateAcquiredBundle(req, b); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.AcquiredBundle){
		func(b *pb.AcquiredBundle) { b.AcquisitionId = envelopeFixture(t).MessageId },
		func(b *pb.AcquiredBundle) { b.ConversationId = requestID },
		func(b *pb.AcquiredBundle) { b.Device = nil },
		func(b *pb.AcquiredBundle) { b.Device.Reference.GenerationId = requestID },
		func(b *pb.AcquiredBundle) { b.Device.IdentityKey[0] = 7 },
		func(b *pb.AcquiredBundle) { b.Device.PublicMaterialReady = false },
		func(b *pb.AcquiredBundle) { b.SignedPreKey = nil },
		func(b *pb.AcquiredBundle) { b.KyberPreKey = nil },
		func(b *pb.AcquiredBundle) { b.SignedPreKey.Signature = nil },
		func(b *pb.AcquiredBundle) { b.KyberPreKey.PublicKey[0] = 7 },
		func(b *pb.AcquiredBundle) { b.OneTimePreKey.PublicKey = nil },
		func(b *pb.AcquiredBundle) { b.NotAfterUnixMs = 0 },
	} {
		changed := proto.Clone(b).(*pb.AcquiredBundle)
		mutate(changed)
		if ValidateAcquiredBundle(req, changed) == nil {
			t.Fatal("invalid public bundle accepted")
		}
	}
	optional := proto.Clone(b).(*pb.AcquiredBundle)
	optional.OneTimePreKey = nil
	optional.KyberPreKey = publicationFixture(t).KyberPreKeys[1]
	if err := ValidateAcquiredBundle(req, optional); err != nil {
		t.Fatal("optional EC/last-resort PQ rejected", err)
	}
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
	for _, field := range []string{"request", "context", "expected peer", "response", "device", "signed", "ec", "pq"} {
		r := proto.Clone(req).(*pb.AcquireBundleRequest)
		out := proto.Clone(b).(*pb.AcquiredBundle)
		var target proto.Message
		switch field {
		case "request":
			target = r
		case "context":
			target = r.Context
		case "expected peer":
			target = r.ExpectedPeer
		case "response":
			target = out
		case "device":
			target = out.Device
		case "signed":
			target = out.SignedPreKey
		case "ec":
			target = out.OneTimePreKey
		case "pq":
			target = out.KyberPreKey
		}
		target.ProtoReflect().SetUnknown(unknown)
		if ValidateAcquiredBundle(r, out) == nil {
			t.Fatal("unknown acquisition semantics accepted", field)
		}
	}
}
