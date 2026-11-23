package e2ee

import (
	"bytes"
	"encoding/hex"
	"math"
	"os"
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func publicationFixture(t *testing.T) *pb.UploadPreKeysRequest {
	t.Helper()
	data, err := os.ReadFile("../../proto/fixtures/e2ee-v1-publication.bin")
	if err != nil {
		t.Fatal(err)
	}
	req := new(pb.UploadPreKeysRequest)
	if err := proto.Unmarshal(data, req); err != nil {
		t.Fatal(err)
	}
	return req
}

func TestPublicationDigest(t *testing.T) {
	req := publicationFixture(t)
	fromReviewed := uploadFixture(t)
	fromReviewed.ExpectedRevision = 7
	if !proto.Equal(req, fromReviewed) {
		t.Fatal("v1 fixture differs from reviewed public key material")
	}
	digest, err := PublicationDigest(req)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(digest[:]) != "1832c4d75117af6ad403fde33200a57ec5cb47c2c2667d59edb373080f1cb6e1" {
		t.Fatal("shared publication digest drift")
	}
	wire, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	// Move the first length-delimited context field to the end.
	_, _, n := protowire.ConsumeTag(wire)
	_, m := protowire.ConsumeBytes(wire[n:])
	end := n + m
	reordered := append(append([]byte{}, wire[end:]...), wire[:end]...)
	decoded := new(pb.UploadPreKeysRequest)
	if err := proto.Unmarshal(reordered, decoded); err != nil {
		t.Fatal(err)
	}
	other, err := PublicationDigest(decoded)
	if err != nil || other != digest {
		t.Fatal("wire order changed digest", err)
	}
	for _, mutate := range []func(*pb.UploadPreKeysRequest){
		func(r *pb.UploadPreKeysRequest) { r.ExpectedRevision++ },
		func(r *pb.UploadPreKeysRequest) { r.BatchId = "88888888-7777-4777-8777-777777777777" },
		func(r *pb.UploadPreKeysRequest) { r.Context.DeviceGenerationId = requestID },
		func(r *pb.UploadPreKeysRequest) { r.SignedPreKey.KeyId-- },
		func(r *pb.UploadPreKeysRequest) { r.SignedPreKey.PublicKey[1] ^= 1 },
		func(r *pb.UploadPreKeysRequest) { r.SignedPreKey.Signature[1] ^= 1 },
		func(r *pb.UploadPreKeysRequest) { r.SignedPreKey.CreatedAtUnixMs++ },
		func(r *pb.UploadPreKeysRequest) { r.SignedPreKey = nil },
		func(r *pb.UploadPreKeysRequest) { r.OneTimePreKeys[0].KeyId++ },
		func(r *pb.UploadPreKeysRequest) { r.OneTimePreKeys[0].PublicKey[1] ^= 1 },
		func(r *pb.UploadPreKeysRequest) { r.OneTimePreKeys = nil },
		func(r *pb.UploadPreKeysRequest) { r.KyberPreKeys[0].KeyId-- },
		func(r *pb.UploadPreKeysRequest) { r.KyberPreKeys[0].PublicKey[1] ^= 1 },
		func(r *pb.UploadPreKeysRequest) { r.KyberPreKeys[0].Signature[1] ^= 1 },
		func(r *pb.UploadPreKeysRequest) { r.KyberPreKeys[1].IsLastResort = false },
		func(r *pb.UploadPreKeysRequest) {
			r.KyberPreKeys[0], r.KyberPreKeys[1] = r.KyberPreKeys[1], r.KyberPreKeys[0]
		},
	} {
		changed := proto.Clone(req).(*pb.UploadPreKeysRequest)
		mutate(changed)
		got, err := PublicationDigest(changed)
		if err != nil || got == digest {
			t.Fatal("publication field not bound", err)
		}
	}
}

func TestPublicationRejectsUnknownFieldsAndRevisionOverflow(t *testing.T) {
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
	for _, field := range []string{"request", "context", "signed", "ec", "pq"} {
		t.Run(field, func(t *testing.T) {
			req := publicationFixture(t)
			var m proto.Message
			switch field {
			case "request":
				m = req
			case "context":
				m = req.Context
			case "signed":
				m = req.SignedPreKey
			case "ec":
				m = req.OneTimePreKeys[0]
			case "pq":
				m = req.KyberPreKeys[0]
			}
			m.ProtoReflect().SetUnknown(bytes.Clone(unknown))
			if _, err := PublicationDigest(req); err == nil {
				t.Fatal("unbound unknown field accepted")
			}
		})
	}
	for _, revision := range []uint64{math.MaxInt64, math.MaxUint64} {
		req := publicationFixture(t)
		req.ExpectedRevision = revision
		if ValidateUpload(req) == nil {
			t.Fatal("unrepresentable revision accepted")
		}
	}
	req := publicationFixture(t)
	req.ExpectedRevision = math.MaxInt64 - 1
	if err := ValidateUpload(req); err != nil {
		t.Fatal(err)
	}
}
