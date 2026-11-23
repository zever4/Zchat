package e2ee

import (
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestBlobSendFailureBindsExactEnvelopeWithoutEchoingCiphertext(t *testing.T) {
	r := epochSend(blobEnvelope(t))
	binding, err := EpochSendBinding(r.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range []v2.BlobSendFailureReason{1, 2} {
		f := &v2.BlobSendFailure{Reason: reason, Binding: binding}
		if err = ValidateBlobSendFailure(r, f); err != nil || proto.Size(f) > 512 {
			t.Fatal("valid compact failure", err)
		}
		for fault := 0; fault < 11; fault++ {
			wrong := proto.Clone(r).(*v2.SendMessageRequest)
			e := wrong.Envelope
			switch fault {
			case 0:
				e.Ciphertext[0] ^= 1
			case 1:
				e.MessageId = e.ConversationId
			case 2:
				e.ConversationId = e.MessageId
			case 3:
				*e.CryptoEpoch++
			case 4:
				e.Sender.UserId = e.Recipient.UserId
			case 5:
				e.Recipient.GenerationId = e.Sender.GenerationId
			case 6:
				e.Blob.Sha256[0] ^= 1
			case 7:
				e.Blob.CiphertextBytes++
			case 8:
				e.Blob.BlobId = e.MessageId
			case 9:
				wrong.Context.DeviceGenerationId = e.Recipient.GenerationId
			case 10:
				e.ProtocolVersion = 2
				e.Blob = nil
			}
			if ValidateBlobSendFailure(wrong, f) == nil {
				t.Fatalf("accepted changed Send %d", fault)
			}
		}
	}
	for fault := 0; fault < 7; fault++ {
		f := &v2.BlobSendFailure{Reason: 1, Binding: proto.Clone(binding).(*pb.EpochSendBinding)}
		switch fault {
		case 0:
			f.Reason = 0
		case 1:
			f.Reason = 99
		case 2:
			f.Binding = nil
		case 3:
			f.Binding.ProtocolVersion = 2
		case 4:
			f.Binding.EnvelopeDigest = f.Binding.EnvelopeDigest[:31]
		case 5:
			f.ProtoReflect().SetUnknown([]byte{0x78, 1})
		case 6:
			f.Binding.ProtoReflect().SetUnknown([]byte{0x78, 1})
		}
		if ValidateBlobSendFailure(r, f) == nil {
			t.Fatalf("accepted malformed failure %d", fault)
		}
	}
	if ValidateBlobSendFailure(nil, nil) == nil || ValidateBlobSendFailure(r, nil) == nil {
		t.Fatal("nil accepted")
	}
}
