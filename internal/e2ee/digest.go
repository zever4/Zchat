package e2ee

import (
	"crypto/sha256"
	"encoding/binary"

	pb "github.com/zever4/Zchat/gen/e2ee"
)

// EnvelopeDigest hashes a versioned, explicitly ordered frame, not protobuf
// serialization (which is not canonical across languages/runtime versions).
// UUIDs are validated lowercase UUIDv4 strings, exactly 36 ASCII bytes each.
func EnvelopeDigest(envelope *pb.Envelope) ([32]byte, error) {
	if err := ValidateEnvelope(envelope); err != nil {
		return [32]byte{}, err
	}
	hash := sha256.New()
	hash.Write([]byte("zchat.e2ee.envelope.v1\x00"))
	writeUint32 := func(value uint32) {
		var bytes [4]byte
		binary.BigEndian.PutUint32(bytes[:], value)
		hash.Write(bytes[:])
	}
	writeDevice := func(ref *pb.DeviceRef) {
		hash.Write([]byte(ref.UserId))
		writeUint32(ref.DeviceId)
		hash.Write([]byte(ref.GenerationId))
	}
	writeUint32(envelope.ProtocolVersion)
	writeUint32(uint32(envelope.CryptoSuite))
	hash.Write([]byte(envelope.MessageId))
	hash.Write([]byte(envelope.ConversationId))
	writeDevice(envelope.Sender)
	writeDevice(envelope.Recipient)
	writeUint32(uint32(envelope.MessageType))
	writeUint32(uint32(len(envelope.Ciphertext)))
	hash.Write(envelope.Ciphertext)
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}

// PublicationDigest includes the expected revision and repeated-key order.
// Unknown fields are rejected, so no unbound v1 semantics can reach storage.
func PublicationDigest(req *pb.UploadPreKeysRequest) ([32]byte, error) {
	if err := ValidateUpload(req); err != nil {
		return [32]byte{}, err
	}
	hash := sha256.New()
	hash.Write([]byte("zchat.e2ee.publication.v1\x00"))
	u32 := func(v uint32) { var b [4]byte; binary.BigEndian.PutUint32(b[:], v); hash.Write(b[:]) }
	u64 := func(v uint64) { var b [8]byte; binary.BigEndian.PutUint64(b[:], v); hash.Write(b[:]) }
	u32(req.Context.ProtocolVersion)
	hash.Write([]byte(req.Context.DeviceGenerationId))
	hash.Write([]byte(req.BatchId))
	u64(req.ExpectedRevision)
	if k := req.SignedPreKey; k != nil {
		u32(1)
		u32(k.KeyId)
		hash.Write(k.PublicKey)
		hash.Write(k.Signature)
		u64(uint64(k.CreatedAtUnixMs))
	} else {
		u32(0)
	}
	u32(uint32(len(req.OneTimePreKeys)))
	for _, k := range req.OneTimePreKeys {
		u32(k.KeyId)
		hash.Write(k.PublicKey)
	}
	u32(uint32(len(req.KyberPreKeys)))
	for _, k := range req.KyberPreKeys {
		u32(k.KeyId)
		hash.Write(k.PublicKey)
		hash.Write(k.Signature)
		if k.IsLastResort {
			u32(1)
		} else {
			u32(0)
		}
	}
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, nil
}
