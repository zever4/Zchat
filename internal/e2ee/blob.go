package e2ee

// Public attachment identity and data framing only. These helpers grant no
// storage/upload permission and do not enable attachment Send on the server.
import v2 "github.com/zever4/Zchat/gen/e2eev2"

const AttachmentEnvelopeVersion = 3
const AttachmentBlobOverhead = 36
const MaxAttachmentBlobBytes = MaxAttachmentPlaintextBytes + AttachmentBlobOverhead

func epochFramingVersion(version uint32) error {
	if version != 2 && version != AttachmentEnvelopeVersion {
		return ErrUnsupportedVersion
	}
	return nil
}

func ValidateBlobRef(ref *v2.BlobRef) error {
	if !recoveryKnown(ref) {
		return invalid("public blob reference")
	}
	if ref.FormatVersion != 1 {
		return ErrUnsupportedVersion
	}
	if !uuid4(ref.BlobId) || ref.CiphertextBytes <= AttachmentBlobOverhead || ref.CiphertextBytes > MaxAttachmentBlobBytes || len(ref.Sha256) != 32 {
		return invalid("public blob reference fields")
	}
	return nil
}
