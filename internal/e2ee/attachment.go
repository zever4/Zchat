package e2ee

// Private plaintext contract helpers only. The Go messaging server receives
// ciphertext, never this descriptor/key/name/MIME, and must not log this proto.
import (
	"encoding/binary"
	"strings"
	"unicode"
	"unicode/utf8"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

const AttachmentPayloadFormat = 3
const MaxAttachmentPlaintextBytes = 50 * 1024 * 1024
const MaxAttachmentMediaBytes = 10 * 1024 * 1024

func attachmentName(value string) bool {
	if value == "" || len(value) > 255 || !utf8.ValidString(value) || strings.TrimSpace(value) != value || value == "." || value == ".." || strings.HasSuffix(value, ".") {
		return false
	}
	for _, c := range value {
		if unicode.IsControl(c) || strings.ContainsRune("<>:\"/\\|?*", c) || c >= '\u202a' && c <= '\u202e' || c >= '\u2066' && c <= '\u2069' {
			return false
		}
	}
	return true
}

func validateAttachment(p *v2.EncryptedPayload, a *v2.AttachmentMessage) error {
	if p.FormatVersion != AttachmentPayloadFormat || !recoveryKnown(a) || a.FormatVersion != 1 || !uuid4(a.BlobId) || !attachmentName(a.Filename) || len(a.Caption) > MaxTextBytes || !utf8.ValidString(a.Caption) || proto.Size(a) > 4096 {
		return invalid("private attachment payload")
	}
	// Native descriptor1 = version/key32/public ciphertext SHA256/size u64BE.
	// Parsing structure is not AEAD verification or a durable-file ACK.
	d := a.PrivateDescriptor
	if len(d) != 73 || d[0] != 1 {
		return invalid("private attachment descriptor")
	}
	size := binary.BigEndian.Uint64(d[65:])
	if size == 0 || size > MaxAttachmentPlaintextBytes {
		return invalid("private attachment size")
	}
	switch a.Kind {
	case v2.AttachmentKind_ATTACHMENT_KIND_PHOTO:
		if size > MaxAttachmentMediaBytes || a.PhotoMetadataStripped == nil || (a.MimeType != "image/jpeg" && a.MimeType != "image/png" && a.MimeType != "image/gif" && a.MimeType != "image/webp") {
			return invalid("private photo metadata")
		}
	case v2.AttachmentKind_ATTACHMENT_KIND_FILE:
		if a.PhotoMetadataStripped != nil || a.MimeType != "application/octet-stream" {
			return invalid("private file metadata")
		}
	case v2.AttachmentKind_ATTACHMENT_KIND_VOICE:
		if size > MaxAttachmentMediaBytes || a.PhotoMetadataStripped != nil || (a.MimeType != "audio/ogg" && a.MimeType != "audio/webm") {
			return invalid("private voice metadata")
		}
	default:
		return invalid("private attachment kind")
	}
	return nil
}
