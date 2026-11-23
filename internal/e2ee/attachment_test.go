package e2ee

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	common "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

func attachmentFixture(t *testing.T) (*v2.EncryptedPayload, *v2.Envelope) {
	t.Helper()
	p := new(v2.EncryptedPayload)
	epochFixture(t, "attachment.bin", p)
	e := epochEnvelope(t)
	e.ProtocolVersion = AttachmentEnvelopeVersion
	a := p.GetAttachment()
	e.Blob = &v2.BlobRef{FormatVersion: 1, BlobId: a.BlobId, CiphertextBytes: binary.BigEndian.Uint64(a.PrivateDescriptor[65:]) + AttachmentBlobOverhead, Sha256: bytes.Clone(a.PrivateDescriptor[33:65])}
	return p, e
}
func attachmentSize(p *v2.EncryptedPayload, size uint64) {
	binary.BigEndian.PutUint64(p.GetAttachment().PrivateDescriptor[65:], size)
}

func TestSharedAttachmentPayloadRequiresPublicReferenceAndPreservesTextFraming(t *testing.T) {
	p, e := attachmentFixture(t)
	if err := ValidateEpochPayloadBinding(p, e); err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(v2.EncryptedPayload)
	if err = proto.Unmarshal(encoded, decoded); err != nil || !proto.Equal(decoded, p) {
		t.Fatal("private payload roundtrip failed")
	}
	p.FormatVersion = 2
	if ValidateEpochPayloadBinding(p, e) == nil {
		t.Fatal("attachment accepted under old private format")
	}
	p.FormatVersion = AttachmentPayloadFormat
	p.Content = &v2.EncryptedPayload_Text{Text: &common.TextMessage{Text: "unchanged text"}}
	if ValidateEpochPayloadBinding(p, e) == nil {
		t.Fatal("private attachment format used for text")
	}
	p.FormatVersion = 2
	if ValidateEpochPayloadBinding(p, e) == nil {
		t.Fatal("text accepted in attachment envelope")
	}
	textEnvelope := proto.Clone(e).(*v2.Envelope)
	textEnvelope.ProtocolVersion, textEnvelope.Blob = 2, nil
	if err := ValidateEpochPayloadBinding(p, textEnvelope); err != nil {
		t.Fatal(err)
	}
}

func TestAttachmentKindMIMEByteCapsAndPhotoDisclosurePresence(t *testing.T) {
	for _, c := range []struct {
		kind v2.AttachmentKind
		mime string
	}{
		{v2.AttachmentKind_ATTACHMENT_KIND_FILE, "application/octet-stream"},
		{v2.AttachmentKind_ATTACHMENT_KIND_PHOTO, "image/jpeg"},
		{v2.AttachmentKind_ATTACHMENT_KIND_PHOTO, "image/png"},
		{v2.AttachmentKind_ATTACHMENT_KIND_PHOTO, "image/gif"},
		{v2.AttachmentKind_ATTACHMENT_KIND_PHOTO, "image/webp"},
		{v2.AttachmentKind_ATTACHMENT_KIND_VOICE, "audio/ogg"},
		{v2.AttachmentKind_ATTACHMENT_KIND_VOICE, "audio/webm"},
	} {
		t.Run(c.mime, func(t *testing.T) {
			p, e := attachmentFixture(t)
			a := p.GetAttachment()
			a.Kind, a.MimeType = c.kind, c.mime
			max := uint64(MaxAttachmentMediaBytes)
			if c.kind == v2.AttachmentKind_ATTACHMENT_KIND_FILE {
				max = MaxAttachmentPlaintextBytes
			}
			if c.kind == v2.AttachmentKind_ATTACHMENT_KIND_PHOTO {
				a.PhotoMetadataStripped = proto.Bool(true)
			}
			attachmentSize(p, max)
			e.Blob.CiphertextBytes = max + AttachmentBlobOverhead
			if err := ValidateEpochPayloadBinding(p, e); err != nil {
				t.Fatal(err)
			}
			if c.kind == v2.AttachmentKind_ATTACHMENT_KIND_PHOTO {
				a.PhotoMetadataStripped = proto.Bool(false)
				if err := ValidateEpochPayloadBinding(p, e); err != nil {
					t.Fatal("explicit original rejected", err)
				}
				a.PhotoMetadataStripped = nil
				if ValidateEpochPayloadBinding(p, e) == nil {
					t.Fatal("missing photo policy accepted")
				}
				a.PhotoMetadataStripped = proto.Bool(true)
			} else {
				a.PhotoMetadataStripped = proto.Bool(false)
				if ValidateEpochPayloadBinding(p, e) == nil {
					t.Fatal("photo metadata flag on nonphoto accepted")
				}
				a.PhotoMetadataStripped = nil
			}
			attachmentSize(p, max+1)
			if ValidateEpochPayloadBinding(p, e) == nil {
				t.Fatal("oversize attachment accepted")
			}
		})
	}
}

func TestAttachmentInvalidPrivateMetadataAndUnknownFieldsFailClosed(t *testing.T) {
	base, e := attachmentFixture(t)
	mutations := map[string]func(*v2.EncryptedPayload){
		"old format":       func(p *v2.EncryptedPayload) { p.FormatVersion = 1 },
		"future format":    func(p *v2.EncryptedPayload) { p.FormatVersion = 4 },
		"nested format":    func(p *v2.EncryptedPayload) { p.GetAttachment().FormatVersion = 2 },
		"unknown kind":     func(p *v2.EncryptedPayload) { p.GetAttachment().Kind = 99 },
		"missing kind":     func(p *v2.EncryptedPayload) { p.GetAttachment().Kind = 0 },
		"URI instead UUID": func(p *v2.EncryptedPayload) { p.GetAttachment().BlobId = "https://example.invalid/blob" },
		"uppercase UUID":   func(p *v2.EncryptedPayload) { p.GetAttachment().BlobId = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" },
		"empty name":       func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = "" },
		"path traversal":   func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = "../secret.txt" },
		"drive path":       func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = "C:\\secret.txt" },
		"trailing space":   func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = "name " },
		"bidi":             func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = "name\u202egpj.exe" },
		"control":          func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = "line\nname" },
		"long name":        func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = strings.Repeat("界", 86) },
		"invalid UTF8":     func(p *v2.EncryptedPayload) { p.GetAttachment().Filename = string([]byte{0xff}) },
		"MIME parameters":  func(p *v2.EncryptedPayload) { p.GetAttachment().MimeType += ";extra=1" },
		"caption limit":    func(p *v2.EncryptedPayload) { p.GetAttachment().Caption = strings.Repeat("x", MaxTextBytes+1) },
		"missing descriptor": func(p *v2.EncryptedPayload) {
			p.GetAttachment().PrivateDescriptor = nil
		},
		"descriptor suffix": func(p *v2.EncryptedPayload) {
			p.GetAttachment().PrivateDescriptor = append(p.GetAttachment().PrivateDescriptor, 0)
		},
		"descriptor format": func(p *v2.EncryptedPayload) { p.GetAttachment().PrivateDescriptor[0] = 2 },
		"zero size":         func(p *v2.EncryptedPayload) { attachmentSize(p, 0) },
		"overflow size":     func(p *v2.EncryptedPayload) { attachmentSize(p, ^uint64(0)) },
		"missing epoch":     func(p *v2.EncryptedPayload) { p.CryptoEpoch = nil },
		"other epoch":       func(p *v2.EncryptedPayload) { p.CryptoEpoch = proto.Uint64(0) },
		"unknown nested field": func(p *v2.EncryptedPayload) {
			p.GetAttachment().ProtoReflect().SetUnknown([]byte{0x78, 1})
		},
		"unknown payload field": func(p *v2.EncryptedPayload) {
			p.ProtoReflect().SetUnknown([]byte{0x78, 1})
		},
		"typed nil attachment": func(p *v2.EncryptedPayload) { p.Content = (*v2.EncryptedPayload_Attachment)(nil) },
		"nil attachment":       func(p *v2.EncryptedPayload) { p.Content = &v2.EncryptedPayload_Attachment{} },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			p := proto.Clone(base).(*v2.EncryptedPayload)
			mutate(p)
			if ValidateEpochPayloadBinding(p, e) == nil {
				t.Fatal("invalid private metadata accepted")
			}
		})
	}
	base.GetAttachment().Filename = "файл.dat"
	base.GetAttachment().Caption = strings.Repeat("\x01", MaxTextBytes)
	if err := ValidateEpochPayloadBinding(base, e); err != nil {
		t.Fatal(err)
	}
}
