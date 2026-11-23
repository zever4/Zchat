package e2ee

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	"google.golang.org/protobuf/proto"
)

func blobDownloadWireFixture() *domain.E2EEBlobReadRequest {
	r := httpMetadataFixture().Reservation
	return &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Recipient.GenerationId}, Binding: &pb.EpochSendBinding{ProtocolVersion: 3, MessageId: r.MessageID, ConversationId: r.ConversationID, Epoch: *r.CryptoEpoch, Sender: r.Sender, Recipient: r.Recipient, EnvelopeDigest: make([]byte, 32)}, Blob: r.Blob}
}
func TestBlobDownloadMetadataVectorAndCanonicalBinding(t *testing.T) {
	data, err := os.ReadFile("../../proto/fixtures/attachment-download-http-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var v struct {
		Header string `json:"metadata_header"`
		JSON   string `json:"canonical_metadata_json"`
		Path   string `json:"path"`
	}
	if err = json.Unmarshal(data, &v); err != nil {
		t.Fatal(err)
	}
	r, err := DecodeBlobDownloadMetadata(v.Header, strings.TrimPrefix(v.Path, "/api/e2ee/attachments/"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := EncodeBlobDownloadMetadata(r)
	if err != nil || h != v.Header || r.Binding.Epoch != 1<<53+17 {
		t.Fatal("wire/precision changed", err)
	}
	raw, _ := base64.RawURLEncoding.DecodeString(h)
	if string(raw) != v.JSON {
		t.Fatal("canonical bytes changed")
	}
	other := blobDownloadWireFixture()
	header, err := EncodeBlobDownloadMetadata(other)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBlobDownloadMetadata(header, other.Binding.MessageId)
	if err != nil || !proto.Equal(got.Binding, other.Binding) || !proto.Equal(got.Context, other.Context) || !proto.Equal(got.Blob, other.Blob) {
		t.Fatal("lossy binding", err)
	}
	if _, err = DecodeBlobDownloadMetadata(header, uuid.NewString()); err == nil {
		t.Fatal("path not bound")
	}
	if _, err = EncodeBlobDownloadMetadata(nil); err == nil {
		t.Fatal("nil request")
	}
	other.Context.ProtocolVersion = 3
	if _, err = EncodeBlobDownloadMetadata(other); err == nil {
		t.Fatal("wrong control version")
	}
}
func TestBlobDownloadMetadataRejectsAmbiguousAndPrivateFields(t *testing.T) {
	r := blobDownloadWireFixture()
	h, err := EncodeBlobDownloadMetadata(r)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := base64.RawURLEncoding.DecodeString(h)
	raw := string(data)
	for name, input := range map[string]string{
		"whitespace": " " + raw, "duplicate": strings.Replace(raw, `"v":1`, `"v":1,"v":1`, 1), "private key": strings.Replace(raw, `"v":1`, `"v":1,"file_key":"not-allowed"`, 1),
		"attempt": strings.Replace(raw, `"v":1`, `"v":1,"attempt_id":"not-allowed"`, 1), "version": strings.Replace(raw, `"v":1`, `"v":2`, 1), "trailing": raw + `{}`,
		"numeric epoch": strings.Replace(raw, `"crypto_epoch":"9007199254740993"`, `"crypto_epoch":9007199254740993`, 1),
		"leading zero":  strings.Replace(raw, `"ciphertext_bytes":"65536"`, `"ciphertext_bytes":"065536"`, 1),
		"short digest":  strings.Replace(raw, `"envelope_digest":"`+strings.Repeat("0", 64)+`"`, `"envelope_digest":"00"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBlobDownloadMetadata(base64.RawURLEncoding.EncodeToString([]byte(input)), r.Binding.MessageId); err == nil {
				t.Fatal("ambiguous metadata accepted")
			}
		})
	}
	for _, bad := range []string{"", h + "=", h + "\n", strings.Repeat("x", 4097), "###"} {
		if _, err := DecodeBlobDownloadMetadata(bad, r.Binding.MessageId); err == nil {
			t.Fatal("invalid base64/length accepted")
		}
	}
}
