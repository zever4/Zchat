package e2ee

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
)

func httpMetadataFixture() *domain.E2EEBlobAttemptRequest {
	epoch := uint64(1<<53 + 1)
	gen := uuid.NewString()
	return &domain.E2EEBlobAttemptRequest{AttemptID: uuid.NewString(), Revision: 1<<53 + 2, Reservation: &domain.E2EEBlobReservationRequest{
		Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: gen}, MessageID: uuid.NewString(), ConversationID: uuid.NewString(), CryptoEpoch: &epoch,
		Sender: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: gen, DeviceId: 1}, Recipient: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: uuid.NewString(), DeviceId: 1},
		Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: 65536, Sha256: make([]byte, 32)}, OperationID: uuid.NewString()}}
}

func TestBlobHTTPMetadataSharedWireVector(t *testing.T) {
	data, err := os.ReadFile("../../proto/fixtures/attachment-upload-http-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector struct {
		Header string `json:"metadata_header"`
		JSON   string `json:"canonical_metadata_json"`
		Path   string `json:"path"`
	}
	if err = json.Unmarshal(data, &vector); err != nil {
		t.Fatal(err)
	}
	header, err := base64.RawURLEncoding.DecodeString(vector.Header)
	if err != nil || string(header) != vector.JSON {
		t.Fatal("wire bytes changed", err)
	}
	a, err := DecodeBlobUploadMetadata(vector.Header, strings.TrimPrefix(vector.Path, "/api/e2ee/blobs/"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := EncodeBlobUploadMetadata(a)
	if err != nil || got != vector.Header || *a.Reservation.CryptoEpoch != 1<<53+17 {
		t.Fatal("shared wire vector/precision changed", err)
	}
}
func TestBlobHTTPMetadataCanonicalBinding(t *testing.T) {
	r := httpMetadataFixture()
	h, err := EncodeBlobUploadMetadata(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeBlobUploadMetadata(h, r.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	again, err := EncodeBlobUploadMetadata(got)
	if err != nil || h != again || *got.Reservation.CryptoEpoch != *r.Reservation.CryptoEpoch || got.Revision != r.Revision {
		t.Fatal("lossy counter roundtrip", err)
	}
	if _, err = DecodeBlobUploadMetadata(h, uuid.NewString()); err == nil {
		t.Fatal("path not bound")
	}
	if _, err = EncodeBlobUploadMetadata(nil); err == nil {
		t.Fatal("nil input")
	}
	r.Reservation.Context.ProtocolVersion = 3
	if _, err = EncodeBlobUploadMetadata(r); err == nil {
		t.Fatal("wrong control protocol")
	}
	data, _ := base64.RawURLEncoding.DecodeString(h)
	for name, raw := range map[string]string{
		"empty": "", "whitespace": " " + string(data), "duplicate": strings.Replace(string(data), `"v":1`, `"v":1,"v":1`, 1),
		"unknown": strings.Replace(string(data), `"v":1`, `"v":1,"secret":"no"`, 1), "version": strings.Replace(string(data), `"v":1`, `"v":2`, 1),
		"numericcounter": strings.Replace(string(data), `"crypto_epoch":"9007199254740993"`, `"crypto_epoch":9007199254740993`, 1),
		"leadingzero":    strings.Replace(string(data), `"crypto_epoch":"9007199254740993"`, `"crypto_epoch":"09007199254740993"`, 1),
		"absentzero":     strings.Replace(string(data), `"expected_revision":"0",`, "", 1), "trailing": string(data) + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBlobUploadMetadata(base64.RawURLEncoding.EncodeToString([]byte(raw)), r.AttemptID); err == nil {
				t.Fatal("noncanonical/ambiguous metadata accepted")
			}
		})
	}
	for _, invalid := range []string{h + "=", strings.Repeat("x", MaxBlobUploadMetadataHeader+1), "###"} {
		if _, err := DecodeBlobUploadMetadata(invalid, r.AttemptID); err == nil {
			t.Fatal("invalid header accepted")
		}
	}
}
