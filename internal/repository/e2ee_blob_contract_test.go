package repository

import (
	"context"
	"errors"
	"os"
	"testing"

	common "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

func TestInvalidAttachmentReferenceRejectedBeforeDatabase(t *testing.T) {
	raw, err := os.ReadFile("../../proto/fixtures/attachment-envelope-v3/envelope.bin")
	if err != nil {
		t.Fatal(err)
	}
	e := new(v2.Envelope)
	if err = proto.Unmarshal(raw, e); err != nil {
		t.Fatal(err)
	}
	req := &v2.SendMessageRequest{Context: &common.ClientContext{ProtocolVersion: 2, DeviceGenerationId: e.Sender.GenerationId}, Envelope: e}
	if err = boundary.ValidateEpochSend(req); err != nil {
		t.Fatal("fixture must be valid contract", err)
	}
	// Pure valid3 now has a repository implementation. Malformed public refs
	// must still be rejected before starting a transaction.
	req.Envelope.Blob.Sha256 = nil
	receipt, err := NewPgE2EEDeliveryRepository(nil).SendV2(context.Background(), 1, req)
	if receipt != nil || err == nil || errors.Is(err, boundary.ErrUnsupportedVersion) {
		t.Fatal("malformed attachment ref reached persistence", err)
	}
}
