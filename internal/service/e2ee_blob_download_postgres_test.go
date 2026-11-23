//go:build linux

package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"google.golang.org/protobuf/proto"
)

// Real auth/session/device/PG/owned filesystem, with opaque synthetic Signal
// descriptor bytes. This is not native crypto or an HTTP/desktop acceptance test.
func acceptedDownloadPostgres(t *testing.T) (*postgresBlobUpload, string, *domain.E2EEBlobReadRequest) {
	t.Helper()
	f := blobUploadPostgres(t)
	if _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data))); err != nil {
		t.Fatal(err)
	}
	r := f.requests.Reservation
	epoch := *r.CryptoEpoch
	send := &v2.SendMessageRequest{Context: proto.Clone(r.Context).(*pb.ClientContext), Envelope: &v2.Envelope{
		ProtocolVersion: 3, CryptoSuite: 1, MessageId: r.MessageID, ConversationId: r.ConversationID,
		Sender: proto.Clone(r.Sender).(*pb.DeviceRef), Recipient: proto.Clone(r.Recipient).(*pb.DeviceRef), CryptoEpoch: &epoch,
		MessageType: 1, Ciphertext: []byte("opaque synthetic accepted attachment descriptor"), Blob: proto.Clone(r.Blob).(*v2.BlobRef)}}
	if _, err := repository.NewPgE2EEDeliveryRepository(f.db).SendV2(t.Context(), f.uid, send); err != nil {
		t.Fatal(err)
	}
	binding, err := boundary.EpochSendBinding(send.Envelope)
	if err != nil {
		t.Fatal(err)
	}
	token, _, _, err := f.auth.Login(t.Context(), "bob@blob.invalid", "Synthetic-blob-test-password")
	if err != nil {
		t.Fatal(err)
	}
	return f, token, &domain.E2EEBlobReadRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: r.Recipient.GenerationId}, Binding: binding, Blob: proto.Clone(r.Blob).(*v2.BlobRef)}
}

func TestBlobDownloadRealRecipientAndDescriptorAckPostgres(t *testing.T) {
	f, token, req := acceptedDownloadPostgres(t)
	// Descriptor ACK removes inbox data, never the separate file or its right.
	if err := repository.NewPgE2EEDeliveryRepository(f.db).AcknowledgeV2(t.Context(), 2, &pb.AcknowledgeMessagesRequest{Context: req.Context, Messages: []*pb.Acknowledgement{{MessageId: req.Binding.MessageId, EnvelopeDigest: req.Binding.EnvelopeDigest, Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}); err != nil {
		t.Fatal(err)
	}
	sink := &downloadSink{}
	if err := f.upload.Download(t.Context(), token, f.credential, req, sink); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sink.Bytes(), f.data) || sink.closes.Load() != 1 || f.budget.Active() {
		t.Fatal("real recipient download/ownership mismatch")
	}
	f.assertState(t, "linked")
	var links int
	if err := f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_message_blobs`).Scan(&links); err != nil || links != 1 {
		t.Fatal("read removed immutable link", links, err)
	}
}

func TestBlobDownloadLogoutInterruptsSinkWithoutAuthOrSQLBarrierPostgres(t *testing.T) {
	f, token, req := acceptedDownloadPostgres(t)
	sink := heldSink(true)
	done := make(chan error, 1)
	go func() { done <- f.upload.Download(t.Context(), token, f.credential, req, sink) }()
	awaitDownload(t, sink.entered)
	// Auth and all delivery rows must be available while the NETWORK write is
	// blocked. A streaming SQL/auth barrier would make this transaction fail.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, q := range []string{`SELECT id FROM users ORDER BY id FOR UPDATE`, `SELECT user_id FROM e2ee_devices ORDER BY user_id FOR UPDATE`,
		`SELECT conversation_id FROM e2ee_conversations FOR UPDATE`, `SELECT message_id FROM e2ee_message_claims FOR UPDATE`,
		`SELECT message_id FROM e2ee_send_receipts FOR UPDATE`, `SELECT message_id FROM e2ee_message_blobs FOR UPDATE`, `SELECT attempt_id FROM e2ee_blob_attempts FOR UPDATE`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			t.Fatal("SQL locks held during sink write", err)
		}
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = f.auth.Logout(ctx, token, false, func(int64, string) {}); err != nil {
		t.Fatal("logout blocked by sink", err)
	}
	if err = awaitDownloadResult(t, done); !errors.Is(err, domain.ErrInvalidSession) {
		t.Fatal(err)
	}
	if sink.closes.Load() != 1 || f.budget.Active() {
		t.Fatal("revoked stream did not join")
	}
	f.assertState(t, "linked") // No deletion/quota release/FILEACK from logout.
}

func TestBlobDownloadInvalidCredentialOrSenderBeforeSinkPostgres(t *testing.T) {
	f, token, req := acceptedDownloadPostgres(t)
	for _, name := range []string{"bad credential", "sender"} {
		t.Run(name, func(t *testing.T) {
			jwt, credential := token, f.credential
			if name == "bad credential" {
				credential = bytes.Repeat([]byte{99}, 32)
			} else {
				jwt = f.token
			}
			sink := &downloadSink{}
			if err := f.upload.Download(t.Context(), jwt, credential, req, sink); err == nil {
				t.Fatal("wrong principal/credential accepted")
			}
			if sink.writes.Load() != 0 || sink.closes.Load() != 1 || f.budget.Active() {
				t.Fatal("rejected read reached sink or leaked permit")
			}
		})
	}
	f.assertState(t, "linked")
}

func TestBlobDownloadRecipientReplacementStopsOldGenerationPostgres(t *testing.T) {
	f, token, req := acceptedDownloadPostgres(t)
	sink := heldSink(true)
	done := make(chan error, 1)
	go func() { done <- f.upload.Download(t.Context(), token, f.credential, req, sink) }()
	awaitDownload(t, sink.entered)
	devices := NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(f.db), f.auth)
	raw := bytes.Repeat([]byte{53}, 32)
	hash := E2EEDeviceCredentialHash(raw)
	identity := bytes.Repeat([]byte{54}, 33)
	identity[0] = 5
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	change, err := devices.Replace(ctx, 2, domain.E2EEDeviceRegistration{RequestID: uuid.NewString(), ExpectedGenerationID: req.Context.DeviceGenerationId, RegistrationID: 123, IdentityKey: identity, CredentialHash: hash[:]}, "Synthetic-blob-test-password", raw)
	if err != nil {
		t.Fatal(err)
	}
	if err = awaitDownloadResult(t, done); !errors.Is(err, domain.ErrE2EEDeviceCredential) {
		t.Fatal("old generation retained stream", err)
	}
	// Replacement keys/JWT never inherit an old accepted file or its descriptor.
	next := copyBlobRead(req)
	next.Context.DeviceGenerationId = change.Device.GenerationID
	next.Binding.Recipient.GenerationId = change.Device.GenerationID
	if err = f.upload.Download(t.Context(), token, raw, next, &downloadSink{}); !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
		t.Fatal("replacement inherited file", err)
	}
	f.assertState(t, "linked")
}
