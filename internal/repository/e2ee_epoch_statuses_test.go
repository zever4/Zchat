package repository

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

func (f *epochPersistenceFixture) statusRequest(requests ...*v2.SendMessageRequest) *pb.GetMessageStatusesRequest {
	r := &pb.GetMessageStatusesRequest{Context: f.context(1)}
	for _, sent := range requests {
		d, _ := boundary.EpochEnvelopeDigest(sent.Envelope)
		r.Messages = append(r.Messages, &pb.MessageReference{MessageId: sent.Envelope.MessageId, EnvelopeDigest: d[:]})
	}
	return r
}

func TestE2EEEpochStatusSealedAndLateAckPostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	old := f.message(0)
	if _, err := f.delivery.SendV2(t.Context(), 1, old); err != nil {
		t.Fatal(err)
	}
	f.transition(t)
	current := f.message(1)
	if _, err := f.delivery.SendV2(t.Context(), 1, current); err != nil {
		t.Fatal(err)
	}
	request := f.statusRequest(current, old) // Request order, not epoch/SQL order.
	got, err := f.delivery.StatusesV2(t.Context(), 1, request)
	if err != nil || boundary.ValidateEpochStatusResponse(request, got) != nil {
		t.Fatal("status binding", err)
	}
	for i, epoch := range []uint64{1, 0} {
		if got.Messages[i].Receipt.GetCryptoEpoch() != epoch || got.Messages[i].State != pb.MessageState_MESSAGE_STATE_PENDING {
			t.Fatal("wrong pending epoch")
		}
	}
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(old)); err != nil {
		t.Fatal(err)
	}
	got, err = f.delivery.StatusesV2(t.Context(), 1, request)
	if err != nil || got.Messages[1].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED || got.Messages[0].State != pb.MessageState_MESSAGE_STATE_PENDING {
		t.Fatal("sealed ACK/status", err)
	}
	// Controlled fixture time only: does not claim a real 30-day soak test.
	expireMessage(t, f.f.db, current.Envelope.MessageId)
	expired, err := f.delivery.StatusesV2(t.Context(), 1, f.statusRequest(current))
	if err != nil || expired.Messages[0].State != pb.MessageState_MESSAGE_STATE_EXPIRED {
		t.Fatal("expiry status", err)
	}
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(current)); err != nil {
		t.Fatal("late ACK", err)
	}
	confirmed, err := f.delivery.StatusesV2(t.Context(), 1, f.statusRequest(current))
	if err != nil || confirmed.Messages[0].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED {
		t.Fatal("late refinement", err)
	}
	var reason string
	var removed time.Time
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT reason,removed_at FROM e2ee_message_removals WHERE message_id=$1`, current.Envelope.MessageId).Scan(&reason, &removed); err != nil || reason != "expired" || removed.UnixMilli() != expired.Messages[0].ObservedAtUnixMs {
		t.Fatal("lost expiry fact", err)
	}
	registryCount(t, f.f.db, "e2ee_message_inbox", 0)
	registryCount(t, f.f.db, "e2ee_message_acknowledgements", 2)
}

func TestE2EEEpochStatusOwnershipAndMixedVersionRollbackPostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	modern := f.message(0)
	if _, err := f.delivery.SendV2(t.Context(), 1, modern); err != nil {
		t.Fatal(err)
	}
	oldEnvelope := proto.Clone(modern.Envelope).(*v2.Envelope)
	legacy := &pb.SendMessageRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.gen(1)}, Envelope: &pb.Envelope{ProtocolVersion: 1, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: f.chat, Sender: oldEnvelope.Sender, Recipient: oldEnvelope.Recipient, MessageType: 1, Ciphertext: []byte("prototype test only")}}
	if _, err := f.delivery.Send(t.Context(), 1, legacy); err != nil {
		t.Fatal(err)
	}
	expireMessage(t, f.f.db, modern.Envelope.MessageId)
	request := f.statusRequest(modern)
	d, _ := boundary.EnvelopeDigest(legacy.Envelope)
	request.Messages = append(request.Messages, &pb.MessageReference{MessageId: legacy.Envelope.MessageId, EnvelopeDigest: d[:]})
	if _, err := f.delivery.StatusesV2(t.Context(), 1, request); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("mixed-version adoption", err)
	}
	registryCount(t, f.f.db, "e2ee_message_removals", 0)
	registryCount(t, f.f.db, "e2ee_message_inbox", 2)
	request = f.statusRequest(modern)
	request.Context = f.context(2)
	if _, err := f.delivery.StatusesV2(t.Context(), 2, request); !errors.Is(err, domain.ErrE2EEMessageNotFound) {
		t.Fatal("recipient read sender status", err)
	}
	request = f.statusRequest(modern)
	request.Messages[0].EnvelopeDigest = bytes.Repeat([]byte{0}, 32)
	if _, err := f.delivery.StatusesV2(t.Context(), 1, request); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("wrong digest", err)
	}
	registryCount(t, f.f.db, "e2ee_message_removals", 0)
}
