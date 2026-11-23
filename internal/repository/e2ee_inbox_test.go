package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

func pendingFor(req *pb.SendMessageRequest) *pb.GetPendingMessagesRequest {
	return &pb.GetPendingMessagesRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: req.Envelope.Recipient.GenerationId}}
}
func ackFor(req *pb.SendMessageRequest) *pb.AcknowledgeMessagesRequest {
	digest, _ := boundary.EnvelopeDigest(req.Envelope)
	return &pb.AcknowledgeMessagesRequest{Context: pendingFor(req).Context, Messages: []*pb.Acknowledgement{{MessageId: req.Envelope.MessageId, EnvelopeDigest: digest[:], Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}
}
func statusesFor(requests ...*pb.SendMessageRequest) *pb.GetMessageStatusesRequest {
	result := &pb.GetMessageStatusesRequest{Context: requests[0].Context}
	for _, req := range requests {
		digest, _ := boundary.EnvelopeDigest(req.Envelope)
		result.Messages = append(result.Messages, &pb.MessageReference{MessageId: req.Envelope.MessageId, EnvelopeDigest: digest[:]})
	}
	return result
}
func savedMessage(t *testing.T, r *PgE2EEDeliveryRepository, uid int64, req *pb.SendMessageRequest) *domain.E2EESendReceipt {
	t.Helper()
	receipt, err := r.Send(t.Context(), uid, req)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
func messageState(t *testing.T, r *PgE2EEDeliveryRepository, req *pb.SendMessageRequest, want pb.MessageState) *pb.MessageStatus {
	t.Helper()
	got, err := r.Statuses(t.Context(), 1, statusesFor(req))
	if err != nil || len(got.GetMessages()) != 1 || got.Messages[0].State != want {
		t.Fatalf("status want %v got %v: %v", want, got, err)
	}
	return got.Messages[0]
}

func TestE2EEInboxCountAndBytesBoundedNonDestructivePostgres(t *testing.T) {
	f, r, req := messageDB(t)
	for range 35 {
		savedMessage(t, r, 1, nextMessage(req))
	}
	first, err := r.Pending(t.Context(), 2, pendingFor(req))
	if err != nil || len(first.GetMessages()) != boundary.MaxInboxMessages {
		t.Fatal("bounded inbox", first, err)
	}
	for i, d := range first.Messages {
		if d.Sequence != int64(i+1) || len(d.EnvelopeDigest) != 32 {
			t.Fatal("sequence/digest")
		}
	}
	again, err := r.Pending(t.Context(), 2, pendingFor(req))
	if err != nil || !proto.Equal(first, again) {
		t.Fatal("read consumed inbox", err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 35)
	ack := &pb.AcknowledgeMessagesRequest{Context: pendingFor(req).Context}
	for _, d := range first.Messages {
		ack.Messages = append(ack.Messages, &pb.Acknowledgement{MessageId: d.Envelope.MessageId, EnvelopeDigest: d.EnvelopeDigest, Disposition: 1})
	}
	if err := r.Acknowledge(t.Context(), 2, ack); err != nil {
		t.Fatal(err)
	}
	rest, err := r.Pending(t.Context(), 2, pendingFor(req))
	if err != nil || len(rest.GetMessages()) != 3 || rest.Messages[0].Sequence != 33 {
		t.Fatal("page advance after durable ACK", err)
	}
	for _, d := range rest.Messages {
		ack.Messages = []*pb.Acknowledgement{{MessageId: d.Envelope.MessageId, EnvelopeDigest: d.EnvelopeDigest, Disposition: 1}}
		if err := r.Acknowledge(t.Context(), 2, ack); err != nil {
			t.Fatal(err)
		}
	}
	// A near-request-limit envelope still fits a response; two do not. Count
	// bounds alone are insufficient because ciphertext length is variable.
	large := nextMessage(req)
	large.Envelope.Ciphertext = bytes.Repeat([]byte{9}, boundary.MaxRequestBytes-512)
	savedMessage(t, r, 1, large)
	savedMessage(t, r, 1, nextMessage(large))
	page, err := r.Pending(t.Context(), 2, pendingFor(req))
	if err != nil || len(page.GetMessages()) != 1 || proto.Size(page) > boundary.MaxResponseBytes || !proto.Equal(page.Messages[0].Envelope, large.Envelope) {
		t.Fatal("byte-bounded page", err)
	}
}

func TestE2EEInboxAuthorizationRetiredSenderAndCorruptionPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	savedMessage(t, r, 1, req)
	for _, uid := range []int64{0, 1, 3} {
		if _, err := r.Pending(t.Context(), uid, pendingFor(req)); err == nil {
			t.Fatal("foreign read", uid)
		}
	}
	own, err := r.Pending(t.Context(), 1, &pb.GetPendingMessagesRequest{Context: req.Context})
	if err != nil || len(own.Messages) != 0 {
		t.Fatal("sender saw receiver inbox", err)
	}
	f.replace(t, 1, f.low.GenerationID)
	got, err := r.Pending(t.Context(), 2, pendingFor(req))
	if err != nil || len(got.Messages) != 1 {
		t.Fatal("accepted data lost with sender retirement", err)
	}
	// Isolated test schema corruption must fail closed, not return altered wire.
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_message_inbox SET envelope=decode('0102','hex')`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Pending(t.Context(), 2, pendingFor(req)); !errors.Is(err, domain.ErrE2EEMessageState) {
		t.Fatal("corrupt ciphertext escaped", err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	f.replace(t, 2, f.high.GenerationID)
	if _, err := r.Pending(t.Context(), 2, pendingFor(req)); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired receiver", err)
	}
}

func TestE2EEAckAtomicIdempotentAndStatusPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	receipt := savedMessage(t, r, 1, req)
	pending := messageState(t, r, req, pb.MessageState_MESSAGE_STATE_PENDING)
	if pending.Receipt.StoredAtUnixMs != receipt.StoredAt.UnixMilli() {
		t.Fatal("receipt drift")
	}
	ack := ackFor(req)
	if err := r.Acknowledge(t.Context(), 2, ack); err != nil {
		t.Fatal(err)
	}
	first := messageState(t, r, req, pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED)
	if err := r.Acknowledge(t.Context(), 2, ack); err != nil {
		t.Fatal("lost ACK response retry", err)
	}
	if again := messageState(t, r, req, pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED); !proto.Equal(first, again) {
		t.Fatal("ACK timestamp changed")
	}
	ack.Messages[0].Disposition = pb.AckDisposition_ACK_DISPOSITION_REJECTED
	if err := r.Acknowledge(t.Context(), 2, ack); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("contradictory ACK", err)
	}
	if again := savedMessage(t, r, 1, req); *again != *receipt {
		t.Fatal("removed body lost dedup receipt")
	}
	messageCounters(t, f.db, f.low.GenerationID, 1, 0, 0, 0)
	messageCounters(t, f.db, f.high.GenerationID, 0, 0, 1, 0)
	registryCount(t, f.db, "e2ee_message_inbox", 0)
	registryCount(t, f.db, "e2ee_send_receipts", 1)
	registryCount(t, f.db, "e2ee_message_acknowledgements", 1)
	registryCount(t, f.db, "e2ee_message_removals", 1)
	rejected := nextMessage(req)
	savedMessage(t, r, 1, rejected)
	reject := ackFor(rejected)
	reject.Messages[0].Disposition = pb.AckDisposition_ACK_DISPOSITION_REJECTED
	if err := r.Acknowledge(t.Context(), 2, reject); err != nil {
		t.Fatal(err)
	}
	messageState(t, r, rejected, pb.MessageState_MESSAGE_STATE_RECIPIENT_REJECTED)
	for _, query := range []string{`UPDATE e2ee_message_acknowledgements SET disposition='rejected' WHERE disposition='stored'`, `UPDATE e2ee_message_removals SET reason='expired'`} {
		if _, err := f.db.ExecContext(t.Context(), query); err == nil {
			t.Fatal("immutable fact changed")
		}
	}
}

func TestE2EEAckAndStatusBatchRollbackAndOwnershipPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	second := nextMessage(req)
	savedMessage(t, r, 1, req)
	savedMessage(t, r, 1, second)
	batch := ackFor(req)
	batch.Messages = append(batch.Messages, ackFor(second).Messages[0])
	batch.Messages[1].EnvelopeDigest[0] ^= 1
	if err := r.Acknowledge(t.Context(), 2, batch); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("bad digest accepted", err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 2)
	registryCount(t, f.db, "e2ee_message_acknowledgements", 0)
	for _, uid := range []int64{1, 3} {
		if err := r.Acknowledge(t.Context(), uid, ackFor(req)); !errors.Is(err, domain.ErrE2EEMessageNotFound) {
			t.Fatal("foreign ACK", uid, err)
		}
	}
	unknown := ackFor(req)
	unknown.Messages = append(unknown.Messages, &pb.Acknowledgement{MessageId: uuid.NewString(), EnvelopeDigest: make([]byte, 32), Disposition: 1})
	if err := r.Acknowledge(t.Context(), 2, unknown); !errors.Is(err, domain.ErrE2EEMessageNotFound) {
		t.Fatal("unknown ACK", err)
	}
	if _, err := r.Statuses(t.Context(), 2, statusesFor(req)); !errors.Is(err, domain.ErrE2EEMessageNotFound) {
		t.Fatal("foreign status", err)
	}
	statuses := statusesFor(second, req)
	got, err := r.Statuses(t.Context(), 1, statuses)
	if err != nil || got.Messages[0].Receipt.MessageId != second.Envelope.MessageId {
		t.Fatal("request order", err)
	}
	// Status cleanup is transactional too: the valid first element is rolled
	// back if the following element has an invalid binding.
	expireMessage(t, f.db, second.Envelope.MessageId)
	statuses.Messages[1].EnvelopeDigest[0] ^= 1
	if _, err := r.Statuses(t.Context(), 1, statuses); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("status conflict", err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 2)
	registryCount(t, f.db, "e2ee_message_removals", 0)
}

// Test-only clock fixture. The immutable ledger trigger is suspended within a
// transaction in this disposable schema, never a production TTL configuration.
func expireMessage(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var gen string
	var seq int64
	var wire []byte
	if err := tx.QueryRowContext(t.Context(), `DELETE FROM e2ee_message_inbox WHERE message_id=$1 RETURNING recipient_generation_id::text,sequence,envelope`, id).Scan(&gen, &seq, &wire); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `ALTER TABLE e2ee_send_receipts DISABLE TRIGGER e2ee_guard_send_receipt`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `WITH moment AS MATERIALIZED (SELECT date_trunc('milliseconds',clock_timestamp()) AS now) UPDATE e2ee_send_receipts SET stored_at=moment.now-INTERVAL '31 days',expires_at=moment.now-INTERVAL '1 day' FROM moment WHERE message_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `ALTER TABLE e2ee_send_receipts ENABLE TRIGGER e2ee_guard_send_receipt`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(t.Context(), `INSERT INTO e2ee_message_inbox(recipient_generation_id,sequence,message_id,envelope,expires_at) SELECT $1,$2,message_id,$3,expires_at FROM e2ee_send_receipts WHERE message_id=$4`, gen, seq, wire, id); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestE2EEOutcomeExpiryLateAckAndReplacementPostgres(t *testing.T) {
	for _, lateRejected := range []bool{false, true} {
		t.Run(map[bool]string{false: "stored", true: "rejected"}[lateRejected], func(t *testing.T) {
			f, r, req := messageDB(t)
			savedMessage(t, r, 1, req)
			expireMessage(t, f.db, req.Envelope.MessageId)
			page, err := r.Pending(t.Context(), 2, pendingFor(req))
			if err != nil || len(page.Messages) != 0 {
				t.Fatal("expired body returned", err)
			}
			registryCount(t, f.db, "e2ee_message_inbox", 1) // Read alone does not remove it.
			expired := messageState(t, r, req, pb.MessageState_MESSAGE_STATE_EXPIRED)
			registryCount(t, f.db, "e2ee_message_inbox", 0)
			messageCounters(t, f.db, f.high.GenerationID, 0, 0, 1, 0)
			ack := ackFor(req)
			want := pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED
			if lateRejected {
				ack.Messages[0].Disposition = 2
				want = pb.MessageState_MESSAGE_STATE_RECIPIENT_REJECTED
			}
			if err := r.Acknowledge(t.Context(), 2, ack); err != nil {
				t.Fatal("late ACK rejected", err)
			}
			confirmed := messageState(t, r, req, want)
			if confirmed.ObservedAtUnixMs < expired.ObservedAtUnixMs {
				t.Fatal("backward time")
			}
			var reason string
			var at time.Time
			if err := f.db.QueryRowContext(t.Context(), `SELECT reason,removed_at FROM e2ee_message_removals WHERE message_id=$1`, req.Envelope.MessageId).Scan(&reason, &at); err != nil || reason != "expired" || at.UnixMilli() != expired.ObservedAtUnixMs {
				t.Fatal("expiry fact overwritten", err)
			}
			f.replace(t, 2, f.high.GenerationID)
			messageState(t, r, req, want) // Existing ACK is stronger than retirement.
			if err := r.Acknowledge(t.Context(), 2, ack); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
				t.Fatal("old installation ACK", err)
			}
			registryCount(t, f.db, "e2ee_message_inbox", 0)
		})
	}
	f, r, req := messageDB(t)
	savedMessage(t, r, 1, req)
	replacement := f.replace(t, 2, f.high.GenerationID)
	status := messageState(t, r, req, pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED)
	if status.Receipt.MessageId != req.Envelope.MessageId {
		t.Fatal("missing original receipt")
	}
	ack := ackFor(req)
	ack.Context.DeviceGenerationId = replacement.GenerationID
	if err := r.Acknowledge(t.Context(), 2, ack); !errors.Is(err, domain.ErrE2EEMessageNotFound) {
		t.Fatal("new installation ACKed old envelope", err)
	}
	messageCounters(t, f.db, f.high.GenerationID, 0, 0, 1, 0)
}

func TestE2EEAckCommitFailureAndCounterFailureRollbackPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	savedMessage(t, r, 1, req)
	if _, err := f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic ACK commit failure'; END $$; CREATE CONSTRAINT TRIGGER reject_ack AFTER INSERT ON e2ee_message_acknowledgements DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_ack()`); err != nil {
		t.Fatal(err)
	}
	if err := r.Acknowledge(t.Context(), 2, ackFor(req)); err == nil {
		t.Fatal("false successful ACK")
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_message_acknowledgements", 0)
	registryCount(t, f.db, "e2ee_message_removals", 0)
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_ack ON e2ee_message_acknowledgements; DROP FUNCTION reject_ack(); UPDATE e2ee_delivery_state SET pending_messages=0 WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if err := r.Acknowledge(t.Context(), 2, ackFor(req)); !errors.Is(err, domain.ErrE2EEMessageState) {
		t.Fatal("counter corruption committed", err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_message_acknowledgements", 0)
}

func TestE2EEOutcomeConcurrentAckStatusSendAndRetirementPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	other := reverseMessage(req)
	savedMessage(t, r, 1, req)
	savedMessage(t, r, 2, other)
	var wg sync.WaitGroup
	for i := range 24 {
		wg.Go(func() {
			var err error
			switch i % 4 {
			case 0:
				err = r.Acknowledge(t.Context(), 2, ackFor(req))
			case 1:
				_, err = r.Statuses(t.Context(), 1, statusesFor(req))
			case 2:
				err = r.Acknowledge(t.Context(), 1, ackFor(other))
			case 3:
				_, err = r.Statuses(t.Context(), 2, statusesFor(other))
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	for range 6 {
		wg.Go(func() {
			if _, err := r.Send(t.Context(), 1, nextMessage(req)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	registryCount(t, f.db, "e2ee_message_acknowledgements", 2)
	registryCount(t, f.db, "e2ee_message_removals", 2)
	registryCount(t, f.db, "e2ee_message_inbox", 6)
	// A real device replacement racing ACK/status must not produce split facts.
	last := nextMessage(req)
	savedMessage(t, r, 1, last)
	wg.Go(func() {
		err := r.Acknowledge(t.Context(), 2, ackFor(last))
		if err != nil && !errors.Is(err, domain.ErrE2EEGenerationChanged) {
			t.Error(err)
		}
	})
	wg.Go(func() { f.replace(t, 2, f.high.GenerationID) })
	wg.Go(func() {
		if _, err := r.Statuses(t.Context(), 1, statusesFor(last)); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	result, err := r.Statuses(t.Context(), 1, statusesFor(last))
	if err != nil {
		t.Fatal(err)
	}
	if s := result.Messages[0].State; s != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED && s != pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED {
		t.Fatal("impossible race outcome", s)
	}
	var mismatch int
	if err := f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_delivery_state d WHERE d.pending_messages<>(SELECT count(*) FROM e2ee_message_inbox i WHERE i.recipient_generation_id=d.generation_id) OR d.pending_bytes<>COALESCE((SELECT sum(octet_length(envelope)) FROM e2ee_message_inbox i WHERE i.recipient_generation_id=d.generation_id),0)`).Scan(&mismatch); err != nil || mismatch != 0 {
		t.Fatal("counter drift", err)
	}
}

func TestE2EEInboxAndAckRespectCancellationPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	savedMessage(t, r, 1, req)
	lock, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(t.Context(), `SELECT id FROM users WHERE id=2 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"pending", "ack", "status"} {
		ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
		var err error
		switch operation {
		case "pending":
			_, err = r.Pending(ctx, 2, pendingFor(req))
		case "ack":
			err = r.Acknowledge(ctx, 2, ackFor(req))
		case "status":
			_, err = r.Statuses(ctx, 1, statusesFor(req))
		}
		cancel()
		if err == nil {
			t.Fatal("ignored lock deadline", operation)
		}
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_message_acknowledgements", 0)
}

func TestE2EEOutcomeMigrationGuardsPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 24); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty Down", err)
	}
	if err := goose.UpTo(db, "../../migrations", 24); err != nil {
		t.Fatal(err)
	}
	f := seedConversations(t, db)
	c := f.create(t)
	r := NewPgE2EEDeliveryRepository(db)
	req := &pb.SendMessageRequest{Context: f.req.Context, Envelope: &pb.Envelope{ProtocolVersion: 1, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: c.ID, Sender: &pb.DeviceRef{UserId: f.low.UserUUID, DeviceId: 1, GenerationId: f.low.GenerationID}, Recipient: &pb.DeviceRef{UserId: f.high.UserUUID, DeviceId: 1, GenerationId: f.high.GenerationID}, MessageType: 1, Ciphertext: []byte{1}}}
	seedHistoricalE2EESend(t, db, req.Envelope)
	if err := r.Acknowledge(t.Context(), 2, ackFor(req)); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("nonempty Down erased facts")
	}
	registryCount(t, db, "e2ee_message_acknowledgements", 1)
	registryCount(t, db, "e2ee_message_removals", 1)
}

func TestE2EEOutcomeMultiPeerBatchLockOrderPostgres(t *testing.T) {
	f, r, toBob := messageDB(t)
	third, err := f.devices.Register(t.Context(), 3, registrationMaterial())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewPgE2EEPreKeyRepository(f.db).Publish(t.Context(), 3, testutil.PublicPreKeyUpload(t, third.Device.GenerationID)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES(1,3,'accepted')`); err != nil {
		t.Fatal(err)
	}
	chat, err := f.repo.Create(t.Context(), 1, &pb.CreateConversationRequest{Context: f.req.Context, PeerUserId: 3, CreationRequestId: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	toThird := nextMessage(toBob)
	toThird.Envelope.ConversationId = chat.ID
	toThird.Envelope.Recipient = &pb.DeviceRef{UserId: third.Device.UserUUID, DeviceId: 1, GenerationId: third.Device.GenerationID}
	fromBob, fromThird := reverseMessage(toBob), reverseMessage(toThird)
	savedMessage(t, r, 1, toBob)
	savedMessage(t, r, 1, toThird)
	savedMessage(t, r, 2, fromBob)
	savedMessage(t, r, 3, fromThird)
	ack := ackFor(fromThird)
	ack.Messages = append(ack.Messages, ackFor(fromBob).Messages[0])
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			var err error
			switch i % 4 {
			case 0:
				batch := proto.Clone(ack).(*pb.AcknowledgeMessagesRequest)
				if i%8 == 0 {
					batch.Messages[0], batch.Messages[1] = batch.Messages[1], batch.Messages[0]
				}
				err = r.Acknowledge(t.Context(), 1, batch)
			case 1:
				_, err = r.Statuses(t.Context(), 1, statusesFor(toBob, toThird))
			case 2:
				_, err = r.Statuses(t.Context(), 1, statusesFor(toThird, toBob))
			case 3:
				_, err = r.Send(t.Context(), 1, nextMessage(toThird))
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	registryCount(t, f.db, "e2ee_message_acknowledgements", 2)
	messageCounters(t, f.db, f.low.GenerationID, 7, 0, 2, 0)
}
