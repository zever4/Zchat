package repository

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

func cleanupPass(t *testing.T, r *PgE2EEDeliveryRepository, lane domain.E2EECleanupLane, after *domain.E2EECleanupPosition) domain.E2EECleanupResult {
	t.Helper()
	result, err := r.Cleanup(t.Context(), lane, after)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func cleanupCountersMatch(t *testing.T, db *sql.DB) {
	t.Helper()
	var mismatch int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_delivery_state s WHERE
 s.pending_messages<>(SELECT count(*) FROM e2ee_message_inbox i WHERE i.recipient_generation_id=s.generation_id) OR
 s.pending_bytes<>COALESCE((SELECT sum(octet_length(envelope)) FROM e2ee_message_inbox i WHERE i.recipient_generation_id=s.generation_id),0) OR
 s.sent_receipts<>(SELECT count(*) FROM e2ee_send_receipts r WHERE r.sender_generation_id=s.generation_id)`).Scan(&mismatch); err != nil || mismatch != 0 {
		t.Fatal("cleanup counter/receipt drift", mismatch, err)
	}
}

func TestE2EECleanupExpiryLateAckAndExactRetryPostgres(t *testing.T) {
	for _, disposition := range []pb.AckDisposition{pb.AckDisposition_ACK_DISPOSITION_STORED, pb.AckDisposition_ACK_DISPOSITION_REJECTED} {
		t.Run(disposition.String(), func(t *testing.T) {
			f, r, req := messageDB(t)
			savedMessage(t, r, 1, req)
			expireMessage(t, f.db, req.Envelope.MessageId)
			// Capture the aged fixture's receipt without a sender STATUS query/cleanup.
			original := savedMessage(t, r, 1, req)
			got := cleanupPass(t, r, domain.E2EECleanupExpired, nil)
			if got.Removed != 1 || got.Visited != 1 || got.After != nil {
				t.Fatal("expiry pass", got)
			}
			registryCount(t, f.db, "e2ee_send_receipts", 1)
			registryCount(t, f.db, "e2ee_message_inbox", 0)
			registryCount(t, f.db, "e2ee_message_acknowledgements", 0)
			var reason string
			var removed time.Time
			if err := f.db.QueryRowContext(t.Context(), `SELECT reason,removed_at FROM e2ee_message_removals`).Scan(&reason, &removed); err != nil || reason != "expired" {
				t.Fatal(reason, err)
			}
			ack := ackFor(req)
			ack.Messages[0].Disposition = disposition
			if err := r.Acknowledge(t.Context(), 2, ack); err != nil {
				t.Fatal("late ACK", err)
			}
			want := pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED
			if disposition == pb.AckDisposition_ACK_DISPOSITION_REJECTED {
				want = pb.MessageState_MESSAGE_STATE_RECIPIENT_REJECTED
			}
			messageState(t, r, req, want)
			var same bool
			if err := f.db.QueryRowContext(t.Context(), `SELECT reason='expired' AND removed_at=$1 FROM e2ee_message_removals`, removed).Scan(&same); err != nil || !same {
				t.Fatal("late ACK rewrote removal", err)
			}
			if again := savedMessage(t, r, 1, req); *again != *original {
				t.Fatal("cleanup changed immutable receipt")
			}
			if err := r.Acknowledge(t.Context(), 2, ack); err != nil {
				t.Fatal(err)
			}
			if got := cleanupPass(t, r, domain.E2EECleanupExpired, nil); got.Removed != 0 {
				t.Fatal("cleanup repeated removal")
			}
			registryCount(t, f.db, "e2ee_message_removals", 1)
			cleanupCountersMatch(t, f.db)
		})
	}
}

func TestE2EECleanupRetiredRecipientAndAcceptedRetiredSenderPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	live, expired, toOld := nextMessage(req), nextMessage(req), reverseMessage(req)
	savedMessage(t, r, 1, live)
	savedMessage(t, r, 1, expired)
	savedMessage(t, r, 2, toOld)
	expireMessage(t, f.db, expired.Envelope.MessageId)
	f.replace(t, 1, f.low.GenerationID)
	if got := cleanupPass(t, r, domain.E2EECleanupReplaced, nil); got.Removed != 1 {
		t.Fatal("recipient cleanup", got)
	}
	if got := cleanupPass(t, r, domain.E2EECleanupExpired, nil); got.Removed != 1 {
		t.Fatal("expired retired sender cleanup", got)
	}
	page, err := r.Pending(t.Context(), 2, pendingFor(req))
	if err != nil || len(page.GetMessages()) != 1 || !proto.Equal(page.Messages[0].Envelope, live.Envelope) {
		t.Fatal("accepted retired sender body lost", err)
	}
	statuses, err := r.Statuses(t.Context(), 2, statusesFor(toOld))
	if err != nil || statuses.Messages[0].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED {
		t.Fatal("replacement status", err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_send_receipts", 3)
	cleanupCountersMatch(t, f.db)
}

func TestE2EECleanupReplacementPrecedesExpiryAndPreservesDedupPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	savedMessage(t, r, 1, req)
	expireMessage(t, f.db, req.Envelope.MessageId)
	original := savedMessage(t, r, 1, req)
	replacement := f.replace(t, 2, f.high.GenerationID)
	if got := cleanupPass(t, r, domain.E2EECleanupExpired, nil); got.Removed != 1 {
		t.Fatal(got)
	}
	messageState(t, r, req, pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED)
	var reason string
	if err := f.db.QueryRowContext(t.Context(), `SELECT reason FROM e2ee_message_removals`).Scan(&reason); err != nil || reason != "recipient_replaced" {
		t.Fatal("precedence", reason, err)
	}
	if got := cleanupPass(t, r, domain.E2EECleanupReplaced, nil); got.Removed != 0 {
		t.Fatal("double removal")
	}
	if retry := savedMessage(t, r, 1, req); *retry != *original {
		t.Fatal("past receipt changed")
	}
	if err := r.Acknowledge(t.Context(), 2, ackFor(req)); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("old generation ACK", err)
	}
	ack := ackFor(req)
	ack.Context.DeviceGenerationId = replacement.GenerationID
	if err := r.Acknowledge(t.Context(), 2, ack); !errors.Is(err, domain.ErrE2EEMessageNotFound) {
		t.Fatal("new generation ACKed old data", err)
	}
	cleanupCountersMatch(t, f.db)
}

func TestE2EECleanupBoundedPagesAndRestartPostgres(t *testing.T) {
	for _, lane := range []domain.E2EECleanupLane{domain.E2EECleanupExpired, domain.E2EECleanupReplaced} {
		t.Run(map[domain.E2EECleanupLane]string{0: "expired", 1: "replaced"}[lane], func(t *testing.T) {
			f, r, req := messageDB(t)
			for range domain.MaxE2EECleanupBatch + 5 {
				message := nextMessage(req)
				savedMessage(t, r, 1, message)
				if lane == domain.E2EECleanupExpired {
					expireMessage(t, f.db, message.Envelope.MessageId)
				}
			}
			if lane == domain.E2EECleanupReplaced {
				f.replace(t, 2, f.high.GenerationID)
			}
			first := cleanupPass(t, r, lane, nil)
			if first.Removed != domain.MaxE2EECleanupBatch || first.Visited != domain.MaxE2EECleanupBatch || first.After == nil {
				t.Fatal("unbounded/early-ended pass", first)
			}
			registryCount(t, f.db, "e2ee_message_inbox", 5)
			// A process restart loses the scheduling cursor; durable rows still converge.
			restarted := NewPgE2EEDeliveryRepository(f.db)
			second := cleanupPass(t, restarted, lane, nil)
			if second.Removed != 5 || second.After != nil {
				t.Fatal("restart lost/repeated work", second)
			}
			third := cleanupPass(t, restarted, lane, first.After)
			if third.Removed != 0 || third.After != nil {
				t.Fatal("old scheduling cursor corrupted state")
			}
			registryCount(t, f.db, "e2ee_send_receipts", domain.MaxE2EECleanupBatch+5)
			registryCount(t, f.db, "e2ee_message_removals", domain.MaxE2EECleanupBatch+5)
			cleanupCountersMatch(t, f.db)
		})
	}
}

func TestE2EECleanupBusyPrefixDoesNotStarveNextPagePostgres(t *testing.T) {
	f, r, req := messageDB(t)
	var ids []string
	for range domain.MaxE2EECleanupBatch {
		message := nextMessage(req)
		savedMessage(t, r, 1, message)
		expireMessage(t, f.db, message.Envelope.MessageId)
		ids = append(ids, message.Envelope.MessageId)
	}
	cold := nextMessage(req)
	savedMessage(t, r, 1, cold)
	expireMessage(t, f.db, cold.Envelope.MessageId)
	lock, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(t.Context(), `SELECT message_id FROM e2ee_send_receipts WHERE message_id=ANY($1::uuid[]) FOR UPDATE`, pq.Array(ids)); err != nil {
		t.Fatal(err)
	}
	first := cleanupPass(t, r, domain.E2EECleanupExpired, nil)
	if first.Busy != domain.MaxE2EECleanupBatch || first.Removed != 0 || first.After == nil {
		t.Fatal("busy prefix", first)
	}
	second := cleanupPass(t, r, domain.E2EECleanupExpired, first.After)
	if second.Removed != 1 || second.Busy != 0 || second.After != nil {
		t.Fatal("cold page starved", second)
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := cleanupPass(t, r, domain.E2EECleanupExpired, nil); got.Removed != domain.MaxE2EECleanupBatch {
		t.Fatal("busy copies lost on wrap", got)
	}
	cleanupCountersMatch(t, f.db)
}

func TestE2EECleanupRollbackAndCorruptCountersPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	savedMessage(t, r, 1, req)
	expireMessage(t, f.db, req.Envelope.MessageId)
	if _, err := f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic deferred cleanup failure'; END $$;
 CREATE CONSTRAINT TRIGGER reject_cleanup AFTER INSERT ON e2ee_message_removals DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_cleanup()`); err != nil {
		t.Fatal(err)
	}
	result, err := r.Cleanup(t.Context(), domain.E2EECleanupExpired, nil)
	if err == nil || result.Removed != 0 {
		t.Fatal("failed COMMIT reported as success", result)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_message_removals", 0)
	cleanupCountersMatch(t, f.db)
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_cleanup ON e2ee_message_removals; DROP FUNCTION reject_cleanup(); UPDATE e2ee_delivery_state SET pending_messages=0 WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	result, err = r.Cleanup(t.Context(), domain.E2EECleanupExpired, nil)
	if !errors.Is(err, domain.ErrE2EEMessageState) || result.Removed != 0 {
		t.Fatal("corrupt counter accepted", result, err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_message_removals", 0)
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_delivery_state SET pending_messages=1 WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if got := cleanupPass(t, NewPgE2EEDeliveryRepository(f.db), domain.E2EECleanupExpired, nil); got.Removed != 1 {
		t.Fatal("retry after rollback", got)
	}
	cleanupCountersMatch(t, f.db)
}

func TestE2EECleanupCancellationBudgetsAndRowLockPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	savedMessage(t, r, 1, req)
	expireMessage(t, f.db, req.Envelope.MessageId)
	lock, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(t.Context(), `SELECT message_id FROM e2ee_message_inbox FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	got := cleanupPass(t, r, domain.E2EECleanupExpired, nil)
	if got.Busy != 1 || got.Removed != 0 || time.Since(start) > time.Second {
		t.Fatal("row lock was not bounded", got)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_message_removals", 0)
	cleanupCountersMatch(t, f.db)
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	lock, err = f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(t.Context(), `LOCK TABLE e2ee_message_inbox IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	for _, budget := range []time.Duration{50 * time.Millisecond, 5 * time.Second} {
		ctx, cancel := context.WithTimeout(t.Context(), budget)
		start := time.Now()
		result, err := r.Cleanup(ctx, domain.E2EECleanupExpired, nil)
		cancel()
		if err == nil || result.Removed != 0 {
			t.Fatal("blocked discovery ignored deadline", result, err)
		}
		elapsed := time.Since(start)
		limit := time.Second
		if budget > time.Second {
			limit = 4 * time.Second
			if elapsed < 1500*time.Millisecond {
				t.Fatal("pass budget unexpectedly short", elapsed)
			}
		}
		if elapsed > limit {
			t.Fatal("cleanup exceeded deadline", elapsed)
		}
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if got := cleanupPass(t, r, domain.E2EECleanupExpired, nil); got.Removed != 1 {
		t.Fatal("pool unusable after cancel", got)
	}
	cleanupCountersMatch(t, f.db)
}

func TestE2EECleanupConcurrentAckStatusSendAndWorkersPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	var requests []*pb.SendMessageRequest
	for range 12 {
		message := nextMessage(req)
		savedMessage(t, r, 1, message)
		expireMessage(t, f.db, message.Envelope.MessageId)
		requests = append(requests, message)
	}
	ack := ackFor(requests[0])
	for _, message := range requests[1:] {
		ack.Messages = append(ack.Messages, ackFor(message).Messages[0])
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 16 {
		wg.Go(func() {
			<-start
			var err error
			switch i % 4 {
			case 0:
				_, err = r.Cleanup(t.Context(), domain.E2EECleanupExpired, nil)
			case 1:
				err = r.Acknowledge(t.Context(), 2, ack)
			case 2:
				batch := statusesFor(requests...)
				for a, b := 0, len(batch.Messages)-1; a < b; a, b = a+1, b-1 {
					batch.Messages[a], batch.Messages[b] = batch.Messages[b], batch.Messages[a]
				}
				_, err = r.Statuses(t.Context(), 1, batch)
			case 3:
				_, err = r.Send(t.Context(), 1, nextMessage(req))
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()
	registryCount(t, f.db, "e2ee_message_acknowledgements", 12)
	registryCount(t, f.db, "e2ee_message_removals", 12)
	registryCount(t, f.db, "e2ee_message_inbox", 4)
	if got := cleanupPass(t, r, domain.E2EECleanupExpired, nil); got.Removed != 0 {
		t.Fatal("live copies removed", got)
	}
	for _, message := range requests {
		messageState(t, r, message, pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED)
	}
	cleanupCountersMatch(t, f.db)
}

func TestE2EECleanupConcurrentRecipientReplacementPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	original := savedMessage(t, r, 1, req)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() { <-start; f.replace(t, 2, f.high.GenerationID) })
	wg.Go(func() {
		<-start
		if _, err := r.Cleanup(t.Context(), domain.E2EECleanupReplaced, nil); err != nil {
			t.Error(err)
		}
	})
	wg.Go(func() {
		<-start
		if err := r.Acknowledge(t.Context(), 2, ackFor(req)); err != nil && !errors.Is(err, domain.ErrE2EEGenerationChanged) {
			t.Error(err)
		}
	})
	wg.Go(func() {
		<-start
		if _, err := r.Statuses(t.Context(), 1, statusesFor(req)); err != nil {
			t.Error(err)
		}
	})
	close(start)
	wg.Wait()
	cleanupPass(t, r, domain.E2EECleanupReplaced, nil)
	statuses, err := r.Statuses(t.Context(), 1, statusesFor(req))
	if err != nil {
		t.Fatal(err)
	}
	state := statuses.Messages[0].State
	if state != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED && state != pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED {
		t.Fatal("impossible replacement outcome", state)
	}
	if replay := savedMessage(t, r, 1, req); *replay != *original {
		t.Fatal("replacement cleanup lost receipt")
	}
	registryCount(t, f.db, "e2ee_message_inbox", 0)
	registryCount(t, f.db, "e2ee_message_removals", 1)
	cleanupCountersMatch(t, f.db)
}

func TestE2EECleanupIndexesMigrationPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 24); err != nil {
		t.Fatal(err)
	}
	f := seedConversations(t, db)
	c := f.create(t)
	r := NewPgE2EEDeliveryRepository(db)
	req := &pb.SendMessageRequest{Context: f.req.Context, Envelope: &pb.Envelope{ProtocolVersion: 1, CryptoSuite: 1, MessageId: registrationMaterial().RequestID, ConversationId: c.ID, Sender: &pb.DeviceRef{UserId: f.low.UserUUID, DeviceId: 1, GenerationId: f.low.GenerationID}, Recipient: &pb.DeviceRef{UserId: f.high.UserUUID, DeviceId: 1, GenerationId: f.high.GenerationID}, MessageType: 1, Ciphertext: []byte{1}}}
	seedHistoricalE2EESend(t, db, req.Envelope)
	if err := goose.UpTo(db, "../../migrations", 25); err != nil {
		t.Fatal(err)
	}
	var indexes int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname IN ('e2ee_retired_delivery_devices','e2ee_pending_delivery_generations')`).Scan(&indexes); err != nil || indexes != 2 {
		t.Fatal("cleanup indexes missing", indexes, err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("index-only rollback", err)
	}
	registryCount(t, db, "e2ee_message_inbox", 1)
	registryCount(t, db, "e2ee_send_receipts", 1)
	if err := goose.UpTo(db, "../../migrations", 25); err != nil {
		t.Fatal(err)
	}
	expireMessage(t, db, req.Envelope.MessageId)
	if got := cleanupPass(t, r, domain.E2EECleanupExpired, nil); got.Removed != 1 {
		t.Fatal(got)
	}
	cleanupCountersMatch(t, db)
}
