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

func messageDB(t *testing.T) (*conversationFixture, *PgE2EEDeliveryRepository, *pb.SendMessageRequest) {
	t.Helper()
	f := conversationDB(t)
	c := f.create(t)
	req := &pb.SendMessageRequest{Context: f.req.Context, Envelope: &pb.Envelope{
		ProtocolVersion: 1, CryptoSuite: pb.CryptoSuite_CRYPTO_SUITE_SIGNAL_PQ_1, MessageId: uuid.NewString(), ConversationId: c.ID,
		Sender: &pb.DeviceRef{UserId: f.low.UserUUID, DeviceId: 1, GenerationId: f.low.GenerationID}, Recipient: &pb.DeviceRef{UserId: f.high.UserUUID, DeviceId: 1, GenerationId: f.high.GenerationID},
		MessageType: pb.CiphertextType_CIPHERTEXT_TYPE_PREKEY_SIGNAL, Ciphertext: []byte("synthetic ciphertext; no private or plaintext chat data"),
	}}
	return f, NewPgE2EEDeliveryRepository(f.db), req
}
func nextMessage(req *pb.SendMessageRequest) *pb.SendMessageRequest {
	r := proto.Clone(req).(*pb.SendMessageRequest)
	r.Envelope.MessageId = uuid.NewString()
	return r
}
func reverseMessage(req *pb.SendMessageRequest) *pb.SendMessageRequest {
	r := nextMessage(req)
	r.Envelope.Sender, r.Envelope.Recipient = r.Envelope.Recipient, r.Envelope.Sender
	r.Context.DeviceGenerationId = r.Envelope.Sender.GenerationId
	return r
}
func messageCounters(t *testing.T, db *sql.DB, gen string, sent, pending, sequence, wireBytes int64) {
	t.Helper()
	var gotSent, gotPending, gotSequence, gotBytes int64
	err := db.QueryRowContext(t.Context(), `SELECT sent_receipts,pending_messages,last_sequence,pending_bytes FROM e2ee_delivery_state WHERE generation_id=$1`, gen).Scan(&gotSent, &gotPending, &gotSequence, &gotBytes)
	if err != nil || gotSent != sent || gotPending != pending || gotSequence != sequence || gotBytes != wireBytes {
		t.Fatalf("delivery state %d/%d/%d/%d, want %d/%d/%d/%d: %v", gotSent, gotPending, gotSequence, gotBytes, sent, pending, sequence, wireBytes, err)
	}
}

func TestE2EESendConcurrentReplayPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	var wg sync.WaitGroup
	results := make(chan *domain.E2EESendReceipt, 12)
	for range 12 {
		wg.Go(func() {
			got, err := r.Send(t.Context(), 1, req)
			if err != nil {
				t.Error(err)
				return
			}
			results <- got
		})
	}
	wg.Wait()
	close(results)
	var first *domain.E2EESendReceipt
	for got := range results {
		if first == nil {
			first = got
		} else if *got != *first {
			t.Fatal("receipt changed on exact retry")
		}
	}
	if first == nil || first.ExpiresAt.Sub(first.StoredAt) != domain.E2EEMessageRetention || time.Since(first.StoredAt) > time.Minute {
		t.Fatal("missing receipt or invalid retention")
	}
	digest, _ := boundary.EnvelopeDigest(req.Envelope)
	if first.Digest != digest {
		t.Fatal("wrong envelope digest")
	}
	registryCount(t, f.db, "e2ee_send_receipts", 1)
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	var wire []byte
	var sequence int64
	if err := f.db.QueryRowContext(t.Context(), `SELECT envelope,sequence FROM e2ee_message_inbox WHERE message_id=$1`, req.Envelope.MessageId).Scan(&wire, &sequence); err != nil {
		t.Fatal(err)
	}
	decoded := new(pb.Envelope)
	if proto.Unmarshal(wire, decoded) != nil || !proto.Equal(decoded, req.Envelope) || sequence != 1 {
		t.Fatal("inbox envelope/sequence differs")
	}
	messageCounters(t, f.db, f.low.GenerationID, 1, 0, 0, 0)
	messageCounters(t, f.db, f.high.GenerationID, 0, 1, 1, int64(len(wire)))
	for _, table := range []string{"messages", "chats", "attachments"} {
		registryCount(t, f.db, table, 0)
	}
}

func TestE2EESendConcurrentDistinctAndBothDirectionsPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			uid := int64(1)
			request := nextMessage(req)
			if i%2 != 0 {
				uid = 2
				request = reverseMessage(req)
			}
			if _, err := r.Send(t.Context(), uid, request); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	registryCount(t, f.db, "e2ee_send_receipts", 20)
	registryCount(t, f.db, "e2ee_message_inbox", 20)
	for _, gen := range []string{f.low.GenerationID, f.high.GenerationID} {
		var pending, sent, sequence int64
		if err := f.db.QueryRowContext(t.Context(), `SELECT pending_messages,sent_receipts,last_sequence FROM e2ee_delivery_state WHERE generation_id=$1`, gen).Scan(&pending, &sent, &sequence); err != nil || pending != 10 || sent != 10 || sequence != 10 {
			t.Fatal("lost counters or sequence", err)
		}
	}
}

func TestE2EESendConflictsAndAuthorizationPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	original, err := r.Send(t.Context(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		bad := proto.Clone(req).(*pb.SendMessageRequest)
		switch i {
		case 0:
			bad.Envelope.Ciphertext = []byte("changed")
		case 1:
			bad.Envelope.MessageType = pb.CiphertextType_CIPHERTEXT_TYPE_SIGNAL
		case 2:
			bad.Envelope.Recipient.GenerationId = uuid.NewString()
		case 3:
			bad.Envelope.Recipient.UserId = uuid.NewString()
		case 4:
			bad.Envelope.Recipient.DeviceId = 2
		}
		if _, err := r.Send(t.Context(), 1, bad); !errors.Is(err, domain.ErrE2EEMessageConflict) {
			t.Fatal("mutated retry accepted", i, err)
		}
	}
	bad := reverseMessage(req)
	bad.Envelope.MessageId = req.Envelope.MessageId
	if _, err := r.Send(t.Context(), 2, bad); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("global UUID reused by other sender", err)
	}
	for _, uid := range []int64{0, 2, 3} {
		if _, err := r.Send(t.Context(), uid, req); err == nil {
			t.Fatal("foreign owner accepted", uid)
		}
	}
	for i := range 4 {
		bad := nextMessage(req)
		switch i {
		case 0:
			bad.Envelope.Sender.UserId = f.high.UserUUID
			bad.Envelope.Recipient.UserId = uuid.NewString()
		case 1:
			bad.Envelope.Sender.DeviceId = 2
		case 2:
			bad.Envelope.Recipient.GenerationId = uuid.NewString()
		case 3:
			bad.Envelope.ConversationId = uuid.NewString()
		}
		if _, err := r.Send(t.Context(), 1, bad); err == nil {
			t.Fatal("forged binding accepted", i)
		}
	}
	registryCount(t, f.db, "e2ee_message_inbox", 1)
	registryCount(t, f.db, "e2ee_send_receipts", 1)
	again, err := r.Send(t.Context(), 1, req)
	if err != nil || *again != *original {
		t.Fatal("conflict changed original", err)
	}
}

func TestE2EESendRetryAfterClosureAndCiphertextRemovalPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	receipt, err := r.Send(t.Context(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a FUTURE successful ACK/expiry cleanup transaction, not an ACK API.
	// Preserve the immutable receipt and update the protected backlog counters.
	if _, err := f.db.ExecContext(t.Context(), `DELETE FROM e2ee_message_inbox; UPDATE e2ee_delivery_state SET pending_messages=0,pending_bytes=0`); err != nil {
		t.Fatal(err)
	}
	f.replace(t, 2, f.high.GenerationID)
	again, err := r.Send(t.Context(), 1, req)
	if err != nil || *again != *receipt {
		t.Fatal("past COMMIT lost after closure", err)
	}
	registryCount(t, f.db, "e2ee_message_inbox", 0)
	registryCount(t, f.db, "e2ee_send_receipts", 1)
	if _, err := r.Send(t.Context(), 1, nextMessage(req)); !errors.Is(err, domain.ErrE2EEConversationClosed) {
		t.Fatal("new ciphertext in closed chat", err)
	}
	f.replace(t, 1, f.low.GenerationID)
	if _, err := r.Send(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired sender retrieved receipt", err)
	}
}

func TestE2EESendExistingChatSurvivesPrivacyButRequiresPublishedCurrentDevicesPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=FALSE; UPDATE friend_requests SET status='rejected'`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Send(t.Context(), 1, req); err != nil {
		t.Fatal("new-chat privacy blocked existing send", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET public_material_ready=FALSE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Send(t.Context(), 1, nextMessage(req)); !errors.Is(err, domain.ErrE2EEKeysNotReady) {
		t.Fatal("unpublished peer accepted", err)
	}
	if _, err := r.Send(t.Context(), 1, req); err != nil {
		t.Fatal("exact receipt retry required fresh public keys", err)
	}
}

func TestE2EESendCommitAndInboxFailureAreAtomicPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	for _, phase := range []string{"commit", "inbox"} {
		t.Run(phase, func(t *testing.T) {
			var setup, cleanup string
			if phase == "commit" {
				setup = `CREATE FUNCTION reject_send() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic send commit failure'; END $$;
            CREATE CONSTRAINT TRIGGER reject_send AFTER INSERT ON e2ee_send_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_send()`
				cleanup = `DROP TRIGGER reject_send ON e2ee_send_receipts; DROP FUNCTION reject_send()`
			} else {
				setup = `CREATE FUNCTION reject_send() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic inbox failure'; END $$;
            CREATE TRIGGER reject_send BEFORE INSERT ON e2ee_message_inbox FOR EACH ROW EXECUTE FUNCTION reject_send()`
				cleanup = `DROP TRIGGER reject_send ON e2ee_message_inbox; DROP FUNCTION reject_send()`
			}
			if _, err := f.db.ExecContext(t.Context(), setup); err != nil {
				t.Fatal(err)
			}
			if receipt, err := r.Send(t.Context(), 1, req); err == nil || receipt != nil {
				t.Fatal("failed TX returned persistence ACK")
			}
			for _, table := range []string{"e2ee_send_receipts", "e2ee_message_inbox", "e2ee_delivery_state"} {
				registryCount(t, f.db, table, 0)
			}
			if _, err := f.db.ExecContext(t.Context(), cleanup); err != nil {
				t.Fatal(err)
			}
		})
	}
	if _, err := r.Send(t.Context(), 1, req); err != nil {
		t.Fatal("same UUID could not recover after rollback", err)
	}
}

func TestE2EESendCapacityChecksPreserveExactReplayPostgres(t *testing.T) {
	f, r, req := messageDB(t)
	saved, err := r.Send(t.Context(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		column string
		value  int64
		gen    string
	}{
		{"sent_receipts", domain.MaxE2EESendReceiptsPerGeneration, f.low.GenerationID},
		{"pending_messages", domain.MaxE2EEPendingMessagesPerGeneration, f.high.GenerationID},
		{"pending_bytes", domain.MaxE2EEPendingBytesPerGeneration, f.high.GenerationID},
		{"last_sequence", 1<<63 - 1, f.high.GenerationID},
	} {
		t.Run(tc.column, func(t *testing.T) {
			var old int64
			if err := f.db.QueryRowContext(t.Context(), `SELECT `+tc.column+` FROM e2ee_delivery_state WHERE generation_id=$1`, tc.gen).Scan(&old); err != nil {
				t.Fatal(err)
			}
			if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_delivery_state SET `+tc.column+`=$1 WHERE generation_id=$2`, tc.value, tc.gen); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Send(t.Context(), 1, nextMessage(req)); !errors.Is(err, domain.ErrE2EEMessageCapacity) {
				t.Fatal("capacity not enforced", err)
			}
			again, err := r.Send(t.Context(), 1, req)
			if err != nil || *again != *saved {
				t.Fatal("capacity blocked known COMMIT recovery", err)
			}
			if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_delivery_state SET `+tc.column+`=$1 WHERE generation_id=$2`, old, tc.gen); err != nil {
				t.Fatal(err)
			}
		})
	}
	registryCount(t, f.db, "e2ee_send_receipts", 1)
	registryCount(t, f.db, "e2ee_message_inbox", 1)
}

func TestE2EESendWaitCancellationAndReplacementRacePostgres(t *testing.T) {
	f, r, req := messageDB(t)
	lock, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(t.Context(), `SELECT id FROM users WHERE id=2 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
	defer cancel()
	if got, err := r.Send(ctx, 1, req); err == nil || got != nil {
		t.Fatal("lock wait ignored deadline")
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	registryCount(t, f.db, "e2ee_send_receipts", 0)
	// Multiple opposite-direction writes/replacement must not deadlock. A send
	// either commits before retirement or is rejected without partial persistence.
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			_, err := r.Send(t.Context(), 1, nextMessage(req))
			if err != nil && !errors.Is(err, domain.ErrE2EEConversationClosed) {
				t.Error(err)
			}
		})
	}
	wg.Go(func() {
		replacement := registrationMaterial()
		replacement.ExpectedGenerationID = f.high.GenerationID
		if _, err := f.devices.Replace(t.Context(), 2, replacement, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}); err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
	var receipts, inbox int
	if err := f.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_send_receipts),(SELECT count(*) FROM e2ee_message_inbox)`).Scan(&receipts, &inbox); err != nil || receipts != inbox {
		t.Fatal("race produced partial send", err)
	}
	if _, err := r.Send(t.Context(), 1, nextMessage(req)); !errors.Is(err, domain.ErrE2EEConversationClosed) {
		t.Fatal("send accepted after replacement", err)
	}
}

func TestE2EEMessageMigrationGuardsPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 23); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty delivery migration did not roll back", err)
	}
	if err := goose.UpTo(db, "../../migrations", 23); err != nil {
		t.Fatal(err)
	}
	f := seedConversations(t, db)
	c := f.create(t)
	req := &pb.SendMessageRequest{Context: f.req.Context, Envelope: &pb.Envelope{ProtocolVersion: 1, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: c.ID, Sender: &pb.DeviceRef{UserId: f.low.UserUUID, DeviceId: 1, GenerationId: f.low.GenerationID}, Recipient: &pb.DeviceRef{UserId: f.high.UserUUID, DeviceId: 1, GenerationId: f.high.GenerationID}, MessageType: 1, Ciphertext: []byte{1}}}
	seedHistoricalE2EESend(t, db, req.Envelope)
	for _, statement := range []string{`UPDATE e2ee_send_receipts SET expires_at=expires_at+INTERVAL '1 hour'`, `UPDATE e2ee_send_receipts SET envelope_digest=decode(repeat('00',32),'hex')`, `UPDATE e2ee_message_inbox SET recipient_generation_id='11111111-1111-4111-8111-111111111111'`, `UPDATE e2ee_delivery_state SET pending_messages=10001`} {
		if _, err := db.ExecContext(t.Context(), statement); err == nil {
			t.Fatal("SQL boundary accepted invalid mutation", statement)
		}
	}
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("nonempty delivery state rolled back")
	}
	var wire []byte
	if err := db.QueryRowContext(t.Context(), `SELECT envelope FROM e2ee_message_inbox WHERE message_id=$1`, req.Envelope.MessageId).Scan(&wire); err != nil || !bytes.Contains(wire, req.Envelope.Ciphertext) {
		t.Fatal("rollback removed ciphertext", err)
	}
}
