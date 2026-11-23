//go:build linux

package repository

import (
	"bytes"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

// These are repository/FS component tests with opaque synthetic Signal bytes.
// They do not exercise the native Signal promotion or the disabled Send3 RPC.
func attachmentSendRequest(r *domain.E2EEBlobReservationRequest) *v2.SendMessageRequest {
	epoch := *r.CryptoEpoch
	return &v2.SendMessageRequest{Context: proto.Clone(r.Context).(*pb.ClientContext), Envelope: &v2.Envelope{
		ProtocolVersion: 3, CryptoSuite: 1, MessageId: r.MessageID, ConversationId: r.ConversationID,
		Sender: proto.Clone(r.Sender).(*pb.DeviceRef), Recipient: proto.Clone(r.Recipient).(*pb.DeviceRef),
		CryptoEpoch: &epoch, MessageType: 1, Ciphertext: []byte("opaque synthetic Signal attachment descriptor"),
		Blob: proto.Clone(r.Blob).(*v2.BlobRef)}}
}

func readyAttachment(t *testing.T, f *blobReservationFixture, r *domain.E2EEBlobWriteRequest, files *blobstore.Store, data []byte) {
	t.Helper()
	if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	if _, err := f.blobs.CompleteWrite(t.Context(), 1, r, writeBlobProof(t, files, r, data)); err != nil {
		t.Fatal(err)
	}
}

func acceptAttachment(t *testing.T, f *blobReservationFixture, r *domain.E2EEBlobReservationRequest) *v2.SendReceipt {
	t.Helper()
	s, err := f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(r))
	if err != nil || s == nil {
		t.Fatal("component attachment Send", s, err)
	}
	return s
}

func assertAttachmentNotAccepted(t *testing.T, f *blobReservationFixture) {
	t.Helper()
	for _, table := range []string{"e2ee_send_receipts", "e2ee_message_inbox", "e2ee_message_blobs", "e2ee_delivery_state"} {
		registryCount(t, f.f.db, table, 0)
	}
}

func TestAttachmentSendAtomicLinkReadersAndMessageAckPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	req := attachmentSendRequest(r.Attempt.Reservation)
	receipt := acceptAttachment(t, f, r.Attempt.Reservation)
	if receipt.ExpiresAtUnixMs-receipt.StoredAtUnixMs != domain.E2EEMessageRetention.Milliseconds() {
		t.Fatal("accepted retention must start at receipt stored-at", receipt)
	}
	var linked bool
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT a.state='linked' AND c.envelope_digest IS NULL
        AND b.revision=$2 AND b.attempt_id=$3 FROM e2ee_message_blobs b
        JOIN e2ee_message_claims c USING(message_id) JOIN e2ee_blob_attempts a ON a.attempt_id=b.attempt_id
        WHERE b.message_id=$1`, req.Envelope.MessageId, r.Attempt.Revision, r.Attempt.AttemptID).Scan(&linked); err != nil || !linked {
		t.Fatal("receipt did not atomically link exact immutable reservation", linked, err)
	}
	f.quota(t, 64, 1, 1)
	// The text and attachment readers share ordering, ACK and bounded counters.
	text := f.message(0)
	if _, err := f.delivery.SendV2(t.Context(), 1, text); err != nil {
		t.Fatal(err)
	}
	page, err := f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 2 || !proto.Equal(page[0].Envelope, req.Envelope) || !bytes.Equal(page[0].Digest[:], receipt.EnvelopeDigest) || !proto.Equal(page[1].Envelope, text.Envelope) || page[0].Sequence >= page[1].Sequence {
		t.Fatal("mixed v2/v3 inbox", page, err)
	}
	if prototype, err := f.delivery.Pending(t.Context(), 2, &pb.GetPendingMessagesRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.gen(2)}}); err != nil || len(prototype.Messages) != 0 {
		t.Fatal("prototype reader parsed epoch ciphertext", prototype, err)
	}
	statusReq := &pb.GetMessageStatusesRequest{Context: f.context(1), Messages: []*pb.MessageReference{{MessageId: receipt.MessageId, EnvelopeDigest: receipt.EnvelopeDigest}}}
	statuses, err := f.delivery.StatusesV2(t.Context(), 1, statusReq)
	if err != nil || len(statuses.Messages) != 1 || statuses.Messages[0].State != pb.MessageState_MESSAGE_STATE_PENDING || !proto.Equal(statuses.Messages[0].Receipt, receipt) {
		t.Fatal("attachment pending status", statuses, err)
	}
	for range 2 {
		if err := f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(req)); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		got, err := f.delivery.SendV2(t.Context(), 1, req)
		if err != nil || !proto.Equal(got, receipt) {
			t.Fatal("lost reply retry after descriptor ACK", got, err)
		}
	}
	statuses, err = f.delivery.StatusesV2(t.Context(), 1, statusReq)
	if err != nil || statuses.Messages[0].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED {
		t.Fatal(statuses, err)
	}
	page, err = f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 1 || !proto.Equal(page[0].Envelope, text.Envelope) {
		t.Fatal("attachment retry requeued descriptor", page, err)
	}
	// A message ACK acknowledges the encrypted descriptor, NEVER the file.
	if err = files.Verify(t.Context(), blobstore.Attempt{ID: r.Attempt.AttemptID, Bytes: int64(len(data)), SHA256: [32]byte(r.Attempt.Reservation.Blob.Sha256)}); err != nil {
		t.Fatal("message ACK removed file", err)
	}
	registryCount(t, f.f.db, "e2ee_message_blobs", 1)
	f.quota(t, 64, 1, 1)
	var sent, sequence, pending, size int64
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT sent_receipts FROM e2ee_delivery_state WHERE generation_id=$1`, f.gen(1)).Scan(&sent); err != nil || sent != 2 {
		t.Fatal(sent, err)
	}
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT last_sequence,pending_messages,pending_bytes FROM e2ee_delivery_state WHERE generation_id=$1`, f.gen(2)).Scan(&sequence, &pending, &size); err != nil || sequence != 2 || pending != 1 || size != int64(proto.Size(text.Envelope)) {
		t.Fatal("duplicate/ACK corrupted delivery counters", sequence, pending, size, err)
	}
}

func TestAttachmentSendReceiptFirstAfterSealReplacementAndLostFilePostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	req := attachmentSendRequest(r.Attempt.Reservation)
	receipt := acceptAttachment(t, f, r.Attempt.Reservation)
	f.transition(t)
	page, err := f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 1 || !proto.Equal(page[0].Envelope, req.Envelope) {
		t.Fatal("sealed accepted attachment hidden", page, err)
	}
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(req)); err != nil {
		t.Fatal(err)
	}
	f.age(t, &domain.E2EEBlobReservation{}) // Test-only staging deadline expiry.
	proof, err := files.RemoveVerified(t.Context(), r.Attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	defer proof.Close() // Simulate external loss, not a supported accepted-file GC.
	f.f.replace(t, 2, f.gen(2))
	got, err := f.delivery.SendV2(t.Context(), 1, req)
	if err != nil || !proto.Equal(got, receipt) {
		t.Fatal("past COMMIT was hidden by seal/expiry/ACK/replacement/missing file", got, err)
	}
	for _, mutate := range []func(*v2.Envelope){
		func(e *v2.Envelope) { e.Ciphertext = append(e.Ciphertext, 1) },
		func(e *v2.Envelope) { e.Blob.Sha256[0]++ },
		func(e *v2.Envelope) { e.Blob.BlobId = uuid.NewString() },
	} {
		changed := proto.Clone(req).(*v2.SendMessageRequest)
		mutate(changed.Envelope)
		if _, err = f.delivery.SendV2(t.Context(), 1, changed); !errors.Is(err, domain.ErrE2EEMessageConflict) {
			t.Fatal("accepted UUID changed under sealed fence", err)
		}
	}
	registryCount(t, f.f.db, "e2ee_send_receipts", 1)
	registryCount(t, f.f.db, "e2ee_message_blobs", 1)
	registryCount(t, f.f.db, "e2ee_message_inbox", 0)
}

func TestAttachmentSendRequiresLiveReadyExactReferencePostgres(t *testing.T) {
	for _, kind := range []string{"missing", "reserved", "writing", "deleting", "expired", "different hash", "different size", "different blob", "text collision"} {
		t.Run(kind, func(t *testing.T) {
			f, r, files, data := blobWriterFixture(t)
			req := attachmentSendRequest(r.Attempt.Reservation)
			want := domain.ErrE2EEBlobNotReadyForSend
			switch kind {
			case "missing":
				req.Envelope.MessageId = uuid.NewString()
			case "writing", "deleting":
				if _, err := f.blobs.BeginWrite(t.Context(), 1, r); err != nil {
					t.Fatal(err)
				}
				if kind == "deleting" {
					if _, err := f.blobs.ClaimCleanup(t.Context(), cleanupRequest(r, domain.E2EEBlobCleanupFailedWriter)); err != nil {
						t.Fatal(err)
					}
				}
			case "expired":
				readyAttachment(t, f, r, files, data)
				f.age(t, &domain.E2EEBlobReservation{})
				want = domain.ErrE2EEBlobLeaseExpired
			case "different hash", "different size", "different blob", "text collision":
				readyAttachment(t, f, r, files, data)
				want = domain.ErrE2EEMessageConflict
				switch kind {
				case "different hash":
					req.Envelope.Blob.Sha256[0]++
				case "different size":
					req.Envelope.Blob.CiphertextBytes++
				case "different blob":
					req.Envelope.Blob.BlobId = uuid.NewString()
				case "text collision":
					req.Envelope.Blob = nil
					req.Envelope.ProtocolVersion = 2
				}
			}
			got, err := f.delivery.SendV2(t.Context(), 1, req)
			if got != nil || !errors.Is(err, want) {
				t.Fatal("non-ready/mismatched file accepted", got, err, want)
			}
			var unavailable *domain.E2EEBlobSendFailure
			typed := errors.As(err, &unavailable)
			if want == domain.ErrE2EEBlobNotReadyForSend || want == domain.ErrE2EEBlobLeaseExpired {
				if !typed || unavailable == nil {
					t.Fatal("Ready gate lost exact failure binding", err)
				}
				reason := v2.BlobSendFailureReason_BLOB_SEND_FAILURE_REASON_NOT_READY
				if want == domain.ErrE2EEBlobLeaseExpired {
					reason = v2.BlobSendFailureReason_BLOB_SEND_FAILURE_REASON_LEASE_EXPIRED
				}
				if boundary.ValidateBlobSendFailure(req, &v2.BlobSendFailure{Reason: reason, Binding: unavailable.Binding}) != nil {
					t.Fatal("wrong failure binding")
				}
			} else if typed {
				t.Fatal("conflict invented retry observation", err)
			}
			assertAttachmentNotAccepted(t, f)
			registryCount(t, f.f.db, "e2ee_message_claims", 1)
			f.quota(t, 64, 1, 1)
		})
	}
}

func TestAttachmentSendSealedFencePrecedesAllBlobChecksPostgres(t *testing.T) {
	for _, kind := range []string{"missing", "reserved", "expired", "reclaimed"} {
		t.Run(kind, func(t *testing.T) {
			f, r, files, data := blobWriterFixture(t)
			req := attachmentSendRequest(r.Attempt.Reservation)
			if kind == "missing" {
				req.Envelope.MessageId = uuid.NewString()
			}
			if kind == "expired" || kind == "reclaimed" {
				readyAttachment(t, f, r, files, data)
				f.age(t, &domain.E2EEBlobReservation{})
			}
			if kind == "reclaimed" {
				c := cleanupRequest(r, domain.E2EEBlobCleanupExpired)
				if _, err := f.blobs.ClaimCleanup(t.Context(), c); err != nil {
					t.Fatal(err)
				}
				proof, err := files.RemoveVerified(t.Context(), c.AttemptID)
				if err != nil {
					t.Fatal(err)
				}
				defer proof.Close()
				if _, err = f.blobs.CompleteReclaim(t.Context(), c, proof); err != nil {
					t.Fatal(err)
				}
			}
			f.transition(t)
			got, err := f.delivery.SendV2(t.Context(), 1, req)
			if got != nil {
				t.Fatal("unsent sealed envelope received receipt", got)
			}
			assertEpochFence(t, err, req)
			assertAttachmentNotAccepted(t, f)
		})
	}
}

func TestAttachmentSendCommitFaultRollsBackLinkAndCountersPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION send3_fault()RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic Send3 commit fault';END $$;
        CREATE CONSTRAINT TRIGGER send3_fault AFTER INSERT ON e2ee_message_inbox DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION send3_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(r.Attempt.Reservation)); got != nil || err == nil {
		t.Fatal("failed COMMIT produced receipt", got, err)
	} else {
		var unavailable *domain.E2EEBlobSendFailure
		if errors.As(err, &unavailable) {
			t.Fatal("uncertain COMMIT minted retry observation", err)
		}
	}
	assertAttachmentNotAccepted(t, f)
	var state string
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts`).Scan(&state); err != nil || state != "ready" {
		t.Fatal("rolled-back Send consumed Ready", state, err)
	}
	f.quota(t, 64, 1, 1)
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER send3_fault ON e2ee_message_inbox; DROP FUNCTION send3_fault()`); err != nil {
		t.Fatal(err)
	}
	acceptAttachment(t, f, r.Attempt.Reservation)
}

func TestAttachmentSendConcurrentDuplicateAndCleanupPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	req := attachmentSendRequest(r.Attempt.Reservation)
	const n = 8
	start := make(chan struct{})
	out := make(chan *v2.SendReceipt, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			<-start
			s, err := f.delivery.SendV2(t.Context(), 1, req)
			if err != nil {
				t.Error(err)
			}
			out <- s
		})
	}
	wg.Go(func() {
		<-start
		if c, err := f.blobs.ClaimCleanup(t.Context(), cleanupRequest(r, domain.E2EEBlobCleanupExpired)); c != nil || !errors.Is(err, domain.ErrE2EEBlobCleanup) {
			t.Error("live/accepted file claimed for deletion", c, err)
		}
	})
	close(start)
	wg.Wait()
	close(out)
	var first *v2.SendReceipt
	for s := range out {
		if s == nil {
			t.Fatal("missing concurrent receipt")
		}
		if first == nil {
			first = s
		} else if !proto.Equal(first, s) {
			t.Fatal("concurrent retries changed receipt")
		}
	}
	for _, table := range []string{"e2ee_send_receipts", "e2ee_message_inbox", "e2ee_message_blobs"} {
		registryCount(t, f.f.db, table, 1)
	}
	f.quota(t, 64, 1, 1)
}

func TestAttachmentSendAcceptedClaimCannotRenewPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	acceptAttachment(t, f, r.Attempt.Reservation)
	for range 2 {
		g := f.reserve(t, 1, r.Attempt.Reservation)
		if g.State != "linked" || g.AttemptID != r.Attempt.AttemptID || g.Revision != r.Attempt.Revision {
			t.Fatal("exact reservation replay lost linked state", g)
		}
		observation, err := f.blobs.Observe(t.Context(), 1, r.Attempt.Reservation)
		if err != nil || observation == nil || observation.State != "linked" || !observation.ExpiresAt.Equal(g.ExpiresAt) {
			t.Fatal("exact immutable claim cannot be observed", observation, err)
		}
	}
	newOp := cloneBlobReservation(r.Attempt.Reservation)
	newOp.OperationID = uuid.NewString()
	newOp.ExpectedRevision, newOp.ExpectedAttemptID = r.Attempt.Revision, r.Attempt.AttemptID
	f.age(t, &domain.E2EEBlobReservation{})
	if got, err := f.blobs.Reserve(t.Context(), 1, newOp); got != nil || !errors.Is(err, domain.ErrE2EEMessageState) {
		t.Fatal("accepted receipt allocated another expired attempt", got, err)
	}
	registryCount(t, f.f.db, "e2ee_blob_attempts", 1)
	f.quota(t, 64, 1, 1)
}

func TestAttachmentSendSchemaGuardsAndMigrationPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	if _, err := f.f.db.ExecContext(t.Context(), `UPDATE e2ee_blob_attempts SET state='linked'`); err == nil {
		t.Fatal("raw Ready->Linked bypassed receipt link")
	}
	// All INSERT conditions except the deferred exact Linked requirement are met.
	digest, err := boundary.EpochEnvelopeDigest(attachmentSendRequest(r.Attempt.Reservation).Envelope)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(t.Context(), `INSERT INTO e2ee_send_receipts(message_id,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,wire_version,crypto_epoch,envelope_digest,stored_at,expires_at)
        SELECT message_id,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,3,crypto_epoch,$2,
        date_trunc('milliseconds',statement_timestamp()),date_trunc('milliseconds',statement_timestamp())+INTERVAL '720 hours'
        FROM e2ee_message_claims WHERE message_id=$1`, r.Attempt.Reservation.MessageID, digest[:])
	if err != nil {
		t.Fatal("receipt fixture failed before deferred COMMIT", err)
	}
	if err = tx.Commit(); err == nil {
		t.Fatal("raw attachment receipt committed without link")
	}
	assertAttachmentNotAccepted(t, f)
	acceptAttachment(t, f, r.Attempt.Reservation)
	if _, err = f.f.db.ExecContext(t.Context(), `INSERT INTO e2ee_blob_attempts(attempt_id,message_id,revision,operation_id,expected_revision,expected_attempt_id,reserved_at,expires_at,charged_bytes)
        SELECT gen_random_uuid(),message_id,revision+1,gen_random_uuid(),revision,attempt_id,
        statement_timestamp(),statement_timestamp()+INTERVAL '1 hour',charged_bytes FROM e2ee_blob_attempts WHERE state='linked'`); err == nil || !strings.Contains(err.Error(), "accepted message cannot allocate") {
		t.Fatal("raw allocation bypassed immutable receipt", err)
	}
	for _, q := range []string{
		`DELETE FROM e2ee_message_blobs`,
		`UPDATE e2ee_message_blobs SET revision=revision+1`,
		`UPDATE e2ee_message_claims SET envelope_digest=decode(repeat('42',32),'hex')`,
		`UPDATE e2ee_blob_heads SET revision=revision+1`,
	} {
		if _, err := f.f.db.ExecContext(t.Context(), q); err == nil {
			t.Fatal("changed immutable accepted binding", q)
		}
	}
	file, err := os.ReadFile("../../migrations/032_e2ee_attachment_receipts.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.f.db.ExecContext(t.Context(), strings.SplitN(string(file), "-- +goose Down", 2)[1]); err == nil {
		t.Fatal("Down discarded accepted attachment receipt")
	}
	// Also exercise the real goose parser and an empty Up/Down/Up cycle.
	if err = goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err = goose.UpTo(db, "../../migrations", 32); err != nil {
		t.Fatal(err)
	}
	if err = goose.Down(db, "../../migrations"); err != nil {
		t.Fatal(err)
	}
	if err = goose.UpTo(db, "../../migrations", 32); err != nil {
		t.Fatal(err)
	}
}

func TestAttachmentSendClockAfterAttemptLockWaitPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	// Test-only deadline shortening, preserving the original one-hour interval.
	if _, err := f.f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt;
        WITH stamp AS MATERIALIZED (SELECT date_trunc('milliseconds',clock_timestamp()) AS t)
        UPDATE e2ee_blob_attempts SET reserved_at=stamp.t-INTERVAL '1 hour'+INTERVAL '400 milliseconds',
        expires_at=stamp.t+INTERVAL '400 milliseconds' FROM stamp;
        ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	lock, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.ExecContext(t.Context(), `SELECT attempt_id FROM e2ee_blob_attempts FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(r.Attempt.Reservation))
		result <- err
	}()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var expired bool
		if err = lock.QueryRowContext(t.Context(), `SELECT clock_timestamp()>=expires_at FROM e2ee_blob_attempts`).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("test clock did not advance")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if err = <-result; !errors.Is(err, domain.ErrE2EEBlobLeaseExpired) {
		t.Fatal("accepted expired Ready using a clock sampled before lock wait", err)
	}
	assertAttachmentNotAccepted(t, f)
}

func TestAttachmentSendConcurrentRecoveryAcceptPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	f.propose(t, f.p, 1)
	req := attachmentSendRequest(r.Attempt.Reservation)
	decision := recoveryDecision(f.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var receipt *v2.SendReceipt
	var sendErr error
	wg.Go(func() { <-start; receipt, sendErr = f.delivery.SendV2(t.Context(), 1, req) })
	wg.Go(func() {
		<-start
		got, err := f.repo.Decide(t.Context(), 2, f.gen(2), decision)
		if err != nil || got.Outcome.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
			t.Error("manual recovery acceptance", got, err)
		}
	})
	close(start)
	wg.Wait()
	f.head(t, 1)
	if receipt == nil {
		assertEpochFence(t, sendErr, req)
		assertAttachmentNotAccepted(t, f)
	} else {
		got, err := f.delivery.SendV2(t.Context(), 1, req)
		if sendErr != nil || err != nil || !proto.Equal(got, receipt) {
			t.Fatal("accepted Send lost exact receipt after concurrent recovery", got, err, sendErr)
		}
		registryCount(t, f.f.db, "e2ee_message_blobs", 1)
		if _, err = f.blobs.ClaimCleanup(t.Context(), cleanupRequest(r, domain.E2EEBlobCleanupInactiveScope)); !errors.Is(err, domain.ErrE2EEBlobCleanup) {
			t.Fatal("recovery erased accepted attachment", err)
		}
	}
}

func TestAttachmentSendLinksRenewedHeadOnlyPostgres(t *testing.T) {
	f, old, files, data := blobWriterFixture(t)
	readyAttachment(t, f, old, files, data)
	f.age(t, &domain.E2EEBlobReservation{})
	q := cloneBlobReservation(old.Attempt.Reservation)
	q.OperationID = uuid.NewString()
	q.ExpectedRevision, q.ExpectedAttemptID = old.Attempt.Revision, old.Attempt.AttemptID
	g := f.reserve(t, 1, q)
	if got, err := f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(q)); got != nil || !errors.Is(err, domain.ErrE2EEBlobNotReadyForSend) {
		t.Fatal("old Ready attempt bypassed new reserved head", got, err)
	}
	r := &domain.E2EEBlobWriteRequest{Attempt: &domain.E2EEBlobAttemptRequest{Reservation: q, Revision: g.Revision, AttemptID: g.AttemptID}, WriterID: uuid.NewString(), StorageID: files.StorageID()}
	readyAttachment(t, f, r, files, data)
	acceptAttachment(t, f, q)
	var attempt string
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT attempt_id::text FROM e2ee_message_blobs`).Scan(&attempt); err != nil || attempt != g.AttemptID {
		t.Fatal("linked superseded attempt", attempt, err)
	}
	f.quota(t, 128, 1, 2)
}

func TestAttachmentSendExpiryCleanupAndLateMessageAckPreserveFilePostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	req := attachmentSendRequest(r.Attempt.Reservation)
	acceptAttachment(t, f, r.Attempt.Reservation)
	expireMessage(t, f.f.db, req.Envelope.MessageId) // Test-only receipt clock travel.
	got, err := f.delivery.Cleanup(t.Context(), domain.E2EECleanupExpired, nil)
	if err != nil || got.Removed != 1 {
		t.Fatal(got, err)
	}
	statuses, err := f.delivery.StatusesV2(t.Context(), 1, f.statusRequest(req))
	if err != nil || statuses.Messages[0].State != pb.MessageState_MESSAGE_STATE_EXPIRED {
		t.Fatal(statuses, err)
	}
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(req)); err != nil {
		t.Fatal("late descriptor ACK", err)
	}
	statuses, err = f.delivery.StatusesV2(t.Context(), 1, f.statusRequest(req))
	if err != nil || statuses.Messages[0].State != pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED {
		t.Fatal("late ACK did not refine expiry", statuses, err)
	}
	var reason string
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT reason FROM e2ee_message_removals`).Scan(&reason); err != nil || reason != "expired" {
		t.Fatal("late ACK erased earlier expiry fact", reason, err)
	}
	saved, err := f.delivery.SendV2(t.Context(), 1, req)
	if err != nil || !proto.Equal(saved, statuses.Messages[0].Receipt) {
		t.Fatal("expired exact Send renewed TTL", saved, err)
	}
	registryCount(t, f.f.db, "e2ee_message_inbox", 0)
	registryCount(t, f.f.db, "e2ee_message_blobs", 1)
	if err = files.Verify(t.Context(), blobstore.Attempt{ID: r.Attempt.AttemptID, Bytes: int64(len(data)), SHA256: [32]byte(r.Attempt.Reservation.Blob.Sha256)}); err != nil {
		t.Fatal("message cleanup removed accepted file", err)
	}
	f.quota(t, 64, 1, 1)
}

func TestAttachmentSendMigrationRetainsTextAndAcquisitionPostgres(t *testing.T) {
	// Own a real goose version history at032, before dependent FILEACK tables.
	// The generic fixture applies raw SQL and has no goose migration ledger.
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 32); err != nil {
		t.Fatal(err)
	}
	f := epochPersistenceOnDB(t, db)
	r := f.message(0)
	receipt, err := f.delivery.SendV2(t.Context(), 1, r)
	if err != nil {
		t.Fatal(err)
	}
	acquire := f.acquire(1, 0)
	keys, err := f.keys.AcquireV2(t.Context(), 1, acquire)
	if err != nil {
		t.Fatal(err)
	}
	// Framing3 belongs to message envelopes, never acquisition snapshots. The
	// original acquisition trigger still rejects it BEFORE unique-key checks.
	_, err = f.f.db.ExecContext(t.Context(), `INSERT INTO e2ee_acquisitions(owner_user_id,owner_generation_id,acquisition_id,conversation_id,peer_user_id,peer_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,signed_prekey_id,ec_prekey_id,kyber_prekey_id,kyber_is_last_resort,public_snapshot,created_at,not_after,wire_version,crypto_epoch)
        SELECT owner_user_id,owner_generation_id,gen_random_uuid(),conversation_id,peer_user_id,peer_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,signed_prekey_id,ec_prekey_id,kyber_prekey_id,kyber_is_last_resort,public_snapshot,created_at,not_after,3,crypto_epoch FROM e2ee_acquisitions`)
	if err == nil || !strings.Contains(err.Error(), "new persistence requires active epoch") {
		t.Fatal("acquisition framing was broadened by receipt migration", err)
	}
	// Roll032 back through its real ledger; Up restores the current schema in
	// dependency order, including033's FILEACK FK. Never use CASCADE here.
	if err = goose.DownTo(f.f.db, "../../migrations", 31); err != nil {
		t.Fatal("text-only receipt migration Down", err)
	}
	if err = goose.Up(f.f.db, "../../migrations"); err != nil {
		t.Fatal("text-only receipt migration Up", err)
	}
	again, err := f.delivery.SendV2(t.Context(), 1, r)
	if err != nil || !proto.Equal(again, receipt) {
		t.Fatal("migration roundtrip changed text receipt", err)
	}
	replayed, err := f.keys.AcquireV2(t.Context(), 1, acquire)
	if err != nil || !proto.Equal(replayed, keys) {
		t.Fatal("migration roundtrip changed acquisition snapshot", err)
	}
	page, err := f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 1 || !proto.Equal(page[0].Envelope, r.Envelope) {
		t.Fatal("migration roundtrip changed pending text", err)
	}
}

func TestAttachmentSendSchemaSerializesConcurrentRawAllocationPostgres(t *testing.T) {
	f, r, files, data := blobWriterFixture(t)
	readyAttachment(t, f, r, files, data)
	gate, err := f.f.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Close()
	// Pause the PRODUCTION Send while its claim/Ready locks are held. The raw
	// competing INSERT deliberately bypasses the repository's canonical locking.
	if _, err = gate.ExecContext(t.Context(), `SELECT pg_advisory_lock(84711932)`); err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			_, _ = gate.ExecContext(t.Context(), `SELECT pg_advisory_unlock(84711932)`)
		}
	}()
	if _, err = f.f.db.ExecContext(t.Context(), `CREATE FUNCTION send3_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        PERFORM pg_advisory_xact_lock(84711932); RETURN NEW; END $$;
        CREATE TRIGGER zz_send3_gate BEFORE INSERT ON e2ee_send_receipts FOR EACH ROW EXECUTE FUNCTION send3_gate()`); err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() {
		_, err := f.delivery.SendV2(t.Context(), 1, attachmentSendRequest(r.Attempt.Reservation))
		sent <- err
	}()
	wait := func(query string, args ...any) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for {
			var blocked bool
			if err := f.f.db.QueryRowContext(t.Context(), query, args...).Scan(&blocked); err != nil {
				t.Fatal(err)
			}
			if blocked {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal("SQL race fixture did not reach lock barrier")
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	wait(`SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND objid=84711932 AND NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`)
	raw, err := f.f.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	var pid int
	if err = raw.QueryRowContext(t.Context(), `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		t.Fatal(err)
	}
	allocated := make(chan error, 1)
	go func() {
		_, err := raw.ExecContext(t.Context(), `INSERT INTO e2ee_blob_attempts(attempt_id,message_id,revision,operation_id,expected_revision,expected_attempt_id,reserved_at,expires_at,charged_bytes)
            SELECT gen_random_uuid(),message_id,revision+1,gen_random_uuid(),revision,attempt_id,
            statement_timestamp(),statement_timestamp()+INTERVAL '1 hour',charged_bytes FROM e2ee_blob_attempts WHERE attempt_id=$1`, r.Attempt.AttemptID)
		allocated <- err
	}()
	wait(`SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND wait_event_type='Lock')`, pid)
	if _, err = gate.ExecContext(t.Context(), `SELECT pg_advisory_unlock(84711932)`); err != nil {
		t.Fatal(err)
	}
	locked = false
	if err = <-sent; err != nil {
		t.Fatal("production Send lost race", err)
	}
	if err = <-allocated; err == nil || !strings.Contains(err.Error(), "accepted message cannot allocate") {
		t.Fatal("raw writer crossed receipt COMMIT and allocated another attempt", err)
	}
	for _, table := range []string{"e2ee_send_receipts", "e2ee_message_inbox", "e2ee_message_blobs", "e2ee_blob_attempts"} {
		registryCount(t, f.f.db, table, 1)
	}
	f.quota(t, 64, 1, 1)
}
