package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

type blobReservationFixture struct {
	*epochPersistenceFixture
	blobs  *PgE2EEBlobRepository
	limits domain.E2EEBlobLimits
}

func blobReservations(t *testing.T, override func(*domain.E2EEBlobLimits)) *blobReservationFixture {
	t.Helper()
	return blobReservationsOnFixture(t, epochPersistenceDB(t), override)
}
func blobReservationsOnFixture(t *testing.T, f *epochPersistenceFixture, override func(*domain.E2EEBlobLimits)) *blobReservationFixture {
	t.Helper()
	l := domain.E2EEBlobLimits{GlobalBytes: 4096, AccountBytes: 2048, GlobalClaims: 16, AccountClaims: 8, GlobalAttempts: 32, AccountAttempts: 16}
	if override != nil {
		override(&l)
	}
	r := NewPgE2EEBlobRepository(f.f.db)
	if err := r.Configure(t.Context(), l); err != nil {
		t.Fatal(err)
	}
	return &blobReservationFixture{f, r, l}
}
func (f *blobReservationFixture) request(uid int64) *domain.E2EEBlobReservationRequest {
	peer := int64(2)
	if uid == 2 {
		peer = 1
	}
	epoch := uint64(0)
	return &domain.E2EEBlobReservationRequest{Context: f.context(uid), MessageID: uuid.NewString(), ConversationID: f.chat, CryptoEpoch: &epoch,
		Sender: f.participant(uid).Device, Recipient: f.participant(peer).Device, Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: 64, Sha256: make([]byte, 32)}, OperationID: uuid.NewString()}
}
func cloneBlobReservation(r *domain.E2EEBlobReservationRequest) *domain.E2EEBlobReservationRequest {
	c := *r
	epoch := *r.CryptoEpoch
	c.CryptoEpoch = &epoch
	c.Context = proto.Clone(r.Context).(*pb.ClientContext)
	c.Sender = proto.Clone(r.Sender).(*pb.DeviceRef)
	c.Recipient = proto.Clone(r.Recipient).(*pb.DeviceRef)
	c.Blob = proto.Clone(r.Blob).(*v2.BlobRef)
	return &c
}
func (f *blobReservationFixture) reserve(t *testing.T, uid int64, r *domain.E2EEBlobReservationRequest) *domain.E2EEBlobReservation {
	t.Helper()
	g, err := f.blobs.Reserve(t.Context(), uid, r)
	if err != nil || g == nil {
		t.Fatal(g, err)
	}
	return g
}
func (f *blobReservationFixture) quota(t *testing.T, bytes, claims, attempts int64) {
	t.Helper()
	var g blobQuotaCounters
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT reserved_bytes,retained_claims,retained_attempts FROM e2ee_blob_global_quota`).Scan(&g.bytes, &g.claims, &g.attempts); err != nil {
		t.Fatal(err)
	}
	if g != (blobQuotaCounters{bytes, claims, attempts}) {
		t.Fatal("unexpected quota", g, bytes, claims, attempts)
	}
	var a blobQuotaCounters
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT COALESCE(sum(reserved_bytes),0),COALESCE(sum(retained_claims),0),COALESCE(sum(retained_attempts),0) FROM e2ee_blob_account_quota`).Scan(&a.bytes, &a.claims, &a.attempts); err != nil {
		t.Fatal(err)
	}
	if a != g {
		t.Fatal("global/account counters diverged", a, g)
	}
}
func (f *blobReservationFixture) age(t *testing.T, g *domain.E2EEBlobReservation) {
	t.Helper()
	tx, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Test-only time travel; runtime has no deadline mutation API.
	if _, err = tx.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt;
        UPDATE e2ee_blob_attempts SET reserved_at=reserved_at-INTERVAL '2 hours',expires_at=expires_at-INTERVAL '2 hours';
        ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	g.ReservedAt = g.ReservedAt.Add(-2 * time.Hour)
	g.ExpiresAt = g.ExpiresAt.Add(-2 * time.Hour)
}

func TestE2EEBlobReservationReplayAndConcurrentFirstUsePostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	const count = 12
	results := make(chan *domain.E2EEBlobReservation, count)
	errs := make(chan error, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range count {
		wg.Go(func() { <-start; g, err := f.blobs.Reserve(t.Context(), 1, r); results <- g; errs <- err })
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var first *domain.E2EEBlobReservation
	for g := range results {
		if g == nil {
			t.Fatal("nil grant")
		}
		if first == nil {
			first = g
		}
		if g.AttemptID != first.AttemptID || g.Revision != 1 || !g.ReservedAt.Equal(first.ReservedAt) || !g.ExpiresAt.Equal(first.ExpiresAt) || g.Expired || g.State != "reserved" {
			t.Fatal("replay changed grant", g, first)
		}
	}
	if first.ExpiresAt.Sub(first.ReservedAt) != time.Hour {
		t.Fatal("staging lifetime", first)
	}
	f.quota(t, 64, 1, 1)
	registryCount(t, f.f.db, "e2ee_message_claims", 1)
	registryCount(t, f.f.db, "e2ee_blob_attempts", 1)
	registryCount(t, f.f.db, "e2ee_send_receipts", 0)
	registryCount(t, f.f.db, "e2ee_message_inbox", 0)
	if err := f.blobs.Configure(t.Context(), f.limits); err != nil {
		t.Fatal(err)
	}
	l := f.limits
	l.GlobalBytes++
	if err := f.blobs.Configure(t.Context(), l); !errors.Is(err, domain.ErrE2EEBlobConfiguration) {
		t.Fatal("silently changed budget", err)
	}
	f.quota(t, 64, 1, 1)
	for name, mutate := range map[string]func(*domain.E2EEBlobReservationRequest){
		"blob UUID":  func(r *domain.E2EEBlobReservationRequest) { r.Blob.BlobId = uuid.NewString() },
		"blob bytes": func(r *domain.E2EEBlobReservationRequest) { r.Blob.CiphertextBytes++ },
		"blob hash":  func(r *domain.E2EEBlobReservationRequest) { r.Blob.Sha256[0]++ },
		"replay arguments": func(r *domain.E2EEBlobReservationRequest) {
			r.ExpectedRevision = 1
			r.ExpectedAttemptID = first.AttemptID
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := cloneBlobReservation(r)
			mutate(bad)
			if _, err := f.blobs.Reserve(t.Context(), 1, bad); !errors.Is(err, domain.ErrE2EEMessageConflict) {
				t.Fatal(err)
			}
			f.quota(t, 64, 1, 1)
		})
	}
	other := f.request(1)
	other.Blob.BlobId = r.Blob.BlobId
	if _, err := f.blobs.Reserve(t.Context(), 1, other); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("blob UUID reused", err)
	}
	other = f.request(1)
	other.OperationID = r.OperationID
	if _, err := f.blobs.Reserve(t.Context(), 1, other); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("operation reused", err)
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 1)
	f.quota(t, 64, 1, 1)
}

func TestE2EEBlobReservationExpiryRenewalCASPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	g := f.reserve(t, 1, r)
	renew := cloneBlobReservation(r)
	renew.OperationID = uuid.NewString()
	renew.ExpectedRevision = g.Revision
	renew.ExpectedAttemptID = g.AttemptID
	if _, err := f.blobs.Reserve(t.Context(), 1, renew); !errors.Is(err, domain.ErrE2EEBlobLeaseActive) {
		t.Fatal("renewed live lease", err)
	}
	f.age(t, g)
	replay := f.reserve(t, 1, r)
	if !replay.Expired || !replay.ExpiresAt.Equal(g.ExpiresAt) || replay.AttemptID != g.AttemptID {
		t.Fatal("replay extended expired lease", replay, g)
	}
	const count = 6
	var wg sync.WaitGroup
	errs := make(chan error, count)
	grants := make(chan *domain.E2EEBlobReservation, count)
	start := make(chan struct{})
	for range count {
		q := cloneBlobReservation(renew)
		q.OperationID = uuid.NewString()
		wg.Go(func() { <-start; grant, err := f.blobs.Reserve(t.Context(), 1, q); errs <- err; grants <- grant })
	}
	close(start)
	wg.Wait()
	close(errs)
	close(grants)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, domain.ErrE2EEBlobCAS) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal("CAS winners", wins)
	}
	var newGrant *domain.E2EEBlobReservation
	for v := range grants {
		if v != nil {
			newGrant = v
		}
	}
	if newGrant.Revision != 2 || newGrant.AttemptID == g.AttemptID || newGrant.Expired {
		t.Fatal(newGrant)
	}
	var state string
	if err := f.f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, g.AttemptID).Scan(&state); err != nil || state != "deleting" {
		t.Fatal(state, err)
	}
	f.quota(t, 128, 1, 2) // Expiry does NOT prove that physical bytes were removed.
	if _, err := f.blobs.Reserve(t.Context(), 1, r); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("old op renewed twice", err)
	}
	stale := cloneBlobReservation(renew)
	stale.OperationID = uuid.NewString()
	if _, err := f.blobs.Reserve(t.Context(), 1, stale); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("stale attempt", err)
	}
	oldOp := cloneBlobReservation(r)
	oldOp.ExpectedRevision = 2
	oldOp.ExpectedAttemptID = newGrant.AttemptID
	if _, err := f.blobs.Reserve(t.Context(), 1, oldOp); !errors.Is(err, domain.ErrE2EEBlobCAS) {
		t.Fatal("superseded op minted lease", err)
	}
	f.quota(t, 128, 1, 2)
}

func TestE2EEBlobReservationCapacityRollbackPostgres(t *testing.T) {
	for _, name := range []string{"account bytes", "global bytes", "account claims", "global claims", "account attempts", "global attempts"} {
		t.Run(name, func(t *testing.T) {
			f := blobReservations(t, func(l *domain.E2EEBlobLimits) {
				switch name {
				case "account bytes":
					l.AccountBytes = 64
				case "global bytes":
					l.GlobalBytes = 64
					l.AccountBytes = 64
				case "account claims":
					l.AccountClaims = 1
				case "global claims":
					l.GlobalClaims = 1
					l.AccountClaims = 1
				case "account attempts":
					l.AccountAttempts = 1
				case "global attempts":
					l.GlobalAttempts = 1
					l.AccountAttempts = 1
				}
			})
			r := f.request(1)
			g := f.reserve(t, 1, r)
			q := f.request(1)
			uid := int64(1)
			if name == "global bytes" || name == "global claims" || name == "global attempts" {
				q = f.request(2)
				uid = 2
			}
			if _, err := f.blobs.Reserve(t.Context(), uid, q); !errors.Is(err, domain.ErrE2EEBlobCapacity) {
				t.Fatal("capacity bypass", err)
			}
			registryCount(t, f.f.db, "e2ee_message_claims", 1)
			registryCount(t, f.f.db, "e2ee_blob_attempts", 1)
			f.quota(t, 64, 1, 1)
			f.age(t, g)
			replay := f.reserve(t, 1, r)
			if !replay.Expired {
				t.Fatal("lost expired replay")
			}
			if name == "account bytes" || name == "global bytes" || name == "account attempts" || name == "global attempts" {
				q = cloneBlobReservation(r)
				q.OperationID = uuid.NewString()
				q.ExpectedRevision = g.Revision
				q.ExpectedAttemptID = g.AttemptID
				if _, err := f.blobs.Reserve(t.Context(), 1, q); !errors.Is(err, domain.ErrE2EEBlobCapacity) {
					t.Fatal("expiry freed capacity", err)
				}
				var state string
				f.f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, g.AttemptID).Scan(&state)
				if state != "reserved" {
					t.Fatal("failed renewal changed old attempt", state)
				}
			}
		})
	}
}

func TestE2EEBlobReservationAuthAndEpochPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	for _, uid := range []int64{0, 2, 99} {
		if _, err := f.blobs.Reserve(t.Context(), uid, r); err == nil {
			t.Fatal("foreign owner", uid)
		}
	}
	bad := cloneBlobReservation(r)
	bad.Recipient.UserId = uuid.NewString()
	if _, err := f.blobs.Reserve(t.Context(), 1, bad); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("foreign peer", err)
	}
	bad = cloneBlobReservation(r)
	*bad.CryptoEpoch = 9
	if _, err := f.blobs.Reserve(t.Context(), 1, bad); !errors.Is(err, domain.ErrE2EEEpochInactive) {
		t.Fatal("unknown epoch", err)
	}
	f.reserve(t, 1, r)
	f.transition(t)
	if _, err := f.blobs.Reserve(t.Context(), 1, r); !errors.Is(err, domain.ErrE2EEEpochInactive) {
		t.Fatal("sealed epoch upload grant", err)
	}
	newReq := f.request(1)
	*newReq.CryptoEpoch = 1
	f.reserve(t, 1, newReq)
	f.quota(t, 128, 2, 2)
	f.f.replace(t, 2, f.gen(2))
	if _, err := f.blobs.Reserve(t.Context(), 1, newReq); !errors.Is(err, domain.ErrE2EEConversationClosed) {
		t.Fatal("closed chat upload grant", err)
	}
	if _, err := f.blobs.Reserve(t.Context(), 2, f.request(2)); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired owner", err)
	}
	f.quota(t, 128, 2, 2)
}

func TestE2EEBlobReservationAccountBudgetSurvivesReplacementPostgres(t *testing.T) {
	f := blobReservations(t, func(l *domain.E2EEBlobLimits) { l.AccountBytes = 64 })
	f.reserve(t, 1, f.request(1))
	d := f.f.replace(t, 1, f.gen(1))
	f.f.low = d
	if _, err := NewPgE2EEPreKeyRepository(f.f.db).Publish(t.Context(), 1, testutil.PublicPreKeyUpload(t, d.GenerationID)); err != nil {
		t.Fatal(err)
	}
	f.f.req.Context.DeviceGenerationId = d.GenerationID
	f.f.req.CreationRequestId = uuid.NewString()
	f.chat = f.f.create(t).ID
	if _, err := f.blobs.Reserve(t.Context(), 1, f.request(1)); !errors.Is(err, domain.ErrE2EEBlobCapacity) {
		t.Fatal("replacement reset account budget", err)
	}
	f.quota(t, 64, 1, 1)
	registryCount(t, f.f.db, "e2ee_blob_account_quota", 1)
}

func TestE2EEBlobReservationSQLAndMigrationPostgres(t *testing.T) {
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 28); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty Down", err)
	}
	if err := goose.UpTo(db, "../../migrations", 28); err != nil {
		t.Fatal(err)
	}
	seed := seedConversations(t, db)
	chat := seed.create(t)
	epoch := uint64(0)
	text := &v2.SendMessageRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: seed.low.GenerationID}, Envelope: &v2.Envelope{ProtocolVersion: 2, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: chat.ID,
		Sender: &pb.DeviceRef{UserId: seed.low.UserUUID, DeviceId: 1, GenerationId: seed.low.GenerationID}, Recipient: &pb.DeviceRef{UserId: seed.high.UserUUID, DeviceId: 1, GenerationId: seed.high.GenerationID}, CryptoEpoch: &epoch, MessageType: 1, Ciphertext: []byte{1}}}
	delivery := NewPgE2EEDeliveryRepository(db)
	before, err := delivery.SendV2(t.Context(), 1, text)
	if err != nil {
		t.Fatal(err)
	}
	if err = goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("text-only Down", err)
	}
	if err = goose.UpTo(db, "../../migrations", 28); err != nil {
		t.Fatal("backfill retained text claims", err)
	}
	after, err := delivery.SendV2(t.Context(), 1, text)
	if err != nil || !proto.Equal(before, after) {
		t.Fatal("migration changed receipt", before, after, err)
	}
	registryCount(t, db, "e2ee_message_claims", 1)
	for _, stmt := range []string{
		`UPDATE e2ee_message_claims SET envelope_digest=decode(repeat('00',32),'hex')`,
		`DELETE FROM e2ee_message_claims`,
		`INSERT INTO e2ee_message_claims SELECT 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,low_user_id,high_user_id,low_generation_id,high_generation_id,wire_version,crypto_epoch,envelope_digest,blob_id,blob_bytes,blob_sha256 FROM e2ee_message_claims`,
	} {
		if _, err = db.ExecContext(t.Context(), stmt); err == nil {
			t.Fatal("SQL accepted claim mutation/orphan", stmt)
		}
	}
	f := blobReservations(t, nil)
	g := f.reserve(t, 1, f.request(1))
	copyClaim := `INSERT INTO e2ee_message_claims SELECT $1,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,wire_version,crypto_epoch,envelope_digest,$2,blob_bytes,blob_sha256 FROM e2ee_message_claims`
	if _, err = f.f.db.ExecContext(t.Context(), copyClaim, uuid.NewString(), uuid.NewString()); err == nil {
		t.Fatal("SQL committed orphan blob claim")
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 1)
	for _, stmt := range []string{
		`UPDATE e2ee_blob_attempts SET expires_at=expires_at+INTERVAL '1 hour',reserved_at=reserved_at+INTERVAL '1 hour'`,
		`UPDATE e2ee_blob_attempts SET operation_id='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'`,
		`UPDATE e2ee_blob_attempts SET state='linked'`,
		`UPDATE e2ee_blob_heads SET revision=revision+1`,
	} {
		if _, err = f.f.db.ExecContext(t.Context(), stmt); err == nil {
			t.Fatal("SQL accepted attempt rewrite", stmt)
		}
	}
	if g.ExpiresAt.Sub(g.ReservedAt) != time.Hour {
		t.Fatal("deadline")
	}
	f.quota(t, 64, 1, 1)
	f.transition(t)
	if _, err = f.f.db.ExecContext(t.Context(), copyClaim, uuid.NewString(), uuid.NewString()); err == nil {
		t.Fatal("SQL reserved in sealed epoch")
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 1)
	// Actual goose rollback with a retained blob, rather than a missing version table.
	br := NewPgE2EEBlobRepository(db)
	if err = br.Configure(t.Context(), f.limits); err != nil {
		t.Fatal(err)
	}
	r := f.request(1)
	r.Context = text.Context
	r.Sender = text.Envelope.Sender
	r.Recipient = text.Envelope.Recipient
	r.ConversationID = chat.ID
	if _, err = br.Reserve(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	if err = goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("Down discarded blob evidence")
	}
	registryCount(t, db, "e2ee_blob_attempts", 1)
	registryCount(t, db, "e2ee_message_claims", 2)
}

func TestE2EEBlobReservationCommitFaultAndCancellationPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION blob_commit_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic blob COMMIT fault';END $$;
        CREATE CONSTRAINT TRIGGER blob_commit_fault AFTER INSERT ON e2ee_blob_attempts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION blob_commit_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	if g, err := f.blobs.Reserve(t.Context(), 1, r); err == nil || g != nil {
		t.Fatal("failed commit returned grant", g, err)
	}
	for _, table := range []string{"e2ee_message_claims", "e2ee_blob_attempts", "e2ee_blob_heads", "e2ee_blob_account_quota"} {
		registryCount(t, f.f.db, table, 0)
	}
	f.quota(t, 0, 0, 0)
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER blob_commit_fault ON e2ee_blob_attempts;DROP FUNCTION blob_commit_fault()`); err != nil {
		t.Fatal(err)
	}
	tx, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(t.Context(), `SELECT singleton FROM e2ee_blob_global_quota FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	if g, err := f.blobs.Reserve(ctx, 1, r); err == nil || g != nil {
		t.Fatal("lock wait ignored cancellation", g, err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 0)
	f.quota(t, 0, 0, 0)
	f.reserve(t, 1, r)
}

func TestE2EEBlobReservationTextUUIDRaceAndSendRollbackPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	for range 8 {
		r := f.request(1)
		text := f.message(0)
		text.Envelope.MessageId = r.MessageID
		var blobErr, textErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Go(func() { <-start; _, blobErr = f.blobs.Reserve(t.Context(), 1, r) })
		wg.Go(func() { <-start; _, textErr = f.delivery.SendV2(t.Context(), 1, text) })
		close(start)
		wg.Wait()
		if (blobErr == nil) == (textErr == nil) {
			t.Fatal("UUID has two winners or no winner", blobErr, textErr)
		}
		loser := blobErr
		if loser == nil {
			loser = textErr
		}
		if !errors.Is(loser, domain.ErrE2EEMessageConflict) {
			t.Fatal(loser)
		}
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 8)
	// Failed text COMMIT must not steal a UUID from a subsequent blob intent.
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION text_commit_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic text COMMIT fault';END $$;
        CREATE CONSTRAINT TRIGGER text_commit_fault AFTER INSERT ON e2ee_send_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION text_commit_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	r := f.request(2)
	text := f.message(0)
	text.Envelope.MessageId = r.MessageID
	if g, err := f.delivery.SendV2(t.Context(), 1, text); err == nil || g != nil {
		t.Fatal("failed text COMMIT returned receipt", g, err)
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 8)
	f.reserve(t, 2, r)
}

func TestE2EEBlobReservationNoConfigurationAndUnreservedSendPostgres(t *testing.T) {
	f := &blobReservationFixture{epochPersistenceFixture: epochPersistenceDB(t)}
	f.blobs = NewPgE2EEBlobRepository(f.f.db)
	r := f.request(1)
	if _, err := f.blobs.Reserve(t.Context(), 1, r); !errors.Is(err, domain.ErrE2EEBlobConfiguration) {
		t.Fatal(err)
	}
	registryCount(t, f.f.db, "e2ee_message_claims", 0)
	if err := f.blobs.Configure(t.Context(), domain.E2EEBlobLimits{}); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal(err)
	}
	text := f.message(0)
	text.Envelope.ProtocolVersion = 3
	text.Envelope.Blob = r.Blob
	if _, err := f.delivery.SendV2(t.Context(), 1, text); !errors.Is(err, domain.ErrE2EEBlobNotReadyForSend) {
		t.Fatal("unlinked attachment enabled", err)
	}
}

func TestE2EEBlobReservationConcurrentGlobalCapacityPostgres(t *testing.T) {
	f := blobReservations(t, func(l *domain.E2EEBlobLimits) { l.GlobalBytes = 64; l.AccountBytes = 64 })
	const count = 12
	errs := make(chan error, count)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range count {
		uid := int64(i%2 + 1)
		r := f.request(uid)
		wg.Go(func() { <-start; _, err := f.blobs.Reserve(t.Context(), uid, r); errs <- err })
	}
	close(start)
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, domain.ErrE2EEBlobCapacity) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal("global reservation winners", wins)
	}
	f.quota(t, 64, 1, 1)
	registryCount(t, f.f.db, "e2ee_message_claims", 1)
	registryCount(t, f.f.db, "e2ee_blob_attempts", 1)
	registryCount(t, f.f.db, "e2ee_blob_account_quota", 1)
}

func TestE2EEBlobReservationRenewalCommitRollbackPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	g := f.reserve(t, 1, r)
	f.age(t, g)
	q := cloneBlobReservation(r)
	q.OperationID = uuid.NewString()
	q.ExpectedRevision = g.Revision
	q.ExpectedAttemptID = g.AttemptID
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION renewal_commit_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic renewal COMMIT fault';END $$;
        CREATE CONSTRAINT TRIGGER renewal_commit_fault AFTER UPDATE ON e2ee_blob_heads DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION renewal_commit_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	if grant, err := f.blobs.Reserve(t.Context(), 1, q); err == nil || grant != nil {
		t.Fatal("failed renewal returned grant", grant, err)
	}
	f.quota(t, 64, 1, 1)
	registryCount(t, f.f.db, "e2ee_blob_attempts", 1)
	replay := f.reserve(t, 1, r)
	if replay.Revision != 1 || replay.AttemptID != g.AttemptID || replay.State != "reserved" || !replay.Expired {
		t.Fatal("failed commit fenced old attempt", replay)
	}
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER renewal_commit_fault ON e2ee_blob_heads;DROP FUNCTION renewal_commit_fault()`); err != nil {
		t.Fatal(err)
	}
	renewed := f.reserve(t, 1, q)
	if renewed.Revision != 2 {
		t.Fatal(renewed)
	}
	f.quota(t, 128, 1, 2)
	firstRetry := f.reserve(t, 1, q)
	if firstRetry.AttemptID != renewed.AttemptID || !firstRetry.ExpiresAt.Equal(renewed.ExpiresAt) {
		t.Fatal("renewal replay changed grant", firstRetry, renewed)
	}
	f.quota(t, 128, 1, 2)
}

func TestE2EEBlobReservationClockAfterQuotaWaitPostgres(t *testing.T) {
	f := blobReservations(t, nil)
	r := f.request(1)
	g := f.reserve(t, 1, r)
	tx, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Test-only short deadline, with the same exact one-hour interval.
	if _, err = tx.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt;
        WITH stamp AS MATERIALIZED (SELECT date_trunc('milliseconds',clock_timestamp()) AS t)
        UPDATE e2ee_blob_attempts SET reserved_at=stamp.t-INTERVAL '1 hour'+INTERVAL '400 milliseconds',
        expires_at=stamp.t+INTERVAL '400 milliseconds' FROM stamp;
        ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	lock, err := f.f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err = lock.ExecContext(t.Context(), `SELECT singleton FROM e2ee_blob_global_quota FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	q := cloneBlobReservation(r)
	q.OperationID = uuid.NewString()
	q.ExpectedRevision = g.Revision
	q.ExpectedAttemptID = g.AttemptID
	result := make(chan error, 1)
	go func() { _, err := f.blobs.Reserve(t.Context(), 1, q); result <- err }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var expired bool
		if err = lock.QueryRowContext(t.Context(), `SELECT clock_timestamp()>=expires_at FROM e2ee_blob_attempts WHERE attempt_id=$1`, g.AttemptID).Scan(&expired); err != nil {
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
	if err = <-result; err != nil {
		t.Fatal("used stale pre-wait clock", err)
	}
	f.quota(t, 128, 1, 2)
}
