package repository

import (
	"bytes"
	"database/sql"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

type epochPersistenceFixture struct {
	*recoveryFixture
	delivery *PgE2EEDeliveryRepository
	keys     *PgE2EEPreKeyRepository
}

func epochPersistenceDB(t *testing.T) *epochPersistenceFixture {
	t.Helper()
	return epochPersistenceOnDB(t, testutil.Postgres(t))
}

func epochPersistenceOnDB(t *testing.T, db *sql.DB) *epochPersistenceFixture {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO users(id,user_uuid,username,email,password_hash) VALUES
        (1,'11111111-1111-4111-8111-111111111111','a','a@epoch.invalid','verified-test-hash'),
        (2,'22222222-2222-4222-8222-222222222222','b','b@epoch.invalid','verified-test-hash');
        INSERT INTO user_settings(user_id)SELECT id FROM users;INSERT INTO friend_requests(from_user_id,to_user_id,status)VALUES(1,2,'accepted')`)
	if err != nil {
		t.Fatal(err)
	}
	f := &conversationFixture{db: db, devices: NewPgE2EEDeviceRepository(db), repo: NewPgE2EEConversationRepository(db)}
	for _, uid := range []int64{1, 2} {
		material := registrationMaterial()
		material.RegistrationID = 16383
		d, err := f.devices.Register(t.Context(), uid, material)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = NewPgE2EEPreKeyRepository(db).Publish(t.Context(), uid, testutil.PublicPreKeyUpload(t, d.Device.GenerationID)); err != nil {
			t.Fatal(err)
		}
		if uid == 1 {
			f.low = d.Device
		} else {
			f.high = d.Device
		}
	}
	f.req = &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.low.GenerationID}, CreationRequestId: uuid.NewString(), PeerUserId: 2}
	c := f.create(t)
	r := &recoveryFixture{f: f, repo: NewPgE2EERecoveryRepository(db), chat: c.ID}
	r.p = r.proposal(1, 0, 0)
	return &epochPersistenceFixture{recoveryFixture: r, delivery: NewPgE2EEDeliveryRepository(db), keys: NewPgE2EEPreKeyRepository(db)}
}
func (f *epochPersistenceFixture) context(uid int64) *pb.ClientContext {
	return &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: f.gen(uid)}
}
func (f *epochPersistenceFixture) message(epoch uint64) *v2.SendMessageRequest {
	return &v2.SendMessageRequest{Context: f.context(1), Envelope: &v2.Envelope{ProtocolVersion: 2, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: f.chat, Sender: f.participant(1).Device, Recipient: f.participant(2).Device, MessageType: 1, Ciphertext: []byte("opaque synthetic v2 ciphertext, not actual encrypted text"), CryptoEpoch: &epoch}}
}
func (f *epochPersistenceFixture) acquire(uid int64, epoch uint64) *v2.AcquireBundleRequest {
	peer := int64(2)
	if uid == 2 {
		peer = 1
	}
	return &v2.AcquireBundleRequest{Context: f.context(uid), ConversationId: f.chat, AcquisitionId: uuid.NewString(), ExpectedPeer: f.participant(peer).Device, CryptoEpoch: &epoch}
}
func (f *epochPersistenceFixture) transition(t *testing.T) {
	t.Helper()
	f.propose(t, f.p, 1)
	f.decide(t, recoveryDecision(f.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT), 2)
}
func (f *epochPersistenceFixture) ack(r *v2.SendMessageRequest) *pb.AcknowledgeMessagesRequest {
	digest, _ := boundary.EpochEnvelopeDigest(r.Envelope)
	return &pb.AcknowledgeMessagesRequest{Context: f.context(2), Messages: []*pb.Acknowledgement{{MessageId: r.Envelope.MessageId, EnvelopeDigest: digest[:], Disposition: pb.AckDisposition_ACK_DISPOSITION_STORED}}}
}
func assertEpochFence(t *testing.T, err error, req *v2.SendMessageRequest) {
	t.Helper()
	var sealed *domain.E2EEEpochSealed
	if !errors.As(err, &sealed) || !errors.Is(err, domain.ErrE2EEEpochSealed) {
		t.Fatal("missing bound fence", err)
	}
	binding, e := boundary.EpochSendBinding(req.Envelope)
	if e != nil {
		t.Fatal(e)
	}
	if e = boundary.ValidateEpochSealedFence(sealed.Fence, binding, sealed.Recovery); e != nil {
		t.Fatal(e)
	}
}

func TestE2EEEpochPersistenceReceiptBeforeSealAndRetainedAckPostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	r := f.message(0)
	receipt, err := f.delivery.SendV2(t.Context(), 1, r)
	if err != nil {
		t.Fatal(err)
	}
	f.transition(t)
	// Ignore the first receipt as if the response was lost. Exact ciphertext/UUID
	// retry after seal must still return that receipt, never EpochSealed/re-enqueue.
	for range 2 {
		got, err := f.delivery.SendV2(t.Context(), 1, r)
		if err != nil || !proto.Equal(got, receipt) {
			t.Fatal("accepted retry lost receipt", err)
		}
	}
	page, err := f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 1 || !proto.Equal(page[0].Envelope, r.Envelope) {
		t.Fatal("sealed accepted ciphertext unavailable", page, err)
	}
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(r)); err != nil {
		t.Fatal(err)
	}
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(r)); err != nil {
		t.Fatal("duplicate ACK", err)
	}
	got, err := f.delivery.SendV2(t.Context(), 1, r)
	if err != nil || !proto.Equal(got, receipt) {
		t.Fatal("ACK discarded immutable receipt", err)
	}
	page, err = f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 0 {
		t.Fatal("retry re-enqueued ACKed ciphertext", err)
	}
	changed := proto.Clone(r).(*v2.SendMessageRequest)
	changed.Envelope.Ciphertext = append(changed.Envelope.Ciphertext, 1)
	if _, err = f.delivery.SendV2(t.Context(), 1, changed); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("changed accepted retry hidden by seal", err)
	}
	registryCount(t, f.f.db, "e2ee_send_receipts", 1)
	registryCount(t, f.f.db, "e2ee_message_acknowledgements", 1)
}

func TestE2EEEpochPersistenceSealBeforeSendAndExactFencePostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	r := f.message(0)
	f.transition(t)
	for range 2 {
		got, err := f.delivery.SendV2(t.Context(), 1, r)
		if got != nil {
			t.Fatal("receipt without COMMIT")
		}
		assertEpochFence(t, err, r)
	}
	registryCount(t, f.f.db, "e2ee_send_receipts", 0)
	registryCount(t, f.f.db, "e2ee_message_inbox", 0)
	registryCount(t, f.f.db, "e2ee_delivery_state", 0)
	current := f.message(1)
	if _, err := f.delivery.SendV2(t.Context(), 1, current); err != nil {
		t.Fatal("new active epoch blocked", err)
	}
	if _, err := f.delivery.SendV2(t.Context(), 1, f.message(2)); !errors.Is(err, domain.ErrE2EEEpochInactive) {
		t.Fatal("future epoch accepted", err)
	}
}

func TestE2EEEpochPersistenceConcurrentSendAcceptAndDuplicatePostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	f.propose(t, f.p, 1)
	r := f.message(0)
	accept := recoveryDecision(f.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var receipt *v2.SendReceipt
	var sendErr error
	wg.Go(func() { <-start; receipt, sendErr = f.delivery.SendV2(t.Context(), 1, r) })
	wg.Go(func() {
		<-start
		v, err := f.repo.Decide(t.Context(), 2, f.gen(2), accept)
		if err != nil || v.Outcome.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
			t.Error("accept", err)
		}
	})
	close(start)
	wg.Wait()
	f.head(t, 1)
	if receipt != nil {
		if sendErr != nil {
			t.Fatal(sendErr)
		}
		again, err := f.delivery.SendV2(t.Context(), 1, r)
		if err != nil || !proto.Equal(again, receipt) {
			t.Fatal(err)
		}
	} else {
		assertEpochFence(t, sendErr, r)
	}
	// Active-epoch duplicate sends share receipt/counters/sequence atomically.
	active := f.message(1)
	start = make(chan struct{})
	out := make(chan *v2.SendReceipt, 8)
	for range 8 {
		wg.Go(func() {
			<-start
			s, err := f.delivery.SendV2(t.Context(), 1, active)
			if err != nil {
				t.Error(err)
				return
			}
			out <- s
		})
	}
	close(start)
	wg.Wait()
	close(out)
	var first *v2.SendReceipt
	for s := range out {
		if first == nil {
			first = s
		} else if !proto.Equal(first, s) {
			t.Fatal("duplicate receipt changed")
		}
	}
	count := 1
	if receipt != nil {
		count = 2
	}
	registryCount(t, f.f.db, "e2ee_send_receipts", count)
	registryCount(t, f.f.db, "e2ee_message_inbox", count)
}

func TestE2EEEpochPersistenceAcquireReplayAndCreatorPostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	req := f.acquire(1, 0)
	first, err := f.keys.AcquireV2(t.Context(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	again, err := f.keys.AcquireV2(t.Context(), 1, req)
	if err != nil || !proto.Equal(first, again) {
		t.Fatal("acquisition replay changed keys/lease", err)
	}
	if _, err = f.f.db.ExecContext(t.Context(), `UPDATE e2ee_acquisitions SET public_snapshot=decode('01','hex')`); err == nil {
		t.Fatal("v2 public snapshot was rewritten")
	}
	f.transition(t)
	if _, err = f.keys.AcquireV2(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEEpochInactive) {
		t.Fatal("sealed snapshot reused for bootstrap", err)
	}
	changed := proto.Clone(req).(*v2.AcquireBundleRequest)
	changed.CryptoEpoch = proto.Uint64(1)
	if _, err = f.keys.AcquireV2(t.Context(), 1, changed); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("acquisition crossed epoch", err)
	}
	if _, err = f.keys.AcquireV2(t.Context(), 2, f.acquire(2, 1)); !errors.Is(err, domain.ErrE2EEBootstrapInitiator) {
		t.Fatal("peer initiated competing bootstrap", err)
	}
	active := f.acquire(1, 1)
	next, err := f.keys.AcquireV2(t.Context(), 1, active)
	if err != nil {
		t.Fatal(err)
	}
	if next.AcquisitionId == first.AcquisitionId || next.OneTimePreKey != nil && first.OneTimePreKey != nil && next.OneTimePreKey.KeyId == first.OneTimePreKey.KeyId {
		t.Fatal("reset/reused one-time allocation")
	}
	// The fixture has only one EC/PQ one-time: the second acquisition uses
	// optional EC exhaustion +retained LR, without resetting identities/key IDs.
	if next.OneTimePreKey != nil || !next.KyberPreKey.IsLastResort {
		t.Fatal("EC/PQ exhaustion contract", next.OneTimePreKey, next.KyberPreKey.IsLastResort)
	}
	registryCount(t, f.f.db, "e2ee_acquisitions", 2)
}

func TestE2EEEpochPersistenceDeferredSendFailureThenFencePostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	r := f.message(0)
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION epoch_send_commit_fault()RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.wire_version=2 THEN RAISE EXCEPTION 'synthetic epoch send COMMIT fault';END IF;RETURN NEW;END $$;
        CREATE CONSTRAINT TRIGGER epoch_send_commit_fault AFTER INSERT ON e2ee_send_receipts DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION epoch_send_commit_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.delivery.SendV2(t.Context(), 1, r)
	if err == nil || got != nil || errors.Is(err, domain.ErrE2EEEpochSealed) {
		t.Fatal("failed commit became receipt/fence", got, err)
	}
	registryCount(t, f.f.db, "e2ee_send_receipts", 0)
	registryCount(t, f.f.db, "e2ee_message_inbox", 0)
	registryCount(t, f.f.db, "e2ee_delivery_state", 0)
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER epoch_send_commit_fault ON e2ee_send_receipts;DROP FUNCTION epoch_send_commit_fault()`); err != nil {
		t.Fatal(err)
	}
	f.transition(t)
	_, err = f.delivery.SendV2(t.Context(), 1, r)
	assertEpochFence(t, err, r)
}

func TestE2EEEpochPersistenceSQLAndMigrationPostgres(t *testing.T) {
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err := goose.UpTo(db, "../../migrations", 27); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty Down", err)
	}
	if err := goose.UpTo(db, "../../migrations", 27); err != nil {
		t.Fatal(err)
	}
	// This DB has an actual goose version table, so a nonempty Down exercises
	// migration027's evidence guard rather than an absent migration history.
	seed := seedConversations(t, db)
	chat := seed.create(t)
	epoch := uint64(0)
	sent := &v2.SendMessageRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: seed.low.GenerationID}, Envelope: &v2.Envelope{ProtocolVersion: 2, CryptoSuite: 1, MessageId: uuid.NewString(), ConversationId: chat.ID,
		Sender: &pb.DeviceRef{UserId: seed.low.UserUUID, DeviceId: 1, GenerationId: seed.low.GenerationID}, Recipient: &pb.DeviceRef{UserId: seed.high.UserUUID, DeviceId: 1, GenerationId: seed.high.GenerationID}, MessageType: 1, Ciphertext: []byte{1}, CryptoEpoch: &epoch}}
	seedHistoricalE2EESend(t, db, sent.Envelope)
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("Down discarded retained epoch binding")
	}
	registryCount(t, db, "e2ee_send_receipts", 1)
	f := epochPersistenceDB(t)
	r := f.message(0)
	if _, err := f.delivery.SendV2(t.Context(), 1, r); err != nil {
		t.Fatal(err)
	}
	if _, err := f.f.db.ExecContext(t.Context(), `UPDATE e2ee_send_receipts SET crypto_epoch=1`); err == nil {
		t.Fatal("rewrote committed epoch")
	}
	f.transition(t)
	// Even a direct SQL insert cannot race past the active-epoch gate.
	_, err := f.f.db.ExecContext(t.Context(), `INSERT INTO e2ee_send_receipts SELECT $1,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,envelope_digest,stored_at,expires_at,wire_version,crypto_epoch FROM e2ee_send_receipts WHERE message_id=$2`, uuid.NewString(), r.Envelope.MessageId)
	if err == nil {
		t.Fatal("SQL bypassed sealed epoch")
	}
	var raw []byte
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT envelope FROM e2ee_message_inbox WHERE message_id=$1`, r.Envelope.MessageId).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	encoded, _ := proto.Marshal(r.Envelope)
	if !bytes.Equal(raw, encoded) {
		t.Fatal("transition rewrote old ciphertext")
	}
}

func TestE2EEEpochPersistenceRejectsPrototypeRowsAndUpgradePostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	modern := f.message(0)
	old := &pb.SendMessageRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.gen(1)}, Envelope: &pb.Envelope{ProtocolVersion: 1, CryptoSuite: 1, MessageId: modern.Envelope.MessageId, ConversationId: f.chat,
		Sender: modern.Envelope.Sender, Recipient: modern.Envelope.Recipient, MessageType: 1, Ciphertext: modern.Envelope.Ciphertext}}
	if _, err := f.delivery.Send(t.Context(), 1, old); err != nil {
		t.Fatal(err)
	}
	if _, err := f.delivery.SendV2(t.Context(), 1, modern); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("v2 adopted old receipt", err)
	}
	page, err := f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 0 {
		t.Fatal("v1 ciphertext reached v2 decoder", err)
	}
	ack := f.ack(modern)
	digest, _ := boundary.EnvelopeDigest(old.Envelope)
	ack.Messages[0].EnvelopeDigest = digest[:]
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, ack); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("v2 ACK adopted old ledger row", err)
	}
	registryCount(t, f.f.db, "e2ee_message_acknowledgements", 0)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	db := testutil.EmptyPostgres(t)
	if err = goose.UpTo(db, "../../migrations", 26); err != nil {
		t.Fatal(err)
	}
	seed := seedConversations(t, db)
	c := seed.create(t)
	legacy := proto.Clone(old).(*pb.SendMessageRequest)
	legacy.Context.DeviceGenerationId = seed.low.GenerationID
	legacy.Envelope.ConversationId = c.ID
	legacy.Envelope.Sender.GenerationId = seed.low.GenerationID
	legacy.Envelope.Recipient.GenerationId = seed.high.GenerationID
	seedHistoricalE2EESend(t, db, legacy.Envelope)
	if err = goose.UpTo(db, "../../migrations", 27); err == nil {
		t.Fatal("silently converted old message state")
	}
	registryCount(t, db, "e2ee_send_receipts", 1)
}

func TestE2EEEpochPersistenceMixedEpochInboxAndAckBudgetPostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	old := f.message(0)
	old.Envelope.Ciphertext = bytes.Repeat([]byte{0x33}, 44000)
	if _, err := f.delivery.SendV2(t.Context(), 1, old); err != nil {
		t.Fatal(err)
	}
	f.transition(t)
	current := f.message(1)
	current.Envelope.Ciphertext = bytes.Repeat([]byte{0x44}, 44000)
	if _, err := f.delivery.SendV2(t.Context(), 1, current); err != nil {
		t.Fatal(err)
	}
	page, err := f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 1 || page[0].Envelope.MessageId != old.Envelope.MessageId {
		t.Fatal("bounded old/new page", err)
	}
	bad := f.ack(old)
	other := f.ack(current).Messages[0]
	other.EnvelopeDigest[0] ^= 1
	bad.Messages = append(bad.Messages, other)
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, bad); !errors.Is(err, domain.ErrE2EEMessageConflict) {
		t.Fatal("mixed bad ACK accepted", err)
	}
	registryCount(t, f.f.db, "e2ee_message_acknowledgements", 0)
	registryCount(t, f.f.db, "e2ee_message_inbox", 2)
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(old)); err != nil {
		t.Fatal(err)
	}
	page, err = f.delivery.PendingV2(t.Context(), 2, f.context(2))
	if err != nil || len(page) != 1 || page[0].Envelope.MessageId != current.Envelope.MessageId {
		t.Fatal("cursor skipped new epoch", err)
	}
	if err = f.delivery.AcknowledgeV2(t.Context(), 2, f.ack(current)); err != nil {
		t.Fatal(err)
	}
	messageCounters(t, f.f.db, f.gen(2), 0, 0, 2, 0)
	ctx1 := f.context(2)
	ctx1.ProtocolVersion = 1
	if _, err = f.delivery.PendingV2(t.Context(), 2, ctx1); err == nil {
		t.Fatal("legacy context accepted")
	}
}

func TestE2EEEpochPersistenceReplacementKeepsAcceptedEvidencePostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	r := f.message(0)
	receipt, err := f.delivery.SendV2(t.Context(), 1, r)
	if err != nil {
		t.Fatal(err)
	}
	f.f.replace(t, 2, f.gen(2))
	got, err := f.delivery.SendV2(t.Context(), 1, r)
	if err != nil || !proto.Equal(got, receipt) {
		t.Fatal("recipient replacement invalidated past COMMIT", err)
	}
	if _, err = f.delivery.SendV2(t.Context(), 1, f.message(0)); !errors.Is(err, domain.ErrE2EEConversationClosed) {
		t.Fatal("new send into closed chat", err)
	}
	if _, err = f.delivery.PendingV2(t.Context(), 2, f.context(2)); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired recipient still reads", err)
	}
}

func TestE2EEEpochPersistenceAcquireAcceptRaceAndCommitFailurePostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	req := f.acquire(1, 0)
	_, err := f.f.db.ExecContext(t.Context(), `CREATE FUNCTION epoch_acquire_commit_fault()RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.wire_version=2 THEN RAISE EXCEPTION 'synthetic epoch acquisition COMMIT fault';END IF;RETURN NEW;END $$;
        CREATE CONSTRAINT TRIGGER epoch_acquire_commit_fault AFTER INSERT ON e2ee_acquisitions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION epoch_acquire_commit_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.keys.AcquireV2(t.Context(), 1, req); err == nil || got != nil {
		t.Fatal("bundle returned before COMMIT", err)
	}
	var consumed int
	if err = f.f.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_ec_prekeys WHERE used)+(SELECT count(*) FROM e2ee_kyber_prekeys WHERE used)`).Scan(&consumed); err != nil || consumed != 0 {
		t.Fatal("failed reservation consumed keys", consumed, err)
	}
	registryCount(t, f.f.db, "e2ee_acquisitions", 0)
	if _, err = f.f.db.ExecContext(t.Context(), `DROP TRIGGER epoch_acquire_commit_fault ON e2ee_acquisitions;DROP FUNCTION epoch_acquire_commit_fault()`); err != nil {
		t.Fatal(err)
	}
	f.propose(t, f.p, 1)
	accept := recoveryDecision(f.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var result *v2.AcquiredBundle
	var acquireErr error
	wg.Go(func() { <-start; result, acquireErr = f.keys.AcquireV2(t.Context(), 1, req) })
	wg.Go(func() {
		<-start
		if _, err := f.repo.Decide(t.Context(), 2, f.gen(2), accept); err != nil {
			t.Error(err)
		}
	})
	close(start)
	wg.Wait()
	f.head(t, 1)
	if result == nil && !errors.Is(acquireErr, domain.ErrE2EEEpochInactive) {
		t.Fatal(acquireErr)
	}
	if result != nil && acquireErr != nil {
		t.Fatal(acquireErr)
	}
	if _, err = f.keys.AcquireV2(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEEpochInactive) {
		t.Fatal("old acquisition returned after seal", err)
	}
}
