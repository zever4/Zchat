package repository

import (
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
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

type conversationFixture struct {
	db        *sql.DB
	devices   *PgE2EEDeviceRepository
	repo      *PgE2EEConversationRepository
	low, high *domain.E2EEDevice
	req       *pb.CreateConversationRequest
}

func conversationDB(t *testing.T) *conversationFixture {
	t.Helper()
	db := testutil.Postgres(t)
	return seedConversations(t, db)
}
func seedConversations(t *testing.T, db *sql.DB) *conversationFixture {
	t.Helper()
	_, err := db.ExecContext(t.Context(), `INSERT INTO users(id,user_uuid,username,email,password_hash) VALUES
        (1,'11111111-1111-4111-8111-111111111111','a','a@conversation.invalid','verified-test-hash'),
        (2,'22222222-2222-4222-8222-222222222222','b','b@conversation.invalid','verified-test-hash'),
        (3,'33333333-3333-4333-8333-333333333333','c','c@conversation.invalid','verified-test-hash');
        INSERT INTO user_settings(user_id) SELECT id FROM users;
        INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES(1,2,'accepted')`)
	if err != nil {
		t.Fatal(err)
	}
	f := &conversationFixture{db: db, devices: NewPgE2EEDeviceRepository(db), repo: NewPgE2EEConversationRepository(db)}
	for _, id := range []int64{1, 2} {
		d, err := f.devices.Register(t.Context(), id, registrationMaterial())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := NewPgE2EEPreKeyRepository(db).Publish(t.Context(), id, testutil.PublicPreKeyUpload(t, d.Device.GenerationID)); err != nil {
			t.Fatal(err)
		}
		if id == 1 {
			f.low = d.Device
		} else {
			f.high = d.Device
		}
	}
	f.req = &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.low.GenerationID}, CreationRequestId: uuid.NewString(), PeerUserId: 2}
	return f
}
func (f *conversationFixture) create(t *testing.T) *domain.E2EEConversation {
	t.Helper()
	c, err := f.repo.Create(t.Context(), 1, f.req)
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func (f *conversationFixture) get(t *testing.T, id string) *domain.E2EEConversation {
	t.Helper()
	c, err := f.repo.Get(t.Context(), 1, &pb.GetConversationRequest{Context: f.req.Context, ConversationId: id})
	if err != nil {
		t.Fatal(err)
	}
	return c
}
func (f *conversationFixture) replace(t *testing.T, id int64, gen string) *domain.E2EEDevice {
	t.Helper()
	req := registrationMaterial()
	req.ExpectedGenerationID = gen
	d, err := f.devices.Replace(t.Context(), id, req, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	return d.Device
}

func TestE2EEConversationConcurrentCreatePostgres(t *testing.T) {
	f := conversationDB(t)
	var wg sync.WaitGroup
	ids := make(chan string, 12)
	for i := range 12 {
		wg.Go(func() {
			uid := int64(1)
			req := proto.Clone(f.req).(*pb.CreateConversationRequest)
			if i%2 != 0 {
				uid = 2
				req.Context.DeviceGenerationId = f.high.GenerationID
				req.PeerUserId = 1
			}
			c, err := f.repo.Create(t.Context(), uid, req)
			if err != nil {
				t.Error(err)
				return
			}
			if c.OwnDevice.UserID != uid || c.PeerDevice.UserID == uid || c.State != "active" || c.Mode != "signal_pq_1" {
				t.Error("orientation/state mismatch")
			}
			if len(c.OwnDevice.CredentialHash) != 0 || len(c.PeerDevice.CredentialHash) != 0 {
				t.Error("credential verifier in conversation read model")
			}
			ids <- c.ID
		})
	}
	wg.Wait()
	close(ids)
	var id string
	for got := range ids {
		if id != "" && got != id {
			t.Fatal("opposite creators got different conversations")
		}
		id = got
	}
	if id == "" {
		t.Fatal("no conversation created")
	}
	registryCount(t, f.db, "e2ee_conversations", 1)
	registryCount(t, f.db, "e2ee_conversation_requests", 2)
	// A different create UUID still opens the same active encrypted chat.
	other := proto.Clone(f.req).(*pb.CreateConversationRequest)
	other.CreationRequestId = uuid.NewString()
	c, err := f.repo.Create(t.Context(), 1, other)
	if err != nil || c.ID != id {
		t.Fatal("new request duplicated active pair", err)
	}
	changed := proto.Clone(f.req).(*pb.CreateConversationRequest)
	changed.PeerUserId = 3
	if _, err := f.repo.Create(t.Context(), 1, changed); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("changed retry accepted", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=FALSE; UPDATE friend_requests SET status='rejected'`); err != nil {
		t.Fatal(err)
	}
	other.CreationRequestId = uuid.NewString()
	if c, err := f.repo.Create(t.Context(), 1, other); err != nil || c.ID != id {
		t.Fatal("existing chat blocked by new-chat privacy", err)
	}
	if f.get(t, id).State != "active" {
		t.Fatal("privacy change closed conversation")
	}
	for _, table := range []string{"chats", "chat_members", "messages", "attachments"} {
		registryCount(t, f.db, table, 0)
	}
}

func TestE2EEConversationCreationPolicyPostgres(t *testing.T) {
	f := conversationDB(t)
	for _, id := range []int{1, 2} {
		if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=$1`, id); err != nil {
			t.Fatal(err)
		}
		if _, err := f.repo.Create(t.Context(), 1, f.req); !errors.Is(err, domain.ErrDirectMessagesDisabled) {
			t.Fatal("privacy bypassed", err)
		}
		if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=TRUE WHERE user_id=$1`, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.db.ExecContext(t.Context(), `DELETE FROM user_settings WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.Create(t.Context(), 1, f.req); !errors.Is(err, domain.ErrDirectMessagesDisabled) {
		t.Fatal("missing privacy row treated as allow", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO user_settings(user_id) VALUES(2); UPDATE friend_requests SET status='pending'`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.Create(t.Context(), 1, f.req); !errors.Is(err, domain.ErrNotFriends) {
		t.Fatal("pending friend request allowed E2EE", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE friend_requests SET status='accepted'; UPDATE e2ee_devices SET public_material_ready=FALSE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.Create(t.Context(), 1, f.req); !errors.Is(err, domain.ErrE2EEKeysNotReady) {
		t.Fatal("unpublished peer accepted", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET public_material_ready=TRUE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	self := proto.Clone(f.req).(*pb.CreateConversationRequest)
	self.PeerUserId = 1
	if _, err := f.repo.Create(t.Context(), 1, self); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal("self chat accepted", err)
	}
	missing := proto.Clone(f.req).(*pb.CreateConversationRequest)
	missing.PeerUserId = 999
	if _, err := f.repo.Create(t.Context(), 1, missing); !errors.Is(err, domain.ErrE2EEPeerUnavailable) {
		t.Fatal("missing peer accepted", err)
	}
	stale := proto.Clone(f.req).(*pb.CreateConversationRequest)
	stale.Context.DeviceGenerationId = uuid.NewString()
	if _, err := f.repo.Create(t.Context(), 1, stale); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("wrong own generation accepted", err)
	}
	registryCount(t, f.db, "e2ee_conversations", 0)
	registryCount(t, f.db, "e2ee_conversation_requests", 0)
	f.create(t)
}

func TestE2EEConversationReplacementClosurePostgres(t *testing.T) {
	f := conversationDB(t)
	old := f.create(t)
	newPeer := f.replace(t, 2, f.high.GenerationID)
	closed := f.get(t, old.ID)
	if closed.State != "closed" || closed.CloseReason != "device_replaced" || closed.ClosedGenerationID != f.high.GenerationID || closed.ClosedAt == nil || closed.PeerDevice.GenerationID != f.high.GenerationID {
		t.Fatal("old chat did not retain explicit bound closure")
	}
	replay, err := f.repo.Create(t.Context(), 1, f.req)
	if err != nil || replay.ID != old.ID || replay.State != "closed" {
		t.Fatal("lost response retry silently created a new chat", err)
	}
	newReq := proto.Clone(f.req).(*pb.CreateConversationRequest)
	newReq.CreationRequestId = uuid.NewString()
	if _, err := f.repo.Create(t.Context(), 1, newReq); !errors.Is(err, domain.ErrE2EEKeysNotReady) {
		t.Fatal("new chat used unpublished replacement", err)
	}
	if _, err := NewPgE2EEPreKeyRepository(f.db).Publish(t.Context(), 2, testutil.PublicPreKeyUpload(t, newPeer.GenerationID)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.Create(t.Context(), 1, newReq); !errors.Is(err, domain.ErrDirectMessagesDisabled) {
		t.Fatal("old encrypted/ordinary relationship bypassed new chat privacy", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=TRUE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	newChat, err := f.repo.Create(t.Context(), 1, newReq)
	if err != nil || newChat.ID == old.ID || newChat.State != "active" || newChat.PeerDevice.GenerationID != newPeer.GenerationID {
		t.Fatal("new generation cannot start separate chat", err)
	}
	if _, err := f.repo.Get(t.Context(), 2, &pb.GetConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: newPeer.GenerationID}, ConversationId: old.ID}); !errors.Is(err, domain.ErrE2EEConversationNotFound) {
		t.Fatal("replacement fetched old generation archive", err)
	}
	f.replace(t, 1, f.low.GenerationID)
	var state, reasonGen string
	var closedAt time.Time
	if err := f.db.QueryRowContext(t.Context(), `SELECT state,closed_generation_id::text,closed_at FROM e2ee_conversations WHERE conversation_id=$1`, old.ID).Scan(&state, &reasonGen, &closedAt); err != nil || state != "closed" || reasonGen != closed.ClosedGenerationID || !closedAt.Equal(*closed.ClosedAt) {
		t.Fatal("later replacement rewrote earlier archive", err)
	}
	if err := f.db.QueryRowContext(t.Context(), `SELECT state,closed_generation_id::text FROM e2ee_conversations WHERE conversation_id=$1`, newChat.ID).Scan(&state, &reasonGen); err != nil || state != "closed" || reasonGen != f.low.GenerationID {
		t.Fatal("own replacement did not close current chat", err)
	}
	registryCount(t, f.db, "e2ee_conversations", 2)
}

func TestE2EEConversationMembershipPostgres(t *testing.T) {
	f := conversationDB(t)
	c := f.create(t)
	for _, tc := range []struct {
		uid     int64
		gen, id string
	}{{3, f.low.GenerationID, c.ID}, {1, f.high.GenerationID, c.ID}, {1, f.low.GenerationID, uuid.NewString()}} {
		if _, err := f.repo.Get(t.Context(), tc.uid, &pb.GetConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: tc.gen}, ConversationId: tc.id}); !errors.Is(err, domain.ErrE2EEConversationNotFound) {
			t.Fatal("membership or generation lookup leaked conversation", err)
		}
	}
	peer, err := f.repo.Get(t.Context(), 2, &pb.GetConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.high.GenerationID}, ConversationId: c.ID})
	if err != nil || peer.OwnDevice.UserID != 2 || peer.PeerDevice.UserID != 1 {
		t.Fatal("peer cannot read own orientation", err)
	}
}

func TestE2EEConversationAtomicFailurePostgres(t *testing.T) {
	f := conversationDB(t)
	_, err := f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_conversation_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit failure'; END $$;
        CREATE CONSTRAINT TRIGGER reject_conversation_commit AFTER INSERT ON e2ee_conversation_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_conversation_commit()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.Create(t.Context(), 1, f.req); err == nil {
		t.Fatal("failed create COMMIT acknowledged")
	}
	registryCount(t, f.db, "e2ee_conversations", 0)
	registryCount(t, f.db, "e2ee_conversation_requests", 0)
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_conversation_commit ON e2ee_conversation_requests; DROP FUNCTION reject_conversation_commit()`); err != nil {
		t.Fatal(err)
	}
	c := f.create(t)
	_, err = f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_replacement_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic replacement commit failure'; END $$;
        CREATE CONSTRAINT TRIGGER reject_replacement_commit AFTER INSERT ON e2ee_device_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN(NEW.operation='replace') EXECUTE FUNCTION reject_replacement_commit()`)
	if err != nil {
		t.Fatal(err)
	}
	req := registrationMaterial()
	req.ExpectedGenerationID = f.high.GenerationID
	if _, err := f.devices.Replace(t.Context(), 2, req, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}); err == nil {
		t.Fatal("failed replacement COMMIT acknowledged")
	}
	if f.get(t, c.ID).State != "active" {
		t.Fatal("replacement rollback left chat closed")
	}
	_, own, err := f.devices.Own(t.Context(), 2)
	if err != nil || own.GenerationID != f.high.GenerationID || !own.PublicMaterialReady {
		t.Fatal("failed replacement retired old keys", err)
	}
	registryCount(t, f.db, "e2ee_devices", 2)
	registryCount(t, f.db, "e2ee_device_requests", 2)
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_replacement_commit ON e2ee_device_requests; DROP FUNCTION reject_replacement_commit()`); err != nil {
		t.Fatal(err)
	}
	// Failure in the closure itself must also roll back device retirement.
	_, err = f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_closure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic closure failure'; END $$;
        CREATE TRIGGER reject_closure BEFORE UPDATE ON e2ee_conversations FOR EACH ROW EXECUTE FUNCTION reject_closure()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.devices.Replace(t.Context(), 2, req, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}); err == nil {
		t.Fatal("closure failure ignored")
	}
	if f.get(t, c.ID).State != "active" {
		t.Fatal("closure failure changed conversation")
	}
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_closure ON e2ee_conversations; DROP FUNCTION reject_closure()`); err != nil {
		t.Fatal(err)
	}
	f.replace(t, 2, f.high.GenerationID)
	if f.get(t, c.ID).State != "closed" {
		t.Fatal("replacement cannot recover after rollback")
	}
}

func TestE2EEConversationConcurrentReplacementPostgres(t *testing.T) {
	t.Run("create versus retire", func(t *testing.T) {
		f := conversationDB(t)
		var wg sync.WaitGroup
		var createErr, replaceErr error
		wg.Go(func() { _, createErr = f.repo.Create(t.Context(), 1, f.req) })
		wg.Go(func() {
			req := registrationMaterial()
			req.ExpectedGenerationID = f.high.GenerationID
			_, replaceErr = f.devices.Replace(t.Context(), 2, req, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1})
		})
		wg.Wait()
		if replaceErr != nil {
			t.Fatal(replaceErr)
		}
		if createErr != nil && !errors.Is(createErr, domain.ErrE2EEKeysNotReady) {
			t.Fatal(createErr)
		}
		var active int
		if err := f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_conversations WHERE state='active' AND (low_generation_id=$1 OR high_generation_id=$1)`, f.high.GenerationID).Scan(&active); err != nil || active != 0 {
			t.Fatal("active chat bound to retired generation", err)
		}
	})
	t.Run("both participants retire", func(t *testing.T) {
		f := conversationDB(t)
		c := f.create(t)
		var wg sync.WaitGroup
		for _, d := range []*domain.E2EEDevice{f.low, f.high} {
			wg.Go(func() {
				req := registrationMaterial()
				req.ExpectedGenerationID = d.GenerationID
				if _, err := f.devices.Replace(t.Context(), d.UserID, req, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}); err != nil {
					t.Error(err)
				}
			})
		}
		wg.Wait()
		var state, gen string
		if err := f.db.QueryRowContext(t.Context(), `SELECT state,closed_generation_id::text FROM e2ee_conversations WHERE conversation_id=$1`, c.ID).Scan(&state, &gen); err != nil || state != "closed" || (gen != f.low.GenerationID && gen != f.high.GenerationID) {
			t.Fatal("simultaneous replacements failed closure", err)
		}
	})
}

func TestE2EEConversationPrivacyWaitAndCancelPostgres(t *testing.T) {
	f := conversationDB(t)
	tx, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := f.repo.Create(ctx, 1, f.req); err == nil {
		t.Fatal("privacy row-lock wait ignored cancellation")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.repo.Create(t.Context(), 1, f.req); !errors.Is(err, domain.ErrDirectMessagesDisabled) {
		t.Fatal("committed privacy change ignored", err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=TRUE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	f.create(t)
}

func TestE2EEConversationSQLInvariantsPostgres(t *testing.T) {
	f := conversationDB(t)
	c := f.create(t)
	for _, query := range []string{
		`UPDATE e2ee_conversations SET mode='ordinary'`,
		`UPDATE e2ee_conversations SET low_generation_id=high_generation_id`,
		`UPDATE e2ee_conversations SET conversation_id=gen_random_uuid()`,
		`UPDATE e2ee_conversations SET low_user_id=high_user_id`,
		`UPDATE e2ee_conversations SET state='closed'`,
		`UPDATE e2ee_conversations SET state='closed',close_reason='device_replaced',closed_generation_id=low_generation_id,closed_at=clock_timestamp()`,
		`UPDATE e2ee_conversation_requests SET owner_user_id=3`,
		`UPDATE e2ee_conversation_requests SET owner_generation_id=high_generation_id`,
	} {
		if _, err := f.db.ExecContext(t.Context(), query); err == nil {
			t.Fatal("SQL invariant missing", query)
		}
	}
	_, err := f.db.ExecContext(t.Context(), `INSERT INTO e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id) VALUES($1,1,2,$2,$3)`, uuid.NewString(), f.low.GenerationID, f.high.GenerationID)
	if err == nil {
		t.Fatal("duplicate active pair accepted")
	}
	// Direct retirement also closes conversations: this does not depend on a
	// service callback that might fail between device and conversation writes.
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET retired_at=clock_timestamp(),public_material_ready=FALSE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if f.get(t, c.ID).State != "closed" {
		t.Fatal("SQL retirement bypassed closure")
	}
	for _, q := range []string{`UPDATE e2ee_conversations SET state='active',close_reason=NULL,closed_generation_id=NULL,closed_at=NULL`, `UPDATE e2ee_conversations SET closed_at=clock_timestamp()`} {
		if _, err := f.db.ExecContext(t.Context(), q); err == nil {
			t.Fatal("closed archive rewritten")
		}
	}
	_, err = f.db.ExecContext(t.Context(), `INSERT INTO e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id) VALUES($1,1,2,$2,$3)`, uuid.NewString(), f.low.GenerationID, f.high.GenerationID)
	if err == nil {
		t.Fatal("new chat bound to retired generation")
	}
}

func TestE2EEConversationRequestCapacityPostgres(t *testing.T) {
	f := conversationDB(t)
	c := f.create(t)
	_, err := f.db.ExecContext(t.Context(), `INSERT INTO e2ee_conversation_requests(owner_user_id,owner_generation_id,request_id,peer_user_id,conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id)
        SELECT low_user_id,low_generation_id,gen_random_uuid(),high_user_id,conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id
        FROM e2ee_conversations CROSS JOIN generate_series(1,$1::integer-1) WHERE conversation_id=$2`, domain.MaxE2EEConversationRequestsPerGeneration, c.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := f.repo.Create(t.Context(), 1, f.req); err != nil || replay.ID != c.ID {
		t.Fatal("receipt cap blocked exact recovery", err)
	}
	newReq := proto.Clone(f.req).(*pb.CreateConversationRequest)
	newReq.CreationRequestId = uuid.NewString()
	if _, err := f.repo.Create(t.Context(), 1, newReq); !errors.Is(err, domain.ErrE2EEConversationCapacity) {
		t.Fatal("request cap bypassed", err)
	}
	registryCount(t, f.db, "e2ee_conversations", 1)
	registryCount(t, f.db, "e2ee_conversation_requests", domain.MaxE2EEConversationRequestsPerGeneration)
}

func TestE2EEConversationMigrationPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 20); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 21); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty conversation rollback", err)
	}
	if err := goose.UpTo(db, "../../migrations", 21); err != nil {
		t.Fatal(err)
	}
	f := seedConversations(t, db)
	c := f.create(t)
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("rollback discarded conversations")
	}
	if f.get(t, c.ID).State != "active" {
		t.Fatal("failed rollback changed state")
	}
}
