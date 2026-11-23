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

func acquisitionDB(t *testing.T) (*conversationFixture, *PgE2EEPreKeyRepository, *pb.AcquireBundleRequest) {
	t.Helper()
	f := conversationDB(t)
	c := f.create(t)
	req := &pb.AcquireBundleRequest{Context: f.req.Context, ConversationId: c.ID, AcquisitionId: uuid.NewString(), ExpectedPeer: &pb.DeviceRef{UserId: f.high.UserUUID, DeviceId: 1, GenerationId: f.high.GenerationID}}
	return f, NewPgE2EEPreKeyRepository(f.db), req
}
func nextAcquisition(req *pb.AcquireBundleRequest) *pb.AcquireBundleRequest {
	r := proto.Clone(req).(*pb.AcquireBundleRequest)
	r.AcquisitionId = uuid.NewString()
	return r
}
func acquisitionUsed(t *testing.T, db *sql.DB, gen string, ec, pq int) {
	t.Helper()
	var ecUsed, pqUsed, lrUsed int
	err := db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_ec_prekeys WHERE generation_id=$1 AND used),
        (SELECT count(*) FROM e2ee_kyber_prekeys WHERE generation_id=$1 AND used AND NOT is_last_resort),
        (SELECT count(*) FROM e2ee_kyber_prekeys WHERE generation_id=$1 AND used AND is_last_resort)`, gen).Scan(&ecUsed, &pqUsed, &lrUsed)
	if err != nil || ecUsed != ec || pqUsed != pq || lrUsed != 0 {
		t.Fatalf("consumption EC=%d PQ=%d LR=%d, want %d/%d/0: %v", ecUsed, pqUsed, lrUsed, ec, pq, err)
	}
}

func TestE2EEAcquisitionConcurrentReplayPostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	var wg sync.WaitGroup
	results := make(chan *pb.AcquiredBundle, 12)
	for range 12 {
		wg.Go(func() {
			bundle, err := r.Acquire(t.Context(), 1, req)
			if err != nil {
				t.Error(err)
				return
			}
			results <- bundle
		})
	}
	wg.Wait()
	close(results)
	var first *pb.AcquiredBundle
	for result := range results {
		if first == nil {
			first = result
		} else if !proto.Equal(first, result) {
			t.Fatal("same acquisition returned different bundle")
		}
	}
	if first == nil || first.OneTimePreKey == nil || first.OneTimePreKey.KeyId != 2147483648 || first.SignedPreKey.KeyId != 4294967293 || first.KyberPreKey.KeyId != 4294967294 || first.KyberPreKey.IsLastResort {
		t.Fatal("full-u32/required material mismatch")
	}
	remaining := time.Until(time.UnixMilli(first.NotAfterUnixMs))
	if remaining < 23*time.Hour || remaining > domain.E2EEAcquisitionLease+time.Minute {
		t.Fatal("lease not bounded near 24 hours")
	}
	registryCount(t, f.db, "e2ee_acquisitions", 1)
	acquisitionUsed(t, f.db, f.high.GenerationID, 1, 1)
	// Reverse direction has a different owner generation/peer pool. UUID reuse
	// across owners is allowed and cannot reveal the other owner's snapshot.
	reverse := proto.Clone(req).(*pb.AcquireBundleRequest)
	reverse.Context = &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.high.GenerationID}
	reverse.ExpectedPeer = &pb.DeviceRef{UserId: f.low.UserUUID, DeviceId: 1, GenerationId: f.low.GenerationID}
	other, err := r.Acquire(t.Context(), 2, reverse)
	if err != nil || other.Device.Reference.GenerationId != f.low.GenerationID {
		t.Fatal("account scope collision", err)
	}
	registryCount(t, f.db, "e2ee_acquisitions", 2)
	acquisitionUsed(t, f.db, f.low.GenerationID, 1, 1)
}

func TestE2EEAcquisitionConcurrentDistinctPostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	upload := testutil.PublicPreKeyUpload(t, f.high.GenerationID)
	upload.ExpectedRevision = 1
	upload.SignedPreKey = nil
	ec := upload.OneTimePreKeys[0]
	pq := upload.KyberPreKeys[0]
	upload.OneTimePreKeys = nil
	upload.KyberPreKeys = nil
	// Reuse reviewed PUBLIC fixture bytes under distinct IDs to test SQL
	// reservations. This is not twelve independent libsignal private key pairs.
	for i := range 11 {
		e := proto.Clone(ec).(*pb.OneTimePreKey)
		e.KeyId = uint32(i)
		upload.OneTimePreKeys = append(upload.OneTimePreKeys, e)
		p := proto.Clone(pq).(*pb.KyberPreKey)
		p.KeyId = uint32(i)
		upload.KyberPreKeys = append(upload.KyberPreKeys, p)
	}
	if _, err := r.Publish(t.Context(), 2, upload); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	bundles := make(chan *pb.AcquiredBundle, 12)
	for range 12 {
		wg.Go(func() {
			b, err := r.Acquire(t.Context(), 1, nextAcquisition(req))
			if err != nil {
				t.Error(err)
				return
			}
			bundles <- b
		})
	}
	wg.Wait()
	close(bundles)
	ecs, pqs := map[uint32]bool{}, map[uint32]bool{}
	for b := range bundles {
		if b.OneTimePreKey == nil || b.KyberPreKey.IsLastResort || ecs[b.OneTimePreKey.KeyId] || pqs[b.KyberPreKey.KeyId] {
			t.Fatal("one-time ID issued twice or skipped")
		}
		ecs[b.OneTimePreKey.KeyId] = true
		pqs[b.KyberPreKey.KeyId] = true
	}
	if len(ecs) != 12 || len(pqs) != 12 {
		t.Fatal("incomplete parallel allocation")
	}
	registryCount(t, f.db, "e2ee_acquisitions", 12)
	acquisitionUsed(t, f.db, f.high.GenerationID, 12, 12)
}

func TestE2EEAcquisitionFallbackRotationPostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	first, err := r.Acquire(t.Context(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Acquire(t.Context(), 1, nextAcquisition(req))
	if err != nil || second.OneTimePreKey != nil || !second.KyberPreKey.IsLastResort || second.KyberPreKey.KeyId != 4294967295 {
		t.Fatal("EC exhaustion/PQ last-resort fallback failed", err)
	}
	upload := testutil.PublicPreKeyUpload(t, f.high.GenerationID)
	upload.ExpectedRevision = 1
	upload.OneTimePreKeys = nil
	upload.SignedPreKey.KeyId = 0
	upload.KyberPreKeys = upload.KyberPreKeys[1:]
	upload.KyberPreKeys[0].KeyId = 0
	if _, err := r.Publish(t.Context(), 2, upload); err != nil {
		t.Fatal(err)
	}
	for _, original := range []*pb.AcquiredBundle{first, second} {
		repeat := proto.Clone(req).(*pb.AcquireBundleRequest)
		repeat.AcquisitionId = original.AcquisitionId
		got, err := r.Acquire(t.Context(), 1, repeat)
		if err != nil || !proto.Equal(got, original) {
			t.Fatal("rotation changed an existing snapshot", err)
		}
	}
	third, err := r.Acquire(t.Context(), 1, nextAcquisition(req))
	if err != nil || third.SignedPreKey.KeyId != 0 || third.KyberPreKey.KeyId != 0 || !third.KyberPreKey.IsLastResort || third.Device.PublicMaterialRevision != 2 {
		t.Fatal("fresh acquisition did not use active rotation", err)
	}
	acquisitionUsed(t, f.db, f.high.GenerationID, 1, 1)
	// Re-uploading a consumed pool with a fresh publication batch cannot make
	// the one-time keys available again.
	refill := testutil.PublicPreKeyUpload(t, f.high.GenerationID)
	refill.ExpectedRevision = 2
	refill.SignedPreKey = nil
	refill.KyberPreKeys = refill.KyberPreKeys[:1]
	if _, err := r.Publish(t.Context(), 2, refill); err != nil {
		t.Fatal(err)
	}
	fourth, err := r.Acquire(t.Context(), 1, nextAcquisition(req))
	if err != nil || fourth.OneTimePreKey != nil || !fourth.KyberPreKey.IsLastResort {
		t.Fatal("publication reset consumed keys", err)
	}
}

func TestE2EEAcquisitionAuthorizationPostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	if _, err := r.Acquire(t.Context(), 3, req); !errors.Is(err, domain.ErrE2EEConversationNotFound) {
		t.Fatal("outsider got bundle", err)
	}
	wrong := proto.Clone(req).(*pb.AcquireBundleRequest)
	wrong.ExpectedPeer.GenerationId = uuid.NewString()
	if _, err := r.Acquire(t.Context(), 1, wrong); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("unexpected generation accepted", err)
	}
	wrong = proto.Clone(req).(*pb.AcquireBundleRequest)
	wrong.ExpectedPeer.UserId = f.low.UserUUID
	if _, err := r.Acquire(t.Context(), 1, wrong); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("unexpected peer accepted", err)
	}
	registryCount(t, f.db, "e2ee_acquisitions", 0)
	acquisitionUsed(t, f.db, f.high.GenerationID, 0, 0)
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=FALSE; UPDATE friend_requests SET status='rejected'`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, req); err != nil {
		t.Fatal("existing conversation blocked by new-chat policy", err)
	}
	wrong = proto.Clone(req).(*pb.AcquireBundleRequest)
	wrong.ExpectedPeer.DeviceId = 2
	if _, err := r.Acquire(t.Context(), 1, wrong); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("changed retry accepted", err)
	}
	f.replace(t, 2, f.high.GenerationID)
	if _, err := r.Acquire(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEConversationClosed) {
		t.Fatal("closed-chat retry returned keys", err)
	}
	if _, err := r.Acquire(t.Context(), 1, nextAcquisition(req)); !errors.Is(err, domain.ErrE2EEConversationClosed) {
		t.Fatal("new reservation in closed chat", err)
	}
	registryCount(t, f.db, "e2ee_acquisitions", 1)
}

func TestE2EEAcquisitionConversationIDConflictPostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	if _, err := r.Acquire(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	d, err := f.devices.Register(t.Context(), 3, registrationMaterial())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 3, testutil.PublicPreKeyUpload(t, d.Device.GenerationID)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES(1,3,'accepted')`); err != nil {
		t.Fatal(err)
	}
	create := &pb.CreateConversationRequest{Context: f.req.Context, CreationRequestId: uuid.NewString(), PeerUserId: 3}
	c, err := f.repo.Create(t.Context(), 1, create)
	if err != nil {
		t.Fatal(err)
	}
	changed := proto.Clone(req).(*pb.AcquireBundleRequest)
	changed.ConversationId = c.ID
	changed.ExpectedPeer = &pb.DeviceRef{UserId: d.Device.UserUUID, DeviceId: 1, GenerationId: d.Device.GenerationID}
	if _, err := r.Acquire(t.Context(), 1, changed); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("acquisition UUID rebound to another conversation", err)
	}
	registryCount(t, f.db, "e2ee_acquisitions", 1)
	acquisitionUsed(t, f.db, d.Device.GenerationID, 0, 0)
}

func TestE2EEAcquisitionAtomicFailurePostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	_, err := f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_acquisition_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit failure'; END $$;
        CREATE CONSTRAINT TRIGGER reject_acquisition_commit AFTER INSERT ON e2ee_acquisitions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_acquisition_commit()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, req); err == nil {
		t.Fatal("failed COMMIT returned a bundle")
	}
	registryCount(t, f.db, "e2ee_acquisitions", 0)
	acquisitionUsed(t, f.db, f.high.GenerationID, 0, 0)
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_acquisition_commit ON e2ee_acquisitions; DROP FUNCTION reject_acquisition_commit()`); err != nil {
		t.Fatal(err)
	}
	_, err = f.db.ExecContext(t.Context(), `CREATE FUNCTION reject_pq_reservation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic PQ failure'; END $$;
        CREATE TRIGGER reject_pq_reservation BEFORE UPDATE ON e2ee_kyber_prekeys FOR EACH ROW EXECUTE FUNCTION reject_pq_reservation()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, req); err == nil {
		t.Fatal("PQ error hidden as empty pool/fallback")
	}
	acquisitionUsed(t, f.db, f.high.GenerationID, 0, 0)
	registryCount(t, f.db, "e2ee_acquisitions", 0)
	if _, err := f.db.ExecContext(t.Context(), `DROP TRIGGER reject_pq_reservation ON e2ee_kyber_prekeys; DROP FUNCTION reject_pq_reservation()`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET public_material_ready=FALSE,active_last_resort_id=NULL WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEKeysNotReady) {
		t.Fatal("missing mandatory key accepted", err)
	}
	acquisitionUsed(t, f.db, f.high.GenerationID, 0, 0)
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET active_last_resort_id=4294967295,public_material_ready=TRUE WHERE user_id=2`); err != nil {
		t.Fatal(err)
	}
	lock, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(t.Context(), `SELECT id FROM users WHERE id=2 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := r.Acquire(ctx, 1, req); err == nil {
		t.Fatal("lock wait ignored cancellation")
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, req); err != nil {
		t.Fatal("pool unusable after rollback", err)
	}
	acquisitionUsed(t, f.db, f.high.GenerationID, 1, 1)
}

func TestE2EEAcquisitionExpiryAndCorruptionPostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	if _, err := r.Acquire(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_acquisitions SET created_at=statement_timestamp()-INTERVAL '25 hours',not_after=statement_timestamp()-INTERVAL '1 hour'`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEAcquisitionExpired) {
		t.Fatal("expired UUID renewed/reallocated", err)
	}
	registryCount(t, f.db, "e2ee_acquisitions", 1)
	acquisitionUsed(t, f.db, f.high.GenerationID, 1, 1)
	fresh := nextAcquisition(req)
	saved, err := r.Acquire(t.Context(), 1, fresh)
	if err != nil {
		t.Fatal(err)
	}
	if saved.OneTimePreKey != nil || !saved.KyberPreKey.IsLastResort {
		t.Fatal("expiry returned consumed keys to pool")
	}
	for i := range 4 {
		changed := proto.Clone(saved).(*pb.AcquiredBundle)
		switch i {
		case 0:
			changed.NotAfterUnixMs++
		case 1:
			changed.Device.IdentityKey[1] ^= 1
		case 2:
			changed.SignedPreKey.KeyId--
		case 3:
			changed.KyberPreKey.IsLastResort = false
		}
		wire, err := proto.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_acquisitions SET public_snapshot=$1 WHERE acquisition_id=$2`, wire, fresh.AcquisitionId); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Acquire(t.Context(), 1, fresh); !errors.Is(err, domain.ErrE2EEAcquisitionState) {
			t.Fatal("inconsistent snapshot served", err)
		}
	}
	wire, err := proto.Marshal(saved)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_acquisitions SET public_snapshot=$1 WHERE acquisition_id=$2`, wire, fresh.AcquisitionId); err != nil {
		t.Fatal(err)
	}
	if replay, err := r.Acquire(t.Context(), 1, fresh); err != nil || !proto.Equal(replay, saved) {
		t.Fatal("snapshot restoration failed", err)
	}
	registryCount(t, f.db, "e2ee_acquisitions", 2)
	acquisitionUsed(t, f.db, f.high.GenerationID, 1, 1)
}

func TestE2EEAcquisitionConcurrentLifecyclePostgres(t *testing.T) {
	t.Run("rotation", func(t *testing.T) {
		f, r, req := acquisitionDB(t)
		if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_ec_prekeys SET used=TRUE; UPDATE e2ee_kyber_prekeys SET used=TRUE WHERE NOT is_last_resort`); err != nil {
			t.Fatal(err)
		}
		u := testutil.PublicPreKeyUpload(t, f.high.GenerationID)
		u.ExpectedRevision = 1
		u.OneTimePreKeys = nil
		u.SignedPreKey.KeyId = 0
		u.KyberPreKeys = u.KyberPreKeys[1:]
		u.KyberPreKeys[0].KeyId = 0
		var wg sync.WaitGroup
		var result *pb.AcquiredBundle
		var acquireErr, publishErr error
		wg.Go(func() { result, acquireErr = r.Acquire(t.Context(), 1, req) })
		wg.Go(func() { _, publishErr = r.Publish(t.Context(), 2, u) })
		wg.Wait()
		if acquireErr != nil || publishErr != nil {
			t.Fatal(acquireErr, publishErr)
		}
		if !((result.SignedPreKey.KeyId == 0 && result.KyberPreKey.KeyId == 0 && result.Device.PublicMaterialRevision == 2) || (result.SignedPreKey.KeyId == 4294967293 && result.KyberPreKey.KeyId == 4294967295 && result.Device.PublicMaterialRevision == 1)) {
			t.Fatal("mixed publication epochs")
		}
		if replay, err := r.Acquire(t.Context(), 1, req); err != nil || !proto.Equal(replay, result) {
			t.Fatal("rotation retry changed snapshot", err)
		}
	})
	t.Run("replacement", func(t *testing.T) {
		f, r, req := acquisitionDB(t)
		var wg sync.WaitGroup
		var acquireErr, replaceErr error
		wg.Go(func() { _, acquireErr = r.Acquire(t.Context(), 1, req) })
		wg.Go(func() {
			replacement := registrationMaterial()
			replacement.ExpectedGenerationID = f.high.GenerationID
			_, replaceErr = f.devices.Replace(t.Context(), 2, replacement, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1})
		})
		wg.Wait()
		if replaceErr != nil || (acquireErr != nil && !errors.Is(acquireErr, domain.ErrE2EEConversationClosed)) {
			t.Fatal(acquireErr, replaceErr)
		}
		if _, err := r.Acquire(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEConversationClosed) {
			t.Fatal("post-replacement replay returned old keys", err)
		}
	})
}

func TestE2EEAcquisitionCapacityPostgres(t *testing.T) {
	f, r, req := acquisitionDB(t)
	first, err := r.Acquire(t.Context(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < domain.MaxE2EELiveAcquisitionsPerDirection; i++ {
		if _, err := r.Acquire(t.Context(), 1, nextAcquisition(req)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := r.Acquire(t.Context(), 1, nextAcquisition(req)); !errors.Is(err, domain.ErrE2EEAcquisitionCapacity) {
		t.Fatal("live quota bypassed", err)
	}
	if b, err := r.Acquire(t.Context(), 1, req); err != nil || !proto.Equal(b, first) {
		t.Fatal("quota prevented exact replay", err)
	}
	acquisitionUsed(t, f.db, f.high.GenerationID, 1, 1)
	// Populate the retained budget with expired PUBLIC reservations so this
	// tests retained accounting separately from the live-per-direction limit.
	_, err = f.db.ExecContext(t.Context(), `INSERT INTO e2ee_acquisitions(owner_user_id,owner_generation_id,acquisition_id,conversation_id,peer_user_id,peer_generation_id,low_user_id,high_user_id,low_generation_id,high_generation_id,signed_prekey_id,ec_prekey_id,kyber_prekey_id,kyber_is_last_resort,public_snapshot,created_at,not_after)
        SELECT owner_user_id,owner_generation_id,gen_random_uuid(),conversation_id,peer_user_id,peer_generation_id,low_user_id,high_user_id,low_generation_id,high_generation_id,signed_prekey_id,NULL,kyber_prekey_id,TRUE,public_snapshot,
        TIMESTAMPTZ '2020-01-01 00:00:00+00',TIMESTAMPTZ '2020-01-02 00:00:00+00'
        FROM e2ee_acquisitions CROSS JOIN generate_series(1,$1::integer) n WHERE acquisition_id=$2`, domain.MaxE2EEAcquisitionsPerGeneration-domain.MaxE2EELiveAcquisitionsPerDirection, firstFallbackID(t, f.db))
	if err != nil {
		t.Fatal(err)
	}
	// Expire the other live reservations, preserving the original for replay.
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_acquisitions SET created_at=TIMESTAMPTZ '2020-01-01 00:00:00+00',not_after=TIMESTAMPTZ '2020-01-02 00:00:00+00' WHERE acquisition_id<>$1`, req.AcquisitionId); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, nextAcquisition(req)); !errors.Is(err, domain.ErrE2EEAcquisitionCapacity) {
		t.Fatal("retained quota bypassed", err)
	}
	if _, err := r.Acquire(t.Context(), 1, req); err != nil {
		t.Fatal("retained cap blocked recovery", err)
	}
	registryCount(t, f.db, "e2ee_acquisitions", domain.MaxE2EEAcquisitionsPerGeneration)
}
func firstFallbackID(t *testing.T, db *sql.DB) string {
	t.Helper()
	var id string
	if err := db.QueryRowContext(t.Context(), `SELECT acquisition_id::text FROM e2ee_acquisitions WHERE kyber_is_last_resort LIMIT 1`).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestE2EEAcquisitionSQLAndMigrationPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 21); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 22); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty acquisition rollback", err)
	}
	if err := goose.UpTo(db, "../../migrations", 22); err != nil {
		t.Fatal(err)
	}
	f := seedConversations(t, db)
	c := f.create(t)
	r := NewPgE2EEPreKeyRepository(db)
	req := &pb.AcquireBundleRequest{Context: f.req.Context, ConversationId: c.ID, AcquisitionId: uuid.NewString(), ExpectedPeer: &pb.DeviceRef{UserId: f.high.UserUUID, DeviceId: 1, GenerationId: f.high.GenerationID}}
	if _, err := r.Acquire(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("rollback discarded reservations")
	}
	for _, q := range []string{
		`UPDATE e2ee_acquisitions SET owner_user_id=3`,
		`UPDATE e2ee_acquisitions SET peer_generation_id=low_generation_id`,
		`UPDATE e2ee_acquisitions SET kyber_is_last_resort=TRUE`,
		`UPDATE e2ee_acquisitions SET not_after=created_at`,
		`UPDATE e2ee_acquisitions SET not_after=created_at+INTERVAL '25 hours'`,
		`UPDATE e2ee_acquisitions SET public_snapshot=decode('','hex')`,
	} {
		if _, err := db.ExecContext(t.Context(), q); err == nil {
			t.Fatal("SQL acquisition invariant missing", q)
		}
	}
	// The receipt's unique constraints are a second defence against a future
	// regression that accidentally resets used. Failed reissue rolls back.
	if _, err := db.ExecContext(t.Context(), `UPDATE e2ee_ec_prekeys SET used=FALSE; UPDATE e2ee_kyber_prekeys SET used=FALSE WHERE NOT is_last_resort`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Acquire(t.Context(), 1, nextAcquisition(req)); err == nil {
		t.Fatal("same one-time key assigned twice")
	}
	registryCount(t, db, "e2ee_acquisitions", 1)
	acquisitionUsed(t, db, f.high.GenerationID, 0, 0)
}
