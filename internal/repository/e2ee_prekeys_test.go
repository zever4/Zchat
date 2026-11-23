package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

func publicationDB(t *testing.T) (*sql.DB, *PgE2EEDeviceRepository, *PgE2EEPreKeyRepository, *pb.UploadPreKeysRequest) {
	t.Helper()
	db, devices := registryDB(t)
	d, err := devices.Register(t.Context(), 1, registrationMaterial())
	if err != nil {
		t.Fatal(err)
	}
	return db, devices, NewPgE2EEPreKeyRepository(db), testutil.PublicPreKeyUpload(t, d.Device.GenerationID)
}
func publicationState(t *testing.T, devices *PgE2EEDeviceRepository, revision uint64, ready bool) {
	t.Helper()
	_, d, err := devices.Own(t.Context(), 1)
	if err != nil || d == nil || d.PublicMaterialRevision != revision || d.PublicMaterialReady != ready {
		t.Fatal("publication state mismatch", err)
	}
}
func nextPublication(req *pb.UploadPreKeysRequest, revision uint64) *pb.UploadPreKeysRequest {
	result := proto.Clone(req).(*pb.UploadPreKeysRequest)
	result.BatchId = uuid.NewString()
	result.ExpectedRevision = revision
	return result
}

func TestE2EEPublicationConcurrentReplayPostgres(t *testing.T) {
	db, devices, r, req := publicationDB(t)
	var fresh atomic.Int32
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			receipt, err := r.Publish(t.Context(), 1, req)
			if err != nil {
				t.Error(err)
				return
			}
			if !receipt.Replayed {
				fresh.Add(1)
			}
			if receipt.Revision != 1 {
				t.Error("replay advanced revision")
			}
		})
	}
	wg.Wait()
	if fresh.Load() != 1 {
		t.Fatal("multiple fresh publications")
	}
	publicationState(t, devices, 1, true)
	registryCount(t, db, "e2ee_publication_batches", 1)
	registryCount(t, db, "e2ee_signed_prekeys", 1)
	registryCount(t, db, "e2ee_ec_prekeys", 1)
	registryCount(t, db, "e2ee_kyber_prekeys", 2)
	for _, mutate := range []func(*pb.UploadPreKeysRequest){func(r *pb.UploadPreKeysRequest) { r.ExpectedRevision++ }, func(r *pb.UploadPreKeysRequest) { r.OneTimePreKeys[0].PublicKey[1] ^= 1 }} {
		changed := proto.Clone(req).(*pb.UploadPreKeysRequest)
		mutate(changed)
		if _, err := r.Publish(t.Context(), 1, changed); !errors.Is(err, domain.ErrE2EERequestConflict) {
			t.Fatal("changed batch replay accepted", err)
		}
	}
	// Simulate a future acquirer's committed consumption. This is NOT bundle API acceptance.
	if _, err := db.ExecContext(t.Context(), `UPDATE e2ee_ec_prekeys SET used=TRUE; UPDATE e2ee_kyber_prekeys SET used=TRUE WHERE NOT is_last_resort`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, nextPublication(req, 1)); err != nil {
		t.Fatal(err)
	}
	var ec, pqUsed int
	if err := db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_ec_prekeys WHERE used),(SELECT count(*) FROM e2ee_kyber_prekeys WHERE used)`).Scan(&ec, &pqUsed); err != nil || ec != 1 || pqUsed != 1 {
		t.Fatal("upload reset consumed keys", err)
	}
	publicationState(t, devices, 2, true)
}

func TestE2EEPublicationReadinessRotationPostgres(t *testing.T) {
	db, devices, r, all := publicationDB(t)
	ec := proto.Clone(all).(*pb.UploadPreKeysRequest)
	ec.SignedPreKey = nil
	ec.KyberPreKeys = nil
	if _, err := r.Publish(t.Context(), 1, ec); err != nil {
		t.Fatal(err)
	}
	publicationState(t, devices, 1, false)
	signed := nextPublication(all, 1)
	signed.OneTimePreKeys = nil
	signed.KyberPreKeys = nil
	if _, err := r.Publish(t.Context(), 1, signed); err != nil {
		t.Fatal(err)
	}
	publicationState(t, devices, 2, false)
	lr := nextPublication(all, 2)
	lr.OneTimePreKeys = nil
	lr.SignedPreKey = nil
	lr.KyberPreKeys = lr.KyberPreKeys[1:]
	if _, err := r.Publish(t.Context(), 1, lr); err != nil {
		t.Fatal(err)
	}
	publicationState(t, devices, 3, true)
	rotation := nextPublication(all, 3)
	rotation.OneTimePreKeys = nil
	rotation.SignedPreKey.KeyId = 0
	rotation.KyberPreKeys = rotation.KyberPreKeys[1:]
	rotation.KyberPreKeys[0].KeyId = 0
	if _, err := r.Publish(t.Context(), 1, rotation); err != nil {
		t.Fatal(err)
	}
	replay, err := r.Publish(t.Context(), 1, lr)
	if err != nil || !replay.Replayed || replay.Revision != 3 {
		t.Fatal("old retry lost receipt", err)
	}
	publicationState(t, devices, 4, true)
	var signedID, lrID int64
	if err := db.QueryRowContext(t.Context(), `SELECT active_signed_prekey_id,active_last_resort_id FROM e2ee_devices WHERE generation_id=$1`, all.Context.DeviceGenerationId).Scan(&signedID, &lrID); err != nil || signedID != 0 || lrID != 0 {
		t.Fatal("late retry rewound active pointers", err)
	}
	stale := nextPublication(rotation, 3)
	if _, err := r.Publish(t.Context(), 1, stale); !errors.Is(err, domain.ErrE2EEPublicationChanged) {
		t.Fatal("stale CAS accepted", err)
	}
	for _, old := range []*pb.UploadPreKeysRequest{signed, lr} {
		if _, err := r.Publish(t.Context(), 1, nextPublication(old, 4)); !errors.Is(err, domain.ErrE2EEKeyConflict) {
			t.Fatal("retired key reactivated", err)
		}
	}
	registryCount(t, db, "e2ee_signed_prekeys", 2)
	registryCount(t, db, "e2ee_kyber_prekeys", 2)
}

func TestE2EEPublicationGenerationIsolationPostgres(t *testing.T) {
	db, devices, r, req := publicationDB(t)
	if _, err := r.Publish(t.Context(), 2, req); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("cross-account publication accepted", err)
	}
	if _, err := r.Publish(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	replacement := registrationMaterial()
	replacement.ExpectedGenerationID = req.Context.DeviceGenerationId
	newDevice, err := devices.Replace(t.Context(), 1, replacement, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1})
	if err != nil {
		t.Fatal(err)
	}
	publicationState(t, devices, 0, false)
	if _, err := r.Publish(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired generation replay accepted", err)
	}
	fresh := proto.Clone(req).(*pb.UploadPreKeysRequest)
	fresh.Context.DeviceGenerationId = newDevice.Device.GenerationID
	if _, err := r.Publish(t.Context(), 1, fresh); err != nil {
		t.Fatal("new generation cannot reuse independent key IDs", err)
	}
	registryCount(t, db, "e2ee_publication_batches", 2)
	registryCount(t, db, "e2ee_ec_prekeys", 2)
	var retiredReady bool
	if err := db.QueryRowContext(t.Context(), `SELECT public_material_ready FROM e2ee_devices WHERE generation_id=$1`, req.Context.DeviceGenerationId).Scan(&retiredReady); err != nil || retiredReady {
		t.Fatal("retired device still ready", err)
	}
}

func TestE2EEPublicationAtomicFailurePostgres(t *testing.T) {
	db, devices, r, req := publicationDB(t)
	_, err := db.ExecContext(t.Context(), `CREATE FUNCTION reject_publication_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic commit failure'; END $$;
        CREATE CONSTRAINT TRIGGER reject_publication_commit AFTER INSERT ON e2ee_publication_batches DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_publication_commit()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, req); err == nil {
		t.Fatal("failed COMMIT acknowledged")
	}
	for _, table := range []string{"e2ee_publication_batches", "e2ee_signed_prekeys", "e2ee_ec_prekeys", "e2ee_kyber_prekeys"} {
		registryCount(t, db, table, 0)
	}
	publicationState(t, devices, 0, false)
	if _, err := db.ExecContext(t.Context(), `DROP TRIGGER reject_publication_commit ON e2ee_publication_batches; DROP FUNCTION reject_publication_commit()`); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.UploadPreKeysRequest){
		func(q *pb.UploadPreKeysRequest) { q.SignedPreKey.Signature[0] ^= 1 },
		func(q *pb.UploadPreKeysRequest) { q.OneTimePreKeys[0].PublicKey[1] ^= 1 },
		func(q *pb.UploadPreKeysRequest) { q.KyberPreKeys[0].Signature[0] ^= 1 },
		func(q *pb.UploadPreKeysRequest) {
			q.KyberPreKeys[0].IsLastResort = true
			q.KyberPreKeys[1].IsLastResort = false
		},
	} {
		collision := nextPublication(req, 1)
		mutate(collision)
		if _, err := r.Publish(t.Context(), 1, collision); !errors.Is(err, domain.ErrE2EEKeyConflict) {
			t.Fatal("changed key accepted", err)
		}
		publicationState(t, devices, 1, true)
		registryCount(t, db, "e2ee_publication_batches", 1)
	}
	// Insert a fresh EC key before a conflicting PQ write; all must roll back.
	collision := nextPublication(req, 1)
	collision.OneTimePreKeys[0].KeyId = 99
	collision.KyberPreKeys[0].Signature[0] ^= 1
	if _, err := r.Publish(t.Context(), 1, collision); !errors.Is(err, domain.ErrE2EEKeyConflict) {
		t.Fatal(err)
	}
	registryCount(t, db, "e2ee_ec_prekeys", 1)
	lock, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(t.Context(), `SELECT id FROM users WHERE id=1 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if _, err := r.Publish(ctx, 1, nextPublication(req, 1)); err == nil {
		t.Fatal("canceled lock waiter published")
	}
	if err := lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, nextPublication(req, 1)); err != nil {
		t.Fatal("connection not reusable after cancellation", err)
	}
	publicationState(t, devices, 2, true)
}

func TestE2EEPublicationCapacityPostgres(t *testing.T) {
	db, devices, r, req := publicationDB(t)
	gen := req.Context.DeviceGenerationId
	// Populate the retained budget directly, without thousands of RPCs. Each
	// row is public test material; this does not simulate private-key generation.
	_, err := db.ExecContext(t.Context(), `INSERT INTO e2ee_ec_prekeys(generation_id,key_id,public_key,used) SELECT $1,n,$2,TRUE FROM generate_series(0,$3::integer-1) n`, gen, req.OneTimePreKeys[0].PublicKey, domain.MaxE2EERetainedPreKeys)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEKeyCapacity) {
		t.Fatal("retained EC cap bypassed", err)
	}
	publicationState(t, devices, 0, false)
	registryCount(t, db, "e2ee_publication_batches", 0)
	registryCount(t, db, "e2ee_signed_prekeys", 0)
	if _, err := db.ExecContext(t.Context(), `DELETE FROM e2ee_ec_prekeys`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO e2ee_kyber_prekeys(generation_id,key_id,public_key,signature,is_last_resort,used) SELECT $1,n,$2,$3,FALSE,TRUE FROM generate_series(0,$4::integer-1) n`, gen, req.KyberPreKeys[0].PublicKey, req.KyberPreKeys[0].Signature, domain.MaxE2EERetainedPreKeys); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEKeyCapacity) {
		t.Fatal("retained PQ cap bypassed", err)
	}
	if _, err := db.ExecContext(t.Context(), `DELETE FROM e2ee_kyber_prekeys`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO e2ee_signed_prekeys(generation_id,key_id,public_key,signature,created_at_unix_ms) SELECT $1,n,$2,$3,0 FROM generate_series(0,$4::integer-1) n`, gen, req.SignedPreKey.PublicKey, req.SignedPreKey.Signature, domain.MaxE2EERetainedSignedKeys); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEKeyCapacity) {
		t.Fatal("retained signed cap bypassed", err)
	}
	if _, err := db.ExecContext(t.Context(), `DELETE FROM e2ee_signed_prekeys`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO e2ee_publication_batches(generation_id,batch_id,digest,publication_revision) SELECT $1,gen_random_uuid(),$2,n FROM generate_series(1,$3::integer) n`, gen, bytes.Repeat([]byte{1}, 32), domain.MaxE2EEPublicationBatches); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Publish(t.Context(), 1, req); !errors.Is(err, domain.ErrE2EEKeyCapacity) {
		t.Fatal("batch cap bypassed", err)
	}
	publicationState(t, devices, 0, false)
	registryCount(t, db, "e2ee_ec_prekeys", 0)
}

func TestE2EEPublicKeySQLConstraintsPostgres(t *testing.T) {
	db, _, r, req := publicationDB(t)
	if _, err := r.Publish(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		`UPDATE e2ee_ec_prekeys SET key_id=-1`,
		`UPDATE e2ee_ec_prekeys SET key_id=4294967296`,
		`UPDATE e2ee_ec_prekeys SET public_key=decode('05','hex')`,
		`UPDATE e2ee_kyber_prekeys SET public_key=decode('08','hex')`,
		`UPDATE e2ee_kyber_prekeys SET used=TRUE WHERE is_last_resort`,
		`UPDATE e2ee_signed_prekeys SET signature=decode('00','hex')`,
		`UPDATE e2ee_signed_prekeys SET created_at_unix_ms=-1`,
		`UPDATE e2ee_devices SET active_last_resort_id=4294967294`,
		`UPDATE e2ee_devices SET last_resort_marker=FALSE`,
		`UPDATE e2ee_devices SET public_material_revision=-1`,
		`UPDATE e2ee_devices SET active_signed_prekey_id=NULL`,
		`UPDATE e2ee_publication_batches SET digest=decode('00','hex')`,
	} {
		if _, err := db.ExecContext(t.Context(), q); err == nil {
			t.Fatal("SQL invariant missing", q)
		}
	}
	// No auto-import into or mutation of the legacy prototype tables.
	registryCount(t, db, "device_keys", 0)
}

func TestE2EEPublicKeyMigrationPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 19); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 20); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty public-key rollback", err)
	}
	if err := goose.UpTo(db, "../../migrations", 20); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO users(id,user_uuid,username,email,password_hash) VALUES(1,'11111111-1111-4111-8111-111111111111','a','a@example.invalid','unused')`); err != nil {
		t.Fatal(err)
	}
	d, err := NewPgE2EEDeviceRepository(db).Register(t.Context(), 1, registrationMaterial())
	if err != nil {
		t.Fatal(err)
	}
	req := testutil.PublicPreKeyUpload(t, d.Device.GenerationID)
	if _, err := NewPgE2EEPreKeyRepository(db).Publish(t.Context(), 1, req); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("rollback discarded public keys")
	}
	registryCount(t, db, "e2ee_publication_batches", 1)
	var pgErr *pq.Error
	_, err = db.ExecContext(t.Context(), `DELETE FROM e2ee_signed_prekeys`)
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatal("active signed FK missing", err)
	}
}
