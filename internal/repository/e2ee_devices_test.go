package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pressly/goose"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
)

func registryDB(t *testing.T) (*sql.DB, *PgE2EEDeviceRepository) {
	t.Helper()
	db := testutil.Postgres(t)
	_, err := db.ExecContext(t.Context(), `INSERT INTO users(id,user_uuid,username,email,password_hash) VALUES
        (1,'11111111-1111-4111-8111-111111111111','device_a','device-a@example.invalid','verified-test-hash'),
        (2,'22222222-2222-4222-8222-222222222222','device_b','device-b@example.invalid','verified-test-hash')`)
	if err != nil {
		t.Fatal(err)
	}
	return db, NewPgE2EEDeviceRepository(db)
}
func registrationMaterial() domain.E2EEDeviceRegistration {
	key := bytes.Repeat([]byte{42}, 33)
	key[0] = 5
	id := uuid.NewString()
	syntheticVerifier := sha256.Sum256([]byte(id))
	return domain.E2EEDeviceRegistration{RequestID: id, RegistrationID: 4294967295, IdentityKey: key, CredentialHash: syntheticVerifier[:]}
}
func registryCount(t *testing.T, db *sql.DB, table string, want int) {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM "+pq.QuoteIdentifier(table)).Scan(&n); err != nil || n != want {
		t.Fatalf("%s count=%d want=%d err=%v", table, n, want, err)
	}
}

func TestE2EEDeviceRegistrationPostgres(t *testing.T) {
	db, r := registryDB(t)
	ctx := t.Context()
	req := registrationMaterial()
	account, d, err := r.Own(ctx, 1)
	if err != nil || d != nil || account != "11111111-1111-4111-8111-111111111111" {
		t.Fatal("unexpected initial state", err)
	}
	var wg sync.WaitGroup
	var fresh atomic.Int32
	generations := make(chan string, 12)
	for range 12 {
		wg.Go(func() {
			result, err := r.Register(ctx, 1, req)
			if err != nil {
				t.Error(err)
				return
			}
			if !result.Replayed {
				fresh.Add(1)
			}
			generations <- result.Device.GenerationID
		})
	}
	wg.Wait()
	close(generations)
	var generation string
	for got := range generations {
		if generation != "" && got != generation {
			t.Fatal("retry registered another generation")
		}
		generation = got
	}
	if fresh.Load() != 1 {
		t.Fatal("concurrent retry created multiple devices")
	}
	registryCount(t, db, "e2ee_devices", 1)
	registryCount(t, db, "e2ee_device_requests", 1)
	_, d, err = r.Own(ctx, 1)
	if err != nil || d.RegistrationID != 4294967295 || d.PublicMaterialReady {
		t.Fatal("material or readiness drift", err)
	}
	collision := req
	collision.IdentityKey = bytes.Clone(req.IdentityKey)
	collision.IdentityKey[1] ^= 1
	if _, err = r.Register(ctx, 1, collision); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("changed retry accepted", err)
	}
	collision = req
	collision.RequestID = uuid.NewString()
	if _, err = r.Register(ctx, 1, collision); !errors.Is(err, domain.ErrE2EEDeviceConflict) {
		t.Fatal("implicit replacement accepted", err)
	}
	// UUID dedup is scoped to authenticated account, never a global request row.
	other, err := r.Register(ctx, 2, req)
	if err != nil || other.Device.UserUUID == d.UserUUID || other.Device.GenerationID == generation {
		t.Fatal("account scope mismatch", err)
	}
	if _, _, err := r.Own(ctx, 999); !errors.Is(err, domain.ErrUserNotFound) {
		t.Fatal("missing user accepted", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO e2ee_devices(generation_id,user_id,registration_id,identity_key,credential_hash) VALUES($1,1,1,$2,$3)`, uuid.NewString(), req.IdentityKey, req.CredentialHash)
	var pgErr *pq.Error
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatal("SQL active-device uniqueness missing", err)
	}
	_, err = db.ExecContext(ctx, `INSERT INTO e2ee_device_requests(user_id,request_id,operation,result_generation_id) VALUES(1,$1,'register',$2)`, uuid.NewString(), other.Device.GenerationID)
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatal("cross-account ledger FK missing", err)
	}
}

func TestE2EEDeviceReplacementPostgres(t *testing.T) {
	db, r := registryDB(t)
	ctx := t.Context()
	firstReq := registrationMaterial()
	first, err := r.Register(ctx, 1, firstReq)
	if err != nil {
		t.Fatal(err)
	}
	verified := domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}
	reused := registrationMaterial()
	reused.ExpectedGenerationID = first.Device.GenerationID
	reused.CredentialHash = firstReq.CredentialHash
	if _, err := r.Replace(ctx, 1, reused, verified); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal("old device credential reused on replacement", err)
	}
	type result struct {
		req    domain.E2EEDeviceRegistration
		change *domain.E2EEDeviceChange
		err    error
	}
	results := make(chan result, 8)
	var wg sync.WaitGroup
	for range 8 {
		req := registrationMaterial()
		req.ExpectedGenerationID = first.Device.GenerationID
		wg.Go(func() { change, err := r.Replace(ctx, 1, req, verified); results <- result{req, change, err} })
	}
	wg.Wait()
	close(results)
	var winner result
	wins := 0
	for got := range results {
		if got.err == nil {
			winner = got
			wins++
		} else if !errors.Is(got.err, domain.ErrE2EEGenerationChanged) {
			t.Fatal(got.err)
		}
	}
	if wins != 1 {
		t.Fatalf("CAS winners=%d", wins)
	}
	replay, err := r.Replace(ctx, 1, winner.req, verified)
	if err != nil || !replay.Replayed || replay.Device.GenerationID != winner.change.Device.GenerationID {
		t.Fatal("replacement retry changed generation", err)
	}
	registryCount(t, db, "e2ee_devices", 2)
	registryCount(t, db, "e2ee_device_requests", 2)
	if _, err = r.Register(ctx, 1, firstReq); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired registration replay reactivated a device", err)
	}
	changed := winner.req
	changed.CredentialHash = bytes.Repeat([]byte{22}, 32)
	if _, err = r.Replace(ctx, 1, changed, verified); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("changed replacement replay accepted", err)
	}
	stalePassword := verified
	stalePassword.PasswordHash = "obsolete-test-hash"
	if _, err = r.Replace(ctx, 1, winner.req, stalePassword); !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatal("stale password snapshot accepted", err)
	}
	staleVersion := verified
	staleVersion.TokenVersion = 2
	if _, err = r.Replace(ctx, 1, winner.req, staleVersion); !errors.Is(err, domain.ErrInvalidCredentials) {
		t.Fatal("stale version snapshot accepted", err)
	}
	next := registrationMaterial()
	next.ExpectedGenerationID = replay.Device.GenerationID
	if _, err = r.Replace(ctx, 1, next, verified); err != nil {
		t.Fatal(err)
	}
	if _, err = r.Replace(ctx, 1, winner.req, verified); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("old replacement replay exposed a new device", err)
	}
}

func TestE2EEDeviceTransactionRollbackPostgres(t *testing.T) {
	db, r := registryDB(t)
	ctx := t.Context()
	first, err := r.Register(ctx, 1, registrationMaterial())
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(ctx, `CREATE FUNCTION reject_device_request_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic deferred failure'; END $$;
        CREATE CONSTRAINT TRIGGER reject_device_request_commit AFTER INSERT ON e2ee_device_requests DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION reject_device_request_commit()`)
	if err != nil {
		t.Fatal(err)
	}
	req := registrationMaterial()
	req.ExpectedGenerationID = first.Device.GenerationID
	if _, err = r.Replace(ctx, 1, req, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}); err == nil {
		t.Fatal("commit failure reported success")
	}
	_, active, err := r.Own(ctx, 1)
	if err != nil || active.GenerationID != first.Device.GenerationID {
		t.Fatal("failed commit retired old device", err)
	}
	registryCount(t, db, "e2ee_devices", 1)
	registryCount(t, db, "e2ee_device_requests", 1)
	if _, err = db.ExecContext(ctx, `DROP TRIGGER reject_device_request_commit ON e2ee_device_requests`); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT id FROM users WHERE id=2 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	limited, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	if _, err = r.Register(limited, 2, registrationMaterial()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("blocked registration ignored deadline", err)
	}
	tx.Rollback()
	registryCount(t, db, "e2ee_devices", 1)
	if _, err = r.Register(ctx, 2, registrationMaterial()); err != nil {
		t.Fatal("pool not usable after cancellation", err)
	}
}

func TestE2EEDeviceMigrationPostgres(t *testing.T) {
	db := testutil.EmptyPostgres(t)
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(db, "../../migrations", 19); err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err != nil {
		t.Fatal("empty registry rollback", err)
	}
	if err := goose.UpTo(db, "../../migrations", 19); err != nil {
		t.Fatal(err)
	}
	_, err := db.ExecContext(t.Context(), `INSERT INTO users(id,user_uuid,username,email,password_hash) VALUES(1,'11111111-1111-4111-8111-111111111111','a','a@example.invalid','unused')`)
	if err != nil {
		t.Fatal(err)
	}
	req := registrationMaterial()
	r := NewPgE2EEDeviceRepository(db)
	result, err := r.Register(t.Context(), 1, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := goose.Down(db, "../../migrations"); err == nil {
		t.Fatal("rollback silently discarded registry")
	}
	var generation string
	err = db.QueryRowContext(t.Context(), `SELECT generation_id::text FROM e2ee_devices WHERE user_id=1 AND retired_at IS NULL`).Scan(&generation)
	if err != nil || generation != result.Device.GenerationID {
		t.Fatal("failed rollback changed registry", err)
	}
	registryCount(t, db, "e2ee_device_requests", 1)
	// Up must not import or rewrite the separate legacy key tables.
	data, err := os.ReadFile("../../migrations/019_e2ee_devices.sql")
	if err != nil || !strings.Contains(string(data), "-- +goose Down") {
		t.Fatal("migration unavailable", err)
	}
}

func TestE2EEDeviceRegistryBoundsPostgres(t *testing.T) {
	db, _ := registryDB(t)
	req := registrationMaterial()
	for _, tc := range []struct {
		name         string
		device       int
		registration int64
		key, hash    []byte
	}{
		{"device zero", 0, 1, req.IdentityKey, req.CredentialHash},
		{"second device", 2, 1, req.IdentityKey, req.CredentialHash},
		{"device overflow", 128, 1, req.IdentityKey, req.CredentialHash},
		{"negative registration", 1, -1, req.IdentityKey, req.CredentialHash},
		{"registration overflow", 1, 4294967296, req.IdentityKey, req.CredentialHash},
		{"short identity", 1, 1, req.IdentityKey[:32], req.CredentialHash},
		{"wrong identity tag", 1, 1, bytes.Repeat([]byte{7}, 33), req.CredentialHash},
		{"short verifier", 1, 1, req.IdentityKey, req.CredentialHash[:31]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(t.Context(), `INSERT INTO e2ee_devices(generation_id,user_id,device_id,registration_id,identity_key,credential_hash) VALUES($1,1,$2,$3,$4,$5)`, uuid.NewString(), tc.device, tc.registration, tc.key, tc.hash)
			var pgErr *pq.Error
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
				t.Fatal("SQL bounds absent", err)
			}
		})
	}
	registryCount(t, db, "e2ee_devices", 0)
	if _, err := db.ExecContext(t.Context(), `INSERT INTO e2ee_devices(generation_id,user_id,registration_id,identity_key,credential_hash) VALUES($1,1,0,$2,$3)`, uuid.NewString(), req.IdentityKey, req.CredentialHash); err != nil {
		t.Fatal("uint32 zero rejected", err)
	}
}
