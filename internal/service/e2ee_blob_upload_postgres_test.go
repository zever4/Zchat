//go:build linux

package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/testutil"
)

type postgresBlobUpload struct {
	db               *sql.DB
	auth             *AuthService
	upload           *E2EEBlobUploadService
	requests         *domain.E2EEBlobAttemptRequest
	token            string
	credential, data []byte
	uid              int64
	path             string
	repo             *repository.PgE2EEBlobRepository
	budget           *storagebudget.Budget
}

func blobUploadPostgres(t *testing.T) *postgresBlobUpload {
	t.Helper()
	db := testutil.Postgres(t)
	auth := NewAuthService(repository.NewPgUserRepository(db), "synthetic-blob-upload-service-secret-32bytes")
	devices := NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	tokens := make([]string, 2)
	ids := make([]int64, 2)
	ds := make([]*domain.E2EEDevice, 2)
	raw := bytes.Repeat([]byte{37}, 32)
	for i := range 2 {
		name := []string{"alice", "bob"}[i]
		token, err := auth.Register(t.Context(), name, name+"@blob.invalid", "Synthetic-blob-test-password")
		if err != nil {
			t.Fatal(err)
		}
		tokens[i] = token
		claims, err := auth.ParseToken(token)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = claims.UserID
		identity := bytes.Repeat([]byte{byte(42 + i)}, 33)
		identity[0] = 5
		hash := E2EEDeviceCredentialHash(raw)
		registered, err := devices.Register(t.Context(), ids[i], domain.E2EEDeviceRegistration{RequestID: uuid.NewString(), RegistrationID: 16383, IdentityKey: identity, CredentialHash: hash[:]}, raw)
		if err != nil {
			t.Fatal(err)
		}
		ds[i] = registered.Device
		if _, err = repository.NewPgE2EEPreKeyRepository(db).Publish(t.Context(), ids[i], testutil.PublicPreKeyUpload(t, ds[i].GenerationID)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO user_settings(user_id)SELECT id FROM users ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')`, ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	chat, err := repository.NewPgE2EEConversationRepository(db).Create(t.Context(), ids[0], &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: ds[0].GenerationID}, CreationRequestId: uuid.NewString(), PeerUserId: ids[1]})
	if err != nil {
		t.Fatal(err)
	}
	// Reuse only the synthetic immutable metadata shape from the unit fixture.
	data := bytes.Repeat([]byte{0x42}, 64)
	r := blobUploadIntent(data)
	r.Reservation.Context.DeviceGenerationId = ds[0].GenerationID
	r.Reservation.ConversationID = chat.ID
	r.Reservation.Sender = &pb.DeviceRef{UserId: ds[0].UserUUID, DeviceId: 1, GenerationId: ds[0].GenerationID}
	r.Reservation.Recipient = &pb.DeviceRef{UserId: ds[1].UserUUID, DeviceId: 1, GenerationId: ds[1].GenerationID}
	sha := sha256.Sum256(data)
	r.Reservation.Blob.Sha256 = sha[:]
	repo := repository.NewPgE2EEBlobRepository(db)
	if err = repo.Configure(t.Context(), domain.E2EEBlobLimits{GlobalBytes: 4096, AccountBytes: 2048, GlobalClaims: 16, AccountClaims: 8, GlobalAttempts: 32, AccountAttempts: 16}); err != nil {
		t.Fatal(err)
	}
	grant, err := repo.Reserve(t.Context(), ids[0], r.Reservation)
	if err != nil {
		t.Fatal(err)
	}
	r.AttemptID = grant.AttemptID
	r.Revision = grant.Revision
	path := t.TempDir()
	if err = os.Chmod(path, 0700); err != nil {
		t.Fatal(err)
	}
	files, err := blobstore.Open(path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1 << 20, WriteTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	upload, err := NewE2EEBlobUploadService(repo, files, auth, devices, E2EEBlobUploadConfig{MaxUploads: 1, Timeout: 3 * time.Second, RecheckInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err = repo.BindStorage(t.Context(), files.StorageID()); err != nil {
		t.Fatal(err)
	}
	if _, err = upload.Reconcile(t.Context(), E2EEBlobStartupConfig{MaxAttempts: 64, MaxArtifacts: 128, MaxVerifyBytes: 1 << 20, Timeout: 3 * time.Second}); err != nil {
		t.Fatal(err)
	}
	budget := testPhysicalBudget(t)
	if err = upload.AttachBudget(t.Context(), budget, startupTestConfig()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := upload.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return &postgresBlobUpload{db, auth, upload, r, tokens[0], raw, data, ids[0], path, repo, budget}
}
func (f *postgresBlobUpload) assertState(t *testing.T, state string) {
	t.Helper()
	var got string
	var quota int64
	if err := f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, f.requests.AttemptID).Scan(&got); err != nil || got != state {
		t.Fatal(got, state, err)
	}
	if err := f.db.QueryRowContext(t.Context(), `SELECT reserved_bytes FROM e2ee_blob_global_quota`).Scan(&quota); err != nil || quota != 64 {
		t.Fatal("quota changed during write", quota, err)
	}
}
func TestBlobUploadRealAuthPostgresAndFilesystem(t *testing.T) {
	f := blobUploadPostgres(t)
	result, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, io.NopCloser(bytes.NewReader(f.data)))
	if err != nil || result == nil || result.Grant.State != "ready" {
		t.Fatal(result, err)
	}
	f.assertState(t, "ready")
	got, err := os.ReadFile(filepath.Join(f.path, f.requests.AttemptID+".blob"))
	if err != nil || !bytes.Equal(got, f.data) {
		t.Fatal("ciphertext changed", err)
	}
}
func TestBlobUploadLogoutUnblocksWithoutBodyBarrierPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	src := blockedUploadSource()
	done := make(chan error, 1)
	go func() { _, err := f.upload.Upload(t.Context(), f.token, f.credential, f.requests, src); done <- err }()
	awaitUploadRead(t, src)
	// Every route/attempt row can be locked while the source is blocked. A
	// streaming transaction would time out here rather than silently passing.
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, q := range []string{`SELECT id FROM users ORDER BY id FOR UPDATE`, `SELECT user_id FROM e2ee_devices ORDER BY user_id FOR UPDATE`,
		`SELECT conversation_id FROM e2ee_conversations FOR UPDATE`, `SELECT message_id FROM e2ee_message_claims FOR UPDATE`, `SELECT attempt_id FROM e2ee_blob_attempts FOR UPDATE`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			t.Fatal("SQL locks held during stream", err)
		}
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	logoutCtx, logoutCancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer logoutCancel()
	if err = f.auth.Logout(logoutCtx, f.token, false, func(int64, string) {}); err != nil {
		t.Fatal("logout blocked by source read", err)
	}
	select {
	case err = <-done:
		if !errors.Is(err, domain.ErrInvalidSession) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("revoked upload did not stop")
	}
	f.assertState(t, "writing") // No cleanup/release before a future confirmed Deleting.
}
func TestBlobUploadInvalidCredentialBeforeFilesystemPostgres(t *testing.T) {
	f := blobUploadPostgres(t)
	result, err := f.upload.Upload(t.Context(), f.token, bytes.Repeat([]byte{9}, 32), f.requests, io.NopCloser(bytes.NewReader(f.data)))
	if result != nil || !errors.Is(err, domain.ErrE2EEDeviceCredential) {
		t.Fatal(result, err)
	}
	f.assertState(t, "reserved")
	if _, err = os.Stat(filepath.Join(f.path, f.requests.AttemptID+".part")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("opened file before auth", err)
	}
}
