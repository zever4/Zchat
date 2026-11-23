//go:build linux

package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/handlers"
	"github.com/zever4/Zchat/internal/metrics"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/testutil"
	"github.com/zever4/Zchat/internal/uploadstorage"
)

type blobLifecycleFixture struct {
	db               *sql.DB
	config           blobStorageConfig
	web              *os.Root
	ordinary         *uploadstorage.Manager
	repo             *repository.PgE2EEBlobRepository
	auth             *service.AuthService
	devices          *service.E2EEDeviceService
	runtime          *blobStorageRuntime
	request          *domain.E2EEBlobAttemptRequest
	data, credential []byte
	token            string
	uid              int64
	events           chan service.E2EEBlobCleanupSweep
}

func blobLifecyclePostgres(t *testing.T) *blobLifecycleFixture {
	t.Helper()
	db := testutil.Postgres(t)
	cfg, web, ordinary := blobRuntimeDirectories(t)
	cfg.Files.HeadroomBytes = 1
	cfg.Upload.Timeout, cfg.Files.WriteTimeout = 3*time.Second, 3*time.Second
	f := &blobLifecycleFixture{db: db, config: cfg, web: web, ordinary: ordinary, repo: repository.NewPgE2EEBlobRepository(db), data: bytes.Repeat([]byte{42}, 64), credential: bytes.Repeat([]byte{37}, 32), events: make(chan service.E2EEBlobCleanupSweep, 64)}
	f.auth = service.NewAuthService(repository.NewPgUserRepository(db), "synthetic-lifecycle-jwt-secret-atleast32bytes")
	f.devices = service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), f.auth)
	ids := make([]int64, 2)
	devices := make([]*domain.E2EEDevice, 2)
	for i, name := range []string{"alice", "bob"} {
		token, err := f.auth.Register(t.Context(), name, name+"@blob-lifecycle.invalid", "Synthetic-lifecycle-password")
		if err != nil {
			t.Fatal(err)
		}
		claims, err := f.auth.ParseToken(token)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = claims.UserID
		identity := bytes.Repeat([]byte{byte(42 + i)}, 33)
		identity[0] = 5
		hash := service.E2EEDeviceCredentialHash(f.credential)
		d, err := f.devices.Register(t.Context(), ids[i], domain.E2EEDeviceRegistration{RequestID: uuid.NewString(), IdentityKey: identity, CredentialHash: hash[:], RegistrationID: 16383}, f.credential)
		if err != nil {
			t.Fatal(err)
		}
		devices[i] = d.Device
		if _, err = repository.NewPgE2EEPreKeyRepository(db).Publish(t.Context(), ids[i], testutil.PublicPreKeyUpload(t, d.Device.GenerationID)); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			f.token, f.uid = token, ids[i]
		}
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status)VALUES($1,$2,'accepted')`, ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	chat, err := repository.NewPgE2EEConversationRepository(db).Create(t.Context(), ids[0], &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: devices[0].GenerationID}, CreationRequestId: uuid.NewString(), PeerUserId: ids[1]})
	if err != nil {
		t.Fatal(err)
	}
	epoch := uint64(0)
	hash := sha256.Sum256(f.data)
	f.request = &domain.E2EEBlobAttemptRequest{Reservation: &domain.E2EEBlobReservationRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: devices[0].GenerationID}, MessageID: uuid.NewString(), ConversationID: chat.ID, CryptoEpoch: &epoch,
		Sender: &pb.DeviceRef{UserId: devices[0].UserUUID, GenerationId: devices[0].GenerationID, DeviceId: 1}, Recipient: &pb.DeviceRef{UserId: devices[1].UserUUID, GenerationId: devices[1].GenerationID, DeviceId: 1}, OperationID: uuid.NewString(), Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: uint64(len(f.data)), Sha256: hash[:]}}}
	f.runtime, err = startBlobStorage(t.Context(), cfg, web, ordinary.Budget(), f.repo, f.auth, f.devices, func(s service.E2EEBlobCleanupSweep, err error) {
		metrics.ObserveBlobCleanup(s, err)
		select {
		case f.events <- s:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		if err := f.runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	grant, err := f.runtime.Reserve(t.Context(), f.uid, f.request.Reservation)
	if err != nil {
		t.Fatal(err)
	}
	f.request.AttemptID, f.request.Revision = grant.AttemptID, grant.Revision
	return f
}
func (f *blobLifecycleFixture) server(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	f.runtime.Register(mux)
	s := httptest.NewServer(metrics.MetricsMiddleware(handlers.LimitRequests(mux, mux), mux))
	t.Cleanup(s.Close)
	return s
}
func (f *blobLifecycleFixture) httpRequest(t *testing.T, url string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPut, url+"/api/e2ee/blobs/"+f.request.AttemptID, bytes.NewReader(f.data))
	if err != nil {
		t.Fatal(err)
	}
	meta, err := boundary.EncodeBlobUploadMetadata(f.request)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Authorization", "Bearer "+f.token)
	r.Header.Set("Content-Type", "application/octet-stream")
	r.Header.Set(boundary.BlobUploadMetadataHeader, meta)
	r.Header.Set(boundary.BlobDeviceCredentialHeader, base64.RawURLEncoding.EncodeToString(f.credential))
	return r
}
func (f *blobLifecycleFixture) write(t *testing.T) {
	t.Helper()
	s := f.server(t)
	resp, err := s.Client().Do(f.httpRequest(t, s.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 201 {
		t.Fatal(resp.StatusCode, string(body))
	}
}
func (f *blobLifecycleFixture) freshBudget(t *testing.T) *uploadstorage.Manager {
	t.Helper()
	if err := f.ordinary.Close(); err != nil {
		t.Fatal(err)
	}
	c, err := storagebudget.ConfigFromEnv(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	c.TotalBytes, c.AccountBytes = 5_000_000_000, 1_000_000_000
	c.AccountEntries = 1 // Distinguish sender attribution from the global census.
	c.FreeBytes, c.FreeEntries = 1, 1
	m, err := uploadstorage.Open(t.Context(), filepath.Join(f.web.Name(), "uploads"), c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Close(); err != nil {
			t.Error(err)
		}
	})
	return m
}
func TestBlobLifecycleActualPostgresRouteAndRestartPreserveReady(t *testing.T) {
	f := blobLifecyclePostgres(t)
	f.write(t)
	if err := f.runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(f.config.Root, ".store-id"))
	if err != nil {
		t.Fatal(err)
	}
	m := f.freshBudget(t)
	r, err := startBlobStorage(t.Context(), f.config, f.web, m.Budget(), f.repo, f.auth, f.devices, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	if r.upload.Ready() != true {
		t.Fatal("restart did not reconcile")
	}
	got, err := os.ReadFile(filepath.Join(f.config.Root, f.request.AttemptID+".blob"))
	if err != nil || !bytes.Equal(got, f.data) {
		t.Fatal("restart lost Ready bytes", err)
	}
	again, err := os.ReadFile(filepath.Join(f.config.Root, ".store-id"))
	if err != nil || !bytes.Equal(marker, again) {
		t.Fatal("restart changed root", err)
	}
	var max, reserved int64
	if err = f.db.QueryRowContext(t.Context(), `SELECT max_bytes,reserved_bytes FROM e2ee_blob_global_quota`).Scan(&max, &reserved); err != nil || max != 50_000_000_000 || reserved != 64 {
		t.Fatal(max, reserved, err)
	}
	if _, err = m.Budget().Reserve(t.Context(), f.uid, storagebudget.Usage{Bytes: 1_000_000_000, Entries: 1}); !errors.Is(err, storagebudget.ErrQuota) {
		t.Fatal("lowered physical quota lost sender census", err)
	}
	other, err := m.Budget().Reserve(t.Context(), f.uid+100, storagebudget.Usage{Bytes: 1, Entries: 1})
	if err != nil {
		t.Fatal("sender charge leaked to independent account", err)
	}
	other.Settle(storagebudget.Usage{})
}
func TestBlobLifecycleActualPostgresConfigurationMismatchCannotResetData(t *testing.T) {
	f := blobLifecyclePostgres(t)
	f.write(t)
	if err := f.runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	m := f.freshBudget(t)
	for _, kind := range []string{"quota", "root"} {
		t.Run(kind, func(t *testing.T) {
			cfg := f.config
			want := domain.ErrE2EEBlobConfiguration
			if kind == "quota" {
				cfg.Limits.AccountBytes--
			} else {
				cfg.Root = filepath.Join(filepath.Dir(f.config.Root), "other-private")
				if err := os.Mkdir(cfg.Root, 0700); err != nil {
					t.Fatal(err)
				}
				want = domain.ErrE2EEBlobStorage
			}
			if r, err := startBlobStorage(t.Context(), cfg, f.web, m.Budget(), f.repo, f.auth, f.devices, nil); r != nil || !errors.Is(err, want) {
				if r != nil {
					r.Close(t.Context())
				}
				t.Fatal(r, err, want)
			}
			got, err := os.ReadFile(filepath.Join(f.config.Root, f.request.AttemptID+".blob"))
			if err != nil || !bytes.Equal(got, f.data) {
				t.Fatal("mismatch changed existing file", err)
			}
		})
	}
	var reserved int64
	if err := f.db.QueryRowContext(t.Context(), `SELECT reserved_bytes FROM e2ee_blob_global_quota`).Scan(&reserved); err != nil || reserved != 64 {
		t.Fatal("mismatch reset counters", reserved, err)
	}
}
func TestBlobLifecycleActualPostgresCollectorRemovesExpiredStaging(t *testing.T) {
	f := blobLifecyclePostgres(t)
	f.write(t)
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt;UPDATE e2ee_blob_attempts SET reserved_at=reserved_at-INTERVAL '2 hours',expires_at=expires_at-INTERVAL '2 hours';ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case s := <-f.events:
			if s.Reclaimed > 0 {
				goto reclaimed
			}
		case <-deadline:
			t.Fatal("owned collector did not reclaim expired staging")
		}
	}
reclaimed:
	var state string
	var reserved int64
	if err := f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, f.request.AttemptID).Scan(&state); err != nil || state != "reclaimed" {
		t.Fatal(state, err)
	}
	if err := f.db.QueryRowContext(t.Context(), `SELECT reserved_bytes FROM e2ee_blob_global_quota`).Scan(&reserved); err != nil || reserved != 0 {
		t.Fatal(reserved, err)
	}
	if _, err := os.Stat(filepath.Join(f.config.Root, f.request.AttemptID+".blob")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("collector retained file", err)
	}
}
func TestBlobLifecycleActualPostgresCloseCancelsSocketAndJoinsBeforeUnlock(t *testing.T) {
	f := blobLifecyclePostgres(t)
	s := f.server(t)
	conn, err := net.Dial("tcp", s.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := f.httpRequest(t, s.URL)
	fmt.Fprintf(conn, "PUT %s HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n", r.URL.Path, len(f.data))
	r.Header.Write(conn)
	io.WriteString(conn, "\r\n")
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err = os.Stat(filepath.Join(f.config.Root, f.request.AttemptID+".part")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("writer not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
	ctx, stop := context.WithTimeout(t.Context(), 2*time.Second)
	defer stop()
	if err = f.runtime.Close(ctx); err != nil {
		t.Fatal("runtime did not join blocked HTTP upload", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatal(resp.StatusCode)
	}
	if _, n := f.ordinary.Budget().Usage(); n != 0 {
		t.Fatal("active reservation survived runtime close", n)
	}
	other, err := blobstore.Open(f.config.Root, f.config.Files)
	if err != nil {
		t.Fatal("joined runtime retained owner", err)
	}
	other.Close()
	if err = f.db.PingContext(t.Context()); err != nil {
		t.Fatal("runtime closed caller-owned DB", err)
	}
}

func TestBlobLifecycleActualPostgresMaintenanceWithoutAPIStillAccountsAndCleans(t *testing.T) {
	f := blobLifecyclePostgres(t)
	f.write(t)
	if err := f.runtime.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	m := f.freshBudget(t)
	cfg := f.config
	cfg.Expose = false
	events := make(chan service.E2EEBlobCleanupSweep, 64)
	r, err := startBlobStorage(t.Context(), cfg, f.web, m.Budget(), f.repo, f.auth, f.devices, func(s service.E2EEBlobCleanupSweep, _ error) {
		select {
		case events <- s:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close(context.Background())
	mux := http.NewServeMux()
	r.Register(mux)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("PUT", "/api/e2ee/blobs/"+f.request.AttemptID, nil))
	if w.Code != 404 {
		t.Fatal("maintenance exposed upload API", w.Code)
	}
	before, _ := m.Budget().Usage()
	if _, err := m.Budget().Reserve(t.Context(), f.uid, storagebudget.Usage{Bytes: 1_000_000_000, Entries: 1}); !errors.Is(err, storagebudget.ErrQuota) {
		t.Fatal("API disable lost account quota", err)
	}
	if _, err = f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt;UPDATE e2ee_blob_attempts SET reserved_at=reserved_at-INTERVAL '2 hours',expires_at=expires_at-INTERVAL '2 hours';ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(3 * time.Second)
	for {
		select {
		case s := <-events:
			if s.Reclaimed > 0 {
				after, _ := m.Budget().Usage()
				if after.Bytes >= before.Bytes {
					t.Fatal("maintenance did not free physical charge", after, before)
				}
				return
			}
		case <-deadline:
			t.Fatal("API disable stopped collector")
		}
	}
}
