//go:build linux

package handlers

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/blobstore"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/storagebudget"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

type blobHTTPFixture struct {
	db               *sql.DB
	auth             *service.AuthService
	devices          *service.E2EEDeviceService
	repo             *repository.PgE2EEBlobRepository
	upload           *service.E2EEBlobUploadService
	intent           *domain.E2EEBlobAttemptRequest
	data, credential []byte
	token, path      string
	uid              int64
	budget           *storagebudget.Budget
}

func blobHTTPPostgres(t *testing.T, wrappers ...func(*repository.PgE2EEBlobRepository) service.E2EEBlobWriterRepository) *blobHTTPFixture {
	t.Helper()
	db := testutil.Postgres(t)
	auth := service.NewAuthService(repository.NewPgUserRepository(db), "synthetic-http-blob-secret-atleast32bytes")
	devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	f := &blobHTTPFixture{db: db, auth: auth, devices: devices, data: bytes.Repeat([]byte{0xff}, 65536), credential: bytes.Repeat([]byte{37}, 32)}
	ids := make([]int64, 2)
	ds := make([]*domain.E2EEDevice, 2)
	for i, name := range []string{"alice", "bob"} {
		token, err := auth.Register(t.Context(), name, name+"@http-blob.invalid", "Synthetic-HTTP-blob-password")
		if err != nil {
			t.Fatal(err)
		}
		claims, err := auth.ParseToken(token)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = claims.UserID
		identity := bytes.Repeat([]byte{byte(42 + i)}, 33)
		identity[0] = 5
		hash := service.E2EEDeviceCredentialHash(f.credential)
		device, err := devices.Register(t.Context(), ids[i], domain.E2EEDeviceRegistration{RequestID: uuid.NewString(), RegistrationID: 16383, IdentityKey: identity, CredentialHash: hash[:]}, f.credential)
		if err != nil {
			t.Fatal(err)
		}
		ds[i] = device.Device
		if _, err = repository.NewPgE2EEPreKeyRepository(db).Publish(t.Context(), ids[i], testutil.PublicPreKeyUpload(t, ds[i].GenerationID)); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			f.token, f.uid = token, ids[i]
		}
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO user_settings(user_id)SELECT id FROM users ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status)VALUES($1,$2,'accepted')`, ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	chat, err := repository.NewPgE2EEConversationRepository(db).Create(t.Context(), f.uid, &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: ds[0].GenerationID}, CreationRequestId: uuid.NewString(), PeerUserId: ids[1]})
	if err != nil {
		t.Fatal(err)
	}
	f.intent = blobHTTPIntent(f.data)
	f.intent.Reservation.Context.DeviceGenerationId = ds[0].GenerationID
	f.intent.Reservation.ConversationID = chat.ID
	f.intent.Reservation.Sender = &pb.DeviceRef{UserId: ds[0].UserUUID, GenerationId: ds[0].GenerationID, DeviceId: 1}
	f.intent.Reservation.Recipient = &pb.DeviceRef{UserId: ds[1].UserUUID, GenerationId: ds[1].GenerationID, DeviceId: 1}
	f.repo = repository.NewPgE2EEBlobRepository(db)
	if err = f.repo.Configure(t.Context(), domain.E2EEBlobLimits{GlobalBytes: 16 << 20, AccountBytes: 8 << 20, GlobalClaims: 16, AccountClaims: 8, GlobalAttempts: 32, AccountAttempts: 16}); err != nil {
		t.Fatal(err)
	}
	grant, err := f.repo.Reserve(t.Context(), f.uid, f.intent.Reservation)
	if err != nil {
		t.Fatal(err)
	}
	f.intent.AttemptID, f.intent.Revision = grant.AttemptID, grant.Revision
	f.path = t.TempDir()
	if err = os.Chmod(f.path, 0700); err != nil {
		t.Fatal(err)
	}
	files, err := blobstore.Open(f.path, blobstore.Config{MaxConcurrent: 2, HeadroomBytes: 1, WriteTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var writer service.E2EEBlobWriterRepository = f.repo
	if len(wrappers) > 0 {
		writer = wrappers[0](f.repo)
	}
	f.upload, err = service.NewE2EEBlobUploadService(writer, files, auth, devices, service.E2EEBlobUploadConfig{MaxUploads: 1, Timeout: 3 * time.Second, RecheckInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := f.upload.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	startup := service.E2EEBlobStartupConfig{MaxAttempts: 64, MaxArtifacts: 128, MaxVerifyBytes: 1 << 20, Timeout: 3 * time.Second}
	if _, err = f.upload.Reconcile(t.Context(), startup); err != nil {
		t.Fatal(err)
	}
	// Register ordinary storage first, then join the same real-filesystem budget.
	m := storageAt(t, t.TempDir())
	f.budget = m.Budget()
	if err = f.upload.AttachBudget(t.Context(), f.budget, startup); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *blobHTTPFixture) state(t *testing.T) string {
	t.Helper()
	var state string
	if err := f.db.QueryRowContext(t.Context(), `SELECT state FROM e2ee_blob_attempts WHERE attempt_id=$1`, f.intent.AttemptID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	return state
}
func (f *blobHTTPFixture) request(t *testing.T, url string, data []byte) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodPut, url+"/api/e2ee/blobs/"+f.intent.AttemptID, bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	setBlobHTTPHeaders(t, r, f.intent, f.token, f.credential)
	return r
}
func (f *blobHTTPFixture) cleanup(t *testing.T) {
	t.Helper()
	req := &domain.E2EEBlobCleanupRequest{MessageID: f.intent.Reservation.MessageID, AttemptID: f.intent.AttemptID, Revision: f.intent.Revision, Reason: domain.E2EEBlobCleanupFailedWriter}
	if err := f.db.QueryRowContext(t.Context(), `SELECT writer_id::text FROM e2ee_blob_attempts WHERE attempt_id=$1`, req.AttemptID).Scan(&req.ExpectedWriterID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.upload.Cleanup(t.Context(), req); err != nil {
		t.Fatal(err)
	}
}
func (f *blobHTTPFixture) waitPart(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(f.path, f.intent.AttemptID+".part")); err == nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("HTTP body did not reach owned writer")
}
func (f *blobHTTPFixture) openIncomplete(t *testing.T, s *httptest.Server) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", s.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	r := f.request(t, s.URL, f.data)
	_, err = fmt.Fprintf(conn, "PUT %s HTTP/1.1\r\nHost: test\r\nContent-Length: %d\r\n", r.URL.Path, len(f.data))
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Header.Write(conn); err != nil {
		t.Fatal(err)
	}
	if _, err = io.WriteString(conn, "\r\n"); err != nil {
		t.Fatal(err)
	}
	return conn
}

func TestBlobHTTPActualPostgresReadyAndReplayWithoutOverwrite(t *testing.T) {
	f := blobHTTPPostgres(t)
	base, _ := f.budget.Usage()
	s := httptest.NewServer(blobHTTPMux(f.upload))
	defer s.Close()
	for i, want := range []int{201, 409} {
		resp, err := s.Client().Do(f.request(t, s.URL, f.data))
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != want {
			t.Fatal(i, resp.StatusCode, string(body), err)
		}
		if i == 0 {
			var result map[string]any
			if err = json.Unmarshal(body, &result); err != nil || result["state"] != "ready" || result["revision"] != strconv.FormatUint(f.intent.Revision, 10) {
				t.Fatal(string(body), err)
			}
		}
	}
	if f.state(t) != "ready" {
		t.Fatal("replay changed committed Ready")
	}
	got, err := os.ReadFile(filepath.Join(f.path, f.intent.AttemptID+".blob"))
	if err != nil || !bytes.Equal(got, f.data) {
		t.Fatal("ciphertext overwritten", err)
	}
	if used, n := f.budget.Usage(); used.Bytes <= base.Bytes || used.Entries != base.Entries+1 || n != 0 {
		t.Fatal(used, base, n)
	}
}

func TestBlobHTTPActualPostgresQuotaRepliesBeforeBody(t *testing.T) {
	f := blobHTTPPostgres(t)
	held, err := f.budget.Reserve(t.Context(), f.uid, storagebudget.Usage{Bytes: 10_000_000_000, Entries: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer held.Settle(storagebudget.Usage{})
	s := httptest.NewServer(blobHTTPMux(f.upload))
	defer s.Close()
	conn := f.openIncomplete(t, s)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 507 {
		t.Fatal(resp.StatusCode)
	}
	if _, err = os.Stat(filepath.Join(f.path, f.intent.AttemptID+".part")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("file created without capacity", err)
	}
	f.cleanup(t)
	if _, n := f.budget.Usage(); n != 1 {
		t.Fatal("cleanup consumed other reservation", n)
	}
}

func TestBlobHTTPActualPostgresLogoutInterruptsBlockedSocket(t *testing.T) {
	f := blobHTTPPostgres(t)
	base, _ := f.budget.Usage()
	s := httptest.NewServer(blobHTTPMux(f.upload))
	defer s.Close()
	conn := f.openIncomplete(t, s)
	f.waitPart(t)
	// No SQL/user/device/conversation/attempt lock may span the body wait.
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	tx, err := f.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, q := range []string{`SELECT id FROM users ORDER BY id FOR UPDATE`, `SELECT user_id FROM e2ee_devices ORDER BY user_id FOR UPDATE`, `SELECT conversation_id FROM e2ee_conversations FOR UPDATE`, `SELECT message_id FROM e2ee_message_claims FOR UPDATE`, `SELECT attempt_id FROM e2ee_blob_attempts FOR UPDATE`} {
		if _, err = tx.ExecContext(ctx, q); err != nil {
			t.Fatal("stream held SQL lock", err)
		}
	}
	tx.Rollback()
	logout, cancelLogout := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancelLogout()
	if err = f.auth.Logout(logout, f.token, false, func(int64, string) {}); err != nil {
		t.Fatal("blocked body prevented logout", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 401 {
		t.Fatal(resp.StatusCode)
	}
	if u, n := f.budget.Usage(); n != 0 || u.Bytes <= base.Bytes {
		t.Fatal("failed source lost charge/slot", u, n)
	}
	f.cleanup(t)
	if u, _ := f.budget.Usage(); u != base {
		t.Fatal("durable cleanup failed to restore budget", u, base)
	}
}

func TestBlobHTTPActualPostgresDisconnectAndShutdown(t *testing.T) {
	for _, kind := range []string{"disconnect", "root cancellation", "device replacement"} {
		t.Run(kind, func(t *testing.T) {
			f := blobHTTPPostgres(t)
			root, cancel := context.WithCancel(t.Context())
			defer cancel()
			s := httptest.NewUnstartedServer(blobHTTPMux(f.upload))
			s.Config.BaseContext = func(net.Listener) context.Context { return root }
			s.Start()
			defer s.Close()
			conn := f.openIncomplete(t, s)
			f.waitPart(t)
			switch kind {
			case "disconnect":
				conn.Close()
			case "root cancellation":
				cancel()
			case "device replacement":
				key := bytes.Repeat([]byte{44}, 33)
				key[0] = 5
				raw := bytes.Repeat([]byte{51}, 32)
				hash := service.E2EEDeviceCredentialHash(raw)
				ctx, stop := context.WithTimeout(t.Context(), 2*time.Second)
				defer stop()
				if _, err := f.devices.Replace(ctx, f.uid, domain.E2EEDeviceRegistration{RequestID: uuid.NewString(), ExpectedGenerationID: f.intent.Reservation.Context.DeviceGenerationId, RegistrationID: 2, IdentityKey: key, CredentialHash: hash[:]}, "Synthetic-HTTP-blob-password", raw); err != nil {
					t.Fatal("replacement blocked by HTTP body", err)
				}
			}
			if kind != "disconnect" {
				resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
				if err != nil {
					t.Fatal(err)
				}
				resp.Body.Close()
				want := 408
				if kind == "device replacement" {
					want = 401
				}
				if resp.StatusCode != want {
					t.Fatal(resp.StatusCode, want)
				}
			}
			ctx, stop := context.WithTimeout(t.Context(), 2*time.Second)
			defer stop()
			if err := f.upload.CancelUpload(ctx, f.intent.AttemptID); err != nil {
				t.Fatal("producer did not join", err)
			}
			if f.state(t) == "ready" {
				t.Fatal("incomplete socket became Ready")
			}
			if _, n := f.budget.Usage(); n != 0 {
				t.Fatal("leaked common stream", n)
			}
		})
	}
}

func TestBlobHTTPActualPostgresStalledBodyTimesOut(t *testing.T) {
	f := blobHTTPPostgres(t)
	s := httptest.NewServer(blobHTTPMux(f.upload))
	defer s.Close()
	conn := f.openIncomplete(t, s)
	f.waitPart(t)
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 408 || f.state(t) != "writing" {
		t.Fatal("stalled socket was not stopped by coordinator deadline", resp.StatusCode)
	}
	if _, n := f.budget.Usage(); n != 0 {
		t.Fatal("timed out body retained active stream", n)
	}
	f.cleanup(t)
}

type blobHTTPUnknownCommit struct {
	*repository.PgE2EEBlobRepository
	phase string
}

var errBlobHTTPUnknown = errors.New("synthetic lost reply after actual PG COMMIT; private internal details")

func (m *blobHTTPUnknownCommit) BeginWrite(ctx context.Context, uid int64, r *domain.E2EEBlobWriteRequest) (*domain.E2EEBlobWriteLease, error) {
	v, err := m.PgE2EEBlobRepository.BeginWrite(ctx, uid, r)
	if err == nil && m.phase == "begin" {
		return nil, errBlobHTTPUnknown
	}
	return v, err
}
func (m *blobHTTPUnknownCommit) CompleteWrite(ctx context.Context, uid int64, r *domain.E2EEBlobWriteRequest, w *blobstore.Written) (*domain.E2EEBlobWriteLease, error) {
	v, err := m.PgE2EEBlobRepository.CompleteWrite(ctx, uid, r, w)
	if err == nil && m.phase == "ready" {
		return nil, errBlobHTTPUnknown
	}
	return v, err
}
func TestBlobHTTPActualPostgresUnknownCommitAndExactLookup(t *testing.T) {
	for _, phase := range []string{"begin", "ready"} {
		t.Run(phase, func(t *testing.T) {
			f := blobHTTPPostgres(t, func(r *repository.PgE2EEBlobRepository) service.E2EEBlobWriterRepository {
				return &blobHTTPUnknownCommit{r, phase}
			})
			base, _ := f.budget.Usage()
			original, err := f.repo.Reserve(t.Context(), f.uid, f.intent.Reservation)
			if err != nil {
				t.Fatal(err)
			}
			s := httptest.NewServer(blobHTTPMux(f.upload))
			defer s.Close()
			resp, err := s.Client().Do(f.request(t, s.URL, f.data))
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 503 || bytes.Contains(body, []byte("private internal")) {
				t.Fatal(resp.StatusCode, string(body), err)
			}
			lookup, err := f.repo.Reserve(t.Context(), f.uid, f.intent.Reservation)
			if err != nil || lookup.AttemptID != original.AttemptID || lookup.Revision != original.Revision || !lookup.ExpiresAt.Equal(original.ExpiresAt) {
				t.Fatal("observation changed grant/TTL", lookup, err)
			}
			if phase == "ready" {
				if lookup.State != "ready" {
					t.Fatal(lookup)
				}
				got, err := os.ReadFile(filepath.Join(f.path, f.intent.AttemptID+".blob"))
				if err != nil || !bytes.Equal(got, f.data) {
					t.Fatal("unknown Ready lost ciphertext", err)
				}
				if u, n := f.budget.Usage(); u.Bytes <= base.Bytes || u.Entries != base.Entries+1 || n != 0 {
					t.Fatal("unknown Ready lost charge", u, n)
				}
			} else {
				if lookup.State != "writing" {
					t.Fatal(lookup)
				}
				if u, n := f.budget.Usage(); u != base || n != 0 {
					t.Fatal("unstarted body changed physical budget", u, base, n)
				}
				f.cleanup(t)
			}
		})
	}
}

func TestBlobHTTPActualPostgresHTTP2Revocation(t *testing.T) {
	f := blobHTTPPostgres(t)
	s := httptest.NewUnstartedServer(blobHTTPMux(f.upload))
	s.EnableHTTP2 = true
	s.StartTLS()
	defer s.Close()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	r, err := http.NewRequestWithContext(ctx, http.MethodPut, s.URL+"/api/e2ee/blobs/"+f.intent.AttemptID, reader)
	if err != nil {
		t.Fatal(err)
	}
	r.ContentLength = int64(len(f.data))
	setBlobHTTPHeaders(t, r, f.intent, f.token, f.credential)
	type result struct {
		response *http.Response
		err      error
	}
	done := make(chan result, 1)
	go func() { resp, err := s.Client().Do(r); done <- result{resp, err} }()
	f.waitPart(t)
	logout, stop := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer stop()
	if err = f.auth.Logout(logout, f.token, false, func(int64, string) {}); err != nil {
		t.Fatal(err)
	}
	select {
	case v := <-done:
		if v.err != nil {
			t.Fatal(v.err)
		}
		defer v.response.Body.Close()
		if v.response.ProtoMajor != 2 || v.response.StatusCode != 401 {
			t.Fatal(v.response.Proto, v.response.StatusCode)
		}
	case <-ctx.Done():
		t.Fatal("HTTP2 body remained blocked", ctx.Err())
	}
	join, stopJoin := context.WithTimeout(t.Context(), 2*time.Second)
	defer stopJoin()
	if err = f.upload.CancelUpload(join, f.intent.AttemptID); err != nil {
		t.Fatal(err)
	}
	f.cleanup(t)
}

func TestBlobHTTPActualPostgresRejectsChangedRefAndCorruptCiphertext(t *testing.T) {
	f := blobHTTPPostgres(t)
	base, _ := f.budget.Usage()
	s := httptest.NewServer(blobHTTPMux(f.upload))
	defer s.Close()
	for _, kind := range []string{"operation", "attempt", "reference", "credential"} {
		t.Run(kind, func(t *testing.T) {
			attempt := *f.intent
			reservation := *attempt.Reservation
			attempt.Reservation = &reservation
			reservation.Blob = proto.Clone(reservation.Blob).(*v2.BlobRef)
			credential := f.credential
			switch kind {
			case "operation":
				reservation.OperationID = uuid.NewString()
			case "attempt":
				attempt.AttemptID = uuid.NewString()
			case "reference":
				reservation.Blob.BlobId = uuid.NewString()
			case "credential":
				credential = bytes.Repeat([]byte{81}, 32)
			}
			r, err := http.NewRequest(http.MethodPut, s.URL+"/api/e2ee/blobs/"+attempt.AttemptID, bytes.NewReader(f.data))
			if err != nil {
				t.Fatal(err)
			}
			setBlobHTTPHeaders(t, r, &attempt, f.token, credential)
			resp, err := s.Client().Do(r)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			want := 409
			if kind == "credential" {
				want = 401
			}
			if resp.StatusCode != want {
				t.Fatal(resp.StatusCode, want)
			}
			if f.state(t) != "reserved" {
				t.Fatal("forged binding changed attempt")
			}
			if u, n := f.budget.Usage(); u != base || n != 0 {
				t.Fatal("forged binding consumed storage", u, n)
			}
		})
	}
	corrupt := bytes.Clone(f.data)
	corrupt[0] ^= 1
	resp, err := s.Client().Do(f.request(t, s.URL, corrupt))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 400 || f.state(t) != "writing" {
		t.Fatal("digest mismatch became Ready", resp.StatusCode)
	}
	if u, n := f.budget.Usage(); u.Bytes <= base.Bytes || n != 0 {
		t.Fatal("corrupt write lost charge", u, n)
	}
	f.cleanup(t)
}
