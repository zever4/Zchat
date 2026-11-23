//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"errors"
	"hash"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	internalGrpc "github.com/zever4/Zchat/internal/grpc"
	"github.com/zever4/Zchat/internal/pkg/postgres"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const fileRetryID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2"
const fileBaseID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
const fileSealedID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3"

// Real production graph, including startBlobStorage/accounting/collector and
// its attachment Send admission. No synthetic Ready/Signal plan/receipt. Only
// the test schema's clock and post-COMMIT network outcomes are controlled.
func TestE2EEOwnerFileInteropPostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN for owned native file acceptance")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("Rust test executable must be absolute")
	}
	db, realClockDB := fileInteropClock(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, "synthetic-owner-file-jwt-secret-atleast32bytes")
	var tokens []string
	var ids []int64
	for _, name := range []string{"file-alice", "file-bob"} {
		token, err := auth.Register(t.Context(), name, name+"@file-interop.invalid", "Synthetic-owner-file-password")
		if err != nil {
			t.Fatal(err)
		}
		claims, err := auth.ParseToken(token)
		if err != nil {
			t.Fatal(err)
		}
		tokens = append(tokens, token)
		ids = append(ids, claims.UserID)
	}
	if _, err := db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')`, ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	cfg, web, ordinary := blobRuntimeDirectories(t)
	cfg.Files.HeadroomBytes = 1
	cfg.Files.WriteTimeout, cfg.Upload.Timeout = 5*time.Second, 5*time.Second
	cfg.Collector.Interval, cfg.Collector.RetryBase = 50*time.Millisecond, 50*time.Millisecond
	blobs, err := startBlobStorage(t.Context(), cfg, web, ordinary.Budget(), repository.NewPgE2EEBlobRepository(db), auth, devices, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := blobs.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	dir := t.TempDir()
	f := &fileInteropFaults{db: db, marker: filepath.Join(dir, "committed"), sends: map[string][][]byte{}, puts: map[string][][32]byte{}, gets: map[string][][32]byte{}, fileAckRequests: map[string][][]byte{}, fileAckTimes: map[string]int64{}}
	cert, ca := fileInteropCertificate(t)
	mux := http.NewServeMux()
	blobs.Register(mux)
	httpServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.http(w, r, mux) }))
	httpServer.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	httpServer.StartTLS()
	defer httpServer.Close()
	// Only storage/Send use the accelerated clock. Keep key acquisition's
	// absolute deadline on real time, so native clock-skew protection stays real.
	keys := repository.NewPgE2EEPreKeyRepository(realClockDB)
	epochService := service.NewE2EEEpochService(repository.NewPgE2EEDeliveryRepository(db), keys, repository.NewPgE2EERecoveryRepository(db)).WithAttachmentAdmission(blobs)
	epoch := internalGrpc.NewE2EEEpochServer(epochService).WithBlobControls(blobs)
	registry := internalGrpc.NewE2EEDeviceServer(devices, service.NewE2EEPreKeyService(keys), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(db)), nil)
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	guard := internalGrpc.NewE2EEAuth(root, auth, devices)
	devices.SetDeviceReplacer(guard.DisconnectGeneration)
	server := grpcgo.NewServer(grpcgo.WaitForHandlers(true), grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})), grpcgo.MaxRecvMsgSize(internalGrpc.MaxRequestSize), grpcgo.ChainUnaryInterceptor(guard.Unary(internalGrpc.NewUserGRPCServer(users, auth).AuthInterceptor), f.rpc))
	internalGrpc.RegisterOptionalEpochMessenger(server, "true", func() (*internalGrpc.E2EEDeviceServer, *internalGrpc.E2EEEpochServer) { return registry, epoch })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); <-done }()
	caPath := filepath.Join(dir, "ca.pem")
	if err = os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	marker := f.marker
	if os.Getenv("ZCHAT_RUST_E2EE_TEST_WINDOWS") == "1" {
		for _, p := range []*string{&caPath, &marker} {
			converted, err := exec.Command("wslpath", "-w", *p).Output()
			if err != nil {
				t.Fatal("convert fixture path", err)
			}
			*p = strings.TrimSpace(string(converted))
		}
	}
	ctx, stop := context.WithTimeout(t.Context(), 120*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::file_tests::go_account_file_worker", "--nocapture")
	// Disposable credentials only through stdin; no args/env/verbose wire logs.
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), httpServer.URL, caPath, tokens[0], tokens[1], strconv.FormatInt(ids[1], 10), marker}, "\n") + "\n")
	gcDone := make(chan error, 1)
	gcJoined := false
	go func() { gcDone <- markOwnerFilesReclaimed(ctx, db, f.marker) }()
	defer func() {
		stop()
		if !gcJoined {
			<-gcDone
		}
	}()
	output, err := command.CombinedOutput()
	if err != nil || !bytes.Contains(output, []byte("zchat-native-account-file-complete:v8-owned-file-gc")) || !bytes.Contains(output, []byte("1 passed; 0 failed")) {
		t.Logf("fault progress: clock=%v reserve=%v HTTP=%v observe=%v Send=%v ACK=%v fence=%v", f.advanced.Load(), f.lostReserve.Load(), f.lostHTTP.Load(), f.lostObserve.Load(), f.lostSend.Load(), f.lostACK.Load(), f.sealed.Load())
		f.mu.Lock()
		t.Log("acquisition method/epoch/status only:", f.acquisitions)
		t.Log("file progress: downloaded, ACK-requested, late-ACK, lost-ACK", len(f.gets), len(f.fileAckRequests), f.lateFileAck.Load(), f.lostFileAck.Load())
		f.mu.Unlock()
		t.Fatalf("native owned file acceptance: %v\n%s", err, output)
	}
	gcErr := <-gcDone
	gcJoined = true
	if gcErr != nil {
		t.Fatal("accepted GC observation", gcErr)
	}
	for name, ok := range map[string]bool{"clock": f.advanced.Load(), "reserve": f.lostReserve.Load(), "HTTP": f.lostHTTP.Load(), "observe": f.lostObserve.Load(), "Send": f.lostSend.Load(), "ACK": f.lostACK.Load(), "signed fence": f.sealed.Load(), "late file ACK": f.lateFileAck.Load(), "lost file ACK": f.lostFileAck.Load()} {
		if !ok {
			t.Fatal("fault was not exercised:", name)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.lostFileStatus.Load() || len(f.fileStatusRequests) != 3 || f.fileStatusMismatch.Load() || f.fileStatusAttempts[fileBaseID] < 2 {
		t.Fatal("status read loss/retry not exercised with identical request")
	}
	for id, attempts := range f.sends {
		for _, wire := range attempts[1:] {
			if !bytes.Equal(wire, attempts[0]) {
				t.Fatal("Send bytes changed", id)
			}
		}
	}
	if len(f.fileAckRequests) != 3 || f.fileAckMismatch.Load() {
		t.Fatal("file ACK facts drifted")
	}
	for id, requests := range f.fileAckRequests {
		if id == fileBaseID && len(requests) < 2 {
			t.Fatal("lost committed FILEACK was not replayed after owner restart")
		}
		for _, request := range requests[1:] {
			if !bytes.Equal(request, requests[0]) {
				t.Fatal("file ACK retry changed", id)
			}
		}
		var at time.Time
		if err := db.QueryRowContext(t.Context(), `SELECT acknowledged_at FROM e2ee_file_acknowledgements WHERE message_id=$1`, id).Scan(&at); err != nil || at.UnixMilli() != f.fileAckTimes[id] {
			t.Fatal("first ACK time changed", err)
		}
		var request v2.AcknowledgeFilesRequest
		if err := proto.Unmarshal(requests[0], &request); err != nil {
			t.Fatal(err)
		}
		var storage string
		if err := db.QueryRowContext(t.Context(), `SELECT a.storage_id::text FROM e2ee_message_blobs l JOIN e2ee_blob_attempts a ON a.attempt_id=l.attempt_id WHERE l.message_id=$1`, id).Scan(&storage); err != nil {
			t.Fatal(err)
		}
		// Unshifted DB clock: denial comes from FILEACK, not injected expiry.
		item := request.Files[0]
		if target, err := repository.NewPgE2EEBlobRepository(realClockDB).ResolveRead(t.Context(), ids[1], storage, &domain.E2EEBlobReadRequest{Context: request.Context, Binding: item.Binding, Blob: item.Blob}); target != nil || !errors.Is(err, domain.ErrE2EEBlobReadUnavailable) {
			t.Fatal("read after FILEACK", target, err)
		}
	}
	var fileAcks int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_file_acknowledgements`).Scan(&fileAcks); err != nil || fileAcks != 3 {
		t.Fatal(fileAcks, err)
	}
	if len(f.puts[fileRetryID]) != 2 || f.puts[fileRetryID][0] != f.puts[fileRetryID][1] {
		t.Fatal("retained transfer changed/duplicated ciphertext")
	}
	if f.bodyMismatch.Load() {
		t.Fatal("transferred ciphertext differs from the exact public reference")
	}
	if len(f.gets) != 3 {
		t.Fatal("missing native cache downloads", len(f.gets))
	}
	for _, id := range []string{"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1", fileRetryID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa4"} {
		if len(f.gets[id]) != 1 {
			t.Fatal("cache reuse/reopen repeated GET or missed file", id, len(f.gets[id]))
		}
	}
	var linked, receipts, acknowledged, recoveries, epochs, claims, attempts int
	err = db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_message_blobs),(SELECT count(*) FROM e2ee_send_receipts WHERE wire_version=3),(SELECT count(*) FROM e2ee_message_acknowledgements a JOIN e2ee_send_receipts r USING(message_id) WHERE r.wire_version=3),(SELECT count(*) FROM e2ee_epoch_recoveries),(SELECT count(*) FROM e2ee_conversation_epochs),(SELECT count(*) FROM e2ee_message_claims WHERE wire_version=3),(SELECT count(*) FROM e2ee_blob_attempts)`).Scan(&linked, &receipts, &acknowledged, &recoveries, &epochs, &claims, &attempts)
	if err != nil || linked != 3 || receipts != 3 || acknowledged != 3 || recoveries != 1 || epochs != 2 || claims != 4 || attempts != 5 {
		t.Fatal("durable facts drifted", linked, receipts, acknowledged, recoveries, epochs, claims, attempts, err)
	}
	rows, err := db.QueryContext(t.Context(), `SELECT a.attempt_id::text,a.state,a.deletion_reason,a.reclaimed_at FROM e2ee_message_blobs l JOIN e2ee_blob_attempts a USING(attempt_id)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, state, reason string
		var reclaimed time.Time
		if err = rows.Scan(&id, &state, &reason, &reclaimed); err != nil {
			t.Fatal(err)
		}
		if state != "reclaimed" || reclaimed.UnixMilli() <= 0 || reason != domain.E2EEBlobReclaimFileAcknowledged && reason != domain.E2EEBlobReclaimAcceptedExpired {
			t.Fatal("invalid immutable accepted removal fact", state, reason, reclaimed)
		}
		for _, suffix := range []string{".part", ".blob"} {
			if _, err := os.Lstat(filepath.Join(cfg.Root, id+suffix)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("server ciphertext retained after automatic GC", id, suffix, err)
			}
		}
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Log("actual owner upload/download/automatic FILEACK +native sender statuses over TLS Go/PG/FS:3file receipts/descriptor ACKs,3GET/cache commits+reopen,3late FILEACK facts; automatic physical GC/queue drain/logical bytes0 with immutable links/receipts/ACK/removal retained; lost replies and signed recovery0->1 preserved; GUI pending")
}

// One owned TEST observer, not a per-file runtime task. The native helper waits
// for this marker before reopening its cache without the server's ciphertext.
func markOwnerFilesReclaimed(ctx context.Context, db *sql.DB, marker string) error {
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		if _, err := os.Stat(marker + ".files-cached"); err == nil {
			var selected, pending int
			var charged int64
			err := db.QueryRowContext(ctx, `SELECT
                (SELECT count(*) FROM e2ee_message_blobs b JOIN e2ee_blob_attempts a USING(attempt_id) WHERE a.state='reclaimed'),
                (SELECT count(*) FROM e2ee_accepted_blob_queue),
                (SELECT reserved_bytes FROM e2ee_blob_global_quota)`).Scan(&selected, &pending, &charged)
			if err != nil {
				return err
			}
			if selected == 3 && pending == 0 && charged == 0 {
				return os.WriteFile(marker+".server-gc", []byte("reclaimed"), 0600)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

type fileInteropFaults struct {
	lateFileAck, lostFileAck, fileAckMismatch                               atomic.Bool
	fileAckRequests                                                         map[string][][]byte
	fileAckTimes                                                            map[string]int64
	lostFileStatus                                                          atomic.Bool
	fileStatusRequests                                                      map[string][]byte
	fileStatusAttempts                                                      map[string]uint64
	fileStatusMismatch                                                      atomic.Bool
	acquisitions                                                            []string
	bodyMismatch                                                            atomic.Bool
	stepMu                                                                  sync.Mutex // Test-only barrier between Ready PUT and its injected Observe fault.
	db                                                                      *sql.DB
	marker                                                                  string
	advanced, lostReserve, lostHTTP, lostObserve, lostSend, lostACK, sealed atomic.Bool
	mu                                                                      sync.Mutex
	sends                                                                   map[string][][]byte
	puts                                                                    map[string][][32]byte
	gets                                                                    map[string][][32]byte
	captured                                                                int
}

func (f *fileInteropFaults) rpc(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, next grpcgo.UnaryHandler) (any, error) {
	if info.FullMethod == v2.Messenger_GetFileStatuses_FullMethodName {
		r := req.(*v2.GetFileStatusesRequest)
		if len(r.Files) != 1 || r.Files[0].Binding == nil {
			return nil, status.Error(codes.InvalidArgument, "fixture expects one owner file per turn")
		}
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
		if err != nil {
			return nil, err
		}
		id := r.Files[0].Binding.MessageId
		f.mu.Lock()
		if f.fileStatusRequests == nil {
			f.fileStatusRequests = make(map[string][]byte)
			f.fileStatusAttempts = make(map[string]uint64)
		}
		// Constant capture memory despite repeated pending observations. Every
		// request, including repeats across owner restarts, must remain exact.
		old, exists := f.fileStatusRequests[id]
		if !exists && len(f.fileStatusRequests) >= 3 {
			f.mu.Unlock()
			return nil, status.Error(codes.ResourceExhausted, "fixture status reference bound")
		}
		if exists && !bytes.Equal(old, wire) {
			f.fileStatusMismatch.Store(true)
		}
		if !exists {
			f.fileStatusRequests[id] = wire
		}
		f.fileStatusAttempts[id]++
		f.mu.Unlock()
	}
	if info.FullMethod == v2.Messenger_AcknowledgeFiles_FullMethodName {
		r := req.(*v2.AcknowledgeFilesRequest)
		// Automatic ACK can race the next explicit GET. Expire only once ALL
		// files are safely cached; then keep BASE's committed reply unknown
		// until the native owner has restarted, without rewriting its fact.
		if _, err := os.Stat(f.marker + ".files-cached"); err != nil {
			return nil, status.Error(codes.Unavailable, "fixture holds FILEACK until all GETs complete")
		}
		if r.Files[0].Binding.MessageId == fileBaseID && f.lostFileAck.Load() {
			if _, err := os.Stat(f.marker + ".ack-resume"); err != nil {
				return nil, status.Error(codes.Unavailable, "fixture keeps FILEACK result unknown until owner restart")
			}
		}
		if f.lateFileAck.CompareAndSwap(false, true) {
			if _, err := f.db.ExecContext(ctx, `UPDATE owner_file_clock SET shift=INTERVAL '724 hours'`); err != nil {
				return nil, status.Error(codes.Internal, "fixture ACK clock failed")
			}
		}
		var expired bool
		if err := f.db.QueryRowContext(ctx, `SELECT clock_timestamp()>=expires_at FROM e2ee_send_receipts WHERE message_id=$1`, r.Files[0].Binding.MessageId).Scan(&expired); err != nil || !expired {
			return nil, status.Error(codes.Internal, "fixture ACK not late")
		}
		wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
		if err != nil {
			return nil, err
		}
		f.mu.Lock()
		if len(f.fileAckRequests[r.Files[0].Binding.MessageId]) >= 4 {
			f.mu.Unlock()
			return nil, status.Error(codes.ResourceExhausted, "fixture FILEACK capture limit")
		}
		f.fileAckRequests[r.Files[0].Binding.MessageId] = append(f.fileAckRequests[r.Files[0].Binding.MessageId], wire)
		f.mu.Unlock()
	}
	if info.FullMethod == v2.Messenger_SendMessage_FullMethodName {
		r := req.(*v2.SendMessageRequest)
		if r.Envelope.MessageId == fileRetryID {
			f.stepMu.Lock()
			defer f.stepMu.Unlock()
		}
		if r.Envelope.ProtocolVersion == 3 {
			wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(r)
			if err != nil {
				return nil, err
			}
			f.mu.Lock()
			if f.captured >= 128 {
				f.mu.Unlock()
				return nil, status.Error(codes.ResourceExhausted, "fixture capture limit")
			}
			f.captured++
			f.sends[r.Envelope.MessageId] = append(f.sends[r.Envelope.MessageId], wire)
			f.mu.Unlock()
			if r.Envelope.MessageId == fileRetryID {
				if f.advanced.CompareAndSwap(false, true) {
					if _, err = f.db.ExecContext(ctx, `UPDATE owner_file_clock SET shift=INTERVAL '2 hours'`); err != nil {
						return nil, status.Error(codes.Internal, "fixture clock failed")
					}
				}
				if f.lostReserve.Load() && !f.lostObserve.Load() {
					var ready bool
					if err = f.db.QueryRowContext(ctx, `SELECT a.state='ready' AND a.revision>1 FROM e2ee_blob_heads h JOIN e2ee_blob_attempts a USING(message_id,revision,attempt_id) WHERE h.message_id=$1`, fileRetryID).Scan(&ready); err != nil {
						return nil, status.Error(codes.Internal, "fixture staging lookup failed")
					}
					if ready {
						return nil, status.Error(codes.Unavailable, "fixture waiting for post-PUT observation fault")
					}
				}
				if f.lostSend.Load() {
					if _, err = os.Stat(f.marker + ".resume"); err != nil {
						return nil, status.Error(codes.Unavailable, "fixture retaining unknown Send until reopen")
					}
				}
			}
			if r.Envelope.MessageId == fileSealedID {
				var head int
				if err = f.db.QueryRowContext(ctx, `SELECT active_epoch FROM e2ee_conversations WHERE conversation_id=$1`, r.Envelope.ConversationId).Scan(&head); err != nil {
					return nil, status.Error(codes.Internal, "fixture head lookup failed")
				}
				if head == 0 {
					return nil, status.Error(codes.Unavailable, "fixture holds unaccepted old Send")
				}
			}
		}
	}
	value, err := next(ctx, req)
	if info.FullMethod == v2.Messenger_AcquireBundle_FullMethodName {
		f.mu.Lock()
		if len(f.acquisitions) < 32 {
			f.acquisitions = append(f.acquisitions, strconv.FormatUint(req.(*v2.AcquireBundleRequest).GetCryptoEpoch(), 10)+":"+status.Code(err).String())
		}
		f.mu.Unlock()
	}
	if info.FullMethod == v2.Messenger_SendMessage_FullMethodName && req.(*v2.SendMessageRequest).Envelope.MessageId == fileSealedID && err != nil {
		for _, detail := range status.Convert(err).Details() {
			if _, ok := detail.(*v2.EpochSealedFailure); ok {
				f.sealed.Store(true)
			}
		}
	}
	if err != nil {
		return value, err
	}
	switch info.FullMethod {
	case v2.Messenger_GetFileStatuses_FullMethodName:
		if f.lostFileStatus.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "fixture lost status read COMMIT reply")
		}
	case v2.Messenger_AcknowledgeFiles_FullMethodName:
		ack := value.(*v2.FileAcknowledgements).Files[0]
		id := ack.Acknowledgement.Binding.MessageId
		f.mu.Lock()
		old := f.fileAckTimes[id]
		if old != 0 && old != ack.AcknowledgedAtUnixMs {
			f.fileAckMismatch.Store(true)
		}
		f.fileAckTimes[id] = ack.AcknowledgedAtUnixMs
		f.mu.Unlock()
		// The exact claim is already committed. Keep the reply unknown locally.
		if id == fileBaseID && f.lostFileAck.CompareAndSwap(false, true) {
			if err := os.WriteFile(f.marker+".file-ack-committed", []byte("committed"), 0600); err != nil {
				return nil, status.Error(codes.Internal, "fixture FILEACK marker failed")
			}
			return nil, status.Error(codes.Unavailable, "fixture lost FILEACK COMMIT reply")
		}
	case v2.Messenger_ReserveBlob_FullMethodName:
		r := req.(*v2.BlobLeaseRequest)
		if r.MessageId == fileRetryID && r.ExpectedRevision > 0 && f.lostReserve.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "fixture lost Reserve COMMIT reply")
		}
	case v2.Messenger_ObserveBlob_FullMethodName:
		r := req.(*v2.BlobLeaseRequest)
		g := value.(*v2.BlobLease)
		if r.MessageId == fileRetryID && g.Revision > 1 && g.State == v2.BlobLeaseState_BLOB_LEASE_STATE_READY && f.lostObserve.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "fixture lost Ready observation")
		}
	case v2.Messenger_SendMessage_FullMethodName:
		if req.(*v2.SendMessageRequest).Envelope.MessageId == fileRetryID && f.lostSend.CompareAndSwap(false, true) {
			if err = os.WriteFile(f.marker, []byte("committed"), 0600); err != nil {
				return nil, status.Error(codes.Internal, "fixture marker failed")
			}
			return nil, status.Error(codes.Unavailable, "fixture lost Send COMMIT reply")
		}
	case v2.Messenger_AcknowledgeMessages_FullMethodName:
		for _, ack := range req.(*pb.AcknowledgeMessagesRequest).Messages {
			if ack.MessageId == "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1" && f.lostACK.CompareAndSwap(false, true) {
				return nil, status.Error(codes.Unavailable, "fixture lost file descriptor ACK COMMIT reply")
			}
		}
	}
	return value, nil
}

type fileResponse struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *fileResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *fileResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *fileResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if w.body.Len()+len(p) > 4096 {
		return 0, io.ErrShortWrite
	}
	return w.body.Write(p)
}

type fileBody struct {
	io.ReadCloser
	sum   hash.Hash
	bytes int64
}

func (b *fileBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	b.bytes += int64(n)
	_, _ = b.sum.Write(p[:n])
	return n, e
}

// Pass streaming writes/deadlines/flush through unchanged. Unlike the PUT fault
// wrapper this records ciphertext incrementally without buffering its body.
type fileReadResponse struct {
	http.ResponseWriter
	status int
	sum    hash.Hash
	bytes  int64
}

func (w *fileReadResponse) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *fileReadResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *fileReadResponse) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	n, err := w.ResponseWriter.Write(p)
	w.bytes += int64(n)
	_, _ = w.sum.Write(p[:n])
	return n, err
}
func (f *fileInteropFaults) http(w http.ResponseWriter, r *http.Request, next http.Handler) {
	if r.Method == http.MethodGet {
		a, err := boundary.DecodeBlobDownloadMetadata(r.Header.Get(boundary.BlobDownloadMetadataHeader), r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
		if err != nil {
			next.ServeHTTP(w, r)
			return
		}
		observed := &fileReadResponse{ResponseWriter: w, sum: sha256.New()}
		next.ServeHTTP(observed, r)
		if observed.status == http.StatusOK {
			digest := observed.sum.Sum(nil)
			if observed.bytes != int64(a.Blob.CiphertextBytes) || !bytes.Equal(digest, a.Blob.Sha256) {
				f.bodyMismatch.Store(true)
			}
			f.mu.Lock()
			f.gets[a.Binding.MessageId] = append(f.gets[a.Binding.MessageId], [32]byte(digest))
			f.mu.Unlock()
		}
		return
	}
	a, err := boundary.DecodeBlobUploadMetadata(r.Header.Get(boundary.BlobUploadMetadataHeader), r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:])
	if err != nil {
		next.ServeHTTP(w, r)
		return
	}
	if a.Reservation.MessageID == fileRetryID && a.Revision > 1 {
		f.stepMu.Lock()
		defer f.stepMu.Unlock()
	}
	b := &fileBody{ReadCloser: r.Body, sum: sha256.New()}
	r.Body = b
	buffered := &fileResponse{ResponseWriter: w}
	next.ServeHTTP(buffered, r)
	if buffered.status == http.StatusCreated {
		if b.bytes != int64(a.Reservation.Blob.CiphertextBytes) || !bytes.Equal(b.sum.Sum(nil), a.Reservation.Blob.Sha256) {
			f.bodyMismatch.Store(true)
		}
		f.mu.Lock()
		f.puts[a.Reservation.MessageID] = append(f.puts[a.Reservation.MessageID], [32]byte(b.sum.Sum(nil)))
		f.mu.Unlock()
		if a.Reservation.MessageID == fileRetryID && a.Revision > 1 && f.lostHTTP.CompareAndSwap(false, true) {
			connection, _, err := http.NewResponseController(w).Hijack()
			if err == nil {
				connection.Close()
				return
			}
		}
	}
	w.WriteHeader(buffered.status)
	_, _ = w.Write(buffered.body.Bytes())
}

// A shadow clock only in this disposable schema, before pg_catalog explicitly
// in THIS fixture pool's search_path. Real repository SQL/triggers execute;
// no deadlines/immutable rows/auth/Ready checks are patched or disabled.
func fileInteropClock(t *testing.T) (*sql.DB, *sql.DB) {
	t.Helper()
	owner := testutil.Postgres(t)
	var schema string
	if err := owner.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	_, err := owner.ExecContext(t.Context(), `CREATE TABLE owner_file_clock(shift interval NOT NULL); INSERT INTO owner_file_clock VALUES(INTERVAL '0 seconds');
        CREATE FUNCTION clock_timestamp() RETURNS timestamptz LANGUAGE sql VOLATILE AS $$ SELECT pg_catalog.clock_timestamp()+shift FROM owner_file_clock $$;
        CREATE FUNCTION statement_timestamp() RETURNS timestamptz LANGUAGE sql STABLE AS $$ SELECT pg_catalog.statement_timestamp()+shift FROM owner_file_clock $$`)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("ZCHAT_TEST_DATABASE_URL"))
	if err != nil || u.Scheme != "postgres" {
		t.Fatal("explicit PostgreSQL test URL required")
	}
	q := u.Query()
	q.Set("search_path", schema+",pg_catalog")
	u.RawQuery = q.Encode()
	db, err := postgres.Open(u.String())
	if err != nil {
		t.Fatal("fixture pool", err)
	}
	t.Cleanup(func() { db.Close() })
	// DEFAULT expressions bind function OIDs when migrations are parsed, before
	// this fixture's shadow clock exists. Rebind ONLY this disposable schema's
	// new FILEACK timestamps to the same accelerated storage clock. Do not
	// rewrite facts, disable guards or change production time validation.
	if _, err = db.ExecContext(t.Context(), `ALTER TABLE e2ee_file_acknowledgements ALTER COLUMN acknowledged_at SET DEFAULT date_trunc('milliseconds',clock_timestamp())`); err != nil {
		t.Fatal(err)
	}
	var delta float64
	if _, err = db.ExecContext(t.Context(), `UPDATE owner_file_clock SET shift=INTERVAL '2 hours'`); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(t.Context(), `SELECT EXTRACT(EPOCH FROM clock_timestamp()-pg_catalog.clock_timestamp())`).Scan(&delta); err != nil || delta < 7199 || delta > 7201 {
		t.Fatal("fixture clock resolution", delta, err)
	}
	if _, err = db.ExecContext(t.Context(), `UPDATE owner_file_clock SET shift=INTERVAL '0 seconds'`); err != nil {
		t.Fatal(err)
	}
	return db, owner
}

func fileInteropCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Disposable owner-file CA"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "Owner-file fixture"}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	serverDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &serverKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{serverDER}, PrivateKey: serverKey}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
