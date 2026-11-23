package grpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	grpcgo "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Opt-in, real TCP/TLS + Go auth/services + migrated PostgreSQL + the actual
// Rust processor unit-test binary. No production server or plaintext relay API.
func TestE2EERustNativePostgres(t *testing.T) {
	binary := os.Getenv("ZCHAT_RUST_E2EE_TEST_BIN")
	if binary == "" {
		t.Skip("set ZCHAT_RUST_E2EE_TEST_BIN to the native Rust e2ee unit-test binary")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("Rust test binary must be an explicit absolute path")
	}
	db := testutil.Postgres(t)
	users := repository.NewPgUserRepository(db)
	auth := service.NewAuthService(users, deviceTestJWTSecret)
	var tokens []string
	var ids []int64
	for _, name := range []string{"alice", "bob"} {
		token, err := auth.Register(t.Context(), name, name+"@rust-interop.invalid", deviceTestPassword)
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
	secondAliceToken, _, _, err := auth.Login(t.Context(), "alice@rust-interop.invalid", deviceTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	if secondAliceToken == tokens[0] {
		t.Fatal("login must issue a distinct session token")
	}
	if _, err := db.ExecContext(t.Context(), "INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')", ids[0], ids[1]); err != nil {
		t.Fatal(err)
	}
	devices := service.NewE2EEDeviceService(repository.NewPgE2EEDeviceRepository(db), auth)
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	guard := NewE2EEAuth(root, auth, devices)
	devices.SetDeviceReplacer(guard.DisconnectGeneration)
	implementation := newPrototypeDeviceServer(devices, service.NewE2EEPreKeyService(repository.NewPgE2EEPreKeyRepository(db)), service.NewE2EEConversationService(repository.NewPgE2EEConversationRepository(db)), service.NewE2EEMessageService(repository.NewPgE2EEDeliveryRepository(db)))
	certificate, ca := rustInteropCertificate(t)
	var deadlines atomic.Int64
	var discoveries, credentialOnDiscovery atomic.Int64
	var acquisitionCalls atomic.Int64
	var replacements, creations, sends atomic.Int64
	var inventoryCalls atomic.Int64
	var maintenanceLost atomic.Bool
	server := grpcgo.NewServer(grpcgo.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})),
		grpcgo.MaxRecvMsgSize(MaxRequestSize), grpcgo.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpcgo.UnaryServerInfo, handler grpcgo.UnaryHandler) (any, error) {
			deadline, ok := ctx.Deadline()
			if ok && time.Until(deadline) > 0 && time.Until(deadline) <= 11*time.Second {
				deadlines.Add(1)
			}
			if info.FullMethod == pb.Messenger_GetOwnDevice_FullMethodName {
				discoveries.Add(1)
				md, _ := metadata.FromIncomingContext(ctx)
				if len(md.Get(E2EEDeviceCredentialMetadata)) != 0 {
					credentialOnDiscovery.Add(1)
				}
			}
			if info.FullMethod == pb.Messenger_SendMessage_FullMethodName {
				sends.Add(1)
			}
			if info.FullMethod == pb.Messenger_CreateConversation_FullMethodName {
				creations.Add(1)
			}
			if info.FullMethod == pb.Messenger_ReplaceOwnDevice_FullMethodName {
				replacements.Add(1)
			}
			// Test-only expiry after two successful copies of the old snapshot.
			// Do not sleep24h or shorten production TTL. The third call must yield
			// a typed expiry proof; its keys stay consumed when a new ID is acquired.
			if info.FullMethod == pb.Messenger_AcquireBundle_FullMethodName && acquisitionCalls.Add(1) == 3 {
				r := req.(*pb.AcquireBundleRequest)
				result, err := db.ExecContext(ctx, `UPDATE e2ee_acquisitions SET created_at=statement_timestamp()-INTERVAL '25 hours',not_after=statement_timestamp()-INTERVAL '1 hour'
                    WHERE owner_generation_id=$1 AND acquisition_id=$2 AND conversation_id=$3`, r.Context.DeviceGenerationId, r.AcquisitionId, r.ConversationId)
				if err != nil {
					return nil, status.Error(codes.Internal, "test expiry setup failed")
				}
				changed, err := result.RowsAffected()
				if err != nil || changed != 1 {
					return nil, status.Error(codes.Internal, "test expiry target missing")
				}
			}
			if info.FullMethod == pb.Messenger_GetPreKeyInventory_FullMethodName {
				inventoryCalls.Add(1)
			}
			result, err := handler(ctx, req)
			if info.FullMethod == pb.Messenger_UploadPreKeys_FullMethodName && err == nil {
				upload := req.(*pb.UploadPreKeysRequest)
				if upload.ExpectedRevision == 2 && upload.SignedPreKey == nil && len(upload.OneTimePreKeys) > 0 && len(upload.OneTimePreKeys) <= 3 && len(upload.KyberPreKeys) > 0 && len(upload.KyberPreKeys) <= 3 && maintenanceLost.CompareAndSwap(false, true) {
					return nil, status.Error(codes.Unavailable, "test lost maintenance response after COMMIT")
				}
			}
			return result, err
		}, guard.Unary(NewUserGRPCServer(users, auth).AuthInterceptor)))
	RegisterOptionalMessenger(server, "true", func() pb.MessengerServer { return implementation })
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { server.Stop(); listener.Close(); <-done }()
	caPath := filepath.Join(t.TempDir(), "test-ca.pem")
	if err := os.WriteFile(caPath, ca, 0600); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("ZCHAT_RUST_E2EE_TEST_WINDOWS") == "1" {
		converted, err := exec.Command("wslpath", "-w", caPath).Output()
		if err != nil {
			t.Fatal("convert test CA path for native Windows Rust:", err)
		}
		caPath = strings.TrimSpace(string(converted))
	}
	ctx, stop := context.WithTimeout(t.Context(), 90*time.Second)
	defer stop()
	command := exec.CommandContext(ctx, binary, "--ignored", "--exact", "encryption::native::tests::go_interop_worker", "--nocapture")
	// Test-only JWTs go directly to stdin, never argv/env or fixture files.
	// No account UUID is supplied: the native client must discover it using JWT.
	command.Stdin = strings.NewReader(strings.Join([]string{"https://" + listener.Addr().String(), caPath, tokens[0], tokens[1], strconv.FormatInt(ids[1], 10), deviceTestPassword, secondAliceToken, strconv.FormatInt(ids[0], 10)}, "\n") + "\n")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Rust native interoperability: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "1 passed; 0 failed") || !strings.Contains(string(output), "zchat-native-interop-complete:v1") {
		t.Fatalf("Rust worker did not run: %s", output)
	}
	if deadlines.Load() == 0 {
		t.Fatal("Rust client did not propagate RPC deadlines")
	}
	if discoveries.Load() < 10 || credentialOnDiscovery.Load() != 0 {
		t.Fatalf("expected JWT-only account discovery: calls=%d callsWithDeviceCredential=%d", discoveries.Load(), credentialOnDiscovery.Load())
	}
	var registered, acquisitions int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_devices").Scan(&registered); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_acquisitions").Scan(&acquisitions); err != nil {
		t.Fatal(err)
	}
	if registered != 4 || acquisitions != 3 {
		t.Fatalf("retry allocated extra devices or bundles: devices=%d acquisitions=%d", registered, acquisitions)
	}
	if !strings.Contains(string(output), "zchat-native-refill-complete:v1") {
		t.Fatal("native refill recovery worker did not complete")
	}
	if !strings.Contains(string(output), "zchat-native-rotation-complete:v1") {
		t.Fatal("native rotation worker did not complete")
	}
	if !strings.Contains(string(output), "zchat-native-maintenance-complete:v1") || !maintenanceLost.Load() || inventoryCalls.Load() != 4 {
		t.Fatal("native owner inventory/maintenance fault probe incomplete", inventoryCalls.Load())
	}
	var availableEC, availablePQ, maintenanceBatches int
	if err := db.QueryRowContext(t.Context(), `SELECT
        (SELECT count(*) FROM e2ee_ec_prekeys WHERE generation_id=d.generation_id AND NOT used),
        (SELECT count(*) FROM e2ee_kyber_prekeys WHERE generation_id=d.generation_id AND NOT used AND NOT is_last_resort),
        (SELECT count(*) FROM e2ee_publication_batches WHERE generation_id=d.generation_id)
        FROM e2ee_devices d WHERE public_material_revision=3`).Scan(&availableEC, &availablePQ, &maintenanceBatches); err != nil || availableEC != 31 || availablePQ != 31 || maintenanceBatches != 3 {
		t.Fatal("automatic refill replay drift", availableEC, availablePQ, maintenanceBatches, err)
	}
	t.Log("native maintenance:4authenticated inventory RPCs, availableEC/PQ32/32 immediately after refill then31/31 after later acquisition,3publication batches; lost COMMIT response replayed without another plan or inventory")
	var refilled, refilledEC, refilledPQ, refillBatches, retainedSigned, activeSigned, activeLR int
	if err := db.QueryRowContext(t.Context(), `SELECT count(*),
        (SELECT count(*) FROM e2ee_ec_prekeys WHERE generation_id=d.generation_id),
        (SELECT count(*) FROM e2ee_kyber_prekeys WHERE generation_id=d.generation_id),
        (SELECT count(*) FROM e2ee_publication_batches WHERE generation_id=d.generation_id),
        (SELECT count(*) FROM e2ee_signed_prekeys WHERE generation_id=d.generation_id),
        d.active_signed_prekey_id,d.active_last_resort_id
        FROM e2ee_devices d WHERE public_material_revision=8 GROUP BY d.generation_id`).Scan(&refilled, &refilledEC, &refilledPQ, &refillBatches, &retainedSigned, &activeSigned, &activeLR); err != nil {
		t.Fatal("native refill durable PostgreSQL facts:", err)
	}
	if refilled != 1 || refilledEC != 38 || refilledPQ != 75 || refillBatches != 8 || retainedSigned != 3 || activeSigned != 3 || activeLR != 74 {
		t.Fatalf("publication replay drift: devices=%d EC=%d PQ=%d batches=%d signed=%d activeSigned=%d activeLR=%d", refilled, refilledEC, refilledPQ, refillBatches, retainedSigned, activeSigned, activeLR)
	}
	var retired, closed int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_devices WHERE retired_at IS NOT NULL").Scan(&retired); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_conversations WHERE closed_at IS NOT NULL").Scan(&closed); err != nil {
		t.Fatal(err)
	}
	if retired != 2 || closed != 2 || replacements.Load() != 5 {
		t.Fatalf("native replacements must retire exactly two devices/close two conversations without password replay on login: retired=%d closed=%d replacementRPCs=%d", retired, closed, replacements.Load())
	}
	var conversations, requests int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_conversations").Scan(&conversations); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_conversation_requests").Scan(&requests); err != nil {
		t.Fatal(err)
	}
	if conversations != 2 || requests != 4 || creations.Load() != 9 {
		t.Fatalf("creation retry/closure/CAS allocated unexpected chats: conversations=%d requests=%d createRPCs=%d", conversations, requests, creations.Load())
	}
	var usedEC, usedPQ int
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_ec_prekeys WHERE used").Scan(&usedEC); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM e2ee_kyber_prekeys WHERE used").Scan(&usedPQ); err != nil {
		t.Fatal(err)
	}
	if usedEC != 3 || usedPQ != 3 {
		t.Fatalf("exact upload replay reset consumed prekeys: EC=%d PQ=%d", usedEC, usedPQ)
	}
	var receipts, inbox, stored, rejected, removals, pending int
	if err := db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_send_receipts),(SELECT count(*) FROM e2ee_message_inbox),
        (SELECT count(*) FROM e2ee_message_acknowledgements WHERE disposition='stored'),(SELECT count(*) FROM e2ee_message_acknowledgements WHERE disposition='rejected'),(SELECT count(*) FROM e2ee_message_removals WHERE reason='acknowledged'),
        (SELECT count(*) FROM e2ee_delivery_state WHERE pending_messages<>0 OR pending_bytes<>0)`).Scan(&receipts, &inbox, &stored, &rejected, &removals, &pending); err != nil {
		t.Fatal(err)
	}
	if receipts != 6 || inbox != 0 || stored != 5 || rejected != 1 || removals != 6 || pending != 0 || sends.Load() != 10 {
		t.Fatalf("real inbox/ACK drift: receipts=%d inbox=%d stored=%d rejected=%d removals=%d nonempty counters=%d sendRPCs=%d", receipts, inbox, stored, rejected, removals, pending, sends.Load())
	}
	t.Logf("real TLS/Go/PostgreSQL/Rust native replacement/archive/durable creation/acquisition renewal/Signal reply/refill passed; original generation38EC/75PQ/3signed/8batches, activeSigned3/LR74; rotation lost receipt/login/CAS/late old proof preserved current pointers, multi-batch refill lost receipt recovered on JWT login plus explicit next refill CAS; %d JWT-only discoveries, %d RPC deadlines;9creationRPCs/4creationRequests/2conversations/5replacementRPCs/4devices/2retired/2closed/3acquisitions/usedEC3/usedPQ3; Send10RPC/6receipts (4real Signal text including accepted retired sender+1automatic Signal confirmation+1malformed fault probe); protected outbox receipt/status recovery; actual inbox RPC -> atomic decrypt+ACK journal COMMIT -> production opaque ACK replay -> sender Stored5/Rejected1;6removals/0server ciphertext/0pending counters; durable Get/Create closure convergence; late closed-chat receive via bounded native poll runner; runner sends automatic session confirmation and real reply/commits receipts and receives/ACKs through TLS; no test-local delivery relay or raw ACK probe", discoveries.Load(), deadlines.Load())
}

func rustInteropCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "Zchat disposable interoperability CA"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	// A CA certificate is not a valid end-entity certificate for rustls/webpki.
	// Sign a separate server leaf; do not weaken client certificate validation.
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: new(big.Int).Add(serial, big.NewInt(1)), Subject: pkix.Name{CommonName: "Zchat interoperability server"},
		NotBefore: template.NotBefore, NotAfter: template.NotAfter, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: template.IPAddresses}
	serverDER, err := x509.CreateCertificate(rand.Reader, leaf, template, &serverKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{serverDER}, PrivateKey: serverKey}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
