package grpc

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func blobRPCRequest() *v2.BlobLeaseRequest {
	g := uuid.NewString()
	return &v2.BlobLeaseRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: g}, MessageId: uuid.NewString(), ConversationId: uuid.NewString(), CryptoEpoch: proto.Uint64(0),
		Sender: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: g, DeviceId: 1}, Recipient: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: uuid.NewString(), DeviceId: 1},
		Blob: &v2.BlobRef{FormatVersion: 1, BlobId: uuid.NewString(), CiphertextBytes: 64, Sha256: make([]byte, 32)}, OperationId: uuid.NewString()}
}
func blobFailure(t *testing.T, err error, code codes.Code, reason v2.BlobFailureReason, r *v2.BlobLeaseRequest) {
	t.Helper()
	s := status.Convert(err)
	details := s.Details()
	if s.Code() != code || len(details) != 1 {
		t.Fatal("unexpected blob failure", s.Code(), err)
	}
	d, ok := details[0].(*v2.BlobFailure)
	if !ok || d.Reason != reason || !proto.Equal(d.Request, r) {
		t.Fatal("wrong failure binding", err)
	}
}

func TestBlobControlFailureDetailsAreExactAndMethodSpecific(t *testing.T) {
	r := blobRPCRequest()
	for _, tc := range []struct {
		err    error
		code   codes.Code
		reason v2.BlobFailureReason
	}{
		{domain.ErrE2EEBlobOperationNotFound, codes.NotFound, v2.BlobFailureReason_BLOB_FAILURE_REASON_OPERATION_NOT_OBSERVED},
		{domain.ErrE2EEBlobCAS, codes.Aborted, v2.BlobFailureReason_BLOB_FAILURE_REASON_CAS_CHANGED},
		{domain.ErrE2EEBlobLeaseActive, codes.FailedPrecondition, v2.BlobFailureReason_BLOB_FAILURE_REASON_ACTIVE_LEASE},
		{domain.ErrE2EEMessageConflict, codes.AlreadyExists, v2.BlobFailureReason_BLOB_FAILURE_REASON_BINDING_CONFLICT},
		{domain.ErrE2EEBlobCapacity, codes.ResourceExhausted, v2.BlobFailureReason_BLOB_FAILURE_REASON_CAPACITY},
		{domain.ErrE2EEBlobNotReady, codes.FailedPrecondition, v2.BlobFailureReason_BLOB_FAILURE_REASON_STORAGE_UNAVAILABLE},
	} {
		blobFailure(t, epochBlobStatus(tc.err, r, true), tc.code, tc.reason, r)
	}
	mapped := status.Convert(epochBlobStatus(domain.ErrE2EEBlobOperationNotFound, r, true))
	d := mapped.Details()[0].(*v2.BlobFailure)
	r.Blob.Sha256[0]++
	if d.Request.Blob.Sha256[0] == r.Blob.Sha256[0] {
		t.Fatal("mutable failure detail")
	}
	for _, observe := range []bool{false, true} {
		for _, err := range []error{context.Canceled, context.DeadlineExceeded, domain.ErrE2EEEpochInactive, domain.ErrE2EEGenerationChanged, domain.ErrE2EEConversationClosed, errors.New("private SQL/path diagnostic")} {
			mapped := status.Convert(epochBlobStatus(err, r, observe))
			if mapped.Message() == "private SQL/path diagnostic" {
				t.Fatal("diagnostic leaked")
			}
			for _, d := range mapped.Details() {
				if _, ok := d.(*v2.BlobFailure); ok {
					t.Fatal("generic failure became blob fact", err)
				}
			}
		}
	}
	if s := status.Convert(epochBlobStatus(domain.ErrE2EEBlobOperationNotFound, r, false)); s.Code() != codes.Unavailable || len(s.Details()) != 0 {
		t.Fatal("Reserve minted absence", s)
	}
	for _, bad := range []*v2.BlobLeaseRequest{nil, {}, {Context: &pb.ClientContext{ProtocolVersion: 1}}} {
		if len(status.Convert(epochBlobStatus(domain.ErrE2EEBlobOperationNotFound, bad, true)).Details()) != 0 {
			t.Fatal("invalid request minted absence")
		}
	}
}

type blobControlFake struct {
	service.E2EEBlobControls // Other methods are not exercised by this reservation-only fake.
	call                     func(context.Context, int64, *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error)
}

func (f blobControlFake) Reserve(c context.Context, u int64, r *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	return f.call(c, u, r)
}
func (f blobControlFake) Observe(c context.Context, u int64, r *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	return f.call(c, u, r)
}

func TestBlobControlRejectsUnauthenticatedInvalidAndMalformedReplies(t *testing.T) {
	r := blobRPCRequest()
	calls := 0
	now := time.Now().Truncate(time.Millisecond)
	g := &domain.E2EEBlobReservation{MessageID: r.MessageId, OperationID: r.OperationId, AttemptID: uuid.NewString(), Revision: 1, ReservedAt: now, ExpiresAt: now.Add(time.Hour), ObservedAt: now, State: "reserved"}
	s := NewE2EEEpochServer(nil).WithBlobControls(blobControlFake{call: func(_ context.Context, uid int64, d *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
		calls++
		if uid != 42 || d.MessageID != r.MessageId {
			t.Fatal("wrong owner/binding")
		}
		return g, nil
	}})
	ctx := context.WithValue(t.Context(), e2eePrincipalKey{}, e2eePrincipal{userID: 42, device: &domain.E2EEDevice{UserID: 42, GenerationID: r.Context.DeviceGenerationId}})
	for _, method := range []func(context.Context, *v2.BlobLeaseRequest) (*v2.BlobLease, error){s.ReserveBlob, s.ObserveBlob} {
		if _, err := method(t.Context(), r); status.Code(err) != codes.Unauthenticated {
			t.Fatal(err)
		}
		if _, err := method(ctx, nil); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
		bad := proto.Clone(r).(*v2.BlobLeaseRequest)
		bad.CryptoEpoch = nil
		if _, err := method(ctx, bad); status.Code(err) != codes.InvalidArgument {
			t.Fatal(err)
		}
		if calls != 0 {
			t.Fatal("invalid request reached repository")
		}
	}
	if got, err := s.ReserveBlob(ctx, r); err != nil || boundary.ValidateBlobLease(r, got) != nil {
		t.Fatal(got, err)
	}
	for _, change := range []func(*domain.E2EEBlobReservation){
		func(g *domain.E2EEBlobReservation) { g.MessageID = uuid.NewString() }, func(g *domain.E2EEBlobReservation) { g.OperationID = uuid.NewString() },
		func(g *domain.E2EEBlobReservation) { g.State = "unknown" }, func(g *domain.E2EEBlobReservation) { g.Expired = true },
	} {
		saved := *g
		change(g)
		if got, err := s.ObserveBlob(ctx, r); got != nil || status.Code(err) != codes.Unavailable || len(status.Convert(err).Details()) != 0 {
			t.Fatal("bad repository success escaped", got, err)
		}
		*g = saved
	}
	s.blobs = nil
	_, err := s.ObserveBlob(ctx, r)
	blobFailure(t, err, codes.FailedPrecondition, v2.BlobFailureReason_BLOB_FAILURE_REASON_STORAGE_UNAVAILABLE, r)
}

func blobRPCPostgres(t *testing.T) (*epochRPCFixture, *repository.PgE2EEBlobRepository, *v2.BlobLeaseRequest) {
	t.Helper()
	f := newEpochRPCFixture(t)
	repo := repository.NewPgE2EEBlobRepository(f.db)
	if err := repo.Configure(t.Context(), domain.E2EEBlobLimits{GlobalBytes: 4096, AccountBytes: 2048, GlobalClaims: 16, AccountClaims: 8, GlobalAttempts: 32, AccountAttempts: 16}); err != nil {
		t.Fatal(err)
	}
	r := blobRPCRequest()
	r.Context = f.wire(1)
	r.Sender = proto.Clone(f.device.ActiveDevice.Reference).(*pb.DeviceRef)
	r.Recipient = proto.Clone(f.bob.ActiveDevice.Reference).(*pb.DeviceRef)
	r.ConversationId = f.chat.ConversationId
	f.epoch.WithBlobControls(repo)
	return f, repo, r
}

func TestBlobControlTLSAuthReserveObserveAndDisabledSendPostgres(t *testing.T) {
	f, _, r := blobRPCPostgres(t)
	ctx := f.ownerContext(t, 1)
	for _, method := range []func(context.Context, *v2.BlobLeaseRequest) (*v2.BlobLease, error){
		func(c context.Context, r *v2.BlobLeaseRequest) (*v2.BlobLease, error) { return f.v2.ReserveBlob(c, r) },
		func(c context.Context, r *v2.BlobLeaseRequest) (*v2.BlobLease, error) { return f.v2.ObserveBlob(c, r) },
	} {
		for _, c := range []context.Context{t.Context(), f.ctx(t, f.aliceToken, nil), f.ctx(t, f.aliceToken, f.bobRaw), f.ownerContext(t, 2)} {
			if _, err := method(c, r); status.Code(err) != codes.Unauthenticated {
				t.Fatal("missing/foreign auth accepted", err)
			}
		}
	}
	_, err := f.v2.ObserveBlob(ctx, r)
	blobFailure(t, err, codes.NotFound, v2.BlobFailureReason_BLOB_FAILURE_REASON_OPERATION_NOT_OBSERVED, r)
	first, err := f.v2.ReserveBlob(ctx, r)
	if err != nil || boundary.ValidateBlobLease(r, first) != nil {
		t.Fatal(first, err)
	}
	for _, method := range []func(context.Context, *v2.BlobLeaseRequest) (*v2.BlobLease, error){
		func(c context.Context, r *v2.BlobLeaseRequest) (*v2.BlobLease, error) { return f.v2.ReserveBlob(c, r) },
		func(c context.Context, r *v2.BlobLeaseRequest) (*v2.BlobLease, error) { return f.v2.ObserveBlob(c, r) },
	} {
		g, err := method(ctx, r)
		if err != nil || g.AttemptId != first.AttemptId || g.ExpiresAtUnixMs != first.ExpiresAtUnixMs || g.Revision != 1 {
			t.Fatal("replay changed lease", g, err)
		}
	}
	bad := proto.Clone(r).(*v2.BlobLeaseRequest)
	bad.Blob.Sha256[0]++
	_, err = f.v2.ObserveBlob(ctx, bad)
	blobFailure(t, err, codes.AlreadyExists, v2.BlobFailureReason_BLOB_FAILURE_REASON_BINDING_CONFLICT, bad)
	renew := proto.Clone(r).(*v2.BlobLeaseRequest)
	renew.OperationId = uuid.NewString()
	renew.ExpectedRevision = 1
	renew.ExpectedAttemptId = first.AttemptId
	_, err = f.v2.ReserveBlob(ctx, renew)
	blobFailure(t, err, codes.FailedPrecondition, v2.BlobFailureReason_BLOB_FAILURE_REASON_ACTIVE_LEASE, renew)
	tx, err := f.db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	// Controlled clock ONLY in the disposable test schema; runtime cannot mutate TTL.
	if _, err = tx.ExecContext(t.Context(), `ALTER TABLE e2ee_blob_attempts DISABLE TRIGGER e2ee_guard_blob_attempt;
        UPDATE e2ee_blob_attempts SET reserved_at=reserved_at-INTERVAL '2 hours',expires_at=expires_at-INTERVAL '2 hours';
        ALTER TABLE e2ee_blob_attempts ENABLE TRIGGER e2ee_guard_blob_attempt`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	expired, err := f.v2.ObserveBlob(ctx, r)
	if err != nil || !expired.Expired || expired.ExpiresAtUnixMs != first.ExpiresAtUnixMs-2*time.Hour.Milliseconds() {
		t.Fatal(expired, err)
	}
	next, err := f.v2.ReserveBlob(ctx, renew)
	if err != nil || next.Revision != 2 || next.AttemptId == first.AttemptId {
		t.Fatal(next, err)
	}
	old, err := f.v2.ObserveBlob(ctx, r)
	if err != nil || old.AttemptId != first.AttemptId || old.State != v2.BlobLeaseState_BLOB_LEASE_STATE_DELETING {
		t.Fatal("lost historical operation", old, err)
	}
	_, err = f.v2.ReserveBlob(ctx, r)
	blobFailure(t, err, codes.Aborted, v2.BlobFailureReason_BLOB_FAILURE_REASON_CAS_CHANGED, r)
	send := f.message(0)
	send.Envelope.ProtocolVersion = 3
	send.Envelope.MessageId = r.MessageId
	send.Envelope.Blob = r.Blob
	if _, err = f.v2.SendMessage(ctx, send); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("Send3 gate lifted", err)
	}
	var attempts, receipts, bytes int64
	if err = f.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_blob_attempts),(SELECT count(*) FROM e2ee_send_receipts),reserved_bytes FROM e2ee_blob_global_quota`).Scan(&attempts, &receipts, &bytes); err != nil || attempts != 2 || receipts != 0 || bytes != 128 {
		t.Fatal("unexpected state", attempts, receipts, bytes, err)
	}
	caps, err := f.v2.GetCapabilities(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || caps.MessagingAvailable || caps.RecoveryAvailable {
		t.Fatal("readiness enabled", caps, err)
	}
	if err = f.auth.Logout(ctx, f.aliceToken, false, f.guard.DisconnectSession); err != nil {
		t.Fatal(err)
	}
	if _, err = f.v2.ObserveBlob(ctx, r); status.Code(err) != codes.Unauthenticated {
		t.Fatal("revoked session accepted", err)
	}
}

func TestBlobControlFailedReadCommitIsNeverAbsencePostgres(t *testing.T) {
	f, repo, r := blobRPCPostgres(t)
	if _, err := repo.Reserve(t.Context(), f.aliceID, rpcBlobDomain(t, r)); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := f.db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("ZCHAT_TEST_DATABASE_URL"))
	if err != nil || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		t.Fatal("explicit PostgreSQL URL required")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	connector, err := pq.NewConnector(u.String())
	if err != nil {
		t.Fatal("test connector")
	}
	for _, name := range []string{"no claim", "unknown operation", "existing operation"} {
		t.Run(name, func(t *testing.T) {
			req := proto.Clone(r).(*v2.BlobLeaseRequest)
			if name == "no claim" {
				req.MessageId = uuid.NewString()
				req.Blob.BlobId = uuid.NewString()
			}
			if name != "existing operation" {
				req.OperationId = uuid.NewString()
			}
			fail := new(atomic.Bool)
			fail.Store(true)
			db := sql.OpenDB(acquisitionCommitConnector{Connector: connector, fail: fail})
			defer db.Close()
			f.epoch.WithBlobControls(repository.NewPgE2EEBlobRepository(db))
			got, err := f.v2.ObserveBlob(f.ownerContext(t, 1), req)
			if got != nil || status.Code(err) != codes.Unavailable || len(status.Convert(err).Details()) != 0 || fail.Load() {
				t.Fatal("failed COMMIT became fact/fault not exercised", got, err)
			}
			got, err = f.v2.ObserveBlob(f.ownerContext(t, 1), req)
			if name == "existing operation" {
				if err != nil || got == nil {
					t.Fatal(got, err)
				}
			} else {
				blobFailure(t, err, codes.NotFound, v2.BlobFailureReason_BLOB_FAILURE_REASON_OPERATION_NOT_OBSERVED, req)
			}
		})
	}
}

func rpcBlobDomain(t *testing.T, r *v2.BlobLeaseRequest) *domain.E2EEBlobReservationRequest {
	t.Helper()
	d, err := boundary.BlobReservationFromWire(r)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

type blobLostReserveResponse struct {
	service.E2EEBlobControls
	lose atomic.Bool
}

func (m *blobLostReserveResponse) Reserve(c context.Context, u int64, r *domain.E2EEBlobReservationRequest) (*domain.E2EEBlobReservation, error) {
	g, err := m.E2EEBlobControls.Reserve(c, u, r)
	if err == nil && m.lose.Swap(false) {
		return nil, errors.New("synthetic response loss AFTER actual COMMIT")
	}
	return g, err
}
func TestBlobControlObserveResolvesLostReserveResponsePostgres(t *testing.T) {
	f, repo, r := blobRPCPostgres(t)
	m := &blobLostReserveResponse{E2EEBlobControls: repo}
	m.lose.Store(true)
	f.epoch.WithBlobControls(m)
	if got, err := f.v2.ReserveBlob(f.ownerContext(t, 1), r); got != nil || status.Code(err) != codes.Unavailable || len(status.Convert(err).Details()) != 0 {
		t.Fatal(got, err)
	}
	observed, err := f.v2.ObserveBlob(f.ownerContext(t, 1), r)
	if err != nil || observed == nil || observed.Revision != 1 {
		t.Fatal(observed, err)
	}
	replayed, err := f.v2.ReserveBlob(f.ownerContext(t, 1), r)
	if err != nil || replayed.AttemptId != observed.AttemptId || replayed.ExpiresAtUnixMs != observed.ExpiresAtUnixMs {
		t.Fatal(replayed, err)
	}
	var attempts, bytes int64
	if err = f.db.QueryRowContext(t.Context(), `SELECT (SELECT count(*) FROM e2ee_blob_attempts),reserved_bytes FROM e2ee_blob_global_quota`).Scan(&attempts, &bytes); err != nil || attempts != 1 || bytes != 64 {
		t.Fatal("retry allocated twice", attempts, bytes, err)
	}
}
