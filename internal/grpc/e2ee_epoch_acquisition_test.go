package grpc

import (
	"context"
	"database/sql"
	"database/sql/driver"
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
	"github.com/zever4/Zchat/internal/repository"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Controlled-time fixture, ONLY the disposable schema made by testutil.Postgres.
// Keep the saved public snapshot/deadline consistent; private/reserved keys and
// production TTL/clock/immutability are untouched. Trigger restoration is atomic.
func expireEpochAcquisitionFixture(ctx context.Context, db *sql.DB, r *v2.AcquireBundleRequest) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var wire []byte
	var now time.Time
	if err = tx.QueryRowContext(ctx, `SELECT public_snapshot,date_trunc('milliseconds',clock_timestamp()) FROM e2ee_acquisitions WHERE owner_generation_id=$1 AND acquisition_id=$2 AND conversation_id=$3 AND wire_version=2 AND crypto_epoch=$4`, r.Context.DeviceGenerationId, r.AcquisitionId, r.ConversationId, *r.CryptoEpoch).Scan(&wire, &now); err != nil {
		return err
	}
	snapshot := new(v2.AcquiredBundle)
	if err = proto.Unmarshal(wire, snapshot); err != nil {
		return err
	}
	expires := now.Add(-time.Hour)
	snapshot.NotAfterUnixMs = expires.UnixMilli()
	wire, err = proto.MarshalOptions{Deterministic: true}.Marshal(snapshot)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `ALTER TABLE e2ee_acquisitions DISABLE TRIGGER e2ee_guard_epoch_acquisition`); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE e2ee_acquisitions SET created_at=$1,not_after=$2,public_snapshot=$3 WHERE owner_generation_id=$4 AND acquisition_id=$5 AND conversation_id=$6 AND wire_version=2 AND crypto_epoch=$7`, expires.Add(-domain.E2EEAcquisitionLease), expires, wire, r.Context.DeviceGenerationId, r.AcquisitionId, r.ConversationId, *r.CryptoEpoch)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return errors.New("synthetic expiry target missing")
	}
	if _, err = tx.ExecContext(ctx, `ALTER TABLE e2ee_acquisitions ENABLE TRIGGER e2ee_guard_epoch_acquisition`); err != nil {
		return err
	}
	return tx.Commit()
}

func TestE2EEEpochAcquisitionExpiryTLSPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	r := &v2.AcquireBundleRequest{Context: f.wire(1), ConversationId: f.chat.ConversationId, AcquisitionId: uuid.NewString(), ExpectedPeer: f.bob.ActiveDevice.Reference, CryptoEpoch: proto.Uint64(0)}
	ctx := f.ownerContext(t, 1)
	first, err := f.v2.AcquireBundle(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if err = expireEpochAcquisitionFixture(t.Context(), f.db, r); err != nil {
		t.Fatal(err)
	}
	_, err = f.v2.AcquireBundle(ctx, r)
	s := status.Convert(err)
	details := s.Details()
	if s.Code() != codes.FailedPrecondition || len(details) != 1 {
		t.Fatal("missing typed expiry", err)
	}
	failure, ok := details[0].(*v2.AcquisitionExpiredFailure)
	if !ok || !proto.Equal(failure.Request, r) {
		t.Fatal("unbound expiry detail", err)
	}
	wrong := proto.Clone(r).(*v2.AcquireBundleRequest)
	wrong.CryptoEpoch = proto.Uint64(1)
	_, err = f.v2.AcquireBundle(ctx, wrong)
	if len(status.Convert(err).Details()) != 0 {
		t.Fatal("wrong epoch minted expiry", err)
	}
	next := proto.Clone(r).(*v2.AcquireBundleRequest)
	next.AcquisitionId = uuid.NewString()
	bundle, err := f.v2.AcquireBundle(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.OneTimePreKey != nil || !bundle.KyberPreKey.IsLastResort || bundle.NotAfterUnixMs <= first.NotAfterUnixMs-1000 {
		t.Fatal("expiry recycled one-time keys or shortened new lease")
	}
	if _, err = f.db.ExecContext(t.Context(), `UPDATE e2ee_acquisitions SET not_after=not_after+INTERVAL '1 minute'`); err == nil {
		t.Fatal("fixture left immutable guard disabled")
	}
	if _, err = f.db.ExecContext(t.Context(), `UPDATE e2ee_devices SET retired_at=clock_timestamp(),public_material_ready=FALSE WHERE generation_id=$1`, f.gen(2)); err != nil {
		t.Fatal(err)
	}
	_, err = f.v2.AcquireBundle(ctx, r)
	for _, d := range status.Convert(err).Details() {
		if _, ok := d.(*v2.AcquisitionExpiredFailure); ok {
			t.Fatal("closed/replaced peer minted expiry", err)
		}
	}
}

func TestE2EEEpochAcquisitionErrorDetailsAreExactAndNeverMintedForGenericFailures(t *testing.T) {
	r := &v2.AcquireBundleRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: uuid.NewString()}, ConversationId: uuid.NewString(), AcquisitionId: uuid.NewString(), ExpectedPeer: &pb.DeviceRef{UserId: uuid.NewString(), GenerationId: uuid.NewString(), DeviceId: 1}, CryptoEpoch: proto.Uint64(0)}
	s := status.Convert(epochAcquireStatus(domain.ErrE2EEAcquisitionExpired, r))
	d := s.Details()
	if s.Code() != codes.FailedPrecondition || len(d) != 1 {
		t.Fatal("expiry mapper", s)
	}
	f, ok := d[0].(*v2.AcquisitionExpiredFailure)
	if !ok || !proto.Equal(f.Request, r) {
		t.Fatal("wrong expiry binding")
	}
	r.AcquisitionId = uuid.NewString()
	if f.Request.AcquisitionId == r.AcquisitionId {
		t.Fatal("error retained mutable request alias")
	}
	for _, err := range []error{context.Canceled, context.DeadlineExceeded, domain.ErrE2EEEpochInactive, domain.ErrE2EEGenerationChanged, domain.ErrE2EERequestConflict, errors.New("private SQL diagnostic")} {
		mapped := epochAcquireStatus(err, r)
		if status.Convert(mapped).Message() == "private SQL diagnostic" {
			t.Fatal("diagnostic leak")
		}
		for _, d := range status.Convert(mapped).Details() {
			if _, ok := d.(*v2.AcquisitionExpiredFailure); ok {
				t.Fatal("generic error minted expiry", mapped)
			}
		}
	}
	for _, r := range []*v2.AcquireBundleRequest{nil, {}, {Context: &pb.ClientContext{ProtocolVersion: 1}}} {
		if len(status.Convert(epochAcquireStatus(domain.ErrE2EEAcquisitionExpired, r)).Details()) != 0 {
			t.Fatal("invalid request minted proof")
		}
	}
}

// Fail ONLY Commit of a real PostgreSQL transaction. The driver wrapper is test
// local, delegates actual queries/locks to lib/pq, rolls back on the injected
// failure and is never reachable by application wiring.
type acquisitionCommitConnector struct {
	driver.Connector
	fail *atomic.Bool
}

func (c acquisitionCommitConnector) Connect(ctx context.Context) (driver.Conn, error) {
	v, e := c.Connector.Connect(ctx)
	if e != nil {
		return nil, e
	}
	return acquisitionCommitConn{Conn: v, fail: c.fail}, nil
}

type acquisitionCommitConn struct {
	driver.Conn
	fail *atomic.Bool
}

func (c acquisitionCommitConn) Begin() (driver.Tx, error) {
	tx, e := c.Conn.Begin()
	if e != nil {
		return nil, e
	}
	return acquisitionCommitTx{Tx: tx, fail: c.fail}, nil
}
func (c acquisitionCommitConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, e := c.Conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if e != nil {
		return nil, e
	}
	return acquisitionCommitTx{Tx: tx, fail: c.fail}, nil
}

type acquisitionCommitTx struct {
	driver.Tx
	fail *atomic.Bool
}

func (tx acquisitionCommitTx) Commit() error {
	if tx.fail.CompareAndSwap(true, false) {
		_ = tx.Tx.Rollback()
		return errors.New("synthetic read COMMIT failure")
	}
	return tx.Tx.Commit()
}

func TestE2EEEpochAcquisitionFailedReadCommitIsNotExpiryPostgres(t *testing.T) {
	f := newEpochRPCFixture(t)
	r := &v2.AcquireBundleRequest{Context: f.wire(1), ConversationId: f.chat.ConversationId, AcquisitionId: uuid.NewString(), ExpectedPeer: f.bob.ActiveDevice.Reference, CryptoEpoch: proto.Uint64(0)}
	if _, err := f.v2.AcquireBundle(f.ownerContext(t, 1), r); err != nil {
		t.Fatal(err)
	}
	if err := expireEpochAcquisitionFixture(t.Context(), f.db, r); err != nil {
		t.Fatal(err)
	}
	var schema string
	if err := f.db.QueryRowContext(t.Context(), `SELECT current_schema()`).Scan(&schema); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(os.Getenv("ZCHAT_TEST_DATABASE_URL"))
	if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" {
		t.Fatal("test expects explicit PostgreSQL URL")
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	connector, err := pq.NewConnector(u.String())
	if err != nil {
		t.Fatal("test connector setup failed")
	}
	fail := new(atomic.Bool)
	fail.Store(true)
	db := sql.OpenDB(acquisitionCommitConnector{Connector: connector, fail: fail})
	t.Cleanup(func() { db.Close() })
	result, err := repository.NewPgE2EEPreKeyRepository(db).AcquireV2(t.Context(), f.aliceID, r)
	if result != nil || err == nil || errors.Is(err, domain.ErrE2EEAcquisitionExpired) || fail.Load() {
		t.Fatal("failed COMMIT became expiry or fault not exercised", err)
	}
	for _, d := range status.Convert(epochAcquireStatus(err, r)).Details() {
		if _, ok := d.(*v2.AcquisitionExpiredFailure); ok {
			t.Fatal("commit error minted proof")
		}
	}
}
