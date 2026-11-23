package grpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestE2EERPCAcquisitionPostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, false)
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	if _, err := f.client.UploadPreKeys(ctx, testutil.PublicPreKeyUpload(t, f.device.ActiveDevice.Reference.GenerationId)); err != nil {
		t.Fatal(err)
	}
	bobRaw := bytes.Repeat([]byte{47}, 32)
	hash := service.E2EEDeviceCredentialHash(bobRaw)
	bob, err := f.client.RegisterDevice(f.ctx(t, f.bobToken, bobRaw), &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 16383, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:]})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.UploadPreKeys(f.ctx(t, f.bobToken, bobRaw), testutil.PublicPreKeyUpload(t, bob.ActiveDevice.Reference.GenerationId)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')`, f.aliceID, f.bobID); err != nil {
		t.Fatal(err)
	}
	c, err := f.client.CreateConversation(ctx, &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.device.ActiveDevice.Reference.GenerationId}, CreationRequestId: uuid.NewString(), PeerUserId: f.bobID})
	if err != nil {
		t.Fatal(err)
	}
	req := &pb.AcquireBundleRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: c.OwnDevice.GenerationId}, ConversationId: c.ConversationId, AcquisitionId: uuid.NewString(), ExpectedPeer: c.PeerDevice.Reference}
	_, err = f.client.AcquireBundle(context.Background(), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.AcquireBundle(f.ctx(t, f.aliceToken, nil), req)
	rpcCode(t, err, codes.Unauthenticated)
	bad := proto.Clone(req).(*pb.AcquireBundleRequest)
	bad.ExpectedPeer.GenerationId = uuid.NewString()
	_, err = f.client.AcquireBundle(ctx, bad)
	rpcCode(t, err, codes.FailedPrecondition)
	bad = proto.Clone(req).(*pb.AcquireBundleRequest)
	bad.Context.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	_, err = f.client.AcquireBundle(ctx, bad)
	rpcCode(t, err, codes.InvalidArgument)
	first, err := f.client.AcquireBundle(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if err := boundary.ValidateAcquiredBundle(req, first); err != nil {
		t.Fatal(err)
	}
	if first.OneTimePreKey == nil || first.KyberPreKey.IsLastResort {
		t.Fatal("initial full bundle incomplete")
	}
	// Same request after a known server COMMIT: an application retry must not
	// allocate another key even if it discarded/lost the earlier response.
	replay, err := f.client.AcquireBundle(ctx, req)
	if err != nil || !proto.Equal(replay, first) {
		t.Fatal("RPC retry changed bundle", err)
	}
	bad = proto.Clone(req).(*pb.AcquireBundleRequest)
	bad.ExpectedPeer.UserId = c.OwnDevice.UserId
	_, err = f.client.AcquireBundle(ctx, bad)
	rpcCode(t, err, codes.AlreadyExists)
	fresh := proto.Clone(req).(*pb.AcquireBundleRequest)
	fresh.AcquisitionId = uuid.NewString()
	second, err := f.client.AcquireBundle(ctx, fresh)
	if err != nil || second.OneTimePreKey != nil || !second.KyberPreKey.IsLastResort {
		t.Fatal("wire fallback mismatch", err)
	}
	thirdToken, err := f.auth.Register(t.Context(), "acquisition_outsider", "acquisition-outsider@example.invalid", deviceTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	thirdRaw := bytes.Repeat([]byte{48}, 32)
	thirdHash := service.E2EEDeviceCredentialHash(thirdRaw)
	third, err := f.client.RegisterDevice(f.ctx(t, thirdToken, thirdRaw), &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 16383, IdentityKey: f.registration.IdentityKey, CredentialHash: thirdHash[:]})
	if err != nil {
		t.Fatal(err)
	}
	outsider := proto.Clone(req).(*pb.AcquireBundleRequest)
	outsider.Context.DeviceGenerationId = third.ActiveDevice.Reference.GenerationId
	_, err = f.client.AcquireBundle(f.ctx(t, thirdToken, thirdRaw), outsider)
	rpcCode(t, err, codes.NotFound)
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_acquisitions RENAME TO e2ee_acquisitions_offline`); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.AcquireBundle(ctx, req)
	rpcCode(t, err, codes.Unavailable)
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_acquisitions_offline RENAME TO e2ee_acquisitions`); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE e2ee_acquisitions SET created_at=statement_timestamp()-INTERVAL '25 hours',not_after=statement_timestamp()-INTERVAL '1 hour' WHERE acquisition_id=$1`, req.AcquisitionId); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.AcquireBundle(ctx, req)
	rpcCode(t, err, codes.FailedPrecondition)
	newRaw := bytes.Repeat([]byte{49}, 32)
	newHash := service.E2EEDeviceCredentialHash(newRaw)
	_, err = f.client.ReplaceOwnDevice(f.ctx(t, f.bobToken, newRaw), &pb.ReplaceOwnDeviceRequest{ProtocolVersion: 1, ReplacementRequestId: uuid.NewString(), ExpectedGenerationId: bob.ActiveDevice.Reference.GenerationId, RegistrationId: 16383, IdentityKey: f.registration.IdentityKey, CredentialHash: newHash[:], CurrentPassword: deviceTestPassword})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.AcquireBundle(ctx, fresh)
	rpcCode(t, err, codes.FailedPrecondition)
	var count int
	if err := f.db.QueryRowContext(t.Context(), `SELECT count(*) FROM e2ee_acquisitions`).Scan(&count); err != nil || count != 2 {
		t.Fatal("unauthorized/failed retry allocated keys", err)
	}
	caps, err := f.client.GetCapabilities(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || caps.MessagingAvailable {
		t.Fatal("acquisition prematurely enabled messaging", err)
	}
}
