package grpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/repository"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestE2EERPCPublicationPostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, false)
	req := testutil.PublicPreKeyUpload(t, f.device.ActiveDevice.Reference.GenerationId)
	_, err := f.client.UploadPreKeys(context.Background(), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.UploadPreKeys(f.ctx(t, f.aliceToken, nil), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.UploadPreKeys(f.ctx(t, f.bobToken, f.aliceRaw), req)
	rpcCode(t, err, codes.Unauthenticated)
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	receipt, err := f.client.UploadPreKeys(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := boundary.PublicationDigest(req)
	if err != nil || receipt.BatchId != req.BatchId || !bytes.Equal(receipt.BatchDigest, digest[:]) || receipt.PublicationRevision != 1 {
		t.Fatal("invalid committed receipt", err)
	}
	replay, err := f.client.UploadPreKeys(ctx, req)
	if err != nil || !proto.Equal(receipt, replay) {
		t.Fatal("RPC replay drift", err)
	}
	own, err := f.client.GetOwnDevice(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || !own.ActiveDevice.PublicMaterialReady || own.ActiveDevice.PublicMaterialRevision != 1 {
		t.Fatal("atomic readiness not visible", err)
	}
	changed := proto.Clone(req).(*pb.UploadPreKeysRequest)
	changed.OneTimePreKeys[0].PublicKey[1] ^= 1
	_, err = f.client.UploadPreKeys(ctx, changed)
	rpcCode(t, err, codes.AlreadyExists)
	changed = proto.Clone(req).(*pb.UploadPreKeysRequest)
	changed.BatchId = uuid.NewString()
	_, err = f.client.UploadPreKeys(ctx, changed)
	rpcCode(t, err, codes.Aborted)
	changed.ExpectedRevision = 1
	changed.OneTimePreKeys[0].PublicKey[1] ^= 1
	_, err = f.client.UploadPreKeys(ctx, changed)
	rpcCode(t, err, codes.AlreadyExists)
	malformed := proto.Clone(req).(*pb.UploadPreKeysRequest)
	malformed.Context.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	_, err = f.client.UploadPreKeys(ctx, malformed)
	rpcCode(t, err, codes.InvalidArgument)
	malformed = proto.Clone(req).(*pb.UploadPreKeysRequest)
	malformed.KyberPreKeys = nil
	for i := range 35 {
		k := proto.Clone(req.KyberPreKeys[0]).(*pb.KyberPreKey)
		k.KeyId = uint32(i)
		malformed.KyberPreKeys = append(malformed.KyberPreKeys, k)
	}
	_, err = f.client.UploadPreKeys(ctx, malformed)
	rpcCode(t, err, codes.InvalidArgument)
	// Any storage failure must fail closed; no apparent empty/successful upload.
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_publication_batches RENAME TO e2ee_batches_offline`); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.UploadPreKeys(ctx, req)
	rpcCode(t, err, codes.Unavailable)
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_batches_offline RENAME TO e2ee_publication_batches`); err != nil {
		t.Fatal(err)
	}
	caps, err := f.client.GetCapabilities(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || caps.MessagingAvailable {
		t.Fatal("publication enabled messaging prematurely", err)
	}
	// A new crypto installation cannot replay the old generation's receipt.
	newRaw := bytes.Repeat([]byte{123}, 32)
	hash := service.E2EEDeviceCredentialHash(newRaw)
	replacement := &pb.ReplaceOwnDeviceRequest{ProtocolVersion: 1, ReplacementRequestId: uuid.NewString(), ExpectedGenerationId: req.Context.DeviceGenerationId, RegistrationId: f.registration.RegistrationId, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:], CurrentPassword: deviceTestPassword}
	newDevice, err := f.client.ReplaceOwnDevice(f.ctx(t, f.aliceToken, newRaw), replacement)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.UploadPreKeys(ctx, req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.UploadPreKeys(f.ctx(t, f.aliceToken, newRaw), req)
	rpcCode(t, err, codes.Unauthenticated)
	if newDevice.ActiveDevice.PublicMaterialReady || newDevice.ActiveDevice.PublicMaterialRevision != 0 {
		t.Fatal("replacement inherited public material")
	}
	_, d, err := repository.NewPgE2EEDeviceRepository(f.db).Own(t.Context(), f.aliceID)
	if err != nil || d.PublicMaterialRevision != 0 {
		t.Fatal(err)
	}
}
