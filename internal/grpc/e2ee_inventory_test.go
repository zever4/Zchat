package grpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestE2EERPCInventoryPostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, false)
	req := &pb.GetPreKeyInventoryRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.device.ActiveDevice.Reference.GenerationId}}
	for _, ctx := range []context.Context{context.Background(), f.ctx(t, f.aliceToken, nil), f.ctx(t, f.bobToken, f.aliceRaw)} {
		_, err := f.client.GetPreKeyInventory(ctx, req)
		rpcCode(t, err, codes.Unauthenticated)
	}
	_, err := f.client.GetPreKeyInventory(f.ctx(t, f.aliceToken, bytes.Repeat([]byte{0}, 32)), req)
	rpcCode(t, err, codes.Unauthenticated)
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	v, err := f.client.GetPreKeyInventory(ctx, req)
	if err != nil || v.PublicMaterialReady || v.PublicationRevision != 0 || !proto.Equal(v.OwnDevice, f.device.ActiveDevice.Reference) {
		t.Fatal("owner inventory", v, err)
	}
	if _, err := f.client.UploadPreKeys(ctx, testutil.PublicPreKeyUpload(t, req.Context.DeviceGenerationId)); err != nil {
		t.Fatal(err)
	}
	v, err = f.client.GetPreKeyInventory(ctx, req)
	if err != nil || v.AvailableEc != 1 || v.AvailablePq != 1 || v.RetainedPq != 2 || v.PublicationRevision != 1 || !v.PublicMaterialReady {
		t.Fatal("ready inventory", v, err)
	}
	for _, nested := range []bool{false, true} {
		bad := proto.Clone(req).(*pb.GetPreKeyInventoryRequest)
		unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
		if nested {
			bad.Context.ProtoReflect().SetUnknown(unknown)
		} else {
			bad.ProtoReflect().SetUnknown(unknown)
		}
		_, err := f.client.GetPreKeyInventory(ctx, bad)
		rpcCode(t, err, codes.InvalidArgument)
	}
	bad := proto.Clone(req).(*pb.GetPreKeyInventoryRequest)
	bad.Context.DeviceGenerationId = uuid.NewString()
	_, err = f.client.GetPreKeyInventory(ctx, bad)
	rpcCode(t, err, codes.Unauthenticated)
	bad = proto.Clone(req).(*pb.GetPreKeyInventoryRequest)
	bad.Context.ProtocolVersion++
	_, err = f.client.GetPreKeyInventory(ctx, bad)
	rpcCode(t, err, codes.FailedPrecondition)
	_, err = f.client.GetPreKeyInventory(ctx, &pb.GetPreKeyInventoryRequest{})
	rpcCode(t, err, codes.InvalidArgument)
	// Storage failure must not masquerade as an empty inventory (refill trigger).
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_publication_batches RENAME TO e2ee_inventory_offline`); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.GetPreKeyInventory(ctx, req)
	rpcCode(t, err, codes.Unavailable)
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_inventory_offline RENAME TO e2ee_publication_batches`); err != nil {
		t.Fatal(err)
	}
}
