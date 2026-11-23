package grpc

import (
	"bytes"
	"context"
	"testing"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/service"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestE2EERPCConversationPostgres(t *testing.T) {
	f := newDeviceRPCFixture(t, false)
	ctx := f.ctx(t, f.aliceToken, f.aliceRaw)
	if _, err := f.client.UploadPreKeys(ctx, testutil.PublicPreKeyUpload(t, f.device.ActiveDevice.Reference.GenerationId)); err != nil {
		t.Fatal(err)
	}
	bobRaw := bytes.Repeat([]byte{86}, 32)
	hash := service.E2EEDeviceCredentialHash(bobRaw)
	bobRegistration := &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 37, IdentityKey: f.registration.IdentityKey, CredentialHash: hash[:]}
	bob, err := f.client.RegisterDevice(f.ctx(t, f.bobToken, bobRaw), bobRegistration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.UploadPreKeys(f.ctx(t, f.bobToken, bobRaw), testutil.PublicPreKeyUpload(t, bob.ActiveDevice.Reference.GenerationId)); err != nil {
		t.Fatal(err)
	}
	req := &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.device.ActiveDevice.Reference.GenerationId}, CreationRequestId: uuid.NewString(), PeerUserId: f.bobID}
	_, err = f.client.CreateConversation(context.Background(), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.CreateConversation(f.ctx(t, f.aliceToken, nil), req)
	rpcCode(t, err, codes.Unauthenticated)
	_, err = f.client.CreateConversation(ctx, req)
	rpcCode(t, err, codes.PermissionDenied)
	if _, err := f.db.ExecContext(t.Context(), `INSERT INTO friend_requests(from_user_id,to_user_id,status) VALUES($1,$2,'accepted')`, f.aliceID, f.bobID); err != nil {
		t.Fatal(err)
	}
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=FALSE WHERE user_id=$1`, f.bobID); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.CreateConversation(ctx, req)
	rpcCode(t, err, codes.PermissionDenied)
	if _, err := f.db.ExecContext(t.Context(), `UPDATE user_settings SET allow_direct_messages=TRUE WHERE user_id=$1`, f.bobID); err != nil {
		t.Fatal(err)
	}
	c, err := f.client.CreateConversation(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if c.State != pb.ConversationState_CONVERSATION_STATE_ACTIVE || c.Mode != pb.ConversationMode_CONVERSATION_MODE_SIGNAL_PQ_1 || c.OwnDevice.GenerationId != req.Context.DeviceGenerationId || c.PeerDevice.Reference.GenerationId != bob.ActiveDevice.Reference.GenerationId || c.CloseReason != pb.ConversationCloseReason_CONVERSATION_CLOSE_REASON_UNSPECIFIED || c.ClosedAtUnixMs != 0 {
		t.Fatal("invalid active conversation response")
	}
	replay, err := f.client.CreateConversation(ctx, req)
	if err != nil || !proto.Equal(c, replay) {
		t.Fatal("create response replay drift", err)
	}
	get := &pb.GetConversationRequest{Context: req.Context, ConversationId: c.ConversationId}
	loaded, err := f.client.GetConversation(ctx, get)
	if err != nil || !proto.Equal(c, loaded) {
		t.Fatal("GetConversation differs", err)
	}
	bobGet := &pb.GetConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: bob.ActiveDevice.Reference.GenerationId}, ConversationId: c.ConversationId}
	peerView, err := f.client.GetConversation(f.ctx(t, f.bobToken, bobRaw), bobGet)
	if err != nil || peerView.OwnDevice.GenerationId != bobGet.Context.DeviceGenerationId || peerView.PeerDevice.Reference.GenerationId != c.OwnDevice.GenerationId {
		t.Fatal("peer orientation mismatch", err)
	}
	thirdToken, err := f.auth.Register(t.Context(), "outsider", "outsider@conversation.invalid", deviceTestPassword)
	if err != nil {
		t.Fatal(err)
	}
	thirdRaw := bytes.Repeat([]byte{88}, 32)
	thirdHash := service.E2EEDeviceCredentialHash(thirdRaw)
	third, err := f.client.RegisterDevice(f.ctx(t, thirdToken, thirdRaw), &pb.RegisterDeviceRequest{ProtocolVersion: 1, RegistrationRequestId: uuid.NewString(), RegistrationId: 39, IdentityKey: f.registration.IdentityKey, CredentialHash: thirdHash[:]})
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.client.GetConversation(f.ctx(t, thirdToken, thirdRaw), &pb.GetConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: third.ActiveDevice.Reference.GenerationId}, ConversationId: c.ConversationId})
	rpcCode(t, err, codes.NotFound)
	unknown := proto.Clone(get).(*pb.GetConversationRequest)
	unknown.ConversationId = uuid.NewString()
	_, err = f.client.GetConversation(ctx, unknown)
	rpcCode(t, err, codes.NotFound)
	unknown = proto.Clone(get).(*pb.GetConversationRequest)
	unknown.ProtoReflect().SetUnknown(protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1))
	_, err = f.client.GetConversation(ctx, unknown)
	rpcCode(t, err, codes.InvalidArgument)
	self := proto.Clone(req).(*pb.CreateConversationRequest)
	self.PeerUserId = f.aliceID
	_, err = f.client.CreateConversation(ctx, self)
	rpcCode(t, err, codes.InvalidArgument)
	// Simulate a storage error: it must not look like an absent conversation.
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_conversations RENAME TO e2ee_conversations_offline`); err != nil {
		t.Fatal(err)
	}
	_, err = f.client.GetConversation(ctx, get)
	rpcCode(t, err, codes.Unavailable)
	if _, err := f.db.ExecContext(t.Context(), `ALTER TABLE e2ee_conversations_offline RENAME TO e2ee_conversations`); err != nil {
		t.Fatal(err)
	}
	newRaw := bytes.Repeat([]byte{87}, 32)
	newHash := service.E2EEDeviceCredentialHash(newRaw)
	newBob, err := f.client.ReplaceOwnDevice(f.ctx(t, f.bobToken, newRaw), &pb.ReplaceOwnDeviceRequest{ProtocolVersion: 1, ReplacementRequestId: uuid.NewString(), ExpectedGenerationId: bobGet.Context.DeviceGenerationId, RegistrationId: 38, IdentityKey: f.registration.IdentityKey, CredentialHash: newHash[:], CurrentPassword: deviceTestPassword})
	if err != nil {
		t.Fatal(err)
	}
	closed, err := f.client.GetConversation(ctx, get)
	if err != nil || closed.State != pb.ConversationState_CONVERSATION_STATE_CLOSED || closed.CloseReason != pb.ConversationCloseReason_CONVERSATION_CLOSE_REASON_DEVICE_REPLACED || closed.ClosedGenerationId != bobGet.Context.DeviceGenerationId || closed.ClosedAtUnixMs <= 0 || closed.PeerDevice.Reference.GenerationId != bobGet.Context.DeviceGenerationId {
		t.Fatal("missing closed chat metadata", err)
	}
	replay, err = f.client.CreateConversation(ctx, req)
	if err != nil || !proto.Equal(closed, replay) {
		t.Fatal("retry silently replaced closed chat", err)
	}
	bobGet.Context.DeviceGenerationId = newBob.ActiveDevice.Reference.GenerationId
	_, err = f.client.GetConversation(f.ctx(t, f.bobToken, newRaw), bobGet)
	rpcCode(t, err, codes.NotFound)
	if _, err := f.client.UploadPreKeys(f.ctx(t, f.bobToken, newRaw), testutil.PublicPreKeyUpload(t, newBob.ActiveDevice.Reference.GenerationId)); err != nil {
		t.Fatal(err)
	}
	newReq := proto.Clone(req).(*pb.CreateConversationRequest)
	newReq.CreationRequestId = uuid.NewString()
	newChat, err := f.client.CreateConversation(ctx, newReq)
	if err != nil || newChat.ConversationId == c.ConversationId || newChat.State != pb.ConversationState_CONVERSATION_STATE_ACTIVE {
		t.Fatal("new chat unavailable after replacement", err)
	}
	caps, err := f.client.GetCapabilities(f.ctx(t, f.aliceToken, nil), &pb.Empty{})
	if err != nil || caps.MessagingAvailable {
		t.Fatal("conversation API prematurely advertised messaging", err)
	}
}
