package e2ee

import (
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestConversationRequestsRejectUnknownSemantics(t *testing.T) {
	ctx := &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: requestID}
	create := &pb.CreateConversationRequest{Context: ctx, CreationRequestId: requestID, PeerUserId: 2}
	get := &pb.GetConversationRequest{Context: ctx, ConversationId: requestID}
	if err := ValidateCreateConversation(create); err != nil {
		t.Fatal(err)
	}
	if err := ValidateGetConversation(get); err != nil {
		t.Fatal(err)
	}
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 99, protowire.VarintType), 1)
	for _, nested := range []bool{false, true} {
		c := proto.Clone(create).(*pb.CreateConversationRequest)
		g := proto.Clone(get).(*pb.GetConversationRequest)
		if nested {
			c.Context.ProtoReflect().SetUnknown(unknown)
			g.Context.ProtoReflect().SetUnknown(unknown)
		} else {
			c.ProtoReflect().SetUnknown(unknown)
			g.ProtoReflect().SetUnknown(unknown)
		}
		if ValidateCreateConversation(c) == nil || ValidateGetConversation(g) == nil {
			t.Fatal("unknown conversation semantics accepted")
		}
	}
}
