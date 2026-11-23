package e2ee

import (
	"math"
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

func TestEpochRPCStrictRequests(t *testing.T) {
	e := epochEnvelope(t)
	c := epochSend(e).Context
	if ValidateEpochPendingRequest(&v2.GetPendingMessagesRequest{Context: c}) != nil {
		t.Fatal("valid pending")
	}
	for _, version := range []uint32{0, 1, 3} {
		bad := proto.Clone(c).(*pb.ClientContext)
		bad.ProtocolVersion = version
		if ValidateEpochPendingRequest(&v2.GetPendingMessagesRequest{Context: bad}) == nil {
			t.Fatal("wrong data version", version)
		}
	}
	if ValidateEpochPendingRequest(nil) == nil || ValidateEpochRecoveryGet(nil) == nil || ValidateEpochRecoveryCommand(nil, true) == nil || ValidateEpochRecoveryPending(nil) == nil || ValidateEpochCatalogRequest(nil) == nil {
		t.Fatal("nil accepted")
	}
	for _, limit := range []uint32{0, 33, math.MaxUint32} {
		if ValidateEpochRecoveryPending(&v2.GetPendingEpochRecoveriesRequest{Context: c, Limit: limit}) == nil {
			t.Fatal("unbounded discovery")
		}
	}
	if ValidateEpochRecoveryPending(&v2.GetPendingEpochRecoveriesRequest{Context: c, Limit: 1, AfterConversationId: "not UUID"}) == nil {
		t.Fatal("bad cursor")
	}
	for _, epoch := range []*uint64{nil, proto.Uint64(0), proto.Uint64(math.MaxInt64)} {
		if ValidateEpochCatalogRequest(&v2.GetConversationEpochsRequest{Context: c, ConversationId: e.ConversationId, AfterEpoch: epoch, Limit: 32}) != nil {
			t.Fatal("valid catalog cursor")
		}
	}
	if ValidateEpochCatalogRequest(&v2.GetConversationEpochsRequest{Context: c, ConversationId: e.ConversationId, AfterEpoch: proto.Uint64(math.MaxUint64), Limit: 1}) == nil {
		t.Fatal("epoch overflow")
	}
	o := recoveryOutcome(t, "accept")
	r := &v2.RecoveryCommandRequest{Context: &pb.ClientContext{ProtocolVersion: 2, DeviceGenerationId: o.Proposal.Control.Initiator.Device.GenerationId}, Command: o.Proposal}
	if ValidateEpochRecoveryCommand(r, true) != nil || ValidateEpochRecoveryCommand(r, false) == nil {
		t.Fatal("proposal routing")
	}
	r.Command = o.Decision
	if ValidateEpochRecoveryCommand(r, false) == nil {
		t.Fatal("wrong signer role")
	}
	r.Context.DeviceGenerationId = o.Decision.Control.Peer.Device.GenerationId
	if ValidateEpochRecoveryCommand(r, false) != nil || ValidateEpochRecoveryCommand(r, true) == nil {
		t.Fatal("decision routing")
	}
	r.Command.Control.Peer.Device.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	if ValidateEpochRecoveryCommand(r, false) == nil {
		t.Fatal("unknown nested identity fields")
	}
}

func TestEpochRPCStatusResponseBindings(t *testing.T) {
	e := epochEnvelope(t)
	d, _ := EpochEnvelopeDigest(e)
	r := &pb.GetMessageStatusesRequest{Context: epochSend(e).Context, Messages: []*pb.MessageReference{{MessageId: e.MessageId, EnvelopeDigest: d[:]}}}
	const stored = int64(1000000)
	const expires = stored + 30*24*60*60*1000
	good := &v2.MessageStatuses{Messages: []*v2.MessageStatus{{Receipt: &v2.SendReceipt{MessageId: e.MessageId, EnvelopeDigest: d[:], StoredAtUnixMs: stored, ExpiresAtUnixMs: expires, CryptoEpoch: proto.Uint64(0)}, State: pb.MessageState_MESSAGE_STATE_PENDING, ObservedAtUnixMs: stored}}}
	if err := ValidateEpochStatusResponse(r, good); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*v2.MessageStatuses){
		"missing epoch":   func(v *v2.MessageStatuses) { v.Messages[0].Receipt.CryptoEpoch = nil },
		"epoch overflow":  func(v *v2.MessageStatuses) { v.Messages[0].Receipt.CryptoEpoch = proto.Uint64(math.MaxUint64) },
		"digest":          func(v *v2.MessageStatuses) { v.Messages[0].Receipt.EnvelopeDigest[0] ^= 1 },
		"UUID":            func(v *v2.MessageStatuses) { v.Messages[0].Receipt.MessageId = e.ConversationId },
		"extended TTL":    func(v *v2.MessageStatuses) { v.Messages[0].Receipt.ExpiresAtUnixMs++ },
		"time overflow":   func(v *v2.MessageStatuses) { v.Messages[0].Receipt.StoredAtUnixMs = math.MaxInt64 },
		"state":           func(v *v2.MessageStatuses) { v.Messages[0].State = 99 },
		"early expiry":    func(v *v2.MessageStatuses) { v.Messages[0].State = pb.MessageState_MESSAGE_STATE_EXPIRED },
		"pending time":    func(v *v2.MessageStatuses) { v.Messages[0].ObservedAtUnixMs++ },
		"unknown receipt": func(v *v2.MessageStatuses) { v.Messages[0].Receipt.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1}) },
		"count":           func(v *v2.MessageStatuses) { v.Messages = nil },
	} {
		t.Run(name, func(t *testing.T) {
			v := proto.Clone(good).(*v2.MessageStatuses)
			mutate(v)
			if ValidateEpochStatusResponse(r, v) == nil {
				t.Fatal("malformed status accepted")
			}
		})
	}
	for _, state := range []pb.MessageState{pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED, pb.MessageState_MESSAGE_STATE_RECIPIENT_REJECTED, pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED, pb.MessageState_MESSAGE_STATE_EXPIRED} {
		v := proto.Clone(good).(*v2.MessageStatuses)
		v.Messages[0].State = state
		v.Messages[0].ObservedAtUnixMs = expires + 1
		if ValidateEpochStatusResponse(r, v) != nil {
			t.Fatal("late valid observation rejected")
		}
	}
	bad := proto.Clone(r).(*pb.GetMessageStatusesRequest)
	bad.Messages = append(bad.Messages, bad.Messages[0])
	if ValidateEpochStatusRequest(bad) == nil {
		t.Fatal("duplicate request")
	}
	bad = proto.Clone(r).(*pb.GetMessageStatusesRequest)
	bad.Messages[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 1})
	if ValidateEpochStatusRequest(bad) == nil {
		t.Fatal("unknown reference")
	}
}
