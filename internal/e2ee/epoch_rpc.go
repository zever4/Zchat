package e2ee

import (
	"bytes"
	"math"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"google.golang.org/protobuf/proto"
)

func ValidateEpochContext(c *pb.ClientContext) error { return epochContext(c) }
func epochRequest(m proto.Message, c *pb.ClientContext) error {
	if !recoveryKnown(m) {
		return invalid("v2 request fields")
	}
	if err := epochContext(c); err != nil {
		return err
	}
	return wireSize(m)
}
func ValidateEpochPendingRequest(r *v2.GetPendingMessagesRequest) error {
	if r == nil {
		return invalid("v2 pending request")
	}
	return epochRequest(r, r.Context)
}
func ValidateEpochStatusRequest(r *pb.GetMessageStatusesRequest) error {
	if r == nil {
		return invalid("v2 status request")
	}
	if err := epochRequest(r, r.Context); err != nil {
		return err
	}
	if len(r.Messages) < 1 || len(r.Messages) > MaxAcknowledgements {
		return invalid("v2 status count")
	}
	ids := map[string]bool{}
	for _, m := range r.Messages {
		if !recoveryKnown(m) || !uuid4(m.MessageId) || len(m.EnvelopeDigest) != 32 || ids[m.MessageId] {
			return invalid("v2 status reference")
		}
		ids[m.MessageId] = true
	}
	return nil
}
func ValidateEpochRecoveryCommand(r *v2.RecoveryCommandRequest, proposal bool) error {
	if r == nil {
		return invalid("v2 recovery request")
	}
	if err := epochRequest(r, r.Context); err != nil {
		return err
	}
	if err := ValidateSignedRecoveryControl(r.Command); err != nil {
		return err
	}
	c := r.Command.Control
	if (c.Action == pb.RecoveryAction_RECOVERY_ACTION_PROPOSE) != proposal {
		return invalid("v2 recovery action")
	}
	actor := c.Initiator
	if c.Action == pb.RecoveryAction_RECOVERY_ACTION_ACCEPT || c.Action == pb.RecoveryAction_RECOVERY_ACTION_DECLINE {
		actor = c.Peer
	}
	if actor.Device.GenerationId != r.Context.DeviceGenerationId {
		return invalid("v2 recovery actor")
	}
	return nil
}
func ValidateEpochRecoveryGet(r *v2.GetEpochRecoveryRequest) error {
	if r == nil {
		return invalid("v2 recovery lookup")
	}
	if err := epochRequest(r, r.Context); err != nil {
		return err
	}
	if !uuid4(r.ConversationId) || !uuid4(r.ProposalId) {
		return invalid("v2 recovery lookup IDs")
	}
	return nil
}
func ValidateEpochRecoveryPending(r *v2.GetPendingEpochRecoveriesRequest) error {
	if r == nil {
		return invalid("v2 recovery page")
	}
	if err := epochRequest(r, r.Context); err != nil {
		return err
	}
	if r.Limit < 1 || r.Limit > 32 || (r.AfterConversationId != "" && !uuid4(r.AfterConversationId)) {
		return invalid("v2 recovery page cursor")
	}
	return nil
}
func ValidateEpochCatalogRequest(r *v2.GetConversationEpochsRequest) error {
	if r == nil {
		return invalid("v2 catalog request")
	}
	if err := epochRequest(r, r.Context); err != nil {
		return err
	}
	if !uuid4(r.ConversationId) || r.Limit < 1 || r.Limit > 32 || (r.AfterEpoch != nil && epochValue(r.AfterEpoch) != nil) {
		return invalid("v2 catalog cursor")
	}
	return nil
}

func ValidateEpochStatusResponse(req *pb.GetMessageStatusesRequest, response *v2.MessageStatuses) error {
	if err := ValidateEpochStatusRequest(req); err != nil {
		return err
	}
	if !recoveryKnown(response) || proto.Size(response) > MaxResponseBytes || len(response.Messages) != len(req.Messages) {
		return invalid("v2 status response")
	}
	const ttl = int64(30 * 24 * 60 * 60 * 1000)
	for i, m := range response.Messages {
		if !recoveryKnown(m) || !recoveryKnown(m.Receipt) || epochValue(m.Receipt.CryptoEpoch) != nil {
			return invalid("v2 status receipt")
		}
		r := m.Receipt
		ref := req.Messages[i]
		if r.MessageId != ref.MessageId || !bytes.Equal(r.EnvelopeDigest, ref.EnvelopeDigest) || r.StoredAtUnixMs <= 0 || r.StoredAtUnixMs > math.MaxInt64-ttl || r.ExpiresAtUnixMs != r.StoredAtUnixMs+ttl || m.ObservedAtUnixMs < r.StoredAtUnixMs {
			return invalid("v2 status binding")
		}
		switch m.State {
		case pb.MessageState_MESSAGE_STATE_PENDING:
			if m.ObservedAtUnixMs != r.StoredAtUnixMs {
				return invalid("v2 pending status time")
			}
		case pb.MessageState_MESSAGE_STATE_EXPIRED:
			if m.ObservedAtUnixMs < r.ExpiresAtUnixMs {
				return invalid("v2 expiry status time")
			}
		case pb.MessageState_MESSAGE_STATE_RECIPIENT_STORED, pb.MessageState_MESSAGE_STATE_RECIPIENT_REJECTED, pb.MessageState_MESSAGE_STATE_RECIPIENT_REPLACED:
		default:
			return invalid("v2 status state")
		}
	}
	return nil
}
