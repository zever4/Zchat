package repository

import (
	"context"
	"database/sql"
	"errors"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// SendV2 validates epoch-aware framing2/3 before sharing the atomic ledger and
// inbox transaction, never a v1 parser/digest fallback. The production service
// separately gates attachment admission until exact-file retry/interop is checked.
func (r *PgE2EEDeliveryRepository) SendV2(ctx context.Context, uid int64, req *v2.SendMessageRequest) (*v2.SendReceipt, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochSend(req); err != nil {
		return nil, err
	}
	e := req.Envelope
	digest, err := boundary.EpochEnvelopeDigest(e)
	if err != nil {
		return nil, err
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(e)
	if err != nil {
		return nil, err
	}
	epoch := *e.CryptoEpoch
	result, err := r.sendPrepared(ctx, uid, &preparedE2EESend{MessageId: e.MessageId, ConversationId: e.ConversationId, Sender: e.Sender, Recipient: e.Recipient,
		generation: req.Context.DeviceGenerationId, digest: digest, wire: wire, epoch: &epoch, blob: e.Blob})
	if err != nil {
		return nil, err
	}
	s := &v2.SendReceipt{MessageId: result.MessageID, EnvelopeDigest: result.Digest[:], StoredAtUnixMs: result.StoredAt.UnixMilli(), ExpiresAtUnixMs: result.ExpiresAt.UnixMilli(), CryptoEpoch: result.CryptoEpoch}
	if boundary.ValidateEpochReceipt(req, s) != nil {
		return nil, domain.ErrE2EEMessageState
	}
	return s, nil
}

// Caller holds canonical user/device/conversation locks. A committed receipt
// was already looked up BEFORE reaching this gate. The seal is permanent.
func gateEpochSend(ctx context.Context, tx *sql.Tx, c *domain.E2EEConversation, low, high int64, lowGen, highGen string, e *preparedE2EESend) error {
	if !messageDeviceMatches(e.Recipient, c.PeerDevice) {
		return domain.ErrE2EEGenerationChanged
	}
	var current uint64
	var state, proposal string
	err := tx.QueryRowContext(ctx, `SELECT c.active_epoch,x.state,COALESCE(x.sealed_by_proposal_id::text,'') FROM e2ee_conversations c
        JOIN e2ee_conversation_epochs x ON x.conversation_id=c.conversation_id AND x.epoch=$2 WHERE c.conversation_id=$1 FOR SHARE OF c,x`, c.ID, *e.epoch).Scan(&current, &state, &proposal)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ErrE2EEEpochInactive
	}
	if err != nil {
		return err
	}
	if state == "active" && current == *e.epoch {
		return nil
	}
	if state != "sealed" || proposal == "" {
		return domain.ErrE2EEEpochInactive
	}
	rc := &recoveryContext{tx: tx, c: c, low: low, high: high, lowGen: lowGen, highGen: highGen}
	saved, err := readRecovery(ctx, rc, proposal)
	if err != nil {
		return err
	}
	if saved.Outcome.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
		return domain.ErrE2EERecoveryState
	}
	p := saved.Outcome.Proposal.Control
	digest, err := boundary.RecoveryDigest(p)
	if err != nil {
		return domain.ErrE2EERecoveryState
	}
	binding := &pb.EpochSendBinding{ProtocolVersion: e.version(), ConversationId: e.ConversationId, Epoch: *e.epoch, MessageId: e.MessageId,
		Sender: proto.Clone(e.Sender).(*pb.DeviceRef), Recipient: proto.Clone(e.Recipient).(*pb.DeviceRef), EnvelopeDigest: append([]byte(nil), e.digest[:]...)}
	f := &pb.EpochSealedFence{Binding: binding, ProposalDigest: digest[:], NextEpoch: saved.Outcome.NewEpoch, EpochRevision: saved.Outcome.EpochRevision}
	if boundary.ValidateEpochSealedFence(f, binding, saved.Outcome) != nil {
		return domain.ErrE2EERecoveryState
	}
	return &domain.E2EEEpochSealed{Fence: f, Recovery: saved.Outcome}
}
