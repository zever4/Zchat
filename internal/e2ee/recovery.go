package e2ee

// SESSION1 control-format validation only. No RPC is enabled here. In particular,
// Go does NOT verify Signal identity signatures; native peers must verify them.
import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/proto"
)

const RecoveryProtocolVersion = 2
const MaxRecoveryBytes = 4096

func recoveryVersion(v uint32) error {
	if v != RecoveryProtocolVersion {
		return ErrUnsupportedVersion
	}
	return nil
}

func recoveryKnown(m proto.Message) bool {
	return m != nil && m.ProtoReflect().IsValid() && len(m.ProtoReflect().GetUnknown()) == 0
}

func recoveryParticipant(p *pb.RecoveryParticipant) bool {
	return recoveryKnown(p) && ValidateDevice(p.Device) == nil && p.Device.DeviceId == 1 && len(p.IdentityFingerprint) == 32
}

// ValidateRecoveryControl checks all fields used by the canonical signing frame.
func ValidateRecoveryControl(c *pb.RecoveryControl) error {
	if !recoveryKnown(c) {
		return invalid("recovery control")
	}
	if err := recoveryVersion(c.ProtocolVersion); err != nil {
		return err
	}
	if c.FormatVersion != 1 || c.CryptoSuite != pb.CryptoSuite_CRYPTO_SUITE_SIGNAL_PQ_1 ||
		!uuid4(c.ConversationId) || !uuid4(c.RequestId) || !uuid4(c.ProposalId) ||
		c.ExpectedEpoch >= math.MaxInt64 || c.ExpectedRevision >= math.MaxInt64 ||
		!recoveryParticipant(c.Initiator) || !recoveryParticipant(c.Peer) || proto.Size(c) > MaxRecoveryBytes {
		return invalid("recovery fields")
	}
	if c.Initiator.Device.UserId == c.Peer.Device.UserId || c.Initiator.Device.GenerationId == c.Peer.Device.GenerationId {
		return invalid("recovery participants")
	}
	switch c.Action {
	case pb.RecoveryAction_RECOVERY_ACTION_PROPOSE:
		if c.RequestId != c.ProposalId || len(c.ProposalDigest) != 0 {
			return invalid("recovery proposal")
		}
	case pb.RecoveryAction_RECOVERY_ACTION_ACCEPT, pb.RecoveryAction_RECOVERY_ACTION_DECLINE, pb.RecoveryAction_RECOVERY_ACTION_CANCEL:
		if c.RequestId == c.ProposalId || len(c.ProposalDigest) != 32 {
			return invalid("recovery decision")
		}
	default:
		return invalid("recovery action")
	}
	return nil
}

// RecoveryFrame is the signed byte frame, NOT a protobuf serialization or hash.
func RecoveryFrame(c *pb.RecoveryControl) ([]byte, error) {
	if err := ValidateRecoveryControl(c); err != nil {
		return nil, err
	}
	b := []byte("zchat.e2ee.epoch-recovery.v1\x00")
	u32 := func(v uint32) { b = binary.BigEndian.AppendUint32(b, v) }
	u64 := func(v uint64) { b = binary.BigEndian.AppendUint64(b, v) }
	blob := func(v []byte) { u32(uint32(len(v))); b = append(b, v...) }
	u32(c.ProtocolVersion)
	u32(c.FormatVersion)
	u32(uint32(c.CryptoSuite))
	u32(uint32(c.Action))
	role := uint32(1) // Initiator signs Propose/Cancel; peer signs Accept/Decline.
	if c.Action == pb.RecoveryAction_RECOVERY_ACTION_ACCEPT || c.Action == pb.RecoveryAction_RECOVERY_ACTION_DECLINE {
		role = 2
	}
	u32(role)
	b = append(b, c.ConversationId...)
	b = append(b, c.RequestId...)
	b = append(b, c.ProposalId...)
	u64(c.ExpectedEpoch)
	u64(c.ExpectedRevision)
	for _, p := range []*pb.RecoveryParticipant{c.Initiator, c.Peer} {
		b = append(b, p.Device.UserId...)
		u32(p.Device.DeviceId)
		b = append(b, p.Device.GenerationId...)
		blob(p.IdentityFingerprint)
	}
	blob(c.ProposalDigest)
	return b, nil
}

func RecoveryDigest(c *pb.RecoveryControl) ([32]byte, error) {
	b, err := RecoveryFrame(c)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(b), nil
}

func ValidateSignedRecoveryControl(s *pb.SignedRecoveryControl) error {
	if !recoveryKnown(s) || len(s.Signature) != 64 || proto.Size(s) > MaxRecoveryBytes {
		return invalid("signed recovery control")
	}
	return ValidateRecoveryControl(s.Control)
}

func sameRecoveryContext(a, b *pb.RecoveryControl) bool {
	return a.ProtocolVersion == b.ProtocolVersion && a.FormatVersion == b.FormatVersion && a.CryptoSuite == b.CryptoSuite &&
		a.ConversationId == b.ConversationId && a.ProposalId == b.ProposalId && a.ExpectedEpoch == b.ExpectedEpoch &&
		a.ExpectedRevision == b.ExpectedRevision && proto.Equal(a.Initiator, b.Initiator) && proto.Equal(a.Peer, b.Peer)
}

// ValidateRecoveryOutcome binds a response to the EXACT durable proposal. This is
// structural validation, not signature verification or proof of a database COMMIT.
func ValidateRecoveryOutcome(o *pb.RecoveryOutcome, expected *pb.SignedRecoveryControl) error {
	if !recoveryKnown(o) || proto.Size(o) > MaxRecoveryBytes {
		return invalid("recovery outcome")
	}
	if err := recoveryVersion(o.ProtocolVersion); err != nil {
		return err
	}
	if ValidateSignedRecoveryControl(expected) != nil || ValidateSignedRecoveryControl(o.Proposal) != nil ||
		expected.Control.Action != pb.RecoveryAction_RECOVERY_ACTION_PROPOSE || !proto.Equal(o.Proposal, expected) {
		return invalid("recovery outcome proposal")
	}
	p := o.Proposal.Control
	var action pb.RecoveryAction
	switch o.State {
	case pb.RecoveryState_RECOVERY_STATE_ACCEPTED:
		action = pb.RecoveryAction_RECOVERY_ACTION_ACCEPT
	case pb.RecoveryState_RECOVERY_STATE_DECLINED:
		action = pb.RecoveryAction_RECOVERY_ACTION_DECLINE
	case pb.RecoveryState_RECOVERY_STATE_CANCELLED:
		action = pb.RecoveryAction_RECOVERY_ACTION_CANCEL
	case pb.RecoveryState_RECOVERY_STATE_PENDING, pb.RecoveryState_RECOVERY_STATE_EXPIRED, pb.RecoveryState_RECOVERY_STATE_SUPERSEDED:
		if o.Decision != nil {
			return invalid("unexpected recovery decision")
		}
	default:
		return invalid("recovery state")
	}
	if action != pb.RecoveryAction_RECOVERY_ACTION_UNSPECIFIED {
		if ValidateSignedRecoveryControl(o.Decision) != nil || o.Decision.Control.Action != action || !sameRecoveryContext(p, o.Decision.Control) {
			return invalid("recovery outcome decision")
		}
		digest, _ := RecoveryDigest(p)
		if !bytes.Equal(o.Decision.Control.ProposalDigest, digest[:]) {
			return invalid("decision proposal digest")
		}
	}
	if o.State == pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
		if o.NewEpoch != p.ExpectedEpoch+1 || o.EpochRevision != p.ExpectedRevision+1 {
			return invalid("recovery CAS result")
		}
	} else if o.NewEpoch != 0 || o.EpochRevision != 0 {
		return invalid("unexpected recovery CAS result")
	}
	return nil
}

func ValidateEpochSendBinding(b *pb.EpochSendBinding) error {
	if !recoveryKnown(b) {
		return invalid("epoch Send binding")
	}
	if err := epochFramingVersion(b.ProtocolVersion); err != nil {
		return err
	}
	if !uuid4(b.ConversationId) || !uuid4(b.MessageId) || b.Epoch > math.MaxInt64 || len(b.EnvelopeDigest) != 32 ||
		ValidateDevice(b.Sender) != nil || ValidateDevice(b.Recipient) != nil || b.Sender.DeviceId != 1 || b.Recipient.DeviceId != 1 ||
		b.Sender.UserId == b.Recipient.UserId || b.Sender.GenerationId == b.Recipient.GenerationId {
		return invalid("epoch Send fields")
	}
	return nil
}

// ValidateEpochSealedFence requires a matching accepted transition in addition to
// the exact Send UUID/digest/participants/epoch. No generic gRPC error can pass it.
func ValidateEpochSealedFence(f *pb.EpochSealedFence, expected *pb.EpochSendBinding, accepted *pb.RecoveryOutcome) error {
	if !recoveryKnown(f) || ValidateEpochSendBinding(expected) != nil || ValidateEpochSendBinding(f.Binding) != nil ||
		!proto.Equal(f.Binding, expected) || accepted == nil || ValidateRecoveryOutcome(accepted, accepted.Proposal) != nil ||
		accepted.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
		return invalid("epoch fence")
	}
	p := accepted.Proposal.Control
	digest, _ := RecoveryDigest(p)
	a, b := p.Initiator.Device, p.Peer.Device
	participants := proto.Equal(expected.Sender, a) && proto.Equal(expected.Recipient, b) || proto.Equal(expected.Sender, b) && proto.Equal(expected.Recipient, a)
	if !participants || expected.ConversationId != p.ConversationId || expected.Epoch != p.ExpectedEpoch ||
		!bytes.Equal(f.ProposalDigest, digest[:]) || f.NextEpoch != accepted.NewEpoch || f.EpochRevision != accepted.EpochRevision {
		return invalid("epoch fence binding")
	}
	return nil
}

// ValidateEpochVersionBinding requires v2 even for the initial epoch0. Development
// builds do not need a legacy v1 reader or an automatic old-state migration.
func ValidateEpochVersionBinding(protocol uint32, epoch, authoritativeEpoch uint64) error {
	if epoch > math.MaxInt64 || authoritativeEpoch > math.MaxInt64 || epoch != authoritativeEpoch {
		return invalid("data epoch binding")
	}
	if protocol == RecoveryProtocolVersion {
		return nil
	}
	return ErrUnsupportedVersion
}
