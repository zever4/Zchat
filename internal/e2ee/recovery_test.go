package e2ee

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"strings"
	"testing"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"google.golang.org/protobuf/proto"
)

const recoveryFixtures = "../../proto/fixtures/epoch-recovery-v1/"

func recoveryFile(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(recoveryFixtures + name)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func recoveryOutcome(t *testing.T, name string) *pb.RecoveryOutcome {
	t.Helper()
	o := new(pb.RecoveryOutcome)
	if err := proto.Unmarshal(recoveryFile(t, name+".bin"), o); err != nil {
		t.Fatal(err)
	}
	return o
}

func TestRecoverySharedCanonicalVectors(t *testing.T) {
	hashes := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(string(recoveryFile(t, "frames.sha256"))), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatal("bad fixture hash manifest")
		}
		hashes[fields[1]] = fields[0]
	}
	for _, name := range []string{"accept", "decline", "cancel"} {
		t.Run(name, func(t *testing.T) {
			o := recoveryOutcome(t, name)
			if err := ValidateRecoveryOutcome(o, o.Proposal); err != nil {
				t.Fatal(err)
			}
			for label, s := range map[string]*pb.SignedRecoveryControl{"propose": o.Proposal, name: o.Decision} {
				frame, err := RecoveryFrame(s.Control)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(frame, recoveryFile(t, label+".frame")) {
					t.Fatal("canonical frame drift")
				}
				digest, err := RecoveryDigest(s.Control)
				if err != nil {
					t.Fatal(err)
				}
				if hex.EncodeToString(digest[:]) != hashes[label+".frame"] {
					t.Fatal("digest drift")
				}
				wire, _ := proto.Marshal(s.Control)
				if !bytes.Equal(wire[:2], []byte{8, 2}) {
					t.Fatal("unexpected version field")
				}
				reordered := append(append([]byte{}, wire[2:]...), wire[:2]...)
				var decoded pb.RecoveryControl
				if err := proto.Unmarshal(reordered, &decoded); err != nil {
					t.Fatal(err)
				}
				rehash, err := RecoveryDigest(&decoded)
				if err != nil || digest != rehash {
					t.Fatal("wire order changed digest")
				}
			}
			p := o.Proposal.Control
			if p.ExpectedEpoch != 1<<53+17 || p.ExpectedRevision != 1<<53+29 {
				t.Fatal("full-width epoch truncated")
			}
			for file, participant := range map[string]*pb.RecoveryParticipant{"initiator.pub": p.Initiator, "peer.pub": p.Peer} {
				key := recoveryFile(t, file)
				if len(key) != 33 || key[0] != 5 {
					t.Fatal("bad synthetic public key")
				}
				fingerprint := sha256.Sum256(key)
				if !bytes.Equal(fingerprint[:], participant.IdentityFingerprint) {
					t.Fatal("identity fingerprint mismatch")
				}
			}
		})
	}
}

func TestRecoveryControlRejectsInvalidOrUnknownSemantics(t *testing.T) {
	original := recoveryOutcome(t, "accept").Proposal.Control
	cases := map[string]func(*pb.RecoveryControl){
		"legacy downgrade":    func(c *pb.RecoveryControl) { c.ProtocolVersion = 1 },
		"future protocol":     func(c *pb.RecoveryControl) { c.ProtocolVersion = 3 },
		"future format":       func(c *pb.RecoveryControl) { c.FormatVersion = 2 },
		"suite":               func(c *pb.RecoveryControl) { c.CryptoSuite = 99 },
		"action":              func(c *pb.RecoveryControl) { c.Action = 99 },
		"epoch overflow":      func(c *pb.RecoveryControl) { c.ExpectedEpoch = math.MaxInt64 },
		"revision overflow":   func(c *pb.RecoveryControl) { c.ExpectedRevision = math.MaxUint64 },
		"proposal identity":   func(c *pb.RecoveryControl) { c.ProposalId = c.ConversationId },
		"proposal digest":     func(c *pb.RecoveryControl) { c.ProposalDigest = make([]byte, 32) },
		"uppercase UUID":      func(c *pb.RecoveryControl) { c.ConversationId = "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA" },
		"same account":        func(c *pb.RecoveryControl) { c.Peer.Device.UserId = c.Initiator.Device.UserId },
		"same generation":     func(c *pb.RecoveryControl) { c.Peer.Device.GenerationId = c.Initiator.Device.GenerationId },
		"missing peer":        func(c *pb.RecoveryControl) { c.Peer = nil },
		"wrong device":        func(c *pb.RecoveryControl) { c.Peer.Device.DeviceId = 2 },
		"fingerprint":         func(c *pb.RecoveryControl) { c.Peer.IdentityFingerprint = nil },
		"unknown control":     func(c *pb.RecoveryControl) { c.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown participant": func(c *pb.RecoveryControl) { c.Peer.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown device":      func(c *pb.RecoveryControl) { c.Peer.Device.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			c := proto.Clone(original).(*pb.RecoveryControl)
			mutate(c)
			if _, err := RecoveryFrame(c); err == nil {
				t.Fatal("invalid control accepted")
			}
		})
	}
	if err := ValidateRecoveryControl(nil); err == nil {
		t.Fatal("nil control accepted")
	}
	if err := ValidateSignedRecoveryControl(nil); err == nil {
		t.Fatal("nil signature accepted")
	}
	max := proto.Clone(original).(*pb.RecoveryControl)
	max.ExpectedEpoch = math.MaxInt64 - 1
	max.ExpectedRevision = math.MaxInt64 - 1
	if err := ValidateRecoveryControl(max); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryOutcomeRejectsReplayAndMismatchedDecision(t *testing.T) {
	original := recoveryOutcome(t, "accept")
	cases := map[string]func(*pb.RecoveryOutcome){
		"missing signature":          func(o *pb.RecoveryOutcome) { o.Decision.Signature = nil },
		"wrong proposal":             func(o *pb.RecoveryOutcome) { o.Proposal.Control.RequestId = o.Proposal.Control.ConversationId },
		"changed proposal signature": func(o *pb.RecoveryOutcome) { o.Proposal.Signature[0] ^= 1 },
		"decision replay":            func(o *pb.RecoveryOutcome) { o.Decision.Control.ProposalDigest[0] ^= 1 },
		"epoch replay":               func(o *pb.RecoveryOutcome) { o.Decision.Control.ExpectedEpoch++ },
		"revision replay":            func(o *pb.RecoveryOutcome) { o.Decision.Control.ExpectedRevision++ },
		"conversation replay":        func(o *pb.RecoveryOutcome) { o.Decision.Control.ConversationId = o.Decision.Control.RequestId },
		"peer generation replay": func(o *pb.RecoveryOutcome) {
			o.Decision.Control.Peer.Device.GenerationId = o.Decision.Control.RequestId
		},
		"identity replay":           func(o *pb.RecoveryOutcome) { o.Decision.Control.Peer.IdentityFingerprint[0] ^= 1 },
		"wrong role":                func(o *pb.RecoveryOutcome) { o.Decision.Control.Action = pb.RecoveryAction_RECOVERY_ACTION_CANCEL },
		"same operation id":         func(o *pb.RecoveryOutcome) { o.Decision.Control.RequestId = o.Decision.Control.ProposalId },
		"epoch result":              func(o *pb.RecoveryOutcome) { o.NewEpoch++ },
		"revision result":           func(o *pb.RecoveryOutcome) { o.EpochRevision++ },
		"missing decision":          func(o *pb.RecoveryOutcome) { o.Decision = nil },
		"unknown outcome":           func(o *pb.RecoveryOutcome) { o.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown signature wrapper": func(o *pb.RecoveryOutcome) { o.Decision.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"state":                     func(o *pb.RecoveryOutcome) { o.State = 99 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			o := proto.Clone(original).(*pb.RecoveryOutcome)
			mutate(o)
			if err := ValidateRecoveryOutcome(o, original.Proposal); err == nil {
				t.Fatal("invalid outcome accepted")
			}
		})
	}
	for _, state := range []pb.RecoveryState{pb.RecoveryState_RECOVERY_STATE_PENDING, pb.RecoveryState_RECOVERY_STATE_EXPIRED, pb.RecoveryState_RECOVERY_STATE_SUPERSEDED} {
		o := &pb.RecoveryOutcome{ProtocolVersion: 2, Proposal: original.Proposal, State: state}
		if err := ValidateRecoveryOutcome(o, original.Proposal); err != nil {
			t.Fatal(err)
		}
		o.Decision = original.Decision
		if err := ValidateRecoveryOutcome(o, original.Proposal); err == nil {
			t.Fatal("unexpected decision accepted")
		}
	}
	// Deliberate boundary: shape-valid forged signatures cannot be detected by Go.
	o := proto.Clone(original).(*pb.RecoveryOutcome)
	o.Decision.Signature[0] ^= 1
	if err := ValidateRecoveryOutcome(o, original.Proposal); err != nil {
		t.Fatal("Go unexpectedly claims signature verification")
	}
}

func TestEpochFenceRequiresExactSendAndAcceptedTransition(t *testing.T) {
	o := recoveryOutcome(t, "accept")
	f := new(pb.EpochSealedFence)
	if err := proto.Unmarshal(recoveryFile(t, "fence.bin"), f); err != nil {
		t.Fatal(err)
	}
	expected := proto.Clone(f.Binding).(*pb.EpochSendBinding)
	if err := ValidateEpochSealedFence(f, expected, o); err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(*pb.EpochSealedFence){
		"digest":          func(f *pb.EpochSealedFence) { f.Binding.EnvelopeDigest[0] ^= 1 },
		"UUID":            func(f *pb.EpochSealedFence) { f.Binding.MessageId = f.Binding.ConversationId },
		"chat":            func(f *pb.EpochSealedFence) { f.Binding.ConversationId = f.Binding.MessageId },
		"epoch":           func(f *pb.EpochSealedFence) { f.Binding.Epoch++ },
		"sender":          func(f *pb.EpochSealedFence) { f.Binding.Sender.GenerationId = f.Binding.MessageId },
		"recipient":       func(f *pb.EpochSealedFence) { f.Binding.Recipient.GenerationId = f.Binding.MessageId },
		"protocol":        func(f *pb.EpochSealedFence) { f.Binding.ProtocolVersion = 1 },
		"transition":      func(f *pb.EpochSealedFence) { f.ProposalDigest[0] ^= 1 },
		"next epoch":      func(f *pb.EpochSealedFence) { f.NextEpoch++ },
		"revision":        func(f *pb.EpochSealedFence) { f.EpochRevision++ },
		"missing binding": func(f *pb.EpochSealedFence) { f.Binding = nil },
		"unknown fence":   func(f *pb.EpochSealedFence) { f.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		"unknown binding": func(f *pb.EpochSealedFence) { f.Binding.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(f).(*pb.EpochSealedFence)
			mutate(bad)
			if ValidateEpochSealedFence(bad, expected, o) == nil {
				t.Fatal("unbound fence accepted")
			}
		})
	}
	if ValidateEpochSealedFence(nil, expected, o) == nil || ValidateEpochSealedFence(f, expected, nil) == nil {
		t.Fatal("missing proof accepted")
	}
	if ValidateEpochSealedFence(f, expected, recoveryOutcome(t, "decline")) == nil {
		t.Fatal("declined recovery treated as sealed")
	}
	// A self-consistent but unrelated Send binding must also match the transition.
	bad := proto.Clone(f).(*pb.EpochSealedFence)
	bad.Binding.Epoch++
	if ValidateEpochSealedFence(bad, bad.Binding, o) == nil {
		t.Fatal("unrelated epoch accepted")
	}
}

func TestRecoveryEpochVersionBoundaryRejectsLegacy(t *testing.T) {
	for _, c := range []struct {
		v             uint32
		epoch, actual uint64
		ok            bool
	}{
		{1, 0, 0, false}, {1, 1, 1, false}, {2, 0, 0, true}, {2, 7, 7, true}, {2, 7, 8, false}, {0, 0, 0, false}, {3, 0, 0, false},
		{2, math.MaxInt64, math.MaxInt64, true}, {2, math.MaxUint64, math.MaxUint64, false},
	} {
		if (ValidateEpochVersionBinding(c.v, c.epoch, c.actual) == nil) != c.ok {
			t.Fatalf("unexpected routing boundary: %+v", c)
		}
	}
	// No incomplete protocol2 parser was enabled in the live v1 validator.
	wire, err := os.ReadFile("../../proto/fixtures/e2ee-v1-envelope.bin")
	if err != nil {
		t.Fatal(err)
	}
	var envelope pb.Envelope
	if err := proto.Unmarshal(wire, &envelope); err != nil {
		t.Fatal(err)
	}
	if err := ValidateEnvelope(&envelope); err != nil {
		t.Fatal(err)
	}
	envelope.ProtocolVersion = 2
	if ValidateEnvelope(&envelope) == nil {
		t.Fatal("v1 validator silently accepts v2")
	}
}
