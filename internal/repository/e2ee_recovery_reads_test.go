package repository

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	"github.com/zever4/Zchat/internal/testutil"
)

func TestE2EERecoveryReadPaginationAndCatalogPostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	f.propose(t, f.p, 1)
	page, err := f.repo.PendingRecoveries(t.Context(), 2, f.gen(2), "", 1)
	if err != nil || len(page.Proposals) != 1 || page.Proposals[0].InitiatedByOwn || page.Proposals[0].ProposalID != f.p.Control.ProposalId || page.NextConversationID != "" {
		t.Fatal("incoming discovery", err)
	}
	// Another pair in the SAME schema makes pagination exercise LIMIT+1 rather
	// than a single-conversation fixture. Account1 initiates both proposals.
	if _, err = f.f.db.ExecContext(t.Context(), `INSERT INTO users(id,user_uuid,username,email,password_hash)VALUES(3,'33333333-3333-4333-8333-333333333333','c','c@epoch.invalid','test-hash');INSERT INTO user_settings(user_id)VALUES(3);INSERT INTO friend_requests(from_user_id,to_user_id,status)VALUES(1,3,'accepted')`); err != nil {
		t.Fatal(err)
	}
	m := registrationMaterial()
	m.RegistrationID = 16383
	d, err := f.f.devices.Register(t.Context(), 3, m)
	if err != nil {
		t.Fatal(err)
	}
	// Discovery requires no prekey consumption; publication readiness is used
	// only to create the second conversation through its normal repository.
	if _, err = f.keys.Publish(t.Context(), 3, testutil.PublicPreKeyUpload(t, d.Device.GenerationID)); err != nil {
		t.Fatal(err)
	}
	c, err := f.f.repo.Create(t.Context(), 1, &pb.CreateConversationRequest{Context: &pb.ClientContext{ProtocolVersion: 1, DeviceGenerationId: f.gen(1)}, CreationRequestId: uuid.NewString(), PeerUserId: 3})
	if err != nil {
		t.Fatal(err)
	}
	other := &recoveryFixture{f: &conversationFixture{db: f.f.db, low: f.f.low, high: d.Device}, repo: f.repo, chat: c.ID}
	other.p = other.proposal(1, 0, 0)
	other.propose(t, other.p, 1)
	first, err := f.repo.PendingRecoveries(t.Context(), 1, f.gen(1), "", 1)
	if err != nil || len(first.Proposals) != 1 || first.NextConversationID != first.Proposals[0].ConversationID || !first.Proposals[0].InitiatedByOwn {
		t.Fatal("first page", err)
	}
	second, err := f.repo.PendingRecoveries(t.Context(), 1, f.gen(1), first.NextConversationID, 1)
	if err != nil || len(second.Proposals) != 1 || second.NextConversationID != "" || first.Proposals[0].ConversationID >= second.Proposals[0].ConversationID {
		t.Fatal("second page", err)
	}
	third, err := f.repo.PendingRecoveries(t.Context(), 3, d.Device.GenerationID, "", 32)
	if err != nil || len(third.Proposals) != 1 || third.Proposals[0].ConversationID != c.ID {
		t.Fatal("cross-pair leak", err)
	}
	if _, err = f.repo.Get(t.Context(), 3, d.Device.GenerationID, f.chat, f.p.Control.ProposalId); !errors.Is(err, domain.ErrE2EEConversationNotFound) {
		t.Fatal("outsider exact lookup", err)
	}
	f.decide(t, recoveryDecision(f.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT), 2)
	for epoch := uint64(1); epoch < 3; epoch++ {
		p := f.proposal(2, epoch, epoch)
		f.propose(t, p, 2)
		f.decide(t, recoveryDecision(p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT), 1)
	}
	var after *uint64
	for epoch := uint64(0); epoch <= 3; epoch++ {
		catalog, err := f.repo.EpochCatalog(t.Context(), 1, f.gen(1), f.chat, after, 1)
		if err != nil || catalog.ActiveEpoch != 3 || catalog.Revision != 3 || len(catalog.Epochs) != 1 || catalog.Epochs[0].Epoch != epoch || catalog.Epochs[0].Sealed != (epoch < 3) {
			t.Fatal("incoherent catalog", epoch, err)
		}
		if epoch < 3 {
			if catalog.NextAfterEpoch == nil || *catalog.NextAfterEpoch != epoch {
				t.Fatal("missing epoch cursor")
			}
		} else if catalog.NextAfterEpoch != nil {
			t.Fatal("spurious final cursor")
		}
		after = &epoch
	}
	page, err = f.repo.PendingRecoveries(t.Context(), 1, f.gen(1), "", 32)
	if err != nil || len(page.Proposals) != 1 || page.Proposals[0].ConversationID != c.ID {
		t.Fatal("terminal still pending", err)
	}
}

func TestE2EERecoveryReadExpiryAndRetiredOwnerPostgres(t *testing.T) {
	f := epochPersistenceDB(t)
	f.repo.lease = time.Millisecond
	f.propose(t, f.p, 1)
	if _, err := f.f.db.ExecContext(t.Context(), `SELECT pg_sleep(0.025)`); err != nil {
		t.Fatal(err)
	}
	page, err := f.repo.PendingRecoveries(t.Context(), 1, f.gen(1), "", 32)
	if err != nil || len(page.Proposals) != 0 {
		t.Fatal("expired advisory reference", err)
	}
	got, err := f.repo.Get(t.Context(), 1, f.gen(1), f.chat, f.p.Control.ProposalId)
	if err != nil || got.Outcome.State != pb.RecoveryState_RECOVERY_STATE_EXPIRED {
		t.Fatal("exact expiry authority", err)
	}
	f.repo.lease = domain.E2EERecoveryProposalLease
	p := f.proposal(1, 0, 0)
	f.propose(t, p, 1)
	f.f.replace(t, 2, f.gen(2))
	page, err = f.repo.PendingRecoveries(t.Context(), 1, f.gen(1), "", 32)
	if err != nil || len(page.Proposals) != 0 {
		t.Fatal("closed chat discovered", err)
	}
	if _, err = f.repo.PendingRecoveries(t.Context(), 2, f.gen(2), "", 32); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
		t.Fatal("retired owner discovery", err)
	}
	c, err := f.repo.EpochCatalog(t.Context(), 1, f.gen(1), f.chat, nil, 32)
	if err != nil || c.Conversation.State != "closed" || c.ActiveEpoch != 0 {
		t.Fatal("closed catalog lost", err)
	}
}
