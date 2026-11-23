package repository

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pressly/goose"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"github.com/zever4/Zchat/internal/testutil"
	"google.golang.org/protobuf/proto"
)

type recoveryFixture struct {
	f    *conversationFixture
	repo *PgE2EERecoveryRepository
	chat string
	p    *pb.SignedRecoveryControl
}

func recoveryDB(t *testing.T) *recoveryFixture {
	t.Helper()
	f := conversationDB(t)
	c := f.create(t)
	r := &recoveryFixture{f: f, repo: NewPgE2EERecoveryRepository(f.db), chat: c.ID}
	r.p = r.proposal(1, 0, 0)
	return r
}
func (r *recoveryFixture) participant(id int64) *pb.RecoveryParticipant {
	d := r.f.low
	if id == 2 {
		d = r.f.high
	}
	hash := sha256.Sum256(d.IdentityKey)
	return &pb.RecoveryParticipant{Device: &pb.DeviceRef{UserId: d.UserUUID, DeviceId: d.DeviceID, GenerationId: d.GenerationID}, IdentityFingerprint: hash[:]}
}
func (r *recoveryFixture) gen(id int64) string {
	if id == 1 {
		return r.f.low.GenerationID
	}
	return r.f.high.GenerationID
}
func (r *recoveryFixture) proposal(initiator int64, epoch, revision uint64) *pb.SignedRecoveryControl {
	id := uuid.NewString()
	peer := int64(2)
	if initiator == 2 {
		peer = 1
	}
	// Synthetic signature SHAPES only: Go is not a Signal verifier. These tests
	// exercise SQL ownership/CAS/replay, NOT actual identity signatures/UI consent.
	return &pb.SignedRecoveryControl{Control: &pb.RecoveryControl{ProtocolVersion: 2, FormatVersion: 1, CryptoSuite: 1, Action: pb.RecoveryAction_RECOVERY_ACTION_PROPOSE,
		ConversationId: r.chat, RequestId: id, ProposalId: id, ExpectedEpoch: epoch, ExpectedRevision: revision, Initiator: r.participant(initiator), Peer: r.participant(peer)}, Signature: bytes.Repeat([]byte{0x42}, 64)}
}
func recoveryDecision(p *pb.SignedRecoveryControl, action pb.RecoveryAction) *pb.SignedRecoveryControl {
	s := proto.Clone(p).(*pb.SignedRecoveryControl)
	s.Control.Action = action
	s.Control.RequestId = uuid.NewString()
	hash, _ := boundary.RecoveryDigest(p.Control)
	s.Control.ProposalDigest = hash[:]
	return s
}
func (r *recoveryFixture) propose(t *testing.T, p *pb.SignedRecoveryControl, uid int64) *domain.E2EERecovery {
	t.Helper()
	got, err := r.repo.Propose(t.Context(), uid, r.gen(uid), p)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func (r *recoveryFixture) decide(t *testing.T, s *pb.SignedRecoveryControl, uid int64) *domain.E2EERecovery {
	t.Helper()
	got, err := r.repo.Decide(t.Context(), uid, r.gen(uid), s)
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func (r *recoveryFixture) head(t *testing.T, expected uint64) {
	t.Helper()
	var e, rev uint64
	if err := r.f.db.QueryRowContext(t.Context(), `SELECT active_epoch,epoch_revision FROM e2ee_conversations WHERE conversation_id=$1`, r.chat).Scan(&e, &rev); err != nil || e != expected || rev != expected {
		t.Fatalf("epoch head %d/%d expected %d: %v", e, rev, expected, err)
	}
}

func TestE2EERecoveryLifecycleReplayAndEpochCatalogPostgres(t *testing.T) {
	r := recoveryDB(t)
	r.head(t, 0)
	initial, err := r.repo.Epochs(t.Context(), 1, r.gen(1), r.chat, nil, 32)
	if err != nil || len(initial) != 1 || initial[0].Epoch != 0 || initial[0].Sealed {
		t.Fatal(initial, err)
	}
	p := r.propose(t, r.p, 1)
	if p.Outcome.State != pb.RecoveryState_RECOVERY_STATE_PENDING || p.Outcome.Decision != nil {
		t.Fatal("auto-accept")
	}
	r.head(t, 0)
	if p.ExpiresAt.Sub(p.CreatedAt) != domain.E2EERecoveryProposalLease {
		t.Fatal("wrong lease")
	}
	// Reopen repository and retry exact request after a lost response.
	r.repo = NewPgE2EERecoveryRepository(r.f.db)
	again := r.propose(t, r.p, 1)
	if !proto.Equal(p.Outcome, again.Outcome) || !p.ExpiresAt.Equal(again.ExpiresAt) {
		t.Fatal("replay changed proposal/lease")
	}
	changed := proto.Clone(r.p).(*pb.SignedRecoveryControl)
	changed.Signature[0] ^= 1
	if _, err := r.repo.Propose(t.Context(), 1, r.gen(1), changed); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("re-signed retry accepted", err)
	}
	accept := recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
	if _, err := r.repo.Decide(t.Context(), 1, r.gen(1), accept); !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatal("initiator accepted for peer", err)
	}
	accepted := r.decide(t, accept, 2)
	if accepted.Outcome.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED || accepted.Outcome.NewEpoch != 1 {
		t.Fatal(accepted.Outcome)
	}
	r.head(t, 1)
	for _, s := range []*pb.SignedRecoveryControl{accept, recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_DECLINE)} {
		got := r.decide(t, s, 2)
		if !proto.Equal(got.Outcome, accepted.Outcome) || !got.ResolvedAt.Equal(*accepted.ResolvedAt) {
			t.Fatal("late/retried decision changed Accepted")
		}
	}
	cancel := recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_CANCEL)
	if got := r.decide(t, cancel, 1); !proto.Equal(got.Outcome, accepted.Outcome) {
		t.Fatal("late Cancel undid Accepted")
	}
	resigned := proto.Clone(accept).(*pb.SignedRecoveryControl)
	resigned.Signature[0] ^= 1
	if _, err := r.repo.Decide(t.Context(), 2, r.gen(2), resigned); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("re-signed decision retry accepted", err)
	}
	collision := r.proposal(1, 1, 1)
	collision.Control.RequestId = cancel.Control.RequestId
	collision.Control.ProposalId = cancel.Control.RequestId
	if _, err := r.repo.Propose(t.Context(), 1, r.gen(1), collision); !errors.Is(err, domain.ErrE2EERequestConflict) {
		t.Fatal("decision UUID reused for proposal", err)
	}
	if got := r.propose(t, r.p, 1); !proto.Equal(got.Outcome, accepted.Outcome) {
		t.Fatal("old proposal replay lost result after CAS")
	}
	catalog, err := r.repo.Epochs(t.Context(), 2, r.gen(2), r.chat, nil, 1)
	if err != nil || len(catalog) != 1 || !catalog[0].Sealed || catalog[0].SealedByProposalID != r.p.Control.ProposalId {
		t.Fatal(catalog, err)
	}
	after := uint64(0)
	catalog, err = r.repo.Epochs(t.Context(), 2, r.gen(2), r.chat, &after, 1)
	if err != nil || len(catalog) != 1 || catalog[0].Epoch != 1 || catalog[0].Sealed || catalog[0].BootstrapUserID != 1 || catalog[0].CreatedByProposalID != r.p.Control.ProposalId {
		t.Fatal(catalog, err)
	}
	stale := r.proposal(1, 0, 0)
	_, err = r.repo.Propose(t.Context(), 1, r.gen(1), stale)
	var conflict *domain.E2EERecoveryConflict
	if !errors.As(err, &conflict) || conflict.Epoch != 1 || conflict.Revision != 1 {
		t.Fatal("stale CAS accepted", err)
	}
	p2 := r.proposal(2, 1, 1)
	r.propose(t, p2, 2)
	r.decide(t, recoveryDecision(p2, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT), 1)
	r.head(t, 2)
	catalog, err = r.repo.Epochs(t.Context(), 1, r.gen(1), r.chat, nil, 32)
	if err != nil || len(catalog) != 3 || catalog[2].BootstrapUserID != 2 || !catalog[1].Sealed {
		t.Fatal(catalog, err)
	}
	for _, limit := range []int{0, 33} {
		if _, err := r.repo.Epochs(t.Context(), 1, r.gen(1), r.chat, nil, limit); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatal("unbounded catalog accepted", err)
		}
	}
	if _, err := r.repo.Epochs(t.Context(), 3, r.gen(1), r.chat, nil, 32); !errors.Is(err, domain.ErrE2EEConversationNotFound) {
		t.Fatal("outsider read catalog", err)
	}
	after = 2
	empty, err := r.repo.Epochs(t.Context(), 1, r.gen(1), r.chat, &after, 32)
	if err != nil || len(empty) != 0 {
		t.Fatal("catalog cursor", err)
	}
	old, err := r.repo.Get(t.Context(), 1, r.gen(1), r.chat, r.p.Control.ProposalId)
	if err != nil || !proto.Equal(old.Outcome, accepted.Outcome) {
		t.Fatal("later epoch rewrote old receipt", err)
	}
	registryCount(t, r.f.db, "e2ee_acquisitions", 0)
	registryCount(t, r.f.db, "e2ee_send_receipts", 0)
}

func TestE2EERecoveryConcurrentProposalsAndExactAcceptPostgres(t *testing.T) {
	r := recoveryDB(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var mu sync.Mutex
	var winner *pb.SignedRecoveryControl
	var pendingIDs []string
	success := 0
	for i := range 8 {
		uid := int64(1 + i%2)
		p := r.proposal(uid, 0, 0)
		wg.Go(func() {
			<-start
			got, err := r.repo.Propose(t.Context(), uid, r.gen(uid), p)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
				winner = got.Outcome.Proposal
			} else {
				var c *domain.E2EERecoveryConflict
				if !errors.As(err, &c) {
					t.Error(err)
				} else {
					pendingIDs = append(pendingIDs, c.PendingProposalID)
				}
			}
		})
	}
	close(start)
	wg.Wait()
	if success != 1 || len(pendingIDs) != 7 {
		t.Fatal("pending uniqueness", success, pendingIDs)
	}
	for _, id := range pendingIDs {
		if id != winner.Control.ProposalId {
			t.Fatal("conflict did not reference winner")
		}
	}
	r.head(t, 0)
	accept := recoveryDecision(winner, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
	uid := int64(2)
	if winner.Control.Peer.Device.UserId == r.f.low.UserUUID {
		uid = 1
	}
	start = make(chan struct{})
	for range 8 {
		wg.Go(func() {
			<-start
			got, err := r.repo.Decide(t.Context(), uid, r.gen(uid), accept)
			if err != nil || got.Outcome.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
				t.Error("exact concurrent accept", err)
			}
		})
	}
	close(start)
	wg.Wait()
	r.head(t, 1)
	registryCount(t, r.f.db, "e2ee_epoch_recoveries", 1)
	registryCount(t, r.f.db, "e2ee_conversation_epochs", 2)
	registryCount(t, r.f.db, "e2ee_recovery_operations", 2)
}

func TestE2EERecoveryCompetingTerminalDecisionsPostgres(t *testing.T) {
	for _, other := range []pb.RecoveryAction{pb.RecoveryAction_RECOVERY_ACTION_CANCEL, pb.RecoveryAction_RECOVERY_ACTION_DECLINE} {
		t.Run(other.String(), func(t *testing.T) {
			r := recoveryDB(t)
			r.propose(t, r.p, 1)
			accept := recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
			alternative := recoveryDecision(r.p, other)
			uid := int64(2)
			if other == pb.RecoveryAction_RECOVERY_ACTION_CANCEL {
				uid = 1
			}
			start := make(chan struct{})
			out := make(chan *domain.E2EERecovery, 2)
			var wg sync.WaitGroup
			for _, job := range []struct {
				uid int64
				s   *pb.SignedRecoveryControl
			}{{2, accept}, {uid, alternative}} {
				wg.Go(func() {
					<-start
					v, err := r.repo.Decide(t.Context(), job.uid, r.gen(job.uid), job.s)
					if err != nil {
						t.Error(err)
						return
					}
					out <- v
				})
			}
			close(start)
			wg.Wait()
			close(out)
			var first *pb.RecoveryOutcome
			count := 0
			for v := range out {
				count++
				if first == nil {
					first = v.Outcome
				} else if !proto.Equal(first, v.Outcome) {
					t.Fatal("terminal outcomes disagree")
				}
			}
			if count != 2 {
				t.Fatal("missing outcome")
			}
			epoch := uint64(0)
			if first.State == pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
				epoch = 1
			}
			r.head(t, epoch)
			registryCount(t, r.f.db, "e2ee_recovery_operations", 3)
		})
	}
}

func TestE2EERecoveryExpiryExactRetryAndFreshProposalPostgres(t *testing.T) {
	r := recoveryDB(t)
	r.repo.lease = 5 * time.Millisecond
	p := r.propose(t, r.p, 1)
	if _, err := r.f.db.ExecContext(t.Context(), `SELECT pg_sleep(0.025)`); err != nil {
		t.Fatal(err)
	}
	expired := r.decide(t, recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT), 2)
	if expired.Outcome.State != pb.RecoveryState_RECOVERY_STATE_EXPIRED || expired.Outcome.Decision != nil || expired.ResolvedAt.Before(p.ExpiresAt) {
		t.Fatal("expired acceptance advanced epoch", expired.Outcome)
	}
	r.head(t, 0)
	again := r.propose(t, r.p, 1)
	if again.Outcome.State != pb.RecoveryState_RECOVERY_STATE_EXPIRED || !again.ExpiresAt.Equal(p.ExpiresAt) {
		t.Fatal("replay renewed lease")
	}
	r.repo.lease = domain.E2EERecoveryProposalLease
	fresh := r.proposal(1, 0, 0)
	if got := r.propose(t, fresh, 1); got.Outcome.State != pb.RecoveryState_RECOVERY_STATE_PENDING {
		t.Fatal("expired pending slot not released")
	}
	registryCount(t, r.f.db, "e2ee_epoch_recoveries", 2)
}

func TestE2EERecoveryReplacementAndOwnerBindingPostgres(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "accepted"}[accepted], func(t *testing.T) {
			r := recoveryDB(t)
			r.propose(t, r.p, 1)
			if accepted {
				r.decide(t, recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT), 2)
			}
			newDevice := r.f.replace(t, 2, r.gen(2))
			got, err := r.repo.Get(t.Context(), 1, r.gen(1), r.chat, r.p.Control.ProposalId)
			if err != nil {
				t.Fatal(err)
			}
			want := pb.RecoveryState_RECOVERY_STATE_SUPERSEDED
			if accepted {
				want = pb.RecoveryState_RECOVERY_STATE_ACCEPTED
			}
			if got.Outcome.State != want {
				t.Fatal(got.Outcome)
			}
			if v := r.decide(t, recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_CANCEL), 1); v.Outcome.State != want {
				t.Fatal("closure rewrote terminal result")
			}
			if _, err := r.repo.Get(t.Context(), 2, r.gen(2), r.chat, r.p.Control.ProposalId); !errors.Is(err, domain.ErrE2EEGenerationChanged) {
				t.Fatal("retired owner read", err)
			}
			if _, err := r.repo.Get(t.Context(), 2, newDevice.GenerationID, r.chat, r.p.Control.ProposalId); !errors.Is(err, domain.ErrE2EEConversationNotFound) {
				t.Fatal("new generation inherited recovery", err)
			}
			if _, err := r.repo.Propose(t.Context(), 1, r.gen(1), r.proposal(1, 0, 0)); !errors.Is(err, domain.ErrE2EEConversationClosed) {
				t.Fatal("recovery reopened replacement closure", err)
			}
		})
	}
	r := recoveryDB(t)
	for _, mutate := range []func(*pb.SignedRecoveryControl){
		func(s *pb.SignedRecoveryControl) { s.Control.Peer.IdentityFingerprint[0] ^= 1 }, func(s *pb.SignedRecoveryControl) { s.Control.Initiator.Device.GenerationId = uuid.NewString() }, func(s *pb.SignedRecoveryControl) { s.Signature = nil },
	} {
		p := proto.Clone(r.p).(*pb.SignedRecoveryControl)
		mutate(p)
		if _, err := r.repo.Propose(t.Context(), 1, r.gen(1), p); err == nil {
			t.Fatal("bad binding accepted")
		}
	}
	if _, err := r.repo.Propose(t.Context(), 2, r.gen(2), r.p); err == nil {
		t.Fatal("peer impersonated proposer")
	}
	registryCount(t, r.f.db, "e2ee_epoch_recoveries", 0)
}

func TestE2EERecoveryQuotasPreserveReplayPostgres(t *testing.T) {
	for _, operations := range []bool{false, true} {
		t.Run(map[bool]string{false: "proposals", true: "operations"}[operations], func(t *testing.T) {
			r := recoveryDB(t)
			if operations {
				r.repo.maxOperations = 2
			} else {
				r.repo.maxProposals = 1
			}
			r.propose(t, r.p, 1)
			cancel := recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_CANCEL)
			r.decide(t, cancel, 1)
			if _, err := r.repo.Propose(t.Context(), 1, r.gen(1), r.proposal(1, 0, 0)); !errors.Is(err, domain.ErrE2EERecoveryCapacity) {
				t.Fatal("quota ignored", err)
			}
			if r.propose(t, r.p, 1).Outcome.State != pb.RecoveryState_RECOVERY_STATE_CANCELLED || r.decide(t, cancel, 1).Outcome.State != pb.RecoveryState_RECOVERY_STATE_CANCELLED {
				t.Fatal("quota blocked exact replay")
			}
			r.head(t, 0)
			registryCount(t, r.f.db, "e2ee_epoch_recoveries", 1)
			registryCount(t, r.f.db, "e2ee_recovery_operations", 2)
		})
	}
}

func TestE2EERecoveryAcceptVersusReplacementPostgres(t *testing.T) {
	r := recoveryDB(t)
	r.propose(t, r.p, 1)
	accept := recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
	start := make(chan struct{})
	var wg sync.WaitGroup
	var accepted *domain.E2EERecovery
	var decisionErr error
	wg.Go(func() { <-start; accepted, decisionErr = r.repo.Decide(t.Context(), 2, r.gen(2), accept) })
	wg.Go(func() {
		<-start
		req := registrationMaterial()
		req.ExpectedGenerationID = r.gen(2)
		if _, err := r.f.devices.Replace(t.Context(), 2, req, domain.E2EEReauthentication{PasswordHash: "verified-test-hash", TokenVersion: 1}); err != nil {
			t.Error(err)
		}
	})
	close(start)
	wg.Wait()
	if decisionErr != nil && !errors.Is(decisionErr, domain.ErrE2EEGenerationChanged) {
		t.Fatal(decisionErr)
	}
	got, err := r.repo.Get(t.Context(), 1, r.gen(1), r.chat, r.p.Control.ProposalId)
	if err != nil {
		t.Fatal(err)
	}
	if accepted != nil {
		if got.Outcome.State != pb.RecoveryState_RECOVERY_STATE_ACCEPTED {
			t.Fatal("replacement rewrote committed Accepted")
		}
		r.head(t, 1)
	} else {
		if got.Outcome.State != pb.RecoveryState_RECOVERY_STATE_SUPERSEDED {
			t.Fatal("replacement allowed stale acceptance")
		}
		r.head(t, 0)
	}
	if r.f.get(t, r.chat).State != "closed" {
		t.Fatal("recovery undid device replacement closure")
	}
}

func TestE2EERecoveryDeferredFailureAndSQLGuardsPostgres(t *testing.T) {
	r := recoveryDB(t)
	r.propose(t, r.p, 1)
	for _, statement := range []string{
		`UPDATE e2ee_conversations SET active_epoch=1,epoch_revision=1`,
		`UPDATE e2ee_conversation_epochs SET state='sealed',sealed_at=clock_timestamp(),sealed_by_proposal_id='` + r.p.Control.ProposalId + `'`,
		`DELETE FROM e2ee_conversation_epochs`, `DELETE FROM e2ee_recovery_operations`, `UPDATE e2ee_epoch_recoveries SET expires_at=expires_at+INTERVAL '1 hour'`,
	} {
		if _, err := r.f.db.ExecContext(t.Context(), statement); err == nil {
			t.Fatal("SQL guard accepted", statement)
		}
		r.head(t, 0)
	}
	for _, statement := range []string{
		`INSERT INTO e2ee_conversation_epochs(conversation_id,epoch,revision,state,parent_epoch,created_by_proposal_id,bootstrap_user_id,created_at,sealed_at,sealed_by_proposal_id)
            VALUES('` + r.chat + `',1,1,'sealed',NULL,'` + r.p.Control.ProposalId + `',1,clock_timestamp(),clock_timestamp(),'` + r.p.Control.ProposalId + `')`,
		`UPDATE e2ee_epoch_recoveries SET state='accepted',decision_wire=decode('01','hex'),resolved_at=clock_timestamp(),new_epoch=NULL,result_revision=NULL`,
	} {
		_, err := r.f.db.ExecContext(t.Context(), statement)
		var pgErr *pq.Error
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatal("NULL escaped SQL CHECK", err)
		}
	}
	_, err := r.f.db.ExecContext(t.Context(), `CREATE FUNCTION recovery_commit_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
        IF NEW.state='accepted' THEN RAISE EXCEPTION 'synthetic deferred commit fault'; END IF; RETURN NEW; END $$;
        CREATE CONSTRAINT TRIGGER recovery_commit_fault AFTER UPDATE ON e2ee_epoch_recoveries DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION recovery_commit_fault()`)
	if err != nil {
		t.Fatal(err)
	}
	accept := recoveryDecision(r.p, pb.RecoveryAction_RECOVERY_ACTION_ACCEPT)
	if got, err := r.repo.Decide(t.Context(), 2, r.gen(2), accept); err == nil || got != nil {
		t.Fatal("receipt returned before COMMIT", got, err)
	}
	r.head(t, 0)
	registryCount(t, r.f.db, "e2ee_conversation_epochs", 1)
	registryCount(t, r.f.db, "e2ee_recovery_operations", 1)
	if _, err := r.f.db.ExecContext(t.Context(), `DROP TRIGGER recovery_commit_fault ON e2ee_epoch_recoveries;DROP FUNCTION recovery_commit_fault()`); err != nil {
		t.Fatal(err)
	}
	r.decide(t, accept, 2)
	r.head(t, 1)
	registryCount(t, r.f.db, "e2ee_recovery_operations", 2)
}

func TestE2EERecoveryMigrationGuardsPostgres(t *testing.T) {
	if err := goose.SetDialect("postgres"); err != nil {
		t.Fatal(err)
	}
	t.Run("empty roundtrip and nonempty Down", func(t *testing.T) {
		db := testutil.EmptyPostgres(t)
		if err := goose.UpTo(db, "../../migrations", 26); err != nil {
			t.Fatal(err)
		}
		if err := goose.Down(db, "../../migrations"); err != nil {
			t.Fatal("empty Down", err)
		}
		if err := goose.UpTo(db, "../../migrations", 26); err != nil {
			t.Fatal(err)
		}
		f := seedConversations(t, db)
		c := f.create(t)
		if err := goose.Down(db, "../../migrations"); err == nil {
			t.Fatal("Down discarded initial epoch")
		}
		if f.get(t, c.ID).State != "active" {
			t.Fatal("failed Down changed conversation")
		}
		registryCount(t, db, "e2ee_conversation_epochs", 1)
	})
	t.Run("no automatic prototype conversion", func(t *testing.T) {
		db := testutil.EmptyPostgres(t)
		if err := goose.UpTo(db, "../../migrations", 25); err != nil {
			t.Fatal(err)
		}
		f := seedConversations(t, db)
		c := f.create(t)
		if err := goose.UpTo(db, "../../migrations", 26); err == nil {
			t.Fatal("silently relabelled prototype data")
		}
		if f.get(t, c.ID).State != "active" {
			t.Fatal("failed Up erased prototype data")
		}
		registryCount(t, db, "e2ee_devices", 2)
	})
}
