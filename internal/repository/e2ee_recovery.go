package repository

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// Internal control repository, NOT an enabled recovery RPC. The future service
// must derive userID/generation from verified JWT+device credential. This layer
// rechecks current ownership, bindings and CAS; Go does NOT verify Signal signatures.
type PgE2EERecoveryRepository struct {
	db                          *sql.DB
	lease                       time.Duration
	maxProposals, maxOperations int
}

func NewPgE2EERecoveryRepository(db *sql.DB) *PgE2EERecoveryRepository {
	return &PgE2EERecoveryRepository{db: db, lease: domain.E2EERecoveryProposalLease, maxProposals: domain.MaxE2EERecoveryProposalsPerConversation, maxOperations: domain.MaxE2EERecoveryOperationsPerGeneration}
}

type recoveryContext struct {
	tx              *sql.Tx
	c               *domain.E2EEConversation
	low, high       int64
	lowGen, highGen string
	epoch, revision uint64
	now             time.Time
}

func recoveryUUID(s string) bool {
	id, err := uuid.Parse(s)
	return err == nil && id.Version() == 4 && id.Variant() == uuid.RFC4122 && id.String() == s
}

// No external RPC or Signal work under DB locks. Users -> devices -> conversation
// order also serializes per-generation retained-operation quota across chats.
func lockRecovery(ctx context.Context, tx *sql.Tx, userID int64, gen, chat string) (*recoveryContext, error) {
	if userID <= 0 || !recoveryUUID(gen) || !recoveryUUID(chat) {
		return nil, domain.ErrInvalidInput
	}
	r := &recoveryContext{tx: tx}
	err := tx.QueryRowContext(ctx, `SELECT low_user_id,high_user_id,low_generation_id::text,high_generation_id::text FROM e2ee_conversations
        WHERE conversation_id=$1 AND ((low_user_id=$2 AND low_generation_id=$3) OR (high_user_id=$2 AND high_generation_id=$3))`, chat, userID, gen).Scan(&r.low, &r.high, &r.lowGen, &r.highGen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEConversationNotFound
	}
	if err != nil {
		return nil, err
	}
	if err = lockAcquisitionRows(ctx, tx, `SELECT id FROM users WHERE id IN($1,$2) ORDER BY id FOR UPDATE`, r.low, r.high); err != nil {
		return nil, err
	}
	if err = lockAcquisitionRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id IN($1,$2) ORDER BY user_id FOR SHARE`, r.lowGen, r.highGen); err != nil {
		return nil, err
	}
	var current string
	err = tx.QueryRowContext(ctx, `SELECT generation_id::text FROM e2ee_devices WHERE user_id=$1 AND retired_at IS NULL`, userID).Scan(&current)
	if errors.Is(err, sql.ErrNoRows) || err == nil && current != gen {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if err != nil {
		return nil, err
	}
	if err = tx.QueryRowContext(ctx, `SELECT active_epoch,epoch_revision FROM e2ee_conversations WHERE conversation_id=$1 FOR UPDATE`, chat).Scan(&r.epoch, &r.revision); err != nil {
		return nil, err
	}
	r.c, err = getE2EEConversation(ctx, tx, userID, gen, chat)
	if err != nil {
		return nil, err
	}
	err = tx.QueryRowContext(ctx, `SELECT date_trunc('milliseconds',clock_timestamp())`).Scan(&r.now)
	return r, err
}
func recoveryDeviceMatches(p *pb.RecoveryParticipant, d *domain.E2EEDevice) bool {
	if p == nil || p.Device == nil {
		return false
	}
	hash := sha256.Sum256(d.IdentityKey)
	return p.Device.UserId == d.UserUUID && p.Device.GenerationId == d.GenerationID && p.Device.DeviceId == d.DeviceID && bytes.Equal(p.IdentityFingerprint, hash[:])
}
func recoveryDevice(r *recoveryContext, id int64) *domain.E2EEDevice {
	if r.c.OwnDevice.UserID == id {
		return r.c.OwnDevice
	}
	if r.c.PeerDevice.UserID == id {
		return r.c.PeerDevice
	}
	return nil
}
func validateRecoveryParticipants(r *recoveryContext, c *pb.RecoveryControl) bool {
	return c.ConversationId == r.c.ID && ((recoveryDeviceMatches(c.Initiator, r.c.OwnDevice) && recoveryDeviceMatches(c.Peer, r.c.PeerDevice)) ||
		(recoveryDeviceMatches(c.Initiator, r.c.PeerDevice) && recoveryDeviceMatches(c.Peer, r.c.OwnDevice)))
}
func authorizeRecoveryCommand(r *recoveryContext, s *pb.SignedRecoveryControl) error {
	c := s.Control
	actor := c.Initiator
	if c.Action == pb.RecoveryAction_RECOVERY_ACTION_ACCEPT || c.Action == pb.RecoveryAction_RECOVERY_ACTION_DECLINE {
		actor = c.Peer
	}
	if !validateRecoveryParticipants(r, c) || !recoveryDeviceMatches(actor, r.c.OwnDevice) {
		return domain.ErrInvalidInput
	}
	return nil
}
func normalizePendingRecovery(ctx context.Context, r *recoveryContext) error {
	_, err := r.tx.ExecContext(ctx, `UPDATE e2ee_epoch_recoveries SET
        state=CASE WHEN $2 OR expected_epoch<>$3 OR expected_revision<>$4 THEN 'superseded' ELSE 'expired' END,resolved_at=$5
        WHERE conversation_id=$1 AND state='pending' AND ($2 OR expected_epoch<>$3 OR expected_revision<>$4 OR expires_at<=$5)`, r.c.ID, r.c.State != "active", r.epoch, r.revision, r.now)
	return err
}
func readRecovery(ctx context.Context, r *recoveryContext, id string) (*domain.E2EERecovery, error) {
	var wire, decision, digest []byte
	var state string
	var epoch, revision sql.NullInt64
	var initID, peerID int64
	var initGen, peerGen string
	var expectedEpoch, expectedRevision uint64
	result := new(domain.E2EERecovery)
	err := r.tx.QueryRowContext(ctx, `SELECT proposal_wire,decision_wire,proposal_digest,state,created_at,expires_at,resolved_at,new_epoch,result_revision,
        initiator_user_id,peer_user_id,initiator_generation_id::text,peer_generation_id::text,expected_epoch,expected_revision
        FROM e2ee_epoch_recoveries WHERE conversation_id=$1 AND proposal_id=$2 FOR UPDATE`, r.c.ID, id).Scan(&wire, &decision, &digest, &state, &result.CreatedAt, &result.ExpiresAt, &result.ResolvedAt, &epoch, &revision, &initID, &peerID, &initGen, &peerGen, &expectedEpoch, &expectedRevision)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EERecoveryNotFound
	}
	if err != nil {
		return nil, err
	}
	proposal := new(pb.SignedRecoveryControl)
	if len(wire) > boundary.MaxRecoveryBytes || proto.Unmarshal(wire, proposal) != nil || boundary.ValidateSignedRecoveryControl(proposal) != nil {
		return nil, domain.ErrE2EERecoveryState
	}
	p := proposal.Control
	init, peer := recoveryDevice(r, initID), recoveryDevice(r, peerID)
	if init == nil || peer == nil || !recoveryDeviceMatches(p.Initiator, init) || !recoveryDeviceMatches(p.Peer, peer) || p.Initiator.Device.GenerationId != initGen || p.Peer.Device.GenerationId != peerGen ||
		p.ConversationId != r.c.ID || p.ProposalId != id || p.ExpectedEpoch != expectedEpoch || p.ExpectedRevision != expectedRevision {
		return nil, domain.ErrE2EERecoveryState
	}
	hash, _ := boundary.RecoveryDigest(p)
	if !bytes.Equal(digest, hash[:]) {
		return nil, domain.ErrE2EERecoveryState
	}
	o := &pb.RecoveryOutcome{ProtocolVersion: 2, Proposal: proposal, State: recoveryState(state)}
	if decision != nil {
		o.Decision = new(pb.SignedRecoveryControl)
		if len(decision) > boundary.MaxRecoveryBytes || proto.Unmarshal(decision, o.Decision) != nil {
			return nil, domain.ErrE2EERecoveryState
		}
	}
	if epoch.Valid {
		o.NewEpoch = uint64(epoch.Int64)
	}
	if revision.Valid {
		o.EpochRevision = uint64(revision.Int64)
	}
	if boundary.ValidateRecoveryOutcome(o, proposal) != nil || !result.ExpiresAt.After(result.CreatedAt) {
		return nil, domain.ErrE2EERecoveryState
	}
	result.Outcome = o
	return result, nil
}
func recoveryState(s string) pb.RecoveryState {
	switch s {
	case "pending":
		return pb.RecoveryState_RECOVERY_STATE_PENDING
	case "accepted":
		return pb.RecoveryState_RECOVERY_STATE_ACCEPTED
	case "declined":
		return pb.RecoveryState_RECOVERY_STATE_DECLINED
	case "cancelled":
		return pb.RecoveryState_RECOVERY_STATE_CANCELLED
	case "expired":
		return pb.RecoveryState_RECOVERY_STATE_EXPIRED
	case "superseded":
		return pb.RecoveryState_RECOVERY_STATE_SUPERSEDED
	}
	return pb.RecoveryState_RECOVERY_STATE_UNSPECIFIED
}
func replayRecoveryOperation(ctx context.Context, r *recoveryContext, s *pb.SignedRecoveryControl) (bool, error) {
	var owner int64
	var chat, id string
	var action int32
	var digest, wire []byte
	err := r.tx.QueryRowContext(ctx, `SELECT owner_user_id,conversation_id::text,proposal_id::text,action,control_digest,control_wire
        FROM e2ee_recovery_operations WHERE owner_generation_id=$1 AND request_id=$2`, r.c.OwnDevice.GenerationID, s.Control.RequestId).Scan(&owner, &chat, &id, &action, &digest, &wire)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	saved := new(pb.SignedRecoveryControl)
	if len(wire) > boundary.MaxRecoveryBytes || proto.Unmarshal(wire, saved) != nil || boundary.ValidateSignedRecoveryControl(saved) != nil {
		return false, domain.ErrE2EERecoveryState
	}
	hash, _ := boundary.RecoveryDigest(saved.Control)
	if owner != r.c.OwnDevice.UserID || chat != saved.Control.ConversationId || id != saved.Control.ProposalId || action != int32(saved.Control.Action) || !bytes.Equal(digest, hash[:]) {
		return false, domain.ErrE2EERecoveryState
	}
	if !proto.Equal(saved, s) {
		return false, domain.ErrE2EERequestConflict
	}
	return true, nil
}
func (repo *PgE2EERecoveryRepository) operationCapacity(ctx context.Context, r *recoveryContext) error {
	var count int
	if err := r.tx.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_recovery_operations WHERE owner_generation_id=$1`, r.c.OwnDevice.GenerationID).Scan(&count); err != nil {
		return err
	}
	if count >= repo.maxOperations {
		return domain.ErrE2EERecoveryCapacity
	}
	return nil
}
func saveRecoveryOperation(ctx context.Context, r *recoveryContext, s *pb.SignedRecoveryControl) error {
	hash, _ := boundary.RecoveryDigest(s.Control)
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(s)
	if err != nil {
		return err
	}
	_, err = r.tx.ExecContext(ctx, `INSERT INTO e2ee_recovery_operations(owner_user_id,owner_generation_id,request_id,conversation_id,proposal_id,action,control_digest,control_wire)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, r.c.OwnDevice.UserID, r.c.OwnDevice.GenerationID, s.Control.RequestId, r.c.ID, s.Control.ProposalId, int32(s.Control.Action), hash[:], wire)
	return err
}
func (repo *PgE2EERecoveryRepository) Propose(parent context.Context, userID int64, gen string, s *pb.SignedRecoveryControl) (*domain.E2EERecovery, error) {
	if boundary.ValidateSignedRecoveryControl(s) != nil || s.Control.Action != pb.RecoveryAction_RECOVERY_ACTION_PROPOSE {
		return nil, domain.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := repo.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	r, err := lockRecovery(ctx, tx, userID, gen, s.Control.ConversationId)
	if err != nil {
		return nil, err
	}
	if err = authorizeRecoveryCommand(r, s); err != nil {
		return nil, err
	}
	if err = normalizePendingRecovery(ctx, r); err != nil {
		return nil, err
	}
	replay, err := replayRecoveryOperation(ctx, r, s)
	if err != nil {
		return nil, err
	}
	if !replay {
		if r.c.State != "active" {
			return nil, domain.ErrE2EEConversationClosed
		}
		if !r.c.OwnDevice.PublicMaterialReady || !r.c.PeerDevice.PublicMaterialReady {
			return nil, domain.ErrE2EEKeysNotReady
		}
		var pending string
		err = tx.QueryRowContext(ctx, `SELECT proposal_id::text FROM e2ee_epoch_recoveries WHERE conversation_id=$1 AND state='pending'`, r.c.ID).Scan(&pending)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		if pending != "" || s.Control.ExpectedEpoch != r.epoch || s.Control.ExpectedRevision != r.revision {
			return nil, &domain.E2EERecoveryConflict{Epoch: r.epoch, Revision: r.revision, PendingProposalID: pending}
		}
		if err = repo.operationCapacity(ctx, r); err != nil {
			return nil, err
		}
		var count int
		if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_epoch_recoveries WHERE conversation_id=$1`, r.c.ID).Scan(&count); err != nil {
			return nil, err
		}
		if count >= repo.maxProposals {
			return nil, domain.ErrE2EERecoveryCapacity
		}
		hash, _ := boundary.RecoveryDigest(s.Control)
		wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(s)
		if err != nil {
			return nil, err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_epoch_recoveries(conversation_id,proposal_id,initiator_user_id,peer_user_id,initiator_generation_id,peer_generation_id,
            low_user_id,high_user_id,low_generation_id,high_generation_id,expected_epoch,expected_revision,proposal_digest,proposal_wire,created_at,expires_at)
            VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`, r.c.ID, s.Control.ProposalId, userID, r.c.PeerDevice.UserID, gen, r.c.PeerDevice.GenerationID,
			r.low, r.high, r.lowGen, r.highGen, r.epoch, r.revision, hash[:], wire, r.now, r.now.Add(repo.lease))
		if err != nil {
			return nil, err
		}
		if err = saveRecoveryOperation(ctx, r, s); err != nil {
			return nil, err
		}
	}
	result, err := readRecovery(ctx, r, s.Control.ProposalId)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (repo *PgE2EERecoveryRepository) Decide(parent context.Context, userID int64, gen string, s *pb.SignedRecoveryControl) (*domain.E2EERecovery, error) {
	if boundary.ValidateSignedRecoveryControl(s) != nil || s.Control.Action == pb.RecoveryAction_RECOVERY_ACTION_PROPOSE {
		return nil, domain.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := repo.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	r, err := lockRecovery(ctx, tx, userID, gen, s.Control.ConversationId)
	if err != nil {
		return nil, err
	}
	if err = authorizeRecoveryCommand(r, s); err != nil {
		return nil, err
	}
	if err = normalizePendingRecovery(ctx, r); err != nil {
		return nil, err
	}
	old, err := readRecovery(ctx, r, s.Control.ProposalId)
	if err != nil {
		return nil, err
	}
	candidate := &pb.RecoveryOutcome{ProtocolVersion: 2, Proposal: old.Outcome.Proposal, Decision: s}
	state := "cancelled"
	switch s.Control.Action {
	case pb.RecoveryAction_RECOVERY_ACTION_ACCEPT:
		state = "accepted"
		candidate.State = pb.RecoveryState_RECOVERY_STATE_ACCEPTED
		candidate.NewEpoch = s.Control.ExpectedEpoch + 1
		candidate.EpochRevision = s.Control.ExpectedRevision + 1
	case pb.RecoveryAction_RECOVERY_ACTION_DECLINE:
		state = "declined"
		candidate.State = pb.RecoveryState_RECOVERY_STATE_DECLINED
	case pb.RecoveryAction_RECOVERY_ACTION_CANCEL:
		candidate.State = pb.RecoveryState_RECOVERY_STATE_CANCELLED
	}
	if boundary.ValidateRecoveryOutcome(candidate, old.Outcome.Proposal) != nil {
		return nil, domain.ErrE2EERequestConflict
	}
	replay, err := replayRecoveryOperation(ctx, r, s)
	if err != nil {
		return nil, err
	}
	if !replay {
		if err = repo.operationCapacity(ctx, r); err != nil {
			return nil, err
		}
		if old.Outcome.State == pb.RecoveryState_RECOVERY_STATE_PENDING {
			var newEpoch, resultRevision any
			if state == "accepted" {
				if !r.c.OwnDevice.PublicMaterialReady || !r.c.PeerDevice.PublicMaterialReady {
					return nil, domain.ErrE2EEKeysNotReady
				}
				newEpoch = s.Control.ExpectedEpoch + 1
				resultRevision = s.Control.ExpectedRevision + 1
				if _, err = tx.ExecContext(ctx, `UPDATE e2ee_conversation_epochs SET state='sealed',sealed_by_proposal_id=$3,sealed_at=$4
                    WHERE conversation_id=$1 AND epoch=$2 AND state='active'`, r.c.ID, r.epoch, s.Control.ProposalId, r.now); err != nil {
					return nil, err
				}
				init := r.c.PeerDevice.UserID // Accept is signed by peer, so the other account created the proposal.
				if _, err = tx.ExecContext(ctx, `INSERT INTO e2ee_conversation_epochs(conversation_id,epoch,revision,parent_epoch,created_by_proposal_id,bootstrap_user_id,created_at)
                    VALUES($1,$2,$3,$4,$5,$6,$7)`, r.c.ID, newEpoch, resultRevision, r.epoch, s.Control.ProposalId, init, r.now); err != nil {
					return nil, err
				}
				changed, err := tx.ExecContext(ctx, `UPDATE e2ee_conversations SET active_epoch=$2,epoch_revision=$3 WHERE conversation_id=$1 AND active_epoch=$4 AND epoch_revision=$5`, r.c.ID, newEpoch, resultRevision, r.epoch, r.revision)
				if err != nil {
					return nil, err
				}
				n, err := changed.RowsAffected()
				if err != nil {
					return nil, err
				}
				if n != 1 {
					return nil, domain.ErrE2EERecoveryState
				}
			}
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(s)
			if err != nil {
				return nil, err
			}
			if _, err = tx.ExecContext(ctx, `UPDATE e2ee_epoch_recoveries SET state=$3,decision_wire=$4,resolved_at=$5,new_epoch=$6,result_revision=$7
                WHERE conversation_id=$1 AND proposal_id=$2 AND state='pending'`, r.c.ID, s.Control.ProposalId, state, wire, r.now, newEpoch, resultRevision); err != nil {
				return nil, err
			}
		}
		// A valid late Cancel/Accept observes the already-committed terminal result;
		// it never overwrites Accepted. Its own UUID remains durable replay evidence.
		if err = saveRecoveryOperation(ctx, r, s); err != nil {
			return nil, err
		}
	}
	result, err := readRecovery(ctx, r, s.Control.ProposalId)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (repo *PgE2EERecoveryRepository) Get(parent context.Context, userID int64, gen, chat, id string) (*domain.E2EERecovery, error) {
	if !recoveryUUID(id) {
		return nil, domain.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := repo.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	r, err := lockRecovery(ctx, tx, userID, gen, chat)
	if err != nil {
		return nil, err
	}
	if err = normalizePendingRecovery(ctx, r); err != nil {
		return nil, err
	}
	result, err := readRecovery(ctx, r, id)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

// Epochs is bounded, ordered and includes archived receiving epochs. after=nil
// includes epoch0. No empty result proves that pending ciphertext can be deleted.
func (repo *PgE2EERecoveryRepository) Epochs(parent context.Context, userID int64, gen, chat string, after *uint64, limit int) ([]domain.E2EEEpoch, error) {
	catalog, err := repo.EpochCatalog(parent, userID, gen, chat, after, limit)
	if err != nil {
		return nil, err
	}
	return catalog.Epochs, nil
}

func (repo *PgE2EERecoveryRepository) EpochCatalog(parent context.Context, userID int64, gen, chat string, after *uint64, limit int) (*domain.E2EEEpochCatalog, error) {
	if limit < 1 || limit > domain.MaxE2EERecoveryPage || (after != nil && *after > uint64(^uint64(0)>>1)) {
		return nil, domain.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := repo.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	r, err := lockRecovery(ctx, tx, userID, gen, chat)
	if err != nil {
		return nil, err
	}
	cursor := int64(-1)
	if after != nil {
		cursor = int64(*after)
	}
	rows, err := tx.QueryContext(ctx, `SELECT epoch,revision,state='sealed',COALESCE(created_by_proposal_id::text,''),COALESCE(sealed_by_proposal_id::text,''),COALESCE(bootstrap_user_id,0),created_at,sealed_at
        FROM e2ee_conversation_epochs WHERE conversation_id=$1 AND epoch>$2 ORDER BY epoch LIMIT $3`, r.c.ID, cursor, limit+1)
	if err != nil {
		return nil, err
	}
	result := make([]domain.E2EEEpoch, 0, limit)
	for rows.Next() {
		var e domain.E2EEEpoch
		if err = rows.Scan(&e.Epoch, &e.Revision, &e.Sealed, &e.CreatedByProposalID, &e.SealedByProposalID, &e.BootstrapUserID, &e.CreatedAt, &e.SealedAt); err != nil {
			rows.Close()
			return nil, err
		}
		result = append(result, e)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	catalog := &domain.E2EEEpochCatalog{Conversation: r.c, ActiveEpoch: r.epoch, Revision: r.revision, Epochs: result}
	if len(result) > limit {
		catalog.Epochs = result[:limit]
		value := catalog.Epochs[limit-1].Epoch
		catalog.NextAfterEpoch = &value
	}
	return catalog, nil
}
