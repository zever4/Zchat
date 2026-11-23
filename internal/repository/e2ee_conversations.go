package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type PgE2EEConversationRepository struct{ db *sql.DB }

func NewPgE2EEConversationRepository(db *sql.DB) *PgE2EEConversationRepository {
	return &PgE2EEConversationRepository{db: db}
}

// Create uses the same ascending user-row order as friendship transactions.
// Replacement/publication lock their single user before changing a generation.
// Serialize before checking the absent-conversation case; a unique index alone
// cannot make concurrent opposite-direction creations return the same result.
func (r *PgE2EEConversationRepository) Create(parent context.Context, userID int64, req *pb.CreateConversationRequest) (*domain.E2EEConversation, error) {
	if err := boundary.ValidateCreateConversation(req); err != nil {
		return nil, err
	}
	if userID <= 0 || userID == req.PeerUserId {
		return nil, domain.ErrInvalidInput
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	pair := orderedPair(userID, req.PeerUserId)
	rows, err := tx.QueryContext(ctx, `SELECT id FROM users WHERE id IN ($1,$2) ORDER BY id FOR UPDATE`, pair[0], pair[1])
	if err != nil {
		return nil, err
	}
	count := 0
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	if count != 2 {
		return nil, domain.ErrE2EEPeerUnavailable
	}
	var ownGeneration string
	err = tx.QueryRowContext(ctx, `SELECT generation_id::text FROM e2ee_devices WHERE user_id=$1 AND retired_at IS NULL`, userID).Scan(&ownGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if err != nil {
		return nil, err
	}
	if ownGeneration != req.Context.DeviceGenerationId {
		return nil, domain.ErrE2EEGenerationChanged
	}
	var peerID int64
	var conversationID string
	err = tx.QueryRowContext(ctx, `SELECT peer_user_id,conversation_id::text FROM e2ee_conversation_requests WHERE owner_generation_id=$1 AND request_id=$2`, ownGeneration, req.CreationRequestId).Scan(&peerID, &conversationID)
	if err == nil {
		if peerID != req.PeerUserId {
			return nil, domain.ErrE2EERequestConflict
		}
		// Return the original result with its current closure state. Do not
		// silently create a new chat if the peer replaced keys after COMMIT.
		result, err := getE2EEConversation(ctx, tx, userID, ownGeneration, conversationID)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return result, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	var requests int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM e2ee_conversation_requests WHERE owner_generation_id=$1`, ownGeneration).Scan(&requests); err != nil {
		return nil, err
	}
	if requests >= domain.MaxE2EEConversationRequestsPerGeneration {
		return nil, domain.ErrE2EEConversationCapacity
	}
	err = tx.QueryRowContext(ctx, `SELECT conversation_id::text FROM e2ee_conversations WHERE low_user_id=$1 AND high_user_id=$2 AND state='active'`, pair[0], pair[1]).Scan(&conversationID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if errors.Is(err, sql.ErrNoRows) {
		// Only NEW conversations check friendship/privacy. An already-active
		// conversation and its exact create retry survive disabling new DMs.
		friends, err := (&PgFriendRequestRepository{db: tx}).AreFriends(ctx, pair[0], pair[1])
		if err != nil {
			return nil, err
		}
		if !friends {
			return nil, domain.ErrNotFriends
		}
		for _, id := range pair {
			var allowed bool
			err := tx.QueryRowContext(ctx, `SELECT allow_direct_messages FROM user_settings WHERE user_id=$1 FOR SHARE`, id).Scan(&allowed)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, domain.ErrDirectMessagesDisabled
			}
			if err != nil {
				return nil, err
			}
			if !allowed {
				return nil, domain.ErrDirectMessagesDisabled
			}
		}
		var low, high string
		for i, id := range pair {
			var gen string
			var ready bool
			err := tx.QueryRowContext(ctx, `SELECT generation_id::text,public_material_ready FROM e2ee_devices WHERE user_id=$1 AND retired_at IS NULL`, id).Scan(&gen, &ready)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, domain.ErrE2EEKeysNotReady
			}
			if err != nil {
				return nil, err
			}
			if !ready {
				return nil, domain.ErrE2EEKeysNotReady
			}
			if i == 0 {
				low = gen
			} else {
				high = gen
			}
		}
		var lowCount, highCount int
		err = tx.QueryRowContext(ctx, `SELECT
            (SELECT count(*) FROM e2ee_conversations WHERE low_user_id=$1 OR high_user_id=$1),
            (SELECT count(*) FROM e2ee_conversations WHERE low_user_id=$2 OR high_user_id=$2)`, pair[0], pair[1]).Scan(&lowCount, &highCount)
		if err != nil {
			return nil, err
		}
		if lowCount >= domain.MaxE2EEConversationsPerAccount || highCount >= domain.MaxE2EEConversationsPerAccount {
			return nil, domain.ErrE2EEConversationCapacity
		}
		conversationID = uuid.NewString()
		_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id) VALUES($1,$2,$3,$4,$5)`, conversationID, pair[0], pair[1], low, high)
		if err != nil {
			return nil, err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_conversation_requests(owner_user_id,owner_generation_id,request_id,peer_user_id,conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id)
        SELECT $1,$2,$3,$4,conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id FROM e2ee_conversations WHERE conversation_id=$5`, userID, ownGeneration, req.CreationRequestId, req.PeerUserId, conversationID)
	if err != nil {
		return nil, err
	}
	result, err := getE2EEConversation(ctx, tx, userID, ownGeneration, conversationID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (r *PgE2EEConversationRepository) Get(parent context.Context, userID int64, req *pb.GetConversationRequest) (*domain.E2EEConversation, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateGetConversation(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	return getE2EEConversation(ctx, r.db, userID, req.Context.DeviceGenerationId, req.ConversationId)
}

// One SQL snapshot returns the original bound generations and closure metadata.
// Never read a peer's credential verifier into this response model. A fresh own
// generation cannot fetch an old generation's archive through this endpoint.
func getE2EEConversation(ctx context.Context, db sqlExecutor, userID int64, gen, id string) (*domain.E2EEConversation, error) {
	c := &domain.E2EEConversation{OwnDevice: new(domain.E2EEDevice), PeerDevice: new(domain.E2EEDevice)}
	targets := []any{&c.ID, &c.Mode, &c.State, &c.CloseReason, &c.ClosedGenerationID, &c.ClosedAt}
	for _, d := range []*domain.E2EEDevice{c.OwnDevice, c.PeerDevice} {
		targets = append(targets, &d.UserID, &d.UserUUID, &d.GenerationID, &d.DeviceID, &d.RegistrationID, &d.IdentityKey, &d.PublicMaterialReady, &d.PublicMaterialRevision, &d.RetiredAt)
	}
	err := db.QueryRowContext(ctx, `SELECT c.conversation_id::text,c.mode,c.state,COALESCE(c.close_reason,''),COALESCE(c.closed_generation_id::text,''),c.closed_at,
        od.user_id,ou.user_uuid,od.generation_id::text,od.device_id,od.registration_id,od.identity_key,od.public_material_ready,od.public_material_revision,od.retired_at,
        pd.user_id,pu.user_uuid,pd.generation_id::text,pd.device_id,pd.registration_id,pd.identity_key,pd.public_material_ready,pd.public_material_revision,pd.retired_at
        FROM e2ee_conversations c
        JOIN e2ee_devices od ON od.generation_id=CASE WHEN c.low_user_id=$1 THEN c.low_generation_id ELSE c.high_generation_id END
        JOIN users ou ON ou.id=od.user_id
        JOIN e2ee_devices pd ON pd.generation_id=CASE WHEN c.low_user_id=$1 THEN c.high_generation_id ELSE c.low_generation_id END
        JOIN users pu ON pu.id=pd.user_id
        WHERE c.conversation_id=$3 AND od.retired_at IS NULL AND
          ((c.low_user_id=$1 AND c.low_generation_id=$2) OR (c.high_user_id=$1 AND c.high_generation_id=$2))`, userID, gen, id).Scan(targets...)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEConversationNotFound
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}
