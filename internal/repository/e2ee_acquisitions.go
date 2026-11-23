package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// Acquire locks users -> bound devices -> conversation in canonical account
// order. Publish/Replace/Create share that ordering. No network I/O is performed
// under these locks. Key reservation and its exact public result commit together.
func (r *PgE2EEPreKeyRepository) Acquire(parent context.Context, userID int64, req *pb.AcquireBundleRequest) (*pb.AcquiredBundle, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateAcquisition(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var lowID, highID int64
	var lowGen, highGen string
	err = tx.QueryRowContext(ctx, `SELECT low_user_id,high_user_id,low_generation_id::text,high_generation_id::text FROM e2ee_conversations
        WHERE conversation_id=$1 AND ((low_user_id=$2 AND low_generation_id=$3) OR (high_user_id=$2 AND high_generation_id=$3))`, req.ConversationId, userID, req.Context.DeviceGenerationId).Scan(&lowID, &highID, &lowGen, &highGen)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEConversationNotFound
	}
	if err != nil {
		return nil, err
	}
	// The first query only discovers immutable lock targets. Authorization/state
	// is checked again below after the locks, never trusted from an earlier Get.
	if err := lockAcquisitionRows(ctx, tx, `SELECT id FROM users WHERE id IN ($1,$2) ORDER BY id FOR UPDATE`, lowID, highID); err != nil {
		return nil, err
	}
	if err := lockAcquisitionRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id IN ($1,$2) ORDER BY user_id FOR SHARE`, lowGen, highGen); err != nil {
		return nil, err
	}
	var locked string
	if err := tx.QueryRowContext(ctx, `SELECT conversation_id::text FROM e2ee_conversations WHERE conversation_id=$1 FOR SHARE`, req.ConversationId).Scan(&locked); err != nil {
		return nil, err
	}
	c, err := getE2EEConversation(ctx, tx, userID, req.Context.DeviceGenerationId, req.ConversationId)
	if err != nil {
		return nil, err
	}
	if c.State != "active" {
		return nil, domain.ErrE2EEConversationClosed
	}
	if c.PeerDevice.RetiredAt != nil {
		return nil, domain.ErrE2EEGenerationChanged
	}
	stored := new(acquisitionRecord)
	err = tx.QueryRowContext(ctx, `SELECT conversation_id::text,peer_user_id,peer_generation_id::text,signed_prekey_id,ec_prekey_id,kyber_prekey_id,kyber_is_last_resort,not_after,
        not_after<=clock_timestamp(),public_snapshot FROM e2ee_acquisitions WHERE owner_generation_id=$1 AND acquisition_id=$2`, req.Context.DeviceGenerationId, req.AcquisitionId).
		Scan(&stored.conversationID, &stored.peerUserID, &stored.peerGenerationID, &stored.signedID, &stored.ecID, &stored.pqID, &stored.lastResort, &stored.expires, &stored.expired, &stored.wire)
	if err == nil {
		if stored.conversationID != req.ConversationId || stored.peerUserID != c.PeerDevice.UserID || stored.peerGenerationID != c.PeerDevice.GenerationID || !expectedAcquisitionPeer(req, c.PeerDevice) {
			return nil, domain.ErrE2EERequestConflict
		}
		if stored.expired {
			return nil, domain.ErrE2EEAcquisitionExpired
		}
		result, err := stored.decode(req, c.PeerDevice)
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
	if !expectedAcquisitionPeer(req, c.PeerDevice) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if !c.OwnDevice.PublicMaterialReady || !c.PeerDevice.PublicMaterialReady {
		return nil, domain.ErrE2EEKeysNotReady
	}
	var total, live int
	err = tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER (WHERE conversation_id=$2 AND not_after>clock_timestamp())
        FROM e2ee_acquisitions WHERE owner_generation_id=$1`, req.Context.DeviceGenerationId, req.ConversationId).Scan(&total, &live)
	if err != nil {
		return nil, err
	}
	if total >= domain.MaxE2EEAcquisitionsPerGeneration || live >= domain.MaxE2EELiveAcquisitionsPerDirection {
		return nil, domain.ErrE2EEAcquisitionCapacity
	}
	d := c.PeerDevice
	reserved, err := reserveAcquisitionKeys(ctx, tx, d)
	if err != nil {
		return nil, err
	}
	result := &pb.AcquiredBundle{AcquisitionId: req.AcquisitionId, ConversationId: req.ConversationId, Device: reserved.device,
		SignedPreKey: reserved.signed, OneTimePreKey: reserved.ec, KyberPreKey: reserved.pq}
	pq := result.KyberPreKey
	var created time.Time
	if err := tx.QueryRowContext(ctx, `SELECT date_trunc('milliseconds',clock_timestamp())`).Scan(&created); err != nil {
		return nil, err
	}
	expires := created.Add(domain.E2EEAcquisitionLease)
	result.NotAfterUnixMs = expires.UnixMilli()
	if err := boundary.ValidateAcquiredBundle(req, result); err != nil {
		return nil, domain.ErrE2EEAcquisitionState
	}
	wire, err := proto.Marshal(result)
	if err != nil {
		return nil, err
	}
	var ecID any
	if result.OneTimePreKey != nil {
		ecID = result.OneTimePreKey.KeyId
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_acquisitions(owner_user_id,owner_generation_id,acquisition_id,conversation_id,peer_user_id,peer_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,signed_prekey_id,ec_prekey_id,kyber_prekey_id,kyber_is_last_resort,public_snapshot,created_at,not_after)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`, userID, req.Context.DeviceGenerationId, req.AcquisitionId, req.ConversationId, d.UserID, d.GenerationID,
		lowID, highID, lowGen, highGen, result.SignedPreKey.KeyId, ecID, pq.KeyId, pq.IsLastResort, wire, created, expires)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func lockAcquisitionRows(ctx context.Context, tx *sql.Tx, query string, first, second any) error {
	rows, err := tx.QueryContext(ctx, query, first, second)
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if count != 2 {
		return domain.ErrE2EEConversationNotFound
	}
	return nil
}

func expectedAcquisitionPeer(req *pb.AcquireBundleRequest, d *domain.E2EEDevice) bool {
	return req.ExpectedPeer.UserId == d.UserUUID && req.ExpectedPeer.GenerationId == d.GenerationID && req.ExpectedPeer.DeviceId == d.DeviceID
}

type acquisitionRecord struct {
	conversationID      string
	peerUserID          int64
	peerGenerationID    string
	signedID, pqID      uint32
	ecID                sql.NullInt64
	lastResort, expired bool
	expires             time.Time
	wire                []byte
}

func (a *acquisitionRecord) decode(req *pb.AcquireBundleRequest, d *domain.E2EEDevice) (*pb.AcquiredBundle, error) {
	result := new(pb.AcquiredBundle)
	if err := proto.Unmarshal(a.wire, result); err != nil {
		return nil, domain.ErrE2EEAcquisitionState
	}
	if err := boundary.ValidateAcquiredBundle(req, result); err != nil {
		return nil, domain.ErrE2EEAcquisitionState
	}
	if result.NotAfterUnixMs != a.expires.UnixMilli() || result.SignedPreKey.KeyId != a.signedID || result.KyberPreKey.KeyId != a.pqID || result.KyberPreKey.IsLastResort != a.lastResort ||
		result.Device.RegistrationId != d.RegistrationID || !bytes.Equal(result.Device.IdentityKey, d.IdentityKey) || (result.OneTimePreKey != nil) != a.ecID.Valid {
		return nil, domain.ErrE2EEAcquisitionState
	}
	if a.ecID.Valid && int64(result.OneTimePreKey.KeyId) != a.ecID.Int64 {
		return nil, domain.ErrE2EEAcquisitionState
	}
	return result, nil
}

// Public material only; caller holds canonical users/devices/conversation locks
// and validates the chosen protocol/epoch before consuming any one-time key.
type reservedAcquisitionKeys struct {
	device *pb.Device
	signed *pb.SignedPreKey
	ec     *pb.OneTimePreKey
	pq     *pb.KyberPreKey
}

func reserveAcquisitionKeys(ctx context.Context, tx *sql.Tx, d *domain.E2EEDevice) (*reservedAcquisitionKeys, error) {
	result := &reservedAcquisitionKeys{device: &pb.Device{Reference: &pb.DeviceRef{UserId: d.UserUUID, DeviceId: d.DeviceID, GenerationId: d.GenerationID},
		RegistrationId: d.RegistrationID, IdentityKey: bytes.Clone(d.IdentityKey), PublicMaterialReady: d.PublicMaterialReady, PublicMaterialRevision: d.PublicMaterialRevision}, signed: new(pb.SignedPreKey)}
	err := tx.QueryRowContext(ctx, `SELECT k.key_id,k.public_key,k.signature,k.created_at_unix_ms FROM e2ee_devices d
        JOIN e2ee_signed_prekeys k ON k.generation_id=d.generation_id AND k.key_id=d.active_signed_prekey_id WHERE d.generation_id=$1`, d.GenerationID).
		Scan(&result.signed.KeyId, &result.signed.PublicKey, &result.signed.Signature, &result.signed.CreatedAtUnixMs)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEKeysNotReady
	}
	if err != nil {
		return nil, err
	}
	ec := new(pb.OneTimePreKey)
	err = tx.QueryRowContext(ctx, `WITH picked AS (SELECT key_id FROM e2ee_ec_prekeys WHERE generation_id=$1 AND NOT used ORDER BY key_id LIMIT 1 FOR UPDATE SKIP LOCKED)
        UPDATE e2ee_ec_prekeys k SET used=TRUE FROM picked p WHERE k.generation_id=$1 AND k.key_id=p.key_id AND NOT k.used RETURNING k.key_id,k.public_key`, d.GenerationID).Scan(&ec.KeyId, &ec.PublicKey)
	if err == nil {
		result.ec = ec
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	pq := new(pb.KyberPreKey)
	err = tx.QueryRowContext(ctx, `WITH picked AS (SELECT key_id FROM e2ee_kyber_prekeys WHERE generation_id=$1 AND NOT used AND NOT is_last_resort ORDER BY key_id LIMIT 1 FOR UPDATE SKIP LOCKED)
        UPDATE e2ee_kyber_prekeys k SET used=TRUE FROM picked p WHERE k.generation_id=$1 AND k.key_id=p.key_id AND NOT k.used AND NOT k.is_last_resort
        RETURNING k.key_id,k.public_key,k.signature,k.is_last_resort`, d.GenerationID).Scan(&pq.KeyId, &pq.PublicKey, &pq.Signature, &pq.IsLastResort)
	if errors.Is(err, sql.ErrNoRows) {
		err = tx.QueryRowContext(ctx, `SELECT k.key_id,k.public_key,k.signature,k.is_last_resort FROM e2ee_devices d JOIN e2ee_kyber_prekeys k
            ON k.generation_id=d.generation_id AND k.key_id=d.active_last_resort_id WHERE d.generation_id=$1 AND k.is_last_resort AND NOT k.used`, d.GenerationID).
			Scan(&pq.KeyId, &pq.PublicKey, &pq.Signature, &pq.IsLastResort)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEKeysNotReady
	}
	if err != nil {
		return nil, err
	}
	result.pq = pq
	return result, nil
}
