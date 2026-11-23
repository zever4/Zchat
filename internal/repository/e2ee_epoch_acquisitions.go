package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// AcquireV2 never returns a sealed-epoch snapshot for a fresh bootstrap, even
// for an exact still-leased request. Key IDs/private retention are not reset.
func (repo *PgE2EEPreKeyRepository) AcquireV2(parent context.Context, uid int64, req *v2.AcquireBundleRequest) (*v2.AcquiredBundle, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateEpochAcquisition(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := repo.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	r, err := lockRecovery(ctx, tx, uid, req.Context.DeviceGenerationId, req.ConversationId)
	if err != nil {
		return nil, err
	}
	c := r.c
	epoch := *req.CryptoEpoch
	if !messageDeviceMatches(req.ExpectedPeer, c.PeerDevice) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	stored := new(acquisitionRecord)
	var version int
	var savedEpoch sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT conversation_id::text,peer_user_id,peer_generation_id::text,signed_prekey_id,ec_prekey_id,kyber_prekey_id,kyber_is_last_resort,
        not_after,not_after<=clock_timestamp(),public_snapshot,wire_version,crypto_epoch FROM e2ee_acquisitions WHERE owner_generation_id=$1 AND acquisition_id=$2`, req.Context.DeviceGenerationId, req.AcquisitionId).
		Scan(&stored.conversationID, &stored.peerUserID, &stored.peerGenerationID, &stored.signedID, &stored.ecID, &stored.pqID, &stored.lastResort, &stored.expires, &stored.expired, &stored.wire, &version, &savedEpoch)
	replay := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if replay && (version != 2 || !savedEpoch.Valid || savedEpoch.Int64 < 0 || uint64(savedEpoch.Int64) != epoch || stored.conversationID != req.ConversationId || stored.peerUserID != c.PeerDevice.UserID || stored.peerGenerationID != c.PeerDevice.GenerationID) {
		return nil, domain.ErrE2EERequestConflict
	}
	if c.State != "active" {
		return nil, domain.ErrE2EEConversationClosed
	}
	var state string
	var bootstrap sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT state,bootstrap_user_id FROM e2ee_conversation_epochs WHERE conversation_id=$1 AND epoch=$2 FOR SHARE`, c.ID, epoch).Scan(&state, &bootstrap)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEEpochInactive
	}
	if err != nil {
		return nil, err
	}
	if epoch != r.epoch || state != "active" {
		return nil, domain.ErrE2EEEpochInactive
	}
	if epoch > 0 && (!bootstrap.Valid || bootstrap.Int64 != uid) {
		return nil, domain.ErrE2EEBootstrapInitiator
	}
	if replay {
		if stored.expired {
			// A failed/canceled read transaction is not authoritative expiry.
			if err = tx.Commit(); err != nil {
				return nil, err
			}
			return nil, domain.ErrE2EEAcquisitionExpired
		}
		result, err := stored.decodeV2(req, c.PeerDevice)
		if err != nil {
			return nil, err
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
		return result, nil
	}
	if !c.OwnDevice.PublicMaterialReady || !c.PeerDevice.PublicMaterialReady {
		return nil, domain.ErrE2EEKeysNotReady
	}
	var total, live int
	err = tx.QueryRowContext(ctx, `SELECT count(*),count(*) FILTER(WHERE conversation_id=$2 AND wire_version=2 AND crypto_epoch=$3 AND not_after>clock_timestamp())
        FROM e2ee_acquisitions WHERE owner_generation_id=$1`, req.Context.DeviceGenerationId, c.ID, epoch).Scan(&total, &live)
	if err != nil {
		return nil, err
	}
	if total >= domain.MaxE2EEAcquisitionsPerGeneration || live >= domain.MaxE2EELiveAcquisitionsPerDirection {
		return nil, domain.ErrE2EEAcquisitionCapacity
	}
	keys, err := reserveAcquisitionKeys(ctx, tx, c.PeerDevice)
	if err != nil {
		return nil, err
	}
	expires := r.now.Add(domain.E2EEAcquisitionLease)
	result := &v2.AcquiredBundle{ProtocolVersion: 2, CryptoEpoch: &epoch, AcquisitionId: req.AcquisitionId, ConversationId: c.ID, Device: keys.device,
		SignedPreKey: keys.signed, OneTimePreKey: keys.ec, KyberPreKey: keys.pq, NotAfterUnixMs: expires.UnixMilli()}
	if boundary.ValidateEpochAcquiredBundle(req, result) != nil {
		return nil, domain.ErrE2EEAcquisitionState
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(result)
	if err != nil {
		return nil, err
	}
	var ec any
	if keys.ec != nil {
		ec = keys.ec.KeyId
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_acquisitions(owner_user_id,owner_generation_id,acquisition_id,conversation_id,peer_user_id,peer_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,signed_prekey_id,ec_prekey_id,kyber_prekey_id,kyber_is_last_resort,public_snapshot,created_at,not_after,wire_version,crypto_epoch)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,2,$18)`, uid, req.Context.DeviceGenerationId, req.AcquisitionId, c.ID, c.PeerDevice.UserID, c.PeerDevice.GenerationID,
		r.low, r.high, r.lowGen, r.highGen, keys.signed.KeyId, ec, keys.pq.KeyId, keys.pq.IsLastResort, wire, r.now, expires, epoch)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return result, nil
}

func (a *acquisitionRecord) decodeV2(req *v2.AcquireBundleRequest, d *domain.E2EEDevice) (*v2.AcquiredBundle, error) {
	result := new(v2.AcquiredBundle)
	if len(a.wire) > 8192 || proto.Unmarshal(a.wire, result) != nil || boundary.ValidateEpochAcquiredBundle(req, result) != nil {
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
