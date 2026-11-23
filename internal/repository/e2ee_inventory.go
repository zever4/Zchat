package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

// Inventory uses ONE statement/MVCC snapshot, including active pointers and all
// counters. It cannot mix a publication/consumption before and after COMMIT.
// No peer ID, credential, private material, or per-key used list is returned.
func (r *PgE2EEPreKeyRepository) Inventory(parent context.Context, userID int64, req *pb.GetPreKeyInventoryRequest) (*pb.PreKeyInventory, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateInventory(req); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	v := &pb.PreKeyInventory{OwnDevice: &pb.DeviceRef{}, MaxRetainedEc: domain.MaxE2EERetainedPreKeys,
		MaxRetainedPq: domain.MaxE2EERetainedPreKeys, MaxRetainedSigned: domain.MaxE2EERetainedSignedKeys,
		MaxPublicationBatches: domain.MaxE2EEPublicationBatches}
	var signed, lr, created sql.NullInt64
	err := r.db.QueryRowContext(ctx, `SELECT u.user_uuid::text,d.device_id,d.generation_id::text,
        d.public_material_revision,d.public_material_ready,d.active_signed_prekey_id,d.active_last_resort_id,s.created_at_unix_ms,
        (SELECT count(*) FROM e2ee_ec_prekeys WHERE generation_id=d.generation_id AND NOT used),
        (SELECT count(*) FROM e2ee_kyber_prekeys WHERE generation_id=d.generation_id AND NOT used AND NOT is_last_resort),
        (SELECT count(*) FROM e2ee_ec_prekeys WHERE generation_id=d.generation_id),
        (SELECT count(*) FROM e2ee_kyber_prekeys WHERE generation_id=d.generation_id),
        (SELECT count(*) FROM e2ee_signed_prekeys WHERE generation_id=d.generation_id),
        (SELECT count(*) FROM e2ee_publication_batches WHERE generation_id=d.generation_id)
        FROM e2ee_devices d JOIN users u ON u.id=d.user_id
        LEFT JOIN e2ee_signed_prekeys s ON s.generation_id=d.generation_id AND s.key_id=d.active_signed_prekey_id
        WHERE d.user_id=$1 AND d.generation_id=$2 AND d.retired_at IS NULL`, userID, req.Context.DeviceGenerationId).Scan(
		&v.OwnDevice.UserId, &v.OwnDevice.DeviceId, &v.OwnDevice.GenerationId, &v.PublicationRevision, &v.PublicMaterialReady,
		&signed, &lr, &created, &v.AvailableEc, &v.AvailablePq, &v.RetainedEc, &v.RetainedPq, &v.RetainedSigned, &v.PublicationBatches)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if err != nil {
		return nil, err
	}
	if signed.Valid {
		id := uint32(signed.Int64) // schema enforces the full uint32 range
		v.ActiveSignedId = &id
	}
	if lr.Valid {
		id := uint32(lr.Int64)
		v.ActiveLastResortId = &id
	}
	if created.Valid {
		v.ActiveSignedCreatedAtUnixMs = &created.Int64
	}
	return v, nil
}
