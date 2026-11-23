package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/lib/pq"
	pb "github.com/zever4/Zchat/gen/e2ee"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
)

type PgE2EEPreKeyRepository struct{ db *sql.DB }

func NewPgE2EEPreKeyRepository(db *sql.DB) *PgE2EEPreKeyRepository {
	return &PgE2EEPreKeyRepository{db: db}
}

// Publish serializes with Register/Replace using the same user -> device lock
// order. Public material, active pointers, readiness, CAS revision and receipt
// commit together. A lost response never consumes another revision.
func (r *PgE2EEPreKeyRepository) Publish(parent context.Context, userID int64, req *pb.UploadPreKeysRequest) (*domain.E2EEPublication, error) {
	if userID <= 0 {
		return nil, domain.ErrInvalidInput
	}
	digest, err := boundary.PublicationDigest(req)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var lockedID int64
	if err = tx.QueryRowContext(ctx, `SELECT id FROM users WHERE id=$1 FOR UPDATE`, userID).Scan(&lockedID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrUserNotFound
		}
		return nil, err
	}
	gen := req.Context.DeviceGenerationId
	var revision uint64
	var activeSigned, activeLR sql.NullInt64
	err = tx.QueryRowContext(ctx, `SELECT public_material_revision,active_signed_prekey_id,active_last_resort_id
        FROM e2ee_devices WHERE user_id=$1 AND generation_id=$2 AND retired_at IS NULL FOR UPDATE`, userID, gen).Scan(&revision, &activeSigned, &activeLR)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrE2EEGenerationChanged
	}
	if err != nil {
		return nil, err
	}
	var storedDigest []byte
	var storedRevision uint64
	err = tx.QueryRowContext(ctx, `SELECT digest,publication_revision FROM e2ee_publication_batches WHERE generation_id=$1 AND batch_id=$2`, gen, req.BatchId).Scan(&storedDigest, &storedRevision)
	if err == nil {
		if !bytes.Equal(digest[:], storedDigest) {
			return nil, domain.ErrE2EERequestConflict
		}
		return &domain.E2EEPublication{BatchID: req.BatchId, Digest: digest, Revision: storedRevision, Replayed: true}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	if req.ExpectedRevision != revision {
		return nil, domain.ErrE2EEPublicationChanged
	}
	if k := req.SignedPreKey; k != nil {
		if err := publicationActivation(ctx, tx, gen, "e2ee_signed_prekeys", k.KeyId, activeSigned); err != nil {
			return nil, err
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO e2ee_signed_prekeys(generation_id,key_id,public_key,signature,created_at_unix_ms) VALUES($1,$2,$3,$4,$5)
            ON CONFLICT(generation_id,key_id) DO UPDATE SET public_key=e2ee_signed_prekeys.public_key
            WHERE e2ee_signed_prekeys.public_key=EXCLUDED.public_key AND e2ee_signed_prekeys.signature=EXCLUDED.signature
              AND e2ee_signed_prekeys.created_at_unix_ms=EXCLUDED.created_at_unix_ms`, gen, k.KeyId, k.PublicKey, k.Signature, k.CreatedAtUnixMs)
		if err := publicationRows(res, err, 1); err != nil {
			return nil, err
		}
		activeSigned = sql.NullInt64{Int64: int64(k.KeyId), Valid: true}
	}
	if len(req.OneTimePreKeys) != 0 {
		ids := make([]int64, 0, len(req.OneTimePreKeys))
		keys := make(pq.ByteaArray, 0, len(req.OneTimePreKeys))
		for _, k := range req.OneTimePreKeys {
			ids = append(ids, int64(k.KeyId))
			keys = append(keys, k.PublicKey)
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO e2ee_ec_prekeys(generation_id,key_id,public_key)
            SELECT $1,key_id,public_key FROM unnest($2::bigint[],$3::bytea[]) AS k(key_id,public_key)
            ON CONFLICT(generation_id,key_id) DO UPDATE SET public_key=e2ee_ec_prekeys.public_key
            WHERE e2ee_ec_prekeys.public_key=EXCLUDED.public_key`, gen, pq.Array(ids), keys)
		if err := publicationRows(res, err, int64(len(ids))); err != nil {
			return nil, err
		}
	}
	if len(req.KyberPreKeys) != 0 {
		ids := make([]int64, 0, len(req.KyberPreKeys))
		keys := make(pq.ByteaArray, 0, len(req.KyberPreKeys))
		sigs := make(pq.ByteaArray, 0, len(req.KyberPreKeys))
		lr := make([]bool, 0, len(req.KyberPreKeys))
		for _, k := range req.KyberPreKeys {
			ids = append(ids, int64(k.KeyId))
			keys = append(keys, k.PublicKey)
			sigs = append(sigs, k.Signature)
			lr = append(lr, k.IsLastResort)
			if k.IsLastResort {
				if err := publicationActivation(ctx, tx, gen, "e2ee_kyber_prekeys", k.KeyId, activeLR); err != nil {
					return nil, err
				}
				activeLR = sql.NullInt64{Int64: int64(k.KeyId), Valid: true}
			}
		}
		res, err := tx.ExecContext(ctx, `INSERT INTO e2ee_kyber_prekeys(generation_id,key_id,public_key,signature,is_last_resort)
            SELECT $1,key_id,public_key,signature,is_last_resort FROM unnest($2::bigint[],$3::bytea[],$4::bytea[],$5::boolean[]) AS k(key_id,public_key,signature,is_last_resort)
            ON CONFLICT(generation_id,key_id) DO UPDATE SET public_key=e2ee_kyber_prekeys.public_key
            WHERE e2ee_kyber_prekeys.public_key=EXCLUDED.public_key AND e2ee_kyber_prekeys.signature=EXCLUDED.signature AND e2ee_kyber_prekeys.is_last_resort=EXCLUDED.is_last_resort`, gen, pq.Array(ids), keys, sigs, pq.Array(lr))
		if err := publicationRows(res, err, int64(len(ids))); err != nil {
			return nil, err
		}
	}
	// Counts include consumed keys: uploading another batch cannot bypass the
	// retained-state budget, even if a future acquirer has exhausted the pool.
	var ec, pqCount, signed, batches int
	err = tx.QueryRowContext(ctx, `SELECT
        (SELECT count(*) FROM e2ee_ec_prekeys WHERE generation_id=$1),
        (SELECT count(*) FROM e2ee_kyber_prekeys WHERE generation_id=$1),
        (SELECT count(*) FROM e2ee_signed_prekeys WHERE generation_id=$1),
        (SELECT count(*) FROM e2ee_publication_batches WHERE generation_id=$1)`, gen).Scan(&ec, &pqCount, &signed, &batches)
	if err != nil {
		return nil, err
	}
	if ec > domain.MaxE2EERetainedPreKeys || pqCount > domain.MaxE2EERetainedPreKeys || signed > domain.MaxE2EERetainedSignedKeys || batches >= domain.MaxE2EEPublicationBatches {
		return nil, domain.ErrE2EEKeyCapacity
	}
	revision++
	_, err = tx.ExecContext(ctx, `UPDATE e2ee_devices SET active_signed_prekey_id=$2,active_last_resort_id=$3,
        public_material_ready=($2::bigint IS NOT NULL AND $3::bigint IS NOT NULL),public_material_revision=$4 WHERE generation_id=$1`, gen, activeSigned, activeLR, revision)
	if err != nil {
		return nil, err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO e2ee_publication_batches(generation_id,batch_id,digest,publication_revision) VALUES($1,$2,$3,$4)`, gen, req.BatchId, digest[:], revision)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return &domain.E2EEPublication{BatchID: req.BatchId, Digest: digest, Revision: revision}, nil
}

// A genuinely new rotation can activate a new ID or keep the active one. It
// cannot reactivate a retained old signed/last-resort key with a fresh batch ID.
func publicationActivation(ctx context.Context, tx *sql.Tx, gen, table string, id uint32, active sql.NullInt64) error {
	if active.Valid && active.Int64 == int64(id) {
		return nil
	}
	var exists bool
	err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM `+pq.QuoteIdentifier(table)+` WHERE generation_id=$1 AND key_id=$2)`, gen, id).Scan(&exists)
	if err != nil {
		return err
	}
	if exists {
		return domain.ErrE2EEKeyConflict
	}
	return nil
}

func publicationRows(result sql.Result, err error, want int64) error {
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n != want {
		return domain.ErrE2EEKeyConflict
	}
	return nil
}
