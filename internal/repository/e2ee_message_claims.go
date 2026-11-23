package repository

import (
	"bytes"
	"context"
	"database/sql"
	"errors"

	"github.com/lib/pq"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
)

type e2eeMessageClaim struct {
	message, chat           string
	sender, recipient       int64
	senderGen, recipientGen string
	low, high               int64
	lowGen, highGen         string
	version                 int
	epoch                   *uint64
	digest                  []byte
	blob                    *v2.BlobRef
}

// Caller holds canonical route/delivery locks. The UUID row, not a prior SELECT
// or separate UNIQUE indexes in receipt/blob tables, serializes first use.
func claimE2EEMessage(ctx context.Context, tx *sql.Tx, c e2eeMessageClaim) (bool, error) {
	var epoch, blobID, blobBytes, blobSHA any
	if c.epoch != nil {
		epoch = *c.epoch
	}
	if c.blob != nil {
		blobID, blobBytes, blobSHA = c.blob.BlobId, c.blob.CiphertextBytes, c.blob.Sha256
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO e2ee_message_claims(message_id,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
        low_user_id,high_user_id,low_generation_id,high_generation_id,wire_version,crypto_epoch,envelope_digest,blob_id,blob_bytes,blob_sha256)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16) ON CONFLICT(message_id) DO NOTHING`,
		c.message, c.chat, c.sender, c.senderGen, c.recipient, c.recipientGen, c.low, c.high, c.lowGen, c.highGen, c.version, epoch, c.digest, blobID, blobBytes, blobSHA)
	if err != nil {
		return false, blobUniqueConflict(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if err = checkE2EEMessageClaim(ctx, tx, c); err != nil {
		return false, err
	}
	return n == 1, nil
}

// Locks and compares an existing binding without minting a first-use claim.
func checkE2EEMessageClaim(ctx context.Context, tx *sql.Tx, c e2eeMessageClaim) error {
	var chat, senderGen, recipientGen string
	var sender, recipient int64
	var version int
	var storedEpoch, storedBytes sql.NullInt64
	var storedBlob sql.NullString
	var digest, sha []byte
	err := tx.QueryRowContext(ctx, `SELECT conversation_id::text,sender_user_id,sender_generation_id::text,recipient_user_id,recipient_generation_id::text,
        wire_version,crypto_epoch,envelope_digest,blob_id::text,blob_bytes,blob_sha256 FROM e2ee_message_claims WHERE message_id=$1 FOR UPDATE`, c.message).
		Scan(&chat, &sender, &senderGen, &recipient, &recipientGen, &version, &storedEpoch, &digest, &storedBlob, &storedBytes, &sha)
	if err != nil {
		return err
	}
	epochMatches := c.epoch == nil && !storedEpoch.Valid || c.epoch != nil && storedEpoch.Valid && uint64(storedEpoch.Int64) == *c.epoch
	blobMatches := c.blob == nil && !storedBlob.Valid && !storedBytes.Valid && sha == nil || c.blob != nil && storedBlob.Valid && storedBytes.Valid &&
		storedBlob.String == c.blob.BlobId && uint64(storedBytes.Int64) == c.blob.CiphertextBytes && bytes.Equal(sha, c.blob.Sha256)
	if chat != c.chat || sender != c.sender || senderGen != c.senderGen || recipient != c.recipient || recipientGen != c.recipientGen ||
		version != c.version || !epochMatches || !bytes.Equal(digest, c.digest) || !blobMatches {
		return domain.ErrE2EEMessageConflict
	}
	return nil
}

func blobUniqueConflict(err error) error {
	var pg *pq.Error
	if errors.As(err, &pg) && pg.Code == "23505" {
		return domain.ErrE2EEMessageConflict
	}
	return err
}
