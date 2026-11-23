package repository

import (
	"context"
	"database/sql"
	"time"

	"github.com/lib/pq"
	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	"github.com/zever4/Zchat/internal/domain"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

// Accepted metadata only, independent of message ACK/inbox and upload state.
// No FS/read/epoch-head gate, expiry mutation or physical-availability assertion.
// The native RPC holds live session/device guards through this read COMMIT.
func (r *PgE2EEBlobRepository) FileStatuses(parent context.Context, uid int64, input *v2.GetFileStatusesRequest) (*v2.FileStatuses, error) {
	if uid <= 0 {
		return nil, domain.ErrInvalidInput
	}
	if err := boundary.ValidateFileStatusRequest(input); err != nil {
		return nil, err
	}
	req := proto.Clone(input).(*v2.GetFileStatusesRequest)
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	ids := make([]string, len(req.Files))
	for i, ref := range req.Files {
		ids[i] = ref.Binding.MessageId
	}
	if err = lockFileStatusOwners(ctx, tx, uid, req.Context.DeviceGenerationId, ids); err != nil {
		return nil, err
	}
	// SAME canonical claim order as FILEACK. Its INSERT cannot race absence
	// observation; future accepted GC must also serialize on this claim.
	if err = lockDeliveryRows(ctx, tx, `SELECT 1 FROM e2ee_message_claims
        WHERE message_id=ANY($1::uuid[]) AND sender_user_id=$2 AND sender_generation_id=$3 AND wire_version=3
        ORDER BY message_id FOR SHARE`, []any{pq.Array(ids), uid, req.Context.DeviceGenerationId}, len(ids)); err != nil {
		return nil, err
	}
	facts, err := loadFileStatusFacts(ctx, tx, ids)
	if err != nil {
		return nil, err
	}
	if len(facts) != len(ids) {
		return nil, domain.ErrE2EEFileStatusUnavailable
	}
	for _, ref := range req.Files {
		if !proto.Equal(ref, facts[ref.Binding.MessageId].reference) {
			return nil, domain.ErrE2EEFileStatusUnavailable
		}
	}
	var now time.Time
	// Deliberately after ALL lock waits, shared by the whole ordered batch.
	if err = tx.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	response := &v2.FileStatuses{Files: make([]*v2.FileStatus, len(ids))}
	for i, id := range ids {
		f := facts[id]
		s := &v2.FileStatus{FormatVersion: boundary.FileStatusFormat, Reference: f.reference, Receipt: f.receipt,
			ObservedAtUnixMs: now.UnixMilli(), ExpiresAtUnixMs: time.UnixMilli(f.receipt.StoredAtUnixMs).Add(domain.E2EEBlobAcceptedRetention).UnixMilli()}
		if f.acknowledged.Valid {
			at := f.acknowledged.Time.UnixMilli()
			s.AcknowledgedAtUnixMs = &at
		}
		if f.retired.Valid {
			at := f.retired.Time.UnixMilli()
			s.RecipientReplacedAtUnixMs = &at
		}
		response.Files[i] = s
	}
	if boundary.ValidateFileStatuses(req, response) != nil {
		return nil, domain.ErrE2EEMessageState
	}
	// Failed/cancelled/unknown COMMIT returns neither partial facts nor absence.
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	return response, nil
}

// Discover ONLY sender-owned immutable receipts before locking all participants
// in the same global users->devices order as Send/message outcomes/replacement.
// Never lock pair1 then pair2, or claim->device (cross-account lock inversion).
func lockFileStatusOwners(ctx context.Context, tx *sql.Tx, uid int64, gen string, ids []string) error {
	rows, err := tx.QueryContext(ctx, `SELECT sender_user_id,sender_generation_id::text,recipient_user_id,recipient_generation_id::text
        FROM e2ee_send_receipts WHERE message_id=ANY($1::uuid[]) AND sender_user_id=$2 AND sender_generation_id=$3 AND wire_version=3`, pq.Array(ids), uid, gen)
	if err != nil {
		return err
	}
	users, generations := map[int64]bool{}, map[string]bool{}
	count := 0
	for rows.Next() {
		var sender, recipient int64
		var sg, rg string
		if err = rows.Scan(&sender, &sg, &recipient, &rg); err != nil {
			rows.Close()
			return err
		}
		users[sender], users[recipient], generations[sg], generations[rg] = true, true, true, true
		count++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if count != len(ids) {
		return domain.ErrE2EEFileStatusUnavailable
	}
	uids := make([]int64, 0, len(users))
	gens := make([]string, 0, len(generations))
	for id := range users {
		uids = append(uids, id)
	}
	for id := range generations {
		gens = append(gens, id)
	}
	if err = lockDeliveryRows(ctx, tx, `SELECT id FROM users WHERE id=ANY($1::bigint[]) ORDER BY id FOR SHARE`, pq.Array(uids), len(uids)); err != nil {
		return err
	}
	if err = lockDeliveryRows(ctx, tx, `SELECT user_id FROM e2ee_devices WHERE generation_id=ANY($1::uuid[]) ORDER BY user_id,generation_id FOR SHARE`, pq.Array(gens), len(gens)); err != nil {
		return err
	}
	_, err = currentDeliveryOwner(ctx, tx, uid, gen)
	return err
}

type fileStatusFact struct {
	reference             *v2.FileStatusReference
	receipt               *v2.SendReceipt
	acknowledged, retired sql.NullTime
}

func loadFileStatusFacts(ctx context.Context, tx *sql.Tx, ids []string) (map[string]*fileStatusFact, error) {
	rows, err := tx.QueryContext(ctx, `SELECT c.message_id::text,c.conversation_id::text,c.crypto_epoch,
        su.user_uuid,c.sender_generation_id::text,ru.user_uuid,c.recipient_generation_id::text,
        c.blob_id::text,c.blob_bytes,c.blob_sha256,r.envelope_digest,r.stored_at,r.expires_at,
        ack.acknowledged_at,d.retired_at,
        c.wire_version=3 AND r.wire_version=3 AND c.envelope_digest IS NULL AND
        ROW(c.conversation_id,c.crypto_epoch,c.sender_user_id,c.sender_generation_id,c.recipient_user_id,c.recipient_generation_id)
        IS NOT DISTINCT FROM ROW(r.conversation_id,r.crypto_epoch,r.sender_user_id,r.sender_generation_id,r.recipient_user_id,r.recipient_generation_id)
        FROM e2ee_message_claims c JOIN e2ee_send_receipts r USING(message_id) JOIN e2ee_message_blobs l USING(message_id)
        JOIN users su ON su.id=c.sender_user_id JOIN users ru ON ru.id=c.recipient_user_id
        JOIN e2ee_devices d ON d.generation_id=c.recipient_generation_id
        LEFT JOIN e2ee_file_acknowledgements ack USING(message_id)
        WHERE c.message_id=ANY($1::uuid[]) ORDER BY c.message_id FOR SHARE OF r,l`, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	facts := map[string]*fileStatusFact{}
	for rows.Next() {
		b := &pb.EpochSendBinding{ProtocolVersion: 3, Sender: &pb.DeviceRef{DeviceId: 1}, Recipient: &pb.DeviceRef{DeviceId: 1}}
		blob := &v2.BlobRef{FormatVersion: 1}
		var epoch sql.NullInt64
		var bytes int64
		var stored, expires time.Time
		var consistent bool
		f := &fileStatusFact{}
		if err = rows.Scan(&b.MessageId, &b.ConversationId, &epoch, &b.Sender.UserId, &b.Sender.GenerationId, &b.Recipient.UserId, &b.Recipient.GenerationId,
			&blob.BlobId, &bytes, &blob.Sha256, &b.EnvelopeDigest, &stored, &expires, &f.acknowledged, &f.retired, &consistent); err != nil {
			return nil, err
		}
		if !consistent || !epoch.Valid || epoch.Int64 < 0 || bytes <= 0 || stored.UnixMilli() <= 0 || stored.Nanosecond()%int(time.Millisecond) != 0 || expires.Sub(stored) != domain.E2EEMessageRetention {
			return nil, domain.ErrE2EEMessageState
		}
		b.Epoch, blob.CiphertextBytes = uint64(epoch.Int64), uint64(bytes)
		f.reference = &v2.FileStatusReference{Binding: b, Blob: blob}
		f.receipt = &v2.SendReceipt{MessageId: b.MessageId, EnvelopeDigest: append([]byte(nil), b.EnvelopeDigest...), StoredAtUnixMs: stored.UnixMilli(), ExpiresAtUnixMs: expires.UnixMilli(), CryptoEpoch: &b.Epoch}
		facts[b.MessageId] = f
	}
	return facts, rows.Err()
}
