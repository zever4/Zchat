package repository

// Historical migration fixtures must write the schema under test, rather than
// teaching today's runtime to fall back when a required table is absent.
import (
	"database/sql"
	"testing"
	"time"

	pb "github.com/zever4/Zchat/gen/e2ee"
	v2 "github.com/zever4/Zchat/gen/e2eev2"
	boundary "github.com/zever4/Zchat/internal/e2ee"
	"google.golang.org/protobuf/proto"
)

func seedHistoricalE2EESend(t *testing.T, db *sql.DB, message proto.Message) {
	t.Helper()
	var table sql.NullString
	if err := db.QueryRowContext(t.Context(), `SELECT to_regclass('e2ee_message_claims')::text`).Scan(&table); err != nil || table.Valid {
		t.Fatal("historical fixture requires pre-claim schema", table, err)
	}
	var id, chat, senderGen, recipientGen string
	var digest [32]byte
	var err error
	var epoch *uint64
	switch e := message.(type) {
	case *pb.Envelope:
		id, chat, senderGen, recipientGen = e.MessageId, e.ConversationId, e.Sender.GenerationId, e.Recipient.GenerationId
		digest, err = boundary.EnvelopeDigest(e)
	case *v2.Envelope:
		id, chat, senderGen, recipientGen = e.MessageId, e.ConversationId, e.Sender.GenerationId, e.Recipient.GenerationId
		epoch = e.CryptoEpoch
		digest, err = boundary.EpochEnvelopeDigest(e)
	default:
		t.Fatal("unsupported historical fixture")
	}
	if err != nil {
		t.Fatal(err)
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	columns := `message_id,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,low_user_id,high_user_id,low_generation_id,high_generation_id,envelope_digest,stored_at,expires_at`
	values := `$1,c.conversation_id,s.user_id,s.generation_id,r.user_id,r.generation_id,c.low_user_id,c.high_user_id,c.low_generation_id,c.high_generation_id,$5,$6,$7`
	now := time.Now().UTC().Truncate(time.Millisecond)
	args := []any{id, chat, senderGen, recipientGen, digest[:], now, now.Add(720 * time.Hour)}
	if epoch != nil {
		columns += `,wire_version,crypto_epoch`
		values += `,2,$8`
		args = append(args, *epoch)
	}
	res, err := tx.ExecContext(t.Context(), `INSERT INTO e2ee_send_receipts(`+columns+`) SELECT `+values+` FROM e2ee_conversations c
        JOIN e2ee_devices s ON s.generation_id=$3 JOIN e2ee_devices r ON r.generation_id=$4 WHERE c.conversation_id=$2`, args...)
	if err != nil {
		t.Fatal(err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	if _, err = tx.ExecContext(t.Context(), `INSERT INTO e2ee_delivery_state(generation_id,user_id) SELECT generation_id,user_id FROM e2ee_devices WHERE generation_id IN ($1,$2)`, senderGen, recipientGen); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(t.Context(), `UPDATE e2ee_delivery_state SET sent_receipts=1 WHERE generation_id=$1`, senderGen); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(t.Context(), `UPDATE e2ee_delivery_state SET last_sequence=1,pending_messages=1,pending_bytes=$2 WHERE generation_id=$1`, recipientGen, len(wire)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(t.Context(), `INSERT INTO e2ee_message_inbox(recipient_generation_id,sequence,message_id,envelope,expires_at) VALUES($1,1,$2,$3,$4)`, recipientGen, id, wire, now.Add(720*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
