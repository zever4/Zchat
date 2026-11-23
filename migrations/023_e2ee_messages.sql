-- +goose Up
-- Per-generation counters/sequence are locked in canonical user order. Sequence
-- increments COMMIT with the inbox row, so concurrent sends cannot publish a
-- later sequence while an earlier sequence is still an uncommitted reservation.
CREATE TABLE e2ee_delivery_state (
    generation_id UUID PRIMARY KEY,
    user_id BIGINT NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence>=0),
    sent_receipts BIGINT NOT NULL DEFAULT 0 CHECK (sent_receipts BETWEEN 0 AND 100000),
    pending_messages INTEGER NOT NULL DEFAULT 0 CHECK (pending_messages BETWEEN 0 AND 10000),
    pending_bytes BIGINT NOT NULL DEFAULT 0 CHECK (pending_bytes BETWEEN 0 AND 67108864),
    FOREIGN KEY (user_id,generation_id) REFERENCES e2ee_devices(user_id,generation_id)
);

-- Global message UUID also matches the recipient's per-generation local inbox
-- key and ACK contract (which has no sender namespace). Reusing it with another
-- sender/conversation/envelope is a conflict, never another delivered message.
-- Receipt survives ACK/expiry/removal of the ciphertext; retry never re-enqueues.
CREATE TABLE e2ee_send_receipts (
    message_id UUID PRIMARY KEY,
    conversation_id UUID NOT NULL,
    sender_user_id BIGINT NOT NULL,
    sender_generation_id UUID NOT NULL,
    recipient_user_id BIGINT NOT NULL,
    recipient_generation_id UUID NOT NULL,
    low_user_id BIGINT NOT NULL,
    high_user_id BIGINT NOT NULL,
    low_generation_id UUID NOT NULL,
    high_generation_id UUID NOT NULL,
    envelope_digest BYTEA NOT NULL CHECK (octet_length(envelope_digest)=32),
    stored_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id)
        REFERENCES e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id),
    CHECK ((sender_user_id=low_user_id AND recipient_user_id=high_user_id AND sender_generation_id=low_generation_id AND recipient_generation_id=high_generation_id) OR
           (sender_user_id=high_user_id AND recipient_user_id=low_user_id AND sender_generation_id=high_generation_id AND recipient_generation_id=low_generation_id)),
    CHECK (expires_at=stored_at+INTERVAL '720 hours'),
    CHECK (substring(message_id::text,15,1)='4' AND substring(message_id::text,20,1) IN ('8','9','a','b')),
    UNIQUE (message_id,recipient_generation_id,expires_at)
);
CREATE INDEX e2ee_sender_receipts ON e2ee_send_receipts(sender_generation_id,stored_at,message_id);

CREATE TABLE e2ee_message_inbox (
    recipient_generation_id UUID NOT NULL REFERENCES e2ee_delivery_state(generation_id),
    sequence BIGINT NOT NULL CHECK (sequence>0),
    message_id UUID NOT NULL UNIQUE,
    envelope BYTEA NOT NULL CHECK (octet_length(envelope) BETWEEN 1 AND 49152),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (recipient_generation_id,sequence),
    FOREIGN KEY (message_id,recipient_generation_id,expires_at)
        REFERENCES e2ee_send_receipts(message_id,recipient_generation_id,expires_at)
);
CREATE INDEX e2ee_inbox_expiry ON e2ee_message_inbox(expires_at,recipient_generation_id,sequence);

-- Receipt mutation must not silently redefine the accepted envelope or extend
-- its lifetime. Future ACK/expiry state belongs outside this immutable ledger.
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_send_receipt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'E2EE persistence receipt is immutable';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_send_receipt BEFORE UPDATE ON e2ee_send_receipts
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_send_receipt();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM e2ee_send_receipts) OR EXISTS (SELECT 1 FROM e2ee_message_inbox)
        OR EXISTS (SELECT 1 FROM e2ee_delivery_state WHERE sent_receipts<>0 OR pending_messages<>0 OR pending_bytes<>0 OR last_sequence<>0) THEN
        RAISE EXCEPTION 'cannot roll back nonempty E2EE message state';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_send_receipt ON e2ee_send_receipts;
DROP FUNCTION e2ee_guard_send_receipt();
DROP TABLE e2ee_message_inbox;
DROP TABLE e2ee_send_receipts;
DROP TABLE e2ee_delivery_state;
