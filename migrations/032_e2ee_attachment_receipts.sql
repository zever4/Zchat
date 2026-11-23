-- +goose Up
-- Reservations remain immutable. Signal digest belongs to the immutable receipt;
-- this separate immutable link selects the exact accepted filesystem attempt.
-- +goose StatementBegin
DO $$ DECLARE binding TEXT; BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_blob_attempts WHERE state='linked') THEN
        RAISE EXCEPTION 'unaccepted prototype Linked state requires explicit cleanup';
    END IF;
    SELECT conname INTO STRICT binding FROM pg_constraint
    WHERE conrelid='e2ee_send_receipts'::regclass AND contype='c'
      AND pg_get_constraintdef(oid) LIKE '%wire_version%';
    EXECUTE format('ALTER TABLE e2ee_send_receipts DROP CONSTRAINT %I',binding);
END $$;
-- +goose StatementEnd
ALTER TABLE e2ee_send_receipts ADD CONSTRAINT e2ee_receipt_wire_epoch
    CHECK((wire_version=1 AND crypto_epoch IS NULL) OR
          (wire_version IN (2,3) AND crypto_epoch IS NOT NULL AND crypto_epoch>=0));
CREATE TABLE e2ee_message_blobs (
    message_id UUID PRIMARY KEY REFERENCES e2ee_send_receipts(message_id),
    revision BIGINT NOT NULL CHECK(revision>0),
    attempt_id UUID NOT NULL UNIQUE,
    FOREIGN KEY(message_id,revision,attempt_id) REFERENCES e2ee_blob_attempts(message_id,revision,attempt_id)
);
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_message_blob() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'accepted blob link is immutable'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_message_blob BEFORE UPDATE OR DELETE ON e2ee_message_blobs
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_message_blob();

-- Acquisition framing/creator checks keep using the original function.
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_epoch_send_v3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE head BIGINT; cs TEXT; es TEXT;
BEGIN
    SELECT active_epoch,state INTO head,cs FROM e2ee_conversations WHERE conversation_id=NEW.conversation_id FOR SHARE;
    IF cs IS DISTINCT FROM 'active' THEN RAISE EXCEPTION 'new persistence requires active conversation'; END IF;
    IF NEW.wire_version=1 THEN
        IF head<>0 THEN RAISE EXCEPTION 'prototype writer cannot cross recovery epoch'; END IF;
        RETURN NEW;
    END IF;
    IF NEW.wire_version NOT IN (2,3) OR NEW.crypto_epoch IS NULL OR NEW.crypto_epoch<>head THEN RAISE EXCEPTION 'new persistence requires active epoch'; END IF;
    SELECT state INTO es FROM e2ee_conversation_epochs WHERE conversation_id=NEW.conversation_id AND epoch=NEW.crypto_epoch FOR SHARE;
    IF es IS DISTINCT FROM 'active' THEN RAISE EXCEPTION 'new persistence requires unsealed epoch'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_epoch_send ON e2ee_send_receipts;
CREATE TRIGGER e2ee_guard_epoch_send BEFORE INSERT ON e2ee_send_receipts
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_epoch_send_v3();
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_receipt_claim_v3() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c e2ee_message_claims%ROWTYPE;
BEGIN
    SELECT * INTO c FROM e2ee_message_claims WHERE message_id=NEW.message_id FOR UPDATE;
    IF NOT FOUND OR
       ROW(c.conversation_id,c.sender_user_id,c.sender_generation_id,c.recipient_user_id,c.recipient_generation_id,
           c.low_user_id,c.high_user_id,c.low_generation_id,c.high_generation_id,c.wire_version,c.crypto_epoch)
       IS DISTINCT FROM
       ROW(NEW.conversation_id,NEW.sender_user_id,NEW.sender_generation_id,NEW.recipient_user_id,NEW.recipient_generation_id,
           NEW.low_user_id,NEW.high_user_id,NEW.low_generation_id,NEW.high_generation_id,NEW.wire_version,NEW.crypto_epoch) OR
       (c.wire_version IN (1,2) AND c.envelope_digest IS DISTINCT FROM NEW.envelope_digest) OR
       (c.wire_version=3 AND c.envelope_digest IS NOT NULL)
       THEN RAISE EXCEPTION 'receipt requires exact immutable claim'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_receipt_claim ON e2ee_send_receipts;
CREATE TRIGGER e2ee_guard_receipt_claim BEFORE INSERT ON e2ee_send_receipts
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_receipt_claim_v3();
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_link_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v SMALLINT; a e2ee_blob_attempts%ROWTYPE;
BEGIN
    SELECT wire_version INTO v FROM e2ee_send_receipts WHERE message_id=NEW.message_id;
    IF v IS DISTINCT FROM 3 THEN RAISE EXCEPTION 'blob link requires attachment receipt'; END IF;
    PERFORM message_id FROM e2ee_message_claims WHERE message_id=NEW.message_id FOR UPDATE;
    SELECT a0.* INTO a FROM e2ee_blob_heads h JOIN e2ee_blob_attempts a0
        ON (a0.message_id,a0.revision,a0.attempt_id)=(h.message_id,h.revision,h.attempt_id)
        WHERE h.message_id=NEW.message_id AND h.revision=NEW.revision AND h.attempt_id=NEW.attempt_id FOR UPDATE OF a0;
    IF NOT FOUND OR a.state<>'ready' OR a.writer_id IS NULL OR a.storage_id IS NULL OR a.expires_at<=clock_timestamp()
        THEN RAISE EXCEPTION 'blob link requires exact live Ready head'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_link_insert BEFORE INSERT ON e2ee_message_blobs
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_link_insert();
-- Receipt, exact link and Ready->Linked must commit together. ACK/inbox removal
-- cannot delete the link or receipt; accepted TTL comes only from the receipt.
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_attachment_receipt_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.wire_version=3 AND NOT EXISTS(
        SELECT 1 FROM e2ee_message_blobs b JOIN e2ee_blob_attempts a
        ON (a.message_id,a.revision,a.attempt_id)=(b.message_id,b.revision,b.attempt_id)
        WHERE b.message_id=NEW.message_id AND a.state='linked') THEN
        RAISE EXCEPTION 'attachment receipt requires committed Linked attempt';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER e2ee_guard_attachment_receipt_commit AFTER INSERT ON e2ee_send_receipts
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION e2ee_guard_attachment_receipt_commit();
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_acceptance() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='INSERT' THEN
        -- Lock BEFORE observing receipts. A later FK wait must not allow an
        -- observation made before a concurrent Send COMMIT to authorize INSERT.
        PERFORM message_id FROM e2ee_message_claims WHERE message_id=NEW.message_id FOR UPDATE;
        IF EXISTS(SELECT 1 FROM e2ee_send_receipts WHERE message_id=NEW.message_id) THEN
            RAISE EXCEPTION 'accepted message cannot allocate another attempt';
        END IF;
    END IF;
    IF TG_OP='UPDATE' AND OLD.state<>NEW.state AND NEW.state='linked' THEN
        IF OLD.state<>'ready' OR OLD.expires_at<=clock_timestamp() OR NOT EXISTS(
            SELECT 1 FROM e2ee_message_blobs b WHERE
            (b.message_id,b.revision,b.attempt_id)=(NEW.message_id,NEW.revision,NEW.attempt_id)) THEN
            RAISE EXCEPTION 'Linked requires live Ready and matching receipt link';
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_acceptance BEFORE INSERT OR UPDATE ON e2ee_blob_attempts
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_acceptance();
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_accepted_blob_head() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    PERFORM message_id FROM e2ee_message_claims WHERE message_id=NEW.message_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM e2ee_send_receipts WHERE message_id=NEW.message_id) THEN
        RAISE EXCEPTION 'accepted blob head cannot be renewed';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_accepted_blob_head BEFORE UPDATE ON e2ee_blob_heads
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_accepted_blob_head();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_send_receipts WHERE wire_version=3) OR EXISTS(SELECT 1 FROM e2ee_message_blobs) THEN
        RAISE EXCEPTION 'cannot discard accepted attachment receipts';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_accepted_blob_head ON e2ee_blob_heads;
DROP FUNCTION e2ee_guard_accepted_blob_head();
DROP TRIGGER e2ee_guard_blob_acceptance ON e2ee_blob_attempts;
DROP FUNCTION e2ee_guard_blob_acceptance();
DROP TRIGGER e2ee_guard_attachment_receipt_commit ON e2ee_send_receipts;
DROP FUNCTION e2ee_guard_attachment_receipt_commit();
DROP TRIGGER e2ee_guard_blob_link_insert ON e2ee_message_blobs;
DROP FUNCTION e2ee_guard_blob_link_insert();
DROP TRIGGER e2ee_guard_message_blob ON e2ee_message_blobs;
DROP FUNCTION e2ee_guard_message_blob();
DROP TABLE e2ee_message_blobs;
DROP TRIGGER e2ee_guard_receipt_claim ON e2ee_send_receipts;
CREATE TRIGGER e2ee_guard_receipt_claim BEFORE INSERT ON e2ee_send_receipts FOR EACH ROW EXECUTE FUNCTION e2ee_guard_receipt_claim();
DROP FUNCTION e2ee_guard_receipt_claim_v3();
DROP TRIGGER e2ee_guard_epoch_send ON e2ee_send_receipts;
CREATE TRIGGER e2ee_guard_epoch_send BEFORE INSERT ON e2ee_send_receipts FOR EACH ROW EXECUTE FUNCTION e2ee_guard_epoch_insert();
DROP FUNCTION e2ee_guard_epoch_send_v3();
ALTER TABLE e2ee_send_receipts DROP CONSTRAINT e2ee_receipt_wire_epoch;
ALTER TABLE e2ee_send_receipts ADD CONSTRAINT e2ee_receipt_wire_epoch
    CHECK((wire_version=1 AND crypto_epoch IS NULL) OR (wire_version=2 AND crypto_epoch IS NOT NULL AND crypto_epoch>=0));
