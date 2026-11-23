-- +goose Up
-- Shared UUID first-use fence. Text claims commit ONLY with Send. Blob claims
-- precede Signal encryption and therefore have NO envelope digest yet.
CREATE TABLE e2ee_message_claims (
    message_id UUID PRIMARY KEY CHECK (substring(message_id::text,15,1)='4' AND substring(message_id::text,20,1) IN ('8','9','a','b')),
    conversation_id UUID NOT NULL,
    sender_user_id BIGINT NOT NULL,
    sender_generation_id UUID NOT NULL,
    recipient_user_id BIGINT NOT NULL,
    recipient_generation_id UUID NOT NULL,
    low_user_id BIGINT NOT NULL,
    high_user_id BIGINT NOT NULL,
    low_generation_id UUID NOT NULL,
    high_generation_id UUID NOT NULL,
    wire_version SMALLINT NOT NULL,
    crypto_epoch BIGINT,
    envelope_digest BYTEA,
    blob_id UUID UNIQUE,
    blob_bytes BIGINT,
    blob_sha256 BYTEA,
    FOREIGN KEY(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id)
        REFERENCES e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id),
    FOREIGN KEY(conversation_id,crypto_epoch) REFERENCES e2ee_conversation_epochs(conversation_id,epoch),
    CHECK ((sender_user_id=low_user_id AND recipient_user_id=high_user_id AND sender_generation_id=low_generation_id AND recipient_generation_id=high_generation_id) OR
           (sender_user_id=high_user_id AND recipient_user_id=low_user_id AND sender_generation_id=high_generation_id AND recipient_generation_id=low_generation_id)),
    CHECK ((wire_version=1 AND crypto_epoch IS NULL) OR (wire_version IN (2,3) AND crypto_epoch IS NOT NULL AND crypto_epoch>=0)),
    CHECK ((wire_version IN (1,2) AND envelope_digest IS NOT NULL AND octet_length(envelope_digest)=32 AND blob_id IS NULL AND blob_bytes IS NULL AND blob_sha256 IS NULL) OR
           (wire_version=3 AND envelope_digest IS NULL AND blob_id IS NOT NULL AND blob_bytes IS NOT NULL AND blob_bytes BETWEEN 37 AND 52428836 AND blob_sha256 IS NOT NULL AND octet_length(blob_sha256)=32 AND
            substring(blob_id::text,15,1)='4' AND substring(blob_id::text,20,1) IN ('8','9','a','b')))
);
INSERT INTO e2ee_message_claims(message_id,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
    low_user_id,high_user_id,low_generation_id,high_generation_id,wire_version,crypto_epoch,envelope_digest)
    SELECT message_id,conversation_id,sender_user_id,sender_generation_id,recipient_user_id,recipient_generation_id,
    low_user_id,high_user_id,low_generation_id,high_generation_id,wire_version,crypto_epoch,envelope_digest FROM e2ee_send_receipts;

-- Backfill above preserves accepted old receipts. Only NEW blob intents need
-- an active epoch and current public-ready devices; old claims stay immutable.
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_claim_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE head BIGINT; cs TEXT; es TEXT;
BEGIN
    IF NEW.wire_version<>3 THEN RETURN NEW; END IF;
    PERFORM user_id FROM e2ee_devices WHERE generation_id IN (NEW.sender_generation_id,NEW.recipient_generation_id) ORDER BY user_id FOR SHARE;
    IF (SELECT count(*) FROM e2ee_devices WHERE generation_id IN (NEW.sender_generation_id,NEW.recipient_generation_id) AND retired_at IS NULL AND public_material_ready)<>2 THEN
        RAISE EXCEPTION 'blob claim requires current ready devices';
    END IF;
    SELECT c.active_epoch,c.state,x.state INTO head,cs,es FROM e2ee_conversations c JOIN e2ee_conversation_epochs x
        ON x.conversation_id=c.conversation_id AND x.epoch=NEW.crypto_epoch WHERE c.conversation_id=NEW.conversation_id FOR SHARE OF c,x;
    IF head IS DISTINCT FROM NEW.crypto_epoch OR cs IS DISTINCT FROM 'active' OR es IS DISTINCT FROM 'active' THEN RAISE EXCEPTION 'blob claim requires active epoch'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_claim_insert BEFORE INSERT ON e2ee_message_claims FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_claim_insert();

-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_message_claim() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'E2EE message claim is immutable'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_message_claim BEFORE UPDATE OR DELETE ON e2ee_message_claims FOR EACH ROW EXECUTE FUNCTION e2ee_guard_message_claim();
ALTER TABLE e2ee_send_receipts ADD FOREIGN KEY(message_id) REFERENCES e2ee_message_claims(message_id);
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_receipt_claim() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE c e2ee_message_claims%ROWTYPE;
BEGIN
    SELECT * INTO c FROM e2ee_message_claims WHERE message_id=NEW.message_id FOR UPDATE;
    IF NOT FOUND OR c.wire_version NOT IN (1,2) OR
       ROW(c.conversation_id,c.sender_user_id,c.sender_generation_id,c.recipient_user_id,c.recipient_generation_id,c.wire_version,c.crypto_epoch,c.envelope_digest)
       IS DISTINCT FROM ROW(NEW.conversation_id,NEW.sender_user_id,NEW.sender_generation_id,NEW.recipient_user_id,NEW.recipient_generation_id,NEW.wire_version,NEW.crypto_epoch,NEW.envelope_digest)
       THEN RAISE EXCEPTION 'E2EE receipt requires its exact text claim'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_receipt_claim BEFORE INSERT ON e2ee_send_receipts FOR EACH ROW EXECUTE FUNCTION e2ee_guard_receipt_claim();
-- A text-only first-use claim may never COMMIT by itself. The deferred guard
-- checks the receipt inserted later in the same Send transaction.
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_claim_commit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.wire_version IN (1,2) AND NOT EXISTS(SELECT 1 FROM e2ee_send_receipts WHERE message_id=NEW.message_id) THEN
        RAISE EXCEPTION 'text claim requires committed receipt';
    END IF;
    IF NEW.wire_version=3 AND NOT EXISTS(SELECT 1 FROM e2ee_blob_heads WHERE message_id=NEW.message_id) THEN
        RAISE EXCEPTION 'blob claim requires committed initial attempt';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER e2ee_guard_claim_commit AFTER INSERT ON e2ee_message_claims DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION e2ee_guard_claim_commit();

-- Explicit persisted configuration; absent limits fail closed. Bytes remain
-- charged after expiry until future FS cleanup has closed/unlinked/synced.
CREATE TABLE e2ee_blob_global_quota (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    max_bytes BIGINT NOT NULL CHECK(max_bytes>0),
    account_max_bytes BIGINT NOT NULL CHECK(account_max_bytes>0 AND account_max_bytes<=max_bytes),
    max_claims BIGINT NOT NULL CHECK(max_claims>0),
    account_max_claims BIGINT NOT NULL CHECK(account_max_claims>0 AND account_max_claims<=max_claims),
    max_attempts BIGINT NOT NULL CHECK(max_attempts>0),
    account_max_attempts BIGINT NOT NULL CHECK(account_max_attempts>0 AND account_max_attempts<=max_attempts),
    reserved_bytes BIGINT NOT NULL DEFAULT 0 CHECK(reserved_bytes BETWEEN 0 AND max_bytes),
    retained_claims BIGINT NOT NULL DEFAULT 0 CHECK(retained_claims BETWEEN 0 AND max_claims),
    retained_attempts BIGINT NOT NULL DEFAULT 0 CHECK(retained_attempts BETWEEN 0 AND max_attempts)
);
CREATE TABLE e2ee_blob_account_quota (
    user_id BIGINT PRIMARY KEY REFERENCES users(id),
    reserved_bytes BIGINT NOT NULL DEFAULT 0 CHECK(reserved_bytes>=0),
    retained_claims BIGINT NOT NULL DEFAULT 0 CHECK(retained_claims>=0),
    retained_attempts BIGINT NOT NULL DEFAULT 0 CHECK(retained_attempts>=0)
);
CREATE TABLE e2ee_blob_attempts (
    attempt_id UUID PRIMARY KEY CHECK(substring(attempt_id::text,15,1)='4' AND substring(attempt_id::text,20,1) IN ('8','9','a','b')),
    message_id UUID NOT NULL REFERENCES e2ee_message_claims(message_id),
    revision BIGINT NOT NULL CHECK(revision>0),
    operation_id UUID NOT NULL UNIQUE CHECK(substring(operation_id::text,15,1)='4' AND substring(operation_id::text,20,1) IN ('8','9','a','b')),
    expected_revision BIGINT NOT NULL CHECK(expected_revision=revision-1),
    expected_attempt_id UUID,
    reserved_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL CHECK(expires_at=reserved_at+INTERVAL '1 hour'),
    charged_bytes BIGINT NOT NULL CHECK(charged_bytes BETWEEN 37 AND 52428836),
    state TEXT NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','writing','ready','linked','deleting','reclaimed')),
    CHECK((revision=1 AND expected_attempt_id IS NULL) OR (revision>1 AND expected_attempt_id IS NOT NULL)),
    UNIQUE(message_id,revision,attempt_id),
    FOREIGN KEY(message_id,expected_revision,expected_attempt_id) REFERENCES e2ee_blob_attempts(message_id,revision,attempt_id)
);
CREATE INDEX e2ee_blob_attempt_expiry ON e2ee_blob_attempts(expires_at,attempt_id) WHERE state IN ('reserved','writing','ready');
CREATE TABLE e2ee_blob_heads (
    message_id UUID PRIMARY KEY REFERENCES e2ee_message_claims(message_id),
    revision BIGINT NOT NULL,
    attempt_id UUID NOT NULL,
    FOREIGN KEY(message_id,revision,attempt_id) REFERENCES e2ee_blob_attempts(message_id,revision,attempt_id)
);
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v SMALLINT; n BIGINT;
BEGIN
    SELECT wire_version,blob_bytes INTO v,n FROM e2ee_message_claims WHERE message_id=NEW.message_id;
    IF v IS DISTINCT FROM 3 OR n IS DISTINCT FROM NEW.charged_bytes THEN RAISE EXCEPTION 'blob attempt requires exact attachment claim'; END IF;
    IF TG_OP='INSERT' AND NEW.state<>'reserved' THEN RAISE EXCEPTION 'blob attempt must start reserved'; END IF;
    IF TG_OP='UPDATE' AND ROW(NEW.attempt_id,NEW.message_id,NEW.revision,NEW.operation_id,NEW.expected_revision,NEW.expected_attempt_id,NEW.reserved_at,NEW.expires_at,NEW.charged_bytes)
        IS DISTINCT FROM ROW(OLD.attempt_id,OLD.message_id,OLD.revision,OLD.operation_id,OLD.expected_revision,OLD.expected_attempt_id,OLD.reserved_at,OLD.expires_at,OLD.charged_bytes)
        THEN RAISE EXCEPTION 'blob attempt binding is immutable'; END IF;
    IF TG_OP='UPDATE' AND NEW.state<>OLD.state AND NOT (
        (OLD.state='reserved' AND NEW.state IN ('writing','deleting')) OR
        (OLD.state='writing' AND NEW.state IN ('ready','deleting')) OR
        (OLD.state='ready' AND NEW.state IN ('linked','deleting')) OR
        (OLD.state='linked' AND NEW.state='deleting') OR
        (OLD.state='deleting' AND NEW.state='reclaimed')) THEN RAISE EXCEPTION 'invalid blob attempt transition'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_attempt BEFORE INSERT OR UPDATE ON e2ee_blob_attempts FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_attempt();
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_head() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous UUID; expected BIGINT; s TEXT; old_state TEXT;
BEGIN
    SELECT expected_revision,expected_attempt_id,state INTO expected,previous,s FROM e2ee_blob_attempts
        WHERE (message_id,revision,attempt_id)=(NEW.message_id,NEW.revision,NEW.attempt_id);
    IF s IS DISTINCT FROM 'reserved' THEN RAISE EXCEPTION 'blob head requires reserved attempt'; END IF;
    IF TG_OP='INSERT' AND (NEW.revision<>1 OR expected<>0 OR previous IS NOT NULL) THEN RAISE EXCEPTION 'invalid initial blob head'; END IF;
    IF TG_OP='UPDATE' THEN
        SELECT state INTO old_state FROM e2ee_blob_attempts WHERE attempt_id=OLD.attempt_id;
        IF NEW.message_id<>OLD.message_id OR NEW.revision<>OLD.revision+1 OR expected<>OLD.revision OR previous IS DISTINCT FROM OLD.attempt_id
            OR old_state NOT IN ('deleting','reclaimed') THEN RAISE EXCEPTION 'invalid blob head CAS'; END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_head BEFORE INSERT OR UPDATE ON e2ee_blob_heads FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_head();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_blob_attempts) OR EXISTS(SELECT 1 FROM e2ee_message_claims WHERE wire_version=3) THEN
        RAISE EXCEPTION 'cannot discard retained blob reservation evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_blob_attempt ON e2ee_blob_attempts;
DROP FUNCTION e2ee_guard_blob_attempt();
DROP TRIGGER e2ee_guard_blob_head ON e2ee_blob_heads;
DROP FUNCTION e2ee_guard_blob_head();
DROP TABLE e2ee_blob_heads,e2ee_blob_attempts,e2ee_blob_account_quota,e2ee_blob_global_quota;
DROP TRIGGER e2ee_guard_receipt_claim ON e2ee_send_receipts;
DROP FUNCTION e2ee_guard_receipt_claim();
ALTER TABLE e2ee_send_receipts DROP CONSTRAINT e2ee_send_receipts_message_id_fkey;
DROP TRIGGER e2ee_guard_message_claim ON e2ee_message_claims;
DROP FUNCTION e2ee_guard_message_claim();
DROP TRIGGER e2ee_guard_blob_claim_insert ON e2ee_message_claims;
DROP FUNCTION e2ee_guard_blob_claim_insert();
DROP TRIGGER e2ee_guard_claim_commit ON e2ee_message_claims;
DROP FUNCTION e2ee_guard_claim_commit();
DROP TABLE e2ee_message_claims;
