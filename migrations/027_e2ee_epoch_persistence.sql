-- +goose Up
-- No conversion of old ciphertext/snapshots. Nullable epoch labels only the
-- temporary prototype writer still exercised by current callers/tests; v2
-- readers never fall back to it. Retire v1 writers at the coordinated cutover.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_send_receipts) OR EXISTS(SELECT 1 FROM e2ee_acquisitions) THEN
        RAISE EXCEPTION 'epoch persistence requires empty prototype message/acquisition state';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE e2ee_send_receipts ADD COLUMN wire_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN crypto_epoch BIGINT,
    ADD CHECK((wire_version=1 AND crypto_epoch IS NULL) OR (wire_version=2 AND crypto_epoch IS NOT NULL AND crypto_epoch>=0)),
    ADD FOREIGN KEY(conversation_id,crypto_epoch) REFERENCES e2ee_conversation_epochs(conversation_id,epoch);
ALTER TABLE e2ee_acquisitions ADD COLUMN wire_version SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN crypto_epoch BIGINT,
    ADD CHECK((wire_version=1 AND crypto_epoch IS NULL) OR (wire_version=2 AND crypto_epoch IS NOT NULL AND crypto_epoch>=0)),
    ADD FOREIGN KEY(conversation_id,crypto_epoch) REFERENCES e2ee_conversation_epochs(conversation_id,epoch);
CREATE INDEX e2ee_acquisition_epoch_live ON e2ee_acquisitions(owner_generation_id,conversation_id,crypto_epoch,not_after) WHERE wire_version=2;

-- Defense against bypassing the repository's active-epoch/creator gate. INSERT
-- takes the SAME conversation lock as recovery; accepted old rows remain valid
-- forever and are never rechecked/rewritten on exact lookup, ACK or expiry.
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_epoch_insert() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE head BIGINT; chat_state TEXT; epoch_state TEXT; bootstrap BIGINT;
BEGIN
    SELECT active_epoch,state INTO head,chat_state FROM e2ee_conversations
        WHERE conversation_id=NEW.conversation_id FOR SHARE;
    IF chat_state IS DISTINCT FROM 'active' THEN RAISE EXCEPTION 'new persistence requires active conversation'; END IF;
    IF NEW.wire_version=1 THEN
        IF head<>0 THEN RAISE EXCEPTION 'prototype writer cannot cross recovery epoch'; END IF;
        RETURN NEW;
    END IF;
    IF NEW.wire_version<>2 OR NEW.crypto_epoch IS NULL OR NEW.crypto_epoch<>head THEN RAISE EXCEPTION 'new persistence requires active epoch'; END IF;
    SELECT state,bootstrap_user_id INTO epoch_state,bootstrap FROM e2ee_conversation_epochs
        WHERE conversation_id=NEW.conversation_id AND epoch=NEW.crypto_epoch FOR SHARE;
    IF epoch_state IS DISTINCT FROM 'active' THEN RAISE EXCEPTION 'new persistence requires unsealed epoch'; END IF;
    IF TG_TABLE_NAME='e2ee_acquisitions' AND NEW.crypto_epoch>0 THEN
        IF NEW.owner_user_id IS DISTINCT FROM bootstrap THEN RAISE EXCEPTION 'only recovery creator may bootstrap'; END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_epoch_send BEFORE INSERT ON e2ee_send_receipts FOR EACH ROW EXECUTE FUNCTION e2ee_guard_epoch_insert();
CREATE TRIGGER e2ee_guard_epoch_acquire BEFORE INSERT ON e2ee_acquisitions FOR EACH ROW EXECUTE FUNCTION e2ee_guard_epoch_insert();

-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_epoch_acquisition() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.wire_version IS DISTINCT FROM OLD.wire_version OR NEW.crypto_epoch IS DISTINCT FROM OLD.crypto_epoch
        OR (OLD.wire_version=2 AND NEW IS DISTINCT FROM OLD) THEN RAISE EXCEPTION 'epoch acquisition is immutable'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_epoch_acquisition BEFORE UPDATE ON e2ee_acquisitions FOR EACH ROW EXECUTE FUNCTION e2ee_guard_epoch_acquisition();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_send_receipts WHERE wire_version=2) OR EXISTS(SELECT 1 FROM e2ee_acquisitions WHERE wire_version=2) THEN
        RAISE EXCEPTION 'cannot discard retained v2 epoch bindings';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_epoch_acquisition ON e2ee_acquisitions;
DROP FUNCTION e2ee_guard_epoch_acquisition();
DROP TRIGGER e2ee_guard_epoch_acquire ON e2ee_acquisitions;
DROP TRIGGER e2ee_guard_epoch_send ON e2ee_send_receipts;
DROP FUNCTION e2ee_guard_epoch_insert();
DROP INDEX e2ee_acquisition_epoch_live;
ALTER TABLE e2ee_acquisitions DROP COLUMN crypto_epoch,DROP COLUMN wire_version;
ALTER TABLE e2ee_send_receipts DROP COLUMN crypto_epoch,DROP COLUMN wire_version;
