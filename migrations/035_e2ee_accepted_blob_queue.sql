-- +goose Up
-- A disposable projection of ACTIVE server copies, not deletion authority.
-- Immutable receipts/links/FILEACK survive queue removal. No history scan at runtime.
CREATE TABLE e2ee_accepted_blob_queue (
    attempt_id UUID PRIMARY KEY,
    message_id UUID NOT NULL,
    revision BIGINT NOT NULL,
    storage_id UUID NOT NULL,
    due_at TIMESTAMPTZ NOT NULL CHECK(isfinite(due_at) AND due_at>TIMESTAMPTZ 'epoch'),
    FOREIGN KEY(message_id,revision,attempt_id) REFERENCES e2ee_blob_attempts(message_id,revision,attempt_id)
);
CREATE INDEX e2ee_accepted_blob_due ON e2ee_accepted_blob_queue(storage_id,due_at,attempt_id)
    INCLUDE(message_id,revision);
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_accepted_blob_queue() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected TIMESTAMPTZ; bound UUID; current_state TEXT;
BEGIN
    IF TG_OP='DELETE' THEN
        SELECT state INTO current_state FROM e2ee_blob_attempts WHERE attempt_id=OLD.attempt_id;
        IF current_state IS DISTINCT FROM 'reclaimed' THEN
            RAISE EXCEPTION 'pending accepted cleanup cannot be discarded';
        END IF;
        RETURN OLD;
    END IF;
    IF TG_OP='UPDATE' AND ROW(NEW.attempt_id,NEW.message_id,NEW.revision,NEW.storage_id)
       IS DISTINCT FROM ROW(OLD.attempt_id,OLD.message_id,OLD.revision,OLD.storage_id) THEN
        RAISE EXCEPTION 'accepted cleanup binding is immutable';
    END IF;
    SELECT LEAST(r.expires_at,COALESCE(f.acknowledged_at,r.expires_at)),a.storage_id
        INTO expected,bound FROM e2ee_blob_attempts a
        JOIN e2ee_message_blobs b ON (b.message_id,b.revision,b.attempt_id)=(a.message_id,a.revision,a.attempt_id)
        JOIN e2ee_send_receipts r ON r.message_id=b.message_id
        LEFT JOIN e2ee_file_acknowledgements f ON f.message_id=b.message_id
        WHERE (a.message_id,a.revision,a.attempt_id)=(NEW.message_id,NEW.revision,NEW.attempt_id)
          AND (a.state='linked' OR (a.state='deleting' AND a.deletion_reason IN ('file_acknowledged','accepted_expired')));
    IF expected IS NULL OR NEW.due_at IS DISTINCT FROM expected OR NEW.storage_id IS DISTINCT FROM bound THEN
        RAISE EXCEPTION 'accepted cleanup queue requires canonical active binding and deadline';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_accepted_blob_queue BEFORE INSERT OR UPDATE OR DELETE ON e2ee_accepted_blob_queue
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_accepted_blob_queue();

-- Queue changes commit with the canonical state, never a best-effort second write.
-- +goose StatementBegin
CREATE FUNCTION e2ee_project_accepted_blob_state() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.state='linked' THEN
        INSERT INTO e2ee_accepted_blob_queue(attempt_id,message_id,revision,storage_id,due_at)
            SELECT NEW.attempt_id,NEW.message_id,NEW.revision,NEW.storage_id,
                LEAST(r.expires_at,COALESCE(f.acknowledged_at,r.expires_at))
            FROM e2ee_send_receipts r LEFT JOIN e2ee_file_acknowledgements f USING(message_id)
            WHERE r.message_id=NEW.message_id;
    ELSIF NEW.state='reclaimed' THEN
        DELETE FROM e2ee_accepted_blob_queue WHERE attempt_id=NEW.attempt_id;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_project_accepted_blob_state AFTER UPDATE OF state ON e2ee_blob_attempts
    FOR EACH ROW WHEN(OLD.state IS DISTINCT FROM NEW.state) EXECUTE FUNCTION e2ee_project_accepted_blob_state();
-- +goose StatementBegin
CREATE FUNCTION e2ee_project_file_ack_cleanup() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    UPDATE e2ee_accepted_blob_queue SET due_at=LEAST(due_at,NEW.acknowledged_at)
        WHERE message_id=NEW.message_id AND due_at>NEW.acknowledged_at;
    RETURN NEW;
END $$;
-- +goose StatementEnd
-- message_id is not the due cursor, but ACK must not scan the active queue either.
CREATE UNIQUE INDEX e2ee_accepted_blob_message ON e2ee_accepted_blob_queue(message_id);
CREATE TRIGGER e2ee_project_file_ack_cleanup AFTER INSERT ON e2ee_file_acknowledgements
    FOR EACH ROW EXECUTE FUNCTION e2ee_project_file_ack_cleanup();

-- One migration-time backfill only, including interrupted admitted deletions.
INSERT INTO e2ee_accepted_blob_queue(attempt_id,message_id,revision,storage_id,due_at)
    SELECT a.attempt_id,a.message_id,a.revision,a.storage_id,
        LEAST(r.expires_at,COALESCE(f.acknowledged_at,r.expires_at))
    FROM e2ee_blob_attempts a JOIN e2ee_message_blobs b
        ON (b.message_id,b.revision,b.attempt_id)=(a.message_id,a.revision,a.attempt_id)
    JOIN e2ee_send_receipts r ON r.message_id=b.message_id
    LEFT JOIN e2ee_file_acknowledgements f ON f.message_id=b.message_id
    WHERE a.state IN ('linked','deleting');

DROP INDEX e2ee_blob_attempt_unfinished;
CREATE INDEX e2ee_blob_attempt_unfinished ON e2ee_blob_attempts(attempt_id)
    WHERE state IN ('writing','deleting') AND
        (deletion_reason IS NULL OR deletion_reason NOT IN ('file_acknowledged','accepted_expired'));

-- +goose Down
DROP TRIGGER e2ee_project_file_ack_cleanup ON e2ee_file_acknowledgements;
DROP FUNCTION e2ee_project_file_ack_cleanup();
DROP TRIGGER e2ee_project_accepted_blob_state ON e2ee_blob_attempts;
DROP FUNCTION e2ee_project_accepted_blob_state();
DROP TABLE e2ee_accepted_blob_queue;
DROP FUNCTION e2ee_guard_accepted_blob_queue();
DROP INDEX e2ee_blob_attempt_unfinished;
CREATE INDEX e2ee_blob_attempt_unfinished ON e2ee_blob_attempts(attempt_id)
    WHERE state IN ('writing','deleting');
