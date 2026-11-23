-- +goose Up
-- Staging and accepted retention are distinct. Accepted admission never removes
-- the immutable receipt/link/FILEACK or releases bytes before a physical proof.
ALTER TABLE e2ee_blob_attempts DROP CONSTRAINT e2ee_blob_attempts_deletion_reason_check;
ALTER TABLE e2ee_blob_attempts ADD CONSTRAINT e2ee_blob_attempts_deletion_reason_check
    CHECK(deletion_reason IN ('expired','writer_failed','scope_inactive','lease_renewed',
        'file_acknowledged','accepted_expired'));
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION e2ee_guard_blob_reclamation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='INSERT' AND (NEW.storage_id IS NOT NULL OR NEW.deletion_reason IS NOT NULL OR NEW.reclaimed_at IS NOT NULL) THEN RAISE EXCEPTION 'new blob attempt must be unbound'; END IF;
    IF TG_OP='UPDATE' THEN
        IF NEW.storage_id IS DISTINCT FROM OLD.storage_id AND NOT (OLD.state='reserved' AND NEW.state='writing' AND OLD.storage_id IS NULL AND NEW.storage_id IS NOT NULL) THEN RAISE EXCEPTION 'blob writer storage is immutable'; END IF;
        IF NEW.deletion_reason IS DISTINCT FROM OLD.deletion_reason AND NOT (OLD.state IN ('reserved','writing','ready','linked') AND NEW.state='deleting' AND OLD.deletion_reason IS NULL AND NEW.deletion_reason IS NOT NULL) THEN RAISE EXCEPTION 'blob deletion reason is immutable'; END IF;
        IF NEW.reclaimed_at IS DISTINCT FROM OLD.reclaimed_at AND NOT (OLD.state='deleting' AND NEW.state='reclaimed' AND OLD.reclaimed_at IS NULL AND NEW.reclaimed_at IS NOT NULL) THEN RAISE EXCEPTION 'blob reclaim fact is immutable'; END IF;
    END IF;
    IF TG_OP='UPDATE' AND OLD.deletion_reason IS NULL AND NEW.deletion_reason IS NOT NULL THEN
        IF OLD.state='linked' THEN
            IF NEW.state<>'deleting' OR NEW.deletion_reason NOT IN ('file_acknowledged','accepted_expired') OR NOT EXISTS(
                SELECT 1 FROM e2ee_message_blobs b JOIN e2ee_send_receipts s USING(message_id)
                WHERE (b.message_id,b.revision,b.attempt_id)=(NEW.message_id,NEW.revision,NEW.attempt_id)
                  AND s.wire_version=3 AND (
                    (NEW.deletion_reason='file_acknowledged' AND EXISTS(
                        SELECT 1 FROM e2ee_file_acknowledgements f WHERE f.message_id=b.message_id)) OR
                    (NEW.deletion_reason='accepted_expired' AND s.expires_at<=clock_timestamp()))) THEN
                RAISE EXCEPTION 'accepted deletion requires exact receipt link and file ACK or accepted deadline';
            END IF;
        ELSIF NEW.deletion_reason IN ('file_acknowledged','accepted_expired') THEN
            RAISE EXCEPTION 'accepted deletion reason requires Linked attempt';
        END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd

-- +goose Down
-- Existing schema cannot represent accepted deletion evidence safely.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_blob_attempts WHERE deletion_reason IN ('file_acknowledged','accepted_expired')) THEN
        RAISE EXCEPTION 'cannot discard accepted blob deletion evidence';
    END IF;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION e2ee_guard_blob_reclamation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='INSERT' AND (NEW.storage_id IS NOT NULL OR NEW.deletion_reason IS NOT NULL OR NEW.reclaimed_at IS NOT NULL) THEN RAISE EXCEPTION 'new blob attempt must be unbound'; END IF;
    IF TG_OP='UPDATE' THEN
        IF NEW.storage_id IS DISTINCT FROM OLD.storage_id AND NOT (OLD.state='reserved' AND NEW.state='writing' AND OLD.storage_id IS NULL AND NEW.storage_id IS NOT NULL) THEN RAISE EXCEPTION 'blob writer storage is immutable'; END IF;
        IF NEW.deletion_reason IS DISTINCT FROM OLD.deletion_reason AND NOT (OLD.state IN ('reserved','writing','ready') AND NEW.state='deleting' AND OLD.deletion_reason IS NULL AND NEW.deletion_reason IS NOT NULL) THEN RAISE EXCEPTION 'blob deletion reason is immutable'; END IF;
        IF NEW.reclaimed_at IS DISTINCT FROM OLD.reclaimed_at AND NOT (OLD.state='deleting' AND NEW.state='reclaimed' AND OLD.reclaimed_at IS NULL AND NEW.reclaimed_at IS NOT NULL) THEN RAISE EXCEPTION 'blob reclaim fact is immutable'; END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
ALTER TABLE e2ee_blob_attempts DROP CONSTRAINT e2ee_blob_attempts_deletion_reason_check;
ALTER TABLE e2ee_blob_attempts ADD CONSTRAINT e2ee_blob_attempts_deletion_reason_check
    CHECK(deletion_reason IN ('expired','writer_failed','scope_inactive','lease_renewed'));
