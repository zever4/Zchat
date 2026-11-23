-- +goose Up
-- Separate from descriptor/message ACK and immutable accepted-attempt selection.
-- The exact route/ref/digest is retained by claim +receipt +message_blob; no
-- duplicate public binding, private descriptor, key or ciphertext is stored here.
CREATE TABLE e2ee_file_acknowledgements (
    message_id UUID PRIMARY KEY REFERENCES e2ee_message_blobs(message_id),
    acknowledged_at TIMESTAMPTZ NOT NULL DEFAULT date_trunc('milliseconds',clock_timestamp()),
    CHECK(isfinite(acknowledged_at) AND acknowledged_at>TIMESTAMPTZ 'epoch'
        AND acknowledged_at=date_trunc('milliseconds',acknowledged_at))
);
-- An authenticated recipient statement is retained even after accepted-file
-- cleanup. It does not change message disposition or prove physical client disk.
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_file_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'file acknowledgement is immutable';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_file_ack BEFORE UPDATE OR DELETE ON e2ee_file_acknowledgements
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_file_ack();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_file_acknowledgements) THEN
        RAISE EXCEPTION 'cannot discard file acknowledgement evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_file_ack ON e2ee_file_acknowledgements;
DROP FUNCTION e2ee_guard_file_ack();
DROP TABLE e2ee_file_acknowledgements;
