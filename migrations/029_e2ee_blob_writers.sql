-- +goose Up
-- One immutable writer per attempt. A retry may inspect the same writer but
-- cannot resume/reopen its file; a successor requires a new reserved attempt.
ALTER TABLE e2ee_blob_attempts ADD COLUMN writer_id UUID,
    ADD CONSTRAINT e2ee_blob_writer_uuid CHECK(writer_id IS NULL OR
        (substring(writer_id::text,15,1)='4' AND substring(writer_id::text,20,1) IN ('8','9','a','b'))),
    ADD CONSTRAINT e2ee_blob_writer_state CHECK(
        (state='reserved' AND writer_id IS NULL) OR
        (state IN ('writing','ready','linked') AND writer_id IS NOT NULL) OR
        state IN ('deleting','reclaimed'));
CREATE UNIQUE INDEX e2ee_blob_writer_unique ON e2ee_blob_attempts(writer_id) WHERE writer_id IS NOT NULL;
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_writer() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='INSERT' AND NEW.writer_id IS NOT NULL THEN RAISE EXCEPTION 'new attempt cannot already have a writer'; END IF;
    IF TG_OP='UPDATE' AND NEW.writer_id IS DISTINCT FROM OLD.writer_id AND NOT (
        OLD.writer_id IS NULL AND OLD.state='reserved' AND NEW.state='writing' AND NEW.writer_id IS NOT NULL)
        THEN RAISE EXCEPTION 'blob writer binding is immutable'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_writer BEFORE INSERT OR UPDATE ON e2ee_blob_attempts FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_writer();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_blob_attempts WHERE writer_id IS NOT NULL) THEN
        RAISE EXCEPTION 'cannot discard retained blob writer evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_blob_writer ON e2ee_blob_attempts;
DROP FUNCTION e2ee_guard_blob_writer();
DROP INDEX e2ee_blob_writer_unique;
ALTER TABLE e2ee_blob_attempts DROP CONSTRAINT e2ee_blob_writer_state,DROP CONSTRAINT e2ee_blob_writer_uuid,DROP COLUMN writer_id;
