-- +goose Up
-- Development cutover: never silently assign a filesystem to old writer/deletion
-- evidence. Reserved metadata has never authorized FS I/O and may be retained.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_blob_attempts WHERE writer_id IS NOT NULL OR state<>'reserved') THEN
        RAISE EXCEPTION 'storage binding requires no unbound writer or deletion evidence';
    END IF;
END $$;
-- +goose StatementEnd
CREATE TABLE e2ee_blob_storage (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    storage_id UUID NOT NULL UNIQUE CHECK(substring(storage_id::text,15,1)='4' AND substring(storage_id::text,20,1) IN ('8','9','a','b'))
);
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_storage_identity() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='DELETE' OR NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'blob storage identity is immutable';END IF;RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_storage_identity BEFORE UPDATE OR DELETE ON e2ee_blob_storage FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_storage_identity();
ALTER TABLE e2ee_blob_attempts ADD COLUMN storage_id UUID REFERENCES e2ee_blob_storage(storage_id),
    ADD COLUMN deletion_reason TEXT CHECK(deletion_reason IN ('expired','writer_failed','scope_inactive','lease_renewed')),
    ADD COLUMN reclaimed_at TIMESTAMPTZ,
    ADD CHECK((writer_id IS NULL)=(storage_id IS NULL)),
    ADD CHECK((state IN ('deleting','reclaimed'))=(deletion_reason IS NOT NULL)),
    ADD CHECK((state='reclaimed')=(reclaimed_at IS NOT NULL));
-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_blob_reclamation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='INSERT' AND (NEW.storage_id IS NOT NULL OR NEW.deletion_reason IS NOT NULL OR NEW.reclaimed_at IS NOT NULL) THEN RAISE EXCEPTION 'new blob attempt must be unbound'; END IF;
    IF TG_OP='UPDATE' THEN
        IF NEW.storage_id IS DISTINCT FROM OLD.storage_id AND NOT (OLD.state='reserved' AND NEW.state='writing' AND OLD.storage_id IS NULL AND NEW.storage_id IS NOT NULL) THEN RAISE EXCEPTION 'blob writer storage is immutable'; END IF;
        IF NEW.deletion_reason IS DISTINCT FROM OLD.deletion_reason AND NOT (OLD.state IN ('reserved','writing','ready') AND NEW.state='deleting' AND OLD.deletion_reason IS NULL AND NEW.deletion_reason IS NOT NULL) THEN RAISE EXCEPTION 'blob deletion reason is immutable'; END IF;
        IF NEW.reclaimed_at IS DISTINCT FROM OLD.reclaimed_at AND NOT (OLD.state='deleting' AND NEW.state='reclaimed' AND OLD.reclaimed_at IS NULL AND NEW.reclaimed_at IS NOT NULL) THEN RAISE EXCEPTION 'blob reclaim fact is immutable'; END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_blob_reclamation BEFORE INSERT OR UPDATE ON e2ee_blob_attempts FOR EACH ROW EXECUTE FUNCTION e2ee_guard_blob_reclamation();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_blob_storage) OR EXISTS(SELECT 1 FROM e2ee_blob_attempts WHERE state<>'reserved') THEN
        RAISE EXCEPTION 'cannot discard bound storage or reclamation evidence';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_blob_reclamation ON e2ee_blob_attempts;
DROP FUNCTION e2ee_guard_blob_reclamation();
ALTER TABLE e2ee_blob_attempts DROP COLUMN reclaimed_at,DROP COLUMN deletion_reason,DROP COLUMN storage_id;
DROP TRIGGER e2ee_guard_blob_storage_identity ON e2ee_blob_storage;
DROP FUNCTION e2ee_guard_blob_storage_identity();
DROP TABLE e2ee_blob_storage;
