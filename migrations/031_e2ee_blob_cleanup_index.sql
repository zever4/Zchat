-- +goose Up
-- A bounded UUID cursor over abandoned writers and durable pending deletions.
-- Expired reservations/Ready use the existing (expires_at,attempt_id) index.
CREATE INDEX e2ee_blob_attempt_unfinished ON e2ee_blob_attempts(attempt_id)
    WHERE state IN ('writing','deleting');

-- +goose Down
DROP INDEX e2ee_blob_attempt_unfinished;
