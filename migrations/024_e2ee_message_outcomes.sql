-- +goose Up
-- Separate immutable facts: removing a server copy does not prove the recipient
-- never saved it. A later authenticated ACK can refine sender status without
-- erasing an earlier expiry/removal observation or restoring the ciphertext.
CREATE TABLE e2ee_message_removals (
    message_id UUID PRIMARY KEY REFERENCES e2ee_send_receipts(message_id),
    reason TEXT NOT NULL CHECK (reason IN ('acknowledged','expired','recipient_replaced')),
    removed_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE e2ee_message_acknowledgements (
    message_id UUID PRIMARY KEY REFERENCES e2ee_send_receipts(message_id),
    disposition TEXT NOT NULL CHECK (disposition IN ('stored','rejected')),
    acknowledged_at TIMESTAMPTZ NOT NULL
);

-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_message_fact() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'E2EE message observation is immutable';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_message_removal BEFORE UPDATE ON e2ee_message_removals
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_message_fact();
CREATE TRIGGER e2ee_guard_message_ack BEFORE UPDATE ON e2ee_message_acknowledgements
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_message_fact();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM e2ee_message_removals) OR EXISTS (SELECT 1 FROM e2ee_message_acknowledgements) THEN
        RAISE EXCEPTION 'cannot roll back nonempty E2EE delivery observations';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_guard_message_ack ON e2ee_message_acknowledgements;
DROP TRIGGER e2ee_guard_message_removal ON e2ee_message_removals;
DROP FUNCTION e2ee_guard_message_fact();
DROP TABLE e2ee_message_acknowledgements;
DROP TABLE e2ee_message_removals;
