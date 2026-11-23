-- +goose Up
-- Discover retired recipient generations without scanning the immutable receipt
-- ledger. Inbox lookup already has (recipient_generation_id,sequence) as its PK;
-- expiry already has its own ordered index. These indexes contain no payloads.
CREATE INDEX e2ee_retired_delivery_devices ON e2ee_devices(retired_at,generation_id)
    WHERE retired_at IS NOT NULL;
CREATE INDEX e2ee_pending_delivery_generations ON e2ee_delivery_state(generation_id)
    WHERE pending_messages>0;

-- +goose Down
DROP INDEX e2ee_pending_delivery_generations;
DROP INDEX e2ee_retired_delivery_devices;
