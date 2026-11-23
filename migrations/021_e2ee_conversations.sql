-- +goose Up
-- Separate UUID namespace: no ordinary messages/chat_members/attachments point
-- here. Exactly two immutable account/generation bindings, one active per pair.
CREATE TABLE e2ee_conversations (
    conversation_id UUID PRIMARY KEY,
    low_user_id BIGINT NOT NULL,
    high_user_id BIGINT NOT NULL,
    low_generation_id UUID NOT NULL,
    high_generation_id UUID NOT NULL,
    mode TEXT NOT NULL DEFAULT 'signal_pq_1' CHECK (mode='signal_pq_1'),
    state TEXT NOT NULL DEFAULT 'active' CHECK (state IN ('active','closed')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    closed_at TIMESTAMPTZ,
    close_reason TEXT,
    closed_generation_id UUID,
    FOREIGN KEY (low_user_id,low_generation_id) REFERENCES e2ee_devices(user_id,generation_id),
    FOREIGN KEY (high_user_id,high_generation_id) REFERENCES e2ee_devices(user_id,generation_id),
    UNIQUE (conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id),
    CHECK (low_user_id<high_user_id),
    CHECK (substring(conversation_id::text,15,1)='4' AND substring(conversation_id::text,20,1) IN ('8','9','a','b')),
    CHECK ((state='active' AND closed_at IS NULL AND close_reason IS NULL AND closed_generation_id IS NULL) OR
           (state='closed' AND closed_at IS NOT NULL AND close_reason IS NOT NULL AND close_reason='device_replaced'
            AND closed_generation_id IS NOT NULL AND closed_generation_id IN (low_generation_id,high_generation_id)))
);
CREATE UNIQUE INDEX e2ee_one_active_conversation ON e2ee_conversations(low_user_id,high_user_id) WHERE state='active';
CREATE INDEX e2ee_active_low_generation ON e2ee_conversations(low_generation_id) WHERE state='active';
CREATE INDEX e2ee_active_high_generation ON e2ee_conversations(high_generation_id) WHERE state='active';
CREATE INDEX e2ee_low_conversations ON e2ee_conversations(low_user_id,created_at,conversation_id);
CREATE INDEX e2ee_high_conversations ON e2ee_conversations(high_user_id,created_at,conversation_id);

-- Persist the result even after closure. A lost create response must not become
-- a new conversation on retry after a peer replaces its keys. Canonical pair
-- and generations also constrain the receipt's ownership at the SQL boundary.
CREATE TABLE e2ee_conversation_requests (
    owner_user_id BIGINT NOT NULL,
    owner_generation_id UUID NOT NULL,
    request_id UUID NOT NULL,
    peer_user_id BIGINT NOT NULL,
    conversation_id UUID NOT NULL,
    low_user_id BIGINT NOT NULL,
    high_user_id BIGINT NOT NULL,
    low_generation_id UUID NOT NULL,
    high_generation_id UUID NOT NULL,
    PRIMARY KEY (owner_generation_id,request_id),
    FOREIGN KEY (conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id)
        REFERENCES e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id),
    CHECK ((owner_user_id=low_user_id AND peer_user_id=high_user_id AND owner_generation_id=low_generation_id) OR
           (owner_user_id=high_user_id AND peer_user_id=low_user_id AND owner_generation_id=high_generation_id)),
    CHECK (substring(request_id::text,15,1)='4' AND substring(request_id::text,20,1) IN ('8','9','a','b'))
);

-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_conversation() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    active_count INTEGER;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.conversation_id,NEW.low_user_id,NEW.high_user_id,NEW.low_generation_id,NEW.high_generation_id,NEW.mode,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.conversation_id,OLD.low_user_id,OLD.high_user_id,OLD.low_generation_id,OLD.high_generation_id,OLD.mode,OLD.created_at) THEN
            RAISE EXCEPTION 'E2EE conversation binding is immutable';
        END IF;
        IF OLD.state='closed' AND ROW(NEW.state,NEW.closed_at,NEW.close_reason,NEW.closed_generation_id)
            IS DISTINCT FROM ROW(OLD.state,OLD.closed_at,OLD.close_reason,OLD.closed_generation_id) THEN
            RAISE EXCEPTION 'closed E2EE conversation cannot be reopened or rewritten';
        END IF;
        IF OLD.state='active' AND NEW.state='closed' AND NOT EXISTS (
            SELECT 1 FROM e2ee_devices WHERE generation_id=NEW.closed_generation_id AND retired_at IS NOT NULL) THEN
            RAISE EXCEPTION 'E2EE replacement closure requires a retired generation';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.state<>'active' THEN
        RAISE EXCEPTION 'new E2EE conversation must be active';
    END IF;
    -- Synchronize INSERT with retirement, including maintenance SQL that does
    -- not use the repository's canonical user-row locks.
    PERFORM generation_id FROM e2ee_devices
        WHERE (user_id=NEW.low_user_id AND generation_id=NEW.low_generation_id)
           OR (user_id=NEW.high_user_id AND generation_id=NEW.high_generation_id)
        ORDER BY user_id FOR SHARE;
    SELECT count(*) INTO active_count FROM e2ee_devices
        WHERE retired_at IS NULL AND public_material_ready AND
            ((user_id=NEW.low_user_id AND generation_id=NEW.low_generation_id)
             OR (user_id=NEW.high_user_id AND generation_id=NEW.high_generation_id));
    IF active_count<>2 THEN
        RAISE EXCEPTION 'E2EE conversation requires current published generations';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_conversation BEFORE INSERT OR UPDATE ON e2ee_conversations
    FOR EACH ROW EXECUTE FUNCTION e2ee_guard_conversation();

-- +goose StatementBegin
CREATE FUNCTION e2ee_close_retired_conversations() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE e2ee_conversations SET state='closed',close_reason='device_replaced',
        closed_generation_id=OLD.generation_id,closed_at=clock_timestamp()
        WHERE state='active' AND (low_generation_id=OLD.generation_id OR high_generation_id=OLD.generation_id);
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_close_retired_conversations AFTER UPDATE OF retired_at ON e2ee_devices
    FOR EACH ROW WHEN (OLD.retired_at IS NULL AND NEW.retired_at IS NOT NULL)
    EXECUTE FUNCTION e2ee_close_retired_conversations();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM e2ee_conversations) OR EXISTS (SELECT 1 FROM e2ee_conversation_requests) THEN
        RAISE EXCEPTION 'cannot roll back nonempty E2EE conversations';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_close_retired_conversations ON e2ee_devices;
DROP FUNCTION e2ee_close_retired_conversations();
DROP TRIGGER e2ee_guard_conversation ON e2ee_conversations;
DROP FUNCTION e2ee_guard_conversation();
DROP TABLE e2ee_conversation_requests;
DROP TABLE e2ee_conversations;
