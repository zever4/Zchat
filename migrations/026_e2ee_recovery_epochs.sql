-- +goose Up
-- Development cutover only: do not reinterpret a v1 ciphertext/private session
-- as v2 epoch0. No deployed state, compatibility reader or destructive auto-reset.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM e2ee_conversations) THEN
        RAISE EXCEPTION 'epoch schema requires empty prototype E2EE conversations; no automatic v1 conversion';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE e2ee_conversations ADD COLUMN active_epoch BIGINT NOT NULL DEFAULT 0 CHECK(active_epoch>=0),
    ADD COLUMN epoch_revision BIGINT NOT NULL DEFAULT 0 CHECK(epoch_revision>=0);

CREATE TABLE e2ee_conversation_epochs (
    conversation_id UUID NOT NULL REFERENCES e2ee_conversations(conversation_id),
    epoch BIGINT NOT NULL CHECK(epoch>=0),
    revision BIGINT NOT NULL CHECK(revision>=0),
    state TEXT NOT NULL DEFAULT 'active' CHECK(state IN('active','sealed')),
    parent_epoch BIGINT,
    created_by_proposal_id UUID,
    sealed_by_proposal_id UUID,
    bootstrap_user_id BIGINT REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL,
    sealed_at TIMESTAMPTZ,
    PRIMARY KEY(conversation_id,epoch),
    UNIQUE(conversation_id,epoch,revision),
    FOREIGN KEY(conversation_id,parent_epoch) REFERENCES e2ee_conversation_epochs(conversation_id,epoch),
    CHECK((epoch=0 AND revision=0 AND parent_epoch IS NULL AND created_by_proposal_id IS NULL AND bootstrap_user_id IS NULL) OR
          (epoch>0 AND revision>0 AND parent_epoch IS NOT NULL AND parent_epoch=epoch-1 AND created_by_proposal_id IS NOT NULL AND bootstrap_user_id IS NOT NULL)),
    CHECK((state='active' AND sealed_by_proposal_id IS NULL AND sealed_at IS NULL) OR
          (state='sealed' AND sealed_by_proposal_id IS NOT NULL AND sealed_at IS NOT NULL AND sealed_at>=created_at))
);
CREATE UNIQUE INDEX e2ee_one_active_epoch ON e2ee_conversation_epochs(conversation_id) WHERE state='active';
ALTER TABLE e2ee_conversations ADD CONSTRAINT e2ee_epoch_head_fk FOREIGN KEY(conversation_id,active_epoch,epoch_revision)
    REFERENCES e2ee_conversation_epochs(conversation_id,epoch,revision) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE e2ee_epoch_recoveries (
    conversation_id UUID NOT NULL,
    proposal_id UUID NOT NULL,
    initiator_user_id BIGINT NOT NULL,
    peer_user_id BIGINT NOT NULL,
    initiator_generation_id UUID NOT NULL,
    peer_generation_id UUID NOT NULL,
    low_user_id BIGINT NOT NULL,
    high_user_id BIGINT NOT NULL,
    low_generation_id UUID NOT NULL,
    high_generation_id UUID NOT NULL,
    expected_epoch BIGINT NOT NULL CHECK(expected_epoch>=0 AND expected_epoch<9223372036854775807),
    expected_revision BIGINT NOT NULL CHECK(expected_revision>=0 AND expected_revision<9223372036854775807),
    proposal_digest BYTEA NOT NULL CHECK(octet_length(proposal_digest)=32),
    proposal_wire BYTEA NOT NULL CHECK(octet_length(proposal_wire) BETWEEN 1 AND 4096),
    decision_wire BYTEA CHECK(octet_length(decision_wire) BETWEEN 1 AND 4096),
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN('pending','accepted','declined','cancelled','expired','superseded')),
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL CHECK(expires_at>created_at),
    resolved_at TIMESTAMPTZ,
    new_epoch BIGINT,
    result_revision BIGINT,
    PRIMARY KEY(conversation_id,proposal_id),
    FOREIGN KEY(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id)
        REFERENCES e2ee_conversations(conversation_id,low_user_id,high_user_id,low_generation_id,high_generation_id),
    FOREIGN KEY(conversation_id,expected_epoch,expected_revision) REFERENCES e2ee_conversation_epochs(conversation_id,epoch,revision),
    FOREIGN KEY(conversation_id,new_epoch,result_revision) REFERENCES e2ee_conversation_epochs(conversation_id,epoch,revision) DEFERRABLE INITIALLY DEFERRED,
    CHECK((initiator_user_id=low_user_id AND peer_user_id=high_user_id AND initiator_generation_id=low_generation_id AND peer_generation_id=high_generation_id) OR
          (initiator_user_id=high_user_id AND peer_user_id=low_user_id AND initiator_generation_id=high_generation_id AND peer_generation_id=low_generation_id)),
    CHECK(substring(proposal_id::text,15,1)='4' AND substring(proposal_id::text,20,1) IN('8','9','a','b')),
    CHECK((state='pending' AND resolved_at IS NULL) OR (state<>'pending' AND resolved_at IS NOT NULL AND resolved_at>=created_at)),
    CHECK((state='accepted' AND decision_wire IS NOT NULL AND new_epoch IS NOT NULL AND result_revision IS NOT NULL AND new_epoch=expected_epoch+1 AND result_revision=expected_revision+1) OR
          (state<>'accepted' AND new_epoch IS NULL AND result_revision IS NULL)),
    CHECK((state IN('accepted','declined','cancelled') AND decision_wire IS NOT NULL) OR
          (state IN('pending','expired','superseded') AND decision_wire IS NULL)),
    CHECK(state<>'expired' OR resolved_at>=expires_at),
    CHECK(state NOT IN('accepted','declined','cancelled') OR resolved_at<expires_at)
);
CREATE UNIQUE INDEX e2ee_one_pending_recovery ON e2ee_epoch_recoveries(conversation_id) WHERE state='pending';
CREATE INDEX e2ee_recovery_initiator_pending ON e2ee_epoch_recoveries(initiator_generation_id,conversation_id) WHERE state='pending';
CREATE INDEX e2ee_recovery_peer_pending ON e2ee_epoch_recoveries(peer_generation_id,conversation_id) WHERE state='pending';
ALTER TABLE e2ee_conversation_epochs ADD CONSTRAINT e2ee_epoch_created_recovery_fk FOREIGN KEY(conversation_id,created_by_proposal_id)
    REFERENCES e2ee_epoch_recoveries(conversation_id,proposal_id) DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT e2ee_epoch_sealed_recovery_fk FOREIGN KEY(conversation_id,sealed_by_proposal_id) REFERENCES e2ee_epoch_recoveries(conversation_id,proposal_id) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE e2ee_recovery_operations (
    owner_user_id BIGINT NOT NULL,
    owner_generation_id UUID NOT NULL,
    request_id UUID NOT NULL,
    conversation_id UUID NOT NULL,
    proposal_id UUID NOT NULL,
    action SMALLINT NOT NULL CHECK(action BETWEEN 1 AND 4),
    control_digest BYTEA NOT NULL CHECK(octet_length(control_digest)=32),
    control_wire BYTEA NOT NULL CHECK(octet_length(control_wire) BETWEEN 1 AND 4096),
    PRIMARY KEY(owner_generation_id,request_id),
    FOREIGN KEY(owner_user_id,owner_generation_id) REFERENCES e2ee_devices(user_id,generation_id),
    FOREIGN KEY(conversation_id,proposal_id) REFERENCES e2ee_epoch_recoveries(conversation_id,proposal_id),
    CHECK(substring(request_id::text,15,1)='4' AND substring(request_id::text,20,1) IN('8','9','a','b'))
);

-- +goose StatementBegin
CREATE FUNCTION e2ee_initialize_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF NEW.active_epoch<>0 OR NEW.epoch_revision<>0 THEN RAISE EXCEPTION 'new conversation epoch must be zero'; END IF;
    INSERT INTO e2ee_conversation_epochs(conversation_id,epoch,revision,created_at)
        VALUES(NEW.conversation_id,0,0,date_trunc('milliseconds',NEW.created_at));
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_initialize_epoch AFTER INSERT ON e2ee_conversations FOR EACH ROW EXECUTE FUNCTION e2ee_initialize_epoch();

-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_epoch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'retained epoch cannot be deleted'; END IF;
    IF ROW(NEW.conversation_id,NEW.epoch,NEW.revision,NEW.parent_epoch,NEW.created_by_proposal_id,NEW.bootstrap_user_id,NEW.created_at)
        IS DISTINCT FROM ROW(OLD.conversation_id,OLD.epoch,OLD.revision,OLD.parent_epoch,OLD.created_by_proposal_id,OLD.bootstrap_user_id,OLD.created_at)
        OR OLD.state='sealed' OR NEW.state<>'sealed' THEN RAISE EXCEPTION 'epoch binding is immutable and seal is terminal'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_epoch BEFORE UPDATE OR DELETE ON e2ee_conversation_epochs FOR EACH ROW EXECUTE FUNCTION e2ee_guard_epoch();

-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_recovery() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'retained recovery cannot be deleted'; END IF;
    IF ROW(NEW.conversation_id,NEW.proposal_id,NEW.initiator_user_id,NEW.peer_user_id,NEW.initiator_generation_id,NEW.peer_generation_id,
           NEW.low_user_id,NEW.high_user_id,NEW.low_generation_id,NEW.high_generation_id,NEW.expected_epoch,NEW.expected_revision,NEW.proposal_digest,NEW.proposal_wire,NEW.created_at,NEW.expires_at)
       IS DISTINCT FROM ROW(OLD.conversation_id,OLD.proposal_id,OLD.initiator_user_id,OLD.peer_user_id,OLD.initiator_generation_id,OLD.peer_generation_id,
           OLD.low_user_id,OLD.high_user_id,OLD.low_generation_id,OLD.high_generation_id,OLD.expected_epoch,OLD.expected_revision,OLD.proposal_digest,OLD.proposal_wire,OLD.created_at,OLD.expires_at)
       OR OLD.state<>'pending' OR NEW.state='pending' THEN RAISE EXCEPTION 'recovery binding is immutable and terminal result cannot change'; END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_recovery BEFORE UPDATE OR DELETE ON e2ee_epoch_recoveries FOR EACH ROW EXECUTE FUNCTION e2ee_guard_recovery();

-- +goose StatementBegin
CREATE FUNCTION e2ee_guard_recovery_operation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    RAISE EXCEPTION 'recovery operation replay evidence is immutable';
END $$;
-- +goose StatementEnd
CREATE TRIGGER e2ee_guard_recovery_operation BEFORE UPDATE OR DELETE ON e2ee_recovery_operations FOR EACH ROW EXECUTE FUNCTION e2ee_guard_recovery_operation();

-- Deferred point checks allow the one TX to seal/insert/move the head, while
-- forbidding detached heads, invented successor epochs or half-accepted results.
-- +goose StatementBegin
CREATE FUNCTION e2ee_check_epoch_head() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE cid UUID; e RECORD; r RECORD;
BEGIN
    cid:=NEW.conversation_id;
    IF NOT EXISTS(SELECT 1 FROM e2ee_conversations c JOIN e2ee_conversation_epochs x
        ON x.conversation_id=c.conversation_id AND x.epoch=c.active_epoch AND x.revision=c.epoch_revision
        WHERE c.conversation_id=cid AND x.state='active') THEN RAISE EXCEPTION 'invalid active epoch head'; END IF;
    IF TG_TABLE_NAME='e2ee_conversation_epochs' THEN
        SELECT * INTO e FROM e2ee_conversation_epochs WHERE conversation_id=cid AND epoch=NEW.epoch;
        IF e.epoch>0 THEN
            SELECT * INTO r FROM e2ee_epoch_recoveries WHERE conversation_id=cid AND proposal_id=e.created_by_proposal_id;
            IF r.state IS DISTINCT FROM 'accepted' OR r.new_epoch<>e.epoch OR r.result_revision<>e.revision
                OR r.expected_epoch IS DISTINCT FROM e.parent_epoch OR r.initiator_user_id<>e.bootstrap_user_id THEN RAISE EXCEPTION 'epoch requires accepted bound recovery'; END IF;
        END IF;
    ELSIF TG_TABLE_NAME='e2ee_epoch_recoveries' THEN
        SELECT * INTO r FROM e2ee_epoch_recoveries WHERE conversation_id=cid AND proposal_id=NEW.proposal_id;
        IF r.state='accepted' AND NOT EXISTS(SELECT 1 FROM e2ee_conversation_epochs x
            WHERE x.conversation_id=cid AND x.epoch=r.expected_epoch AND x.revision=r.expected_revision
              AND x.state='sealed' AND x.sealed_by_proposal_id=r.proposal_id) THEN RAISE EXCEPTION 'acceptance requires old epoch seal'; END IF;
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE CONSTRAINT TRIGGER e2ee_check_head AFTER INSERT OR UPDATE ON e2ee_conversations DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION e2ee_check_epoch_head();
CREATE CONSTRAINT TRIGGER e2ee_check_epoch AFTER INSERT OR UPDATE ON e2ee_conversation_epochs DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION e2ee_check_epoch_head();
CREATE CONSTRAINT TRIGGER e2ee_check_recovery AFTER INSERT OR UPDATE ON e2ee_epoch_recoveries DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION e2ee_check_epoch_head();

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM e2ee_conversation_epochs) OR EXISTS(SELECT 1 FROM e2ee_epoch_recoveries)
        OR EXISTS(SELECT 1 FROM e2ee_recovery_operations) THEN RAISE EXCEPTION 'cannot discard retained epoch/recovery evidence'; END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER e2ee_check_recovery ON e2ee_epoch_recoveries;
DROP TRIGGER e2ee_check_epoch ON e2ee_conversation_epochs;
DROP TRIGGER e2ee_check_head ON e2ee_conversations;
DROP FUNCTION e2ee_check_epoch_head();
DROP TRIGGER e2ee_guard_recovery_operation ON e2ee_recovery_operations;
DROP FUNCTION e2ee_guard_recovery_operation();
DROP TRIGGER e2ee_guard_recovery ON e2ee_epoch_recoveries;
DROP FUNCTION e2ee_guard_recovery();
DROP TRIGGER e2ee_guard_epoch ON e2ee_conversation_epochs;
DROP FUNCTION e2ee_guard_epoch();
DROP TRIGGER e2ee_initialize_epoch ON e2ee_conversations;
DROP FUNCTION e2ee_initialize_epoch();
DROP TABLE e2ee_recovery_operations;
ALTER TABLE e2ee_conversation_epochs DROP CONSTRAINT e2ee_epoch_created_recovery_fk,
    DROP CONSTRAINT e2ee_epoch_sealed_recovery_fk;
DROP TABLE e2ee_epoch_recoveries;
ALTER TABLE e2ee_conversations DROP CONSTRAINT e2ee_epoch_head_fk;
DROP TABLE e2ee_conversation_epochs;
ALTER TABLE e2ee_conversations DROP COLUMN active_epoch,DROP COLUMN epoch_revision;
