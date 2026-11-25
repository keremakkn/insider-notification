-- Core schema. PostgreSQL is the source of truth for every notification;
-- Redis only holds IDs that the dispatcher copies from here (docs/DESIGN.md §5).

CREATE TABLE batches (
    id          UUID        PRIMARY KEY,
    total       INT         NOT NULL CHECK (total BETWEEN 1 AND 1000),
    accepted    INT         NOT NULL CHECK (accepted >= 0),
    rejected    INT         NOT NULL CHECK (rejected >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (accepted + rejected = total)
);

CREATE TABLE notifications (
    id                  UUID        PRIMARY KEY,
    batch_id            UUID        REFERENCES batches (id),
    idempotency_key     TEXT,
    request_hash        TEXT,
    correlation_id      TEXT        NOT NULL,

    recipient           TEXT        NOT NULL,
    channel             TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    content             TEXT        NOT NULL,
    priority            SMALLINT    NOT NULL CHECK (priority BETWEEN 0 AND 2), -- 0 high, 1 normal, 2 low

    status              TEXT        NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'processing', 'sent', 'failed', 'cancelled')),
    attempts            INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts        INT         NOT NULL CHECK (max_attempts > 0),

    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT now(), -- the only time the dispatcher reads
    scheduled_at        TIMESTAMPTZ,                        -- informational: what the client asked for
    enqueued_at         TIMESTAMPTZ,                        -- last push to Redis
    lease_expires_at    TIMESTAMPTZ,                        -- set while processing

    provider_message_id TEXT,
    last_error          TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at             TIMESTAMPTZ,

    -- A key without its hash could not detect reuse with different content.
    CONSTRAINT idempotency_key_has_hash CHECK ((idempotency_key IS NULL) = (request_hash IS NULL)),
    -- A processing row without a lease could never be recovered if its worker died.
    CONSTRAINT processing_has_lease CHECK (status <> 'processing' OR lease_expires_at IS NOT NULL),
    CONSTRAINT sent_has_sent_at CHECK (status <> 'sent' OR sent_at IS NOT NULL)
);

-- Idempotency: one notification per key.
CREATE UNIQUE INDEX notifications_idempotency_key_uq
    ON notifications (idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Dispatcher selection, per channel and priority. Partial: only pending rows, so it stays small.
CREATE INDEX notifications_dispatch_idx
    ON notifications (channel, priority, next_attempt_at) WHERE status = 'pending';

-- Lease recovery.
CREATE INDEX notifications_lease_idx
    ON notifications (lease_expires_at) WHERE status = 'processing';

CREATE INDEX notifications_batch_idx
    ON notifications (batch_id) WHERE batch_id IS NOT NULL;

-- Listing with keyset pagination on (created_at, id), optionally filtered.
CREATE INDEX notifications_list_idx         ON notifications (created_at DESC, id DESC);
CREATE INDEX notifications_list_status_idx  ON notifications (status, created_at DESC, id DESC);
CREATE INDEX notifications_list_channel_idx ON notifications (channel, created_at DESC, id DESC);

-- Keep updated_at correct no matter which code path writes the row.
CREATE FUNCTION set_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notifications_set_updated_at
    BEFORE UPDATE ON notifications
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- One row per provider call: debugging, latency, and the dead-letter history of failed notifications.
CREATE TABLE delivery_attempts (
    id              BIGSERIAL   PRIMARY KEY,
    notification_id UUID        NOT NULL REFERENCES notifications (id) ON DELETE CASCADE,
    attempt_no      INT         NOT NULL CHECK (attempt_no > 0),
    http_status     INT,
    error           TEXT,
    duration_ms     INT         NOT NULL CHECK (duration_ms >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (notification_id, attempt_no)
);
