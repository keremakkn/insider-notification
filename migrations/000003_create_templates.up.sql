-- Message templates with {{variable}} placeholders. A notification created
-- from a template stores the rendered text, so editing a template never
-- changes notifications that already exist.

CREATE TABLE templates (
    id         UUID        PRIMARY KEY,
    name       TEXT        NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,99}$'),
    channel    TEXT        NOT NULL CHECK (channel IN ('sms', 'email', 'push')),
    body       TEXT        NOT NULL CHECK (length(body) > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER templates_set_updated_at
    BEFORE UPDATE ON templates
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE notifications ADD COLUMN template_id UUID REFERENCES templates (id);
