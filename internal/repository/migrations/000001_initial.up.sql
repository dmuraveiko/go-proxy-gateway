CREATE TABLE IF NOT EXISTS proxy_operations (
 id text PRIMARY KEY, command_type text NOT NULL, command_version integer NOT NULL, command jsonb NOT NULL,
 status text NOT NULL CHECK(status IN ('pending','processing','retrying','completed','failed','outcome_unknown','manual_review')),
 attempts integer NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL, lease_until timestamptz,
 last_error_code text, last_error text, result jsonb, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS proxy_operations_due_idx ON proxy_operations(next_attempt_at) WHERE status IN ('pending','retrying');
CREATE TABLE IF NOT EXISTS proxy_webhook_events (id text PRIMARY KEY, provider text NOT NULL, event_type text NOT NULL, payload jsonb NOT NULL, received_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS proxy_outbox (id text PRIMARY KEY, subject text NOT NULL, payload bytea NOT NULL, publish_attempts integer NOT NULL DEFAULT 0, lease_until timestamptz, published_at timestamptz, created_at timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS proxy_outbox_pending_idx ON proxy_outbox(created_at) WHERE published_at IS NULL;
