CREATE TABLE proxy_http_requests (
  request_id text PRIMARY KEY,
  client_id text NOT NULL,
  proxy_id text NOT NULL,
  request jsonb NOT NULL,
  status text NOT NULL CHECK (status IN (
    'awaiting_acceptance_ack', 'ready', 'reserved', 'dispatching', 'retry_wait',
    'http_completed', 'result_delivered', 'failed', 'unknown', 'canceled'
  )),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz,
  dispatch_token text,
  result jsonb,
  last_error text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX proxy_http_requests_due_idx
  ON proxy_http_requests(next_attempt_at, created_at)
  WHERE status IN ('ready', 'retry_wait');
CREATE INDEX proxy_http_requests_stale_idx
  ON proxy_http_requests(lease_until)
  WHERE status IN ('reserved', 'dispatching');
CREATE INDEX proxy_http_requests_client_idx
  ON proxy_http_requests(client_id, created_at DESC);

CREATE TABLE proxy_host_rate_windows (
  host text NOT NULL,
  window_start timestamptz NOT NULL,
  used integer NOT NULL CHECK (used > 0),
  PRIMARY KEY(host, window_start)
);
CREATE TABLE proxy_host_permits (
  token text PRIMARY KEY,
  host text NOT NULL,
  lease_until timestamptz NOT NULL
);
CREATE INDEX proxy_host_permits_host_idx
  ON proxy_host_permits(host, lease_until);

CREATE TABLE proxy_webhook_routes (
  webhook_id text PRIMARY KEY,
  owner_client_id text NOT NULL,
  name text NOT NULL,
  path_token_hash bytea NOT NULL,
  mode text NOT NULL CHECK (mode IN ('static', 'delegated')),
  static_response jsonb NOT NULL,
  responder_client_id text,
  response_timeout_ms bigint NOT NULL CHECK (response_timeout_ms > 0),
  max_body_bytes bigint NOT NULL CHECK (max_body_bytes > 0),
  enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CHECK (mode = 'static' OR responder_client_id IS NOT NULL)
);
CREATE INDEX proxy_webhook_routes_owner_idx
  ON proxy_webhook_routes(owner_client_id, created_at DESC);

CREATE TABLE proxy_webhook_subscribers (
  webhook_id text NOT NULL REFERENCES proxy_webhook_routes(webhook_id) ON DELETE CASCADE,
  client_id text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY(webhook_id, client_id)
);

CREATE TABLE proxy_webhook_events (
  event_id text PRIMARY KEY,
  webhook_id text NOT NULL REFERENCES proxy_webhook_routes(webhook_id) ON DELETE CASCADE,
  request jsonb NOT NULL,
  response jsonb,
  status text NOT NULL CHECK (status IN ('received', 'awaiting_response', 'responded', 'timed_out')),
  received_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX proxy_webhook_events_retention_idx
  ON proxy_webhook_events(updated_at);
CREATE INDEX proxy_webhook_events_route_idx
  ON proxy_webhook_events(webhook_id, received_at DESC);

CREATE TABLE proxy_deliveries (
  delivery_id text PRIMARY KEY,
  kind text NOT NULL CHECK (kind IN ('acceptance', 'result', 'webhook')),
  client_id text NOT NULL,
  subject text NOT NULL,
  message_type text NOT NULL,
  payload jsonb NOT NULL,
  request_id text REFERENCES proxy_http_requests(request_id) ON DELETE CASCADE,
  webhook_event_id text REFERENCES proxy_webhook_events(event_id) ON DELETE CASCADE,
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  lease_until timestamptz,
  acknowledged_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (
    (kind IN ('acceptance', 'result') AND request_id IS NOT NULL AND webhook_event_id IS NULL)
    OR
    (kind = 'webhook' AND request_id IS NULL AND webhook_event_id IS NOT NULL)
  )
);
CREATE INDEX proxy_deliveries_due_idx
  ON proxy_deliveries(next_attempt_at, created_at)
  WHERE acknowledged_at IS NULL;
CREATE INDEX proxy_deliveries_request_idx
  ON proxy_deliveries(request_id) WHERE request_id IS NOT NULL;
CREATE INDEX proxy_deliveries_webhook_idx
  ON proxy_deliveries(webhook_event_id) WHERE webhook_event_id IS NOT NULL;
