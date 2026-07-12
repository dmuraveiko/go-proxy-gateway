CREATE TABLE IF NOT EXISTS proxy_rate_limits (
 provider text NOT NULL,
 window_start timestamptz NOT NULL,
 used integer NOT NULL,
 PRIMARY KEY(provider, window_start)
);
