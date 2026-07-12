# Decisions

## ADR-001 — PostgreSQL durable state

Use PostgreSQL for restart-safe operations, webhook deduplication and transactional outbox. It supports horizontal replicas and removes the single-writer limitation of the initial bbolt prototype.

## ADR-002 — At-least-once and caller IDs

Use caller-provided request IDs for deduplication and require idempotent downstream APIs. Exactly-once claims are avoided.

## ADR-003 — Generic proxy, provider adapters later

Keep the first module provider-neutral. Provider authentication, callback verification, schema validation, rate limits, and circuit breakers are adapters added once concrete APIs are chosen.

## ADR-004 — JetStream instead of Core NATS

Use JetStream for commands, results and events because Core NATS cannot retain messages while consumers are unavailable. Core NATS alone conflicts with the required failure guarantees.
