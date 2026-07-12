# Log

## [2026-07-12] ingest | RealWallet V2 preliminary requirements

Extracted proxy-related constraints, identified the Core NATS/durability contradiction, and created the initial architecture, decisions, and open questions.

## [2026-07-12] implementation | HTTP-NATS proxy foundation

Added the Go module, durable inbox, signed envelopes, NATS request/result flow, HTTP executor, retry policy, allowlists, webhook ingress/callbacks, health endpoints, container build, and initial tests.

## [2026-07-12] refactor | Integration gateway

Replaced caller-controlled HTTP jobs and local bbolt with a typed integration registry, JetStream durable consumers, PostgreSQL state machine, transactional outbox, horizontal workers, authenticated/deduplicated webhooks, rate limiting and DLQ delivery.

## [2026-07-12] hardening | Production readiness

Split JetStream traffic into bounded replicated streams, added explicit consumer configuration, versioned migrations, shutdown-safe operation release, correlation metadata, key-ID verification and authorization, metrics/readiness, circuit breaking, retention, Kubernetes security manifests, alerts, runbooks, SLO and fuzz tests.

## [2026-07-12] architecture | Review-ready integration gateway

Added durable consumer reconciliation, fail-closed webhook event mapping, parallel bounded outbox publishers, timeout-aligned operation leases, fail-fast endpoint validation, startup validation of key permissions and cleanup of distributed rate-limit state. Replaced the root README with a detailed Russian technical specification.
