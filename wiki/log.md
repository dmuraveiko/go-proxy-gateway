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

## [2026-07-13] decision | Simplified Core NATS HTTP proxy target

Recorded the revised target architecture agreed after technical review: arbitrary caller-selected HTTP destinations, no JetStream or provider registry, PostgreSQL-backed durability with application ACKs, Ed25519 service authorization, `net/http` with application-data preservation rather than wire-byte identity, caller-controlled retries, and durable webhook fan-out. Implementation was intentionally left unchanged for a later phase.

## [2026-07-13] decision | Asynchronous application protocol

Confirmed that commands, durable acceptance acknowledgements, final HTTP results and result acknowledgements are separate asynchronous messages. Webhook ingress remains synchronously open only until its durable PostgreSQL commit; internal fan-out is asynchronous.

## [2026-07-14] implementation | Core NATS durable HTTP proxy

Replaced the JetStream/provider-specific gateway with the agreed universal proxy: Core NATS application ACKs, PostgreSQL state machine, Ed25519 client ACL, synchronous Go client, unrestricted `net/http`, unknown-outcome protection, shared per-host throttling, connection pooling, retention, and static/delegated webhook fan-out. Verified outgoing HTTP and both webhook modes against real NATS and PostgreSQL in Docker.

## [2026-07-14] documentation | Review package and squashed schema

Squashed the prototype migration history into one clean-database `000001_initial`, synchronized the README, runbook, SLO, production checklist and wiki with the implemented Core NATS design, and added a plain-language behavior document for technical review with normal and failure scenarios.
