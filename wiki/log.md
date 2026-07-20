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

## [2026-07-16] review | Per-proxy storage and standard HTTP adapters

Recorded review feedback as a proposed target: one PostgreSQL per unique proxy, client-owned durable storage, the full 15-step ACK exchange, automatic unknown-error delivery, outgoing integration through `http.RoundTripper`, callback integration through `http.Handler`, and explicit separation of outgoing and callback code. Marked the current implementation as a prototype pending agreement and refactoring.

## [2026-07-16] query | Refactoring blockers and failure scenarios

Compared the proposed target with the current prototype and recorded the remaining decisions around proxy/database identity, client persistence, request IDs, cancellation, `net/http` compatibility, proxy failover, retries, callback behavior, NATS retry lifetime and production limits.

## [2026-07-16] decision | Minimized architecture questions

Resolved implementation-level choices locally: request IDs, cancellation after durable acceptance, no automatic proxy failover, explicit HTTP retries, durable callback control, provider deduplication boundary, configurable throttling and automatic retention. Reduced technical-director confirmation to three contract decisions: proxy/database identity, client-side durable storage and application-data versus wire-byte HTTP preservation.

## [2026-07-16] decision | Client storage, proxy replicas and signed HTTP data

Accepted a built-in client PostgreSQL Store with configurable `natsproxyclient_` table prefix, `http.RoundTripper`, multiple physical instances per logical proxy/database coordinated by NATS queue groups and DB leases, and exact preservation of signed URL/body data while allowing standard `net/http` header ordering and framing.

## [2026-07-16] implementation | Standard client adapters and durable callbacks

Implemented the built-in client PostgreSQL store and migrations, `http.RoundTripper`, automatic recovery of pending client operations, `http.Handler` callback adapter, durable callback control/result ACKs, automatic unknown-error completion, database-to-proxy identity binding, and a two-instance Core NATS integration environment. Verified outgoing HTTP plus static and delegated callbacks against real NATS and PostgreSQL.

## [2026-07-16] implementation | Shared host pacing and retention

Added PostgreSQL-coordinated per-host `min_interval` alongside RPS/concurrency, automatic cleanup for completed client operations/callbacks and acknowledged webhook control commands, and physically separated outgoing and callback client/transport code.

## [2026-07-18] refactor | Functional code boundaries

Split the NATS transport and PostgreSQL repository into explicit outgoing HTTP, callback, durable delivery, host-limit and maintenance files. Removed unused legacy webhook repository methods and merged duplicate webhook ACK handlers without changing the external protocol.

## [2026-07-19] decision | Callback response recovery and default single instance

Recorded one process per `proxy_id` as the normal deployment and retained shared-DB
replicas only as an optional HA mode. Changed delegated callback completion to publish
the handler response first, persist it in the client database second, and recover by
resending the stored response without calling the handler again. Proxy response
deduplication is bound to the original event and delivery IDs.

## [2026-07-20] implementation | Multi-proxy client storage isolation

Changed the built-in client Store to use `(proxy_id, request_id)` and
`(proxy_id, delivery_id)` primary keys, filter recovery by Proxy, and include
`proxy_id` in webhook events. Added an in-place migration from the previous single-ID
client tables and integration coverage for identical IDs belonging to different
Proxy instances.
