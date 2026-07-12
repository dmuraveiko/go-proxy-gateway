# Integration gateway architecture

```text
Service A -> signed command -> JetStream -> PostgreSQL inbox/state machine
                                                   |
                                            bounded workers
                                                   |
                                              integration handler -> Internet API
                                                   |
                                            transactional outbox -> JetStream result/DLQ

External provider -> authenticated webhook handler -> PostgreSQL dedup + outbox -> JetStream event -> Service C
```

Handlers receive typed payloads, never caller-controlled URLs. Each handler owns endpoint configuration, credentials, schema validation, timeout, rate limit and retry classification. The registry keys handlers by `(type, version)` so contracts can evolve without breaking existing producers.

JetStream provides durable transport. PostgreSQL is the source of truth for gateway processing state and makes horizontal workers safe through short leases and `SKIP LOCKED`. Transactional outbox closes the crash window between committing a result/webhook and publishing it. JetStream message IDs deduplicate repeated outbox publications.

The system intentionally promises at-least-once rather than exactly-once. External write operations must accept an idempotency key derived from command ID. Read operations can safely repeat. Failed commands produce a normal failure result for the caller and a DLQ record for operations.

Security boundaries: NATS envelope signatures, NATS subject ACLs/credentials, provider-specific webhook authentication, HTTPS-only fixed external endpoints, DNS/private-network rejection, bounded bodies, no redirects, and secrets supplied outside the repository.
