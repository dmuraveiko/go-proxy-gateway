# Proxy requirements

## Confirmed direction

The service bridges HTTP and NATS asynchronously, calls only approved Internet resources, accepts only approved webhook routes, retries transient failures, confirms acceptance/results, survives restarts, and remains extensible for provider-specific authentication, rate limits, and health probes. NATS messages are expected to use ed25519 signatures.

## Tensions in the source

The source says Core NATS without JetStream and no database, while the newer request requires failure-safe delivery. Core NATS is at-most-once and an in-memory service cannot preserve work across crashes. The initial implementation therefore uses a local durable bbolt inbox. This is local service state rather than a shared business database, but it changes the original stateless deployment assumption.

Blockchain consensus across two API providers belongs above the generic transport layer: the proxy reports independent provider results; a domain service decides whether they agree.
