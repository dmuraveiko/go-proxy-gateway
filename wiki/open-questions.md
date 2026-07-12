# Open questions

1. Which external providers and exact endpoints are allowed in each environment?
2. What authentication/signature scheme does each inbound webhook use?
3. Is JetStream prohibited, or merely absent from the initial estimate?
4. What is the required result acknowledgement protocol and retention period?
5. What are per-provider rate limits, timeout/SLA, payload caps, and retry rules?
6. How are ed25519 public keys rotated and mapped to service identities?
7. Which NATS TLS/credentials mechanism and subject ACLs are mandated?
8. Is horizontal scaling required with shared durable state?
