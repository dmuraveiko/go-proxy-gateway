# Operations runbook

## Backlog

Check `/metrics`, PostgreSQL operation counts and JetStream consumer pending/ack floor. Do not purge command streams. Scale workers only after confirming provider and database limits.

## DLQ

Inspect `proxy.dlq.>` without logging secrets. Classify permanent contract errors, provider rejection and exhausted transient failures. Replay using the original command ID only after the root cause is fixed; deduplication prevents accidental duplicate DB operations.

## Unknown outcome

Never blindly retry a payment/write after a timeout. Query the provider by idempotency key or transaction identifier, then transition it through a dedicated reconciliation workflow. Escalate unresolved operations to manual review.

## Key rotation

Deploy the new public key and permissions first, rotate producers, observe old-key traffic, then revoke the old key. Rotate the gateway signing key only after consumers trust both keys.

## Disaster recovery

Restore PostgreSQL to the agreed PITR point, restore/recover JetStream quorum, start migration job, then gateway. Compare pending operations and outbox with JetStream sequences before reopening traffic.
