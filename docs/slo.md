# Service levels

Initial targets, to be validated by load tests:

- Availability: 99.95% monthly for accepting durable commands/webhooks.
- Processing latency: 99% of healthy-provider read commands completed within 10 seconds.
- Durable acceptance: no acknowledged command or webhook event lost.
- RPO: 0 for replicated JetStream acknowledgements and committed PostgreSQL transactions.
- RTO: 30 minutes.

Provider downtime is tracked separately from gateway availability. Error-budget burn alerts should cover 1-hour and 6-hour windows.
