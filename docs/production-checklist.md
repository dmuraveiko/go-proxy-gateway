# Production checklist

- NATS: 3 or 5 JetStream nodes on separate SSDs/AZs; TLS, JWT/NKeys, accounts and subject ACLs enabled.
- PostgreSQL: HA deployment, TLS, PITR backups, tested restore, connection proxy and alerting.
- Run `proxy migrate` as a one-shot deployment job before rolling out application pods.
- Configure ed25519 key permissions; private key comes from Vault/KMS/CSI secret provider.
- Replace generic webhook with provider-specific canonical signature verification and event mapping.
- Pin container digest, generate SBOM, scan image and dependencies, sign the image.
- Apply NetworkPolicy with explicit provider CIDRs and configure ingress TLS/rate limits.
- Load-test at 2× expected peak, then test NATS loss, DB loss, provider timeouts and pod termination.
- Verify dashboards/alerts, DLQ access control, retention job and operational ownership.
