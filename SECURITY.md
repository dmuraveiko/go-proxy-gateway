# Security

Report vulnerabilities privately to the owning security team; do not open public issues containing credentials or payloads. Secrets and NATS/Ed25519 private keys must never enter this repository. Production requires authenticated NATS, TLS, Ed25519 client-to-proxy authorization, subject ACL, payload limits and audit. Arbitrary destination access is an explicit business requirement and includes internal/metadata addresses reachable from the Proxy network; compensate with strict client authorization and environment-level segmentation.
