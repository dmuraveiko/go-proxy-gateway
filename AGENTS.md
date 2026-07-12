# Project knowledge workflow

This repository follows the persistent wiki pattern described in the source gist.

- `raw/` contains immutable source material. Never edit an ingested source.
- `wiki/` contains maintained synthesis. Read `wiki/index.md` first; update affected pages when architecture or requirements change.
- Append material ingests, architecture decisions, queries, and lint passes to `wiki/log.md` using `## [YYYY-MM-DD] kind | title`.
- Keep code, tests, README, and wiki consistent. Flag contradictions and uncertainty explicitly instead of silently choosing requirements.
- Do not store secrets, wallet keys, NATS seeds, or production payloads in the repository.
