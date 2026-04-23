# OpenG2P Audit Manager

A centralised audit-event service for OpenG2P. Accepts structured audit events
from any OpenG2P service (FastAPI, Odoo, external/webhook sources) over HTTP,
buffers them through Kafka, and persists them to a partitioned PostgreSQL
table for long-term retention and forensic querying.

---

## Documentation

Full documentation — design, event schema, API, scalability, deployment,
operational runbook, security — lives at:

**[docs.openg2p.org — Audit Manager](https://docs.openg2p.org/platform/platform-services/audit-manager)**

Topics covered there:

- Why this service exists and why the design was chosen over alternatives
- CloudEvents-based event schema and `data` conventions
- HTTP API (`/v1/auditmanager/events`, health, version, config)
- Scalability model (Kafka consumer groups, HPA, partition count ceiling)
- Reliability / delivery guarantees and failure modes
- Retention & monthly partitioning
- Configuration reference (env vars, YAML, Helm values)
- Operational runbook (backpressure, lag, DLQ, investigation queries)
- Security considerations

---

## Quick start

Local dev with Docker Compose:

```bash
docker compose up --build
```

- API:      http://localhost:8000/v1/auditmanager/
- Swagger:  http://localhost:8000/docs
- Health:   http://localhost:8000/v1/auditmanager/health

Smoke test and sample events:

```bash
tests/smoke.sh                             # end-to-end HTTP → Kafka → Postgres
pytest tests/unit/ -v                      # schema unit tests
```

See [`tests/README.md`](tests/README.md) for the full test layout and the
Postman collection.

---

## Repository layout

| Path | Purpose |
| --- | --- |
| `src/audit_manager/` | Service source (FastAPI app, Kafka producer/consumer, DB models) |
| `config/default.yaml` | Built-in defaults; overridable via env vars and Helm values |
| `helm/openg2p-audit-manager/` | Helm chart (depends on `postgres-init`, ships a Kafka topic-init hook) |
| `tests/` | Unit tests, smoke / load scripts, Postman collection, sample events |
| `scripts/generate_openapi.py` | Regenerates `docs/openapi.json` from the live FastAPI app |
| `Dockerfile` / `docker-entrypoint.sh` | Multi-stage image (Python 3.13-slim, non-root) |
| `docker-compose.yaml` | Local Postgres + Kafka + audit-manager stack |

---

## License

SPDX-License-Identifier: MPL-2.0

Part of the [OpenG2P](https://www.openg2p.org/) platform.
