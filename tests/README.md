# Tests

Three layers, matched to what they prove:

| Layer          | What it covers                                              | Runs against          |
| -------------- | ----------------------------------------------------------- | --------------------- |
| **Unit**       | CloudEvents schema — required fields, invariants, mapping   | No infra, pure Python |
| **Smoke**      | End-to-end: HTTP → Kafka → Postgres, plus idempotency       | `docker compose up`   |
| **Load**       | Concurrent POST throughput + no-drop verification           | `docker compose up`   |

Plus a **Postman collection** for interactive exploration.

---

## 1. Unit tests (`tests/unit/`)

Pure pydantic validation — runs in under a second, no Docker required.

```bash
python -m venv .venv && . .venv/bin/activate
pip install -e ".[test]"
pytest tests/unit/ -v
```

Covers:

- Every shipped `tests/sample-events/*.json` parses cleanly as a `CloudEvent`
  or `EventBatch`.
- Each required field (`source`, `type`, `data.actor`, `data.action`,
  `data.outcome`) produces a `ValidationError` when missing.
- Enum constraints on `outcome` and `actor.type`.
- `specversion` is pinned to `"1.0"`.
- RFC3339 timestamps parse correctly with both `Z` and explicit offsets.
- `data` and envelope both allow extra/custom fields (CloudEvents spec allows this).
- `CloudEvent.to_record()` produces the exact column shape inserted into
  Postgres, including `trace_id` extraction from a W3C `traceparent`.

28 test cases, <1 s runtime.

---

## 2. Smoke test (`tests/smoke.sh`)

Validates the full pipeline. Start the stack first:

```bash
docker compose up --build -d
```

Wait for health:

```bash
curl -sf http://localhost:8000/v1/auditmanager/health
```

Then:

```bash
tests/smoke.sh
```

What it does, in order:

1. Pre-flight: checks `curl`, `docker`, `jq` are installed; hits `/health`.
2. POSTs each of the 7 valid sample events (login, login-failed,
   beneficiary-viewed with trace, beneficiary-updated with diff,
   payment-approved, system-initiated payment-reversed, access-denied),
   asserting `202` on each.
3. POSTs the 3-event batch to `/events/batch`.
4. POSTs the intentionally-invalid sample, asserting a 4xx.
5. Sleeps 4 s for Kafka → Postgres flush.
6. Queries Postgres (via `docker compose exec`) and asserts every expected
   id is present.
7. Spot-checks one row: verifies `type`, `actor_id`, `resource_type`,
   `outcome`, and that `envelope->'data'->'changes'` still has 2 elements
   (full CloudEvents fidelity preserved).
8. Re-POSTs a duplicate and confirms Postgres still has exactly one row for
   that id (idempotency).

Override points:

```bash
AUDIT_URL=http://other-host:8000 tests/smoke.sh
SETTLE_SECONDS=10 tests/smoke.sh               # slower env
COMPOSE_PG_SERVICE=pg COMPOSE_PG_USER=me tests/smoke.sh
```

Exit code: `0` on all-green, `1` on any failure. The failure output
includes the commands to debug (`docker compose logs`, direct `psql` query).

---

## 3. Load test (`tests/load.sh`)

Sends `N` uniquely-id'd events concurrently, then confirms every one landed
in Postgres.

```bash
tests/load.sh                    # defaults: 1000 events, 20 concurrent
N=5000 C=50 tests/load.sh
AUDIT_URL=http://host:8000 tests/load.sh
```

Output includes the HTTP-side request rate (how fast your client could
submit) and the Postgres row count for the run.

This is a *smoke* load test, not a rigorous benchmark. For serious load
testing use `k6`, `wrk`, or `vegeta`.

---

## 4. Postman collection

Import `tests/postman/OpenG2P-Audit-Manager.postman_collection.json` into
Postman (or any compatible tool — Bruno, Insomnia with conversion, etc.).

Folders:

- **Service endpoints** — `/health`, `/version`, `/config`, `/docs`
- **Single events — success** — login, beneficiary view, beneficiary update
  with diff, payment approve, system-initiated payment reverse
- **Single events — failure / denied** — login_failed, access-denied
- **Batch** — 3 events in one POST
- **Negative tests** — schema-invalid payloads that should 4xx

Each request includes a Postman test assertion (`202 Accepted`, expected ids
echoed, etc.). Use the **Runner** to fire the whole collection and see green.

Collection variable `baseUrl` defaults to `http://localhost:8000`; override
for any other environment.

---

## Sample events (`tests/sample-events/`)

Reusable `curl -d @file.json` fixtures:

| File                                   | Event type                              | Notes                                 |
| -------------------------------------- | --------------------------------------- | ------------------------------------- |
| `01-login-success.json`                | `org.openg2p.auth.login`                | No resource, context only             |
| `02-login-failed.json`                 | `org.openg2p.auth.login_failed`         | `outcome=failure`, `reason` populated |
| `03-beneficiary-viewed.json`           | `org.openg2p.beneficiary.viewed`        | Has `traceparent` for correlation     |
| `04-beneficiary-updated.json`          | `org.openg2p.beneficiary.updated`       | Includes `data.changes` diff          |
| `05-payment-approved.json`             | `org.openg2p.payment.approved`          | Resource carries amount/currency      |
| `06-payment-reversed-system.json`      | `org.openg2p.payment.reversed`          | `actor.type=system`                   |
| `07-access-denied.json`                | `org.openg2p.beneficiary.viewed`        | `outcome=denied`                      |
| `08-batch.json`                        | `EventBatch` (3 events)                 | For `/events/batch`                   |
| `99-invalid-missing-actor.json`        | `org.openg2p.beneficiary.updated`       | Missing `data.actor` — must 4xx       |

Example single POST:

```bash
curl -sX POST http://localhost:8000/v1/auditmanager/events \
  -H 'content-type: application/json' \
  --data-binary @tests/sample-events/04-beneficiary-updated.json
```

Example batch:

```bash
curl -sX POST http://localhost:8000/v1/auditmanager/events/batch \
  -H 'content-type: application/json' \
  --data-binary @tests/sample-events/08-batch.json
```
