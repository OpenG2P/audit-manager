#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Smoke test — Partner API → Audit Manager
#
# Hits POST /partner/ingest_data with a minimal JSON body N times. The handler
# will likely return an error envelope wrapped in 200 (or a 5xx) — either way
# the audit middleware fires. We're testing the AUDIT pipeline, not ingestion.
#
# DB verification is left to the user — query audit_events manually after
# this script finishes.
#
# Usage:
#   ./test-partner-api-audit.sh
#
# Override defaults via env vars:
#   PARTNER_API_URL   (default: http://localhost:8000)
#   DATA_MODEL        (default: smoke_test_audit) — passed as ?data_model=
#   N                 (default: 3) — number of calls to fire
#
# Examples:
#   PARTNER_API_URL=http://localhost:8001 ./test-partner-api-audit.sh
#   PARTNER_API_URL=https://partner-myrelease.trial.openg2p.org N=5 \
#     ./test-partner-api-audit.sh
# -----------------------------------------------------------------------------

set -eu

PARTNER_API_URL="${PARTNER_API_URL:-http://localhost:8000}"
DATA_MODEL="${DATA_MODEL:-smoke_test_audit}"
N="${N:-3}"

ENDPOINT="${PARTNER_API_URL%/}/partner/ingest_data?data_model=${DATA_MODEL}"

echo "============================================================"
echo "Partner API → Audit Manager smoke test"
echo "============================================================"
echo "Target:    ${ENDPOINT}"
echo "Calls:     ${N}"
echo

RUN_START="$(date -u +'%Y-%m-%dT%H:%M:%S')"
echo "Run start (UTC): ${RUN_START}"
echo "  → use this as the lower bound when querying audit_events:"
echo "    SELECT ... FROM audit_events"
echo "    WHERE source = '/openg2p/registry-partner-api'"
echo "      AND occurred_at >= '${RUN_START}'"
echo "    ORDER BY occurred_at DESC;"
echo

for i in $(seq 1 "${N}"); do
  REQ_ID="audit-smoke-$(date +%s)-${i}"
  echo "--- call ${i}/${N} — X-Request-ID: ${REQ_ID} ---"

  HTTP_STATUS=$(curl -sS -o /tmp/audit-smoke-resp.json -w '%{http_code}' \
    -X POST \
    -H 'Content-Type: application/json' \
    -H "X-Request-ID: ${REQ_ID}" \
    --data '{
      "transaction_id": "'"${REQ_ID}"'",
      "request_payload": {
        "note": "audit smoke test — payload intentionally minimal"
      }
    }' \
    "${ENDPOINT}")

  echo "  HTTP ${HTTP_STATUS}"
  echo "  Response (first 300 chars):"
  head -c 300 /tmp/audit-smoke-resp.json | sed 's/^/    /'
  echo
  echo
done

rm -f /tmp/audit-smoke-resp.json

echo "Done. Verify audit rows manually in the Audit Manager DB."
