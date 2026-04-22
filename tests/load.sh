#!/usr/bin/env bash
# =============================================================================
# Simple concurrent-POST load generator for the Audit Manager.
#
# Sends N events total, with C concurrent workers, each sending uniquely-id'd
# login events. At the end, verifies that all N rows landed in Postgres.
#
# Usage:
#   tests/load.sh                                # 1000 events, 20 concurrent
#   N=5000 C=50 tests/load.sh
#   AUDIT_URL=http://host:8000 tests/load.sh
# =============================================================================
set -euo pipefail

N="${N:-1000}"
C="${C:-20}"
AUDIT_URL="${AUDIT_URL:-http://localhost:8000}"
COMPOSE_PG_SERVICE="${COMPOSE_PG_SERVICE:-postgres}"
COMPOSE_PG_USER="${COMPOSE_PG_USER:-postgres}"
COMPOSE_PG_DB="${COMPOSE_PG_DB:-auditmanager}"
RUN_ID="load-$(date -u +%Y%m%dT%H%M%SZ)"

echo "Sending $N events, $C concurrent workers, run_id=$RUN_ID"
start=$(date +%s)

post_one() {
  local i="$1"
  local id="${RUN_ID}-${i}"
  curl -s -o /dev/null -w "" \
    -H 'content-type: application/json' \
    -X POST "${AUDIT_URL}/v1/auditmanager/events" \
    -d "{
      \"specversion\": \"1.0\",
      \"id\": \"${id}\",
      \"source\": \"/openg2p/loadtest\",
      \"type\": \"org.openg2p.auth.login\",
      \"time\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
      \"data\": {
        \"actor\":   { \"type\": \"user\", \"id\": \"u_${i}\" },
        \"action\":  \"login\",
        \"outcome\": \"success\"
      }
    }"
}
export -f post_one
export AUDIT_URL RUN_ID

seq 1 "$N" | xargs -P "$C" -I {} bash -c 'post_one "$@"' _ {}

end=$(date +%s)
elapsed=$((end - start))
if [ "$elapsed" -eq 0 ]; then elapsed=1; fi
echo "Posted $N events in ${elapsed}s — $((N / elapsed)) req/s (HTTP-side)"

echo "Waiting 5s for Kafka → Postgres..."
sleep 5

count=$(docker compose exec -T "${COMPOSE_PG_SERVICE}" \
  psql -U "${COMPOSE_PG_USER}" -d "${COMPOSE_PG_DB}" -At \
  -c "SELECT COUNT(*) FROM audit_events WHERE id LIKE '${RUN_ID}-%';" | tr -d '\r')

echo "Postgres rows for this run: ${count} / ${N}"
if [ "$count" = "$N" ]; then
  echo "LOAD TEST PASSED"
  exit 0
else
  echo "LOAD TEST: ${N} sent, ${count} persisted — possible lag or drops" >&2
  echo "Try increasing the settle time or re-query Postgres after a few more seconds." >&2
  exit 1
fi
