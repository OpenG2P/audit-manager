#!/usr/bin/env bash
# =============================================================================
# OpenG2P Audit Manager — end-to-end smoke test
# =============================================================================
# Assumes `docker compose up` is running (from the repo root) and that the
# service is healthy. Sends each sample event to /v1/auditmanager/events,
# posts the batch file to /events/batch, triggers an expected 400, then
# queries Postgres to confirm every expected id landed in audit_events.
#
# Usage:
#   tests/smoke.sh                         # against http://localhost:8000
#   AUDIT_URL=http://host:8000 tests/smoke.sh
# =============================================================================
set -euo pipefail

# --- config -----------------------------------------------------------------
AUDIT_URL="${AUDIT_URL:-http://localhost:8000}"
COMPOSE_PG_SERVICE="${COMPOSE_PG_SERVICE:-postgres}"
COMPOSE_PG_USER="${COMPOSE_PG_USER:-postgres}"
COMPOSE_PG_DB="${COMPOSE_PG_DB:-auditmanager}"
SETTLE_SECONDS="${SETTLE_SECONDS:-4}"   # time for Kafka → Postgres flush

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SAMPLES_DIR="${SCRIPT_DIR}/sample-events"

# --- ANSI colors ------------------------------------------------------------
if [ -t 1 ]; then
  RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; OFF=$'\033[0m'
else
  RED=""; GREEN=""; YELLOW=""; BOLD=""; OFF=""
fi

PASS=0
FAIL=0

pass() { echo "${GREEN}  ✓${OFF} $*"; PASS=$((PASS+1)); }
fail() { echo "${RED}  ✗${OFF} $*"; FAIL=$((FAIL+1)); }
step() { echo; echo "${BOLD}==> $*${OFF}"; }

# --- pre-flight -------------------------------------------------------------
step "Pre-flight checks"

for cmd in curl docker jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "${RED}Required command not found: $cmd${OFF}" >&2
    exit 2
  fi
done
pass "tools present: curl, docker, jq"

http_code=$(curl -s -o /tmp/am-health.json -w '%{http_code}' \
  "${AUDIT_URL}/v1/auditmanager/health" || true)
if [ "$http_code" != "200" ]; then
  fail "service health check failed (got $http_code). Is \`docker compose up\` running?"
  cat /tmp/am-health.json 2>/dev/null || true
  exit 1
fi
pass "service healthy at ${AUDIT_URL}"

# --- send single events -----------------------------------------------------
step "POST single events"

expected_ids=()

for file in \
    "${SAMPLES_DIR}/01-login-success.json" \
    "${SAMPLES_DIR}/02-login-failed.json" \
    "${SAMPLES_DIR}/03-beneficiary-viewed.json" \
    "${SAMPLES_DIR}/04-beneficiary-updated.json" \
    "${SAMPLES_DIR}/05-payment-approved.json" \
    "${SAMPLES_DIR}/06-payment-reversed-system.json" \
    "${SAMPLES_DIR}/07-access-denied.json"; do
  id=$(jq -r .id "$file")
  type=$(jq -r .type "$file")
  code=$(curl -s -o /tmp/am-post.json -w '%{http_code}' \
      -H 'content-type: application/json' \
      -X POST "${AUDIT_URL}/v1/auditmanager/events" \
      --data-binary @"$file")
  if [ "$code" = "202" ]; then
    pass "POST ${type} (id=${id}) → 202"
    expected_ids+=("$id")
  else
    fail "POST ${type} (id=${id}) → ${code}"
    cat /tmp/am-post.json; echo
  fi
done

# --- send batch -------------------------------------------------------------
step "POST batch"

code=$(curl -s -o /tmp/am-batch.json -w '%{http_code}' \
    -H 'content-type: application/json' \
    -X POST "${AUDIT_URL}/v1/auditmanager/events/batch" \
    --data-binary @"${SAMPLES_DIR}/08-batch.json")
if [ "$code" = "202" ]; then
  accepted=$(jq -r '.response.accepted | length' /tmp/am-batch.json)
  pass "POST /events/batch → 202 (accepted ${accepted})"
  while IFS= read -r id; do expected_ids+=("$id"); done < <(jq -r '.response.accepted[]' /tmp/am-batch.json)
else
  fail "POST /events/batch → ${code}"
  cat /tmp/am-batch.json; echo
fi

# --- negative test: invalid event should be rejected ------------------------
step "POST invalid event (expect 4xx)"

code=$(curl -s -o /tmp/am-invalid.json -w '%{http_code}' \
    -H 'content-type: application/json' \
    -X POST "${AUDIT_URL}/v1/auditmanager/events" \
    --data-binary @"${SAMPLES_DIR}/99-invalid-missing-actor.json")
if [[ "$code" =~ ^4 ]]; then
  pass "invalid event rejected with ${code}"
else
  fail "invalid event returned ${code} (expected 4xx)"
  cat /tmp/am-invalid.json; echo
fi

# --- wait for Kafka → Postgres to settle ------------------------------------
step "Waiting ${SETTLE_SECONDS}s for Kafka → Postgres flush"
sleep "${SETTLE_SECONDS}"

# --- verify Postgres --------------------------------------------------------
step "Verify rows in Postgres"

# Build a Postgres IN-list safely.
ids_sql=""
for id in "${expected_ids[@]}"; do
  if [ -z "$ids_sql" ]; then
    ids_sql="'${id}'"
  else
    ids_sql="${ids_sql}, '${id}'"
  fi
done

query="SELECT id FROM audit_events WHERE id IN (${ids_sql}) ORDER BY id;"
present_ids=$(docker compose exec -T "${COMPOSE_PG_SERVICE}" \
    psql -U "${COMPOSE_PG_USER}" -d "${COMPOSE_PG_DB}" \
    -At -c "${query}" | tr -d '\r' || true)

echo "  Expected ${#expected_ids[@]} events; Postgres returned $(echo -n "$present_ids" | grep -c . || true) rows."

missing=()
for id in "${expected_ids[@]}"; do
  if grep -qx "$id" <<<"$present_ids"; then
    pass "row present: ${id}"
  else
    fail "row missing: ${id}"
    missing+=("$id")
  fi
done

# --- spot-check one row's shape --------------------------------------------
step "Spot-check one row: smoke-04-beneficiary-updated"

row_json=$(docker compose exec -T "${COMPOSE_PG_SERVICE}" \
    psql -U "${COMPOSE_PG_USER}" -d "${COMPOSE_PG_DB}" \
    -At -c "SELECT row_to_json(t) FROM (SELECT id, type, actor_id, resource_type, resource_id, outcome, details->'changes' AS changes FROM audit_events WHERE id = 'smoke-04-beneficiary-updated') t;")

if [ -n "$row_json" ]; then
  echo "  $(echo "$row_json" | jq -C .)"
  # Validate key fields
  if [ "$(echo "$row_json" | jq -r .type)" = "org.openg2p.beneficiary.updated" ] \
     && [ "$(echo "$row_json" | jq -r .actor_id)" = "u_4421" ] \
     && [ "$(echo "$row_json" | jq -r .resource_type)" = "beneficiary" ] \
     && [ "$(echo "$row_json" | jq -r .outcome)" = "success" ] \
     && [ "$(echo "$row_json" | jq -r '.changes | length')" = "2" ]; then
    pass "flat columns + details.changes preserved"
  else
    fail "row fields did not match expected values"
  fi
else
  fail "spot-check row not found"
fi

# --- idempotency: repost an event and confirm no duplicate ------------------
step "Idempotency: re-POST login-success, expect no duplicate row"

curl -s -o /dev/null \
  -H 'content-type: application/json' \
  -X POST "${AUDIT_URL}/v1/auditmanager/events" \
  --data-binary @"${SAMPLES_DIR}/01-login-success.json"
sleep "${SETTLE_SECONDS}"

count=$(docker compose exec -T "${COMPOSE_PG_SERVICE}" \
    psql -U "${COMPOSE_PG_USER}" -d "${COMPOSE_PG_DB}" \
    -At -c "SELECT COUNT(*) FROM audit_events WHERE id = 'smoke-01-login-success';" | tr -d '\r')
if [ "$count" = "1" ]; then
  pass "idempotent: still exactly 1 row for smoke-01-login-success"
else
  fail "expected 1 row, found ${count}"
fi

# --- summary ----------------------------------------------------------------
echo
echo "${BOLD}====================================${OFF}"
if [ "$FAIL" -eq 0 ]; then
  echo "${GREEN}${BOLD}Smoke test PASSED${OFF}   ($PASS checks)"
  exit 0
else
  echo "${RED}${BOLD}Smoke test FAILED${OFF}   (${FAIL} failures, ${PASS} passes)"
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "Missing event ids:"
    printf '  %s\n' "${missing[@]}"
  fi
  echo
  echo "Useful debugging:"
  echo "  docker compose logs audit-manager --tail=200"
  echo "  docker compose exec ${COMPOSE_PG_SERVICE} psql -U ${COMPOSE_PG_USER} -d ${COMPOSE_PG_DB} -c 'SELECT COUNT(*) FROM audit_events;'"
  exit 1
fi
