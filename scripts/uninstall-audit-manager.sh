#!/usr/bin/env bash
#
# uninstall-audit-manager.sh
# --------------------------
# Cleanly uninstall an OpenG2P Audit Manager Helm release and every resource
# it touched — including the PostgreSQL database + role that live inside the
# commons-postgresql instance (not owned by audit-manager's Helm release, so
# `helm uninstall` leaves them) and, optionally, the Kafka topics created by
# the topic-init Helm hook (`openg2p.audit.events` and `openg2p.audit.dlq`).
#
# What it does, in order:
#   1. helm uninstall <release>        (audit-manager workloads, Service, Istio
#                                        VS, helm-owned secrets/configmaps)
#   2. Delete leftover Jobs + pods     (postgres-init + topic-init pin themselves
#                                        with hook-delete-policy)
#   3. Sweep any other leftover        (labels: app.kubernetes.io/instance)
#      Secrets / ConfigMaps
#   4. Drop Postgres DB + role         (via `kubectl exec` into commons-postgresql)
#   5. Optional: delete Kafka topics   (only with --delete-kafka-topics — calls
#                                        kafka-topics.sh inside commons-kafka pod)
#   6. Delete PVCs by label            (audit-manager has none today; kept for
#                                        parity with sibling services)
#   7. Delete PVs still Released       (typically reclaimPolicy=Retain volumes)
#
# Requires: kubectl (cluster admin), helm, bash 4+, jq.
#
# USAGE:
#   ./uninstall-audit-manager.sh \
#       --namespace <ns> \
#       [--release <name>]              (default: audit-manager)
#       [--postgres-release <name>]     (default: commons-postgresql)
#       [--postgres-namespace <ns>]     (default: same as --namespace)
#       [--kafka-release <name>]        (default: commons-kafka)
#       [--kafka-namespace <ns>]        (default: same as --namespace)
#       [--audit-topic <name>]          (default: openg2p.audit.events)
#       [--audit-dlq-topic <name>]      (default: openg2p.audit.dlq)
#       [--delete-kafka-topics]         (also drop the two Kafka topics)
#       [--keep-pvs]                    (delete PVCs but not PVs)
#       [--dry-run]                     (print actions, change nothing)
#       [--yes]                         (skip interactive confirmation)
#
# EXAMPLES:
#   # Dry run first — no changes made:
#   ./uninstall-audit-manager.sh --namespace trial --dry-run
#
#   # For real, with confirmation prompt:
#   ./uninstall-audit-manager.sh --namespace trial
#
#   # Full blast including Kafka topics (non-interactive, CI):
#   ./uninstall-audit-manager.sh --namespace trial --delete-kafka-topics --yes

set -euo pipefail

# ---------- defaults ----------
RELEASE="audit-manager"
NAMESPACE=""
POSTGRES_RELEASE="commons-postgresql"
POSTGRES_NAMESPACE=""
KAFKA_RELEASE="commons-kafka"
KAFKA_NAMESPACE=""
AUDIT_TOPIC="openg2p.audit.events"
AUDIT_DLQ_TOPIC="openg2p.audit.dlq"
DELETE_KAFKA_TOPICS=false
KEEP_PVS=false
DRY_RUN=false
ASSUME_YES=false

# ---------- cli ----------
usage() { sed -n '2,50p' "$0"; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)              RELEASE="$2";              shift 2 ;;
    --namespace|-n)         NAMESPACE="$2";            shift 2 ;;
    --postgres-release)     POSTGRES_RELEASE="$2";     shift 2 ;;
    --postgres-namespace)   POSTGRES_NAMESPACE="$2";   shift 2 ;;
    --kafka-release)        KAFKA_RELEASE="$2";        shift 2 ;;
    --kafka-namespace)      KAFKA_NAMESPACE="$2";      shift 2 ;;
    --audit-topic)          AUDIT_TOPIC="$2";          shift 2 ;;
    --audit-dlq-topic)      AUDIT_DLQ_TOPIC="$2";      shift 2 ;;
    --delete-kafka-topics)  DELETE_KAFKA_TOPICS=true;  shift ;;
    --keep-pvs)             KEEP_PVS=true;             shift ;;
    --dry-run)              DRY_RUN=true;              shift ;;
    --yes|-y)               ASSUME_YES=true;           shift ;;
    -h|--help)              usage ;;
    *) echo "Unknown argument: $1"; usage ;;
  esac
done

[[ -z "$NAMESPACE" ]] && { echo "ERROR: --namespace is required"; exit 1; }
[[ -z "$POSTGRES_NAMESPACE" ]] && POSTGRES_NAMESPACE="$NAMESPACE"
[[ -z "$KAFKA_NAMESPACE" ]] && KAFKA_NAMESPACE="$NAMESPACE"

# ---------- derived: DB / user names (templated exactly like values.yaml) ----------
# helm/openg2p-audit-manager/values.yaml:
#   auditManagerDB:     '{{ printf "%s" .Release.Name | replace "-" "_" }}'
#   auditManagerDBUser: '{{ printf "%s_user" .Release.Name | replace "-" "_" }}'
RELEASE_UNDERSCORED="${RELEASE//-/_}"
AUDIT_DB="${RELEASE_UNDERSCORED}"
AUDIT_USER="${RELEASE_UNDERSCORED}_user"

# ---------- helpers ----------
_red()   { printf "\033[31m%s\033[0m\n" "$*"; }
_green() { printf "\033[32m%s\033[0m\n" "$*"; }
_yellow(){ printf "\033[33m%s\033[0m\n" "$*"; }
_blue()  { printf "\033[34m%s\033[0m\n" "$*"; }

run() {
  # Print + execute, or just print if --dry-run. Never aborts on non-zero
  # exit — cleanup must be idempotent. Already-gone resources just produce
  # a notice and we move on.
  echo "  \$ $*"
  if [[ "$DRY_RUN" == false ]]; then
    eval "$@" || _yellow "  (command returned non-zero — continuing)"
  fi
}

kexec_psql() {
  # Run SQL as postgres superuser inside the commons-postgresql pod.
  local sql="$1"
  local cmd=(kubectl exec -n "$POSTGRES_NAMESPACE" "$PG_POD" -c postgresql -- \
             bash -c "PGPASSWORD=\"\$POSTGRES_PASSWORD\" psql -U postgres -v ON_ERROR_STOP=0 -c \"$sql\"")
  echo "  \$ psql -U postgres -c \"$sql\""
  if [[ "$DRY_RUN" == false ]]; then
    "${cmd[@]}" || _yellow "  (psql returned non-zero — continuing)"
  fi
}

kexec_kafka_topics() {
  # Run kafka-topics.sh inside the Kafka pod. Auto-detects the binary path:
  # bitnami images have /opt/bitnami/kafka/bin/, apache/kafka has /opt/kafka/bin/.
  local args=("$@")
  echo "  \$ kafka-topics.sh ${args[*]}"
  if [[ "$DRY_RUN" == false ]]; then
    kubectl exec -n "$KAFKA_NAMESPACE" "$KAFKA_POD" -- \
      bash -c "
        for bin in /opt/bitnami/kafka/bin/kafka-topics.sh /opt/kafka/bin/kafka-topics.sh; do
          if [ -x \"\$bin\" ]; then
            \"\$bin\" $(printf '%q ' "${args[@]}")
            exit \$?
          fi
        done
        echo 'kafka-topics.sh not found in either /opt/bitnami/kafka/bin or /opt/kafka/bin' >&2
        exit 127
      " || _yellow "  (kafka-topics.sh returned non-zero — continuing)"
  fi
}

# ---------- pre-flight ----------
_blue "==> Pre-flight checks"

command -v kubectl >/dev/null || { _red "kubectl not found"; exit 1; }
command -v helm    >/dev/null || { _red "helm not found";    exit 1; }
command -v jq      >/dev/null || { _red "jq not found";      exit 1; }

if kubectl get ns "$NAMESPACE" >/dev/null 2>&1; then
  NAMESPACE_EXISTS=true
  _green "  Namespace '$NAMESPACE' exists"
else
  NAMESPACE_EXISTS=false
  _yellow "  Namespace '$NAMESPACE' does not exist — namespace-scoped cleanup will be skipped"
fi

# Locate commons-postgresql pod. Bitnami's chart uses these labels.
PG_POD=""
if kubectl get ns "$POSTGRES_NAMESPACE" >/dev/null 2>&1; then
  PG_POD=$(kubectl get pod -n "$POSTGRES_NAMESPACE" \
    -l "app.kubernetes.io/instance=$POSTGRES_RELEASE,app.kubernetes.io/name=postgresql" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  if [[ -z "$PG_POD" ]] && kubectl get pod -n "$POSTGRES_NAMESPACE" "${POSTGRES_RELEASE}-0" >/dev/null 2>&1; then
    PG_POD="${POSTGRES_RELEASE}-0"
  fi
fi

if [[ -z "$PG_POD" ]]; then
  PG_POD_FOUND=false
  _yellow "  commons-postgresql pod not found — DB / role drop step will be skipped"
else
  PG_POD_FOUND=true
  _green "  Found Postgres pod: $PG_POD (namespace: $POSTGRES_NAMESPACE)"
fi

# Locate Kafka pod (only needed if --delete-kafka-topics).
KAFKA_POD=""
KAFKA_POD_FOUND=false
if [[ "$DELETE_KAFKA_TOPICS" == true ]] && kubectl get ns "$KAFKA_NAMESPACE" >/dev/null 2>&1; then
  # Try Bitnami StatefulSet labels first (controller pod in KRaft mode).
  KAFKA_POD=$(kubectl get pod -n "$KAFKA_NAMESPACE" \
    -l "app.kubernetes.io/instance=$KAFKA_RELEASE,app.kubernetes.io/name=kafka" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  # Fallbacks for various chart layouts
  if [[ -z "$KAFKA_POD" ]] && kubectl get pod -n "$KAFKA_NAMESPACE" "${KAFKA_RELEASE}-controller-0" >/dev/null 2>&1; then
    KAFKA_POD="${KAFKA_RELEASE}-controller-0"
  fi
  if [[ -z "$KAFKA_POD" ]] && kubectl get pod -n "$KAFKA_NAMESPACE" "${KAFKA_RELEASE}-0" >/dev/null 2>&1; then
    KAFKA_POD="${KAFKA_RELEASE}-0"
  fi

  if [[ -n "$KAFKA_POD" ]]; then
    KAFKA_POD_FOUND=true
    _green "  Found Kafka pod: $KAFKA_POD (namespace: $KAFKA_NAMESPACE)"
  else
    _yellow "  Kafka pod not found — Kafka topic delete step will be skipped"
  fi
fi

if helm -n "$NAMESPACE" status "$RELEASE" >/dev/null 2>&1; then
  _green "  Helm release '$RELEASE' found in namespace '$NAMESPACE'"
  HELM_RELEASE_EXISTS=true
else
  _yellow "  Helm release '$RELEASE' not found — will skip helm uninstall step"
  HELM_RELEASE_EXISTS=false
fi

# ---------- blast radius ----------
_blue "==> Resources to be deleted"

echo
echo "Helm release:       $RELEASE (namespace: $NAMESPACE)"
echo "Postgres database:  $AUDIT_DB"
echo "Postgres role:      $AUDIT_USER"
echo "Postgres pod:       ${PG_POD:-<not found — will skip DB drop>} ($POSTGRES_NAMESPACE)"
if [[ "$DELETE_KAFKA_TOPICS" == true ]]; then
  echo "Kafka topics:       $AUDIT_TOPIC, $AUDIT_DLQ_TOPIC (cluster: $KAFKA_RELEASE / $KAFKA_NAMESPACE)"
fi
echo

if [[ "$NAMESPACE_EXISTS" == true ]]; then
  echo "Jobs (label app.kubernetes.io/instance=$RELEASE):"
  kubectl -n "$NAMESPACE" get job -l "app.kubernetes.io/instance=$RELEASE" \
    --no-headers 2>/dev/null | awk '{print "  - " $1}' || echo "  (none)"

  echo "Secrets (label app.kubernetes.io/instance=$RELEASE):"
  kubectl -n "$NAMESPACE" get secret -l "app.kubernetes.io/instance=$RELEASE" \
    --no-headers 2>/dev/null | awk '{print "  - " $1}' || echo "  (none)"

  echo "ConfigMaps (label app.kubernetes.io/instance=$RELEASE):"
  kubectl -n "$NAMESPACE" get configmap -l "app.kubernetes.io/instance=$RELEASE" \
    --no-headers 2>/dev/null | awk '{print "  - " $1}' || echo "  (none)"

  echo "PVCs (label app.kubernetes.io/instance=$RELEASE):"
  kubectl -n "$NAMESPACE" get pvc -l "app.kubernetes.io/instance=$RELEASE" \
    --no-headers 2>/dev/null | awk '{print "  - " $1}' || echo "  (none)"
else
  echo "(namespace '$NAMESPACE' does not exist — no namespace-scoped resources to preview)"
fi

if [[ "$KEEP_PVS" == false ]]; then
  echo "PVs (bound to above PVCs / labeled with release):"
  kubectl get pv -o json 2>/dev/null | \
    jq -r --arg ns "$NAMESPACE" --arg rel "$RELEASE" \
      '.items[] | select((.spec.claimRef.namespace==$ns) or (.metadata.labels["app.kubernetes.io/instance"]==$rel)) | "  - " + .metadata.name + " (" + .status.phase + ")"' \
    2>/dev/null | sort -u || true
fi
echo

# ---------- confirmation ----------
if [[ "$DRY_RUN" == true ]]; then
  _yellow "DRY-RUN: no changes will be made."
fi

if [[ "$ASSUME_YES" == false && "$DRY_RUN" == false ]]; then
  _red "This is destructive. Type the release name ('$RELEASE') to confirm:"
  read -r CONFIRM
  if [[ "$CONFIRM" != "$RELEASE" ]]; then
    _red "Confirmation did not match. Aborting."
    exit 1
  fi
fi

# ========== STEP 1: helm uninstall ==========
_blue "==> [1/7] Helm uninstall"
if [[ "$HELM_RELEASE_EXISTS" == true ]]; then
  run "helm uninstall '$RELEASE' -n '$NAMESPACE' --wait --timeout 5m || true"
else
  echo "  (skipped — release not present)"
fi

# ========== STEP 2: leftover Jobs ==========
# postgres-init and topic-init hook Jobs pin themselves with
# `hook-delete-policy: before-hook-creation,hook-succeeded` and may
# remain (e.g. if they failed). Purge them explicitly BEFORE dropping
# the DB so their Pods close Postgres connections cleanly.
_blue "==> [2/7] Delete leftover Jobs and Pods"
if [[ "$NAMESPACE_EXISTS" == true ]]; then
  run "kubectl -n '$NAMESPACE' delete job -l 'app.kubernetes.io/instance=$RELEASE' --ignore-not-found --wait=true --timeout=2m"
  run "kubectl -n '$NAMESPACE' delete pod -l 'app.kubernetes.io/instance=$RELEASE' --ignore-not-found --field-selector=status.phase!=Running"
else
  echo "  (skipped — namespace '$NAMESPACE' not present)"
fi

# ========== STEP 3: sweep leftover Secrets & ConfigMaps ==========
_blue "==> [3/7] Sweep leftover Secrets / ConfigMaps"
if [[ "$NAMESPACE_EXISTS" == true ]]; then
  run "kubectl -n '$NAMESPACE' delete secret    -l 'app.kubernetes.io/instance=$RELEASE' --ignore-not-found"
  run "kubectl -n '$NAMESPACE' delete configmap -l 'app.kubernetes.io/instance=$RELEASE' --ignore-not-found"
else
  echo "  (skipped — namespace '$NAMESPACE' not present)"
fi

# ========== STEP 4: drop Postgres DB + role ==========
_blue "==> [4/7] Drop Postgres database and role"
if [[ "$PG_POD_FOUND" == true ]]; then
  echo "  - Database: $AUDIT_DB"
  kexec_psql "REVOKE CONNECT ON DATABASE \\\"$AUDIT_DB\\\" FROM PUBLIC;"
  kexec_psql "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '$AUDIT_DB' AND pid <> pg_backend_pid();"
  kexec_psql "DROP DATABASE IF EXISTS \\\"$AUDIT_DB\\\";"

  echo "  - Role: $AUDIT_USER"
  # Reassign/drop stray ownership outside the dropped DB.
  kexec_psql "REASSIGN OWNED BY \\\"$AUDIT_USER\\\" TO postgres;"
  kexec_psql "DROP OWNED BY \\\"$AUDIT_USER\\\";"
  kexec_psql "DROP ROLE IF EXISTS \\\"$AUDIT_USER\\\";"
else
  echo "  (skipped — commons-postgresql pod not reachable; if Postgres is already gone, DB is gone too)"
fi

# ========== STEP 5: Kafka topics (optional) ==========
_blue "==> [5/7] Delete Kafka topics"
if [[ "$DELETE_KAFKA_TOPICS" == false ]]; then
  _yellow "  (skipped — pass --delete-kafka-topics to also delete '$AUDIT_TOPIC' and '$AUDIT_DLQ_TOPIC')"
elif [[ "$KAFKA_POD_FOUND" != true ]]; then
  echo "  (skipped — Kafka pod not reachable)"
else
  for topic in "$AUDIT_TOPIC" "$AUDIT_DLQ_TOPIC"; do
    echo "  - Topic: $topic"
    kexec_kafka_topics --bootstrap-server localhost:9092 --delete --topic "$topic"
  done
fi

# ========== STEP 6: PVCs ==========
_blue "==> [6/7] Delete PVCs"
if [[ "$NAMESPACE_EXISTS" == true ]]; then
  run "kubectl -n '$NAMESPACE' delete pvc -l 'app.kubernetes.io/instance=$RELEASE' --ignore-not-found"
else
  echo "  (skipped — namespace '$NAMESPACE' not present)"
fi

# ========== STEP 7: PVs ==========
_blue "==> [7/7] Delete PVs"
if [[ "$KEEP_PVS" == true ]]; then
  _yellow "  (skipped — --keep-pvs)"
else
  pv_list=$(kubectl get pv -o json 2>/dev/null | \
    jq -r --arg ns "$NAMESPACE" \
      '.items[] | select(.spec.claimRef.namespace==$ns) | select(.status.phase=="Released" or .status.phase=="Failed") | .metadata.name' \
    2>/dev/null || true)
  pv_labeled=$(kubectl get pv -l "app.kubernetes.io/instance=$RELEASE" \
                 -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)
  pv_all=$(echo "$pv_list $pv_labeled" | tr ' ' '\n' | sort -u | tr '\n' ' ' | sed 's/^ *//;s/ *$//')

  if [[ -z "$pv_all" ]]; then
    echo "  (no PVs to delete)"
  else
    for pv in $pv_all; do
      run "kubectl delete pv '$pv' --ignore-not-found"
    done
  fi
fi

echo
_green "==> Done."
if [[ "$DRY_RUN" == true ]]; then
  _yellow "    (dry-run — nothing was actually changed)"
fi
