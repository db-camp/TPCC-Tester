#!/usr/bin/env bash
set -euo pipefail

# This script owns process and filesystem lifecycle only. The Rust tester owns
# the final2026 workload schedule, warmup, measurement windows, deadlines, and
# semantic checks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
DEFAULT_RMDB_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
DEFAULT_TPCC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd -P)"
DIAGNOSTIC_METRICS_HELPER="${SCRIPT_DIR}/diagnostic_metrics.py"
RESOURCE_HELPER="${RMDB_TPCC_RESOURCE_HELPER_OVERRIDE:-${SCRIPT_DIR}/resource_sampler.py}"

MODE="all"
LABEL="final2026"
DB_NAME="tpcc_final2026"
RMDB_DIR="${RMDB_DIR_OVERRIDE:-${DEFAULT_RMDB_DIR}}"
TPCC_DIR="${TPCC_TESTER_DIR:-${DEFAULT_TPCC_DIR}}"
RECORD_ROOT=""
BUILD_DIR="build-perf"
HOST="127.0.0.1"
PORT="8765"
PROFILE="final2026"
SEED="2026"
SEED_CALLER_SUPPLIED=0
READY_TIMEOUT_SECONDS="90"
SKIP_BUILD=0
PLAN_ONLY=0
INIT_BEFORE_RUN=0
CLEAN_DB_ON_EXIT="auto"
DIAGNOSTICS_REQUESTED=0
ALLOW_DEVIATION=0
SCALE=""
CLIENTS=""
WARMUP_SECONDS=""
WINDOW_SECONDS=""
SERVER_BIN_OVERRIDE=""
TPCC_BIN_OVERRIDE=""
STATE_DIR_OVERRIDE=""

SERVER_PID=""
SERVER_PGID=""
SERVER_IDENTITY=""
PROBE_PID=""
TRACE_PID=""
TRACE_EXIT_STATUS=""
RESOURCE_PID=""
RESOURCE_IDENTITY=""
RESOURCE_PARENT_PID=""
RESOURCE_SEGMENT_PATH=""
RESOURCE_GENERATION=0
RESOURCE_DATABASE_IDENTITY=""
RESOURCE_STATUS="pending"
RESOURCE_FINALIZED=0
TESTER_RESOURCE_TIMELINE=""
STOPPING_SERVER=0
SERVER_LOG=""
RESULT_DIR=""
RUN_TEMP_DIR=""
CSV_DIR=""
LOAD_DIR=""
STATE_DIR=""
RUN_MARKER=""
DB_MARKER=""
OWNER_TOKEN=""
PROCESS_OWNER_TOKEN=""
DB_PATH=""
DATASET_RUN_ID=""
DB_OWNED=0
WORKFLOW_SUCCEEDED=0
WORKFLOW_STATUS="running"
MANIFEST_READY=0
RMDB_SHA="unavailable"
TPCC_SHA="unavailable"
PHASE_SETUP="not_applicable"
PHASE_RANK="not_applicable"
PHASE_ONLINE="not_applicable"
PHASE_CRASH_RESTART="not_applicable"
PHASE_RECOVERY="not_applicable"
PHASE_DIAGNOSTICS="not_requested"

PUBLIC_SCALE=50
PUBLIC_CLIENTS=32
PUBLIC_WARMUP_SECONDS=30
PUBLIC_WINDOWS=3
PUBLIC_WINDOW_SECONDS=150
PUBLIC_READY_TIMEOUT_SECONDS=90
DIAGNOSTIC_WARMUP_SECONDS=10
DIAGNOSTIC_OBSERVATION_SECONDS=60
RESOURCE_INTERVAL_MS=1000

usage() {
  cat <<'EOF'
Usage:
  run_workflow.sh [options]

Lifecycle modes:
  --mode all         Create/load, rank, online-check, SIGKILL, restart, recovery-check
  --mode init        Create/load a new database and retain it
  --mode rank        Rank an existing database and run the online checks
  --mode recovery    Start an existing database and run the recovery checks
  --mode tools       Record available build/runtime tools only

Official final2026 options:
  --profile final2026
  --seed <u64>
  --host <numeric-ip>
  --port <port>
  --db-name <safe-single-component>

Paths/build:
  --target-dir <RMDB-root>
  --tpcc-dir <TPCC-Tester-root>
  --record-root <result-root>
  --state-dir <run-state-dir>
  --build-dir <relative-single-component>
  --server-bin <path>
  --tpcc-bin <path>
  --skip-build

Lifecycle:
  --init-db                    Initialize before --mode rank
  --ready-timeout-seconds <n>  Public default: 90; other values are deviations
  --diagnostics                Compatibility flag; --mode all automatically
                               runs non-ranked diagnostics after every gate
  --keep-db-artifacts
  --clean-db-on-exit
  --label <safe-single-component>
  --plan-only | --dry-run

Short local deviations (never enabled implicitly):
  --allow-deviation
  --scale <n>
  --clients <n>               --threads is accepted as an alias
  --warmup-seconds <n>
  --window-seconds <n>        --measure-seconds is accepted as an alias

The shell deliberately has no transaction-mix, transaction-count, output-file,
or per-window timeout controls. Those are part of the Rust final2026 contract.
EOF
}

log() {
  printf '[tpcc-workflow] %s\n' "$*"
}

warn() {
  printf '[tpcc-workflow] WARN: %s\n' "$*" >&2
}

die() {
  printf '[tpcc-workflow] ERROR: %s\n' "$*" >&2
  exit 1
}

need_value() {
  local option="$1"
  local value="${2-}"
  [[ -n "${value}" ]] || die "${option} requires a value"
}

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

validate_positive_integer() {
  local option="$1"
  local value="$2"
  is_uint "${value}" || die "${option} must be a positive integer"
  (( 10#${value} > 0 )) || die "${option} must be a positive integer"
}

validate_nonnegative_integer() {
  local option="$1"
  local value="$2"
  is_uint "${value}" || die "${option} must be a non-negative integer"
}

validate_component() {
  local option="$1"
  local value="$2"
  [[ -n "${value}" ]] || die "${option} must not be empty"
  [[ "${value}" != "." && "${value}" != ".." ]] \
    || die "${option} must be a safe single path component"
  [[ "${value}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,79}$ ]] \
    || die "${option} must be a safe single path component"
}

canonical_existing_dir() {
  local path="$1"
  (cd "${path}" 2>/dev/null && pwd -P)
}

canonical_existing_file() {
  local path="$1"
  local directory=""
  local filename=""
  directory="$(dirname "${path}")"
  filename="$(basename "${path}")"
  directory="$(canonical_existing_dir "${directory}")" || return 1
  [[ -f "${directory}/${filename}" ]] || return 1
  printf '%s/%s\n' "${directory}" "${filename}"
}

git_sha_or_unavailable() {
  local directory="$1"
  local sha=""
  if command -v git >/dev/null 2>&1; then
    sha="$(git -C "${directory}" rev-parse --verify HEAD 2>/dev/null || true)"
  fi
  if [[ "${sha}" =~ ^[0-9a-fA-F]{40}$ ]]; then
    printf '%s\n' "${sha}"
  else
    printf '%s\n' "unavailable"
  fi
}

normalize_numeric_host() {
  python3 - "$1" <<'PY'
import ipaddress
import sys

try:
    address = ipaddress.ip_address(sys.argv[1])
except ValueError:
    raise SystemExit(1)
print(f"{address.version}\t{address.compressed}")
PY
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)
      need_value "$1" "${2-}"; MODE="$2"; shift 2 ;;
    --label)
      need_value "$1" "${2-}"; LABEL="$2"; shift 2 ;;
    --db-name)
      need_value "$1" "${2-}"; DB_NAME="$2"; shift 2 ;;
    --target-dir)
      need_value "$1" "${2-}"; RMDB_DIR="$2"; shift 2 ;;
    --tpcc-dir)
      need_value "$1" "${2-}"; TPCC_DIR="$2"; shift 2 ;;
    --record-root)
      need_value "$1" "${2-}"; RECORD_ROOT="$2"; shift 2 ;;
    --state-dir)
      need_value "$1" "${2-}"; STATE_DIR_OVERRIDE="$2"; shift 2 ;;
    --build-dir)
      need_value "$1" "${2-}"; BUILD_DIR="$2"; shift 2 ;;
    --host)
      need_value "$1" "${2-}"; HOST="$2"; shift 2 ;;
    --port)
      need_value "$1" "${2-}"; PORT="$2"; shift 2 ;;
    --profile)
      need_value "$1" "${2-}"; PROFILE="$2"; shift 2 ;;
    --seed)
      need_value "$1" "${2-}"; SEED="$2"; SEED_CALLER_SUPPLIED=1; shift 2 ;;
    --ready-timeout-seconds|--server-start-timeout-seconds)
      need_value "$1" "${2-}"; READY_TIMEOUT_SECONDS="$2"; shift 2 ;;
    --server-bin)
      need_value "$1" "${2-}"; SERVER_BIN_OVERRIDE="$2"; shift 2 ;;
    --tpcc-bin)
      need_value "$1" "${2-}"; TPCC_BIN_OVERRIDE="$2"; shift 2 ;;
    --skip-build)
      SKIP_BUILD=1; shift ;;
    --init-db)
      INIT_BEFORE_RUN=1; shift ;;
    --diagnostics)
      DIAGNOSTICS_REQUESTED=1; shift ;;
    --keep-db-artifacts)
      CLEAN_DB_ON_EXIT=0; shift ;;
    --clean-db-on-exit)
      CLEAN_DB_ON_EXIT=1; shift ;;
    --plan-only|--dry-run)
      PLAN_ONLY=1; shift ;;
    --allow-deviation)
      ALLOW_DEVIATION=1; shift ;;
    --scale)
      need_value "$1" "${2-}"; SCALE="$2"; shift 2 ;;
    --clients|--threads)
      need_value "$1" "${2-}"; CLIENTS="$2"; shift 2 ;;
    --warmup-seconds)
      need_value "$1" "${2-}"; WARMUP_SECONDS="$2"; shift 2 ;;
    --window-seconds|--measure-seconds)
      need_value "$1" "${2-}"; WINDOW_SECONDS="$2"; shift 2 ;;
    --check)
      # final2026 rank/all always run the prescribed checks.
      shift ;;
    --help|-h)
      usage; exit 0 ;;
    *)
      die "unknown or obsolete option: $1" ;;
  esac
done

if [[ "${MODE}" == "benchmark" ]]; then
  MODE="rank"
fi
case "${MODE}" in
  all|init|rank|recovery|tools) ;;
  *) die "unsupported mode: ${MODE}" ;;
esac
if [[ "${DIAGNOSTICS_REQUESTED}" == "1" && "${MODE}" != "all" ]]; then
  die "--diagnostics requires --mode all so rank, online, and recovery gates complete first"
fi
validate_component "--db-name" "${DB_NAME}"
validate_component "--label" "${LABEL}"
validate_component "--build-dir" "${BUILD_DIR}"
[[ "${PROFILE}" == "final2026" ]] || die "only --profile final2026 is supported"
validate_nonnegative_integer "--seed" "${SEED}"
validate_positive_integer "--port" "${PORT}"
(( 10#${PORT} <= 65535 )) || die "--port must be at most 65535"
validate_positive_integer "--ready-timeout-seconds" "${READY_TIMEOUT_SECONDS}"
HOST_INFO="$(normalize_numeric_host "${HOST}")" \
  || die "--host must be a numeric IPv4 or IPv6 address"
IFS=$'\t' read -r HOST_VERSION HOST <<<"${HOST_INFO}"

if [[ -n "${SCALE}${CLIENTS}${WARMUP_SECONDS}${WINDOW_SECONDS}" \
  || "${READY_TIMEOUT_SECONDS}" != "${PUBLIC_READY_TIMEOUT_SECONDS}" ]] \
  && [[ "${ALLOW_DEVIATION}" != "1" ]]; then
  die "local sizing/timing/readiness overrides require --allow-deviation"
fi
if [[ "${ALLOW_DEVIATION}" == "1" ]]; then
  [[ -z "${SCALE}" ]] || validate_positive_integer "--scale" "${SCALE}"
  [[ -z "${CLIENTS}" ]] || validate_positive_integer "--clients" "${CLIENTS}"
  [[ -z "${WARMUP_SECONDS}" ]] \
    || validate_nonnegative_integer "--warmup-seconds" "${WARMUP_SECONDS}"
  [[ -z "${WINDOW_SECONDS}" ]] \
    || validate_positive_integer "--window-seconds" "${WINDOW_SECONDS}"
fi

EFFECTIVE_SCALE="${SCALE:-${PUBLIC_SCALE}}"
EFFECTIVE_CLIENTS="${CLIENTS:-${PUBLIC_CLIENTS}}"
EFFECTIVE_WARMUP_SECONDS="${WARMUP_SECONDS:-${PUBLIC_WARMUP_SECONDS}}"
EFFECTIVE_WINDOW_SECONDS="${WINDOW_SECONDS:-${PUBLIC_WINDOW_SECONDS}}"
RANKED_CONFIGURATION=1
if [[ "${EFFECTIVE_SCALE}" != "${PUBLIC_SCALE}" \
  || "${EFFECTIVE_CLIENTS}" != "${PUBLIC_CLIENTS}" \
  || "${EFFECTIVE_WARMUP_SECONDS}" != "${PUBLIC_WARMUP_SECONDS}" \
  || "${EFFECTIVE_WINDOW_SECONDS}" != "${PUBLIC_WINDOW_SECONDS}" \
  || "${READY_TIMEOUT_SECONDS}" != "${PUBLIC_READY_TIMEOUT_SECONDS}" ]]; then
  RANKED_CONFIGURATION=0
fi
if [[ "${RANKED_CONFIGURATION}" == "1" ]]; then
  CONFORMANCE="public_spec_aligned"
else
  CONFORMANCE="non_ranked_deviation"
fi

RMDB_DIR="$(canonical_existing_dir "${RMDB_DIR}")" \
  || die "RMDB root does not exist: ${RMDB_DIR}"
TPCC_DIR="$(canonical_existing_dir "${TPCC_DIR}")" \
  || die "TPCC-Tester root does not exist: ${TPCC_DIR}"
[[ "${RMDB_DIR}" != "/" ]] || die "refusing to use filesystem root as RMDB root"
if [[ -n "${SERVER_BIN_OVERRIDE}" ]]; then
  SERVER_BIN_OVERRIDE="$(canonical_existing_file "${SERVER_BIN_OVERRIDE}")" \
    || die "--server-bin does not name an existing regular file"
fi
if [[ -n "${TPCC_BIN_OVERRIDE}" ]]; then
  TPCC_BIN_OVERRIDE="$(canonical_existing_file "${TPCC_BIN_OVERRIDE}")" \
    || die "--tpcc-bin does not name an existing regular file"
fi
if [[ -n "${STATE_DIR_OVERRIDE}" ]]; then
  STATE_DIR_OVERRIDE="$(canonical_existing_dir "${STATE_DIR_OVERRIDE}")" \
    || die "--state-dir must name an existing real directory"
fi
RMDB_SHA="$(git_sha_or_unavailable "${RMDB_DIR}")"
TPCC_SHA="$(git_sha_or_unavailable "${TPCC_DIR}")"

if [[ -z "${RECORD_ROOT}" ]]; then
  RECORD_ROOT="${RMDB_DIR}/performance_test_record"
fi

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)_$$"
RESULT_DIR="${RECORD_ROOT}/${RUN_ID}_${LABEL}"
RUN_TEMP_DIR="${RMDB_DIR}/.tpcc-workflow/${RUN_ID}"
CSV_DIR="${RUN_TEMP_DIR}/csv"
LOAD_DIR="../.tpcc-workflow/${RUN_ID}/csv"
RUN_MARKER="${RUN_TEMP_DIR}/.owner"
DB_PATH="${RMDB_DIR}/${DB_NAME}"
DB_MARKER="${DB_PATH}/.tpcc-workflow-owner"
OWNER_TOKEN="tpcc-final2026:${RUN_ID}:${DB_PATH}"
PROCESS_OWNER_TOKEN="tpcc-process:${RUN_ID}:$$"
if [[ -n "${STATE_DIR_OVERRIDE}" ]]; then
  STATE_DIR="${STATE_DIR_OVERRIDE}"
else
  STATE_DIR="${RESULT_DIR}/state"
fi
if [[ "${MODE}" == "recovery" ]] \
  || { [[ "${MODE}" == "rank" ]] && [[ "${INIT_BEFORE_RUN}" != "1" ]]; }; then
  [[ -n "${STATE_DIR_OVERRIDE}" ]] \
    || die "--mode ${MODE} on an existing database requires --state-dir from its setup run"
fi
DATASET_RUN_ID="${RUN_ID}"
if [[ "${MODE}" == "recovery" ]] \
  || { [[ "${MODE}" == "rank" ]] && [[ "${INIT_BEFORE_RUN}" != "1" ]]; }; then
  DATASET_FILE="${STATE_DIR}/dataset.state"
  [[ -f "${DATASET_FILE}" && ! -L "${DATASET_FILE}" ]] \
    || die "existing state-dir must contain a real dataset.state file"
  DATASET_RUN_ID="$(sed -n 's/^run_id=//p' "${DATASET_FILE}")"
  if [[ -z "${DATASET_RUN_ID}" || "${DATASET_RUN_ID}" == *$'\n'* \
    || ${#DATASET_RUN_ID} -gt 120 ]]; then
    die "dataset.state must contain exactly one safe run_id"
  fi
  case "${DATASET_RUN_ID}" in
    *[!A-Za-z0-9._:-]*)
      die "dataset.state contains an unsafe run_id" ;;
  esac
fi

if [[ "${CLEAN_DB_ON_EXIT}" == "auto" ]]; then
  if [[ "${MODE}" == "all" ]]; then
    CLEAN_DB_ON_EXIT=1
  else
    CLEAN_DB_ON_EXIT=0
  fi
fi

case "${MODE}" in
  all)
    PHASE_SETUP="pending"
    PHASE_RANK="pending"
    PHASE_ONLINE="pending"
    PHASE_CRASH_RESTART="pending"
    PHASE_RECOVERY="pending"
    ;;
  init)
    PHASE_SETUP="pending"
    ;;
  rank)
    if [[ "${INIT_BEFORE_RUN}" == "1" ]]; then
      PHASE_SETUP="pending"
    fi
    PHASE_RANK="pending"
    PHASE_ONLINE="pending"
    ;;
  recovery)
    PHASE_RECOVERY="pending"
    ;;
  tools)
    ;;
esac
if [[ "${MODE}" == "all" && "${RANKED_CONFIGURATION}" == "1" ]]; then
  DIAGNOSTICS_REQUESTED=1
elif [[ "${MODE}" == "all" ]]; then
  DIAGNOSTICS_REQUESTED=0
  PHASE_DIAGNOSTICS="not_applicable_non_ranked"
fi
if [[ "${DIAGNOSTICS_REQUESTED}" == "1" ]]; then
  PHASE_DIAGNOSTICS="pending"
fi

if [[ -n "${SERVER_BIN_OVERRIDE}" ]]; then
  SERVER_BIN="${SERVER_BIN_OVERRIDE}"
else
  SERVER_BIN="${RMDB_DIR}/${BUILD_DIR}/bin/rmdb"
fi
if [[ -n "${TPCC_BIN_OVERRIDE}" ]]; then
  TPCC_BIN="${TPCC_BIN_OVERRIDE}"
else
  TPCC_BIN="${TPCC_DIR}/target/release/tpcc-tester"
fi

print_plan() {
  cat <<EOF
mode=${MODE}
profile=${PROFILE}
conformance=${CONFORMANCE}
ranked_configuration=${RANKED_CONFIGURATION}
seed=${SEED}
seed_caller_supplied=${SEED_CALLER_SUPPLIED}
run_id=${RUN_ID}
dataset_run_id=${DATASET_RUN_ID}
rmdb_dir=${RMDB_DIR}
tpcc_dir=${TPCC_DIR}
rmdb_sha=${RMDB_SHA}
tpcc_tester_sha=${TPCC_SHA}
build_dir=${RMDB_DIR}/${BUILD_DIR}
server_bin=${SERVER_BIN}
tpcc_bin=${TPCC_BIN}
db_name=${DB_NAME}
db_path=${DB_PATH}
result_dir=${RESULT_DIR}
csv_dir=${CSV_DIR}
state_dir=${STATE_DIR}
host=${HOST}
port=${PORT}
ready_probe=tpcc-tester --probe-ready --host ${HOST} --port ${PORT}
recovery_ready_budget_seconds=${READY_TIMEOUT_SECONDS}
schedule_owner=rust
effective_scale=${EFFECTIVE_SCALE}
effective_clients=${EFFECTIVE_CLIENTS}
effective_warmup_seconds=${EFFECTIVE_WARMUP_SECONDS}
effective_windows=${PUBLIC_WINDOWS}
effective_window_seconds=${EFFECTIVE_WINDOW_SECONDS}
diagnostics_requested=${DIAGNOSTICS_REQUESTED}
diagnostics_phase=${PHASE_DIAGNOSTICS}
resource_sampler=${RESOURCE_HELPER}
resource_sample_interval_ms=${RESOURCE_INTERVAL_MS}
resource_ranked=false
EOF
}

if [[ "${PLAN_ONLY}" == "1" ]]; then
  print_plan
  exit 0
fi

mkdir -p "${RECORD_ROOT}"
[[ ! -e "${RESULT_DIR}" && ! -L "${RESULT_DIR}" ]] \
  || die "result directory already exists: ${RESULT_DIR}"
mkdir "${RESULT_DIR}"
if [[ -z "${STATE_DIR_OVERRIDE}" ]]; then
  mkdir "${STATE_DIR}"
fi

RUN_STATE_ROOT="${RMDB_DIR}/.tpcc-workflow"
if [[ -e "${RUN_STATE_ROOT}" || -L "${RUN_STATE_ROOT}" ]]; then
  [[ -d "${RUN_STATE_ROOT}" && ! -L "${RUN_STATE_ROOT}" ]] \
    || die "workflow state root is not a safe directory: ${RUN_STATE_ROOT}"
else
  mkdir "${RUN_STATE_ROOT}"
fi
[[ ! -e "${RUN_TEMP_DIR}" && ! -L "${RUN_TEMP_DIR}" ]] \
  || die "refusing to adopt an existing run directory: ${RUN_TEMP_DIR}"
mkdir "${RUN_TEMP_DIR}"
mkdir "${CSV_DIR}"
printf '%s\n' "${OWNER_TOKEN}" >"${RUN_MARKER}"
SERVER_LOG="${RESULT_DIR}/server.log"
RESOURCE_SEGMENT_LIST="${RUN_TEMP_DIR}/resource_segments.list"
RESOURCE_TIMELINE="${RESULT_DIR}/rank_timeline.state"
RESOURCE_RANK_COMPLETE="${RESULT_DIR}/rank_completion.json"
RESOURCE_METRICS="${RESULT_DIR}/resource_metrics.json"
: >"${RESOURCE_SEGMENT_LIST}"
if [[ "${MODE}" == "tools" ]]; then
  RESOURCE_STATUS="not_applicable"
fi

write_manifest() {
  {
    print_plan
    echo "workflow_status=${WORKFLOW_STATUS}"
    echo "allow_deviation=${ALLOW_DEVIATION}"
    echo "scale=${SCALE}"
    echo "clients=${CLIENTS}"
    echo "warmup_seconds=${WARMUP_SECONDS}"
    echo "window_seconds=${WINDOW_SECONDS}"
    echo "phase_setup=${PHASE_SETUP}"
    echo "phase_rank=${PHASE_RANK}"
    echo "phase_online=${PHASE_ONLINE}"
    echo "phase_crash_restart=${PHASE_CRASH_RESTART}"
    echo "phase_recovery=${PHASE_RECOVERY}"
    echo "phase_diagnostics=${PHASE_DIAGNOSTICS}"
    echo "resource_status=${RESOURCE_STATUS}"
    echo "resource_artifact=resource_metrics.json"
  } >"${RESULT_DIR}/manifest.txt"

  python3 - "${RESULT_DIR}/manifest.json" \
    "${WORKFLOW_STATUS}" "${MODE}" "${RUN_ID}" "${DATASET_RUN_ID}" "${PROFILE}" \
    "${RANKED_CONFIGURATION}" "${SEED}" "${SEED_CALLER_SUPPLIED}" \
    "${ALLOW_DEVIATION}" "${EFFECTIVE_SCALE}" "${EFFECTIVE_CLIENTS}" \
    "${EFFECTIVE_WARMUP_SECONDS}" "${PUBLIC_WINDOWS}" \
    "${EFFECTIVE_WINDOW_SECONDS}" "${RMDB_SHA}" "${TPCC_SHA}" \
    "${RMDB_DIR}" "${TPCC_DIR}" "${DB_PATH}" "${RESULT_DIR}" "${STATE_DIR}" \
    "${PHASE_SETUP}" "${PHASE_RANK}" "${PHASE_ONLINE}" \
    "${PHASE_CRASH_RESTART}" "${PHASE_RECOVERY}" "${PHASE_DIAGNOSTICS}" \
    "${DIAGNOSTICS_REQUESTED}" "${DIAGNOSTIC_WARMUP_SECONDS}" \
    "${DIAGNOSTIC_OBSERVATION_SECONDS}" "${RESOURCE_STATUS}" \
    "${RESOURCE_INTERVAL_MS}" "${RESOURCE_METRICS}" <<'PY'
import json
import os
from pathlib import Path
import sys

(
    output,
    workflow_status,
    mode,
    run_id,
    dataset_run_id,
    profile,
    ranked_configuration,
    seed,
    seed_caller_supplied,
    allow_deviation,
    scale,
    clients,
    warmup_seconds,
    windows,
    window_seconds,
    rmdb_sha,
    tpcc_sha,
    rmdb_dir,
    tpcc_dir,
    db_path,
    result_dir,
    state_dir,
    phase_setup,
    phase_rank,
    phase_online,
    phase_crash_restart,
    phase_recovery,
    phase_diagnostics,
    diagnostics_requested,
    diagnostic_warmup,
    diagnostic_observation,
    resource_status,
    resource_interval_ms,
    resource_metrics_path,
) = sys.argv[1:]

artifact_names = {
    "proc_before": "diagnostic_proc_before.json",
    "proc_after": "diagnostic_proc_after.json",
    "proc_delta": "diagnostic_proc_delta.json",
    "strace_summary": "diagnostic_strace_summary.txt",
    "strace_metrics": "diagnostic_strace_metrics.json",
}


def describe_artifact(name):
    path = Path(result_dir) / name
    descriptor = {"path": name, "status": "missing"}
    if path.is_symlink():
        descriptor["status"] = "unsafe"
        return descriptor
    if not path.is_file():
        return descriptor
    if path.stat().st_size == 0:
        descriptor["status"] = "empty"
        return descriptor
    if path.suffix == ".json":
        try:
            with path.open("r", encoding="utf-8") as stream:
                artifact = json.load(stream)
            artifact_status = artifact.get("status")
        except (OSError, json.JSONDecodeError, AttributeError):
            artifact_status = "invalid"
        descriptor["status"] = (
            artifact_status
            if isinstance(artifact_status, str)
            else "present"
        )
    else:
        descriptor["status"] = "present"
    return descriptor


def load_resource_metrics():
    path = Path(resource_metrics_path)
    descriptor = {"path": path.name, "status": "missing"}
    if path.is_symlink():
        descriptor["status"] = "unsafe"
        return None, descriptor
    if not path.is_file():
        return None, descriptor
    try:
        if path.stat().st_size == 0:
            descriptor["status"] = "empty"
            return None, descriptor
        with path.open("r", encoding="utf-8") as stream:
            artifact = json.load(stream)
    except (OSError, json.JSONDecodeError):
        descriptor["status"] = "invalid"
        return None, descriptor
    expected_fields = {
        "schema_version",
        "kind",
        "status",
        "ranked",
        "score_effect",
        "run_id",
        "scope",
        "database_path",
        "database_identity",
        "sample_interval_ms",
        "expected_server_generations",
        "requested_server_generations",
        "valid_server_generations",
        "max_rss",
        "database_disk",
        "rank_cpu",
        "warnings",
        "segments",
    }
    valid_statuses = {"available", "partial", "unavailable"}
    if (
        not isinstance(artifact, dict)
        or set(artifact) != expected_fields
        or artifact.get("schema_version") != 1
        or artifact.get("kind") != "rmdb_resource_metrics"
        or artifact.get("status") not in valid_statuses
        or artifact.get("ranked") is not False
        or artifact.get("score_effect") != "none"
        or artifact.get("run_id") != run_id
        or artifact.get("database_path") != db_path
        or artifact.get("sample_interval_ms") != int(resource_interval_ms)
        or not isinstance(artifact.get("max_rss"), dict)
        or not isinstance(artifact.get("database_disk"), dict)
        or not isinstance(artifact.get("rank_cpu"), dict)
    ):
        descriptor["status"] = "invalid"
        return None, descriptor
    if artifact["status"] == "available" and any(
        artifact[key].get("status") != "available"
        for key in ("max_rss", "database_disk", "rank_cpu")
        if artifact[key].get("status") != "not_applicable"
    ):
        descriptor["status"] = "invalid"
        return None, descriptor
    descriptor["status"] = artifact["status"]
    return artifact, descriptor


resource_metrics, resource_descriptor = load_resource_metrics()
if resource_metrics is None:
    safe_resource_status = (
        resource_status
        if resource_status
        in {
            "pending",
            "collecting",
            "partial",
            "unavailable",
            "failed",
            "not_applicable",
        }
        else "failed"
    )
    resource_max_rss = {"status": "unavailable"}
    resource_database_disk = {"status": "unavailable"}
    resource_rank_cpu = {
        "status": (
            "not_applicable"
            if mode in {"init", "recovery", "tools"}
            else "unavailable"
        )
    }
    resource_scope = None
    resource_generations = {
        "expected": 0,
        "requested": 0,
        "valid": 0,
    }
else:
    safe_resource_status = resource_metrics["status"]
    resource_max_rss = resource_metrics["max_rss"]
    resource_database_disk = resource_metrics["database_disk"]
    resource_rank_cpu = resource_metrics["rank_cpu"]
    resource_scope = resource_metrics["scope"]
    resource_generations = {
        "expected": resource_metrics["expected_server_generations"],
        "requested": resource_metrics["requested_server_generations"],
        "valid": resource_metrics["valid_server_generations"],
    }


payload = {
    "schema_version": 1,
    "conformance": (
        "public_spec_aligned"
        if ranked_configuration == "1"
        else "non_ranked_deviation"
    ),
    "embeds_unpublished_official_values": False,
    "status": workflow_status,
    "mode": mode,
    "run_id": run_id,
    "dataset_run_id": dataset_run_id,
    "profile": profile,
    "ranked_configuration": ranked_configuration == "1",
    "seed": {
        "value": int(seed),
        "caller_supplied": seed_caller_supplied == "1",
        "source": (
            "caller"
            if seed_caller_supplied == "1"
            else "local_workflow_default_not_official"
        ),
    },
    "effective": {
        "warehouses": int(scale),
        "clients": int(clients),
        "warmup_seconds": int(warmup_seconds),
        "measurement_windows": int(windows),
        "window_seconds": int(window_seconds),
        "transaction_mix_percent": {
            "new_order": 45,
            "payment": 43,
            "order_status": 4,
            "delivery": 4,
            "stock_level": 4,
        },
        "derived_write_ratio": 0.92,
        "deviation_opt_in": allow_deviation == "1",
        "deviation_active": ranked_configuration != "1",
    },
    "source": {
        "rmdb_sha": rmdb_sha,
        "tpcc_tester_sha": tpcc_sha,
    },
    "paths": {
        "rmdb": rmdb_dir,
        "tpcc_tester": tpcc_dir,
        "database": db_path,
        "result": result_dir,
        "state": state_dir,
    },
    "phases": {
        "setup": phase_setup,
        "rank": phase_rank,
        "online": phase_online,
        "crash_restart": phase_crash_restart,
        "recovery": phase_recovery,
        "diagnostics": phase_diagnostics,
    },
    "diagnostics": {
        "requested": diagnostics_requested == "1",
        "ranked": False,
        "public_warmup_seconds": int(diagnostic_warmup),
        "public_observation_seconds": int(diagnostic_observation),
        "native_single_observation_supported": True,
        "status": phase_diagnostics,
        "artifacts": {
            key: describe_artifact(name)
            for key, name in artifact_names.items()
        },
    },
    "resources": {
        "observation_only": True,
        "ranked": False,
        "score_effect": "none",
        "status": safe_resource_status,
        "sample_interval_ms": int(resource_interval_ms),
        "sampling": {
            "cadence": "fixed_local_one_second",
            "process_scope": "registered_rmdb_process_tree",
            "official_hidden_sampler_reproduced": False,
        },
        "scope": resource_scope,
        "server_generations": resource_generations,
        "max_rss": resource_max_rss,
        "database_disk": resource_database_disk,
        "rank_cpu": resource_rank_cpu,
        "artifact": resource_descriptor,
    },
}

path = Path(output)
temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
with temporary.open("w", encoding="utf-8") as stream:
    json.dump(payload, stream, ensure_ascii=False, indent=2, sort_keys=True)
    stream.write("\n")
    stream.flush()
    os.fsync(stream.fileno())
os.replace(temporary, path)
PY
  MANIFEST_READY=1
}

set_phase_status() {
  local phase="$1"
  local status="$2"
  case "${phase}" in
    setup) PHASE_SETUP="${status}" ;;
    rank) PHASE_RANK="${status}" ;;
    online) PHASE_ONLINE="${status}" ;;
    crash_restart) PHASE_CRASH_RESTART="${status}" ;;
    recovery) PHASE_RECOVERY="${status}" ;;
    diagnostics) PHASE_DIAGNOSTICS="${status}" ;;
    *) die "internal unknown workflow phase: ${phase}" ;;
  esac
  write_manifest
}

mark_unfinished_phases_after_failure() {
  [[ "${PHASE_SETUP}" != "running" ]] || PHASE_SETUP="failed"
  [[ "${PHASE_RANK}" != "running" ]] || PHASE_RANK="failed"
  [[ "${PHASE_ONLINE}" != "running" ]] || PHASE_ONLINE="failed"
  [[ "${PHASE_CRASH_RESTART}" != "running" ]] \
    || PHASE_CRASH_RESTART="failed"
  [[ "${PHASE_RECOVERY}" != "running" ]] || PHASE_RECOVERY="failed"
  [[ "${PHASE_DIAGNOSTICS}" != "running" ]] \
    || PHASE_DIAGNOSTICS="failed"

  [[ "${PHASE_SETUP}" != "pending" ]] \
    || PHASE_SETUP="skipped_due_to_failure"
  [[ "${PHASE_RANK}" != "pending" ]] \
    || PHASE_RANK="skipped_due_to_failure"
  [[ "${PHASE_ONLINE}" != "pending" ]] \
    || PHASE_ONLINE="skipped_due_to_failure"
  [[ "${PHASE_CRASH_RESTART}" != "pending" ]] \
    || PHASE_CRASH_RESTART="skipped_due_to_failure"
  [[ "${PHASE_RECOVERY}" != "pending" ]] \
    || PHASE_RECOVERY="skipped_due_to_failure"
  [[ "${PHASE_DIAGNOSTICS}" != "pending" ]] \
    || PHASE_DIAGNOSTICS="skipped_due_to_failure"
}

record_tools() {
  local tool
  : >"${RESULT_DIR}/tool_status.txt"
  for tool in bash cmake cargo python3; do
    if command -v "${tool}" >/dev/null 2>&1; then
      printf '%s=%s\n' "${tool}" "$(command -v "${tool}")" \
        >>"${RESULT_DIR}/tool_status.txt"
    else
      printf '%s=\n' "${tool}" >>"${RESULT_DIR}/tool_status.txt"
    fi
  done
  uname -a >"${RESULT_DIR}/system_info.txt" 2>&1 || true
}

owned_marker_matches() {
  local marker="$1"
  [[ -f "${marker}" ]] || return 1
  [[ "$(sed -n '1p' "${marker}")" == "${OWNER_TOKEN}" ]]
}

monotonic_millis() {
  python3 - <<'PY'
import time

print(time.monotonic_ns() // 1_000_000)
PY
}

process_identity() {
  local pid="$1"
  local absolute_deadline_millis="${2-0}"
  python3 - "${pid}" "${absolute_deadline_millis}" <<'PY'
from pathlib import Path
import ctypes
import subprocess
import sys
import time

pid = int(sys.argv[1])
deadline_millis = int(sys.argv[2])


def remaining_seconds():
    if deadline_millis <= 0:
        return None
    remaining = (
        deadline_millis - time.monotonic_ns() // 1_000_000
    ) / 1000.0
    if remaining <= 0:
        raise SystemExit(124)
    return remaining


def darwin_start_identity(process_id):
    class ProcBsdInfo(ctypes.Structure):
        _fields_ = [
            ("pbi_flags", ctypes.c_uint32),
            ("pbi_status", ctypes.c_uint32),
            ("pbi_xstatus", ctypes.c_uint32),
            ("pbi_pid", ctypes.c_uint32),
            ("pbi_ppid", ctypes.c_uint32),
            ("pbi_uid", ctypes.c_uint32),
            ("pbi_gid", ctypes.c_uint32),
            ("pbi_ruid", ctypes.c_uint32),
            ("pbi_rgid", ctypes.c_uint32),
            ("pbi_svuid", ctypes.c_uint32),
            ("pbi_svgid", ctypes.c_uint32),
            ("rfu_1", ctypes.c_uint32),
            ("pbi_comm", ctypes.c_char * 16),
            ("pbi_name", ctypes.c_char * 32),
            ("pbi_nfiles", ctypes.c_uint32),
            ("pbi_pgid", ctypes.c_uint32),
            ("pbi_pjobc", ctypes.c_uint32),
            ("e_tdev", ctypes.c_uint32),
            ("e_tpgid", ctypes.c_uint32),
            ("pbi_nice", ctypes.c_int32),
            ("pbi_start_tvsec", ctypes.c_uint64),
            ("pbi_start_tvusec", ctypes.c_uint64),
        ]

    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError:
        return None
    library.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    library.proc_pidinfo.restype = ctypes.c_int
    info = ProcBsdInfo()
    copied = library.proc_pidinfo(
        process_id,
        3,
        0,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if copied != ctypes.sizeof(info):
        return None
    return f"darwin:{info.pbi_start_tvsec}:{info.pbi_start_tvusec}"


stat_path = Path("/proc") / str(pid) / "stat"
if stat_path.is_file():
    try:
        remaining_seconds()
        text = stat_path.read_text(encoding="ascii")
        remaining_seconds()
        fields = text[text.rfind(")") + 2 :].split()
        print(f"linux:{fields[19]}")
    except (OSError, IndexError, ValueError):
        raise SystemExit(1)
elif sys.platform == "darwin":
    remaining_seconds()
    identity = darwin_start_identity(pid)
    remaining_seconds()
    if identity is None:
        raise SystemExit(1)
    print(identity)
else:
    try:
        result = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(pid)],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=remaining_seconds(),
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(124)
    remaining_seconds()
    identity = result.stdout.strip()
    if result.returncode != 0 or not identity:
        raise SystemExit(1)
    print(f"ps:{identity}")
PY
}

process_owner_matches() {
  local pid="$1"
  local absolute_deadline_millis="$2"
  python3 - "${pid}" "${PROCESS_OWNER_TOKEN}" \
    "${absolute_deadline_millis}" <<'PY'
from pathlib import Path
import subprocess
import sys
import time

pid = int(sys.argv[1])
token = sys.argv[2]
deadline_millis = int(sys.argv[3])
expected = f"RMDB_WORKFLOW_PROCESS_OWNER={token}"


def remaining_seconds():
    remaining = (
        deadline_millis - time.monotonic_ns() // 1_000_000
    ) / 1000.0
    if remaining <= 0:
        raise SystemExit(124)
    return remaining


environment_path = Path("/proc") / str(pid) / "environ"
if environment_path.is_file():
    remaining_seconds()
    try:
        entries = environment_path.read_bytes().split(b"\0")
    except OSError:
        raise SystemExit(1)
    remaining_seconds()
    raise SystemExit(
        0 if expected.encode("utf-8") in entries else 1
    )
if sys.platform != "darwin":
    raise SystemExit(1)
try:
    result = subprocess.run(
        ["ps", "-E", "-ww", "-o", "command=", "-p", str(pid)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        timeout=remaining_seconds(),
    )
except subprocess.TimeoutExpired:
    raise SystemExit(124)
remaining_seconds()
raise SystemExit(
    0 if result.returncode == 0 and expected in result.stdout.split() else 1
)
PY
}

establish_cleanup_identity() {
  local pid="$1"
  local cleanup_deadline=""
  local cleanup_identity_before=""
  local cleanup_identity_after=""
  local cleanup_pgid=""
  cleanup_deadline=$(( $(monotonic_millis) + 1000 ))
  cleanup_identity_before="$(
    process_identity "${pid}" "${cleanup_deadline}"
  )" || return 1
  process_owner_matches "${pid}" "${cleanup_deadline}" || return 1
  cleanup_pgid="$(python3 - "${pid}" <<'PY'
import os
import sys

try:
    print(os.getpgid(int(sys.argv[1])))
except ProcessLookupError:
    raise SystemExit(1)
PY
)" || return 1
  [[ "${cleanup_pgid}" == "${pid}" ]] || return 1
  cleanup_identity_after="$(
    process_identity "${pid}" "${cleanup_deadline}"
  )" || return 1
  [[ "${cleanup_identity_before}" == "${cleanup_identity_after}" ]] \
    || return 1
  SERVER_IDENTITY="${cleanup_identity_after}"
  SERVER_PGID="${cleanup_pgid}"
}

server_process_helper() {
  local action="$1"
  local signal_name="${2-}"
  local absolute_deadline_millis="${3-0}"
  python3 - "${action}" "${SERVER_PID}" "${SERVER_IDENTITY}" \
    "${SERVER_PGID}" "${PORT}" "${HOST}" "${HOST_VERSION}" "${signal_name}" \
    "${absolute_deadline_millis}" "$$" <<'PY'
import os
from pathlib import Path
import ctypes
import ipaddress
import signal
import subprocess
import sys
import time

(
    action,
    root_text,
    expected_identity,
    pgid_text,
    port_text,
    host,
    host_version_text,
    signal_name,
    deadline_text,
    expected_parent_text,
) = sys.argv[1:]
root_pid = int(root_text)
registered_pgid = int(pgid_text)
port = int(port_text)
host_version = int(host_version_text)
absolute_deadline_millis = int(deadline_text)
expected_parent_pid = int(expected_parent_text)


class DeadlineExpired(Exception):
    pass


def remaining_seconds():
    if absolute_deadline_millis <= 0:
        return None
    remaining = (
        absolute_deadline_millis - time.monotonic_ns() // 1_000_000
    ) / 1000.0
    if remaining <= 0:
        raise DeadlineExpired
    return remaining


def check_deadline():
    remaining_seconds()


def run_inspection(command):
    check_deadline()
    try:
        result = subprocess.run(
            command,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=remaining_seconds(),
        )
    except subprocess.TimeoutExpired as error:
        raise DeadlineExpired from error
    check_deadline()
    return result


def darwin_start_identity(process_id):
    class ProcBsdInfo(ctypes.Structure):
        _fields_ = [
            ("pbi_flags", ctypes.c_uint32),
            ("pbi_status", ctypes.c_uint32),
            ("pbi_xstatus", ctypes.c_uint32),
            ("pbi_pid", ctypes.c_uint32),
            ("pbi_ppid", ctypes.c_uint32),
            ("pbi_uid", ctypes.c_uint32),
            ("pbi_gid", ctypes.c_uint32),
            ("pbi_ruid", ctypes.c_uint32),
            ("pbi_rgid", ctypes.c_uint32),
            ("pbi_svuid", ctypes.c_uint32),
            ("pbi_svgid", ctypes.c_uint32),
            ("rfu_1", ctypes.c_uint32),
            ("pbi_comm", ctypes.c_char * 16),
            ("pbi_name", ctypes.c_char * 32),
            ("pbi_nfiles", ctypes.c_uint32),
            ("pbi_pgid", ctypes.c_uint32),
            ("pbi_pjobc", ctypes.c_uint32),
            ("e_tdev", ctypes.c_uint32),
            ("e_tpgid", ctypes.c_uint32),
            ("pbi_nice", ctypes.c_int32),
            ("pbi_start_tvsec", ctypes.c_uint64),
            ("pbi_start_tvusec", ctypes.c_uint64),
        ]

    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError:
        return None
    library.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    library.proc_pidinfo.restype = ctypes.c_int
    info = ProcBsdInfo()
    copied = library.proc_pidinfo(
        process_id,
        3,
        0,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if copied != ctypes.sizeof(info):
        return None
    return f"darwin:{info.pbi_start_tvsec}:{info.pbi_start_tvusec}"


def linux_stat(pid):
    check_deadline()
    try:
        text = (Path("/proc") / str(pid) / "stat").read_text(encoding="ascii")
        check_deadline()
        fields = text[text.rfind(")") + 2 :].split()
        return {
            "pid": pid,
            "state": fields[0],
            "ppid": int(fields[1]),
            "pgrp": int(fields[2]),
            "identity": f"linux:{fields[19]}",
        }
    except (OSError, IndexError, ValueError):
        return None


def process_table():
    table = {}
    proc = Path("/proc")
    if (proc / "self" / "stat").is_file():
        for entry in proc.iterdir():
            check_deadline()
            if not entry.name.isdigit():
                continue
            item = linux_stat(int(entry.name))
            if item is not None:
                table[item["pid"]] = item
        return table

    result = run_inspection(["ps", "-axo", "pid=,ppid=,pgid=,state="])
    if result.returncode != 0:
        raise RuntimeError("cannot inspect the process table")
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        try:
            pid, ppid, pgrp = map(int, fields[:3])
        except ValueError:
            continue
        table[pid] = {
            "pid": pid,
            "ppid": ppid,
            "pgrp": pgrp,
            "state": fields[3],
            "identity": None,
        }
    return table


def current_identity(pid):
    check_deadline()
    item = linux_stat(pid)
    if item is not None:
        return item["identity"]
    if sys.platform == "darwin":
        identity = darwin_start_identity(pid)
        check_deadline()
        return identity
    result = run_inspection(["ps", "-o", "lstart=", "-p", str(pid)])
    identity = result.stdout.strip()
    if result.returncode != 0 or not identity:
        return None
    return f"ps:{identity}"


def descendants(table):
    owned = {root_pid}
    changed = True
    while changed:
        check_deadline()
        changed = False
        for pid, item in table.items():
            check_deadline()
            if pid not in owned and item["ppid"] in owned:
                owned.add(pid)
                changed = True
    return owned


def root_status(table):
    item = table.get(root_pid)
    if item is None:
        return "absent"
    if current_identity(root_pid) != expected_identity:
        return "reused"
    return "owned"


def listener_owners_linux():
    try:
        target_address = ipaddress.ip_address(host)
    except ValueError as error:
        raise RuntimeError(f"invalid cached readiness host {host}: {error}")
    if target_address.version != host_version:
        raise RuntimeError("cached readiness host family changed")
    target_addresses = {target_address}

    def decode_address(name, encoded):
        raw = bytes.fromhex(encoded)
        if name == "tcp":
            return ipaddress.ip_address(raw[::-1])
        reordered = b"".join(
            raw[offset : offset + 4][::-1]
            for offset in range(0, len(raw), 4)
        )
        return ipaddress.ip_address(reordered)

    def matches_target(name, encoded):
        try:
            local_address = decode_address(name, encoded)
        except ValueError:
            return False
        matching_family = {
            address
            for address in target_addresses
            if address.version == local_address.version
        }
        return bool(matching_family) and (
            local_address.is_unspecified or local_address in matching_family
        )

    inodes = set()
    inspected = False
    for name in ("tcp", "tcp6"):
        check_deadline()
        path = Path("/proc/net") / name
        if not path.exists():
            continue
        inspected = True
        try:
            lines = path.read_text(encoding="ascii").splitlines()[1:]
            check_deadline()
        except OSError:
            raise RuntimeError(f"cannot inspect {path}")
        for line in lines:
            check_deadline()
            fields = line.split()
            if len(fields) < 10 or fields[3] != "0A":
                continue
            try:
                encoded_address, encoded_port = fields[1].rsplit(":", 1)
                local_port = int(encoded_port, 16)
            except (IndexError, ValueError):
                continue
            if local_port == port and matches_target(name, encoded_address):
                inodes.add(fields[9])
    if not inspected:
        raise RuntimeError("neither /proc/net/tcp nor tcp6 is available")
    if not inodes:
        return set(), False

    owners = set()
    mapped = set()
    for entry in Path("/proc").iterdir():
        check_deadline()
        if not entry.name.isdigit():
            continue
        try:
            descriptors = (entry / "fd").iterdir()
            for descriptor in descriptors:
                check_deadline()
                try:
                    target = os.readlink(descriptor)
                except OSError:
                    continue
                if target.startswith("socket:[") and target.endswith("]"):
                    inode = target[8:-1]
                    if inode in inodes:
                        owners.add(int(entry.name))
                        mapped.add(inode)
        except (OSError, PermissionError):
            continue
    if mapped != inodes:
        raise RuntimeError("cannot identify every listening socket owner")
    return owners, True


def listener_owners_lsof():
    check_deadline()
    try:
        target_address = ipaddress.ip_address(host)
    except ValueError as error:
        raise RuntimeError(f"invalid cached readiness host {host}: {error}")
    if target_address.version != host_version:
        raise RuntimeError("cached readiness host family changed")
    target_addresses = {target_address}

    def parse_endpoint(endpoint):
        if endpoint.startswith("*:"):
            return None
        if endpoint.startswith("["):
            closing = endpoint.find("]")
            if closing <= 1:
                raise RuntimeError("lsof returned an invalid IPv6 endpoint")
            address_text = endpoint[1:closing]
        else:
            try:
                address_text, _ = endpoint.rsplit(":", 1)
            except ValueError as error:
                raise RuntimeError(
                    "lsof returned an invalid listener endpoint"
                ) from error
        try:
            return ipaddress.ip_address(address_text)
        except ValueError as error:
            raise RuntimeError(
                "lsof returned a non-numeric listener address"
            ) from error

    owners = set()
    for version in sorted({address.version for address in target_addresses}):
        try:
            result = subprocess.run(
                [
                    "lsof",
                    "-nP",
                    "-a",
                    f"-i{version}TCP:{port}",
                    "-sTCP:LISTEN",
                    "-Fpn",
                ],
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=remaining_seconds(),
            )
        except FileNotFoundError:
            raise RuntimeError("lsof is required to validate listener ownership")
        except subprocess.TimeoutExpired as error:
            raise DeadlineExpired from error
        check_deadline()
        if result.returncode == 1 and not result.stdout:
            continue
        if result.returncode != 0:
            raise RuntimeError("lsof could not inspect listening sockets")

        family_targets = {
            address for address in target_addresses if address.version == version
        }
        current_pid = None
        for line in result.stdout.splitlines():
            if line.startswith("p") and line[1:].isdigit():
                current_pid = int(line[1:])
            elif line.startswith("n"):
                if current_pid is None:
                    raise RuntimeError("lsof returned an ownerless socket")
                local_address = parse_endpoint(line[1:])
                if local_address is None or local_address in family_targets:
                    owners.add(current_pid)
    if not owners:
        return set(), False
    return owners, True


try:
    check_deadline()
    table = process_table()
    status = root_status(table)
except DeadlineExpired:
    print("process ownership inspection exceeded readiness deadline", file=sys.stderr)
    raise SystemExit(124)
except RuntimeError as error:
    print(error, file=sys.stderr)
    raise SystemExit(2)

if action == "root-alive":
    raise SystemExit(0 if status == "owned" else (1 if status == "absent" else 2))

if action == "root-running":
    if status == "reused":
        print("registered RMDB pid was reused", file=sys.stderr)
        raise SystemExit(2)
    if status == "absent" or table[root_pid]["state"].upper().startswith("Z"):
        raise SystemExit(1)
    raise SystemExit(0)

if action == "group-running":
    if status == "reused":
        print("registered RMDB pid was reused", file=sys.stderr)
        raise SystemExit(2)
    running = any(
        item["pgrp"] == registered_pgid
        and not item["state"].upper().startswith("Z")
        for item in table.values()
    )
    raise SystemExit(0 if running else 1)

if action == "listener":
    if status != "owned":
        print("registered RMDB process identity is not live", file=sys.stderr)
        raise SystemExit(2)
    try:
        if (Path("/proc") / "self" / "stat").is_file():
            first_owners, present = listener_owners_linux()
        else:
            first_owners, present = listener_owners_lsof()
        if not present:
            raise SystemExit(1)

        refreshed_table = process_table()
        if root_status(refreshed_table) != "owned":
            raise RuntimeError(
                "registered RMDB identity changed during listener inspection"
            )
        refreshed_owned = descendants(refreshed_table)
        owner_identities = {
            pid: current_identity(pid) for pid in first_owners
        }
        if any(identity is None for identity in owner_identities.values()):
            raise RuntimeError("listener owner exited during ownership inspection")

        if (Path("/proc") / "self" / "stat").is_file():
            second_owners, second_present = listener_owners_linux()
        else:
            second_owners, second_present = listener_owners_lsof()
        if not second_present or second_owners != first_owners:
            raise SystemExit(1)
        if any(
            current_identity(pid) != identity
            for pid, identity in owner_identities.items()
        ):
            raise RuntimeError(
                "listener owner identity changed during ownership inspection"
            )
    except DeadlineExpired:
        print("listener ownership inspection exceeded readiness deadline", file=sys.stderr)
        raise SystemExit(124)
    except RuntimeError as error:
        print(error, file=sys.stderr)
        raise SystemExit(2)
    if not second_owners.issubset(refreshed_owned):
        print(
            "listening socket is owned outside the registered RMDB process tree",
            file=sys.stderr,
        )
        raise SystemExit(2)
    raise SystemExit(0)

if action == "signal":
    group_members = {
        pid
        for pid, item in table.items()
        if item["pgrp"] == registered_pgid
        and not item["state"].upper().startswith("Z")
    }
    if status == "reused":
        print("refusing to signal a reused RMDB pid", file=sys.stderr)
        raise SystemExit(2)
    if status == "owned":
        root = table[root_pid]
        if root["pgrp"] != registered_pgid or registered_pgid != root_pid:
            print("registered RMDB process group identity changed", file=sys.stderr)
            raise SystemExit(2)
        owned = descendants(table)
        live_owned = {
            pid
            for pid in owned
            if pid in table and not table[pid]["state"].upper().startswith("Z")
        }
        if not group_members.issubset(owned) or any(
            table[pid]["pgrp"] != registered_pgid for pid in live_owned
        ):
            print(
                "RMDB process tree escaped or was joined by another process group",
                file=sys.stderr,
            )
            raise SystemExit(2)
    elif group_members:
        print(
            "refusing to signal a process group after its registered root exited",
            file=sys.stderr,
        )
        raise SystemExit(2)
    else:
        raise SystemExit(0)

    signals = {
        "INT": signal.SIGINT,
        "TERM": signal.SIGTERM,
        "KILL": signal.SIGKILL,
    }
    try:
        requested = signals[signal_name]
    except KeyError:
        raise SystemExit(2)
    try:
        os.killpg(registered_pgid, requested)
    except ProcessLookupError:
        pass
    except PermissionError:
        print("permission denied while signaling RMDB process group", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(0)

if action == "signal-root":
    if signal_name != "KILL":
        print("unregistered RMDB root cleanup only supports SIGKILL", file=sys.stderr)
        raise SystemExit(2)
    if status != "owned":
        print("unregistered RMDB root identity is not live", file=sys.stderr)
        raise SystemExit(2)

    def validate_unregistered_root(candidate_table):
        if root_status(candidate_table) != "owned":
            return False
        root = candidate_table[root_pid]
        if root["ppid"] != expected_parent_pid:
            return False
        if registered_pgid != root_pid or root["pgrp"] == registered_pgid:
            return False
        return descendants(candidate_table) == {root_pid}

    if not validate_unregistered_root(table):
        print(
            "refusing to signal an unproven pre-group RMDB child",
            file=sys.stderr,
        )
        raise SystemExit(2)
    try:
        refreshed_table = process_table()
    except (DeadlineExpired, RuntimeError):
        print(
            "could not revalidate pre-group RMDB child",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if not validate_unregistered_root(refreshed_table):
        print(
            "pre-group RMDB child identity changed during cleanup",
            file=sys.stderr,
        )
        raise SystemExit(2)
    try:
        os.kill(root_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    except PermissionError:
        print("permission denied while signaling RMDB child", file=sys.stderr)
        raise SystemExit(2)
    raise SystemExit(0)

print(f"unknown process-helper action: {action}", file=sys.stderr)
raise SystemExit(2)
PY
}

port_is_available() {
  local absolute_deadline_millis="${1-0}"
  python3 - "${HOST}" "${HOST_VERSION}" "${PORT}" \
    "${absolute_deadline_millis}" <<'PY'
import socket
import sys
import time

host = sys.argv[1]
host_version = int(sys.argv[2])
port = int(sys.argv[3])
deadline_millis = int(sys.argv[4])


def remaining_seconds():
    if deadline_millis <= 0:
        return None
    remaining = (
        deadline_millis - time.monotonic_ns() // 1_000_000
    ) / 1000.0
    if remaining <= 0:
        raise SystemExit(124)
    return remaining


remaining_seconds()
if host_version == 4:
    family = socket.AF_INET
    address = (host, port)
elif host_version == 6:
    family = socket.AF_INET6
    address = (host, port, 0, 0)
else:
    raise SystemExit(2)
remaining = remaining_seconds()
sock = socket.socket(family, socket.SOCK_STREAM)
try:
    if remaining is not None:
        sock.settimeout(remaining)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(address)
except OSError:
    sock.close()
    raise SystemExit(1)
sock.close()
remaining_seconds()
raise SystemExit(0)
PY
}

wait_for_server_group_exit() {
  local deadline="$1"
  local now=""
  while true; do
    if server_process_helper group-running "" "${deadline}"; then
      :
    else
      local status=$?
      [[ "${status}" == "1" ]] && return 0
      return "${status}"
    fi
    now="$(monotonic_millis)"
    (( now < deadline )) || return 1
    sleep 0.05
  done
}

wait_for_server_root_exit() {
  local deadline="$1"
  local now=""
  while true; do
    if server_process_helper root-running "" "${deadline}"; then
      :
    else
      local status=$?
      [[ "${status}" == "1" ]] && return 0
      return "${status}"
    fi
    now="$(monotonic_millis)"
    (( now < deadline )) || return 1
    sleep 0.05
  done
}

wait_for_listener_gone() {
  local deadline="$1"
  local now=""
  while true; do
    port_is_available "${deadline}" && return 0
    now="$(monotonic_millis)"
    (( now < deadline )) || return 1
    sleep 0.05
  done
}

capture_resource_database_identity() {
  local identity=""
  if ! identity="$(python3 - "${DB_PATH}" <<'PY'
import os
from pathlib import Path
import stat
import sys

path = Path(sys.argv[1])
try:
    metadata = path.lstat()
except OSError:
    raise SystemExit(1)
if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
    raise SystemExit(1)
print(f"{metadata.st_dev}:{metadata.st_ino}")
PY
)"; then
    warn "could not bind non-ranked resource evidence to database ${DB_PATH}"
    return 1
  fi
  RESOURCE_DATABASE_IDENTITY="${identity}"
}

resource_monitor_process_helper() {
  local action="$1"
  local pid="$2"
  local expected_identity="$3"
  local expected_parent_pid="$4"
  local signal_name="${5-}"
  local deadline_millis=""
  deadline_millis=$(( $(monotonic_millis) + 1000 ))
  python3 - "${action}" "${pid}" "${expected_identity}" \
    "${expected_parent_pid}" "${signal_name}" "${deadline_millis}" <<'PY'
import ctypes
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

(
    action,
    pid_text,
    expected_identity,
    expected_parent_text,
    signal_name,
    deadline_text,
) = sys.argv[1:]
pid = int(pid_text)
expected_parent = int(expected_parent_text)
deadline_millis = int(deadline_text)


def remaining_seconds():
    remaining = (
        deadline_millis - time.monotonic_ns() // 1_000_000
    ) / 1000.0
    if remaining <= 0:
        raise TimeoutError
    return remaining


def linux_record():
    try:
        text = (Path("/proc") / str(pid) / "stat").read_text(
            encoding="ascii"
        )
        fields = text[text.rfind(")") + 2 :].split()
        state = fields[0]
        parent_pid = int(fields[1])
        identity = f"linux:{fields[19]}"
    except (OSError, IndexError, ValueError):
        return None
    if state.upper().startswith("Z"):
        return None
    return identity, parent_pid


def darwin_record():
    class ProcBsdInfo(ctypes.Structure):
        _fields_ = [
            ("pbi_flags", ctypes.c_uint32),
            ("pbi_status", ctypes.c_uint32),
            ("pbi_xstatus", ctypes.c_uint32),
            ("pbi_pid", ctypes.c_uint32),
            ("pbi_ppid", ctypes.c_uint32),
            ("pbi_uid", ctypes.c_uint32),
            ("pbi_gid", ctypes.c_uint32),
            ("pbi_ruid", ctypes.c_uint32),
            ("pbi_rgid", ctypes.c_uint32),
            ("pbi_svuid", ctypes.c_uint32),
            ("pbi_svgid", ctypes.c_uint32),
            ("rfu_1", ctypes.c_uint32),
            ("pbi_comm", ctypes.c_char * 16),
            ("pbi_name", ctypes.c_char * 32),
            ("pbi_nfiles", ctypes.c_uint32),
            ("pbi_pgid", ctypes.c_uint32),
            ("pbi_pjobc", ctypes.c_uint32),
            ("e_tdev", ctypes.c_uint32),
            ("e_tpgid", ctypes.c_uint32),
            ("pbi_nice", ctypes.c_int32),
            ("pbi_start_tvsec", ctypes.c_uint64),
            ("pbi_start_tvusec", ctypes.c_uint64),
        ]

    try:
        library = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
    except OSError:
        return None
    library.proc_pidinfo.argtypes = [
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_uint64,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    library.proc_pidinfo.restype = ctypes.c_int
    info = ProcBsdInfo()
    copied = library.proc_pidinfo(
        pid,
        3,
        0,
        ctypes.byref(info),
        ctypes.sizeof(info),
    )
    if copied != ctypes.sizeof(info) or info.pbi_status == 5:
        return None
    return (
        f"darwin:{info.pbi_start_tvsec}:{info.pbi_start_tvusec}",
        int(info.pbi_ppid),
    )


def portable_record():
    try:
        result = subprocess.run(
            [
                "ps",
                "-o",
                "state=",
                "-o",
                "ppid=",
                "-o",
                "lstart=",
                "-p",
                str(pid),
            ],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=remaining_seconds(),
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    fields = result.stdout.strip().split(None, 2)
    if result.returncode != 0 or len(fields) != 3:
        return None
    try:
        parent_pid = int(fields[1])
    except ValueError:
        return None
    if fields[0].upper().startswith("Z"):
        return None
    return f"ps:{fields[2]}", parent_pid


def sample_record():
    remaining_seconds()
    if (Path("/proc") / "self" / "stat").is_file():
        record = linux_record()
    elif sys.platform == "darwin":
        record = darwin_record()
    else:
        record = portable_record()
    remaining_seconds()
    return record


try:
    first = sample_record()
    if first is None:
        raise SystemExit(1)
    time.sleep(min(0.005, remaining_seconds()))
    second = sample_record()
except TimeoutError:
    raise SystemExit(124)
if (
    second is None
    or first != second
    or second[1] != expected_parent
    or (expected_identity and second[0] != expected_identity)
):
    raise SystemExit(1)
if action == "capture":
    print(second[0])
    raise SystemExit(0)
if action == "alive":
    raise SystemExit(0)
if action != "signal" or signal_name not in {"INT", "TERM", "KILL"}:
    raise SystemExit(2)
try:
    os.kill(pid, getattr(signal, f"SIG{signal_name}"))
except ProcessLookupError:
    raise SystemExit(1)
PY
}

resource_monitor_job_running() {
  local expected_pid="$1"
  local active_pid=""
  while IFS= read -r active_pid; do
    [[ "${active_pid}" == "${expected_pid}" ]] && return 0
  done < <(jobs -pr)
  return 1
}

start_resource_monitor() {
  local database_identity="${RESOURCE_DATABASE_IDENTITY:-auto}"
  local generation=""
  local monitor_identity=""
  local output=""
  [[ -n "${SERVER_PID}" && -n "${SERVER_PGID}" \
    && -n "${SERVER_IDENTITY}" ]] || {
    warn "resource monitor cannot bind an unregistered RMDB process"
    RESOURCE_STATUS="unavailable"
    return 0
  }
  if [[ -n "${RESOURCE_PID}" ]]; then
    warn "stopping an unexpectedly active resource monitor before restart"
    stop_resource_monitor
  fi

  RESOURCE_GENERATION=$((RESOURCE_GENERATION + 1))
  generation="${RESOURCE_GENERATION}"
  output="${RESULT_DIR}/resource_segment_${generation}.json"
  RESOURCE_SEGMENT_PATH="${output}"
  printf '%s\n' "${output}" >>"${RESOURCE_SEGMENT_LIST}"

  if [[ ! -f "${RESOURCE_HELPER}" || -L "${RESOURCE_HELPER}" ]]; then
    warn "resource sampler is missing or unsafe; ranked workflow continues"
    RESOURCE_STATUS="unavailable"
    return 0
  fi
  RESOURCE_STATUS="collecting"
  python3 "${RESOURCE_HELPER}" sample \
    --run-id "${RUN_ID}" \
    --generation "${generation}" \
    --root-pid "${SERVER_PID}" \
    --root-identity "${SERVER_IDENTITY}" \
    --process-group "${SERVER_PGID}" \
    --database-path "${DB_PATH}" \
    --database-identity "${database_identity}" \
    --interval-ms "${RESOURCE_INTERVAL_MS}" \
    --output "${output}" \
    >"${RESULT_DIR}/resource_sampler_${generation}.log" 2>&1 &
  RESOURCE_PID=$!
  RESOURCE_PARENT_PID="$$"
  printf '%s\n' "${RESOURCE_PID}" >>"${RESULT_DIR}/resource_sampler.pids"
  if monitor_identity="$(
    resource_monitor_process_helper capture "${RESOURCE_PID}" "" \
      "${RESOURCE_PARENT_PID}"
  )"; then
    RESOURCE_IDENTITY="${monitor_identity}"
  else
    RESOURCE_IDENTITY=""
    warn "could not bind resource sampler ${RESOURCE_PID} to a stable child identity"
  fi
}

stop_resource_monitor() {
  local pid="${RESOURCE_PID}"
  local identity="${RESOURCE_IDENTITY}"
  local parent_pid="${RESOURCE_PARENT_PID}"
  local output="${RESOURCE_SEGMENT_PATH}"
  local attempts=0
  local monitor_rc=0
  local recaptured_identity=""
  local status=""
  [[ -n "${pid}" ]] || return 0

  if [[ -z "${identity}" || -z "${parent_pid}" ]]; then
    parent_pid="$$"
    if recaptured_identity="$(
      resource_monitor_process_helper capture "${pid}" "" "${parent_pid}"
    )"; then
      identity="${recaptured_identity}"
    fi
  fi

  if resource_monitor_job_running "${pid}"; then
    if [[ -n "${identity}" && -n "${parent_pid}" ]] \
      && resource_monitor_process_helper signal "${pid}" "${identity}" \
        "${parent_pid}" INT; then
      :
    else
      warn "refusing SIGINT for unverified resource sampler pid ${pid}"
    fi
    while resource_monitor_job_running "${pid}" \
      && [[ -n "${identity}" && -n "${parent_pid}" ]] \
      && resource_monitor_process_helper alive "${pid}" "${identity}" \
        "${parent_pid}" \
      && (( attempts < 100 )); do
      sleep 0.05
      attempts=$((attempts + 1))
    done
  fi
  if resource_monitor_job_running "${pid}"; then
    warn "resource sampler ${pid} did not stop after SIGINT; escalating"
    if [[ -n "${identity}" && -n "${parent_pid}" ]] \
      && resource_monitor_process_helper signal "${pid}" "${identity}" \
        "${parent_pid}" TERM; then
      attempts=0
    else
      warn "refusing SIGTERM for unverified resource sampler pid ${pid}"
      attempts=20
    fi
    while resource_monitor_job_running "${pid}" \
      && [[ -n "${identity}" && -n "${parent_pid}" ]] \
      && resource_monitor_process_helper alive "${pid}" "${identity}" \
        "${parent_pid}" \
      && (( attempts < 20 )); do
      sleep 0.05
      attempts=$((attempts + 1))
    done
  fi
  if resource_monitor_job_running "${pid}"; then
    if [[ -n "${identity}" && -n "${parent_pid}" ]] \
      && resource_monitor_process_helper signal "${pid}" "${identity}" \
        "${parent_pid}" KILL; then
      attempts=0
      while resource_monitor_job_running "${pid}" \
        && (( attempts < 20 )); do
        sleep 0.05
        attempts=$((attempts + 1))
      done
    else
      warn "refusing SIGKILL for unverified resource sampler pid ${pid}"
    fi
  fi
  if resource_monitor_job_running "${pid}"; then
    monitor_rc=125
    warn "resource sampler ${pid} remains active after safe bounded cleanup"
  else
    wait "${pid}" 2>/dev/null || monitor_rc=$?
  fi
  RESOURCE_PID=""
  RESOURCE_IDENTITY=""
  RESOURCE_PARENT_PID=""
  RESOURCE_SEGMENT_PATH=""
  if [[ "${monitor_rc}" != "0" ]]; then
    warn "resource sampler exited with status ${monitor_rc}; ranking is unchanged"
  fi

  if ! status="$(python3 - "${output}" <<'PY'
import json
from pathlib import Path
import sys

path = Path(sys.argv[1])
try:
    with path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
if (
    not isinstance(payload, dict)
    or payload.get("schema_version") != 1
    or payload.get("kind") != "rmdb_resource_segment"
    or payload.get("ranked") is not False
    or payload.get("score_effect") != "none"
    or payload.get("status")
    not in {"available", "partial", "unavailable", "failed"}
):
    raise SystemExit(1)
print(payload["status"])
PY
)"; then
    warn "resource sampler did not publish a valid terminal segment"
  elif [[ "${status}" != "available" ]]; then
    warn "resource segment ended ${status}; ranking is unchanged"
  fi
}

publish_rank_completion() {
  if [[ ! -f "${RESOURCE_HELPER}" || -L "${RESOURCE_HELPER}" ]]; then
    warn "resource sampler cannot bind rank completion; ranking is unchanged"
    return 0
  fi
  if ! python3 "${RESOURCE_HELPER}" complete-rank \
      --run-id "${RUN_ID}" \
      --timeline "${RESOURCE_TIMELINE}" \
      --output "${RESOURCE_RANK_COMPLETE}" \
      >>"${RESULT_DIR}/resource_aggregate.log" 2>&1; then
    warn "rank resource timeline is unavailable; ranking is unchanged"
  fi
}

read_resource_metrics_status() {
  python3 - "${RESOURCE_METRICS}" "${RUN_ID}" <<'PY'
import json
from pathlib import Path
import sys

path, run_id = Path(sys.argv[1]), sys.argv[2]
try:
    with path.open("r", encoding="utf-8") as stream:
        payload = json.load(stream)
except (OSError, json.JSONDecodeError):
    raise SystemExit(1)
if (
    not isinstance(payload, dict)
    or payload.get("schema_version") != 1
    or payload.get("kind") != "rmdb_resource_metrics"
    or payload.get("ranked") is not False
    or payload.get("score_effect") != "none"
):
    raise SystemExit(1)
status = payload.get("status")
if status == "failed":
    print(status)
    raise SystemExit(0)
if status not in {"available", "partial", "unavailable"}:
    raise SystemExit(1)
if payload.get("run_id") != run_id:
    raise SystemExit(1)
print(status)
PY
}

finalize_resource_metrics() {
  local -a command
  local segment=""
  local status=""
  [[ "${RESOURCE_FINALIZED}" == "0" ]] || return 0
  RESOURCE_FINALIZED=1
  stop_resource_monitor

  if [[ "${MODE}" == "tools" ]]; then
    RESOURCE_STATUS="not_applicable"
    return 0
  fi
  if (( RESOURCE_GENERATION == 0 )); then
    RESOURCE_STATUS="unavailable"
    warn "no RMDB resource generation was registered"
    return 0
  fi
  if [[ -z "${RESOURCE_DATABASE_IDENTITY}" ]]; then
    capture_resource_database_identity || {
      RESOURCE_STATUS="unavailable"
      return 0
    }
  fi
  if [[ ! -f "${RESOURCE_HELPER}" || -L "${RESOURCE_HELPER}" ]]; then
    RESOURCE_STATUS="unavailable"
    warn "resource sampler is missing or unsafe; no metrics were aggregated"
    return 0
  fi

  command=(
    python3 "${RESOURCE_HELPER}" aggregate
    --run-id "${RUN_ID}"
    --expected-generations "${RESOURCE_GENERATION}"
    --database-path "${DB_PATH}"
    --database-identity "${RESOURCE_DATABASE_IDENTITY}"
    --timeline "${RESOURCE_TIMELINE}"
    --rank-complete "${RESOURCE_RANK_COMPLETE}"
    --expected-warmup-seconds "${EFFECTIVE_WARMUP_SECONDS}"
    --expected-window-seconds "${EFFECTIVE_WINDOW_SECONDS}"
    --mode "${MODE}"
    --interval-ms "${RESOURCE_INTERVAL_MS}"
    --output "${RESOURCE_METRICS}"
  )
  while IFS= read -r segment; do
    [[ -n "${segment}" ]] || continue
    command+=(--segment "${segment}")
  done <"${RESOURCE_SEGMENT_LIST}"
  if ! "${command[@]}" >>"${RESULT_DIR}/resource_aggregate.log" 2>&1; then
    warn "resource aggregation failed; workflow and ranking remain unchanged"
  fi
  if ! status="$(read_resource_metrics_status)"; then
    RESOURCE_STATUS="failed"
    warn "resource metrics artifact is missing or invalid"
    return 0
  fi
  RESOURCE_STATUS="${status}"
  if [[ "${RESOURCE_STATUS}" != "available" ]]; then
    warn "resource observation is ${RESOURCE_STATUS}; ranking is unchanged"
  fi
  return 0
}

clear_server_registration() {
  SERVER_PID=""
  SERVER_PGID=""
  SERVER_IDENTITY=""
  STOPPING_SERVER=0
}

stop_server() {
  local pid="${SERVER_PID}"
  local wait_status=0
  local phase_deadline=""
  [[ -n "${pid}" ]] || return 0
  if [[ "${STOPPING_SERVER}" == "1" ]]; then
    warn "server shutdown is already in progress"
    return 1
  fi
  STOPPING_SERVER=1
  if [[ -z "${SERVER_IDENTITY}" ]] \
    && ! establish_cleanup_identity "${pid}"; then
    warn "could not prove ownership of unregistered RMDB pid ${pid}"
    STOPPING_SERVER=0
    return 1
  fi

  phase_deadline=$(( $(monotonic_millis) + 10000 ))
  if ! server_process_helper signal INT "${phase_deadline}"; then
    warn "refusing unsafe RMDB shutdown for registered pid ${pid}"
    STOPPING_SERVER=0
    return 1
  fi
  wait_for_server_group_exit "${phase_deadline}" || wait_status=$?
  if (( wait_status > 1 )); then
    warn "could not safely inspect registered RMDB process group ${SERVER_PGID}"
    STOPPING_SERVER=0
    return 1
  fi
  if [[ "${wait_status}" == "1" ]]; then
    phase_deadline=$(( $(monotonic_millis) + 5000 ))
    if ! server_process_helper signal TERM "${phase_deadline}"; then
      warn "refusing unsafe RMDB TERM escalation for registered pid ${pid}"
      STOPPING_SERVER=0
      return 1
    fi
    wait_status=0
    wait_for_server_group_exit "${phase_deadline}" || wait_status=$?
    if (( wait_status > 1 )); then
      warn "could not safely inspect registered RMDB process group ${SERVER_PGID}"
      STOPPING_SERVER=0
      return 1
    fi
  fi
  if [[ "${wait_status}" == "1" ]]; then
    phase_deadline=$(( $(monotonic_millis) + 5000 ))
    if ! server_process_helper signal KILL "${phase_deadline}"; then
      warn "refusing unsafe RMDB KILL escalation for registered pid ${pid}"
      STOPPING_SERVER=0
      return 1
    fi
    wait_status=0
    wait_for_server_group_exit "${phase_deadline}" || wait_status=$?
    if [[ "${wait_status}" != "0" ]]; then
      warn "registered RMDB process group ${SERVER_PGID} did not terminate"
      STOPPING_SERVER=0
      return 1
    fi
  fi
  wait "${pid}" 2>/dev/null || true
  stop_resource_monitor
  phase_deadline=$(( $(monotonic_millis) + 5000 ))
  if ! wait_for_listener_gone "${phase_deadline}"; then
    warn "listener ${HOST}:${PORT} remained after registered RMDB shutdown"
    clear_server_registration
    return 1
  fi
  clear_server_registration
}

stop_probe() {
  local pid="${PROBE_PID}"
  [[ -n "${pid}" ]] || return 0
  if kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    sleep 0.05
  fi
  if kill -0 "${pid}" 2>/dev/null; then
    kill -KILL "${pid}" 2>/dev/null || true
  fi
  wait "${pid}" 2>/dev/null || true
  PROBE_PID=""
}

stop_trace() {
  local pid="${TRACE_PID}"
  local attempts=0
  local trace_rc=0
  local forced_kill=0
  [[ -n "${pid}" ]] || return 0
  if kill -0 "${pid}" 2>/dev/null; then
    kill -INT "${pid}" 2>/dev/null || true
    while kill -0 "${pid}" 2>/dev/null && (( attempts < 40 )); do
      sleep 0.05
      attempts=$((attempts + 1))
    done
  fi
  if kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    attempts=0
    while kill -0 "${pid}" 2>/dev/null && (( attempts < 20 )); do
      sleep 0.05
      attempts=$((attempts + 1))
    done
  fi
  if kill -0 "${pid}" 2>/dev/null; then
    forced_kill=1
    kill -KILL "${pid}" 2>/dev/null || true
  fi
  wait "${pid}" 2>/dev/null || trace_rc=$?
  if [[ "${forced_kill}" == "1" && "${trace_rc}" == "0" ]]; then
    trace_rc=137
  fi
  TRACE_EXIT_STATUS="${trace_rc}"
  TRACE_PID=""
  case "${trace_rc}" in
    0|130|143) return 0 ;;
    *) return "${trace_rc}" ;;
  esac
}

crash_server() {
  local pid="${SERVER_PID}"
  local crash_deadline=""
  [[ -n "${pid}" ]] || die "cannot crash an unregistered server"
  crash_deadline=$(( $(monotonic_millis) + 5000 ))
  server_process_helper root-alive "" "${crash_deadline}" \
    || die "registered server ${pid} is not the live RMDB process"
  log "SIGKILL registered RMDB process group ${SERVER_PGID}"
  force_stop_server 5000 \
    || die "registered RMDB process group did not terminate safely"
}

force_stop_server() {
  local listener_timeout_millis="$1"
  local pid="${SERVER_PID}"
  local process_deadline=""
  local listener_deadline=""
  [[ -n "${pid}" ]] || return 0
  if [[ -z "${SERVER_IDENTITY}" ]] \
    && ! establish_cleanup_identity "${pid}"; then
    return 1
  fi
  process_deadline=$(( $(monotonic_millis) + 5000 ))
  server_process_helper signal KILL "${process_deadline}" \
    || return 1
  wait_for_server_group_exit "${process_deadline}" \
    || return 1
  wait "${pid}" 2>/dev/null || true
  stop_resource_monitor
  listener_deadline=$(( $(monotonic_millis) + listener_timeout_millis ))
  if ! wait_for_listener_gone "${listener_deadline}"; then
    clear_server_registration
    return 1
  fi
  clear_server_registration
}

force_stop_unregistered_child() {
  local listener_timeout_millis="$1"
  local pid="${SERVER_PID}"
  local captured_identity="${SERVER_IDENTITY}"
  local identity_before=""
  local identity_after=""
  local proof_deadline=""
  local process_deadline=""
  local listener_deadline=""
  [[ -n "${pid}" ]] || return 0

  proof_deadline=$(( $(monotonic_millis) + 1000 ))
  identity_before="$(
    process_identity "${pid}" "${proof_deadline}"
  )" || return 1
  if [[ -n "${captured_identity}" \
    && "${captured_identity}" != "${identity_before}" ]]; then
    return 1
  fi
  process_owner_matches "${pid}" "${proof_deadline}" || return 1
  identity_after="$(
    process_identity "${pid}" "${proof_deadline}"
  )" || return 1
  [[ "${identity_before}" == "${identity_after}" ]] || return 1
  SERVER_IDENTITY="${identity_after}"
  SERVER_PGID="${pid}"

  process_deadline=$(( $(monotonic_millis) + 5000 ))
  server_process_helper signal-root KILL "${process_deadline}" || return 1
  wait_for_server_root_exit "${process_deadline}" || return 1
  wait "${pid}" 2>/dev/null || true
  listener_deadline=$(( $(monotonic_millis) + listener_timeout_millis ))
  if ! wait_for_listener_gone "${listener_deadline}"; then
    clear_server_registration
    return 1
  fi
  clear_server_registration
}

remove_current_owned_database() {
  [[ "${DB_OWNED}" == "1" ]] || return 0
  [[ "${DB_PATH}" == "${RMDB_DIR}/${DB_NAME}" ]] \
    || die "internal database path invariant failed"
  [[ -d "${DB_PATH}" && ! -L "${DB_PATH}" ]] || return 0
  owned_marker_matches "${DB_MARKER}" \
    || die "refusing to remove database without the current run ownership marker"
  log "removing current run-owned database ${DB_PATH}"
  rm -rf -- "${DB_PATH}"
  DB_OWNED=0
}

remove_current_run_temp() {
  [[ "${RUN_TEMP_DIR}" == "${RMDB_DIR}/.tpcc-workflow/${RUN_ID}" ]] || return 0
  [[ -d "${RUN_TEMP_DIR}" && ! -L "${RUN_TEMP_DIR}" ]] || return 0
  owned_marker_matches "${RUN_MARKER}" || return 0
  rm -rf -- "${RUN_TEMP_DIR}"
}

cleanup() {
  CLEANUP_RC=$?
  local rc="${CLEANUP_RC}"
  trap - EXIT
  stop_trace || true
  stop_probe || true
  stop_server || true
  stop_resource_monitor || true
  finalize_resource_metrics || true
  if [[ "${rc}" != "0" && "${MANIFEST_READY}" == "1" ]]; then
    WORKFLOW_STATUS="failed"
    mark_unfinished_phases_after_failure
    write_manifest || true
  fi
  if [[ "${WORKFLOW_SUCCEEDED}" == "1" && "${CLEAN_DB_ON_EXIT}" == "1" ]]; then
    remove_current_owned_database || rc=$?
  fi
  remove_current_run_temp || true
  exit "${rc}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

check_cmake_cache_source() {
  local cache="${RMDB_DIR}/${BUILD_DIR}/CMakeCache.txt"
  local cached_source=""
  [[ -f "${cache}" ]] || return 0
  cached_source="$(sed -n 's/^CMAKE_HOME_DIRECTORY:INTERNAL=//p' "${cache}" | head -n 1)"
  [[ -z "${cached_source}" || "${cached_source}" == "${RMDB_DIR}" ]] || {
    die "stale CMake cache: ${cache} belongs to ${cached_source}; choose a fresh --build-dir (the workflow will not delete it)"
  }
}

portable_jobs() {
  local jobs=""
  jobs="$(getconf _NPROCESSORS_ONLN 2>/dev/null || true)"
  if ! is_uint "${jobs}" || (( 10#${jobs} <= 0 )); then
    jobs="$(sysctl -n hw.ncpu 2>/dev/null || true)"
  fi
  if ! is_uint "${jobs}" || (( 10#${jobs} <= 0 )); then
    jobs=2
  fi
  printf '%s\n' "${jobs}"
}

ensure_binaries() {
  if [[ "${SKIP_BUILD}" != "1" ]]; then
    if [[ -z "${TPCC_BIN_OVERRIDE}" ]]; then
      [[ -f "${TPCC_DIR}/Cargo.toml" ]] \
        || die "Cargo.toml not found in TPCC-Tester root: ${TPCC_DIR}"
      log "building TPCC-Tester"
      cargo build --release --manifest-path "${TPCC_DIR}/Cargo.toml" \
        >"${RESULT_DIR}/tpcc_build.log" 2>&1
    fi

    check_cmake_cache_source
    log "configuring RMDB"
    cmake -S "${RMDB_DIR}" -B "${RMDB_DIR}/${BUILD_DIR}" \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DCMAKE_CXX_FLAGS_RELWITHDEBINFO="-O2 -g -fno-omit-frame-pointer" \
      >"${RESULT_DIR}/cmake_configure.log" 2>&1
    log "building RMDB"
    cmake --build "${RMDB_DIR}/${BUILD_DIR}" --target rmdb \
      -j"$(portable_jobs)" >"${RESULT_DIR}/cmake_build.log" 2>&1
  fi
  [[ -x "${TPCC_BIN}" ]] || die "tpcc-tester is not executable: ${TPCC_BIN}"
  [[ -x "${SERVER_BIN}" ]] || die "RMDB server is not executable: ${SERVER_BIN}"
}

ensure_port_available() {
  if ! port_is_available; then
    die "port ${HOST}:${PORT} is already in use; the workflow will not kill its owner"
  fi
}

probe_ready() {
  local absolute_deadline_millis="$1"
  python3 - "${absolute_deadline_millis}" "${TPCC_DIR}" "${TPCC_BIN}" \
    "${HOST}" "${PORT}" <<'PY' >>"${RESULT_DIR}/ready_probe.log" 2>&1 &
import os
import signal
import sys
import time

deadline_millis = int(sys.argv[1])
working_directory, executable, host, port = sys.argv[2:]
started_millis = time.monotonic_ns() // 1_000_000
if started_millis >= deadline_millis:
    raise SystemExit(124)

probe_pid = None


def kill_probe():
    if probe_pid is None:
        return
    try:
        os.killpg(probe_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        os.kill(probe_pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def terminate_probe(signum, _frame):
    kill_probe()
    if probe_pid is not None:
        try:
            os.waitpid(probe_pid, 0)
        except ChildProcessError:
            pass
    raise SystemExit(128 + signum)


blocked_signals = {signal.SIGINT, signal.SIGTERM}
signal.pthread_sigmask(signal.SIG_BLOCK, blocked_signals)
signal.signal(signal.SIGINT, terminate_probe)
signal.signal(signal.SIGTERM, terminate_probe)
try:
    probe_pid = os.fork()
    if probe_pid == 0:
        try:
            os.setsid()
            signal.pthread_sigmask(signal.SIG_UNBLOCK, blocked_signals)
            os.chdir(working_directory)
            os.execv(
                executable,
                [
                    executable,
                    "--probe-ready",
                    "--host",
                    host,
                    "--port",
                    port,
                ],
            )
        except BaseException as error:
            print(f"could not start readiness probe: {error}", file=sys.stderr)
            os._exit(127)
except OSError as error:
    print(f"could not fork readiness probe: {error}", file=sys.stderr)
finally:
    signal.pthread_sigmask(signal.SIG_UNBLOCK, blocked_signals)
if probe_pid is None:
    raise SystemExit(127)

while True:
    waited_pid, status = os.waitpid(probe_pid, os.WNOHANG)
    if waited_pid == probe_pid:
        exit_code = os.waitstatus_to_exitcode(status)
        raise SystemExit(exit_code if exit_code >= 0 else 128 - exit_code)
    now_millis = time.monotonic_ns() // 1_000_000
    if now_millis >= deadline_millis:
        kill_probe()
        try:
            os.waitpid(probe_pid, 0)
        except ChildProcessError:
            pass
        raise SystemExit(124)
    time.sleep(min(0.01, (deadline_millis - now_millis) / 1000.0))
PY
  PROBE_PID=$!
  local probe_rc=0
  wait "${PROBE_PID}" || probe_rc=$?
  PROBE_PID=""
  return "${probe_rc}"
}

wait_for_ready() {
  local deadline_millis="$1"
  local now_millis=""
  local listener_status=0
  while true; do
    now_millis="$(monotonic_millis)"
    (( now_millis < deadline_millis )) || return 1
    if [[ -z "${SERVER_PID}" ]] \
      || ! server_process_helper root-alive "" "${deadline_millis}"; then
      return 1
    fi

    if server_process_helper listener "" "${deadline_millis}"; then
      if probe_ready "${deadline_millis}"; then
        now_millis="$(monotonic_millis)"
        (( now_millis <= deadline_millis )) || return 1
        server_process_helper listener "" "${deadline_millis}" || return 1
        now_millis="$(monotonic_millis)"
        (( now_millis <= deadline_millis )) || return 1
        return 0
      fi
    else
      listener_status=$?
      if [[ "${listener_status}" != "1" ]]; then
        return 1
      fi
    fi
    now_millis="$(monotonic_millis)"
    (( now_millis < deadline_millis )) || return 1
    python3 - "${deadline_millis}" <<'PY'
import sys
import time

remaining = (int(sys.argv[1]) - time.monotonic_ns() // 1_000_000) / 1000.0
if remaining > 0:
    time.sleep(min(0.25, remaining))
PY
  done
}

register_server_process() {
  local pid="$1"
  local readiness_deadline_millis="$2"
  local registration_deadline_millis=""
  local now_millis=""
  local current_pgid=""
  if ! SERVER_IDENTITY="$(
    process_identity "${pid}" "${readiness_deadline_millis}"
  )"; then
    warn "could not capture registered RMDB process identity"
    return 1
  fi
  now_millis="$(monotonic_millis)"
  registration_deadline_millis=$(( now_millis + 2000 ))
  if (( registration_deadline_millis > readiness_deadline_millis )); then
    registration_deadline_millis="${readiness_deadline_millis}"
  fi
  while true; do
    if ! server_process_helper root-alive "" "${readiness_deadline_millis}"; then
      warn "RMDB exited before its process group could be registered"
      return 1
    fi
    if ! current_pgid="$(python3 - "${pid}" <<'PY'
import os
import sys

try:
    print(os.getpgid(int(sys.argv[1])))
except ProcessLookupError:
    raise SystemExit(1)
PY
)"; then
      warn "could not inspect registered RMDB process group"
      return 1
    fi
    if [[ "${current_pgid}" == "${pid}" ]]; then
      SERVER_PGID="${current_pgid}"
      return 0
    fi
    if (( $(monotonic_millis) >= registration_deadline_millis )); then
      warn "RMDB did not enter its dedicated process group before readiness deadline"
      return 1
    fi
    sleep 0.02
  done
}

start_server() {
  local purpose="$1"
  local readiness_started_millis=""
  local readiness_deadline_millis=""
  ensure_port_available
  printf '\n[server start: %s]\n' "${purpose}" >>"${SERVER_LOG}"
  log "starting RMDB for ${purpose}"
  readiness_started_millis="$(monotonic_millis)"
  readiness_deadline_millis=$(( (10#${READY_TIMEOUT_SECONDS} * 1000) \
    + readiness_started_millis ))
  (
    cd "${RMDB_DIR}"
    exec env RMDB_PORT="${PORT}" \
      RMDB_WORKFLOW_PROCESS_OWNER="${PROCESS_OWNER_TOKEN}" python3 -c \
      'import os, sys
if os.getpgrp() != os.getpid():
    os.setpgid(0, 0)
os.execv(sys.argv[1], sys.argv[1:])' \
      "${SERVER_BIN}" "${DB_NAME}"
  ) >>"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  SERVER_PGID="${SERVER_PID}"
  if ! register_server_process \
      "${SERVER_PID}" "${readiness_deadline_millis}"; then
    if ! force_stop_server 1000 \
      && ! force_stop_unregistered_child 1000; then
      warn "could not safely stop RMDB after process registration failure"
    fi
    die "RMDB process registration exceeded the shared readiness budget"
  fi
  start_resource_monitor
  printf '%s\n' "${SERVER_PID}" >"${RESULT_DIR}/server.pid"
  if ! wait_for_ready "${readiness_deadline_millis}"; then
    force_stop_server 1000 || true
    die "RMDB did not pass the exact show-tables readiness probe within ${READY_TIMEOUT_SECONDS}s; see ${SERVER_LOG}"
  fi
}

claim_new_database() {
  [[ -d "${DB_PATH}" && ! -L "${DB_PATH}" ]] \
    || die "RMDB did not create the expected database directory: ${DB_PATH}"
  [[ ! -e "${DB_MARKER}" ]] \
    || die "new database unexpectedly already contains an ownership marker"
  printf '%s\n' "${OWNER_TOKEN}" >"${DB_MARKER}"
  DB_OWNED=1
}

start_new_database() {
  if [[ -e "${DB_PATH}" || -L "${DB_PATH}" ]]; then
    die "database path already exists: ${DB_PATH}; choose another --db-name or remove it explicitly"
  fi
  start_server "new database setup"
  claim_new_database
  capture_resource_database_identity || true
}

start_existing_database() {
  [[ -d "${DB_PATH}" && ! -L "${DB_PATH}" ]] \
    || die "existing database directory is missing or unsafe: ${DB_PATH}"
  capture_resource_database_identity || true
  start_server "existing database"
}

run_tester() {
  local log_path="$1"
  local -a tester_environment
  shift
  tester_environment=(
    "RMDB_TPCC_CSV_DIR=${CSV_DIR}"
    "RMDB_TPCC_LOAD_DIR=${LOAD_DIR}"
    "RMDB_TPCC_RUN_ID=${DATASET_RUN_ID}"
  )
  if [[ -n "${TESTER_RESOURCE_TIMELINE}" ]]; then
    tester_environment+=(
      "RMDB_TPCC_RESOURCE_TIMELINE_FILE=${TESTER_RESOURCE_TIMELINE}"
    )
  fi
  (
    cd "${TPCC_DIR}"
    env "${tester_environment[@]}" "${TPCC_BIN}" "$@" \
      --recovery-ready-budget-seconds "${READY_TIMEOUT_SECONDS}"
  ) >"${log_path}" 2>&1
}

run_profile_tester() {
  local log_path="$1"
  shift
  local -a command
  command=("$@")
  if [[ "${ALLOW_DEVIATION}" == "1" ]]; then
    command+=(--allow-deviation)
    [[ -z "${SCALE}" ]] || command+=(--scale "${SCALE}")
    [[ -z "${CLIENTS}" ]] || command+=(--clients "${CLIENTS}")
    [[ -z "${WARMUP_SECONDS}" ]] \
      || command+=(--warmup-seconds "${WARMUP_SECONDS}")
    [[ -z "${WINDOW_SECONDS}" ]] \
      || command+=(--window-seconds "${WINDOW_SECONDS}")
  fi
  run_tester "${log_path}" "${command[@]}"
}

run_setup() {
  log "creating and loading final2026 dataset"
  set_phase_status setup running
  if run_profile_tester "${RESULT_DIR}/setup.log" \
      --create-schema --init --check --check-scope setup \
      --profile "${PROFILE}" --seed "${SEED}" --state-dir "${STATE_DIR}" \
      --host "${HOST}" --port "${PORT}"; then
    set_phase_status setup passed
  else
    set_phase_status setup failed
    die "TPC-C setup failed; see ${RESULT_DIR}/setup.log"
  fi
}

run_rank() {
  local rank_rc=0
  log "running one Rust-owned final2026 benchmark"
  set_phase_status rank running
  TESTER_RESOURCE_TIMELINE="${RESOURCE_TIMELINE}"
  run_profile_tester "${RESULT_DIR}/rank.log" \
      --benchmark --profile "${PROFILE}" --seed "${SEED}" \
      --state-dir "${STATE_DIR}" \
      --host "${HOST}" --port "${PORT}" || rank_rc=$?
  TESTER_RESOURCE_TIMELINE=""
  if [[ "${rank_rc}" == "0" ]]; then
    publish_rank_completion
    set_phase_status rank passed
  else
    set_phase_status rank failed
    die "TPC-C ranking failed; see ${RESULT_DIR}/rank.log"
  fi
}

run_check() {
  local scope="$1"
  log "running ${scope} consistency checks"
  set_phase_status "${scope}" running
  if run_profile_tester "${RESULT_DIR}/check_${scope}.log" \
      --check --check-scope "${scope}" --profile "${PROFILE}" --seed "${SEED}" \
      --state-dir "${STATE_DIR}" \
      --host "${HOST}" --port "${PORT}"; then
    set_phase_status "${scope}" passed
  else
    set_phase_status "${scope}" failed
    die "TPC-C ${scope} checks failed; see ${RESULT_DIR}/check_${scope}.log"
  fi
}

run_crash_restart() {
  set_phase_status crash_restart running
  crash_server
  start_existing_database
  set_phase_status crash_restart passed
}

run_final_diagnostics() {
  [[ "${DIAGNOSTICS_REQUESTED}" == "1" ]] || return 0
  if [[ "${PHASE_RANK}" != "passed" \
    || "${PHASE_ONLINE}" != "passed" \
    || "${PHASE_RECOVERY}" != "passed" ]]; then
    die "internal diagnostics gate opened before rank, online, and recovery passed"
  fi

  set_phase_status diagnostics running
  if ! command -v strace >/dev/null 2>&1; then
    warn "strace is unavailable; skipping non-ranked 10s warmup + 60s observation"
    set_phase_status diagnostics unavailable
    return 0
  fi
  if [[ ! -f "${DIAGNOSTIC_METRICS_HELPER}" \
    || -L "${DIAGNOSTIC_METRICS_HELPER}" ]]; then
    warn "diagnostic metrics helper is missing or unsafe; ranked result remains valid"
    set_phase_status diagnostics failed
    return 0
  fi

  log "running ${DIAGNOSTIC_WARMUP_SECONDS}s non-ranked diagnostic warmup"
  if ! run_tester "${RESULT_DIR}/diagnostic_warmup.log" \
      --diagnostic-workload-seconds "${DIAGNOSTIC_WARMUP_SECONDS}" \
      --diagnostic-segment warmup \
      --profile "${PROFILE}" --seed "${SEED}" --state-dir "${STATE_DIR}" \
      --host "${HOST}" --port "${PORT}"; then
    warn "diagnostic warmup failed; ranked result remains valid"
    set_phase_status diagnostics failed
    return 0
  fi

  log "attaching strace to registered RMDB pid ${SERVER_PID}"
  LC_ALL=C strace -c -f -p "${SERVER_PID}" \
    -o "${RESULT_DIR}/diagnostic_strace_summary.txt" \
    >"${RESULT_DIR}/strace_attach.log" 2>&1 &
  TRACE_PID=$!
  sleep 0.20
  if ! kill -0 "${TRACE_PID}" 2>/dev/null; then
    wait "${TRACE_PID}" 2>/dev/null || true
    TRACE_PID=""
    warn "strace could not attach; see ${RESULT_DIR}/strace_attach.log"
    set_phase_status diagnostics unavailable
    return 0
  fi

  local diagnostics_failed=0
  local tracer_survived_observation=1
  if ! python3 "${DIAGNOSTIC_METRICS_HELPER}" capture \
      --pid "${SERVER_PID}" \
      --output "${RESULT_DIR}/diagnostic_proc_before.json" \
      --require-available; then
    warn "could not capture the pre-observation process counters"
    diagnostics_failed=1
  fi

  log "running ${DIAGNOSTIC_OBSERVATION_SECONDS}s non-ranked diagnostic observation"
  if ! run_tester "${RESULT_DIR}/diagnostic_observation.log" \
      --diagnostic-workload-seconds "${DIAGNOSTIC_OBSERVATION_SECONDS}" \
      --diagnostic-segment observation \
      --profile "${PROFILE}" --seed "${SEED}" --state-dir "${STATE_DIR}" \
      --host "${HOST}" --port "${PORT}"; then
    warn "diagnostic observation failed; ranked result remains valid"
    diagnostics_failed=1
  fi

  if ! python3 "${DIAGNOSTIC_METRICS_HELPER}" capture \
      --pid "${SERVER_PID}" \
      --output "${RESULT_DIR}/diagnostic_proc_after.json" \
      --require-available; then
    warn "could not capture the post-observation process counters"
    diagnostics_failed=1
  fi

  if ! kill -0 "${TRACE_PID}" 2>/dev/null; then
    tracer_survived_observation=0
    wait "${TRACE_PID}" 2>/dev/null || TRACE_EXIT_STATUS=$?
    TRACE_PID=""
    warn "strace exited before the diagnostic observation completed"
    diagnostics_failed=1
  elif ! stop_trace; then
    warn "strace exited abnormally with status ${TRACE_EXIT_STATUS}"
    diagnostics_failed=1
  fi

  if [[ -f "${RESULT_DIR}/diagnostic_proc_before.json" \
    && -f "${RESULT_DIR}/diagnostic_proc_after.json" ]]; then
    if ! python3 "${DIAGNOSTIC_METRICS_HELPER}" delta \
        --before "${RESULT_DIR}/diagnostic_proc_before.json" \
        --after "${RESULT_DIR}/diagnostic_proc_after.json" \
        --output "${RESULT_DIR}/diagnostic_proc_delta.json" \
        --require-available; then
      warn "could not calculate process-counter deltas"
      diagnostics_failed=1
    fi
  else
    diagnostics_failed=1
  fi

  if [[ "${tracer_survived_observation}" == "1" \
    && -s "${RESULT_DIR}/diagnostic_strace_summary.txt" ]]; then
    if ! python3 "${DIAGNOSTIC_METRICS_HELPER}" strace \
        --input "${RESULT_DIR}/diagnostic_strace_summary.txt" \
        --output "${RESULT_DIR}/diagnostic_strace_metrics.json"; then
      warn "could not parse the strace -c summary"
      diagnostics_failed=1
    fi
  else
    warn "strace did not produce a non-empty summary"
    diagnostics_failed=1
  fi

  if [[ "${diagnostics_failed}" == "1" ]]; then
    warn "one or more diagnostic observations failed; ranked result remains valid"
    set_phase_status diagnostics failed
  else
    set_phase_status diagnostics passed
  fi
  return 0
}

write_summary() {
  {
    echo "# TPCC final2026 workflow"
    echo
    echo "- mode: ${MODE}"
    echo "- profile: ${PROFILE}"
    echo "- seed: ${SEED}"
    echo "- status: success"
    echo "- Rust owns warmup, all three formal windows, and semantic gates."
    echo "- final diagnostics: ${PHASE_DIAGNOSTICS} (never ranked)"
    echo "- resource observation: ${RESOURCE_STATUS} (never ranked)"
  } >"${RESULT_DIR}/summary.md"
}

write_manifest
record_tools

if [[ "${MODE}" == "tools" ]]; then
  WORKFLOW_STATUS="success"
  write_manifest
  write_summary
  WORKFLOW_SUCCEEDED=1
  log "tool report written to ${RESULT_DIR}"
  exit 0
fi

ensure_binaries

case "${MODE}" in
  init)
    start_new_database
    run_setup
    stop_server
    ;;
  rank)
    if [[ "${INIT_BEFORE_RUN}" == "1" ]]; then
      start_new_database
      run_setup
    else
      start_existing_database
    fi
    run_rank
    run_check online
    stop_server
    ;;
  recovery)
    start_existing_database
    run_check recovery
    stop_server
    ;;
  all)
    start_new_database
    run_setup
    run_rank
    run_check online
    run_crash_restart
    run_check recovery
    run_final_diagnostics
    stop_server
    ;;
esac

finalize_resource_metrics
WORKFLOW_STATUS="success"
write_manifest
write_summary
WORKFLOW_SUCCEEDED=1
log "results written to ${RESULT_DIR}"
