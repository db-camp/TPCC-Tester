#!/usr/bin/env bash
set -euo pipefail

# This script owns process and filesystem lifecycle only. The Rust tester owns
# the final2026 workload schedule, warmup, measurement windows, deadlines, and
# semantic checks.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
DEFAULT_RMDB_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd -P)"
DEFAULT_TPCC_DIR="$(cd "${SCRIPT_DIR}/.." && pwd -P)"

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
READY_TIMEOUT_SECONDS="90"
SKIP_BUILD=0
PLAN_ONLY=0
INIT_BEFORE_RUN=0
CLEAN_DB_ON_EXIT="auto"
ALLOW_DEVIATION=0
SCALE=""
CLIENTS=""
WARMUP_SECONDS=""
WINDOW_SECONDS=""
SERVER_BIN_OVERRIDE=""
TPCC_BIN_OVERRIDE=""

SERVER_PID=""
PROBE_PID=""
STOPPING_SERVER=0
SERVER_LOG=""
RESULT_DIR=""
RUN_TEMP_DIR=""
CSV_DIR=""
LOAD_DIR=""
RUN_MARKER=""
DB_MARKER=""
OWNER_TOKEN=""
DB_PATH=""
DB_OWNED=0
WORKFLOW_SUCCEEDED=0

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
  --host <host>
  --port <port>
  --db-name <safe-single-component>

Paths/build:
  --target-dir <RMDB-root>
  --tpcc-dir <TPCC-Tester-root>
  --record-root <result-root>
  --build-dir <relative-single-component>
  --server-bin <path>
  --tpcc-bin <path>
  --skip-build

Lifecycle:
  --init-db                    Initialize before --mode rank
  --ready-timeout-seconds <n>  Default: 90
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
    --build-dir)
      need_value "$1" "${2-}"; BUILD_DIR="$2"; shift 2 ;;
    --host)
      need_value "$1" "${2-}"; HOST="$2"; shift 2 ;;
    --port)
      need_value "$1" "${2-}"; PORT="$2"; shift 2 ;;
    --profile)
      need_value "$1" "${2-}"; PROFILE="$2"; shift 2 ;;
    --seed)
      need_value "$1" "${2-}"; SEED="$2"; shift 2 ;;
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

validate_component "--db-name" "${DB_NAME}"
validate_component "--label" "${LABEL}"
validate_component "--build-dir" "${BUILD_DIR}"
[[ "${PROFILE}" == "final2026" ]] || die "only --profile final2026 is supported"
validate_nonnegative_integer "--seed" "${SEED}"
validate_positive_integer "--port" "${PORT}"
(( 10#${PORT} <= 65535 )) || die "--port must be at most 65535"
validate_positive_integer "--ready-timeout-seconds" "${READY_TIMEOUT_SECONDS}"

if [[ -n "${SCALE}${CLIENTS}${WARMUP_SECONDS}${WINDOW_SECONDS}" ]] \
  && [[ "${ALLOW_DEVIATION}" != "1" ]]; then
  die "local sizing/timing overrides require --allow-deviation"
fi
if [[ "${ALLOW_DEVIATION}" == "1" ]]; then
  [[ -z "${SCALE}" ]] || validate_positive_integer "--scale" "${SCALE}"
  [[ -z "${CLIENTS}" ]] || validate_positive_integer "--clients" "${CLIENTS}"
  [[ -z "${WARMUP_SECONDS}" ]] \
    || validate_nonnegative_integer "--warmup-seconds" "${WARMUP_SECONDS}"
  [[ -z "${WINDOW_SECONDS}" ]] \
    || validate_positive_integer "--window-seconds" "${WINDOW_SECONDS}"
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

if [[ "${CLEAN_DB_ON_EXIT}" == "auto" ]]; then
  if [[ "${MODE}" == "all" ]]; then
    CLEAN_DB_ON_EXIT=1
  else
    CLEAN_DB_ON_EXIT=0
  fi
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
seed=${SEED}
rmdb_dir=${RMDB_DIR}
tpcc_dir=${TPCC_DIR}
build_dir=${RMDB_DIR}/${BUILD_DIR}
server_bin=${SERVER_BIN}
tpcc_bin=${TPCC_BIN}
db_name=${DB_NAME}
db_path=${DB_PATH}
result_dir=${RESULT_DIR}
csv_dir=${CSV_DIR}
host=${HOST}
port=${PORT}
ready_probe=tpcc-tester --probe-ready --host ${HOST} --port ${PORT}
schedule_owner=rust
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

write_manifest() {
  {
    print_plan
    echo "allow_deviation=${ALLOW_DEVIATION}"
    echo "scale=${SCALE}"
    echo "clients=${CLIENTS}"
    echo "warmup_seconds=${WARMUP_SECONDS}"
    echo "window_seconds=${WINDOW_SECONDS}"
  } >"${RESULT_DIR}/manifest.txt"
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

stop_server() {
  local pid="${SERVER_PID}"
  local attempts=0
  [[ -n "${pid}" ]] || return 0
  if [[ "${STOPPING_SERVER}" == "1" ]]; then
    kill -KILL "${pid}" 2>/dev/null || true
    wait "${pid}" 2>/dev/null || true
    SERVER_PID=""
    return 0
  fi
  STOPPING_SERVER=1
  if ! kill -0 "${pid}" 2>/dev/null; then
    wait "${pid}" 2>/dev/null || true
    SERVER_PID=""
    STOPPING_SERVER=0
    return 0
  fi
  kill -INT "${pid}" 2>/dev/null || true
  while kill -0 "${pid}" 2>/dev/null && (( attempts < 40 )); do
    sleep 0.25
    attempts=$((attempts + 1))
  done
  if kill -0 "${pid}" 2>/dev/null; then
    kill -TERM "${pid}" 2>/dev/null || true
    attempts=0
    while kill -0 "${pid}" 2>/dev/null && (( attempts < 20 )); do
      sleep 0.25
      attempts=$((attempts + 1))
    done
  fi
  if kill -0 "${pid}" 2>/dev/null; then
    kill -KILL "${pid}" 2>/dev/null || true
  fi
  wait "${pid}" 2>/dev/null || true
  SERVER_PID=""
  STOPPING_SERVER=0
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

crash_server() {
  local pid="${SERVER_PID}"
  [[ -n "${pid}" ]] || die "cannot crash an unregistered server"
  kill -0 "${pid}" 2>/dev/null || die "registered server ${pid} is not running"
  log "SIGKILL registered RMDB pid ${pid}"
  kill -KILL "${pid}"
  wait "${pid}" 2>/dev/null || true
  SERVER_PID=""
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
  stop_probe || true
  stop_server || true
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
  if ! python3 - "${HOST}" "${PORT}" <<'PY'
import socket
import sys

host, port = sys.argv[1], int(sys.argv[2])
addresses = {
    (family, socktype, proto, address)
    for family, socktype, proto, _, address in socket.getaddrinfo(
        host, port, type=socket.SOCK_STREAM)
}
if not addresses:
    raise SystemExit(1)
for family, socktype, proto, address in addresses:
    sock = socket.socket(family, socktype, proto)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(address)
    except OSError:
        sock.close()
        raise SystemExit(1)
    sock.close()
raise SystemExit(0)
PY
  then
    die "port ${HOST}:${PORT} is already in use; the workflow will not kill its owner"
  fi
}

probe_ready() {
  (
    cd "${TPCC_DIR}"
    exec "${TPCC_BIN}" --probe-ready --host "${HOST}" --port "${PORT}"
  ) >>"${RESULT_DIR}/ready_probe.log" 2>&1 &
  PROBE_PID=$!

  local attempts=0
  local probe_rc=0
  while kill -0 "${PROBE_PID}" 2>/dev/null && (( attempts < 40 )); do
    sleep 0.05
    attempts=$((attempts + 1))
  done
  if kill -0 "${PROBE_PID}" 2>/dev/null; then
    stop_probe
    return 1
  fi
  wait "${PROBE_PID}" || probe_rc=$?
  PROBE_PID=""
  return "${probe_rc}"
}

wait_for_ready() {
  local deadline=$(( $(date +%s) + 10#${READY_TIMEOUT_SECONDS} ))
  while (( $(date +%s) <= deadline )); do
    if [[ -z "${SERVER_PID}" ]] || ! kill -0 "${SERVER_PID}" 2>/dev/null; then
      return 1
    fi
    if probe_ready; then
      return 0
    fi
    sleep 0.25
  done
  return 1
}

start_server() {
  local purpose="$1"
  ensure_port_available
  printf '\n[server start: %s]\n' "${purpose}" >>"${SERVER_LOG}"
  log "starting RMDB for ${purpose}"
  (
    cd "${RMDB_DIR}"
    exec env RMDB_PORT="${PORT}" "${SERVER_BIN}" "${DB_NAME}"
  ) >>"${SERVER_LOG}" 2>&1 &
  SERVER_PID=$!
  printf '%s\n' "${SERVER_PID}" >"${RESULT_DIR}/server.pid"
  if ! wait_for_ready; then
    stop_server
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
}

start_existing_database() {
  [[ -d "${DB_PATH}" && ! -L "${DB_PATH}" ]] \
    || die "existing database directory is missing or unsafe: ${DB_PATH}"
  start_server "existing database"
}

run_tester() {
  local log_path="$1"
  shift
  (
    cd "${TPCC_DIR}"
    RMDB_TPCC_CSV_DIR="${CSV_DIR}" \
    RMDB_TPCC_LOAD_DIR="${LOAD_DIR}" \
      "${TPCC_BIN}" "$@"
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
  run_profile_tester "${RESULT_DIR}/setup.log" \
    --create-schema --init --profile "${PROFILE}" --seed "${SEED}" \
    --host "${HOST}" --port "${PORT}" \
    || die "TPC-C setup failed; see ${RESULT_DIR}/setup.log"
}

run_rank() {
  log "running one Rust-owned final2026 benchmark"
  run_profile_tester "${RESULT_DIR}/rank.log" \
    --benchmark --profile "${PROFILE}" --seed "${SEED}" \
    --host "${HOST}" --port "${PORT}" \
    || die "TPC-C ranking failed; see ${RESULT_DIR}/rank.log"
}

run_check() {
  local scope="$1"
  log "running ${scope} consistency checks"
  run_profile_tester "${RESULT_DIR}/check_${scope}.log" \
    --check --check-scope "${scope}" --profile "${PROFILE}" --seed "${SEED}" \
    --host "${HOST}" --port "${PORT}" \
    || die "TPC-C ${scope} checks failed; see ${RESULT_DIR}/check_${scope}.log"
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
  } >"${RESULT_DIR}/summary.md"
}

write_manifest
record_tools

if [[ "${MODE}" == "tools" ]]; then
  WORKFLOW_SUCCEEDED=1
  write_summary
  log "tool report written to ${RESULT_DIR}"
  exit 0
fi

ensure_binaries

case "${MODE}" in
  init)
    start_new_database
    run_setup
    run_check online
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
    crash_server
    start_existing_database
    run_check recovery
    stop_server
    ;;
esac

WORKFLOW_SUCCEEDED=1
write_summary
log "results written to ${RESULT_DIR}"
